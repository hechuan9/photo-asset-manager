import Foundation
import Testing
import KeepsAPI
@testable import PhotoAssetManager

@MainActor struct DirectoryTrashTrackingTests {
    private func store(_ host: String, preferences: UserDefaults) -> LibraryStore {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [TrashProtocol.self]
        let store = LibraryStore(configuration: KeepsConfiguration(baseURL: URL(string: "https://\(host)")!, libraryID: "test"),
            session: URLSession(configuration: configuration), loadSavedSettings: false, preferences: preferences)
        store.directoryTrashPollInterval = .milliseconds(5)
        return store
    }

    @Test func submissionTimeoutAndTemporaryPollingFailureStillCompleteOneTask() async throws {
        let preferences = UserDefaults(suiteName: UUID().uuidString)!
        let store = store("timeout.invalid", preferences: preferences)
        let directory = KeepsNavigationDirectory(path: "folder", name: "folder", photoCount: 1, hasChildren: false)
        let task = Task { try await store.trashDirectory(directory, confirmationName: "folder") }
        while !store.isDirectoryTrashBlocking { await Task.yield() }
        store.showLibrary(directory: "other")
        store.setDirectoryExpanded("other", expanded: true)
        #expect(store.query.directory == nil)
        #expect(store.expandedPaths.isEmpty)
        #expect(await store.checkConnection(baseURL: "https://other.invalid", libraryID: "other", accessCredential: "", save: true) == false)
        do {
            try await store.trashDirectory(directory, confirmationName: "folder")
            Issue.record("duplicate deletion accepted")
        } catch {}
        try await task.value
        #expect(!store.isDirectoryTrashBlocking)
        #expect(TrashProtocol.count("timeout.invalidPOST") == 1)
        #expect(TrashProtocol.count("timeout.invalidGET") == 2)
        #expect(preferences.data(forKey: "keeps.pendingDirectoryTrash") == nil)
    }

    @Test func restartQueriesAcceptedTaskWithoutResubmitting() async throws {
        let preferences = UserDefaults(suiteName: UUID().uuidString)!
        let pending = PendingDirectoryTrash(id: UUID(), path: "folder", name: "folder", baseURL: "https://resume.invalid", libraryID: "test", startedAt: Date(), accepted: true)
        preferences.set(try JSONEncoder().encode(pending), forKey: "keeps.pendingDirectoryTrash")
        let store = store("resume.invalid", preferences: preferences)
        #expect(store.isDirectoryTrashBlocking)
        #expect(store.directoryToTrash?.path == "folder")
        await store.directoryTrashTracking?.value
        #expect(!store.isDirectoryTrashBlocking)
        #expect(TrashProtocol.count("resume.invalidPOST") == 0)
        #expect(TrashProtocol.count("resume.invalidGET") == 1)
    }

    @Test func explicitSubmission404StopsAndCanBeAcknowledged() async throws {
        let preferences = UserDefaults(suiteName: UUID().uuidString)!
        let store = store("reject.invalid", preferences: preferences)
        let directory = KeepsNavigationDirectory(path: "folder", name: "folder", photoCount: 1, hasChildren: false)
        try await store.trashDirectory(directory, confirmationName: "folder")
        #expect(store.directoryTrashFinished)
        #expect(TrashProtocol.count("reject.invalidPOST") == 1)
        #expect(TrashProtocol.count("reject.invalidGET") == 0)
        let restored = self.store("reject.invalid", preferences: preferences)
        #expect(restored.directoryTrashFinished)
        #expect(restored.directoryTrashTracking == nil)
        #expect(TrashProtocol.count("reject.invalidPOST") == 1)
        store.acknowledgeDirectoryTrashFailure()
        #expect(!store.isDirectoryTrashBlocking)
    }

    @Test func unacceptedRecoveredTaskRetriesWithPersistedRequestID() async throws {
        let preferences = UserDefaults(suiteName: UUID().uuidString)!
        let pending = PendingDirectoryTrash(id: UUID(), path: "folder", name: "folder", baseURL: "https://retry.invalid", libraryID: "test", startedAt: Date(), accepted: false)
        preferences.set(try JSONEncoder().encode(pending), forKey: "keeps.pendingDirectoryTrash")
        let store = store("retry.invalid", preferences: preferences)
        await store.directoryTrashTracking?.value
        #expect(!store.isDirectoryTrashBlocking)
        #expect(TrashProtocol.count("retry.invalidPOST") == 1)
        #expect(TrashProtocol.count(pending.id.uuidString.lowercased()) == 1)
    }

    @Test func missingAcceptedTaskRequiresAcknowledgementAndNeverRepeatsDeletion() async throws {
        let preferences = UserDefaults(suiteName: UUID().uuidString)!
        let pending = PendingDirectoryTrash(id: UUID(), path: "folder", name: "folder", baseURL: "https://missing.invalid", libraryID: "test", startedAt: Date(), accepted: true)
        preferences.set(try JSONEncoder().encode(pending), forKey: "keeps.pendingDirectoryTrash")
        let store = store("missing.invalid", preferences: preferences)
        await store.directoryTrashTracking?.value
        #expect(store.isDirectoryTrashBlocking)
        #expect(store.directoryTrashFinished)
        #expect(store.directoryTrashMessage?.contains("不会重新提交") == true)
        #expect(TrashProtocol.count("missing.invalidPOST") == 0)
        store.acknowledgeDirectoryTrashFailure()
        #expect(!store.isDirectoryTrashBlocking)
    }
}

private final class TrashProtocol: URLProtocol, @unchecked Sendable {
    private static let state = Counts()
    private final class Counts: @unchecked Sendable {
        let lock = NSLock()
        var requests: [String: Int] = [:]
        func value(_ key: String, increment: Bool) -> Int {
            lock.lock(); defer { lock.unlock() }
            if increment { requests[key, default: 0] += 1 }
            return requests[key, default: 0]
        }
    }
    static func count(_ key: String) -> Int { state.value(key, increment: false) }
    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func startLoading() {
        let url = request.url!
        guard url.path.contains("/directories/trash") else {
            client?.urlProtocol(self, didFailWithError: URLError(.unsupportedURL)); return
        }
        let host = url.host!
        let method = request.httpMethod!
        let count = Self.state.value(host + method, increment: true)
        if host == "timeout.invalid", method == "POST" || count == 1 {
            client?.urlProtocol(self, didFailWithError: URLError(.timedOut)); return
        }
        let status = host == "missing.invalid" || host == "reject.invalid" || (host == "retry.invalid" && method == "GET") ? 404 : 200
        var id = url.lastPathComponent
        if method == "POST" {
            var data = request.httpBody ?? Data()
            if let stream = request.httpBodyStream {
                stream.open(); defer { stream.close() }
                var buffer = [UInt8](repeating: 0, count: 4096)
                while stream.hasBytesAvailable {
                    let size = stream.read(&buffer, maxLength: buffer.count)
                    if size <= 0 { break }
                    data.append(buffer, count: size)
                }
            }
            let object = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
            id = object?["requestID"] as? String ?? "missing-request-id"
            _ = Self.state.value(id, increment: true)
        }
        let body = status == 404 ? "task missing" : "{\"id\":\"\(id)\",\"path\":\"folder\",\"status\":\"completed\",\"phase\":\"completed\",\"createdAt\":0,\"updatedAt\":1}"
        client?.urlProtocol(self, didReceive: HTTPURLResponse(url: url, statusCode: status, httpVersion: nil, headerFields: nil)!, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: Data(body.utf8))
        client?.urlProtocolDidFinishLoading(self)
    }
    override func stopLoading() {}
}
