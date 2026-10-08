import Foundation
import ImageIO
import KeepsAPI
import Testing
@testable import PhotoAssetManager

@MainActor struct AIEditingBatchTests {
    @Test func confirmationFreezesSelectionAndSurvivesRestart() throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        let preferences = UserDefaults(suiteName: UUID().uuidString)!
        let library = LibraryStore(configuration: .init(baseURL: URL(string: "https://batch.invalid")!, libraryID: "test"), loadSavedSettings: false, preferences: preferences)
        let selected = Set([UUID(), UUID(), UUID()])
        library.selectedIDs = selected
        let batch = AIEditingBatchStore(root: root)
        batch.prepare(library: library)
        #expect(batch.isAwaitingConfirmation)
        #expect(batch.totalCount == 3)
        #expect(library.isOperationBlocking)
        library.selectedIDs = [UUID()]
        #expect(Set(batch.batch!.items.map(\.assetID)) == selected)
        let restored = AIEditingBatchStore(root: root)
        #expect(restored.totalCount == 3)
        #expect(restored.isAwaitingConfirmation)
        #expect(restored.batch?.items.map(\.id) == batch.batch?.items.map(\.id))
        restored.restore(library: library)
        #expect(!restored.isRunning)
        restored.cancel()
        #expect(restored.isFinished)
        #expect(library.isOperationBlocking)
        restored.dismiss()
        #expect(!library.isOperationBlocking)
        #expect(AIEditingBatchStore(root: root).batch == nil)
    }

    @Test func importingOrOtherOperationCannotStartBatch() {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        let library = LibraryStore(configuration: .init(baseURL: URL(string: "https://batch.invalid")!, libraryID: "test"), loadSavedSettings: false)
        library.selectedIDs = [UUID()]
        let batch = AIEditingBatchStore(root: root)
        library.openImportWindows = [UUID()]
        batch.prepare(library: library)
        #expect(!batch.isBlocking)
        library.openImportWindows = []
        library.isAISettingsBusy = true
        batch.prepare(library: library)
        #expect(!batch.isBlocking)
    }

    @Test func recoveryKeepsPendingUploadAndIdempotencyIdentity() throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        let item = AIEditingBatch.Item(id: UUID(), assetID: UUID(), name: "public.jpg", phase: .uploading,
            source: .init(negativeContentHash: String(repeating: "a", count: 64), revision: 12, hasEdit: false, sourceAvailable: true),
            result: .init(status: "selected", reason: "verified", fullSize: root.appendingPathComponent("full.jpg"), recipeJSON: "{}", xmp: "<xmp/>"))
        var batch = AIEditingBatch(id: UUID(), baseURL: "https://batch.invalid", libraryID: "test", items: [item])
        batch.confirmed = true
        try JSONEncoder().encode(batch).write(to: root.appendingPathComponent("batch.json"))
        let restored = AIEditingBatchStore(root: root)
        #expect(restored.batch?.items[0].id == item.id)
        #expect(restored.batch?.items[0].phase == .uploading)
        #expect(restored.batch?.items[0].source?.revision == 12)
        #expect(restored.batch?.items[0].result?.recipeJSON == "{}")
        #expect(!restored.isAwaitingConfirmation)
        #expect(!restored.isFinished)
    }

    @Test func createsAllThreeDisplaySizesFromPublicImage() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        let source = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
            .appendingPathComponent("scripts/runtime-sample/sample.jpg")
        let before = try await AIEditingImages.inspect(source, image: true)
        let outputs = try await AIEditingImages.derivatives(from: source, directory: root)
        #expect(Set(outputs.keys) == ["standard", "thumbnail", "browse"])
        for (role, maximum) in [("standard", 1280), ("thumbnail", 512), ("browse", 64)] {
            let info = try await AIEditingImages.inspect(outputs[role]!, image: true)
            #expect(max(info.width, info.height) == maximum)
            #expect(info.size > 0)
            #expect(info.hash.count == 64)
        }
        #expect(try await AIEditingImages.inspect(source, image: false).hash == before.hash)
    }

    @Test func restartAfterLostCommitResponseDoesNotRegradeOrReupload() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        let runtime = root.appendingPathComponent("runtime")
        for name in ["codex", "codex-code-mode-host", "keeps-color-mcp", "darktable.app/Contents/MacOS/darktable-cli"] {
            let url = runtime.appendingPathComponent(name)
            try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
            try Data("#!/bin/sh\nexit 91\n".utf8).write(to: url)
            try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: url.path)
        }
        try Data("test".utf8).write(to: runtime.appendingPathComponent("SKILL.md"))
        let home = root.appendingPathComponent("editor/codex")
        try FileManager.default.createDirectory(at: home, withIntermediateDirectories: true)
        let payload = try JSONSerialization.data(withJSONObject: ["email": "test@example.com"]).base64EncodedString()
        try JSONSerialization.data(withJSONObject: ["tokens": ["id_token": "header.\(payload).signature"]]).write(to: home.appendingPathComponent("auth.json"))
        let editor = AIEditingSettingsStore(root: home.deletingLastPathComponent(), runtime: runtime, defaults: UserDefaults(suiteName: UUID().uuidString)!)
        editor.expectedEmail = "test@example.com"
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [CommittedBatchProtocol.self]
        let library = LibraryStore(configuration: .init(baseURL: URL(string: "https://committed.invalid")!, libraryID: "test"), session: URLSession(configuration: configuration), loadSavedSettings: false)
        let id = UUID()
        let item = AIEditingBatch.Item(id: id, assetID: id, name: "sample.jpg", phase: .uploading,
            source: .init(negativeContentHash: String(repeating: "a", count: 64), revision: 1, hasEdit: false,
                          sourceFilename: "sample.jpg", sourceAvailable: true))
        var manifest = AIEditingBatch(id: UUID(), baseURL: "https://committed.invalid", libraryID: "test", items: [item])
        manifest.confirmed = true
        try JSONEncoder().encode(manifest).write(to: root.appendingPathComponent("batch.json"))
        let restored = AIEditingBatchStore(root: root, editor: editor)
        restored.restore(library: library)
        for _ in 0..<200 where restored.isRunning { try await Task.sleep(for: .milliseconds(5)) }
        #expect(restored.errorMessage == nil)
        #expect(restored.isFinished)
        #expect(restored.completedCount == 1)
        #expect(restored.batch?.items[0].phase == .done)
        #expect(library.isOperationBlocking)

        let rejectedJob = root.appendingPathComponent("rejected")
        try FileManager.default.createDirectory(at: rejectedJob, withIntermediateDirectories: true)
        try Data("{}".utf8).write(to: rejectedJob.appendingPathComponent("result.json"))
        let source = root.appendingPathComponent("sample.jpg")
        try Data([0xff, 0xd8, 0xff, 0xd9]).write(to: source)
        do {
            _ = try await editor.gradePhoto(source, job: rejectedJob)
            Issue.record("The deliberately failing fixture executable must be invoked on retry")
        } catch { #expect(error.localizedDescription.contains("Codex 版本")) }
        #expect(!FileManager.default.fileExists(atPath: rejectedJob.appendingPathComponent("result.json").path))
        #expect(try FileManager.default.contentsOfDirectory(atPath: rejectedJob.path).contains { $0.hasPrefix("rejected-result-") })
    }

    @Test func savedResultCanResumeWithoutAIAccountAndProgressStopsOnCancel() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let full = root.appendingPathComponent("completed.jpg")
        try Data("preserved result".utf8).write(to: full)
        let item = AIEditingBatch.Item(id: UUID(), assetID: UUID(), name: "public.jpg", phase: .uploading,
            source: .init(negativeContentHash: String(repeating: "a", count: 64), revision: 1, hasEdit: false, sourceFilename: "public.jpg", sourceAvailable: true),
            result: .init(status: "selected", reason: "done", fullSize: full, recipeJSON: "{}", xmp: "xmp"))
        var state = AIEditingBatch(id: UUID(), baseURL: "https://waiting.invalid", libraryID: "test", items: [item])
        state.confirmed = true
        try JSONEncoder().encode(state).write(to: root.appendingPathComponent("batch.json"))
        let config = URLSessionConfiguration.ephemeral; config.protocolClasses = [WaitingBatchProtocol.self]
        let library = LibraryStore(configuration: .init(baseURL: URL(string: state.baseURL)!, libraryID: "test"), session: URLSession(configuration: config), loadSavedSettings: false, preferences: UserDefaults(suiteName: UUID().uuidString)!)
        let editor = AIEditingSettingsStore(root: root.appendingPathComponent("no-account"), runtime: root, defaults: UserDefaults(suiteName: UUID().uuidString)!)
        let store = AIEditingBatchStore(root: root, editor: editor)
        store.restore(library: library)
        let deadline = Date().addingTimeInterval(3)
        while !store.activeItems.contains(where: { $0.ceiling > $0.progress }), Date() < deadline {
            try await Task.sleep(for: .milliseconds(5))
        }
        #expect(store.isRunning && store.isAwaitingUpload)
        #expect(store.errorMessage == nil)
        #expect(store.pendingResultURL == full)
        let first = store.overallProgress
        store.tickProgress(); store.tickProgress()
        #expect(store.overallProgress > first)
        #expect(store.overallProgress < 1)
        store.cancel()
        for _ in 0..<100 where store.isRunning { try await Task.sleep(for: .milliseconds(5)) }
        let paused = store.overallProgress
        store.tickProgress()
        #expect(store.overallProgress == paused)
        #expect(store.batch?.items.first?.phase == .uploading)
        #expect(try Data(contentsOf: full) == Data("preserved result".utf8))
    }

    @Test func concurrentUploadsRespectLimitAndOneFailureDoesNotBlockOthers() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let failedID = UUID(uuidString: "00000000-0000-0000-0000-000000000001")!
        let ids = [failedID] + (0..<5).map { _ in UUID() }
        let items = ids.map { id in
            AIEditingBatch.Item(id: id, assetID: id, name: "sample.jpg", phase: .uploading,
                source: .init(negativeContentHash: String(repeating: "a", count: 64), revision: 1,
                              hasEdit: false, sourceFilename: "sample.jpg", sourceAvailable: true))
        }
        var manifest = AIEditingBatch(id: UUID(), baseURL: "https://parallel.invalid", libraryID: "test", items: items)
        manifest.confirmed = true
        try JSONEncoder().encode(manifest).write(to: root.appendingPathComponent("batch.json"))
        let defaults = UserDefaults(suiteName: UUID().uuidString)!
        var limits = AIEditingLimits(); limits.photos = 4; limits.uploads = 2
        limits.save(defaults: defaults)
        let editor = AIEditingSettingsStore(root: root.appendingPathComponent("editor"), runtime: root, defaults: defaults)
        let config = URLSessionConfiguration.ephemeral; config.protocolClasses = [ConcurrentBatchProtocol.self]
        let library = LibraryStore(configuration: .init(baseURL: URL(string: manifest.baseURL)!, libraryID: "test"),
            session: URLSession(configuration: config), loadSavedSettings: false, preferences: defaults)
        ConcurrentBatchProtocol.reset()
        let store = AIEditingBatchStore(root: root, editor: editor)
        store.restore(library: library)
        var peakPhotos = 0
        for _ in 0..<400 where store.isRunning {
            peakPhotos = max(peakPhotos, store.activeCount)
            try await Task.sleep(for: .milliseconds(10))
        }
        #expect(!store.isRunning)
        #expect(store.completedCount == 5)
        #expect(store.failedItems.map(\.id) == [failedID])
        #expect(ConcurrentBatchProtocol.peak == 2)
        #expect(peakPhotos == 4)
        #expect(store.batch?.items.allSatisfy { ($0.phaseSeconds?["uploading"] ?? 0) > 0 } == true)
        let restored = AIEditingBatchStore(root: root, editor: editor)
        #expect(restored.completedCount == 5)
        #expect(restored.failedItems.map(\.id) == [failedID])
    }

    @Test func onlyTransientNetworkErrorsRetryAutomatically() {
        #expect(AIEditingBatchStore.isTransient(URLError(.notConnectedToInternet)))
        #expect(AIEditingBatchStore.isTransient(KeepsAPIError.http(503, "offline")))
        #expect(!AIEditingBatchStore.isTransient(KeepsAPIError.http(409, "conflict")))
        #expect(!AIEditingBatchStore.isTransient(KeepsAPIError.http(401, "login")))
        #expect(!AIEditingBatchStore.isTransient(URLError(.cancelled)))
    }
}

private final class CommittedBatchProtocol: URLProtocol, @unchecked Sendable {
    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func startLoading() {
        guard request.httpMethod == "GET", request.url?.lastPathComponent == "edit" else {
            Issue.record("A committed item must not start new grading or upload requests")
            client?.urlProtocol(self, didFailWithError: URLError(.badURL)); return
        }
        let id = request.url!.deletingLastPathComponent().lastPathComponent.lowercased()
        let state = KeepsEditState(negativeContentHash: String(repeating: "a", count: 64), revision: 2, hasEdit: true, sourceAvailable: true, lastRequestID: id)
        do {
            let data = try JSONEncoder().encode(state)
            client?.urlProtocol(self, didReceive: HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: data)
            client?.urlProtocolDidFinishLoading(self)
        } catch { client?.urlProtocol(self, didFailWithError: error) }
    }
    override func stopLoading() {}
}

private final class WaitingBatchProtocol: URLProtocol, @unchecked Sendable {
    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func startLoading() {}
    override func stopLoading() {}
}

private final class ConcurrentBatchProtocol: URLProtocol, @unchecked Sendable {
    private static let lock = NSLock()
    nonisolated(unsafe) private static var active = 0
    nonisolated(unsafe) private static var maximum = 0
    static var peak: Int { lock.withLock { maximum } }
    static func reset() { lock.withLock { active = 0; maximum = 0 } }
    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func startLoading() {
        Self.lock.withLock { Self.active += 1; Self.maximum = max(Self.maximum, Self.active) }
        DispatchQueue.global().asyncAfter(deadline: .now() + 0.05) { [self] in
            Self.lock.withLock { Self.active -= 1 }
            let id = request.url!.deletingLastPathComponent().lastPathComponent.lowercased()
            let failed = id == "00000000-0000-0000-0000-000000000001"
            let state = KeepsEditState(negativeContentHash: String(repeating: "a", count: 64), revision: 2,
                hasEdit: true, sourceAvailable: true, lastRequestID: id)
            let data = failed ? Data("conflict".utf8) : try! JSONEncoder().encode(state)
            client?.urlProtocol(self, didReceive: HTTPURLResponse(url: request.url!, statusCode: failed ? 409 : 200,
                httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: data)
            client?.urlProtocolDidFinishLoading(self)
        }
    }
    override func stopLoading() {}
}
