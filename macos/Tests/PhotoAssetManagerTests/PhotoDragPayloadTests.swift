import Foundation
import Testing
import KeepsAPI
@testable import PhotoAssetManager

@MainActor struct PhotoDragPayloadTests {
    @Test func draggingSelectedAssetIncludesGroupAndUnselectedAssetIncludesOnlyItself() async throws {
        let store = try await loadedStore()
        let ids = store.assets.map(\.id)
        store.select(ids[0])
        store.select(ids[2], extending: true)
        let selectedPayload = try #require(store.photoDragPayload(for: ids[0]))
        #expect(Set(selectedPayload.assetIDs) == [ids[0], ids[2]])
        let otherPayload = try #require(store.photoDragPayload(for: ids[1]))
        #expect(otherPayload.assetIDs == [ids[1]])
        #expect(store.canMovePhotos(selectedPayload, to: "/target"))
        #expect(!store.canMovePhotos(selectedPayload, to: "/source"))
        #expect(store.photoDragProvider(for: ids[0]).registeredTypeIdentifiers.contains(PhotoDragPayload.pasteboardType))
    }

    @Test func staleDirectoryQueryAndConnectionPayloadsAreRejected() async throws {
        let store = try await loadedStore()
        let payload = try #require(store.photoDragPayload(for: store.assets[0].id))
        let roundTrip = try JSONDecoder().decode(PhotoDragPayload.self, from: JSONEncoder().encode(payload))
        #expect(store.canMovePhotos(roundTrip, to: "/target"))
        store.query.directory = "/other"
        #expect(!store.canMovePhotos(payload, to: "/target"))
        store.query = payload.query
        store.query.minRating = 3
        #expect(!store.canMovePhotos(payload, to: "/target"))
        store.query = payload.query
        let staleConnection = PhotoDragPayload(assetIDs: payload.assetIDs, sourcePath: payload.sourcePath,
            baseURL: "https://other.invalid", libraryID: payload.libraryID, query: payload.query)
        #expect(!store.canMovePhotos(staleConnection, to: "/target"))
        let staleLibrary = PhotoDragPayload(assetIDs: payload.assetIDs, sourcePath: payload.sourcePath,
            baseURL: payload.baseURL, libraryID: "other", query: payload.query)
        #expect(!store.canMovePhotos(staleLibrary, to: "/target"))
    }

    @Test func selectAllPaginationAndNoDirectoryDisableDragging() async throws {
        let store = try await loadedStore(paginated: true)
        let id = store.assets[0].id
        let payload = try #require(store.photoDragPayload(for: id))
        store.selectAll()
        #expect(store.isSelectingAll)
        #expect(store.photoDragPayload(for: id) == nil)
        #expect(!store.canMovePhotos(payload, to: "/target"))
        try await waitUntil { !store.isLoading }
        #expect(store.photoDragPayload(for: id)?.assetIDs.count == 5)
        store.query.directory = nil
        #expect(store.photoDragPayload(for: id) == nil)
    }

    private func loadedStore(paginated: Bool = false) async throws -> LibraryStore {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [PhotoDragProtocol.self]
        let store = LibraryStore(configuration: KeepsConfiguration(baseURL: URL(string: paginated ? "https://pages.invalid" : "https://all.invalid")!, libraryID: "selection"), session: URLSession(configuration: configuration), loadSavedSettings: false, preferences: UserDefaults(suiteName: UUID().uuidString)!, persistConfiguration: { _ in })
        store.query.directory = "/source"
        store.refresh()
        try await waitUntil { !store.isLoading }
        try #require(store.lastError == nil)
        return store
    }

    private func waitUntil(_ predicate: () -> Bool) async throws {
        for _ in 0..<200 {
            if predicate() { return }
            try await Task.sleep(for: .milliseconds(5))
        }
        Issue.record("selection request did not complete")
    }
}

private final class PhotoDragProtocol: URLProtocol, @unchecked Sendable {
    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func startLoading() {
        let url = request.url!
        let body: String
        if url.path.hasSuffix("/assets") {
            let paginated = url.host == "pages.invalid"
            let cursor = URLComponents(url: url, resolvingAgainstBaseURL: false)?.queryItems?.contains { $0.name == "cursor" } == true
            let numbers = paginated ? (cursor ? Array(3...5) : Array(1...2)) : Array(1...5)
            let items = numbers.map { number in
                """
                {"id":"00000000-0000-0000-0000-00000000000\(number)","cameraMake":"","cameraModel":"","lensModel":"","originalFilename":"photo.jpg","contentFingerprint":"hash","metadataFingerprint":"meta","rating":0,"flagState":"unflagged","tags":[],"createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:00Z","trashed":false}
                """
            }.joined(separator: ",")
            body = "{\"items\":[\(items)],\"total\":5,\"revision\":1,\"isUpdating\":false" + (paginated && !cursor ? ",\"nextCursor\":\"next\"}" : "}")
        } else if url.path.hasSuffix("/revision") { body = "{\"revision\":1,\"isUpdating\":false}" }
        else { body = "{\"all\":5,\"picked\":0,\"trashed\":0}" }
        client?.urlProtocol(self, didReceive: HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: Data(body.utf8))
        client?.urlProtocolDidFinishLoading(self)
    }
    override func stopLoading() {}
}
