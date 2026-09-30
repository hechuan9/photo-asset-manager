import Foundation
import Testing
import KeepsAPI
@testable import KeepsIOSState

@MainActor
struct IOSLibraryStoreTests {
    @Test func returningToDirectoryUsesCacheWithoutAnyNetwork() async {
        let fixture = LibraryFixture()
        let store = fixture.store()
        store.directory = "/a"
        await store.refresh()
        store.directory = "/b"
        await store.refresh()
        store.directory = "/a"
        await store.refresh()
        await store.refresh()
        #expect(fixture.assetRequests == 2)
        #expect(fixture.revisionPaths.isEmpty)
        #expect(store.assets.first?.originalFilename == "/a")
        await store.refreshFromBottom()
        #expect(fixture.assetRequests == 2)
        #expect(fixture.revisionPaths == ["/a"])
        store.showingPicked = true
        await store.refresh()
        #expect(fixture.assetRequests == 3)
    }

    @Test func onlyBottomRefreshChecksChangesAndUpdatingState() async {
        let fixture = LibraryFixture()
        let store = fixture.store()
        await store.refresh()
        fixture.set(revision: 2, updating: true)
        await store.refresh()
        #expect(fixture.assetRequests == 1)
        #expect(fixture.revisionPaths.isEmpty)
        await store.refreshFromBottom()
        #expect(fixture.assetRequests == 2)
        await store.refresh()
        #expect(fixture.assetRequests == 2)
        await store.refreshFromBottom()
        #expect(fixture.assetRequests == 3)
        fixture.set(revision: 2, updating: false)
        await store.refreshFromBottom()
        #expect(fixture.assetRequests == 4)
        await store.refreshFromBottom()
        #expect(fixture.assetRequests == 4)
    }

    @Test func revisionFailurePreservesVisibleContentUntilBottomRetry() async {
        let fixture = LibraryFixture()
        let store = fixture.store()
        await store.refresh()
        let original = store.assets
        fixture.setFailure(true)
        await store.refreshFromBottom()
        #expect(store.assets == original)
        #expect(store.lastError != nil)
        #expect(!store.isLoading)
        fixture.setFailure(false)
        let count = fixture.revisionPaths.count
        await store.refresh()
        #expect(fixture.revisionPaths.count == count)
        await store.refreshFromBottom()
        #expect(store.lastError == nil)
        #expect(fixture.assetRequests == 1)
    }

    @Test func paginationVersionChangeWaitsForExplicitBottomRefresh() async {
        let fixture = LibraryFixture()
        fixture.setCursor("next")
        let store = fixture.store()
        await store.refresh()
        let original = store.assets
        fixture.set(revision: 2, updating: false)
        await store.loadMore()
        #expect(fixture.assetRequests == 2)
        #expect(store.assets == original)
        #expect(store.lastError == "内容已变化，请从底部上拉刷新")
        #expect(!store.canLoadMore)
        await store.loadMore()
        await store.refresh()
        #expect(fixture.assetRequests == 2)
        await store.refreshFromBottom()
        #expect(fixture.assetRequests == 3)
        #expect(store.canLoadMore)
        #expect(store.lastError == nil)
    }

    @Test func updatingPaginationIsVisibleButNeverCachedAsStable() async {
        let fixture = LibraryFixture()
        fixture.set(revision: 1, updating: true)
        fixture.setCursor("next")
        let store = fixture.store()
        await store.refresh()
        await store.loadMore()
        #expect(store.assets.count == 2)
        fixture.setCursor(nil)
        fixture.set(revision: 1, updating: false)
        await store.refresh()
        #expect(fixture.assetRequests == 2)
        #expect(store.assets.count == 2)
        await store.refreshFromBottom()
        #expect(fixture.assetRequests == 3)
        #expect(store.assets.count == 1)
    }

    @Test func credentialChangeClearsCachesAndOldRequestCannotPublish() async {
        let fixture = LibraryFixture()
        fixture.setDelay(0.1)
        let store = fixture.store()
        let first = Task { await store.refresh() }
        while fixture.assetRequests == 0 { await Task.yield() }
        var configuration = fixture.configuration
        configuration.accessCredential = "replacement"
        store.configure(configuration)
        #expect(store.assets.isEmpty)
        fixture.setDelay(0)
        store.directory = "/new"
        await store.refresh()
        await first.value
        #expect(store.assets.first?.originalFilename == "/new")
        #expect(!store.isLoading)
        #expect(fixture.assetRequests == 2)
    }

    @Test func concurrentLoadingDoesNotDuplicateOrCancelRequest() async {
        let fixture = LibraryFixture()
        fixture.setDelay(0.1)
        let store = fixture.store()
        let first = Task { await store.refresh() }
        while fixture.assetRequests == 0 { await Task.yield() }
        await store.refresh()
        await first.value
        #expect(fixture.assetRequests == 1)
        #expect(!store.isLoading)
        #expect(store.assets.count == 1)
    }

    @Test func mutationUpdatesVisibleSnapshotWithoutStartingRefresh() async {
        let fixture = LibraryFixture()
        let store = fixture.store()
        store.directory = "/a"
        await store.refresh()
        store.directory = "/b"
        await store.refresh()
        var updated = store.assets[0]
        updated.rating = 5
        store.update(updated)
        await store.refresh()
        #expect(store.assets[0].rating == 5)
        #expect(fixture.assetRequests == 2)
        #expect(fixture.revisionPaths.isEmpty)
        await store.refreshFromBottom()
        #expect(fixture.assetRequests == 3)
        store.directory = "/a"
        await store.refresh()
        #expect(fixture.assetRequests == 4)
    }

}

private final class LibraryFixture: @unchecked Sendable {
    let configuration: KeepsConfiguration
    let session: URLSession
    private let lock = NSLock()
    private var revision = 1
    private var updating = false
    private var failure = false
    private var cursor: String?
    private var delay: TimeInterval = 0
    private var assetCount = 0
    private var paths: [String] = []
    var assetRequests: Int { lock.withLock { assetCount } }
    var revisionPaths: [String] { lock.withLock { paths } }

    init() {
        let host = UUID().uuidString.lowercased() + ".invalid"
        configuration = KeepsConfiguration(baseURL: URL(string: "https://\(host)")!, libraryID: "library", accessCredential: "initial")
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [LibraryProtocol.self]
        session = URLSession(configuration: config)
        LibraryProtocol.register(host: host) { [weak self] request in self!.response(request) }
    }
    deinit { LibraryProtocol.remove(host: configuration.baseURL.host!); session.invalidateAndCancel() }
    @MainActor func store() -> IOSLibraryStore {
        IOSLibraryStore(configuration: configuration, session: session)
    }
    func set(revision: Int, updating: Bool) { lock.withLock { self.revision = revision; self.updating = updating } }
    func setFailure(_ failure: Bool) { lock.withLock { self.failure = failure } }
    func setCursor(_ cursor: String?) { lock.withLock { self.cursor = cursor } }
    func setDelay(_ delay: TimeInterval) { lock.withLock { self.delay = delay } }
    private func response(_ request: URLRequest) -> (Int, String, TimeInterval) {
        lock.withLock {
            let parts = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)!
            func parameter(_ name: String) -> String? { parts.queryItems?.first { $0.name == name }?.value }
            if parts.path.hasSuffix("revision") {
                paths.append(parameter("path") ?? "root")
                return (failure ? 500 : 200, "{\"revision\":\(revision),\"isUpdating\":\(updating)}", delay)
            }
            assetCount += 1
            let second = parameter("cursor") != nil
            let name = second ? "second" : (parameter("directory") ?? "root")
            let id = second ? "00000000-0000-0000-0000-000000000002" : "00000000-0000-0000-0000-000000000001"
            let next = second ? "null" : cursor.map { "\"\($0)\"" } ?? "null"
            return (200, """
            {"items":[{"id":"\(id)","cameraMake":"","cameraModel":"","lensModel":"","originalFilename":"\(name)","contentFingerprint":"hash","metadataFingerprint":"meta","rating":0,"flagState":"unflagged","tags":[],"createdAt":"now","updatedAt":"now","trashed":false}],"total":1,"nextCursor":\(next),"revision":\(revision),"isUpdating":\(updating)}
            """, delay)
        }
    }
}

private final class LibraryProtocol: URLProtocol, @unchecked Sendable {
    private static let lock = NSLock()
    nonisolated(unsafe) private static var handlers: [String: (URLRequest) -> (Int, String, TimeInterval)] = [:]
    private var work: DispatchWorkItem?
    static func register(host: String, handler: @escaping (URLRequest) -> (Int, String, TimeInterval)) { lock.withLock { handlers[host] = handler } }
    static func remove(host: String) { _ = lock.withLock { handlers.removeValue(forKey: host) } }
    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func startLoading() {
        let handler = Self.lock.withLock { Self.handlers[request.url!.host!]! }
        let (status, body, delay) = handler(request)
        let work = DispatchWorkItem { [weak self] in
            guard let self else { return }
            client?.urlProtocol(self, didReceive: HTTPURLResponse(url: request.url!, statusCode: status, httpVersion: nil, headerFields: nil)!, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: Data(body.utf8))
            client?.urlProtocolDidFinishLoading(self)
        }
        self.work = work
        DispatchQueue.global().asyncAfter(deadline: .now() + delay, execute: work)
    }
    override func stopLoading() { work?.cancel() }
}
