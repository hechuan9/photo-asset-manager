import Foundation
import Testing
import KeepsAPI
@testable import KeepsIOSState

@MainActor
struct IOSDirectoryStoreTests {
    let configuration = KeepsConfiguration(baseURL: URL(string: "https://example.test")!, libraryID: "library")

    private func response(_ name: String = "photo") throws -> KeepsNavigation {
        let json = """
        {"directories":[{"path":"/\(name)","name":"\(name)","photoCount":1,"hasChildren":true}]}
        """
        let decoder = JSONDecoder()
        return try decoder.decode(KeepsNavigation.self, from: Data(json.utf8))
    }

    @Test func cachedDirectoriesOnlyReloadOnExplicitRequest() async throws {
        var requests = 0
        let store = IOSDirectoryStore() { _, _ in
            requests += 1
            return try response("version\(requests)")
        }
        await store.load(configuration: configuration, path: nil)
        await store.load(configuration: configuration, path: nil)
        #expect(requests == 1)
        await store.load(configuration: configuration, path: nil)
        #expect(requests == 1)
        await store.load(configuration: configuration, path: nil)
        #expect(requests == 1)
        await store.load(configuration: configuration, path: nil, force: true)
        #expect(requests == 2)
        #expect(store.state(for: nil, configuration: configuration).directories?.first?.name == "version2")
    }

    @Test func failedRefreshPreservesDirectoriesAndThrottlesRetry() async throws {
        var requests = 0
        let store = IOSDirectoryStore() { _, _ in
            requests += 1
            if requests > 1 { throw URLError(.notConnectedToInternet) }
            return try response()
        }
        await store.load(configuration: configuration, path: "photo")
        await store.load(configuration: configuration, path: "photo", force: true)
        let state = store.state(for: "photo", configuration: configuration)
        #expect(state.directories?.first?.name == "photo")
        #expect(state.error != nil)
        #expect(!state.loading)
        await store.load(configuration: configuration, path: "photo")
        #expect(requests == 2)
    }

    @Test func emptyDirectoriesAreCachedAndPathsStaySeparate() async throws {
        var requests = 0
        let empty = try JSONDecoder().decode(KeepsNavigation.self, from: Data("{\"directories\":[]}".utf8))
        let store = IOSDirectoryStore(fetch: { _, path in
            requests += 1
            return path == nil ? empty : try response()
        })
        await store.load(configuration: configuration, path: nil)
        await store.load(configuration: configuration, path: "photo")
        await store.load(configuration: configuration, path: nil)
        #expect(requests == 2)
        #expect(store.state(for: nil, configuration: configuration).directories == [])
        #expect(store.state(for: "photo", configuration: configuration).directories?.count == 1)
    }

    @Test func inFlightRequestsCoalesceAndOldConnectionCannotPublish() async throws {
        var requests = 0
        var continuation: CheckedContinuation<KeepsNavigation, Error>?
        let store = IOSDirectoryStore(fetch: { _, _ in
            requests += 1
            return try await withCheckedThrowingContinuation { continuation = $0 }
        })
        let task = Task { await store.load(configuration: configuration, path: nil) }
        while continuation == nil { await Task.yield() }
        await store.load(configuration: configuration, path: nil)
        #expect(requests == 1)
        let other = KeepsConfiguration(baseURL: configuration.baseURL, libraryID: "other")
        store.configure(other)
        continuation?.resume(returning: try response())
        await task.value
        #expect(store.state(for: nil, configuration: other).directories == nil)
    }

    @Test func closingPanelDoesNotDiscardCacheFill() async throws {
        var continuation: CheckedContinuation<KeepsNavigation, Error>?
        var requests = 0
        let store = IOSDirectoryStore(fetch: { _, _ in
            requests += 1
            if requests == 1 {
                return try await withCheckedThrowingContinuation { continuation = $0 }
            }
            return try response()
        })
        let task = Task { await store.load(configuration: configuration, path: nil) }
        while continuation == nil { await Task.yield() }
        task.cancel()
        continuation?.resume(returning: try response())
        await task.value
        #expect(!store.state(for: nil, configuration: configuration).loading)
        await store.load(configuration: configuration, path: nil)
        #expect(requests == 1)
        #expect(store.state(for: nil, configuration: configuration).directories?.count == 1)
    }
}
