import Foundation
import Testing
import KeepsAPI
@testable import KeepsIOSState

@MainActor
struct IOSDirectoryStoreTests {
    @Test func directoryBrowsingReadsPersistentDatabaseAndOnlyExplicitRefreshSynchronizes() async throws {
        let root = URL.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        let configuration = KeepsConfiguration(baseURL: URL(string: "https://example.invalid")!, libraryID: "test")
        let db = try KeepsLibraryDatabase(configuration: configuration, rootDirectory: root)
        let navigation = try JSONDecoder().decode(KeepsNavigation.self, from: Data(#"{"directories":[{"path":"/photos","name":"photos","photoCount":10,"hasChildren":false}]}"#.utf8))
        try db.saveNavigation(navigation)
        let reopened = try KeepsLibraryDatabase(configuration: configuration, rootDirectory: root)
        let store = IOSDirectoryStore()
        store.configure(configuration, database: reopened)
        var syncs = 0
        store.synchronize = { syncs += 1 }
        #expect(store.state(for: nil, configuration: configuration).directories?.first?.path == "/photos")
        await store.load(configuration: configuration, path: nil)
        #expect(syncs == 0)
        await store.load(configuration: configuration, path: nil, force: true)
        #expect(syncs == 1)
        store.configure(nil, database: nil)
        #expect(store.state(for: nil, configuration: nil).directories == nil)
    }
}
