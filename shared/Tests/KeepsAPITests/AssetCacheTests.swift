import Foundation
import Testing
@testable import KeepsAPI

@MainActor
struct AssetCacheTests {
    private func page(revision: Int64 = 1, updating: Bool = false, count: Int = 0) throws -> KeepsAssetPage {
        let asset = try JSONDecoder().decode(KeepsAsset.self, from: Data("""
        {"id":"00000000-0000-0000-0000-000000000001","cameraMake":"","cameraModel":"","lensModel":"","originalFilename":"a.jpg","contentFingerprint":"h","metadataFingerprint":"m","rating":0,"flagState":"unflagged","tags":[],"createdAt":"2026-09-28","updatedAt":"2026-09-28","trashed":false}
        """.utf8))
        return KeepsAssetPage(items: Array(repeating: asset, count: count), total: count, nextCursor: "next", revision: revision, isUpdating: updating)
    }

    @Test func equalVersionRequiresBothSnapshotsToBeStable() throws {
        let stable = try page()
        #expect(stable.isCurrent(at: KeepsCatalogRevision(revision: 1, isUpdating: false)))
        #expect(!stable.isCurrent(at: KeepsCatalogRevision(revision: 1, isUpdating: true)))
        #expect(!stable.isCurrent(at: KeepsCatalogRevision(revision: 2, isUpdating: false)))
        #expect(try !page(updating: true).isCurrent(at: KeepsCatalogRevision(revision: 1, isUpdating: false)))
    }

    @Test func fullQueryIdentitySeparatesFiltersButRestoresLoadedPrefixAcrossCursors() throws {
        let cache = KeepsAssetCache()
        let query = KeepsAssetQuery()
        cache.store(try page(), for: query)
        var continued = query
        continued.cursor = "page2"
        #expect(cache.page(for: continued)?.nextCursor == "next")
        let changes: [(inout KeepsAssetQuery) -> Void] = [
            { $0.q = "q|true" }, { $0.minRating = 4 }, { $0.flagState = "picked" },
            { $0.colorLabel = "red" }, { $0.tag = "tag" }, { $0.trashed = true },
            { $0.sort = "capture_asc" }, { $0.folderID = "folder" }, { $0.directory = "/photos" },
            { $0.recursive = false }, { $0.showHidden = true }, { $0.limit = 200 }
        ]
        for change in changes {
            var other = query
            change(&other)
            #expect(cache.page(for: other) == nil)
        }
        #expect(cache.page(for: query)?.items.isEmpty == true)
    }

    @Test func cacheEvictsLeastRecentlyUsedQueriesAndBoundsRetainedAssets() throws {
        let cache = KeepsAssetCache(maxEntries: 2, maxAssets: 3)
        var a = KeepsAssetQuery(); a.directory = "/a"
        var b = KeepsAssetQuery(); b.directory = "/b"
        var c = KeepsAssetQuery(); c.directory = "/c"
        cache.store(try page(count: 1), for: a)
        cache.store(try page(count: 1), for: b)
        _ = cache.page(for: a)
        cache.store(try page(count: 2), for: c)
        #expect(cache.page(for: b) == nil)
        #expect(cache.page(for: a) != nil)
        #expect(cache.page(for: c)?.items.count == 2)
        cache.store(try page(count: 4), for: c)
        #expect(cache.page(for: c) == nil)
        cache.removeAll()
        #expect(cache.page(for: a) == nil)
    }
}
