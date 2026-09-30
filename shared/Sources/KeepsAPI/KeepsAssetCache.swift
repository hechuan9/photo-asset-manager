import Foundation

/// A disposable, per-connection cache of complete loaded query prefixes.
@MainActor
public final class KeepsAssetCache {
    private var pages: [KeepsAssetQuery: KeepsAssetPage] = [:]
    private var recent: [KeepsAssetQuery] = []
    private let maxEntries: Int
    private let maxAssets: Int

    public init(maxEntries: Int = 20, maxAssets: Int = 10_000) {
        self.maxEntries = max(0, maxEntries)
        self.maxAssets = max(0, maxAssets)
    }

    public func page(for query: KeepsAssetQuery) -> KeepsAssetPage? {
        let key = key(for: query)
        guard let page = pages[key] else { return nil }
        touch(key)
        return page
    }

    public func store(_ page: KeepsAssetPage, for query: KeepsAssetQuery) {
        let key = key(for: query)
        pages.removeValue(forKey: key)
        recent.removeAll { $0 == key }
        guard maxEntries > 0, page.items.count <= maxAssets else { return }
        pages[key] = page
        touch(key)
        var assetCount = pages.values.reduce(0) { $0 + $1.items.count }
        while recent.count > maxEntries || assetCount > maxAssets {
            let oldest = recent.removeFirst()
            assetCount -= pages.removeValue(forKey: oldest)?.items.count ?? 0
        }
    }

    public func removeAll() {
        pages.removeAll()
        recent.removeAll()
    }

    private func key(for query: KeepsAssetQuery) -> KeepsAssetQuery {
        var key = query
        key.cursor = nil
        return key
    }

    private func touch(_ key: KeepsAssetQuery) {
        recent.removeAll { $0 == key }
        recent.append(key)
    }
}
