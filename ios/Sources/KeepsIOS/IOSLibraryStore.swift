import Foundation
import SwiftUI
import KeepsAPI

@MainActor
final class IOSLibraryStore: ObservableObject {
    @Published private(set) var assets: [KeepsAsset] = []
    @Published private(set) var total = 0
    @Published private(set) var isLoading = false
    @Published private(set) var configuration: KeepsConfiguration?
    @Published var lastError: String?
    @Published var search = ""
    @Published var showingTrash = false
    @Published var sort = "capture_desc"
    private var nextCursor: String?
    private var generation = 0

    init() { reloadConfiguration() }

    func reloadConfiguration() {
        generation += 1
        assets = []
        total = 0
        nextCursor = nil
        isLoading = false
        configuration = nil
        do { configuration = try KeepsSettings.load() }
        catch { lastError = String(reflecting: error) }
    }

    func refresh() async {
        generation += 1
        let requestGeneration = generation
        assets = []
        total = 0
        nextCursor = nil
        await fetchPage(cursor: nil, generation: requestGeneration)
    }

    func loadMore() async {
        guard !isLoading, let nextCursor else { return }
        await fetchPage(cursor: nextCursor, generation: generation)
    }

    private func fetchPage(cursor: String?, generation requestGeneration: Int) async {
        guard let configuration else { isLoading = false; return }
        isLoading = true
        lastError = nil
        var query = KeepsAssetQuery()
        query.q = search
        query.trashed = showingTrash
        query.sort = sort
        query.cursor = cursor
        do {
            let page = try await KeepsClient(configuration: configuration).assets(query: query)
            guard generation == requestGeneration else { return }
            let existing = Set(assets.map(\.id))
            assets.append(contentsOf: page.items.filter { !existing.contains($0.id) })
            total = page.total
            nextCursor = page.nextCursor
        } catch {
            guard generation == requestGeneration else { return }
            lastError = String(reflecting: error)
        }
        if generation == requestGeneration { isLoading = false }
    }

    func update(_ asset: KeepsAsset) {
        guard let index = assets.firstIndex(where: { $0.id == asset.id }) else { return }
        if asset.trashed != showingTrash { assets.remove(at: index); total = max(0, total - 1) }
        else { assets[index] = asset }
    }

    var canLoadMore: Bool { nextCursor != nil }
}
