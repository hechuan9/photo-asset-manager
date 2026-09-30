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
    @Published var showingPicked = false
    @Published var directory: String?
    let sort = "capture_desc"
    var chronologicalAssets: [KeepsAsset] { assets.reversed() }
    private var nextCursor: String?
    private var generation = 0
    private var displayedQuery: KeepsAssetQuery?
    private var displayedPage: KeepsAssetPage?
    private let cache = KeepsAssetCache()
    private let session: URLSession
    private var requestTask: Task<Void, Never>?
    private var paginationNeedsRefresh = false

    init(configuration: KeepsConfiguration? = nil, session: URLSession = KeepsClient.apiSession,
         loadSettings: Bool = true) {
        self.session = session
        if let configuration { self.configuration = configuration }
        else if loadSettings { reloadConfiguration() }
    }

    func configure(_ configuration: KeepsConfiguration?) {
        guard self.configuration != configuration else { return }
        generation += 1
        requestTask?.cancel()
        requestTask = nil
        cache.removeAll()
        displayedQuery = nil
        displayedPage = nil
        assets = []
        total = 0
        nextCursor = nil
        paginationNeedsRefresh = false
        isLoading = false
        lastError = nil
        self.configuration = configuration
    }

    func reloadConfiguration() {
        do { configure(try KeepsSettings.load()) }
        catch { configure(nil); lastError = String(reflecting: error) }
    }

    private var query: KeepsAssetQuery {
        var query = KeepsAssetQuery()
        query.q = search
        query.trashed = showingTrash
        query.flagState = showingPicked ? "picked" : nil
        query.directory = directory
        query.sort = sort
        query.limit = 200
        return query
    }

    func refresh() async {
        await load(validateRevision: false)
    }

    func refreshFromBottom() async {
        await load(validateRevision: true)
    }

    private func load(validateRevision: Bool) async {
        guard let configuration else { return }
        let query = query
        if displayedQuery != query {
            generation += 1
            requestTask?.cancel()
            requestTask = nil
            displayedQuery = query
            displayedPage = cache.page(for: query)
            assets = displayedPage?.items ?? []
            total = displayedPage?.total ?? 0
            nextCursor = displayedPage?.nextCursor
            paginationNeedsRefresh = false
            lastError = nil
            isLoading = false
        } else if let requestTask {
            await requestTask.value
            return
        }
        guard validateRevision || displayedPage == nil else { return }
        let requestGeneration = generation
        let client = KeepsClient(configuration: configuration, session: session)
        isLoading = displayedPage == nil
        lastError = nil
        let task = Task {
            defer {
                if generation == requestGeneration {
                    isLoading = false
                    requestTask = nil
                }
            }
            do {
                if validateRevision, let displayedPage {
                    let revision = try await client.revision(path: query.directory)
                    try Task.checkCancellation()
                    guard generation == requestGeneration else { return }
                    if displayedPage.isCurrent(at: revision) { return }
                }
                isLoading = true
                try await fetchPages(client: client, query: query, cursor: nil, generation: requestGeneration)
            } catch {
                guard generation == requestGeneration, !Task.isCancelled else { return }
                lastError = String(reflecting: error)
            }
        }
        requestTask = task
        await task.value
    }

    func loadMore() async {
        guard !paginationNeedsRefresh, requestTask == nil, let nextCursor, let configuration, displayedQuery == query else { return }
        let requestGeneration = generation
        let query = query
        let client = KeepsClient(configuration: configuration, session: session)
        isLoading = true
        lastError = nil
        let task = Task {
            defer {
                if generation == requestGeneration { isLoading = false; requestTask = nil }
            }
            do { try await fetchPages(client: client, query: query, cursor: nextCursor, generation: requestGeneration) }
            catch {
                guard generation == requestGeneration, !Task.isCancelled else { return }
                lastError = String(reflecting: error)
            }
        }
        requestTask = task
        await task.value
    }

    private func fetchPages(client: KeepsClient, query: KeepsAssetQuery, cursor: String?, generation requestGeneration: Int) async throws {
        var request = query
        request.cursor = cursor
        var loaded = cursor == nil ? [] : assets
        let countToReload = cursor == nil ? assets.count : 0
        var previous = cursor == nil ? nil : displayedPage
        var isUpdating = previous?.isUpdating ?? false
        var page: KeepsAssetPage
        repeat {
            page = try await client.assets(query: request)
            try Task.checkCancellation()
            guard generation == requestGeneration else { return }
            // A moving catalog cannot safely combine offset pages from different snapshots.
            if let previous, previous.revision != page.revision {
                if cursor != nil {
                    paginationNeedsRefresh = true
                    lastError = "内容已变化，请从底部上拉刷新"
                    return
                }
                var first = query
                first.cursor = nil
                let fresh = try await client.assets(query: first)
                try Task.checkCancellation()
                guard generation == requestGeneration else { return }
                publish(fresh, query: query)
                return
            }
            var existing = Set(loaded.map(\.id))
            loaded.append(contentsOf: page.items.filter { existing.insert($0.id).inserted })
            isUpdating = isUpdating || page.isUpdating
            previous = page
            request.cursor = page.nextCursor
        } while cursor == nil && loaded.count < countToReload && page.nextCursor != nil
        publish(KeepsAssetPage(items: loaded, total: page.total, nextCursor: page.nextCursor,
                               revision: page.revision, isUpdating: isUpdating), query: query)
    }

    private func publish(_ page: KeepsAssetPage, query: KeepsAssetQuery) {
        displayedPage = page
        assets = page.items
        total = page.total
        nextCursor = page.nextCursor
        paginationNeedsRefresh = false
        cache.store(page, for: query)
    }

    func update(_ asset: KeepsAsset) {
        // An asset can appear under several directory aliases and filter combinations.
        cache.removeAll()
        generation += 1
        requestTask?.cancel()
        requestTask = nil
        isLoading = false
        if let index = assets.firstIndex(where: { $0.id == asset.id }) {
            if asset.trashed != showingTrash || (showingPicked && asset.flagState != "picked") {
                assets.remove(at: index); total = max(0, total - 1)
            } else { assets[index] = asset }
        }
        if let displayedQuery, let page = displayedPage {
            let updated = KeepsAssetPage(items: assets, total: total, nextCursor: nextCursor,
                                         revision: page.revision, isUpdating: true)
            displayedPage = updated
            cache.store(updated, for: displayedQuery)
        }
    }

    var canLoadMore: Bool { nextCursor != nil && !paginationNeedsRefresh }
    var hasLoadedResults: Bool { displayedPage != nil }
}
