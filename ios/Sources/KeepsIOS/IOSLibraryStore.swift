import Foundation
import SwiftUI
import KeepsAPI

@MainActor
final class IOSLibraryStore: ObservableObject {
    @Published private(set) var assets: [KeepsAsset] = []
    @Published private(set) var total = 0
    @Published private(set) var layoutRevision = 0
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

    init(configuration: KeepsConfiguration? = nil, session: URLSession = KeepsClient.apiSession,
         loadSettings: Bool = true) {
        self.session = session
        if let configuration { self.configuration = configuration }
        else if loadSettings { reloadConfiguration() }
    }

    func configure(_ configuration: KeepsConfiguration?) {
        guard self.configuration != configuration else { return }
        generation += 1
        layoutRevision += 1
        requestTask?.cancel()
        requestTask = nil
        cache.removeAll()
        displayedQuery = nil
        displayedPage = nil
        assets = []
        total = 0
        nextCursor = nil
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
        let currentGeneration = generation
        await load(validateRevision: true)
        if generation == currentGeneration { layoutRevision += 1 }
    }

    private func load(validateRevision: Bool) async {
        guard let configuration else { return }
        let query = query
        if displayedQuery != query {
            generation += 1
            requestTask?.cancel()
            requestTask = nil
            layoutRevision += 1
            displayedQuery = query
            displayedPage = cache.page(for: query)
            assets = displayedPage?.items ?? []
            total = displayedPage?.total ?? 0
            nextCursor = displayedPage?.nextCursor
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
        guard requestTask == nil, let nextCursor, let configuration, displayedQuery == query else { return }
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
        var countToReload = cursor == nil ? assets.count : 0
        var previous = cursor == nil ? nil : displayedPage
        var isUpdating = previous?.isUpdating ?? false
        var rebuilding = cursor == nil
        var page: KeepsAssetPage
        while true {
            page = try await client.assets(query: request)
            try Task.checkCancellation()
            guard generation == requestGeneration else { return }
            if let prior = previous, prior.revision != page.revision || prior.isUpdating || page.isUpdating {
                if !rebuilding {
                    // Re-read the loaded window before using a cursor from the new catalog.
                    countToReload = assets.count + query.limit + max(0, page.total - total)
                    rebuilding = true
                    loaded = []
                    request.cursor = nil
                    previous = nil
                    isUpdating = false
                    continue
                }
                // A changing catalog remains browsable but must not become a stable cache.
                isUpdating = true
            }
            var positions = Dictionary(uniqueKeysWithValues: loaded.enumerated().map { ($0.element.id, $0.offset) })
            for asset in page.items {
                if let index = positions[asset.id] { loaded[index] = asset }
                else {
                    positions[asset.id] = loaded.count
                    loaded.append(asset)
                }
            }
            isUpdating = isUpdating || page.isUpdating
            previous = page
            request.cursor = page.nextCursor
            if !rebuilding || loaded.count >= countToReload || request.cursor == nil { break }
        }
        publish(KeepsAssetPage(items: loaded, total: page.total, nextCursor: page.nextCursor,
                               revision: page.revision, isUpdating: isUpdating), query: query)
    }

    private func publish(_ page: KeepsAssetPage, query: KeepsAssetQuery) {
        displayedPage = page
        assets = page.items
        total = page.total
        nextCursor = page.nextCursor
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

    var canLoadMore: Bool { nextCursor != nil }
    var hasLoadedResults: Bool { displayedPage != nil }
}
