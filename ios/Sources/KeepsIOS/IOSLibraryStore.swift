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
    private var displayedQuery: String?

    private var revisionTask: Task<Void, Never>?
    private var observedRevision: Int64?
    private var isActive = false

    func setActive(_ active: Bool) {
        isActive = active
        revisionTask?.cancel()
        revisionTask = nil
        guard active, let configuration else { return }
        let client = KeepsClient(configuration: configuration)
        revisionTask = Task {
            var pollingError: String?
            while !Task.isCancelled {
                do {
                    let revision = try await client.revision()
                    try Task.checkCancellation()
                    guard self.configuration == configuration else { return }
                    if let pollingError, lastError == pollingError { lastError = nil }
                    pollingError = nil
                    if observedRevision != revision {
                        await refresh()
                        try Task.checkCancellation()
                        guard self.configuration == configuration else { return }
                        if lastError == nil { observedRevision = revision }
                    }
                } catch {
                    if Task.isCancelled { return }
                    guard self.configuration == configuration else { return }
                    pollingError = String(reflecting: error)
                    lastError = pollingError
                }
                do { try await Task.sleep(for: .seconds(5)) }
                catch { return }
            }
        }
    }

    init() { reloadConfiguration() }

    func reloadConfiguration() {
        generation += 1
        assets = []
        total = 0
        nextCursor = nil
        isLoading = false
        configuration = nil
        lastError = nil
        do { configuration = try KeepsSettings.load() }
        catch { lastError = String(reflecting: error) }
        observedRevision = nil
        setActive(isActive)
    }

    func refresh() async {
        generation += 1
        let requestGeneration = generation
        let queryKey = "\(search)|\(showingTrash)|\(sort)"
        if displayedQuery != queryKey {
            assets = []
            total = 0
            nextCursor = nil
        }
        displayedQuery = queryKey
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
            if cursor == nil { assets = [] }
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
