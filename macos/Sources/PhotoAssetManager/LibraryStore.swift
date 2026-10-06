import Foundation
import KeepsAPI
import SwiftUI
import OSLog

@MainActor
final class LibraryStore: ObservableObject {
    @Published private(set) var hiddenDirectoryPaths: Set<String> = []
    @Published private(set) var isUpdatingHiddenDirectory = false
    @Published var hiddenDirectoryFilterEnabled = true {
        didSet {
            guard hiddenDirectoryFilterEnabled != oldValue else { return }
            preferences.set(hiddenDirectoryFilterEnabled, forKey: "keeps.hiddenDirectoryFilterEnabled")
            refresh()
        }
    }
    @Published private(set) var assets: [KeepsAsset] = []
    @Published var selectedIDs: Set<UUID> = []
    @Published var query = KeepsAssetQuery()
    @Published private(set) var counts: KeepsCounts?
    @Published private(set) var directories: [KeepsNavigationDirectory] = []
    @Published private(set) var expandedPaths: Set<String> = []
    @Published private(set) var directoryChildren: [String: [KeepsNavigationDirectory]] = [:]
    @Published private(set) var directoryErrors: [String: String] = [:]
    @Published private(set) var loadingDirectories: Set<String> = []
    @Published private(set) var navigationError: String?
    @Published private(set) var isLoadingNavigation = false
    @Published private(set) var navigationGeneration = 0
    @Published private(set) var total = 0
    @Published private(set) var nextCursor: String?
    @Published private(set) var isLoading = false
    @Published private(set) var isMutating = false
    @Published private(set) var isCheckingRevision = false
    var hasLoadedResults: Bool { displayedPage != nil }
    @Published private(set) var configuration: KeepsConfiguration?
    @Published var lastError: String?
    @Published private(set) var isCheckingConnection = false
    @Published private(set) var connectionMessage: String?
    @Published private(set) var connectionError: String?
    private(set) var client: KeepsClient?
    private var loadTask: Task<Void, Never>?
    private var loadGeneration = 0
    private var paginationVisible = false
    private var rootRequestGeneration = 0
    private static let navigationLogger = Logger(subsystem: "local.keeps", category: "navigation")
    private let preferences: UserDefaults
    private let session: URLSession
    private let persistConfiguration: (KeepsConfiguration) throws -> Void

    private var revisionTask: Task<Void, Never>?
    private var isActive = false
    private let assetCache = KeepsAssetCache()
    private var displayedQuery: KeepsAssetQuery?
    private var displayedPage: KeepsAssetPage?
    private var lastAssetFetch = Date.distantPast
    private var lastNavigationFetch = Date.distantPast
    private var lastCountsFetch = Date.distantPast
    private var countsShowHidden: Bool?

    func setActive(_ active: Bool) {
        isActive = active
        revisionTask?.cancel()
        revisionTask = nil
        guard active, configuration != nil else { return }
        revisionTask = Task {
            while !Task.isCancelled {
                if Date().timeIntervalSince(lastNavigationFetch) >= 300 { refreshNavigation(force: false) }
                if !isLoading { refresh() }
                do { try await Task.sleep(for: .seconds(5)) }
                catch { return }
            }
        }
    }

    init(
        configuration: KeepsConfiguration? = nil,
        session: URLSession = KeepsClient.apiSession,
        loadSavedSettings: Bool = true,
        preferences: UserDefaults = .standard,
        persistConfiguration: @escaping (KeepsConfiguration) throws -> Void = { value in
            try KeepsSettings.save(baseURLString: value.baseURL.absoluteString, libraryID: value.libraryID, accessCredential: value.accessCredential ?? "")
        }
    ) {
        self.preferences = preferences
        hiddenDirectoryFilterEnabled = preferences.object(forKey: "keeps.hiddenDirectoryFilterEnabled") as? Bool ?? true
        self.session = session
        self.persistConfiguration = persistConfiguration
        do {
            let settings = try configuration ?? (loadSavedSettings ? KeepsSettings.load() : nil)
            self.configuration = settings
            client = settings.map { KeepsClient(configuration: $0, session: session) }
        } catch { lastError = Self.describe(error) }
    }

    var selectedAsset: KeepsAsset? { return assets.first { selectedIDs.contains($0.id) } }

    var locationTitle: String {
        if let path = query.directory {
            let root = directories.filter { path == $0.path || path.hasPrefix($0.path + "/") }.max { $0.path.count < $1.path.count }
            if let root {
                let suffix = String(path.dropFirst(root.path.count)).split(separator: "/").joined(separator: " / ")
                return "来源 / " + root.name + (suffix.isEmpty ? "" : " / " + suffix)
            }
            let name = directoryChildren.values.lazy.flatMap { $0 }.first { $0.path == path }?.name
            return "来源 / " + (name ?? path)
        }
        return query.trashed ? "回收站" : query.flagState == "picked" ? "精选" : "全部照片"
    }

    func isDirectoryHidden(_ path: String) -> Bool {
        hiddenDirectoryPaths.contains { path == $0 || path.hasPrefix($0 + "/") }
    }

    func setDirectoryHidden(_ path: String, hidden: Bool) {
        guard let client, !isUpdatingHiddenDirectory else { return }
        let generation = navigationGeneration
        isUpdatingHiddenDirectory = true
        assetCache.removeAll()
        Task {
            defer { isUpdatingHiddenDirectory = false }
            do {
                let response = try await client.setDirectoryHidden(path: path, hidden: hidden)
                guard generation == navigationGeneration else { return }
                hiddenDirectoryPaths = Set(response.paths)
                refresh(force: true)
            } catch {
                if generation == navigationGeneration { lastError = Self.describe(error) }
            }
        }
    }

    func setDirectoryExpanded(_ path: String, expanded: Bool) {
        if expanded {
            expandedPaths.insert(path)
            if directoryErrors[path] == nil { loadChildren(of: path) }
        } else { expandedPaths.remove(path) }
    }

    func showLibrary(directory: String? = nil, trashed: Bool = false, picked: Bool = false) {
        query = KeepsAssetQuery()
        query.directory = directory
        query.recursive = true
        query.trashed = trashed
        query.flagState = picked ? "picked" : nil
        refresh()
    }

    private func clearResults() {
        loadTask?.cancel()
        loadGeneration += 1
        isLoading = false
        paginationVisible = false
        assets = []; selectedIDs = []; total = 0; nextCursor = nil; lastError = nil
    }

    func refreshNavigation(force: Bool = true) {
        guard let client else { return }
        if !force && navigationError == nil && Date().timeIntervalSince(lastNavigationFetch) < 300 { return }
        rootRequestGeneration += 1
        let requestGeneration = rootRequestGeneration
        let generation = navigationGeneration
        lastNavigationFetch = Date()
        isLoadingNavigation = true
        navigationError = nil
        Task {
            defer { if requestGeneration == rootRequestGeneration && generation == navigationGeneration { isLoadingNavigation = false } }
            do {
                let navigation = try await client.navigation()
                let hidden = try await client.hiddenDirectories()
                guard generation == navigationGeneration, requestGeneration == rootRequestGeneration else { return }
                removeMissingDirectories(old: directories, new: navigation.directories)
                hiddenDirectoryPaths = Set(hidden.paths)
                directories = navigation.directories
                for path in expandedPaths { loadChildren(of: path, refresh: true) }
            } catch {
                if generation == navigationGeneration && requestGeneration == rootRequestGeneration { navigationError = Self.describe(error) }
            }
        }
    }

    func loadChildren(of path: String, refresh: Bool = false) {
        guard let client, (refresh || directoryChildren[path] == nil), !loadingDirectories.contains(path) else { return }
        let generation = navigationGeneration
        loadingDirectories.insert(path)
        directoryErrors[path] = nil
        let started = ContinuousClock.now
        Self.navigationLogger.info("Directory request started")
        Task {
            defer { if generation == navigationGeneration { loadingDirectories.remove(path) } }
            do {
                let navigation = try await client.navigation(path: path)
                let elapsed = started.duration(to: .now).components
                let milliseconds = Double(elapsed.seconds) * 1_000 + Double(elapsed.attoseconds) / 1e15
                Self.navigationLogger.info("Directory request completed in \(milliseconds, privacy: .public) ms; children \(navigation.directories.count, privacy: .public)")
                guard generation == navigationGeneration else { return }
                guard directories.contains(where: { path == $0.path }) || directoryChildren.values.contains(where: { $0.contains(where: { $0.path == path }) }) else { return }
                removeMissingDirectories(old: directoryChildren[path] ?? [], new: navigation.directories)
                directoryChildren[path] = navigation.directories
            } catch {
                let elapsed = started.duration(to: .now).components
                let milliseconds = Double(elapsed.seconds) * 1_000 + Double(elapsed.attoseconds) / 1e15
                Self.navigationLogger.error("Directory request failed after \(milliseconds, privacy: .public) ms")
                if generation == navigationGeneration { directoryErrors[path] = Self.describe(error) }
            }
        }
    }

    private func removeMissingDirectories(old: [KeepsNavigationDirectory], new: [KeepsNavigationDirectory]) {
        let removed = Set(old.map(\.path)).subtracting(new.map(\.path))
        for path in removed {
            expandedPaths = expandedPaths.filter { $0 != path && !$0.hasPrefix(path + "/") }
            directoryChildren = directoryChildren.filter { $0.key != path && !$0.key.hasPrefix(path + "/") }
            directoryErrors = directoryErrors.filter { $0.key != path && !$0.key.hasPrefix(path + "/") }
            if let selected = query.directory, selected == path || selected.hasPrefix(path + "/") { showLibrary() }
        }
    }

    private func resetNavigation() {
        navigationGeneration += 1
        rootRequestGeneration += 1
        directories = []; directoryChildren = [:]; directoryErrors = [:]
        expandedPaths = []; loadingDirectories = []
        navigationError = nil
        hiddenDirectoryPaths = []
    }

    func clearConnectionFeedback() {
        connectionMessage = nil
        connectionError = nil
    }

    @discardableResult
    func checkConnection(baseURL: String, libraryID: String, accessCredential: String, save: Bool) async -> Bool {
        guard !isCheckingConnection else { return false }
        isCheckingConnection = true
        clearConnectionFeedback()
        defer { isCheckingConnection = false }
        do {
            let base = baseURL.trimmingCharacters(in: .whitespacesAndNewlines)
            let library = libraryID.trimmingCharacters(in: .whitespacesAndNewlines)
            guard let url = URL(string: base), ["http", "https"].contains(url.scheme?.lowercased() ?? ""),
                  url.host != nil, url.user == nil, url.password == nil, url.query == nil, url.fragment == nil,
                  !library.isEmpty else { throw KeepsAPIError.invalidConfiguration }
            let credential = accessCredential.trimmingCharacters(in: .whitespacesAndNewlines)
            let candidate = KeepsConfiguration(baseURL: url, libraryID: library, accessCredential: credential.isEmpty ? nil : credential)
            let candidateClient = KeepsClient(configuration: candidate, session: session)
            _ = try await candidateClient.counts()
            var probe = KeepsAssetQuery()
            probe.limit = 1
            _ = try await candidateClient.assets(query: probe)
            try Task.checkCancellation()
            if save {
                try persistConfiguration(candidate)
                configuration = candidate
                client = candidateClient
                clearResults()
                assetCache.removeAll()
                displayedQuery = nil
                displayedPage = nil
                lastCountsFetch = .distantPast
                lastNavigationFetch = .distantPast
                setActive(isActive)
                assets = []; selectedIDs = []; counts = nil
                resetNavigation()
                query = KeepsAssetQuery()
                refreshNavigation()
                refresh()
                connectionMessage = "已连接并保存 Keeps Server。"
            } else {
                connectionMessage = "连接成功，服务鉴权与资料库 API 均可用。"
            }
            return true
        } catch {
            connectionError = Self.describe(error)
            return false
        }
    }

    private var effectiveQuery: KeepsAssetQuery {
        var value = query
        value.cursor = nil
        value.showHidden = !hiddenDirectoryFilterEnabled
        return value
    }

    private func display(_ page: KeepsAssetPage) {
        displayedPage = page
        assets = page.items
        total = page.total
        nextCursor = page.nextCursor
        selectedIDs.formIntersection(Set(assets.map(\.id)))
    }

    func refresh(force: Bool = false) {
        guard let client else { isLoading = false; return }
        let requestedQuery = effectiveQuery
        let changedQuery = displayedQuery != requestedQuery
        if !changedQuery && isLoading && !force { return }
        loadTask?.cancel()
        loadGeneration += 1
        if changedQuery {
            clearResults()
            displayedQuery = requestedQuery
            displayedPage = nil
            if let cached = assetCache.page(for: requestedQuery) { display(cached) }
        }
        let requestGeneration = loadGeneration
        if force { assetCache.removeAll() }
        let targetCount = max(assets.count, 1)
        isLoading = true
        isCheckingRevision = !force && displayedPage != nil
        lastError = nil
        loadTask = Task {
            defer { finishLoading(generation: requestGeneration) }
            do {
                var needsFetch = force || displayedPage == nil
                if !force {
                    let revision = try await client.revision(path: requestedQuery.directory)
                    try Task.checkCancellation()
                    guard requestGeneration == loadGeneration else { return }
                    needsFetch = displayedPage.map { !$0.isCurrent(at: revision) } ?? true
                    if !changedQuery, let current = displayedPage, revision.isUpdating && current.isUpdating && revision.revision == current.revision
                        && Date().timeIntervalSince(lastAssetFetch) < 30 { needsFetch = false }
                }
                isCheckingRevision = false
                if needsFetch {
                    var nextQuery = requestedQuery
                    var combined = try await client.assets(query: nextQuery)
                    while combined.items.count < targetCount, let cursor = combined.nextCursor {
                        nextQuery.cursor = cursor
                        let next = try await client.assets(query: nextQuery)
                        guard next.revision == combined.revision else {
                            combined = try await client.assets(query: requestedQuery)
                            break
                        }
                        let known = Set(combined.items.map(\.id))
                        combined = KeepsAssetPage(items: combined.items + next.items.filter { !known.contains($0.id) }, total: next.total,
                            nextCursor: next.nextCursor, revision: next.revision, isUpdating: combined.isUpdating || next.isUpdating)
                    }
                    try Task.checkCancellation()
                    guard requestGeneration == loadGeneration else { return }
                    display(combined)
                    assetCache.store(combined, for: requestedQuery)
                    lastAssetFetch = Date()
                }
                // Counts describe the whole library, independently of the selected directory.
                if force || counts == nil || countsShowHidden != requestedQuery.showHidden || Date().timeIntervalSince(lastCountsFetch) >= 60 {
                    let summary = try await client.counts(showHidden: requestedQuery.showHidden)
                    try Task.checkCancellation()
                    guard requestGeneration == loadGeneration else { return }
                    counts = summary
                    countsShowHidden = requestedQuery.showHidden
                    lastCountsFetch = Date()
                }
            } catch {
                if !Task.isCancelled && requestGeneration == loadGeneration { lastError = Self.describe(error) }
            }
        }
    }

    func setPaginationVisible(_ visible: Bool) {
        paginationVisible = visible
        if visible { loadMoreIfNeeded() }
    }

    private func loadMoreIfNeeded() {
        guard paginationVisible, lastError == nil else { return }
        loadMore()
    }

    private func finishLoading(generation: Int) {
        guard generation == loadGeneration else { return }
        isLoading = false
        isCheckingRevision = false
        loadMoreIfNeeded()
    }

    func loadMore() {
        guard let client, let cursor = nextCursor, !isLoading, displayedQuery == effectiveQuery else { return }
        let generation = loadGeneration
        var requestedQuery = query
        requestedQuery.cursor = cursor
        requestedQuery.showHidden = !hiddenDirectoryFilterEnabled
        isLoading = true
        lastError = nil
        loadTask = Task {
            defer { finishLoading(generation: generation) }
            do {
                let page = try await client.assets(query: requestedQuery)
                try Task.checkCancellation()
                guard generation == loadGeneration else { return }
                guard let current = displayedPage, current.revision == page.revision else {
                    isLoading = false
                    refresh(force: true)
                    return
                }
                let known = Set(assets.map(\.id))
                let combined = KeepsAssetPage(items: assets + page.items.filter { !known.contains($0.id) },
                    total: page.total, nextCursor: page.nextCursor, revision: page.revision, isUpdating: current.isUpdating || page.isUpdating)
                display(combined)
                assetCache.store(combined, for: effectiveQuery)
            } catch {
                if !Task.isCancelled && generation == loadGeneration { lastError = Self.describe(error) }
            }
        }
    }

    func select(_ id: UUID, extending: Bool) {
        guard assets.contains(where: { $0.id == id }) else { return }
        if extending {
            if selectedIDs.contains(id) { selectedIDs.remove(id) } else { selectedIDs.insert(id) }
        } else { selectedIDs = [id] }
    }

    func selectAdjacent(_ offset: Int) {
        guard !assets.isEmpty else { return }
        let index = assets.firstIndex { selectedIDs.contains($0.id) } ?? 0
        selectedIDs = [assets[min(max(index + offset, 0), assets.count - 1)].id]
    }

    func updateSelected(_ patch: KeepsAssetPatch) {
        mutateSelected { client, id in try await client.updateAsset(id: id, patch: patch) }
    }

    func restoreSelected() { mutateSelected(refreshDirectories: true) { client, id in try await client.restoreAsset(id: id) } }

    private func mutateSelected(refreshDirectories: Bool = false, _ mutation: @escaping @Sendable (KeepsClient, UUID) async throws -> KeepsAsset) {
        guard let client, !isMutating, !selectedIDs.isEmpty else { return }
        let ids = selectedIDs
        let generation = loadGeneration
        isMutating = true
        assetCache.removeAll()
        lastError = nil
        Task {
            defer { isMutating = false; assetCache.removeAll() }
            var completed = 0
            do {
                for id in ids {
                    let updated = try await mutation(client, id)
                    if generation == loadGeneration, let index = assets.firstIndex(where: { $0.id == id }) { assets[index] = updated }
                    completed += 1
                }
                if generation == loadGeneration { refresh(force: true); if refreshDirectories { refreshNavigation() } }
            } catch { if generation == loadGeneration { lastError = "已完成 \(completed)/\(ids.count) 项。\n" + Self.describe(error) } }
        }
    }

    func refreshPreview(id: UUID) async throws -> URL {
        guard let client else { throw KeepsAPIError.invalidConfiguration }
        return try await client.refreshPreview(assetID: id)
    }

    static func describe(_ error: Error) -> String {
        String(reflecting: error) + "\n" + error.localizedDescription
    }
}
