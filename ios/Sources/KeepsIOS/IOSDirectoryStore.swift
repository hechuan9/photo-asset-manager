import Combine
import Foundation
import KeepsAPI

@MainActor
final class IOSDirectoryStore: ObservableObject {
    struct Entry {
        var directories: [KeepsNavigationDirectory]?
        var loading = false
        var error: String?
    }

    @Published private var entries: [String?: Entry] = [:]
    private var configuration: KeepsConfiguration?
    private var generation = 0
    private var requests: [String?: Task<KeepsNavigation, Error>] = [:]
    private let fetch: (KeepsConfiguration, String?) async throws -> KeepsNavigation

    init(fetch: @escaping (KeepsConfiguration, String?) async throws -> KeepsNavigation = {
             try await KeepsClient(configuration: $0).navigation(path: $1)
         }) {
        self.fetch = fetch
    }

    func configure(_ configuration: KeepsConfiguration?) {
        guard self.configuration != configuration else { return }
        self.configuration = configuration
        generation += 1
        for request in requests.values { request.cancel() }
        requests = [:]
        entries = [:]
    }

    func state(for path: String?, configuration: KeepsConfiguration?) -> Entry {
        guard self.configuration == configuration else { return Entry(loading: true) }
        return entries[path] ?? Entry(loading: true)
    }

    func load(configuration: KeepsConfiguration?, path: String?, force: Bool = false) async {
        configure(configuration)
        guard let configuration else { return }
        var entry = entries[path] ?? Entry()
        guard !entry.loading else { return }
        if !force, entries[path] != nil { return }
        let requestGeneration = generation
        entry.loading = true
        entry.error = nil
        entries[path] = entry
        // Closing the directory panel must not cancel a shared cache fill.
        let request = Task { try await fetch(configuration, path) }
        requests[path] = request
        defer { if generation == requestGeneration { requests[path] = nil } }
        do {
            let result = try await request.value
            guard generation == requestGeneration else { return }
            entry.directories = result.directories
        } catch {
            guard generation == requestGeneration else { return }
            if !(error is CancellationError) { entry.error = String(reflecting: error) }
        }
        entry.loading = false
        entries[path] = entry
    }

}
