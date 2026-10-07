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
    private var database: KeepsLibraryDatabase?
    var synchronize: (() async -> Void)?

    func configure(_ configuration: KeepsConfiguration?, database: KeepsLibraryDatabase?) {
        guard self.configuration != configuration || self.database !== database else { return }
        self.configuration = configuration
        self.database = database
        entries = [:]
    }

    func state(for path: String?, configuration: KeepsConfiguration?) -> Entry {
        guard self.configuration == configuration else { return Entry() }
        if let entry = entries[path] { return entry }
        return read(path)
    }

    func load(configuration: KeepsConfiguration?, path: String?, force: Bool = false) async {
        guard self.configuration == configuration else { return }
        if force { await synchronize?() }
        entries[path] = read(path)
    }

    func reload() {
        for path in Array(entries.keys) { entries[path] = read(path) }
        objectWillChange.send()
    }

    private func read(_ path: String?) -> Entry {
        do { return Entry(directories: try database?.navigation(path: path)?.directories) }
        catch { return Entry(error: String(reflecting: error)) }
    }
}
