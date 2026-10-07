import CryptoKit
import Foundation
import SQLite3

/// iCloud transports snapshots and immutable thumbnails; SQLite remains the active local catalog.
public actor KeepsCloudReplica {
    public static let shared = KeepsCloudReplica()
    private let cloudRoot: URL?
    private let cache: PreviewCache

    public init(cloudRoot: URL? = nil, cache: PreviewCache = .thumbnails) {
        self.cloudRoot = cloudRoot
        self.cache = cache
    }

    @discardableResult
    public func restore(configuration: KeepsConfiguration, databaseRoot: URL? = nil,
                        progress: (@Sendable (String) async -> Void)? = nil,
                        workProgress: (@Sendable (Int64, Int64) async -> Void)? = nil) async throws -> Bool {
        guard let root = replicaRoot(configuration) else {
            await progress?("iCloud 当前不可用，本地图库保留。")
            return false
        }
        try Task.checkCancellation()
        await progress?("正在查找 iCloud 图库副本…")
        let inventory = try await inventory(root)
        let snapshot = root.appendingPathComponent("catalog.snapshot")
        guard inventory.contains(snapshot) else { return false }
        let temporary = try temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: temporary) }
        await progress?("正在从 iCloud 恢复图库数据库…")
        try await makeAvailable(snapshot)
        let localSnapshot = temporary.appendingPathComponent("catalog.snapshot")
        try coordinatedRead(snapshot, to: localSnapshot)
        let database = try KeepsLibraryDatabase(configuration: configuration, rootDirectory: databaseRoot)
        let restored = try database.restoreSnapshot(from: localSnapshot)
        let work = ReplicaWorkProgress(total: try thumbnailWorkTotal(database), callback: workProgress)
        await work.advance(1, force: true)
        var processed = 0
        var cached = try await cache.cachedKeys()
        let clock = ContinuousClock()
        var lastProgress = clock.now
        try await forEachThumbnailPage(database: database, configuration: configuration) { keys, itemCount in
            let missing = keys.filter { !cached.contains($0) && inventory.contains(thumbnailURL($0, root: root)) }
            await work.advance(Int64(itemCount - missing.count))
            if cloudRoot == nil {
                for key in missing {
                    try Task.checkCancellation()
                    try FileManager.default.startDownloadingUbiquitousItem(at: thumbnailURL(key, root: root))
                }
            }
            for key in missing {
                try Task.checkCancellation()
                let source = thumbnailURL(key, root: root)
                try await makeAvailable(source, downloadRequested: true)
                let localFile = temporary.appendingPathComponent(key)
                try coordinatedRead(source, to: localFile)
                try await cache.importCachedFile(from: localFile, key: key)
                try FileManager.default.removeItem(at: localFile)
                cached.insert(key)
                processed += 1
                await work.advance(1)
                if lastProgress.duration(to: clock.now) >= .seconds(1) {
                    await progress?("已从 iCloud 恢复 \(processed) 张缩略图…")
                    lastProgress = clock.now
                }
            }
        }
        await work.report()
        return restored
    }

    public func backup(configuration: KeepsConfiguration, databaseRoot: URL? = nil,
                       progress: (@Sendable (String) async -> Void)? = nil,
                       workProgress: (@Sendable (Int64, Int64) async -> Void)? = nil) async throws {
        guard let root = replicaRoot(configuration) else {
            await progress?("iCloud 当前不可用，本地图库保留。")
            return
        }
        try Task.checkCancellation()
        let database = try KeepsLibraryDatabase(configuration: configuration, rootDirectory: databaseRoot)
        let temporary = try temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: temporary) }
        let snapshot = temporary.appendingPathComponent("catalog.snapshot")
        try database.exportSnapshot(to: snapshot)
        let stable = try KeepsLibraryDatabase(configuration: configuration,
                                             rootDirectory: temporary.appendingPathComponent("catalog"))
        guard try stable.restoreSnapshot(from: snapshot) else { return }
        await progress?("正在把图库数据库交给 iCloud 同步…")
        var existing = try await inventory(root)
        try Task.checkCancellation()
        let remoteSnapshot = root.appendingPathComponent("catalog.snapshot")
        let revision = try Self.snapshotRevision(snapshot)
        var stageCatalog = true
        if existing.contains(remoteSnapshot) {
            try await makeAvailable(remoteSnapshot)
            stageCatalog = try coordinatedRevision(remoteSnapshot) < revision
        }
        if stageCatalog {
            try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
            try coordinatedWrite(snapshot, to: remoteSnapshot, replacingWithRevision: revision)
        }
        let work = ReplicaWorkProgress(total: try thumbnailWorkTotal(stable), callback: workProgress)
        await work.advance(1, force: true)
        var staged = 0
        let clock = ContinuousClock()
        var lastProgress = clock.now
        try await forEachThumbnailPage(database: stable, configuration: configuration) { keys, itemCount in
            await work.advance(Int64(itemCount - keys.count))
            for key in keys {
                try Task.checkCancellation()
                let destination = thumbnailURL(key, root: root)
                guard !existing.contains(destination), let source = try await cache.cachedFileURL(forKey: key) else {
                    await work.advance(1)
                    continue
                }
                try FileManager.default.createDirectory(at: destination.deletingLastPathComponent(), withIntermediateDirectories: true)
                try coordinatedWrite(source, to: destination)
                existing.insert(destination)
                staged += 1
                await work.advance(1)
                if lastProgress.duration(to: clock.now) >= .seconds(1) {
                    await progress?("已把 \(staged) 张缩略图交给 iCloud 同步…")
                    lastProgress = clock.now
                }
            }
        }
        await work.report()
        await progress?("图库副本已交给 iCloud 同步，上传由系统继续完成。")
    }

    private func replicaRoot(_ configuration: KeepsConfiguration) -> URL? {
        let root = cloudRoot ?? FileManager.default.url(forUbiquityContainerIdentifier: "iCloud.com.hechuan.Keeps")?
            .appendingPathComponent("Data/Replicas", isDirectory: true)
        let identity = [configuration.baseURL.absoluteString, configuration.libraryID]
            .map { "\($0.utf8.count):\($0)" }.joined()
        let namespace = SHA256.hash(data: Data(identity.utf8)).map { String(format: "%02x", $0) }.joined()
        return root?.resolvingSymlinksInPath().appendingPathComponent(namespace, isDirectory: true)
    }

    private func thumbnailURL(_ key: String, root: URL) -> URL {
        root.appendingPathComponent("thumbnails/\(key.prefix(2))/\(key)")
    }

    private func inventory(_ root: URL) async throws -> Set<URL> {
        if cloudRoot != nil {
            guard let files = FileManager.default.enumerator(at: root, includingPropertiesForKeys: [.isRegularFileKey]) else { return [] }
            var result: Set<URL> = []
            while let url = files.nextObject() as? URL {
                try Task.checkCancellation()
                if try url.resourceValues(forKeys: [.isRegularFileKey]).isRegularFile == true { result.insert(url.resolvingSymlinksInPath()) }
            }
            return result
        }
        let discovered = try await CloudReplicaMetadata.urls(in: root)
        var result: Set<URL> = []
        for url in discovered {
            try Task.checkCancellation()
            let canonical = url.resolvingSymlinksInPath()
            if canonical.path.hasPrefix(root.path + "/") { result.insert(canonical) }
        }
        return result
    }

    private func makeAvailable(_ url: URL, downloadRequested: Bool = false) async throws {
        try Task.checkCancellation()
        guard cloudRoot == nil else { return }
        if !downloadRequested { try FileManager.default.startDownloadingUbiquitousItem(at: url) }
        let clock = ContinuousClock()
        let deadline = clock.now.advanced(by: .seconds(120))
        while true {
            try Task.checkCancellation()
            var refreshed = url
            refreshed.removeAllCachedResourceValues()
            let values = try refreshed.resourceValues(forKeys: [.ubiquitousItemDownloadingStatusKey, .ubiquitousItemDownloadingErrorKey])
            if let error = values.ubiquitousItemDownloadingError { throw error }
            if values.ubiquitousItemDownloadingStatus == .current { return }
            guard clock.now < deadline else { throw ReplicaError.downloadTimedOut(url) }
            try await Task.sleep(for: .milliseconds(250))
        }
    }

    private func thumbnailWorkTotal(_ database: KeepsLibraryDatabase) throws -> Int64 {
        var query = KeepsAssetQuery()
        query.showHidden = true
        query.limit = 1
        let active = try database.assets(query: query).total
        query.trashed = true
        return 1 + Int64(active) + Int64(try database.assets(query: query).total)
    }

    private func forEachThumbnailPage(database: KeepsLibraryDatabase, configuration: KeepsConfiguration,
                                  visit: ([String], Int) async throws -> Void) async throws {
        for trashed in [false, true] {
            var query = KeepsAssetQuery()
            query.showHidden = true
            query.trashed = trashed
            query.limit = 100
            var boundary: KeepsAsset?
            var total: Int?
            while true {
                try Task.checkCancellation()
                let page = try database.assets(query: query, olderThan: boundary, knownTotal: total)
                total = page.total
                let keys = page.items.compactMap { asset in
                    asset.thumbnail.map { PreviewCache.key(assetID: asset.id, preview: $0,
                                                            configuration: configuration, role: .thumbnail) }
                }
                try await visit(keys, page.items.count)
                guard page.nextCursor != nil, let last = page.items.last else { break }
                boundary = last
            }
        }
    }

    private func temporaryDirectory() throws -> URL {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("keeps-cloud-" + UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return directory
    }

    private func coordinatedRead(_ source: URL, to destination: URL) throws {
        var coordinationError: NSError?
        var copyError: Error?
        NSFileCoordinator().coordinate(readingItemAt: source, options: .withoutChanges, error: &coordinationError) { url in
            do { try FileManager.default.copyItem(at: url, to: destination) }
            catch { copyError = error }
        }
        if let error = coordinationError ?? copyError { throw error }
    }

    private func coordinatedWrite(_ source: URL, to destination: URL, replacingWithRevision revision: Int64? = nil) throws {
        var coordinationError: NSError?
        var copyError: Error?
        NSFileCoordinator().coordinate(writingItemAt: destination, options: .forReplacing, error: &coordinationError) { url in
            do {
                let exists = FileManager.default.fileExists(atPath: url.path)
                if exists {
                    guard let revision else { return }
                    // Recheck under the write coordination in case another device advanced the snapshot.
                    if try Self.snapshotRevision(url) >= revision { return }
                }
                let staged = url.deletingLastPathComponent().appendingPathComponent("." + UUID().uuidString + ".stage")
                defer { try? FileManager.default.removeItem(at: staged) }
                try FileManager.default.copyItem(at: source, to: staged)
                if exists { _ = try FileManager.default.replaceItemAt(url, withItemAt: staged) }
                else { try FileManager.default.moveItem(at: staged, to: url) }
            } catch { copyError = error }
        }
        if let error = coordinationError ?? copyError { throw error }
    }

    private func coordinatedRevision(_ source: URL) throws -> Int64 {
        var coordinationError: NSError?
        var result: Result<Int64, Error>?
        NSFileCoordinator().coordinate(readingItemAt: source, options: .withoutChanges, error: &coordinationError) { url in
            result = Result { try Self.snapshotRevision(url) }
        }
        if let coordinationError { throw coordinationError }
        guard let result else { throw ReplicaError.snapshotUnreadable(source) }
        return try result.get()
    }

    private static func snapshotRevision(_ file: URL) throws -> Int64 {
        var database: OpaquePointer?
        let opened = sqlite3_open_v2(file.path, &database, SQLITE_OPEN_READONLY, nil)
        defer { sqlite3_close(database) }
        func failure() -> KeepsLibraryDatabase.DatabaseError {
            .init(message: "Cloud snapshot revision at \(file.path): \(database.map { String(cString: sqlite3_errmsg($0)) } ?? "Cannot open SQLite")")
        }
        guard opened == SQLITE_OK else { throw failure() }
        var statement: OpaquePointer?
        let sql = "SELECT value FROM metadata WHERE key='revision' AND NOT EXISTS (SELECT 1 FROM metadata WHERE key='sync_checkpoint')"
        guard sqlite3_prepare_v2(database, sql, -1, &statement, nil) == SQLITE_OK else { throw failure() }
        defer { sqlite3_finalize(statement) }
        let result = sqlite3_step(statement)
        guard result == SQLITE_ROW else {
            if result != SQLITE_DONE { throw failure() }
            throw ReplicaError.snapshotUnreadable(file)
        }
        guard let bytes = sqlite3_column_text(statement, 0), let revision = Int64(String(cString: bytes)) else {
            throw ReplicaError.snapshotUnreadable(file)
        }
        return revision
    }

    private enum ReplicaError: Error {
        case downloadTimedOut(URL)
        case snapshotUnreadable(URL)
    }
}

/// Metadata queries need a running main run loop, but database and file work stay on the replica actor.
@MainActor
private final class CloudReplicaMetadata: NSObject {
    private let query = NSMetadataQuery()
    private var gathered = false

    static func urls(in root: URL) async throws -> Set<URL> {
        try await CloudReplicaMetadata().gather(root)
    }

    private func gather(_ root: URL) async throws -> Set<URL> {
        query.searchScopes = [NSMetadataQueryUbiquitousDataScope]
        query.predicate = NSPredicate(format: "%K CONTAINS %@", NSMetadataItemPathKey, "/" + root.lastPathComponent + "/")
        query.operationQueue = .main
        NotificationCenter.default.addObserver(self, selector: #selector(finished),
                                               name: .NSMetadataQueryDidFinishGathering, object: query)
        defer {
            query.stop()
            NotificationCenter.default.removeObserver(self)
        }
        guard query.start() else { throw MetadataError.unavailable }
        let clock = ContinuousClock()
        let deadline = clock.now.advanced(by: .seconds(30))
        while !gathered {
            try Task.checkCancellation()
            guard clock.now < deadline else { throw MetadataError.timedOut }
            try await Task.sleep(for: .milliseconds(100))
        }
        query.disableUpdates()
        var result: Set<URL> = []
        for index in 0..<query.resultCount {
            try Task.checkCancellation()
            if let item = query.result(at: index) as? NSMetadataItem,
               let url = item.value(forAttribute: NSMetadataItemURLKey) as? URL { result.insert(url) }
            if index.isMultiple(of: 256) { await Task.yield() }
        }
        return result
    }

    @objc private func finished() { gathered = true }
    private enum MetadataError: Error { case unavailable, timedOut }
}

/// Report only completed catalog checks and file work, never elapsed waiting time.
private actor ReplicaWorkProgress {
    let total: Int64
    let callback: (@Sendable (Int64, Int64) async -> Void)?
    private var completed: Int64 = 0
    private let clock = ContinuousClock()
    private var lastReport = ContinuousClock.now

    init(total: Int64, callback: (@Sendable (Int64, Int64) async -> Void)?) {
        self.total = total
        self.callback = callback
    }

    func advance(_ units: Int64, force: Bool = false) async {
        completed += units
        if force || lastReport.duration(to: clock.now) >= .seconds(1) { await report() }
    }

    func report() async {
        lastReport = clock.now
        await callback?(completed, total)
    }
}
