import Foundation
import CryptoKit
import Testing
@testable import KeepsAPI

@Suite(.serialized)
struct KeepsOfflineRebuildTests {
    private let configuration = KeepsConfiguration(baseURL: URL(string: "https://nas.invalid")!, libraryID: "main")
    private func tar(_ entries: [(String, Data)], type: UInt8 = 48) -> Data {
        var result = Data()
        for (name, body) in entries {
            var header = Data(repeating: 0, count: 512)
            func put(_ text: String, _ index: Int) { header.replaceSubrange(index..<(index + text.utf8.count), with: text.utf8) }
            put(name, 0); put(String(format: "%011o", body.count), 124)
            header[156] = type; put("ustar", 257); put("00", 263)
            for i in 148..<156 { header[i] = 32 }
            let sum = header.reduce(0) { $0 + Int($1) }
            put(String(format: "%06o", sum), 148); header[154] = 0; header[155] = 32
            result.append(header); result.append(body)
            result.append(Data(repeating: 0, count: (512 - body.count % 512) % 512))
        }
        result.append(Data(repeating: 0, count: 1024)); return result
    }
    private func fixture(_ root: URL) throws -> [(String, Data)] {
        let db = try KeepsLibraryDatabase(configuration: configuration, rootDirectory: root.appendingPathComponent("source"))
        try db.completeSync(revision: 42, isStable: true)
        let snapshot = root.appendingPathComponent("source.sqlite")
        try db.exportSnapshot(to: snapshot)
        let manifest = KeepsOfflineManifest(formatVersion: 1, libraryID: "main", revision: 42, assetCount: 0, thumbnailCount: 0, missingThumbnailCount: 0)
        return [("manifest.json", try JSONEncoder().encode(manifest)), ("catalog.sqlite", try Data(contentsOf: snapshot))]
    }
    @Test func singleTransferResumesOnlyAfterInterruptionAndReusesServerJob() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        let archive = tar(try fixture(root))
        OfflineBundleProtocol.state.configure(archive)
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [OfflineBundleProtocol.self]
        let session = URLSession(configuration: config)
        let service = KeepsOfflineRebuild(session: session)
        let destination = root.appendingPathComponent("destination")
        await #expect(throws: (any Error).self) {
            _ = try await service.run(configuration: configuration, databaseRoot: destination)
        }
        let result = try await service.run(configuration: configuration, databaseRoot: destination)
        #expect(result.revision == 42)
        #expect(result.browseThumbnailCount == nil)
        let requests = OfflineBundleProtocol.state.requests
        #expect(requests.filter { $0.httpMethod == "POST" }.count == 1)
        let downloads = requests.filter { $0.url!.path.hasSuffix("download") }
        #expect(downloads.count == 2)
        #expect(downloads.first?.value(forHTTPHeaderField: "Range") == nil)
        #expect(downloads.last?.value(forHTTPHeaderField: "Range") == "bytes=1024-")
        let reader = try KeepsLibraryDatabase(configuration: configuration, rootDirectory: destination)
        #expect(try reader.revision == 42)
    }

    @Test func installsCompleteSnapshotAndKeepsExistingReaderConnection() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        let entries = try fixture(root)
        let destination = root.appendingPathComponent("destination")
        let reader = try KeepsLibraryDatabase(configuration: configuration, rootDirectory: destination)
        try reader.completeSync(revision: 1, isStable: true)
        let archive = root.appendingPathComponent("test.tar")
        try tar(entries).write(to: archive)
        let service = KeepsOfflineRebuild()
        let result = try await service.unpack(archive, root: root, configuration: configuration, databaseRoot: destination, progress: { _ in })
        #expect(result.revision == 42)
        #expect(try reader.revision == 42)
    }
    @Test func rejectsMalformedArchivesWithoutPublishingDatabase() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        let entries = try fixture(root)
        let destination = root.appendingPathComponent("destination")
        let reader = try KeepsLibraryDatabase(configuration: configuration, rootDirectory: destination)
        try reader.completeSync(revision: 7, isStable: true)
        let archive = root.appendingPathComponent("test.tar")
        let service = KeepsOfflineRebuild()
        let invalid = [tar(entries + [("../escape", Data())]), tar(entries, type: 50), tar(entries + [entries[0]]), tar(entries).dropLast(600)]
        for bytes in invalid {
            try reader.completeSync(revision: 7, isStable: true)
            try bytes.write(to: archive)
            await #expect(throws: (any Error).self) {
                _ = try await service.unpack(archive, root: root, configuration: configuration, databaseRoot: destination, progress: { _ in })
            }
            #expect(try reader.revision == 7)
        }
        #expect(!FileManager.default.fileExists(atPath: root.deletingLastPathComponent().appendingPathComponent("escape").path))
    }
    @Test func optionalThumbnailFailureKeepsDatabaseAndExistingCache() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        let source = try KeepsLibraryDatabase(configuration: configuration, rootDirectory: root.appendingPathComponent("source"))
        let preview = KeepsPreview(downloadURL: URL(string: "https://nas.invalid/thumbnail")!, width: 1, height: 1, version: "v2")
        let asset = KeepsAsset(id: UUID(), captureTime: nil, cameraMake: "", cameraModel: "", lensModel: "", originalFilename: "test.jpg", contentFingerprint: "c", metadataFingerprint: "m", rating: 0, flagState: "none", colorLabel: nil, tags: [], createdAt: "2026-01-01", updatedAt: "2026-01-01", trashed: false, preview: nil, thumbnail: preview)
        try source.ingest([asset]); try source.completeSync(revision: 55, isStable: true)
        let snapshot = root.appendingPathComponent("snapshot.sqlite")
        try source.exportSnapshot(to: snapshot)
        let manifest = KeepsOfflineManifest(formatVersion: 1, libraryID: "main", revision: 55, assetCount: 1, thumbnailCount: 1, missingThumbnailCount: 0)
        let image = Data(base64Encoded: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aEl0AAAAASUVORK5CYII=")!
        let cache = PreviewCache(directory: root.appendingPathComponent("cache"), role: .thumbnail)
        let service = KeepsOfflineRebuild(cache: cache)
        let destination = root.appendingPathComponent("destination")
        let reader = try KeepsLibraryDatabase(configuration: configuration, rootDirectory: destination)
        try reader.completeSync(revision: 7, isStable: true)
        let archive = root.appendingPathComponent("test.tar")
        let prefix = [("manifest.json", try JSONEncoder().encode(manifest)), ("catalog.sqlite", try Data(contentsOf: snapshot))]
        try tar(prefix + [("thumbnails/\(asset.id.uuidString).image", Data("broken".utf8))]).write(to: archive)
        let existingFile = root.appendingPathComponent("existing.png")
        try image.write(to: existingFile)
        let existingKey = String(repeating: "a", count: 64)
        try await cache.importCachedFile(from: existingFile, key: existingKey)
        _ = try await service.unpack(archive, root: root, configuration: configuration, databaseRoot: destination, progress: { _ in })
        #expect(try reader.revision == 55)
        #expect(try reader.assets(query: .init()).items.first?.id == asset.id)
        #expect(try await cache.cachedFileURL(forKey: existingKey) != nil)
        let missingKey = PreviewCache.key(assetID: asset.id, preview: preview, configuration: configuration, role: .thumbnail)
        #expect(try await cache.cachedFileURL(forKey: missingKey) == nil)
        try tar(prefix + [("thumbnails/\(asset.id.uuidString).image", image)]).write(to: archive)
        let blockedCacheFile = root.appendingPathComponent("cache").appendingPathComponent(missingKey)
        try FileManager.default.createDirectory(at: blockedCacheFile, withIntermediateDirectories: true)
        _ = try await service.unpack(archive, root: root, configuration: configuration, databaseRoot: destination, progress: { _ in })
        #expect(try reader.revision == 55)
        #expect(try await cache.cachedFileURL(forKey: existingKey) != nil)
        #expect(try await cache.cachedFileURL(forKey: missingKey) == nil)
        try FileManager.default.removeItem(at: blockedCacheFile)
        let freshDestination = root.appendingPathComponent("paused-destination")
        let paused = Task {
            try await service.unpack(archive, root: root, configuration: configuration, databaseRoot: freshDestination) { progress in
                if progress.message == "导入离线图库" { withUnsafeCurrentTask { $0?.cancel() } }
            }
        }
        await #expect(throws: CancellationError.self) { _ = try await paused.value }
        #expect(try KeepsLibraryDatabase(configuration: configuration, rootDirectory: freshDestination).revision == nil)
        let cachedFile = try #require(try await cache.cachedFileURL(forKey: missingKey))
        let cachedDate = try cachedFile.resourceValues(forKeys: [.contentModificationDateKey]).contentModificationDate
        #expect(FileManager.default.fileExists(atPath: archive.path))
        _ = try await service.unpack(archive, root: root, configuration: configuration, databaseRoot: freshDestination, progress: { _ in })
        #expect(try KeepsLibraryDatabase(configuration: configuration, rootDirectory: freshDestination).revision == 55)
        #expect(try cachedFile.resourceValues(forKeys: [.contentModificationDateKey]).contentModificationDate == cachedDate)
        _ = try await service.unpack(archive, root: root, configuration: configuration, databaseRoot: destination, progress: { _ in })
        #expect(try reader.revision == 55)
        let key = PreviewCache.key(assetID: asset.id, preview: preview, configuration: configuration, role: .thumbnail)
        #expect(try await cache.cachedFileURL(forKey: key) != nil)
        #expect(try reader.assets(query: .init()).items.first?.id == asset.id)
    }

    @Test func browseBundleImportsBothRolesAndRejectsWrongCounts() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        let source = try KeepsLibraryDatabase(configuration: configuration, rootDirectory: root.appendingPathComponent("source"))
        let preview = KeepsPreview(downloadURL: URL(string: "https://nas.invalid/thumbnail")!, width: 1, height: 1, version: "v2")
        var asset = KeepsAsset(id: UUID(), captureTime: nil, cameraMake: "", cameraModel: "", lensModel: "", originalFilename: "test.jpg", contentFingerprint: "c", metadataFingerprint: "m", rating: 0, flagState: "none", colorLabel: nil, tags: [], createdAt: "2026-01-01", updatedAt: "2026-01-01", trashed: false, preview: nil, thumbnail: preview)
        asset.browseThumbnail = preview
        try source.ingest([asset]); try source.completeSync(revision: 55, isStable: true)
        let snapshot = root.appendingPathComponent("snapshot.sqlite")
        try source.exportSnapshot(to: snapshot)
        var manifest = KeepsOfflineManifest(formatVersion: 1, libraryID: "main", revision: 55, assetCount: 1, thumbnailCount: 1, missingThumbnailCount: 0, browseThumbnailCount: 1, missingBrowseThumbnailCount: 0)
        let image = Data(base64Encoded: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aEl0AAAAASUVORK5CYII=")!
        let previewCache = PreviewCache(directory: root.appendingPathComponent("preview"), role: .thumbnail)
        let browseCache = PreviewCache(directory: root.appendingPathComponent("browse"), role: .browse)
        let service = KeepsOfflineRebuild(cache: previewCache, browsingCache: browseCache)
        let archive = root.appendingPathComponent("bundle.tar")
        let entries = [("catalog.sqlite", try Data(contentsOf: snapshot)), ("thumbnails/\(asset.id.uuidString).image", image), ("browse-thumbnails/\(asset.id.uuidString).image", image)]
        try tar([("manifest.json", try JSONEncoder().encode(manifest))] + entries).write(to: archive)
        let result = try await service.unpack(archive, root: root, configuration: configuration, databaseRoot: root.appendingPathComponent("target"), progress: { _ in })
        #expect(result.browseThumbnailCount == 1)
        #expect(try await previewCache.cachedKeys().count == 1)
        #expect(try await browseCache.cachedKeys().count == 1)
        manifest.browseThumbnailCount = 0; manifest.missingBrowseThumbnailCount = 1
        try tar([("manifest.json", try JSONEncoder().encode(manifest))] + entries).write(to: archive)
        let rejected = root.appendingPathComponent("rejected")
        await #expect(throws: (any Error).self) {
            _ = try await service.unpack(archive, root: root, configuration: configuration, databaseRoot: rejected, progress: { _ in })
        }
        #expect(try KeepsLibraryDatabase(configuration: configuration, rootDirectory: rejected).revision == nil)
        try tar([("manifest.json", try JSONEncoder().encode(manifest))] + entries.dropLast()).write(to: archive)
        let missing = try await service.unpack(archive, root: root, configuration: configuration, databaseRoot: rejected, progress: { _ in })
        #expect(missing.missingBrowseThumbnailCount == 1)
    }

    @Test(.enabled(if: ProcessInfo.processInfo.environment["KEEPS_OFFLINE_BUNDLE_FIXTURE"] != nil))
    func importsRustGeneratedContractFixture() async throws {
        let archive = URL(fileURLWithPath: ProcessInfo.processInfo.environment["KEEPS_OFFLINE_BUNDLE_FIXTURE"]!)
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        let configuration = KeepsConfiguration(baseURL: URL(string: "https://fixture.invalid")!, libraryID: "photos")
        let cache = PreviewCache(directory: root.appendingPathComponent("cache"), role: .thumbnail)
        let browseCache = PreviewCache(directory: root.appendingPathComponent("browse-cache"), role: .browse)
        let result = try await KeepsOfflineRebuild(cache: cache, browsingCache: browseCache).unpack(archive, root: root, configuration: configuration, databaseRoot: root.appendingPathComponent("db"), progress: { _ in })
        #expect(result.assetCount == 3 && result.thumbnailCount == 1 && result.missingThumbnailCount == 2)
        #expect(result.browseThumbnailCount == 1 && result.missingBrowseThumbnailCount == 2)
        #expect(try await browseCache.cachedKeys().count == 1)
        let db = try KeepsLibraryDatabase(configuration: configuration, rootDirectory: root.appendingPathComponent("db"))
        #expect(try db.revision == 5)
        #expect(try db.assets(query: .init()).total == 1)
        var query = KeepsAssetQuery(); query.showHidden = true
        #expect(try db.assets(query: query).total == 2)
        query.trashed = true
        #expect(try db.assets(query: query).total == 1)
        #expect(try db.hiddenDirectories().count == 1)
        let navigation = try #require(try db.navigation())
        #expect(navigation.directories.count == 1)
        #expect(try db.navigation(path: navigation.directories[0].path) != nil)
        #expect(try await cache.cachedKeys().count == 1)
    }

    @Test func manifestMismatchRetainsPriorCatalog() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        var entries = try fixture(root)
        let wrong = KeepsOfflineManifest(formatVersion: 1, libraryID: "main", revision: 99, assetCount: 0, thumbnailCount: 0, missingThumbnailCount: 0)
        entries[0].1 = try JSONEncoder().encode(wrong)
        let destination = root.appendingPathComponent("destination")
        let reader = try KeepsLibraryDatabase(configuration: configuration, rootDirectory: destination)
        try reader.completeSync(revision: 7, isStable: true)
        let archive = root.appendingPathComponent("test.tar")
        try tar(entries).write(to: archive)
        await #expect(throws: (any Error).self) {
            _ = try await KeepsOfflineRebuild().unpack(archive, root: root, configuration: configuration, databaseRoot: destination, progress: { _ in })
        }
        #expect(try reader.revision == 7)
    }
}

private final class OfflineBundleProtocol: URLProtocol {
    private final class Delivery: @unchecked Sendable {
        let operation: () -> Void
        init(_ operation: @escaping () -> Void) { self.operation = operation }
    }
    final class State: @unchecked Sendable {
        let lock = NSLock()
        private var body = Data()
        private var recorded: [URLRequest] = []
        private var downloads = 0
        var requests: [URLRequest] { lock.lock(); defer { lock.unlock() }; return recorded }
        func configure(_ data: Data) { lock.lock(); defer { lock.unlock() }; body = data; recorded = []; downloads = 0 }
        func response(_ request: URLRequest) -> (Data, Int) {
            lock.lock(); defer { lock.unlock() }
            recorded.append(request)
            if request.url!.path.hasSuffix("download") { downloads += 1 }
            return (body, downloads)
        }
    }
    static let state = State()
    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func stopLoading() {}
    override func startLoading() {
        let (archive, downloadCount) = Self.state.response(request)
        let hash = SHA256.hash(data: archive).map { String(format: "%02x", $0) }.joined()
        if !request.url!.path.hasSuffix("download") {
            let job: [String: Any] = ["id": "00000000-0000-0000-0000-000000000001", "state": "ready", "phase": "archive", "completed": 1, "total": 1, "revision": 42, "byteCount": archive.count, "sha256": hash]
            let data = try! JSONSerialization.data(withJSONObject: job)
            client?.urlProtocol(self, didReceive: HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: ["Content-Length": String(data.count)])!, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: data); client?.urlProtocolDidFinishLoading(self)
            return
        }
        let offset = request.value(forHTTPHeaderField: "Range") == nil ? 0 : 1024
        let headers = ["Content-Length": String(archive.count - offset), "ETag": "\"\(hash)\"", "Content-Range": "bytes \(offset)-\(archive.count - 1)/\(archive.count)"]
        client?.urlProtocol(self, didReceive: HTTPURLResponse(url: request.url!, statusCode: offset == 0 ? 200 : 206, httpVersion: nil, headerFields: headers)!, cacheStoragePolicy: .notAllowed)
        if downloadCount == 1 {
            client?.urlProtocol(self, didLoad: archive.prefix(1024))
            let delivery = Delivery { self.client?.urlProtocol(self, didFailWithError: URLError(.networkConnectionLost)) }
            DispatchQueue.global().asyncAfter(deadline: .now() + 0.1) { delivery.operation() }
        } else {
            client?.urlProtocol(self, didLoad: archive.dropFirst(offset))
            client?.urlProtocolDidFinishLoading(self)
        }
    }
}
