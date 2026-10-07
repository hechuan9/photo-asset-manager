import Foundation
import Testing
import ImageIO
import UniformTypeIdentifiers
@testable import KeepsAPI

@Suite(.serialized)
struct PreviewCacheTests {
    let id = UUID()
    let configuration = KeepsConfiguration(baseURL: URL(string: "https://cache.invalid")!, libraryID: "main", accessCredential: "secret")
    var preview: KeepsPreview { KeepsPreview(downloadURL: URL(string: "https://cache.invalid/old")!, width: 32, height: 16, version: "v1") }

    private func fixture(limit: Int = 4096) throws -> (PreviewCache, URL, URLSession) {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [CacheProtocol.self]
        let session = URLSession(configuration: config)
        return (PreviewCache(directory: directory, diskLimit: limit, session: session), directory, session)
    }
    private func png() throws -> Data {
        let context = CGContext(data: nil, width: 32, height: 16, bitsPerComponent: 8, bytesPerRow: 128, space: CGColorSpaceCreateDeviceRGB(), bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue)!
        let data = NSMutableData()
        let destination = CGImageDestinationCreateWithData(data, UTType.png.identifier as CFString, 1, nil)!
        CGImageDestinationAddImage(destination, context.makeImage()!, nil)
        #expect(CGImageDestinationFinalize(destination))
        return data as Data
    }

    @Test func importedImageIsValidatedAndRetryReusesPersistedCopy() async throws {
        let (cache, directory, session) = try fixture()
        let source = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory); try? FileManager.default.removeItem(at: source); session.invalidateAndCancel() }
        let key = PreviewCache.key(assetID: id, preview: preview, configuration: configuration)
        try Data("not an image".utf8).write(to: source)
        await #expect(throws: URLError.self) { try await cache.importCachedFile(from: source, key: key) }
        #expect(try await cache.cachedFileURL(forKey: key) == nil)
        await #expect(throws: CocoaError.self) { try await cache.importCachedFile(from: source, key: "../outside") }
        let data = try png()
        try data.write(to: source)
        try await cache.importCachedFile(from: source, key: key)
        let file = try #require(try await cache.cachedFileURL(forKey: key))
        #expect(try Data(contentsOf: file) == data)
        try FileManager.default.removeItem(at: source)
        try await cache.importCachedFile(from: source, key: key)
        #expect(try Data(contentsOf: file) == data)
    }

    @Test func cacheInventoryReadsFreshRegularNonemptyFilesOnly() async throws {
        let (cache, directory, session) = try fixture()
        defer { try? FileManager.default.removeItem(at: directory); session.invalidateAndCancel() }
        let key = PreviewCache.key(assetID: id, preview: preview, configuration: configuration)
        try await cache.store(png(), key: key)
        let emptyKey = String(repeating: "e", count: 64)
        let directoryKey = String(repeating: "d", count: 64)
        try Data().write(to: directory.appendingPathComponent(emptyKey))
        try FileManager.default.createDirectory(at: directory.appendingPathComponent(directoryKey), withIntermediateDirectories: true)
        try Data([1]).write(to: directory.appendingPathComponent("unfinished"))
        #expect(try await cache.cachedKeys() == [key])
        #expect(try await cache.containsCachedKey(key))
        try FileManager.default.removeItem(at: directory.appendingPathComponent(key))
        #expect(try await cache.cachedKeys().isEmpty)
        #expect(try await cache.containsCachedKey(key) == false)
    }

    @Test func mediaRolesHaveIndependentIdentityAndBudgets() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        let small = PreviewCache(directory: root.appendingPathComponent("thumbnail"), diskLimit: 4096, role: .thumbnail)
        let standard = PreviewCache(directory: root.appendingPathComponent("standard"), diskLimit: 1, role: .standard)
        let smallKey = PreviewCache.key(assetID: id, preview: preview, configuration: configuration, role: .thumbnail)
        let largeKey = PreviewCache.key(assetID: id, preview: preview, configuration: configuration, role: .standard)
        #expect(smallKey != largeKey)
        try await small.store(Data([1, 2]), key: smallKey)
        try await standard.store(Data([1, 2]), key: largeKey)
        #expect(FileManager.default.fileExists(atPath: root.appendingPathComponent("thumbnail/" + smallKey).path))
        #expect(!FileManager.default.fileExists(atPath: root.appendingPathComponent("standard/" + largeKey).path))
    }

    @Test func legacyMigrationOnlyRemovesDedicatedCacheOnce() throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        let legacy = root.appendingPathComponent("KeepsPreviews")
        try FileManager.default.createDirectory(at: legacy, withIntermediateDirectories: true)
        let photo = root.appendingPathComponent("original.jpg")
        try Data([1, 2, 3]).write(to: photo)
        try PreviewCache.migrateLegacyCache(in: root)
        #expect(!FileManager.default.fileExists(atPath: legacy.path))
        #expect(try Data(contentsOf: photo) == Data([1, 2, 3]))
        try FileManager.default.createDirectory(at: legacy, withIntermediateDirectories: true)
        try PreviewCache.migrateLegacyCache(in: root)
        #expect(FileManager.default.fileExists(atPath: legacy.path))
    }

    @Test func fullDiskSkipsPersistenceButOtherWriteFailuresRemainVisible() async throws {
        let (cache, directory, session) = try fixture()
        defer { try? FileManager.default.removeItem(at: directory); session.invalidateAndCancel() }
        let key = PreviewCache.key(assetID: id, preview: preview, configuration: configuration)
        try await cache.store(Data([1]), key: key, write: { _, _ in throw CocoaError(.fileWriteOutOfSpace) })
        #expect(try FileManager.default.contentsOfDirectory(atPath: directory.path).isEmpty)
        await #expect(throws: CocoaError.self) {
            try await cache.store(Data([1]), key: key, write: { _, _ in throw CocoaError(.fileWriteNoPermission) })
        }
        try await cache.store(Data([1]), key: key)
        #expect(try FileManager.default.contentsOfDirectory(atPath: directory.path) == [key])
    }

    @Test func stableIdentitySeparatesVersionLibraryAndServer() {
        let key = PreviewCache.key(assetID: id, preview: preview, configuration: configuration)
        var other = preview
        other.downloadURL = URL(string: "https://cache.invalid/new-signature")!
        #expect(key == PreviewCache.key(assetID: id, preview: other, configuration: configuration))
        other.version = "v2"
        #expect(key != PreviewCache.key(assetID: id, preview: other, configuration: configuration))
        var config = configuration
        config.libraryID = "different"
        #expect(key != PreviewCache.key(assetID: id, preview: preview, configuration: config))
        config = configuration
        config.baseURL = URL(string: "https://other.invalid")!
        #expect(key != PreviewCache.key(assetID: id, preview: preview, configuration: config))
    }

    @Test func coalescesDownloadsAndReusesDiskAfterRestartAndURLChange() async throws {
        let (cache, directory, session) = try fixture()
        defer { try? FileManager.default.removeItem(at: directory); session.invalidateAndCancel() }
        let data = try png()
        CacheProtocol.reset { _ in (200, data) }
        async let first = cache.image(assetID: id, preview: preview, configuration: configuration, maxPixelSize: 8)
        async let second = cache.image(assetID: id, preview: preview, configuration: configuration, maxPixelSize: 16)
        let (small, large) = try await (first, second)
        #expect(small.width == 8 && large.width == 16)
        #expect(CacheProtocol.count == 1)
        let restarted = PreviewCache(directory: directory, diskLimit: 4096, session: session)
        var changedURL = preview
        changedURL.downloadURL = URL(string: "https://cache.invalid/reissued")!
        _ = try await restarted.image(assetID: id, preview: changedURL, configuration: configuration, maxPixelSize: 8)
        #expect(CacheProtocol.count == 1)
        changedURL.version = "v2"
        _ = try await restarted.image(assetID: id, preview: changedURL, configuration: configuration, maxPixelSize: 8)
        #expect(CacheProtocol.count == 2)
    }

    @Test func expiredLinkRefreshesOnceAndStoresUnderFreshVersion() async throws {
        let (cache, directory, session) = try fixture()
        defer { try? FileManager.default.removeItem(at: directory); session.invalidateAndCancel() }
        let data = try png()
        CacheProtocol.reset { request in
            if request.url!.path == "/old" { return (403, Data(#"{"detail":{"code":"preview_token_expired"}}"#.utf8)) }
            if request.url!.path.hasPrefix("/derivatives/") {
                #expect(request.value(forHTTPHeaderField: "Authorization") == "Bearer secret")
                return (200, Data(#"{"downloadURL":"https://cache.invalid/new","width":32,"height":16,"version":"v2"}"#.utf8))
            }
            return (200, data)
        }
        #expect(try await cache.prefetch(assetID: id, preview: preview, configuration: configuration))
        #expect(CacheProtocol.count == 3)
        var fresh = preview; fresh.version = "v2"
        let oldKey = PreviewCache.key(assetID: id, preview: preview, configuration: configuration)
        let newKey = PreviewCache.key(assetID: id, preview: fresh, configuration: configuration)
        #expect(!FileManager.default.fileExists(atPath: directory.appendingPathComponent(oldKey).path))
        #expect(FileManager.default.fileExists(atPath: directory.appendingPathComponent(newKey).path))
        _ = try await cache.image(assetID: id, preview: fresh, configuration: configuration, maxPixelSize: 8)
        #expect(CacheProtocol.count == 3)
    }

    @Test func thumbnailExpiryRefreshesItsRoleAndSurvivesRestart() async throws {
        let (_, directory, session) = try fixture()
        defer { try? FileManager.default.removeItem(at: directory); session.invalidateAndCancel() }
        let cache = PreviewCache(directory: directory, session: session, role: .thumbnail)
        let data = try png()
        CacheProtocol.reset { request in
            if request.url!.path == "/old" { return (403, Data(#"{"detail":{"code":"preview_token_expired"}}"#.utf8)) }
            if request.url!.path.hasPrefix("/derivatives/") {
                let query = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)!.queryItems!
                #expect(query.contains(URLQueryItem(name: "role", value: "thumbnail")))
                return (200, Data(#"{"downloadURL":"https://cache.invalid/new","width":32,"height":16,"version":"v1"}"#.utf8))
            }
            return (200, data)
        }
        _ = try await cache.image(assetID: id, preview: preview, configuration: configuration, maxPixelSize: 8)
        let restarted = PreviewCache(directory: directory, session: session, role: .thumbnail)
        _ = try await restarted.image(assetID: id, preview: preview, configuration: configuration, maxPixelSize: 8)
        #expect(CacheProtocol.count == 3)
    }

    @Test func prefetchOnlySucceedsWhenDownloadedBytesRemainOnDisk() async throws {
        let (cache, directory, session) = try fixture(limit: 1)
        defer { try? FileManager.default.removeItem(at: directory); session.invalidateAndCancel() }
        let data = try png()
        CacheProtocol.reset { _ in (200, data) }
        #expect(try await cache.prefetch(assetID: id, preview: preview, configuration: configuration) == false)
        #expect(CacheProtocol.count == 1)
        #expect(try FileManager.default.contentsOfDirectory(atPath: directory.path).isEmpty)
    }

    @Test func retriesNeitherInvalidTokenNorEndlessExpiry() async throws {
        let (cache, directory, session) = try fixture()
        defer { try? FileManager.default.removeItem(at: directory); session.invalidateAndCancel() }
        CacheProtocol.reset { _ in (400, Data(#"{"detail":{"code":"invalid_derivative_storage_token"}}"#.utf8)) }
        await #expect(throws: KeepsAPIError.self) {
            _ = try await cache.image(assetID: id, preview: preview, configuration: configuration, maxPixelSize: 8)
        }
        #expect(CacheProtocol.count == 1)
        CacheProtocol.reset { request in
            if request.url!.path.hasPrefix("/derivatives/") {
                return (200, Data(#"{"downloadURL":"https://cache.invalid/old","width":32,"height":16,"version":"v1"}"#.utf8))
            }
            return (403, Data(#"{"detail":{"code":"preview_token_expired"}}"#.utf8))
        }
        await #expect(throws: KeepsAPIError.self) {
            _ = try await cache.image(assetID: id, preview: preview, configuration: configuration, maxPixelSize: 8)
        }
        #expect(CacheProtocol.count == 3)
    }

    @Test func startupDiskHitDefersEvictionAndPreservesReadRecency() async throws {
        let data = try png()
        let (_, directory, session) = try fixture()
        defer { try? FileManager.default.removeItem(at: directory); session.invalidateAndCancel() }
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let ids = (0..<4).map { _ in UUID() }
        let keys = ids.map { PreviewCache.key(assetID: $0, preview: preview, configuration: configuration) }
        for (index, key) in keys.prefix(3).enumerated() {
            let file = directory.appendingPathComponent(key)
            try data.write(to: file)
            try FileManager.default.setAttributes([.modificationDate: Date(timeIntervalSince1970: Double(index))], ofItemAtPath: file.path)
        }
        CacheProtocol.reset { _ in (200, data) }
        let restarted = PreviewCache(directory: directory, diskLimit: data.count * 2, session: session)
        _ = try await restarted.image(assetID: ids[0], preview: preview, configuration: configuration, maxPixelSize: 8)
        #expect(CacheProtocol.count == 0)
        // An over-budget cache must still render the first requested image before maintenance.
        #expect(try FileManager.default.contentsOfDirectory(atPath: directory.path).count == 3)
        _ = try await restarted.image(assetID: ids[3], preview: preview, configuration: configuration, maxPixelSize: 8)
        let remaining = try FileManager.default.contentsOfDirectory(atPath: directory.path)
        #expect(Set(remaining) == Set([keys[0], keys[3]]))
        #expect(CacheProtocol.count == 1)
    }

    @Test func lruUsesReadsAndSurvivesRestart() async throws {
        let data = try png()
        let (cache, directory, session) = try fixture(limit: data.count * 3)
        defer { try? FileManager.default.removeItem(at: directory); session.invalidateAndCancel() }
        CacheProtocol.reset { _ in (200, data) }
        let ids = (0..<4).map { _ in UUID() }
        for asset in ids.prefix(3) {
            _ = try await cache.image(assetID: asset, preview: preview, configuration: configuration, maxPixelSize: 8)
        }
        _ = try await cache.image(assetID: ids[0], preview: preview, configuration: configuration, maxPixelSize: 8)
        let restarted = PreviewCache(directory: directory, diskLimit: data.count * 3, session: session)
        _ = try await restarted.image(assetID: ids[3], preview: preview, configuration: configuration, maxPixelSize: 8)
        let files = try FileManager.default.contentsOfDirectory(atPath: directory.path)
        #expect(files.count == 2)
        for index in [0, 3] { #expect(files.contains(PreviewCache.key(assetID: ids[index], preview: preview, configuration: configuration))) }
    }

    @Test func corruptedOrPurgedDiskEntryDownloadsAgain() async throws {
        let data = try png()
        let (cache, directory, session) = try fixture()
        defer { try? FileManager.default.removeItem(at: directory); session.invalidateAndCancel() }
        CacheProtocol.reset { _ in (200, data) }
        _ = try await cache.image(assetID: id, preview: preview, configuration: configuration, maxPixelSize: 8)
        let file = directory.appendingPathComponent(PreviewCache.key(assetID: id, preview: preview, configuration: configuration))
        try Data("corrupt".utf8).write(to: file)
        _ = try await cache.image(assetID: id, preview: preview, configuration: configuration, maxPixelSize: 16)
        try FileManager.default.removeItem(at: file)
        _ = try await cache.image(assetID: id, preview: preview, configuration: configuration, maxPixelSize: 32)
        #expect(CacheProtocol.count == 3)
        try FileManager.default.removeItem(at: directory)
        _ = try await cache.image(assetID: UUID(), preview: preview, configuration: configuration, maxPixelSize: 8)
        #expect(CacheProtocol.count == 4)
    }
}

private final class CacheProtocol: URLProtocol {
    private static let lock = NSLock()
    nonisolated(unsafe) private static var handler: ((URLRequest) -> (Int, Data))?
    nonisolated(unsafe) private static var requests = 0
    static var count: Int { lock.lock(); defer { lock.unlock() }; return requests }
    static func reset(_ block: @escaping (URLRequest) -> (Int, Data)) {
        lock.lock(); defer { lock.unlock() }; handler = block; requests = 0
    }
    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func startLoading() {
        Self.lock.lock(); Self.requests += 1; let block = Self.handler!; Self.lock.unlock()
        let (status, data) = block(request)
        client?.urlProtocol(self, didReceive: HTTPURLResponse(url: request.url!, statusCode: status, httpVersion: nil, headerFields: nil)!, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: data)
        client?.urlProtocolDidFinishLoading(self)
    }
    override func stopLoading() {}
}
