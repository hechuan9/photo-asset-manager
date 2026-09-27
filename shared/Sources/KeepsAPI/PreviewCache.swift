import Foundation
import CryptoKit
import ImageIO
import OSLog

public actor PreviewCache {
    public static let shared = PreviewCache()
    public static let diskLimit = 5 * 1024 * 1024 * 1024
    private let directory: URL
    private let limit: Int
    private let session: URLSession
    private let images = NSCache<NSString, CGImage>()
    private struct Entry { var size: Int; var accessed: Date }
    private var entries: [String: Entry] = [:]
    private var indexed = false
    private var pending: [String: Task<(Data, String), Error>] = [:]

    public init(directory: URL = URL.cachesDirectory.appendingPathComponent("KeepsPreviews", isDirectory: true),
                diskLimit: Int = PreviewCache.diskLimit, session: URLSession? = nil) {
        self.directory = directory
        self.limit = diskLimit
        if let session { self.session = session }
        else {
            let config = URLSessionConfiguration.ephemeral
            config.urlCache = nil
            self.session = URLSession(configuration: config)
        }
        #if os(iOS)
        images.totalCostLimit = 64 * 1024 * 1024
        #else
        images.totalCostLimit = 256 * 1024 * 1024
        #endif
    }

    public nonisolated static func key(assetID: UUID, preview: KeepsPreview, configuration: KeepsConfiguration) -> String {
        // Length-delimited encoding prevents ambiguous namespace boundaries.
        let parts = [configuration.baseURL.absoluteString, configuration.libraryID, assetID.uuidString, preview.version]
        let encoded = parts.map { "\($0.utf8.count):\($0)" }.joined()
        return SHA256.hash(data: Data(encoded.utf8)).map { String(format: "%02x", $0) }.joined()
    }

    public func image(assetID: UUID, preview: KeepsPreview, configuration: KeepsConfiguration, maxPixelSize: Int) async throws -> CGImage {
        let key = Self.key(assetID: assetID, preview: preview, configuration: configuration)
        let pixels = max(1, maxPixelSize)
        let memoryKey = "\(key):\(pixels)" as NSString
        if let image = images.object(forKey: memoryKey) {
            try touch(key)
            return image
        }
        let (data, actualKey) = try await bytes(key: key, assetID: assetID, preview: preview, configuration: configuration)
        try Task.checkCancellation()
        let image = try Self.decode(data, pixels: pixels)
        images.setObject(image, forKey: "\(actualKey):\(pixels)" as NSString, cost: image.bytesPerRow * image.height)
        return image
    }

    private func bytes(key: String, assetID: UUID, preview: KeepsPreview, configuration: KeepsConfiguration) async throws -> (Data, String) {
        try prepare()
        if entries[key] != nil {
            let file = directory.appendingPathComponent(key)
            do {
                let data = try Data(contentsOf: file)
                guard Self.isImage(data) else {
                    try FileManager.default.removeItem(at: file)
                    entries[key] = nil
                    return try await download(key: key, assetID: assetID, preview: preview, configuration: configuration)
                }
                try touch(key)
                return (data, key)
            } catch let error as CocoaError where error.code == .fileReadNoSuchFile {
                entries[key] = nil
            }
        }
        return try await download(key: key, assetID: assetID, preview: preview, configuration: configuration)
    }

    private func download(key: String, assetID: UUID, preview: KeepsPreview, configuration: KeepsConfiguration) async throws -> (Data, String) {
        if let task = pending[key] { return try await task.value }
        let session = self.session
        let task = Task<(Data, String), Error> {
            var descriptor = preview
            var (data, response) = try await session.data(from: descriptor.downloadURL)
            if Self.isExpired(data: data, response: response) {
                descriptor = try await KeepsClient(configuration: configuration, session: session).refreshPreviewDescriptor(assetID: assetID)
                (data, response) = try await session.data(from: descriptor.downloadURL)
            }
            guard let http = response as? HTTPURLResponse else { throw KeepsAPIError.invalidResponse }
            guard (200..<300).contains(http.statusCode) else {
                throw KeepsAPIError.http(http.statusCode, String(decoding: data, as: UTF8.self))
            }
            guard Self.isImage(data) else { throw URLError(.cannotDecodeContentData) }
            let actualKey = Self.key(assetID: assetID, preview: descriptor, configuration: configuration)
            try self.store(data, key: actualKey)
            return (data, actualKey)
        }
        pending[key] = task
        defer { pending[key] = nil }
        return try await task.value
    }

    static func isExpired(data: Data, response: URLResponse) -> Bool {
        guard let http = response as? HTTPURLResponse, http.statusCode == 403,
              let body = try? JSONDecoder().decode(Failure.self, from: data) else { return false }
        return body.detail.code == "preview_token_expired"
    }
    private struct Failure: Decodable { var detail: Detail; struct Detail: Decodable { var code: String } }
    private static func isImage(_ data: Data) -> Bool {
        guard let source = CGImageSourceCreateWithData(data as CFData, nil) else { return false }
        return CGImageSourceGetCount(source) > 0 && CGImageSourceGetStatus(source) == .statusComplete
    }
    static func decode(_ data: Data, pixels: Int) throws -> CGImage {
        guard let source = CGImageSourceCreateWithData(data as CFData, [kCGImageSourceShouldCache: false] as CFDictionary),
              let image = CGImageSourceCreateThumbnailAtIndex(source, 0, [
                kCGImageSourceCreateThumbnailFromImageAlways: true,
                kCGImageSourceCreateThumbnailWithTransform: true,
                kCGImageSourceThumbnailMaxPixelSize: pixels,
                kCGImageSourceShouldCacheImmediately: true
              ] as CFDictionary) else { throw URLError(.cannotDecodeContentData) }
        return image
    }

    private func prepare() throws {
        guard !indexed || !FileManager.default.fileExists(atPath: directory.path) else { return }
        entries.removeAll()
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        for file in try FileManager.default.contentsOfDirectory(at: directory, includingPropertiesForKeys: [.fileSizeKey, .contentModificationDateKey, .isRegularFileKey]) {
            let name = file.lastPathComponent
            guard name.count == 64, name.allSatisfy({ $0.isHexDigit }) else { continue }
            let values = try file.resourceValues(forKeys: [.fileSizeKey, .contentModificationDateKey, .isRegularFileKey])
            guard values.isRegularFile == true else { continue }
            entries[name] = Entry(size: values.fileSize ?? 0, accessed: values.contentModificationDate ?? .distantPast)
        }
        indexed = true
        try trim()
    }
    private func touch(_ key: String) throws {
        guard var entry = entries[key] else { return }
        entry.accessed = Date()
        do { try FileManager.default.setAttributes([.modificationDate: entry.accessed], ofItemAtPath: directory.appendingPathComponent(key).path) }
        catch let error as CocoaError where error.code == .fileNoSuchFile || error.code == .fileReadNoSuchFile {
            entries[key] = nil
            return
        }
        entries[key] = entry
    }
    func store(_ data: Data, key: String, write: @Sendable (Data, URL) throws -> Void = { try $0.write(to: $1, options: .atomic) }) throws {
        try prepare()
        guard data.count <= limit else { return }
        try trim()
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        do { try write(data, directory.appendingPathComponent(key)) }
        catch let error as CocoaError where error.code == .fileWriteOutOfSpace {
            // Disk persistence is optional; a full device must still display downloaded previews.
            Logger(subsystem: "local.keeps", category: "preview-cache").error("Preview cache write failed: \(String(reflecting: error), privacy: .public)")
            return
        }
        entries[key] = Entry(size: data.count, accessed: Date())
        try trim()
    }
    private func trim() throws {
        var total = entries.values.reduce(0) { $0 + $1.size }
        let available = try directory.resourceValues(forKeys: [.volumeAvailableCapacityForImportantUsageKey]).volumeAvailableCapacityForImportantUsage
        let lowSpace = available.map { $0 < 1024 * 1024 * 1024 } ?? false
        guard total > limit || lowSpace else { return }
        let target = lowSpace ? 0 : limit * 4 / 5
        for (key, entry) in entries.sorted(by: { $0.value.accessed < $1.value.accessed }) {
            guard total > target else { break }
            do { try FileManager.default.removeItem(at: directory.appendingPathComponent(key)) }
            catch let error as CocoaError where error.code == .fileNoSuchFile { }
            entries[key] = nil
            total -= entry.size
        }
    }
}
