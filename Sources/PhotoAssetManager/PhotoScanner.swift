import AppKit
import Foundation
import ImageIO
import UniformTypeIdentifiers

private let skippedDirectoryNames: Set<String> = ["#recycle", ".spotlight-v100", ".trashes", ".fseventsd"]

struct ScannedFile {
    var url: URL
    var deviceID: String
    var storageKind: StorageKind
    var fileRole: FileRole
    var authorityRole: AuthorityRole
    var syncStatus: SyncStatus
    var sizeBytes: Int64
    var contentHash: String
    var metadataFingerprint: String
    var captureTime: Date?
    var cameraMake: String
    var cameraModel: String
    var lensModel: String
    var rating: Int
    var thumbnailURL: URL?
    var thumbnailHash: String?
    var thumbnailSize: Int64
}

struct PhotoScanner: @unchecked Sendable {
    init() {}

    func scanDirectory(_ root: URL, storageKind: StorageKind, derivativeRoot: URL?, database: SQLiteDatabase, progress: @escaping @MainActor (ScanReport) -> Void) async -> ScanReport {
        var report = ScanReport()
        report.phase = "准备扫描"
        let batchID: UUID
        do {
            batchID = try await MainActor.run {
                try database.createImportBatch(sourcePath: root.path, deviceID: currentDeviceID())
            }
        } catch {
            report.errors.append(error.fullTrace)
            return report
        }

        do {
            report.phase = "统计候选文件"
            await MainActor.run { progress(report) }
            report.totalFiles = try await countCandidateFiles(root: root, report: report, progress: progress)
            report.discoveredFiles = report.totalFiles
            report.phase = "扫描并入库"
            report.currentPath = ""
            await MainActor.run { progress(report) }

            guard let enumerator = makeEnumerator(root: root) else {
                throw CocoaError(.fileReadUnknown)
            }
            while let url = enumerator.nextObject() as? URL {
                do {
                    if shouldSkipDirectory(url) {
                        enumerator.skipDescendants()
                        continue
                    }
                    guard try isRegularFile(url) else { continue }
                    guard SupportedFiles.isPhoto(url) else {
                        report.skippedFiles += 1
                        continue
                    }
                    let values = try url.resourceValues(forKeys: [.fileSizeKey])
                    let size = Int64(values.fileSize ?? 0)
                    let alreadyIndexed = try await MainActor.run {
                        try database.hasUnchangedFileInstance(path: url.path, sizeBytes: size)
                    }
                    if alreadyIndexed {
                        let rating = ImageMetadata.read(url: url).rating
                        try await MainActor.run {
                            try database.applyScannedRatingIfEmpty(path: url.path, rating: rating)
                        }
                        report.skippedExistingFiles += 1
                        report.scannedFiles += 1
                        if report.scannedFiles % 25 == 0 {
                            await MainActor.run { progress(report) }
                        }
                        continue
                    }
                    report.currentPath = url.path
                    await MainActor.run { progress(report) }
                    guard let scanned = try scanFile(url, storageKind: storageKind, derivativeRoot: derivativeRoot) else {
                        report.skippedFiles += 1
                        continue
                    }
                    let inserted = try await MainActor.run {
                        try database.upsertScannedFile(scanned, batchID: batchID)
                    }
                    report.scannedFiles += 1
                    if inserted {
                        report.importedAssets += 1
                    } else {
                        report.newLocations += 1
                    }
                    if report.scannedFiles % 3 == 0 {
                        await MainActor.run { progress(report) }
                    }
                } catch {
                    report.errors.append("\(url.path)\n\(error.fullTrace)")
                }
            }
            try await MainActor.run {
                try database.finishImportBatch(batchID, status: report.errors.isEmpty ? "finished" : "finished_with_errors")
            }
        } catch {
            report.errors.append(error.fullTrace)
            try? await MainActor.run {
                try database.finishImportBatch(batchID, status: "failed")
            }
        }
        await MainActor.run { progress(report) }
        return report
    }

    private func countCandidateFiles(root: URL, report initialReport: ScanReport, progress: @escaping @MainActor (ScanReport) -> Void) async throws -> Int {
        guard let enumerator = makeEnumerator(root: root) else {
            throw CocoaError(.fileReadUnknown)
        }
        var report = initialReport
        var total = 0
        while let url = enumerator.nextObject() as? URL {
            if shouldSkipDirectory(url) {
                enumerator.skipDescendants()
                continue
            }
            guard try isRegularFile(url), SupportedFiles.isPhoto(url) else { continue }
            total += 1
            report.discoveredFiles = total
            report.currentPath = url.path
            if total == 1 || total % 50 == 0 {
                await MainActor.run { progress(report) }
            }
        }
        report.discoveredFiles = total
        report.currentPath = ""
        await MainActor.run { progress(report) }
        return total
    }

    private func makeEnumerator(root: URL) -> FileManager.DirectoryEnumerator? {
        FileManager.default.enumerator(
            at: root,
            includingPropertiesForKeys: [.isRegularFileKey, .isDirectoryKey, .isHiddenKey, .fileSizeKey],
            options: [.skipsHiddenFiles, .skipsPackageDescendants]
        )
    }

    private func isRegularFile(_ url: URL) throws -> Bool {
        let values = try url.resourceValues(forKeys: [.isRegularFileKey])
        return values.isRegularFile == true
    }

    private func shouldSkipDirectory(_ url: URL) -> Bool {
        if url.pathComponents.contains(where: { skippedDirectoryNames.contains($0.lowercased()) }) {
            return true
        }
        guard let values = try? url.resourceValues(forKeys: [.isDirectoryKey]), values.isDirectory == true else {
            return false
        }
        return skippedDirectoryNames.contains(url.lastPathComponent.lowercased())
    }

    private func scanFile(_ url: URL, storageKind: StorageKind, derivativeRoot: URL?) throws -> ScannedFile? {
        guard SupportedFiles.isPhoto(url) else { return nil }
        let values = try url.resourceValues(forKeys: [.fileSizeKey])
        let size = Int64(values.fileSize ?? 0)
        let hash = try FileHasher.sha256(url: url)
        let metadata = ImageMetadata.read(url: url)
        let fingerprint = metadata.fingerprint(filename: url.lastPathComponent, sizeBytes: size)
        let role = SupportedFiles.isRaw(url) ? FileRole.rawOriginal : FileRole.jpegOriginal
        let authority = storageKind == .nas ? AuthorityRole.canonical : AuthorityRole.workingCopy
        let syncStatus = storageKind == .nas ? SyncStatus.synced : SyncStatus.needsArchive
        let thumbnail = try generateThumbnail(source: url, contentHash: hash, derivativeRoot: derivativeRoot)

        return ScannedFile(
            url: url,
            deviceID: currentDeviceID(),
            storageKind: storageKind,
            fileRole: role,
            authorityRole: authority,
            syncStatus: syncStatus,
            sizeBytes: size,
            contentHash: hash,
            metadataFingerprint: fingerprint,
            captureTime: metadata.captureTime,
            cameraMake: metadata.cameraMake,
            cameraModel: metadata.cameraModel,
            lensModel: metadata.lensModel,
            rating: metadata.rating,
            thumbnailURL: thumbnail,
            thumbnailHash: thumbnail.flatMap { try? FileHasher.sha256(url: $0) },
            thumbnailSize: thumbnail.flatMap { (try? $0.resourceValues(forKeys: [.fileSizeKey]).fileSize).map(Int64.init) } ?? 0
        )
    }

    private func generateThumbnail(source: URL, contentHash: String, derivativeRoot: URL?) throws -> URL? {
        guard let derivativeRoot else { return nil }
        let thumbDir = derivativeRoot.appendingPathComponent("thumbnails", isDirectory: true)
        try FileManager.default.createDirectory(at: thumbDir, withIntermediateDirectories: true)

        let thumbnailURL = thumbDir.appendingPathComponent("\(contentHash)-320.jpg")
        if FileManager.default.fileExists(atPath: thumbnailURL.path) {
            return thumbnailURL
        }

        guard let image = ImageRenderer.renderableImage(url: source) else {
            return nil
        }
        try ImageRenderer.writeJPEG(image: image, maxPixel: 320, destination: thumbnailURL)
        return thumbnailURL
    }
}

enum SupportedFiles {
    static let rawExtensions: Set<String> = ["3fr", "ari", "arw", "bay", "cr2", "cr3", "crw", "dcr", "dng", "erf", "fff", "iiq", "k25", "kdc", "mef", "mos", "mrw", "nef", "nrw", "orf", "pef", "raf", "raw", "rw2", "rwl", "sr2", "srf", "srw"]
    static let jpegExtensions: Set<String> = ["jpg", "jpeg", "heic", "heif", "png", "tif", "tiff"]
    static let sidecarExtensions: Set<String> = ["xmp", "dop", "cos", "pp3"]
    static let exportExtensions: Set<String> = ["jpg", "jpeg", "png", "tif", "tiff", "webp"]

    static func isPhoto(_ url: URL) -> Bool {
        let ext = url.pathExtension.lowercased()
        return rawExtensions.contains(ext) || jpegExtensions.contains(ext)
    }

    static func isRaw(_ url: URL) -> Bool {
        rawExtensions.contains(url.pathExtension.lowercased())
    }

    static func isSidecar(_ url: URL) -> Bool {
        sidecarExtensions.contains(url.pathExtension.lowercased())
    }
}

struct ImageMetadata {
    var captureTime: Date?
    var cameraMake: String
    var cameraModel: String
    var lensModel: String
    var rating: Int

    static func read(url: URL) -> ImageMetadata {
        guard let source = CGImageSourceCreateWithURL(url as CFURL, nil),
              let properties = CGImageSourceCopyPropertiesAtIndex(source, 0, nil) as? [CFString: Any] else {
            return ImageMetadata(captureTime: nil, cameraMake: "", cameraModel: "", lensModel: "", rating: readSidecarRating(for: url) ?? 0)
        }

        let exif = properties[kCGImagePropertyExifDictionary] as? [CFString: Any]
        let tiff = properties[kCGImagePropertyTIFFDictionary] as? [CFString: Any]
        let iptc = properties[kCGImagePropertyIPTCDictionary] as? [CFString: Any]
        let captureText = exif?[kCGImagePropertyExifDateTimeOriginal] as? String
        let captureTime = captureText.flatMap(parseEXIFDate)
        let make = (tiff?[kCGImagePropertyTIFFMake] as? String) ?? ""
        let model = (tiff?[kCGImagePropertyTIFFModel] as? String) ?? ""
        let lens = (exif?[kCGImagePropertyExifLensModel] as? String) ?? ""
        let rating = readEmbeddedRating(source: source) ?? readDictionaryRating(properties) ?? readDictionaryRating(iptc ?? [:]) ?? readSidecarRating(for: url) ?? 0
        return ImageMetadata(captureTime: captureTime, cameraMake: make, cameraModel: model, lensModel: lens, rating: rating)
    }

    func fingerprint(filename: String, sizeBytes: Int64) -> String {
        let baseName = URL(fileURLWithPath: filename).deletingPathExtension().lastPathComponent
        return [captureTime.map(DateCoding.encode) ?? "", cameraMake, cameraModel, lensModel, normalizedBaseName(baseName)]
            .joined(separator: "|")
    }

    private func normalizedBaseName(_ name: String) -> String {
        name
            .replacingOccurrences(of: #"(?i)\s*\(\d+\)$"#, with: "", options: .regularExpression)
            .replacingOccurrences(of: #"(?i)[-_ ]?(edit|edited|export|copy|副本|已编辑)$"#, with: "", options: .regularExpression)
    }

    private static func parseEXIFDate(_ value: String) -> Date? {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.dateFormat = "yyyy:MM:dd HH:mm:ss"
        return formatter.date(from: value)
    }

    private static func readEmbeddedRating(source: CGImageSource) -> Int? {
        guard let metadata = CGImageSourceCopyMetadataAtIndex(source, 0, nil) else { return nil }
        let paths = ["xmp:Rating", "aux:Rating", "photoshop:Urgency"]
        for path in paths {
            guard let tag = CGImageMetadataCopyTagWithPath(metadata, nil, path as CFString),
                  let value = CGImageMetadataTagCopyValue(tag) else {
                continue
            }
            if let rating = normalizeRating(value) {
                return rating
            }
        }
        return nil
    }

    private static func readDictionaryRating(_ dictionary: [CFString: Any]) -> Int? {
        let keys = ["Rating", "rating", "StarRating", "Urgency"]
        for key in keys {
            if let value = dictionary[key as CFString], let rating = normalizeRating(value) {
                return rating
            }
        }
        return nil
    }

    private static func readSidecarRating(for url: URL) -> Int? {
        let candidates = [
            url.deletingPathExtension().appendingPathExtension("xmp"),
            URL(fileURLWithPath: url.path + ".xmp")
        ]
        for candidate in candidates where FileManager.default.fileExists(atPath: candidate.path) {
            guard let data = try? Data(contentsOf: candidate),
                  let text = String(data: data, encoding: .utf8) ?? String(data: data, encoding: .utf16) else {
                continue
            }
            if let rating = firstRating(in: text) {
                return rating
            }
        }
        return nil
    }

    private static func firstRating(in text: String) -> Int? {
        let patterns = [
            #"xmp:Rating\s*=\s*["'](-?\d+)["']"#,
            #"<xmp:Rating>\s*(-?\d+)\s*</xmp:Rating>"#,
            #"Rating\s*=\s*["'](-?\d+)["']"#
        ]
        for pattern in patterns {
            guard let regex = try? NSRegularExpression(pattern: pattern),
                  let match = regex.firstMatch(in: text, range: NSRange(text.startIndex..., in: text)),
                  let range = Range(match.range(at: 1), in: text) else {
                continue
            }
            if let value = Int(text[range]) {
                return clampRating(value)
            }
        }
        return nil
    }

    private static func normalizeRating(_ value: Any) -> Int? {
        if let int = value as? Int {
            return clampRating(int)
        }
        if let number = value as? NSNumber {
            return clampRating(number.intValue)
        }
        if let string = value as? String, let int = Int(string.trimmingCharacters(in: .whitespacesAndNewlines)) {
            return clampRating(int)
        }
        return nil
    }

    private static func clampRating(_ value: Int) -> Int {
        max(0, min(5, value))
    }
}

enum ImageRenderer {
    static func renderableImage(url: URL) -> NSImage? {
        if let source = CGImageSourceCreateWithURL(url as CFURL, nil),
           let cgImage = CGImageSourceCreateThumbnailAtIndex(
               source,
               0,
               [
                   kCGImageSourceCreateThumbnailFromImageAlways: true,
                   kCGImageSourceCreateThumbnailWithTransform: true,
                   kCGImageSourceThumbnailMaxPixelSize: 2200
               ] as CFDictionary
           ) {
            return NSImage(cgImage: cgImage, size: NSSize(width: cgImage.width, height: cgImage.height))
        }
        return NSImage(contentsOf: url)
    }

    static func writeJPEG(image: NSImage, maxPixel: CGFloat, destination: URL) throws {
        guard let resized = image.resized(maxPixel: maxPixel),
              let tiff = resized.tiffRepresentation,
              let bitmap = NSBitmapImageRep(data: tiff),
              let data = bitmap.representation(using: .jpeg, properties: [.compressionFactor: 0.84]) else {
            throw CocoaError(.fileWriteUnknown)
        }
        try data.write(to: destination, options: .atomic)
    }
}

private extension NSImage {
    func resized(maxPixel: CGFloat) -> NSImage? {
        let width = size.width
        let height = size.height
        guard width > 0, height > 0 else { return nil }
        let scale = min(maxPixel / max(width, height), 1)
        let newSize = NSSize(width: width * scale, height: height * scale)
        let image = NSImage(size: newSize)
        image.lockFocus()
        NSGraphicsContext.current?.imageInterpolation = .high
        draw(in: NSRect(origin: .zero, size: newSize), from: NSRect(origin: .zero, size: size), operation: .copy, fraction: 1)
        image.unlockFocus()
        return image
    }
}

extension Error {
    var fullTrace: String {
        "\(type(of: self)): \(localizedDescription)"
    }
}
