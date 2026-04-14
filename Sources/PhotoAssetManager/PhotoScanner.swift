import AppKit
import Foundation
import ImageIO
import UniformTypeIdentifiers

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
    var thumbnailURL: URL?
    var thumbnailHash: String?
    var thumbnailSize: Int64
    var previewURL: URL?
    var previewHash: String?
    var previewSize: Int64
}

struct PhotoScanner: @unchecked Sendable {
    private let cacheRoot: URL

    init(cacheRoot: URL) {
        self.cacheRoot = cacheRoot
    }

    func scanDirectory(_ root: URL, storageKind: StorageKind, database: SQLiteDatabase, progress: @escaping @MainActor (ScanReport) -> Void) async -> ScanReport {
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
                        report.skippedExistingFiles += 1
                        report.scannedFiles += 1
                        if report.scannedFiles % 25 == 0 {
                            await MainActor.run { progress(report) }
                        }
                        continue
                    }
                    report.currentPath = url.path
                    await MainActor.run { progress(report) }
                    guard let scanned = try scanFile(url, storageKind: storageKind) else {
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
        guard let values = try? url.resourceValues(forKeys: [.isDirectoryKey]), values.isDirectory == true else {
            return false
        }
        let name = url.lastPathComponent.lowercased()
        return name == "#recycle" || name == ".spotlight-v100" || name == ".trashes" || name == ".fseventsd"
    }

    private func scanFile(_ url: URL, storageKind: StorageKind) throws -> ScannedFile? {
        guard SupportedFiles.isPhoto(url) else { return nil }
        let values = try url.resourceValues(forKeys: [.fileSizeKey])
        let size = Int64(values.fileSize ?? 0)
        let hash = try FileHasher.sha256(url: url)
        let metadata = ImageMetadata.read(url: url)
        let fingerprint = metadata.fingerprint(filename: url.lastPathComponent, sizeBytes: size)
        let role = SupportedFiles.isRaw(url) ? FileRole.rawOriginal : FileRole.jpegOriginal
        let authority = storageKind == .nas ? AuthorityRole.canonical : AuthorityRole.workingCopy
        let syncStatus = storageKind == .nas ? SyncStatus.synced : SyncStatus.needsArchive
        let derivatives = try generateDerivatives(source: url, contentHash: hash)

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
            thumbnailURL: derivatives.thumbnail,
            thumbnailHash: derivatives.thumbnail.flatMap { try? FileHasher.sha256(url: $0) },
            thumbnailSize: derivatives.thumbnail.flatMap { (try? $0.resourceValues(forKeys: [.fileSizeKey]).fileSize).map(Int64.init) } ?? 0,
            previewURL: derivatives.preview,
            previewHash: derivatives.preview.flatMap { try? FileHasher.sha256(url: $0) },
            previewSize: derivatives.preview.flatMap { (try? $0.resourceValues(forKeys: [.fileSizeKey]).fileSize).map(Int64.init) } ?? 0
        )
    }

    private func generateDerivatives(source: URL, contentHash: String) throws -> (thumbnail: URL?, preview: URL?) {
        let thumbDir = cacheRoot.appendingPathComponent("thumbnails", isDirectory: true)
        let previewDir = cacheRoot.appendingPathComponent("previews", isDirectory: true)
        try FileManager.default.createDirectory(at: thumbDir, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: previewDir, withIntermediateDirectories: true)

        let thumbnailURL = thumbDir.appendingPathComponent("\(contentHash)-320.jpg")
        let previewURL = previewDir.appendingPathComponent("\(contentHash)-1600.jpg")
        if FileManager.default.fileExists(atPath: thumbnailURL.path), FileManager.default.fileExists(atPath: previewURL.path) {
            return (thumbnailURL, previewURL)
        }

        guard let image = ImageRenderer.renderableImage(url: source) else {
            return (nil, nil)
        }
        if !FileManager.default.fileExists(atPath: thumbnailURL.path) {
            try ImageRenderer.writeJPEG(image: image, maxPixel: 320, destination: thumbnailURL)
        }
        if !FileManager.default.fileExists(atPath: previewURL.path) {
            try ImageRenderer.writeJPEG(image: image, maxPixel: 1600, destination: previewURL)
        }
        return (thumbnailURL, previewURL)
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

    static func read(url: URL) -> ImageMetadata {
        guard let source = CGImageSourceCreateWithURL(url as CFURL, nil),
              let properties = CGImageSourceCopyPropertiesAtIndex(source, 0, nil) as? [CFString: Any] else {
            return ImageMetadata(captureTime: nil, cameraMake: "", cameraModel: "", lensModel: "")
        }

        let exif = properties[kCGImagePropertyExifDictionary] as? [CFString: Any]
        let tiff = properties[kCGImagePropertyTIFFDictionary] as? [CFString: Any]
        let captureText = exif?[kCGImagePropertyExifDateTimeOriginal] as? String
        let captureTime = captureText.flatMap(parseEXIFDate)
        let make = (tiff?[kCGImagePropertyTIFFMake] as? String) ?? ""
        let model = (tiff?[kCGImagePropertyTIFFModel] as? String) ?? ""
        let lens = (exif?[kCGImagePropertyExifLensModel] as? String) ?? ""
        return ImageMetadata(captureTime: captureTime, cameraMake: make, cameraModel: model, lensModel: lens)
    }

    func fingerprint(filename: String, sizeBytes: Int64) -> String {
        [captureTime.map(DateCoding.encode) ?? "", cameraMake, cameraModel, lensModel, filename, String(sizeBytes)]
            .joined(separator: "|")
    }

    private static func parseEXIFDate(_ value: String) -> Date? {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.dateFormat = "yyyy:MM:dd HH:mm:ss"
        return formatter.date(from: value)
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
