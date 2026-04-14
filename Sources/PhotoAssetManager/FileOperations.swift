import AppKit
import Foundation

enum FileOperationError: LocalizedError {
    case nasUnavailable(URL)
    case destinationExists(URL)
    case cannotWrite(URL)
    case hashMismatch(source: String, destination: String)
    case noOriginal

    var errorDescription: String? {
        switch self {
        case .nasUnavailable(let url): "NAS 目录不可用：\(url.path)"
        case .destinationExists(let url): "目标文件已存在，已停止以避免覆盖：\(url.path)"
        case .cannotWrite(let url): "目标目录不可写：\(url.path)"
        case .hashMismatch(let source, let destination): "复制前后 hash 不一致：\(source) -> \(destination)"
        case .noOriginal: "没有可用原片"
        }
    }
}

@MainActor
struct FileOperations {
    private let fileManager = FileManager.default

    func archive(asset: Asset, files: [FileInstance], nasRoot: URL, database: SQLiteDatabase) throws {
        guard directoryWritable(nasRoot) else {
            try? database.writeOperation(action: "archive", source: asset.primaryPath, destination: nasRoot.path, status: "failed", detail: FileOperationError.nasUnavailable(nasRoot).localizedDescription)
            throw FileOperationError.nasUnavailable(nasRoot)
        }
        let originals = files.filter { ($0.fileRole == .rawOriginal || $0.fileRole == .jpegOriginal) && $0.storageKind != .nas && $0.availability == .online }
        guard !originals.isEmpty else { throw FileOperationError.noOriginal }

        for file in originals {
            let source = URL(fileURLWithPath: file.path)
            let destination = archiveDestination(asset: asset, source: source, nasRoot: nasRoot)
            try copyVerified(source: source, destination: destination, expectedHash: file.contentHash)
            let size = try Int64(destination.resourceValues(forKeys: [.fileSizeKey]).fileSize ?? 0)
            let hash = try FileHasher.sha256(url: destination)
            try database.insertCopiedFile(assetID: asset.id, source: file, destination: destination, storageKind: .nas, authorityRole: .canonical, syncStatus: .synced, hash: hash, sizeBytes: size)
            try database.markFileStatus(id: file.id, syncStatus: .synced, authorityRole: .workingCopy)
            try database.writeOperation(action: "archive", source: source.path, destination: destination.path, status: "success", detail: "hash=\(hash)")
        }
    }

    func syncChanges(asset: Asset, files: [FileInstance], nasRoot: URL, database: SQLiteDatabase) throws {
        guard directoryWritable(nasRoot) else {
            try? database.writeOperation(action: "sync", source: asset.primaryPath, destination: nasRoot.path, status: "failed", detail: FileOperationError.nasUnavailable(nasRoot).localizedDescription)
            throw FileOperationError.nasUnavailable(nasRoot)
        }

        let candidates = files.filter { $0.syncStatus == .needsSync || $0.fileRole == .export || $0.fileRole == .sidecar }
        for file in candidates where file.availability == .online {
            let source = URL(fileURLWithPath: file.path)
            let destination = syncDestination(asset: asset, source: source, nasRoot: nasRoot, role: file.fileRole)
            try copyVerified(source: source, destination: destination, expectedHash: file.contentHash)
            let size = try Int64(destination.resourceValues(forKeys: [.fileSizeKey]).fileSize ?? 0)
            let hash = try FileHasher.sha256(url: destination)
            try database.insertCopiedFile(assetID: asset.id, source: file, destination: destination, storageKind: .nas, authorityRole: .canonical, syncStatus: .synced, hash: hash, sizeBytes: size)
            try database.markFileStatus(id: file.id, syncStatus: .synced)
            try database.writeOperation(action: "sync", source: source.path, destination: destination.path, status: "success", detail: "hash=\(hash)")
        }
    }

    func reveal(_ file: FileInstance) {
        NSWorkspace.shared.activateFileViewerSelecting([URL(fileURLWithPath: file.path)])
    }

    func open(_ file: FileInstance) {
        NSWorkspace.shared.open(URL(fileURLWithPath: file.path))
    }

    private func copyVerified(source: URL, destination: URL, expectedHash: String) throws {
        let parent = destination.deletingLastPathComponent()
        guard directoryWritable(parent) else { throw FileOperationError.cannotWrite(parent) }
        if fileManager.fileExists(atPath: destination.path) {
            throw FileOperationError.destinationExists(destination)
        }

        let sourceHash = try FileHasher.sha256(url: source)
        if !expectedHash.isEmpty, sourceHash != expectedHash {
            throw FileOperationError.hashMismatch(source: expectedHash, destination: sourceHash)
        }
        try fileManager.copyItem(at: source, to: destination)
        let destinationHash = try FileHasher.sha256(url: destination)
        guard sourceHash == destinationHash else {
            try? fileManager.removeItem(at: destination)
            throw FileOperationError.hashMismatch(source: sourceHash, destination: destinationHash)
        }
    }

    private func archiveDestination(asset: Asset, source: URL, nasRoot: URL) -> URL {
        let date = asset.captureTime ?? asset.createdAt
        let components = Calendar.current.dateComponents([.year, .month], from: date)
        let year = String(format: "%04d", components.year ?? 0)
        let month = String(format: "%02d", components.month ?? 0)
        let directory = nasRoot
            .appendingPathComponent("Originals", isDirectory: true)
            .appendingPathComponent(year, isDirectory: true)
            .appendingPathComponent(month, isDirectory: true)
        try? fileManager.createDirectory(at: directory, withIntermediateDirectories: true)
        return directory.appendingPathComponent(source.lastPathComponent)
    }

    private func syncDestination(asset: Asset, source: URL, nasRoot: URL, role: FileRole) -> URL {
        let base = nasRoot
            .appendingPathComponent(role == .export ? "Exports" : "Sidecars", isDirectory: true)
            .appendingPathComponent(asset.id.uuidString, isDirectory: true)
        try? fileManager.createDirectory(at: base, withIntermediateDirectories: true)
        return base.appendingPathComponent(source.lastPathComponent)
    }

    private func directoryWritable(_ url: URL) -> Bool {
        var isDirectory: ObjCBool = false
        guard fileManager.fileExists(atPath: url.path, isDirectory: &isDirectory), isDirectory.boolValue else {
            return false
        }
        return fileManager.isWritableFile(atPath: url.path)
    }
}
