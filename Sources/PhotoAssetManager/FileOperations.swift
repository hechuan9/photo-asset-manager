import AppKit
import Foundation

enum FileOperationError: LocalizedError {
    case nasUnavailable(URL)
    case destinationExists(URL)
    case cannotWrite(URL)
    case hashMismatch(source: String, destination: String)
    case noOriginal
    case sourceFolderMissing(URL)
    case destinationInsideSource(URL)
    case sourceFileMissing(URL)
    case noImportableFiles(URL)
    case noMovableFiles

    var errorDescription: String? {
        switch self {
        case .nasUnavailable(let url): "NAS 目录不可用：\(url.path)"
        case .destinationExists(let url): "目标文件已存在，已停止以避免覆盖：\(url.path)"
        case .cannotWrite(let url): "目标目录不可写：\(url.path)"
        case .hashMismatch(let source, let destination): "复制前后 hash 不一致：\(source) -> \(destination)"
        case .noOriginal: "没有可用原片"
        case .sourceFolderMissing(let url): "源文件夹不存在：\(url.path)"
        case .destinationInsideSource(let url): "不能移动到源文件夹内部：\(url.path)"
        case .sourceFileMissing(let url): "源文件不存在：\(url.path)"
        case .noImportableFiles(let url): "没有找到可导入的照片或 sidecar 文件：\(url.path)"
        case .noMovableFiles: "没有可移动的在线照片文件。"
        }
    }
}

struct FileOperations: Sendable {
    private var fileManager: FileManager { FileManager.default }

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

    func buildFolderMovePlan(source: FolderMoveSource, destinationParent: URL, database: SQLiteDatabase) throws -> (destination: URL, items: [FolderMovePlanItem]) {
        let sourceURL = URL(fileURLWithPath: source.path, isDirectory: true)
        let sourcePath = normalizedDirectoryPath(sourceURL.path)
        let destination = destinationParent.appendingPathComponent(sourceURL.lastPathComponent, isDirectory: true)
        let destinationPath = normalizedDirectoryPath(destination.path)

        var isDirectory: ObjCBool = false
        guard fileManager.fileExists(atPath: sourcePath, isDirectory: &isDirectory), isDirectory.boolValue else {
            throw FileOperationError.sourceFolderMissing(sourceURL)
        }
        guard directoryWritable(destinationParent) else {
            throw FileOperationError.cannotWrite(destinationParent)
        }
        if destinationPath == sourcePath || destinationPath.hasPrefix(sourcePath + "/") {
            throw FileOperationError.destinationInsideSource(destination)
        }
        if fileManager.fileExists(atPath: destinationPath) {
            throw FileOperationError.destinationExists(destination)
        }

        let knownFiles = try database.fileInstancesForFolderMove(sourcePath: sourcePath)
        guard let enumerator = fileManager.enumerator(
            at: sourceURL,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: []
        ) else {
            throw FileOperationError.sourceFolderMissing(sourceURL)
        }

        var items: [FolderMovePlanItem] = []
        while let url = enumerator.nextObject() as? URL {
            let values = try url.resourceValues(forKeys: [.isRegularFileKey])
            guard values.isRegularFile == true else { continue }
            let sourceFilePath = normalizedDirectoryPath(url.path)
            let relativePath = relativeFilePath(sourceFilePath, under: sourcePath)
            let destinationFile = destination.appendingPathComponent(relativePath, isDirectory: false)
            let known = knownFiles[sourceFilePath]
            items.append(FolderMovePlanItem(
                sourcePath: sourceFilePath,
                destinationPath: destinationFile.path,
                fileInstanceID: known?.0,
                contentHash: known?.1 ?? ""
            ))
        }

        return (destination, items.sorted { $0.sourcePath.localizedStandardCompare($1.sourcePath) == .orderedAscending })
    }

    func buildPhotoImportPlan(source: URL, destinationTarget: PhotoImportTarget) throws -> (destination: URL, items: [PhotoImportPlanItem]) {
        let sourcePath = normalizedDirectoryPath(source.path)
        let destinationParent = URL(fileURLWithPath: destinationTarget.path, isDirectory: true)
        let destination = destinationParent.appendingPathComponent(source.lastPathComponent, isDirectory: true)
        let destinationPath = normalizedDirectoryPath(destination.path)

        var isDirectory: ObjCBool = false
        guard fileManager.fileExists(atPath: sourcePath, isDirectory: &isDirectory), isDirectory.boolValue else {
            throw FileOperationError.sourceFolderMissing(source)
        }
        guard directoryWritable(destinationParent) else {
            throw FileOperationError.cannotWrite(destinationParent)
        }
        if destinationPath == sourcePath || destinationPath.hasPrefix(sourcePath + "/") {
            throw FileOperationError.destinationInsideSource(destination)
        }
        if fileManager.fileExists(atPath: destinationPath) {
            throw FileOperationError.destinationExists(destination)
        }

        guard let enumerator = fileManager.enumerator(
            at: source,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles, .skipsPackageDescendants]
        ) else {
            throw FileOperationError.sourceFolderMissing(source)
        }

        var items: [PhotoImportPlanItem] = []
        while let url = enumerator.nextObject() as? URL {
            let values = try url.resourceValues(forKeys: [.isRegularFileKey])
            guard values.isRegularFile == true else { continue }
            guard SupportedFiles.isPhoto(url) || SupportedFiles.isSidecar(url) else { continue }
            let sourceFilePath = normalizedDirectoryPath(url.path)
            let relativePath = relativeFilePath(sourceFilePath, under: sourcePath)
            let destinationFile = destination.appendingPathComponent(relativePath, isDirectory: false)
            items.append(PhotoImportPlanItem(
                sourcePath: sourceFilePath,
                destinationPath: destinationFile.path,
                contentHash: try FileHasher.sha256(url: url)
            ))
        }

        guard !items.isEmpty else {
            throw FileOperationError.noImportableFiles(source)
        }
        return (destination, items.sorted { $0.sourcePath.localizedStandardCompare($1.sourcePath) == .orderedAscending })
    }

    func buildAssetFileMovePlan(assetIDs: [UUID], destinationTarget: FolderMoveTarget, database: SQLiteDatabase) throws -> [AssetFileMovePlanItem] {
        let destinationParent = URL(fileURLWithPath: destinationTarget.path, isDirectory: true)
        guard directoryWritable(destinationParent) else {
            throw FileOperationError.cannotWrite(destinationParent)
        }

        let files = try database.movableFileInstances(assetIDs: assetIDs)
        var plannedDestinations: Set<String> = []
        var items: [AssetFileMovePlanItem] = []
        for file in files {
            let source = URL(fileURLWithPath: file.path)
            guard fileManager.fileExists(atPath: source.path) else {
                throw FileOperationError.sourceFileMissing(source)
            }
            let destination = destinationParent.appendingPathComponent(source.lastPathComponent, isDirectory: false)
            if fileManager.fileExists(atPath: destination.path) || !plannedDestinations.insert(destination.path).inserted {
                throw FileOperationError.destinationExists(destination)
            }
            items.append(AssetFileMovePlanItem(
                fileInstanceID: file.id,
                sourcePath: source.path,
                destinationPath: destination.path,
                contentHash: file.contentHash
            ))
        }
        guard !items.isEmpty else {
            throw FileOperationError.noMovableFiles
        }
        return items.sorted { $0.sourcePath.localizedStandardCompare($1.sourcePath) == .orderedAscending }
    }

    func moveFolder(job: FolderMoveJob, database: SQLiteDatabase, progress: (FolderMoveJob, FolderMoveItem) async throws -> Void) async throws {
        let destinationURL = URL(fileURLWithPath: job.destinationPath, isDirectory: true)
        try fileManager.createDirectory(at: destinationURL, withIntermediateDirectories: true)
        try database.markFolderMoveJobRunning(id: job.id)

        for item in try database.pendingFolderMoveItems(jobID: job.id) {
            try await progress(job, item)
            let source = URL(fileURLWithPath: item.sourcePath)
            let destination = URL(fileURLWithPath: item.destinationPath)
            try moveFolderItem(item, source: source, destination: destination, database: database)
        }
        try emptySourceDirectoryTree(root: URL(fileURLWithPath: job.sourcePath, isDirectory: true))
    }

    func copyImportedFolder(destination: URL, items: [PhotoImportPlanItem], progress: (PhotoImportPlanItem, Int) async throws -> Void) async throws {
        try fileManager.createDirectory(at: destination, withIntermediateDirectories: true)
        for (index, item) in items.enumerated() {
            try await progress(item, index)
            let source = URL(fileURLWithPath: item.sourcePath)
            let destination = URL(fileURLWithPath: item.destinationPath)
            let parent = destination.deletingLastPathComponent()
            try fileManager.createDirectory(at: parent, withIntermediateDirectories: true)
            if fileManager.fileExists(atPath: destination.path) {
                throw FileOperationError.destinationExists(destination)
            }
            let sourceHash = try FileHasher.sha256(url: source)
            guard sourceHash == item.contentHash else {
                throw FileOperationError.hashMismatch(source: item.contentHash, destination: sourceHash)
            }
            try fileManager.copyItem(at: source, to: destination)
            let destinationHash = try FileHasher.sha256(url: destination)
            guard sourceHash == destinationHash else {
                try? fileManager.removeItem(at: destination)
                throw FileOperationError.hashMismatch(source: sourceHash, destination: destinationHash)
            }
        }
    }

    func moveAssetFiles(items: [AssetFileMovePlanItem], database: SQLiteDatabase, progress: (AssetFileMovePlanItem, Int) async throws -> Void) async throws {
        for (index, item) in items.enumerated() {
            try await progress(item, index)
            let source = URL(fileURLWithPath: item.sourcePath)
            let destination = URL(fileURLWithPath: item.destinationPath)
            try moveAssetFileItem(item, source: source, destination: destination, database: database)
        }
    }

    @MainActor
    func reveal(_ file: FileInstance) {
        NSWorkspace.shared.activateFileViewerSelecting([URL(fileURLWithPath: file.path)])
    }

    @MainActor
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

    private func moveFolderItem(_ item: FolderMoveItem, source: URL, destination: URL, database: SQLiteDatabase) throws {
        let parent = destination.deletingLastPathComponent()
        try fileManager.createDirectory(at: parent, withIntermediateDirectories: true)

        if fileManager.fileExists(atPath: destination.path) {
            let destinationHash = try FileHasher.sha256(url: destination)
            if !item.contentHash.isEmpty, destinationHash != item.contentHash {
                throw FileOperationError.destinationExists(destination)
            }
            if fileManager.fileExists(atPath: source.path) {
                try fileManager.removeItem(at: source)
            }
            let size = try Int64(destination.resourceValues(forKeys: [.fileSizeKey]).fileSize ?? 0)
            try database.completeFolderMoveItem(item, hash: destinationHash, sizeBytes: size)
            return
        }

        guard fileManager.fileExists(atPath: source.path) else {
            throw FileOperationError.sourceFileMissing(source)
        }
        let sourceHash = try FileHasher.sha256(url: source)
        if !item.contentHash.isEmpty, sourceHash != item.contentHash {
            throw FileOperationError.hashMismatch(source: item.contentHash, destination: sourceHash)
        }
        try fileManager.copyItem(at: source, to: destination)
        let destinationHash = try FileHasher.sha256(url: destination)
        guard sourceHash == destinationHash else {
            try? fileManager.removeItem(at: destination)
            throw FileOperationError.hashMismatch(source: sourceHash, destination: destinationHash)
        }
        try fileManager.removeItem(at: source)
        let size = try Int64(destination.resourceValues(forKeys: [.fileSizeKey]).fileSize ?? 0)
        try database.completeFolderMoveItem(item, hash: destinationHash, sizeBytes: size)
    }

    private func moveAssetFileItem(_ item: AssetFileMovePlanItem, source: URL, destination: URL, database: SQLiteDatabase) throws {
        let parent = destination.deletingLastPathComponent()
        try fileManager.createDirectory(at: parent, withIntermediateDirectories: true)
        if fileManager.fileExists(atPath: destination.path) {
            throw FileOperationError.destinationExists(destination)
        }
        guard fileManager.fileExists(atPath: source.path) else {
            throw FileOperationError.sourceFileMissing(source)
        }

        let sourceHash = try FileHasher.sha256(url: source)
        if !item.contentHash.isEmpty, sourceHash != item.contentHash {
            throw FileOperationError.hashMismatch(source: item.contentHash, destination: sourceHash)
        }
        try fileManager.copyItem(at: source, to: destination)
        let destinationHash = try FileHasher.sha256(url: destination)
        guard sourceHash == destinationHash else {
            try? fileManager.removeItem(at: destination)
            throw FileOperationError.hashMismatch(source: sourceHash, destination: destinationHash)
        }
        try fileManager.removeItem(at: source)
        let size = try Int64(destination.resourceValues(forKeys: [.fileSizeKey]).fileSize ?? 0)
        try database.completeAssetFileMoveItem(item, hash: destinationHash, sizeBytes: size)
    }

    private func emptySourceDirectoryTree(root: URL) throws {
        guard let enumerator = fileManager.enumerator(
            at: root,
            includingPropertiesForKeys: [.isDirectoryKey],
            options: []
        ) else {
            return
        }
        let directories = enumerator.compactMap { item -> URL? in
            guard let url = item as? URL,
                  (try? url.resourceValues(forKeys: [.isDirectoryKey]).isDirectory) == true else {
                return nil
            }
            return url
        }
        for directory in directories.sorted(by: { $0.path.count > $1.path.count }) {
            if (try? fileManager.contentsOfDirectory(atPath: directory.path).isEmpty) == true {
                try fileManager.removeItem(at: directory)
            }
        }
    }

    private func relativeFilePath(_ path: String, under root: String) -> String {
        let prefix = root == "/" ? "/" : root + "/"
        guard path.hasPrefix(prefix) else { return URL(fileURLWithPath: path).lastPathComponent }
        return String(path.dropFirst(prefix.count))
    }

    private func normalizedDirectoryPath(_ path: String) -> String {
        guard path.count > 1 else { return path }
        return path.hasSuffix("/") ? String(path.dropLast()) : path
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
