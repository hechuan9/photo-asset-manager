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
    case ambiguousRawMatch(String, [String])
    case invalidImportConfiguration(String)
    case noMovableFiles
    case noDeletableFiles
    case folderContainsFiles(URL)
    case unsupportedFolderDeletion(URL)
    case assetFileDeletionFailed(URL, trashError: String)

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
        case .ambiguousRawMatch(let photo, let candidates):
            "无法唯一匹配 RAW：\(photo)\n候选：\(candidates.joined(separator: "\n"))"
        case .invalidImportConfiguration(let detail): detail
        case .noMovableFiles: "没有可移动的在线照片文件。"
        case .noDeletableFiles: "没有可删除的在线照片文件。"
        case .folderContainsFiles(let url): "文件夹内仍有文件，已阻止彻底删除：\(url.path)"
        case .unsupportedFolderDeletion(let url): "这个文件夹类型不支持物理删除：\(url.path)"
        case .assetFileDeletionFailed(let url, let trashError):
            "删除文件失败：\(url.path)\n废纸篓失败：\(trashError)"
        }
    }
}

enum AssetFileDeletionMethod: String, Sendable {
    case trash
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

    func buildPhotoImportPlan(
        configuration: PhotoImportConfiguration,
        progress: ((String, String, Int, Int) -> Void)? = nil
    ) throws -> PhotoImportPlan {
        let context = try makePhotoImportContext(configuration: configuration, progress: progress)
        if !context.ambiguousPhotos.isEmpty {
            throw FileOperationError.ambiguousRawMatch(
                context.ambiguousPhotos[0],
                context.rawMatches[context.ambiguousPhotos[0]].map { match in
                    if case .ambiguous(let files) = match {
                        return files.map(\.url.path)
                    }
                    return []
                } ?? []
            )
        }

        progress?("整理导入列表", context.mainDestination.path, 0, context.importPhotos.count)

        var items: [PhotoImportPlanItem] = []
        var rawSidecarSources: Set<String> = []

        var plannedDestinations: Set<String> = []
        for (index, photo) in context.importPhotos.enumerated() {
            progress?("整理导入列表", photo.sourcePath, index + 1, context.importPhotos.count)
            try appendFlattenedImportItem(
                source: photo.url,
                destinationRoot: context.mainDestination,
                routesToHasselbladRaw: false,
                plannedDestinations: &plannedDestinations,
                items: &items
            )

            for sidecar in try sidecarsBesidePhoto(photo.url) where !SupportedFiles.isRaw(sidecar) {
                try appendFlattenedImportItem(
                    source: sidecar,
                    destinationRoot: context.mainDestination,
                    routesToHasselbladRaw: false,
                    plannedDestinations: &plannedDestinations,
                    items: &items
                )
            }

            guard let rawDestination = context.rawDestination else { continue }
            guard case .matched(let rawFile) = context.rawMatches[photo.sourcePath] else { continue }

            try appendFlattenedImportItem(
                source: rawFile.url,
                destinationRoot: rawDestination,
                routesToHasselbladRaw: true,
                plannedDestinations: &plannedDestinations,
                items: &items
            )

            for sidecar in try sidecarsBesidePhoto(rawFile.url) {
                let sidecarPath = normalizedDirectoryPath(sidecar.path)
                guard rawSidecarSources.insert(sidecarPath).inserted else { continue }
                try appendFlattenedImportItem(
                    source: sidecar,
                    destinationRoot: rawDestination,
                    routesToHasselbladRaw: true,
                    plannedDestinations: &plannedDestinations,
                    items: &items
                )
            }
        }

        let sortedItems = items.sorted { $0.sourcePath.localizedStandardCompare($1.sourcePath) == .orderedAscending }
        let matchedRawCount = context.rawMatches.values.reduce(into: 0) { count, match in
            if case .matched = match { count += 1 }
        }
        return PhotoImportPlan(
            mainDestination: context.mainDestination,
            hasselbladRawDestination: sortedItems.contains(where: \.routesToHasselbladRaw) ? context.rawDestination : nil,
            items: sortedItems,
            stats: PhotoImportStats(
                photoCount: context.importPhotos.count,
                matchedRawCount: matchedRawCount,
                unmatchedPhotoCount: context.unmatchedPhotos.count
            )
        )
    }

    private struct ImportPhotoCandidate {
        var url: URL
        var sourcePath: String
        var relativePath: String
        var metadata: ImageMetadata
        var fingerprint: String
    }

    private struct IndexedRawFile {
        var url: URL
        var sourcePath: String
        var normalizedBaseName: String
        var metadata: ImageMetadata
        var fingerprint: String
    }

    private enum RawMatchResult {
        case matched(IndexedRawFile)
        case unmatched
        case ambiguous([IndexedRawFile])
    }

    private struct PhotoImportContext {
        var mainDestination: URL
        var rawDestination: URL?
        var importPhotos: [ImportPhotoCandidate]
        var rawIndex: [IndexedRawFile]
        var rawMatches: [String: RawMatchResult]
        var unmatchedPhotos: [String]
        var ambiguousPhotos: [String]
    }

    private func makePhotoImportContext(
        configuration: PhotoImportConfiguration,
        progress: ((String, String, Int, Int) -> Void)? = nil
    ) throws -> PhotoImportContext {
        let importSource = configuration.importSource.resolvingSymlinksInPath()
        let importSourcePath = normalizedDirectoryPath(importSource.path)
        let destinationParent = URL(fileURLWithPath: configuration.target.path, isDirectory: true)
        let mainDestination = destinationParent
        let mainDestinationPath = normalizedDirectoryPath(mainDestination.path)
        let rawDestination = configuration.targetRawRoot
        let rawDestinationPath = rawDestination.map { normalizedDirectoryPath($0.path) }

        try validateImportDirectories(
            importSource: importSource,
            importSourcePath: importSourcePath,
            rawSource: configuration.rawSource,
            destinationParent: destinationParent,
            mainDestination: mainDestination,
            mainDestinationPath: mainDestinationPath,
            rawDestination: rawDestination,
            rawDestinationPath: rawDestinationPath
        )

        let importPhotos = try collectImportPhotos(from: importSource, rootPath: importSourcePath, progress: progress)
        guard !importPhotos.isEmpty else {
            throw FileOperationError.noImportableFiles(importSource)
        }
        progress?("扫描照片", importSourcePath, importPhotos.count, importPhotos.count)

        let rawIndex: [IndexedRawFile]
        if let rawSource = configuration.rawSource {
            rawIndex = try indexRawFiles(in: rawSource, progress: progress)
        } else {
            rawIndex = []
        }
        var rawMatches: [String: RawMatchResult] = [:]
        var unmatchedPhotos: [String] = []
        var ambiguousPhotos: [String] = []

        if configuration.rawSource != nil {
            for (index, photo) in importPhotos.enumerated() {
                progress?("匹配 RAW", photo.sourcePath, index + 1, importPhotos.count)
                let match = matchRawFile(
                    normalizedBaseName: ImportFileMatcher.normalizedBaseName(photo.url.deletingPathExtension().lastPathComponent),
                    metadata: photo.metadata,
                    fingerprint: photo.fingerprint,
                    candidates: rawIndex
                )
                rawMatches[photo.sourcePath] = match
                switch match {
                case .unmatched:
                    unmatchedPhotos.append(photo.url.lastPathComponent)
                case .ambiguous:
                    ambiguousPhotos.append(photo.url.lastPathComponent)
                case .matched:
                    break
                }
            }
            progress?("匹配 RAW", importSourcePath, importPhotos.count, importPhotos.count)
        }

        return PhotoImportContext(
            mainDestination: mainDestination,
            rawDestination: rawDestination,
            importPhotos: importPhotos,
            rawIndex: rawIndex,
            rawMatches: rawMatches,
            unmatchedPhotos: unmatchedPhotos.sorted(),
            ambiguousPhotos: ambiguousPhotos.sorted()
        )
    }

    private func validateImportDirectories(
        importSource: URL,
        importSourcePath: String,
        rawSource: URL?,
        destinationParent: URL,
        mainDestination: URL,
        mainDestinationPath: String,
        rawDestination: URL?,
        rawDestinationPath: String?
    ) throws {
        var isDirectory: ObjCBool = false
        guard fileManager.fileExists(atPath: importSourcePath, isDirectory: &isDirectory), isDirectory.boolValue else {
            throw FileOperationError.sourceFolderMissing(importSource)
        }
        guard directoryWritable(destinationParent) else {
            throw FileOperationError.cannotWrite(destinationParent)
        }
        if let rawSource {
            let rawSourcePath = normalizedDirectoryPath(rawSource.resolvingSymlinksInPath().path)
            guard fileManager.fileExists(atPath: rawSourcePath, isDirectory: &isDirectory), isDirectory.boolValue else {
                throw FileOperationError.sourceFolderMissing(rawSource)
            }
            guard let rawDestination else {
                throw FileOperationError.invalidImportConfiguration("已选择 RAW 文件夹时，必须同时选择目标 RAW 文件夹。")
            }
            guard directoryWritable(rawDestination) else {
                throw FileOperationError.cannotWrite(rawDestination)
            }
        }
        if mainDestinationPath == importSourcePath || mainDestinationPath.hasPrefix(importSourcePath + "/") {
            throw FileOperationError.destinationInsideSource(mainDestination)
        }
        if let rawDestinationPath,
           let rawSource,
           rawDestinationPath == normalizedDirectoryPath(rawSource.path) || rawDestinationPath.hasPrefix(normalizedDirectoryPath(rawSource.path) + "/") {
            throw FileOperationError.destinationInsideSource(rawDestination!)
        }
    }

    private func appendFlattenedImportItem(
        source: URL,
        destinationRoot: URL,
        routesToHasselbladRaw: Bool,
        plannedDestinations: inout Set<String>,
        items: inout [PhotoImportPlanItem]
    ) throws {
        let destination = destinationRoot.appendingPathComponent(source.lastPathComponent, isDirectory: false)
        let destinationPath = normalizedDirectoryPath(destination.path)
        if fileManager.fileExists(atPath: destinationPath) || !plannedDestinations.insert(destinationPath).inserted {
            throw FileOperationError.destinationExists(destination)
        }
        items.append(PhotoImportPlanItem(
            sourcePath: normalizedDirectoryPath(source.path),
            destinationPath: destinationPath,
            contentHash: "",
            routesToHasselbladRaw: routesToHasselbladRaw
        ))
    }

    private func collectImportPhotos(
        from root: URL,
        rootPath: String,
        progress: ((String, String, Int, Int) -> Void)? = nil
    ) throws -> [ImportPhotoCandidate] {
        guard let enumerator = fileManager.enumerator(
            at: root,
            includingPropertiesForKeys: [.isRegularFileKey, .fileSizeKey],
            options: [.skipsHiddenFiles, .skipsPackageDescendants]
        ) else {
            throw FileOperationError.sourceFolderMissing(root)
        }

        var photos: [ImportPhotoCandidate] = []
        var discovered = 0
        while let url = enumerator.nextObject() as? URL {
            let values = try url.resourceValues(forKeys: [.isRegularFileKey, .fileSizeKey])
            guard values.isRegularFile == true else { continue }
            guard SupportedFiles.isPhoto(url), !SupportedFiles.isRaw(url) else { continue }
            let sourceFilePath = normalizedDirectoryPath(url.path)
            let metadata = ImageMetadata.read(url: url)
            let sizeBytes = Int64(values.fileSize ?? 0)
            photos.append(ImportPhotoCandidate(
                url: url,
                sourcePath: sourceFilePath,
                relativePath: relativeFilePath(sourceFilePath, under: rootPath),
                metadata: metadata,
                fingerprint: metadata.fingerprint(filename: url.lastPathComponent, sizeBytes: sizeBytes)
            ))
            discovered += 1
            if discovered == 1 || discovered % 20 == 0 {
                progress?("扫描照片", sourceFilePath, discovered, 0)
            }
        }
        return photos.sorted { $0.sourcePath.localizedStandardCompare($1.sourcePath) == .orderedAscending }
    }

    private func indexRawFiles(
        in root: URL,
        progress: ((String, String, Int, Int) -> Void)? = nil
    ) throws -> [IndexedRawFile] {
        guard let enumerator = fileManager.enumerator(
            at: root,
            includingPropertiesForKeys: [.isRegularFileKey, .fileSizeKey],
            options: [.skipsHiddenFiles, .skipsPackageDescendants]
        ) else {
            throw FileOperationError.sourceFolderMissing(root)
        }

        var indexed: [IndexedRawFile] = []
        var discovered = 0
        while let url = enumerator.nextObject() as? URL {
            let values = try url.resourceValues(forKeys: [.isRegularFileKey, .fileSizeKey])
            guard values.isRegularFile == true else { continue }
            guard SupportedFiles.isRaw(url) else { continue }
            let metadata = ImageMetadata.read(url: url)
            let sizeBytes = Int64(values.fileSize ?? 0)
            let sourcePath = normalizedDirectoryPath(url.path)
            indexed.append(IndexedRawFile(
                url: url,
                sourcePath: sourcePath,
                normalizedBaseName: ImportFileMatcher.normalizedBaseName(url.deletingPathExtension().lastPathComponent),
                metadata: metadata,
                fingerprint: metadata.fingerprint(filename: url.lastPathComponent, sizeBytes: sizeBytes)
            ))
            discovered += 1
            if discovered == 1 || discovered % 20 == 0 {
                progress?("索引 RAW", sourcePath, discovered, 0)
            }
        }
        progress?("索引 RAW", root.path, indexed.count, indexed.count)
        return indexed
    }

    private func sidecarsBesidePhoto(_ photo: URL) throws -> [URL] {
        let directory = photo.deletingLastPathComponent()
        let baseName = photo.deletingPathExtension().lastPathComponent
        return SupportedFiles.sidecarExtensions
            .map { directory.appendingPathComponent(baseName).appendingPathExtension($0) }
            .filter { fileManager.fileExists(atPath: $0.path) }
    }

    private func matchRawFile(
        normalizedBaseName: String,
        metadata: ImageMetadata,
        fingerprint: String,
        candidates: [IndexedRawFile]
    ) -> RawMatchResult {
        let basenameMatches = candidates.filter { $0.normalizedBaseName == normalizedBaseName }
        switch basenameMatches.count {
        case 0:
            return .unmatched
        case 1:
            return .matched(basenameMatches[0])
        default:
            let fingerprintMatches = basenameMatches.filter { $0.fingerprint == fingerprint }
            if fingerprintMatches.count == 1 {
                return .matched(fingerprintMatches[0])
            }
            if fingerprintMatches.count > 1 {
                return .ambiguous(fingerprintMatches)
            }

            let scored = basenameMatches.map { candidate in
                (candidate, metadataCompatibilityScore(photo: metadata, raw: candidate.metadata))
            }
            let bestScore = scored.map(\.1).max() ?? 0
            guard bestScore >= 4 else {
                return .ambiguous(basenameMatches)
            }
            let winners = scored.filter { $0.1 == bestScore }.map(\.0)
            if winners.count == 1 {
                return .matched(winners[0])
            }
            return .ambiguous(winners)
        }
    }

    private func metadataCompatibilityScore(photo: ImageMetadata, raw: ImageMetadata) -> Int {
        var score = 0
        if let photoTime = photo.captureTime, let rawTime = raw.captureTime,
           abs(photoTime.timeIntervalSince(rawTime)) <= 2 {
            score += 4
        }
        if !photo.cameraMake.isEmpty, photo.cameraMake == raw.cameraMake {
            score += 1
        }
        if !photo.cameraModel.isEmpty, photo.cameraModel == raw.cameraModel {
            score += 2
        }
        if !photo.lensModel.isEmpty, photo.lensModel == raw.lensModel {
            score += 1
        }
        return score
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
            try copyImportedItem(item)
        }
    }

    func runPhotoImportJob(
        job: PhotoImportJob,
        database: SQLiteDatabase,
        scanner: PhotoScanner,
        derivativeRoot: URL?,
        batchID: UUID,
        ledgerContext: ScannedFileLedgerContext?,
        progress: (PhotoImportJob, PhotoImportItem) async throws -> Void,
        didPersist: (ScannedFileUpsertResult) -> Void
    ) async throws {
        let mainDestination = URL(fileURLWithPath: job.targetPath, isDirectory: true)
        try fileManager.createDirectory(at: mainDestination, withIntermediateDirectories: true)
        if let rawRootPath = job.targetRawRootPath {
            try fileManager.createDirectory(at: URL(fileURLWithPath: rawRootPath, isDirectory: true), withIntermediateDirectories: true)
        }
        try database.markPhotoImportJobRunning(id: job.id)

        for item in try database.pendingPhotoImportItems(jobID: job.id) {
            try await progress(job, item)
            let destination = URL(fileURLWithPath: item.destinationPath)
            if fileManager.fileExists(atPath: destination.path) {
                if !item.contentHash.isEmpty {
                    let destinationHash = try FileHasher.sha256(url: destination)
                    if destinationHash != item.contentHash {
                        throw FileOperationError.destinationExists(destination)
                    }
                }
            } else {
                try copyImportedItem(PhotoImportPlanItem(
                    sourcePath: item.sourcePath,
                    destinationPath: item.destinationPath,
                    contentHash: item.contentHash,
                    routesToHasselbladRaw: item.routesToHasselbladRaw
                ))
            }
            if let result = try scanner.persistImportedFile(
                at: destination,
                storageKind: job.storageKind,
                derivativeRoot: derivativeRoot,
                database: database,
                batchID: batchID,
                ledgerContext: ledgerContext
            ) {
                didPersist(result)
            }
            try database.completePhotoImportItem(item)
        }
        try database.completePhotoImportJob(job)
    }

    private func copyImportedItem(_ item: PhotoImportPlanItem) throws {
        let source = URL(fileURLWithPath: item.sourcePath)
        let destination = URL(fileURLWithPath: item.destinationPath)
        let parent = destination.deletingLastPathComponent()
        try fileManager.createDirectory(at: parent, withIntermediateDirectories: true)
        if fileManager.fileExists(atPath: destination.path) {
            throw FileOperationError.destinationExists(destination)
        }
        let sourceHash = try FileHasher.sha256(url: source)
        if !item.contentHash.isEmpty {
            guard sourceHash == item.contentHash else {
                throw FileOperationError.hashMismatch(source: item.contentHash, destination: sourceHash)
            }
        }
        try fileManager.copyItem(at: source, to: destination)
        let destinationHash = try FileHasher.sha256(url: destination)
        guard sourceHash == destinationHash else {
            try? fileManager.removeItem(at: destination)
            throw FileOperationError.hashMismatch(source: sourceHash, destination: destinationHash)
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

    private func deleteAssetFile(_ file: FileInstance) throws -> AssetFileDeletionMethod {
        let url = URL(fileURLWithPath: file.path)
        guard fileManager.fileExists(atPath: url.path) else {
            throw FileOperationError.sourceFileMissing(url)
        }

        var trashedURL: NSURL?
        do {
            try fileManager.trashItem(at: url, resultingItemURL: &trashedURL)
            return .trash
        } catch {
            let trashTrace = error.fullTrace
            throw FileOperationError.assetFileDeletionFailed(url, trashError: trashTrace)
        }
    }

    func deleteAssetFiles(files: [FileInstance], database: SQLiteDatabase, progress: (FileInstance, Int) async throws -> Void) async throws {
        guard !files.isEmpty else {
            throw FileOperationError.noDeletableFiles
        }
        for (index, file) in files.enumerated() {
            try await progress(file, index)
            do {
                let method = try deleteAssetFile(file)
                try database.removeDeletedFileInstance(file, deletionMethod: method)
            } catch {
                try? database.writeOperation(
                    action: "delete_asset_file",
                    source: file.path,
                    destination: nil,
                    status: "failed",
                    detail: error.fullTrace
                )
                throw error
            }
        }
    }

    func deleteEmptyFolderTree(at url: URL, storageKind: StorageKind) throws {
        try ensureFolderTreeContainsOnlyDirectories(at: url)
        switch storageKind {
        case .local:
            try trashEmptyFolderTree(at: url)
        case .nas, .externalDrive:
            try removeEmptyDirectoryTree(at: url)
        case .cloudPreview:
            throw FileOperationError.unsupportedFolderDeletion(url)
        }
    }

    private func ensureFolderTreeContainsOnlyDirectories(at url: URL) throws {
        var isDirectory: ObjCBool = false
        guard fileManager.fileExists(atPath: url.path, isDirectory: &isDirectory), isDirectory.boolValue else {
            throw FileOperationError.sourceFolderMissing(url)
        }
        guard let enumerator = fileManager.enumerator(
            at: url,
            includingPropertiesForKeys: [.isRegularFileKey, .isDirectoryKey, .isSymbolicLinkKey],
            options: []
        ) else {
            throw FileOperationError.sourceFolderMissing(url)
        }
        while let item = enumerator.nextObject() as? URL {
            let values = try item.resourceValues(forKeys: [.isRegularFileKey, .isDirectoryKey, .isSymbolicLinkKey])
            if values.isDirectory != true || values.isSymbolicLink == true || values.isRegularFile == true {
                throw FileOperationError.folderContainsFiles(item)
            }
        }
    }

    private func trashEmptyFolderTree(at url: URL) throws {
        var trashedURL: NSURL?
        try fileManager.trashItem(at: url, resultingItemURL: &trashedURL)
    }

    private func removeEmptyDirectoryTree(at url: URL) throws {
        var directories = [url]
        if let enumerator = fileManager.enumerator(
            at: url,
            includingPropertiesForKeys: [.isDirectoryKey],
            options: []
        ) {
            while let item = enumerator.nextObject() as? URL {
                let values = try item.resourceValues(forKeys: [.isDirectoryKey])
                if values.isDirectory == true {
                    directories.append(item)
                }
            }
        }
        for directory in directories.sorted(by: { $0.path.count > $1.path.count }) {
            let contents = try fileManager.contentsOfDirectory(at: directory, includingPropertiesForKeys: [])
            guard contents.isEmpty else {
                throw FileOperationError.folderContainsFiles(contents[0])
            }
            try fileManager.removeItem(at: directory)
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
        let fileURL = URL(fileURLWithPath: path).resolvingSymlinksInPath()
        let rootURL = URL(fileURLWithPath: root, isDirectory: true).resolvingSymlinksInPath()
        let rootPath = normalizedDirectoryPath(rootURL.path)
        let filePath = normalizedDirectoryPath(fileURL.path)
        let prefix = rootPath == "/" ? "/" : rootPath + "/"
        guard filePath.hasPrefix(prefix) else {
            return fileURL.lastPathComponent
        }
        return String(filePath.dropFirst(prefix.count))
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

enum ImportFileMatcher {
    static func normalizedBaseName(_ name: String) -> String {
        name
            .replacingOccurrences(of: #"(?i)\s*\(\d+\)$"#, with: "", options: .regularExpression)
            .replacingOccurrences(of: #"(?i)[-_ ]?(edit|edited|export|copy|副本|已编辑)$"#, with: "", options: .regularExpression)
    }
}
