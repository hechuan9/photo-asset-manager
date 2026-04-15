import AppKit
import Foundation

@MainActor
final class LibraryStore: ObservableObject {
    @Published var assets: [Asset] = []
    @Published var selectedAssetID: UUID?
    @Published var selectedFiles: [FileInstance] = []
    @Published var filter = LibraryFilter()
    @Published var counts: [AssetStatus: Int] = [:]
    @Published var isScanning = false
    @Published var scanReport = ScanReport()
    @Published var lastError: String?
    @Published var nasRoot: URL?
    @Published var interruptedScanPath: String?
    @Published var sourceDirectories: [SourceDirectory] = []
    @Published var derivativeStorageURL: URL?
    @Published var migrationReport: String?
    @Published var blockingTask: BlockingTaskReport?

    private let database: SQLiteDatabase
    private let scanner: PhotoScanner
    private let fileOperations = FileOperations()

    init() {
        do {
            let support = try Self.applicationSupport()
            database = try SQLiteDatabase(path: support.appendingPathComponent("Library.sqlite"))
            scanner = PhotoScanner()
            try database.markInterruptedImportBatches()
            interruptedScanPath = try database.latestInterruptedScanPath()
            derivativeStorageURL = try database.derivativeStoragePath().map { URL(fileURLWithPath: $0, isDirectory: true) }
            try database.markMissingFiles()
            sourceDirectories = try database.sourceDirectories()
            refresh()
        } catch {
            fatalError(error.fullTrace)
        }
    }

    var selectedAsset: Asset? {
        assets.first { $0.id == selectedAssetID }
    }

    var isBusy: Bool {
        isScanning || blockingTask != nil
    }

    func chooseAndScan(storageKind: StorageKind) {
        chooseAndAddFolders(scanImmediately: true)
    }

    func chooseAndAddFolders(scanImmediately: Bool) {
        guard !isBusy else { return }
        let panel = NSOpenPanel()
        panel.canChooseDirectories = true
        panel.canChooseFiles = false
        panel.allowsMultipleSelection = true
        panel.message = "添加一个或多个照片文件夹"
        if panel.runModal() == .OK {
            addSourceDirectories(panel.urls, scanImmediately: scanImmediately)
        }
    }

    func addSourceDirectories(_ urls: [URL], scanImmediately: Bool) {
        guard !isBusy else { return }
        guard !urls.isEmpty else { return }
        do {
            for url in urls {
                let storageKind = storageKind(for: url)
                try database.upsertSourceDirectory(path: url.path, storageKind: storageKind)
                if storageKind == .nas, nasRoot == nil {
                    nasRoot = nasRootURL(for: url)
                }
            }
            sourceDirectories = try database.sourceDirectories()
            if scanImmediately {
                scanSources(sourceDirectories.filter { source in
                    urls.contains { $0.path == source.path }
                })
            }
        } catch {
            lastError = error.fullTrace
        }
    }

    func scan(_ url: URL, storageKind: StorageKind) {
        guard !isBusy else { return }
        isScanning = true
        scanReport = ScanReport()
        lastError = nil
        Task {
            let report = await scanner.scanDirectory(url, storageKind: storageKind, derivativeRoot: derivativeStorageURL, database: database) { [weak self] report in
                self?.scanReport = report
            }
            scanReport = report
            isScanning = false
            if !report.errors.isEmpty {
                lastError = report.errors.joined(separator: "\n\n")
            }
            try? database.markSourceDirectoryScanned(path: url.path)
            try? database.clearInterruptedBatches(sourcePath: url.path)
            interruptedScanPath = try? database.latestInterruptedScanPath()
            sourceDirectories = (try? database.sourceDirectories()) ?? sourceDirectories
            refresh()
        }
    }

    func scanSource(_ source: SourceDirectory) {
        guard source.isTracked else { return }
        scan(URL(fileURLWithPath: source.path), storageKind: source.storageKind)
    }

    func scanTrackedSources() {
        guard !isBusy else { return }
        scanSources(sourceDirectories.filter(\.isTracked))
    }

    func stopTrackingSource(_ source: SourceDirectory) {
        guard !isBusy else { return }
        do {
            try database.setSourceDirectoryTracked(id: source.id, isTracked: false)
            sourceDirectories = try database.sourceDirectories()
        } catch {
            lastError = error.fullTrace
        }
    }

    func resumeTrackingSource(_ source: SourceDirectory) {
        guard !isBusy else { return }
        do {
            try database.setSourceDirectoryTracked(id: source.id, isTracked: true)
            sourceDirectories = try database.sourceDirectories()
        } catch {
            lastError = error.fullTrace
        }
    }

    func removeSourceDirectory(_ source: SourceDirectory) {
        guard !isBusy else { return }
        do {
            try database.removeSourceDirectory(id: source.id)
            sourceDirectories = try database.sourceDirectories()
        } catch {
            lastError = error.fullTrace
        }
    }

    func chooseDerivativeMigrationLocation() {
        guard !isBusy else { return }
        let panel = NSOpenPanel()
        panel.canChooseDirectories = true
        panel.canChooseFiles = false
        panel.allowsMultipleSelection = false
        panel.message = "选择缩略图迁移目标位置"
        if panel.runModal() == .OK, let url = panel.url {
            migrateDerivativeStorage(to: url.appendingPathComponent("PhotoAssetManagerDerivatives", isDirectory: true))
        }
    }

    func clearDerivativeStorageLocation() {
        guard !isBusy else { return }
        setDerivativeStorageURL(nil)
    }

    func resumeInterruptedScan() {
        guard !isBusy else { return }
        guard let interruptedScanPath else { return }
        let url = URL(fileURLWithPath: interruptedScanPath)
        let storageKind: StorageKind = interruptedScanPath.hasPrefix("/Volumes/") ? .nas : .local
        if storageKind == .nas {
            nasRoot = url
        }
        scan(url, storageKind: storageKind)
    }

    func refresh() {
        do {
            try database.markMissingFiles()
            assets = try database.queryAssets(filter: filter)
            counts = try database.countsByStatus()
            sourceDirectories = try database.sourceDirectories()
            if selectedAssetID == nil {
                selectedAssetID = assets.first?.id
            }
            loadSelectedFiles()
        } catch {
            lastError = error.fullTrace
        }
    }

    func setStatusFilter(_ status: AssetStatus?) {
        filter.status = status
        refresh()
    }

    func loadSelectedFiles() {
        guard let selectedAssetID else {
            selectedFiles = []
            return
        }
        do {
            selectedFiles = try database.fileInstances(assetID: selectedAssetID)
        } catch {
            lastError = error.fullTrace
        }
    }

    func update(asset: Asset) {
        do {
            try database.updateAssetMetadata(asset: asset)
            refresh()
        } catch {
            lastError = error.fullTrace
        }
    }

    func archiveSelected() {
        guard !isBusy else { return }
        guard let asset = selectedAsset else { return }
        guard let root = preferredNASRoot() else {
            lastError = "没有可用的 NAS 文件夹。请先添加一个 /Volumes 下的文件夹。"
            return
        }
        archive(asset: asset, nasRoot: root)
    }

    func syncSelected() {
        guard !isBusy else { return }
        guard let asset = selectedAsset else { return }
        guard let root = preferredNASRoot() else {
            lastError = "没有可用的 NAS 文件夹。请先添加一个 /Volumes 下的文件夹。"
            return
        }
        sync(asset: asset, nasRoot: root)
    }

    func recordExportForSelected() {
        guard !isBusy else { return }
        guard let asset = selectedAsset else { return }
        let panel = NSOpenPanel()
        panel.canChooseDirectories = false
        panel.canChooseFiles = true
        panel.allowsMultipleSelection = true
        panel.message = "选择这个资产导出的 JPEG、PNG 或 TIFF"
        if panel.runModal() == .OK {
            do {
                for url in panel.urls {
                    try database.insertExport(assetID: asset.id, exportURL: url, sourceVersionID: nil)
                    try database.writeOperation(action: "record_export", source: asset.primaryPath, destination: url.path, status: "success", detail: "记录导出文件")
                }
                refresh()
            } catch {
                lastError = error.fullTrace
            }
        }
    }

    func reveal(file: FileInstance) {
        fileOperations.reveal(file)
    }

    func open(file: FileInstance) {
        fileOperations.open(file)
    }

    private func archive(asset: Asset, nasRoot: URL) {
        do {
            try fileOperations.archive(asset: asset, files: selectedFiles, nasRoot: nasRoot, database: database)
            refresh()
        } catch {
            lastError = error.fullTrace
        }
    }

    private func sync(asset: Asset, nasRoot: URL) {
        do {
            try fileOperations.syncChanges(asset: asset, files: selectedFiles, nasRoot: nasRoot, database: database)
            refresh()
        } catch {
            lastError = error.fullTrace
        }
    }

    private func scanSources(_ sources: [SourceDirectory]) {
        guard !isBusy else { return }
        guard !sources.isEmpty else { return }
        Task {
            for source in sources where source.isTracked {
                await MainActor.run {
                    scan(URL(fileURLWithPath: source.path), storageKind: source.storageKind)
                }
                while await MainActor.run(body: { isScanning }) {
                    try? await Task.sleep(nanoseconds: 200_000_000)
                }
            }
        }
    }

    private func setDerivativeStorageURL(_ url: URL?) {
        do {
            if let url {
                try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
            }
            try database.setDerivativeStoragePath(url?.path)
            derivativeStorageURL = url
        } catch {
            lastError = error.fullTrace
        }
    }

    private func migrateDerivativeStorage(to destinationRoot: URL) {
        do {
            blockingTask = BlockingTaskReport(title: "迁移缩略图", phase: "准备迁移", currentPath: destinationRoot.path)
            migrationReport = nil
            try FileManager.default.createDirectory(at: destinationRoot.appendingPathComponent("thumbnails", isDirectory: true), withIntermediateDirectories: true)
            let thumbnails = try database.thumbnailFileInstances()
            blockingTask?.totalItems = thumbnails.count
            blockingTask?.phase = thumbnails.isEmpty ? "没有可迁移缩略图" : "复制并校验"
            var copied = 0
            var skippedMissing = 0
            for (index, thumbnail) in thumbnails.enumerated() {
                let source = URL(fileURLWithPath: thumbnail.path)
                blockingTask?.currentPath = source.path
                blockingTask?.completedItems = index
                blockingTask?.skippedItems = skippedMissing
                guard FileManager.default.fileExists(atPath: source.path) else {
                    skippedMissing += 1
                    blockingTask?.completedItems = index + 1
                    blockingTask?.skippedItems = skippedMissing
                    blockingTask?.message = "已迁移 \(copied)，跳过 \(skippedMissing)"
                    RunLoop.current.run(mode: .default, before: Date(timeIntervalSinceNow: 0.001))
                    continue
                }
                let destination = destinationRoot
                    .appendingPathComponent("thumbnails", isDirectory: true)
                    .appendingPathComponent(source.lastPathComponent)
                if !FileManager.default.fileExists(atPath: destination.path) {
                    try FileManager.default.copyItem(at: source, to: destination)
                }
                let sourceHash = try FileHasher.sha256(url: source)
                let destinationHash = try FileHasher.sha256(url: destination)
                guard sourceHash == destinationHash else {
                    throw FileOperationError.hashMismatch(source: sourceHash, destination: destinationHash)
                }
                let size = try Int64(destination.resourceValues(forKeys: [.fileSizeKey]).fileSize ?? 0)
                try database.updateFileInstanceLocation(id: thumbnail.id, path: destination.path, hash: destinationHash, sizeBytes: size)
                copied += 1
                blockingTask?.completedItems = index + 1
                blockingTask?.message = "已迁移 \(copied)，跳过 \(skippedMissing)"
                RunLoop.current.run(mode: .default, before: Date(timeIntervalSinceNow: 0.001))
            }
            try database.setDerivativeStoragePath(destinationRoot.path)
            derivativeStorageURL = destinationRoot
            migrationReport = "缩略图迁移完成：\(copied) 个已迁移，\(skippedMissing) 个源文件缺失。旧文件未删除。"
            blockingTask = nil
            refresh()
        } catch {
            blockingTask = nil
            lastError = error.fullTrace
        }
    }

    private func preferredNASRoot() -> URL? {
        if let nasRoot {
            return nasRoot
        }
        return sourceDirectories
            .filter { $0.isTracked && $0.storageKind == .nas }
            .map { nasRootURL(for: URL(fileURLWithPath: $0.path)) }
            .first
    }

    private func storageKind(for url: URL) -> StorageKind {
        url.path.hasPrefix("/Volumes/") ? .nas : .local
    }

    private func nasRootURL(for url: URL) -> URL {
        let components = url.pathComponents
        guard components.count >= 3, components[1] == "Volumes" else {
            return url
        }
        return URL(fileURLWithPath: "/" + components[1] + "/" + components[2], isDirectory: true)
    }

    private static func applicationSupport() throws -> URL {
        let root = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("PhotoAssetManager", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        return root
    }
}
