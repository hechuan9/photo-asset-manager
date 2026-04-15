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
    @Published var backgroundTask: BackgroundTaskReport?
    @Published var hasMoreAssets = false

    private let database: SQLiteDatabase
    private let scanner: PhotoScanner
    private let fileOperations = FileOperations()
    private var availabilityTask: Task<Void, Never>?
    private let assetPageSize = 600

    init() {
        do {
            let support = try Self.applicationSupport()
            database = try SQLiteDatabase(path: support.appendingPathComponent("Library.sqlite"))
            scanner = PhotoScanner()
            try database.markInterruptedImportBatches()
            interruptedScanPath = try database.latestInterruptedScanPath()
            derivativeStorageURL = try database.derivativeStoragePath().map { URL(fileURLWithPath: $0, isDirectory: true) }
            sourceDirectories = try database.sourceDirectories()
            refresh()
            startAvailabilityRefreshInBackground()
        } catch {
            fatalError(error.fullTrace)
        }
    }

    deinit {
        availabilityTask?.cancel()
    }

    var selectedAsset: Asset? {
        assets.first { $0.id == selectedAssetID }
    }

    var isBusy: Bool {
        isScanning || blockingTask != nil
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
        scan(URL(fileURLWithPath: source.path), storageKind: source.storageKind)
    }

    func scanTrackedSources() {
        guard !isBusy else { return }
        scanSources(sourceDirectories)
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

    func moveSourceDirectory(_ source: SourceDirectory, to parent: SourceDirectory?) {
        guard !isBusy else { return }
        do {
            try database.moveSourceDirectory(id: source.id, parentID: parent?.id)
            sourceDirectories = try database.sourceDirectories()
        } catch {
            lastError = error.fullTrace
        }
    }

    func topLevelMoveTargets(excluding source: SourceDirectory) -> [SourceDirectory] {
        SourceDirectoryTreeBuilder
            .topLevelSources(in: sourceDirectories)
            .filter { $0.id != source.id }
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
            let page = try database.queryAssets(filter: filter, limit: assetPageSize + 1)
            assets = Array(page.prefix(assetPageSize))
            hasMoreAssets = page.count > assetPageSize
            sourceDirectories = try database.sourceDirectories()
            if selectedAssetID == nil || !assets.contains(where: { $0.id == selectedAssetID }) {
                selectedAssetID = assets.first?.id
            }
            loadSelectedFiles()
        } catch {
            lastError = error.fullTrace
        }
    }

    func refreshCounts() {
        do {
            counts = try database.countsByStatus()
        } catch {
            lastError = error.fullTrace
        }
    }

    func loadMoreAssets() {
        guard hasMoreAssets else { return }
        do {
            let page = try database.queryAssets(filter: filter, limit: assetPageSize + 1, offset: assets.count)
            assets.append(contentsOf: page.prefix(assetPageSize))
            hasMoreAssets = page.count > assetPageSize
        } catch {
            lastError = error.fullTrace
        }
    }

    func startAvailabilityRefreshInBackground() {
        guard availabilityTask == nil else { return }
        backgroundTask = BackgroundTaskReport(title: "后台任务", phase: "准备校验文件状态", message: "应用可以继续使用")
        availabilityTask = Task { [weak self] in
            guard let self else { return }
            do {
                refreshCounts()
                let targets = try database.availabilityCheckTargets()
                backgroundTask = BackgroundTaskReport(
                    title: "后台任务",
                    phase: targets.isEmpty ? "没有需要校验的文件" : "校验文件状态",
                    totalItems: targets.count,
                    message: "应用可以继续使用"
                )

                let batchSize = 250
                var completed = 0
                for batch in targets.chunked(size: batchSize) {
                    guard !Task.isCancelled else { return }
                    let updates = await Self.checkAvailability(batch)
                    try database.updateFileAvailability(updates)
                    completed += batch.count
                    backgroundTask = BackgroundTaskReport(
                        title: "后台任务",
                        phase: "校验文件状态",
                        currentPath: batch.last?.path ?? "",
                        totalItems: targets.count,
                        completedItems: completed,
                        message: "应用可以继续使用"
                    )
                    await Task.yield()
                }

                refresh()
                refreshCounts()
                backgroundTask = BackgroundTaskReport(
                    title: "后台任务",
                    phase: "文件状态校验完成",
                    totalItems: targets.count,
                    completedItems: targets.count,
                    message: "资产状态已更新",
                    isFinished: true
                )
            } catch {
                lastError = error.fullTrace
                backgroundTask = BackgroundTaskReport(
                    title: "后台任务",
                    phase: "文件状态校验失败",
                    message: error.localizedDescription,
                    isFinished: true
                )
            }
            availabilityTask = nil
        }
    }

    func setStatusFilter(_ status: AssetStatus?) {
        filter.status = status
        refresh()
    }

    func selectFolder(path: String) {
        do {
            let node = try database.upsertBrowseFolderNode(path: path, storageKind: storageKind(for: URL(fileURLWithPath: path, isDirectory: true)))
            filter.browseSelection = BrowseSelection(
                nodeID: node.id,
                kind: node.kind,
                path: node.displayPath,
                displayName: node.displayName,
                scope: filter.browseSelection?.scope ?? .recursive
            )
            refresh()
        } catch {
            lastError = error.fullTrace
        }
    }

    func clearBrowseSelection() {
        filter.browseSelection = nil
        refresh()
    }

    func setBrowseScope(_ scope: BrowseScope) {
        guard var selection = filter.browseSelection else { return }
        selection.scope = scope
        filter.browseSelection = selection
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
            for source in sources {
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
        blockingTask = BlockingTaskReport(title: "迁移缩略图", phase: "准备迁移", currentPath: destinationRoot.path)
        migrationReport = nil
        Task {
            await Task.yield()
            try? await Task.sleep(nanoseconds: 120_000_000)
            do {
                try FileManager.default.createDirectory(at: destinationRoot.appendingPathComponent("thumbnails", isDirectory: true), withIntermediateDirectories: true)
                let thumbnails = try database.thumbnailFileInstances()
                blockingTask?.totalItems = thumbnails.count
                blockingTask?.phase = thumbnails.isEmpty ? "没有可迁移缩略图" : "复制并校验"
                await Task.yield()
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
                        await Task.yield()
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
                    await Task.yield()
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
    }

    private func preferredNASRoot() -> URL? {
        if let nasRoot {
            return nasRoot
        }
        return sourceDirectories
            .filter { $0.storageKind == .nas }
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

    nonisolated private static func checkAvailability(_ targets: [AvailabilityCheckTarget]) async -> [FileAvailabilityUpdate] {
        await Task.detached(priority: .utility) {
            targets.map { target in
                FileAvailabilityUpdate(
                    id: target.id,
                    availability: FileManager.default.fileExists(atPath: target.path) ? .online : .missing
                )
            }
        }.value
    }
}

private extension Array {
    func chunked(size: Int) -> [[Element]] {
        precondition(size > 0, "chunk size must be positive")
        return stride(from: 0, to: count, by: size).map { start in
            Array(self[start..<Swift.min(start + size, count)])
        }
    }
}
