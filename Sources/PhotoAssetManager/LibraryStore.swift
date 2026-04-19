import AppKit
import Foundation
import SwiftUI

@MainActor
final class LibraryStore: ObservableObject {
    @Published var assets: [Asset] = []
    @Published var selectedAssetID: UUID?
    @Published var selectedAssetIDs: Set<UUID> = []
    @Published var selectedFiles: [FileInstance] = []
    @Published var filter = LibraryFilter()
    @Published var counts: [AssetStatus: Int] = [:]
    @Published var isScanning = false
    @Published var scanReport = ScanReport()
    @Published var lastError: String?
    @Published var nasRoot: URL?
    @Published var interruptedScanPath: String?
    @Published var sourceDirectories: [SourceDirectory] = []
    @Published var indexedBrowseFolders: [BrowseNode] = []
    @Published var derivativeStorageURL: URL?
    @Published var migrationReport: String?
    @Published var blockingTask: BlockingTaskReport?
    @Published var backgroundTask: BackgroundTaskReport?
    @Published var hasMoreAssets = false
    @Published var pendingBrowseSelection: BrowseSelection?

    private let databasePath: URL
    private let database: SQLiteDatabase
    private let scanner: PhotoScanner
    private let fileOperations = FileOperations()
    private let nasMountManager = NASMountManager()
    private var availabilityTask: Task<Void, Never>?
    private var startupOrganizationTask: Task<Void, Never>?
    private var folderSelectionTask: Task<Void, Never>?
    private var folderSelectionID: UUID?
    private var assetSelectionAnchorID: UUID?
    private var startupNASMountSucceeded = false
    private let assetPageSize = 96
    private let assetLoadAheadThreshold = 24
    private let availabilityRefreshInterval: TimeInterval = 24 * 60 * 60

    init() {
        do {
            let support = try Self.applicationSupport()
            let libraryDatabasePath = support.appendingPathComponent("Library.sqlite")
            databasePath = libraryDatabasePath
            database = try SQLiteDatabase(path: libraryDatabasePath)
            scanner = PhotoScanner()
            try database.markInterruptedImportBatches()
            try database.markInterruptedFolderMoveJobs()
            interruptedScanPath = try database.latestInterruptedScanPath()
            derivativeStorageURL = try database.derivativeStoragePath().map { URL(fileURLWithPath: $0, isDirectory: true) }
            sourceDirectories = try database.sourceDirectories()
            indexedBrowseFolders = try database.browseFolders()
            refresh()
            resumeInterruptedFolderMoveIfNeeded()
            startStartupLibraryOrganizationIfNeeded()
        } catch {
            fatalError(error.fullTrace)
        }
    }

    deinit {
        availabilityTask?.cancel()
        startupOrganizationTask?.cancel()
        folderSelectionTask?.cancel()
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

    func choosePhotoImportSource() -> URL? {
        guard !isBusy else { return nil }
        let panel = NSOpenPanel()
        panel.canChooseDirectories = true
        panel.canChooseFiles = false
        panel.allowsMultipleSelection = false
        panel.message = "选择要导入的库外照片文件夹"
        return panel.runModal() == .OK ? panel.url : nil
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
            indexedBrowseFolders = try database.browseFolders()
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
            indexedBrowseFolders = (try? database.browseFolders()) ?? indexedBrowseFolders
            refresh()
        }
    }

    func scanSource(_ source: SourceDirectory) {
        scan(URL(fileURLWithPath: source.path), storageKind: source.storageKind)
    }

    func scanTrackedSources() {
        guard !isBusy else { return }
        scanSources(sourceDirectories.filter(\.isTracked))
    }

    func removeSourceDirectory(_ source: SourceDirectory) {
        guard !isBusy else { return }
        do {
            try database.removeSourceDirectory(id: source.id)
            sourceDirectories = try database.sourceDirectories()
            indexedBrowseFolders = try database.browseFolders()
        } catch {
            lastError = error.fullTrace
        }
    }

    func removeFolder(_ source: FolderMoveSource, deleteEmptyFolder: Bool) {
        guard !isBusy else { return }
        if deleteEmptyFolder {
            trashFolderAfterEmptyScan(source)
            return
        }
        guard let sourceDirectoryID = source.sourceDirectoryID else {
            lastError = "只有已添加到资料库的文件夹可以仅移除。"
            return
        }
        do {
            try database.removeBrowseFolderTree(path: source.path)
            try database.removeSourceDirectory(id: sourceDirectoryID)
            clearBrowseSelectionIfNeeded(removedPath: source.path)
            finishFolderRemovalRefresh()
        } catch {
            lastError = error.fullTrace
        }
    }

    func moveSourceDirectory(_ source: SourceDirectory, to parent: SourceDirectory?) {
        guard !isBusy else { return }
        guard let parent else { return }
        startFolderMove(FolderMoveSource(source: source), destinationParentPath: parent.path, parentID: parent.id)
    }

    func moveSourceDirectory(_ source: SourceDirectory, to target: FolderMoveTarget) {
        guard !isBusy else { return }
        moveFolder(FolderMoveSource(source: source), to: target)
    }

    func moveFolder(_ source: FolderMoveSource, to target: FolderMoveTarget) {
        guard !isBusy else { return }
        startFolderMove(source, destinationParentPath: target.path, parentID: parentSourceID(for: target.path, excluding: source.sourceDirectoryID))
    }

    func availableFolderMoveTargets(for source: FolderMoveSource) -> [FolderMoveTarget] {
        SourceDirectoryTreeBuilder.moveTargets(
            for: source.path,
            sources: sourceDirectories,
            indexedBrowseFolders: indexedBrowseFolders
        )
    }

    func availablePhotoImportTargets() -> [PhotoImportTarget] {
        var targetsByPath: [String: PhotoImportTarget] = [:]
        for source in sourceDirectories {
            let path = Self.normalizedDirectoryPath(source.path)
            targetsByPath[path] = PhotoImportTarget(
                path: path,
                displayName: path,
                storageKind: source.storageKind
            )
        }
        for folder in indexedBrowseFolders {
            let path = Self.normalizedDirectoryPath(folder.displayPath)
            targetsByPath[path] = PhotoImportTarget(
                path: path,
                displayName: folder.displayName,
                storageKind: folder.storageKind
            )
        }
        return targetsByPath.values.sorted { $0.path.localizedStandardCompare($1.path) == .orderedAscending }
    }

    func importPhotoFolder(_ source: URL, to target: PhotoImportTarget) {
        guard !isBusy else { return }
        let sourcePath = Self.normalizedDirectoryPath(source.path)
        guard !isLibraryPath(sourcePath) else {
            lastError = "导入来源已经在资料库中：\(sourcePath)"
            return
        }
        availabilityTask?.cancel()
        availabilityTask = nil
        backgroundTask = nil
        blockingTask = BlockingTaskReport(
            title: "导入照片",
            phase: "准备导入",
            currentPath: sourcePath,
            message: "\(sourcePath) -> \(target.path)"
        )
        scanReport = ScanReport()
        lastError = nil

        let database = database
        let scanner = scanner
        let derivativeStorageURL = derivativeStorageURL
        Task.detached(priority: .userInitiated) {
            do {
                let plan = try FileOperations().buildPhotoImportPlan(source: source, destinationTarget: target)
                await MainActor.run {
                    self.blockingTask = BlockingTaskReport(
                        title: "导入照片",
                        phase: "复制并校验",
                        currentPath: sourcePath,
                        totalItems: plan.items.count,
                        message: "\(sourcePath) -> \(plan.destination.path)"
                    )
                }

                try await FileOperations().copyImportedFolder(destination: plan.destination, items: plan.items) { item, index in
                    await MainActor.run {
                        self.blockingTask = BlockingTaskReport(
                            title: "导入照片",
                            phase: "复制并校验",
                            currentPath: item.sourcePath,
                            totalItems: plan.items.count,
                            completedItems: index,
                            message: "\(item.sourcePath) -> \(item.destinationPath)"
                        )
                    }
                }

                await MainActor.run {
                    self.isScanning = true
                    self.blockingTask = BlockingTaskReport(
                        title: "导入照片",
                        phase: "扫描导入结果",
                        currentPath: plan.destination.path,
                        totalItems: plan.items.count,
                        completedItems: plan.items.count,
                        message: "正在把复制后的文件写入资料库。"
                    )
                }
                let report = await scanner.scanDirectory(
                    plan.destination,
                    storageKind: target.storageKind,
                    derivativeRoot: derivativeStorageURL,
                    database: database
                ) { report in
                    self.scanReport = report
                    self.blockingTask = BlockingTaskReport(
                        title: "导入照片",
                        phase: report.phase.isEmpty ? "扫描导入结果" : report.phase,
                        currentPath: report.currentPath.isEmpty ? plan.destination.path : report.currentPath,
                        totalItems: report.totalFiles > 0 ? report.totalFiles : plan.items.count,
                        completedItems: report.scannedFiles,
                        skippedItems: report.skippedFiles,
                        message: "新增 \(report.importedAssets)，位置更新 \(report.newLocations)"
                    )
                }

                try database.markSourceDirectoryScanned(path: target.path)
                let sourceDirectories = try database.sourceDirectories()
                let indexedBrowseFolders = try database.browseFolders()
                let interruptedScanPath = try database.latestInterruptedScanPath()
                await MainActor.run {
                    self.scanReport = report
                    self.isScanning = false
                    self.sourceDirectories = sourceDirectories
                    self.indexedBrowseFolders = indexedBrowseFolders
                    self.interruptedScanPath = interruptedScanPath
                    self.blockingTask = nil
                    if !report.errors.isEmpty {
                        self.lastError = report.errors.joined(separator: "\n\n")
                    }
                    self.refresh()
                    self.startAvailabilityRefreshInBackground()
                }
            } catch {
                await MainActor.run {
                    self.isScanning = false
                    self.blockingTask = nil
                    self.lastError = error.fullTrace
                }
            }
        }
    }

    func resumeInterruptedFolderMoveIfNeeded() {
        guard blockingTask == nil else { return }
        do {
            guard let job = try database.unfinishedFolderMoveJob() else { return }
            continueFolderMove(job, parentID: parentSourceID(for: job.destinationParentPath, excluding: job.sourceDirectoryID))
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
            indexedBrowseFolders = try database.browseFolders()
            if selectedAssetID == nil || !assets.contains(where: { $0.id == selectedAssetID }) {
                selectedAssetID = assets.first?.id
            }
            selectedAssetIDs.formIntersection(Set(assets.map(\.id)))
            if let selectedAssetID, selectedAssetIDs.isEmpty {
                selectedAssetIDs = [selectedAssetID]
            }
            if assetSelectionAnchorID == nil || !assets.contains(where: { $0.id == assetSelectionAnchorID }) {
                assetSelectionAnchorID = selectedAssetID
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

    func loadMoreAssetsIfNeeded(currentAssetID: UUID) {
        guard hasMoreAssets else { return }
        guard let currentIndex = assets.firstIndex(where: { $0.id == currentAssetID }) else { return }
        guard assets.distance(from: currentIndex, to: assets.endIndex) <= assetLoadAheadThreshold else { return }
        loadMoreAssets()
    }

    func selectAsset(_ asset: Asset, modifiers: EventModifiers) {
        let isRangeSelection = modifiers.contains(.shift)
        let isToggleSelection = modifiers.contains(.command)

        if isRangeSelection, let anchor = assetSelectionAnchorID,
           let anchorIndex = assets.firstIndex(where: { $0.id == anchor }),
           let selectedIndex = assets.firstIndex(where: { $0.id == asset.id }) {
            let bounds = min(anchorIndex, selectedIndex)...max(anchorIndex, selectedIndex)
            selectedAssetIDs = Set(assets[bounds].map(\.id))
        } else if isToggleSelection {
            if selectedAssetIDs.contains(asset.id), selectedAssetIDs.count > 1 {
                selectedAssetIDs.remove(asset.id)
            } else {
                selectedAssetIDs.insert(asset.id)
            }
            assetSelectionAnchorID = asset.id
        } else {
            selectedAssetIDs = [asset.id]
            assetSelectionAnchorID = asset.id
        }

        if selectedAssetIDs.contains(asset.id) {
            selectedAssetID = asset.id
        } else {
            selectedAssetID = selectedAssetIDs.sorted { $0.uuidString < $1.uuidString }.first
        }
        loadSelectedFiles()
    }

    func startAvailabilityRefreshInBackground(force: Bool = false) {
        guard availabilityTask == nil else { return }
        guard startupNASMountSucceeded else {
            backgroundTask = BackgroundTaskReport(
                title: "后台任务",
                phase: "等待 NAS 挂载",
                message: "启动挂载完成后再校验文件状态。",
                isFinished: true
            )
            return
        }
        backgroundTask = BackgroundTaskReport(title: "后台任务", phase: "准备校验文件状态", message: "应用可以继续使用")
        availabilityTask = Task { [weak self] in
            guard let self else { return }
            do {
                guard try shouldRunAvailabilityRefresh(force: force) else {
                    backgroundTask = BackgroundTaskReport(
                        title: "后台任务",
                        phase: "文件状态最近已校验",
                        message: "已跳过本次启动全量校验",
                        isFinished: true
                    )
                    availabilityTask = nil
                    return
                }
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

                try database.markAvailabilityRefreshCompleted(at: Date())
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

    func forceAvailabilityRefreshInBackground() {
        startAvailabilityRefreshInBackground(force: true)
    }

    private func shouldRunAvailabilityRefresh(force: Bool) throws -> Bool {
        if force { return true }
        guard let lastRefresh = try database.lastAvailabilityRefreshAt() else { return true }
        return Date().timeIntervalSince(lastRefresh) >= availabilityRefreshInterval
    }

    func sourcesNeedingStartupOrganization() throws -> [SourceDirectory] {
        let repairPaths = try database.sourceDirectoryPathsNeedingBrowseGraphRepair()
        return sourceDirectories.filter { source in
            source.isTracked && (source.lastScannedAt == nil || repairPaths.contains(source.path))
        }
    }

    func startStartupLibraryOrganizationIfNeeded() {
        guard blockingTask == nil else { return }
        guard startupOrganizationTask == nil else { return }
        let sources: [SourceDirectory]
        do {
            sources = try sourcesNeedingStartupOrganization()
        } catch {
            lastError = error.fullTrace
            return
        }

        isScanning = true
        scanReport = ScanReport()
        lastError = nil
        blockingTask = BlockingTaskReport(
            title: "系统整理中",
            phase: "挂载 NAS 来源",
            totalItems: max(sourceDirectories.filter { $0.storageKind == .nas }.count, sources.count),
            message: "正在挂载已登记的 NAS 照片来源。"
        )

        startupOrganizationTask = Task { [weak self] in
            guard let self else { return }
            var completedSources = 0
            var scanErrors: [String] = []
            var shouldClearBlockingTask = true
            defer {
                isScanning = false
                if shouldClearBlockingTask {
                    blockingTask = nil
                }
                startupOrganizationTask = nil
            }

            do {
                let mountReport = await mountNASRootsAtStartup()
                guard !mountReport.hasFailures else {
                    shouldClearBlockingTask = false
                    blockingTask = BlockingTaskReport(
                        title: "系统整理中",
                        phase: "NAS 挂载未完成",
                        totalItems: mountReport.checkedRootCount,
                        completedItems: mountReport.alreadyAvailableRoots.count + mountReport.mountedRoots.count,
                        message: "请确认 NAS 可访问后重启应用，暂不扫描或校验文件状态。"
                    )
                    startupNASMountSucceeded = false
                    return
                }
                startupNASMountSucceeded = true

                guard !sources.isEmpty else {
                    refresh()
                    startAvailabilityRefreshInBackground()
                    return
                }

                try database.backfillBrowseGraphFromFileInstances()
                sourceDirectories = try database.sourceDirectories()
                indexedBrowseFolders = try database.browseFolders()

                for source in sources {
                    guard !Task.isCancelled else { throw CancellationError() }
                    let completedBeforeSource = completedSources
                    let sourceURL = URL(fileURLWithPath: source.path, isDirectory: true)
                    blockingTask = BlockingTaskReport(
                        title: "系统整理中",
                        phase: "正在整理照片索引",
                        currentPath: source.path,
                        totalItems: sources.count,
                        completedItems: completedBeforeSource,
                        message: "正在整理 \(source.path)"
                    )

                    let report = await scanner.scanDirectory(
                        sourceURL,
                        storageKind: source.storageKind,
                        derivativeRoot: derivativeStorageURL,
                        database: database
                    ) { [weak self] report in
                        self?.scanReport = report
                        self?.blockingTask = BlockingTaskReport(
                            title: "系统整理中",
                            phase: report.phase.isEmpty ? "正在整理照片索引" : report.phase,
                            currentPath: report.currentPath.isEmpty ? source.path : report.currentPath,
                            totalItems: sources.count,
                            completedItems: completedBeforeSource,
                            skippedItems: report.skippedFiles,
                            message: self?.startupOrganizationMessage(sourcePath: source.path, report: report) ?? "正在整理 \(source.path)"
                        )
                    }

                    scanReport = report
                    scanErrors.append(contentsOf: report.errors)
                    try database.markSourceDirectoryScanned(path: source.path)
                    try database.clearInterruptedBatches(sourcePath: source.path)
                    completedSources += 1
                    sourceDirectories = try database.sourceDirectories()
                    indexedBrowseFolders = try database.browseFolders()
                    blockingTask?.completedItems = completedSources
                    await Task.yield()
                }

                interruptedScanPath = try database.latestInterruptedScanPath()
                try database.backfillBrowseGraphFromFileInstances()
                sourceDirectories = try database.sourceDirectories()
                indexedBrowseFolders = try database.browseFolders()
                if !scanErrors.isEmpty {
                    lastError = scanErrors.joined(separator: "\n\n")
                }
                refresh()
                startAvailabilityRefreshInBackground()
            } catch is CancellationError {
            } catch {
                lastError = error.fullTrace
            }
        }
    }

    private func mountNASRootsAtStartup() async -> NASMountReport {
        blockingTask = BlockingTaskReport(
            title: "系统整理中",
            phase: "挂载 NAS 来源",
            totalItems: sourceDirectories.filter { $0.storageKind == .nas }.count + (derivativeStorageURL == nil ? 0 : 1),
            message: "正在挂载已登记的 NAS 照片来源。"
        )
        let report = await nasMountManager.mountNASRootsIfNeeded(for: sourceDirectories, derivativeStorageURL: derivativeStorageURL)
        blockingTask = BlockingTaskReport(
            title: "系统整理中",
            phase: report.hasFailures ? "NAS 挂载未完成" : "NAS 挂载完成",
            totalItems: report.checkedRootCount,
            completedItems: report.alreadyAvailableRoots.count + report.mountedRoots.count,
            message: report.hasFailures ? "仍有 NAS 来源不可访问。" : "NAS 来源已可访问，继续启动整理。"
        )
        return report
    }

    private func startupOrganizationMessage(sourcePath: String, report: ScanReport) -> String {
        if report.totalFiles > 0 {
            return "正在整理 \(sourcePath)，已扫描 \(report.scannedFiles) / \(report.totalFiles) 个候选文件。"
        }
        if report.discoveredFiles > 0 {
            return "正在整理 \(sourcePath)，已发现 \(report.discoveredFiles) 个候选文件。"
        }
        return "正在整理 \(sourcePath)"
    }

    func setStatusFilter(_ status: AssetStatus?) {
        filter.status = status
        refresh()
    }

    func setMinimumRatingFilter(_ rating: Int) {
        filter.minimumRating = max(0, min(5, rating))
        refresh()
    }

    func setFlaggedOnlyFilter(_ flaggedOnly: Bool) {
        filter.flaggedOnly = flaggedOnly
        refresh()
    }

    func toggleColorLabelFilter(_ colorLabel: AssetColorLabel) {
        if filter.colorLabels.contains(colorLabel) {
            filter.colorLabels.remove(colorLabel)
        } else {
            filter.colorLabels.insert(colorLabel)
        }
        refresh()
    }

    func setSortOrder(_ sortOrder: LibrarySortOrder) {
        filter.sortOrder = sortOrder
        refresh()
    }

    func selectFolder(path: String) {
        let normalizedPath = Self.normalizedDirectoryPath(path)
        PerformanceLog.event("folder-selection-click", detail: normalizedPath)
        let selectionID = UUID()
        let selection = BrowseSelection(
            nodeID: UUID(),
            kind: .folder,
            path: normalizedPath,
            displayName: URL(fileURLWithPath: normalizedPath, isDirectory: true).lastPathComponent,
            scope: filter.browseSelection?.scope ?? .recursive
        )
        filter.browseSelection = selection
        pendingBrowseSelection = selection
        folderSelectionID = selectionID
        blockingTask = BlockingTaskReport(
            title: "正在打开文件夹",
            phase: "正在打开",
            currentPath: normalizedPath,
            message: "已选中该文件夹，正在加载照片。"
        )
        assets = []
        selectedAssetID = nil
        selectedAssetIDs = []
        assetSelectionAnchorID = nil
        selectedFiles = []
        hasMoreAssets = false
        lastError = nil

        folderSelectionTask?.cancel()
        folderSelectionTask = Task(priority: .userInitiated) { [weak self] in
            await Task.yield()
            guard !Task.isCancelled else { return }
            await self?.finishSelectingFolder(path: normalizedPath, scope: selection.scope, selectionID: selectionID)
        }
    }

    func clearBrowseSelection() {
        folderSelectionTask?.cancel()
        folderSelectionID = nil
        pendingBrowseSelection = nil
        filter.browseSelection = nil
        refresh()
    }

    func setBrowseScope(_ scope: BrowseScope) {
        guard var selection = filter.browseSelection else { return }
        selection.scope = scope
        filter.browseSelection = selection
        pendingBrowseSelection = selection
        refresh()
        pendingBrowseSelection = nil
    }

    private func finishSelectingFolder(path: String, scope: BrowseScope, selectionID: UUID) async {
        defer {
            if folderSelectionID == selectionID {
                pendingBrowseSelection = nil
                blockingTask = nil
                folderSelectionTask = nil
                folderSelectionID = nil
            }
        }

        do {
            let databasePath = databasePath
            let filterSnapshot = filter
            let assetPageSize = assetPageSize
            let result = try await Task.detached(priority: .userInitiated) { () throws -> FolderSelectionLoadResult in
                try Task.checkCancellation()
                return try PerformanceLog.measure("folder-selection-load") {
                    let readDatabase = try SQLiteDatabase(path: databasePath, migrateSchema: false, readOnly: true)
                    let normalizedPath = SourceDirectoryTreeBuilder.normalizedDirectoryPath(path)
                    let node = try readDatabase.browseFolder(path: normalizedPath)
                    let selection = BrowseSelection(
                        nodeID: node?.id ?? UUID(),
                        kind: node?.kind ?? .folder,
                        path: node?.displayPath ?? normalizedPath,
                        displayName: node?.displayName ?? URL(fileURLWithPath: normalizedPath, isDirectory: true).lastPathComponent,
                        scope: scope
                    )
                    var filtered = filterSnapshot
                    filtered.browseSelection = selection
                    let page = try readDatabase.queryAssets(filter: filtered, limit: assetPageSize + 1)
                    let assets = Array(page.prefix(assetPageSize))
                    let selectedAssetID = assets.first?.id
                    let selectedFiles = try selectedAssetID.map { try readDatabase.fileInstances(assetID: $0) } ?? []
                    return FolderSelectionLoadResult(
                        selection: selection,
                        assets: assets,
                        selectedAssetID: selectedAssetID,
                        selectedFiles: selectedFiles,
                        hasMoreAssets: page.count > assetPageSize
                    )
                }
            }.value
            guard folderSelectionID == selectionID else { return }
            filter.browseSelection = result.selection
            assets = result.assets
            selectedAssetID = result.selectedAssetID
            selectedAssetIDs = result.selectedAssetID.map { [$0] } ?? []
            assetSelectionAnchorID = result.selectedAssetID
            selectedFiles = result.selectedFiles
            hasMoreAssets = result.hasMoreAssets
        } catch is CancellationError {
        } catch {
            guard folderSelectionID == selectionID else { return }
            lastError = error.fullTrace
        }
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

    func moveAssets(_ assetIDs: [UUID], to target: FolderMoveTarget) {
        guard !isBusy else { return }
        let assetIDs = Array(Set(assetIDs)).sorted { $0.uuidString < $1.uuidString }
        guard !assetIDs.isEmpty else { return }
        availabilityTask?.cancel()
        availabilityTask = nil
        backgroundTask = nil
        blockingTask = BlockingTaskReport(
            title: "移动文件",
            phase: "准备移动",
            currentPath: target.path,
            totalItems: assetIDs.count,
            message: "准备移动 \(assetIDs.count) 个资产的文件到 \(target.path)"
        )
        lastError = nil

        let database = database
        Task.detached(priority: .userInitiated) {
            do {
                let plan = try FileOperations().buildAssetFileMovePlan(
                    assetIDs: assetIDs,
                    destinationTarget: target,
                    database: database
                )
                await MainActor.run {
                    self.blockingTask = BlockingTaskReport(
                        title: "移动文件",
                        phase: "复制、校验并删除源文件",
                        currentPath: target.path,
                        totalItems: plan.count,
                        message: "移动 \(plan.count) 个文件到 \(target.path)"
                    )
                }
                try await FileOperations().moveAssetFiles(items: plan, database: database) { item, index in
                    await MainActor.run {
                        self.blockingTask = BlockingTaskReport(
                            title: "移动文件",
                            phase: "复制、校验并删除源文件",
                            currentPath: item.sourcePath,
                            totalItems: plan.count,
                            completedItems: index,
                            message: "\(item.sourcePath) -> \(item.destinationPath)"
                        )
                    }
                }
                let sourceDirectories = try database.sourceDirectories()
                let indexedBrowseFolders = try database.browseFolders()
                await MainActor.run {
                    self.sourceDirectories = sourceDirectories
                    self.indexedBrowseFolders = indexedBrowseFolders
                    self.blockingTask = nil
                    self.refresh()
                    self.startAvailabilityRefreshInBackground()
                }
            } catch {
                await MainActor.run {
                    self.blockingTask = nil
                    self.lastError = error.fullTrace
                }
            }
        }
    }

    func deleteAssets(_ assetIDs: [UUID]) {
        guard !isBusy else { return }
        let assetIDs = Array(Set(assetIDs)).sorted { $0.uuidString < $1.uuidString }
        guard !assetIDs.isEmpty else { return }
        availabilityTask?.cancel()
        availabilityTask = nil
        backgroundTask = nil
        blockingTask = BlockingTaskReport(
            title: "删除照片",
            phase: "准备删除",
            totalItems: assetIDs.count,
            message: "准备删除 \(assetIDs.count) 个资产的在线文件"
        )
        lastError = nil

        let database = database
        Task.detached(priority: .userInitiated) {
            do {
                let files = try database.deletableFileInstances(assetIDs: assetIDs)
                let visibleDeletionFiles = files.filter { $0.fileRole != .thumbnail }
                let visibleDeletionFileOffsets = Dictionary(uniqueKeysWithValues: visibleDeletionFiles.enumerated().map { ($0.element.id, $0.offset) })
                await MainActor.run {
                    self.blockingTask = BlockingTaskReport(
                        title: "删除照片",
                        phase: "移入废纸篓或文件系统删除",
                        totalItems: visibleDeletionFiles.count,
                        message: visibleDeletionFiles.isEmpty ? "后台清理缩略图" : "删除 \(visibleDeletionFiles.count) 个在线文件，缩略图后台清理"
                    )
                }
                try await FileOperations().deleteAssetFiles(files: files, database: database) { file, index in
                    guard file.fileRole != .thumbnail else { return }
                    await MainActor.run {
                        self.blockingTask = BlockingTaskReport(
                            title: "删除照片",
                            phase: "移入废纸篓或文件系统删除",
                            currentPath: file.path,
                            totalItems: visibleDeletionFiles.count,
                            completedItems: visibleDeletionFileOffsets[file.id] ?? index,
                            message: file.fileRole.label
                        )
                    }
                }
                let sourceDirectories = try database.sourceDirectories()
                let indexedBrowseFolders = try database.browseFolders()
                await MainActor.run {
                    self.sourceDirectories = sourceDirectories
                    self.indexedBrowseFolders = indexedBrowseFolders
                    self.blockingTask = nil
                    self.refresh()
                    self.startAvailabilityRefreshInBackground()
                }
            } catch {
                await MainActor.run {
                    self.blockingTask = nil
                    self.lastError = error.fullTrace
                }
            }
        }
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

    private func startFolderMove(_ source: FolderMoveSource, destinationParentPath: String, parentID: UUID?) {
        var source = source
        if source.sourceDirectoryID == nil {
            source.sourceDirectoryID = parentSourceID(for: source.path, excluding: nil)
        }
        blockingTask = BlockingTaskReport(
            title: "移动文件夹",
            phase: "准备移动",
            currentPath: source.path,
            message: "\(source.path) -> \(destinationParentPath)"
        )
        lastError = nil

        let database = database
        Task.detached(priority: .userInitiated) { [source, destinationParentPath, parentID, database] in
            do {
                let destinationParent = URL(fileURLWithPath: destinationParentPath, isDirectory: true)
                let plan = try FileOperations().buildFolderMovePlan(source: source, destinationParent: destinationParent, database: database)
                let job = try database.createFolderMoveJob(
                    source: source,
                    destinationParentPath: destinationParentPath,
                    destinationPath: plan.destination.path,
                    items: plan.items
                )
                await MainActor.run {
                    self.continueFolderMove(job, parentID: parentID)
                }
            } catch {
                await MainActor.run {
                    self.blockingTask = nil
                    self.lastError = error.fullTrace
                }
            }
        }
    }

    private func trashFolderAfterEmptyScan(_ source: FolderMoveSource) {
        blockingTask = BlockingTaskReport(
            title: "彻底删除文件夹",
            phase: "扫描文件夹",
            currentPath: source.path,
            message: "正在确认文件夹内没有任何文件。"
        )
        lastError = nil

        let database = database
        Task.detached(priority: .userInitiated) { [source, database] in
            do {
                try FileOperations().deleteEmptyFolderTree(at: URL(fileURLWithPath: source.path, isDirectory: true), storageKind: source.storageKind)
                try database.removeBrowseFolderTree(path: source.path)
                if let sourceDirectoryID = source.sourceDirectoryID {
                    try database.removeSourceDirectory(id: sourceDirectoryID)
                }
                let sourceDirectories = try database.sourceDirectories()
                let indexedBrowseFolders = try database.browseFolders()
                await MainActor.run {
                    self.sourceDirectories = sourceDirectories
                    self.indexedBrowseFolders = indexedBrowseFolders
                    self.blockingTask = nil
                    self.clearBrowseSelectionIfNeeded(removedPath: source.path)
                    self.refresh()
                }
            } catch {
                await MainActor.run {
                    self.blockingTask = nil
                    self.lastError = error.fullTrace
                }
            }
        }
    }

    private func finishFolderRemovalRefresh() {
        sourceDirectories = (try? database.sourceDirectories()) ?? sourceDirectories
        indexedBrowseFolders = (try? database.browseFolders()) ?? indexedBrowseFolders
        refresh()
    }

    private func clearBrowseSelectionIfNeeded(removedPath: String) {
        let normalizedPath = Self.normalizedDirectoryPath(removedPath)
        if let selection = filter.browseSelection {
            let selectedPath = Self.normalizedDirectoryPath(selection.path)
            if selectedPath == normalizedPath || selectedPath.hasPrefix(normalizedPath + "/") {
                clearBrowseSelection()
            }
        }
    }

    private func continueFolderMove(_ job: FolderMoveJob, parentID: UUID?) {
        availabilityTask?.cancel()
        availabilityTask = nil
        backgroundTask = nil
        blockingTask = BlockingTaskReport(
            title: "移动文件夹",
            phase: "准备移动",
            currentPath: job.sourcePath,
            totalItems: job.totalFiles,
            completedItems: job.completedFiles,
            message: "\(job.sourcePath) -> \(job.destinationPath)"
        )
        lastError = nil

        let database = database
        Task.detached(priority: .userInitiated) {
            do {
                try await FileOperations().moveFolder(job: job, database: database) { job, item in
                    let pending = (try? database.pendingFolderMoveItems(jobID: job.id).count) ?? job.totalFiles
                    let completed = max(0, job.totalFiles - pending)
                    await MainActor.run {
                        self.blockingTask = BlockingTaskReport(
                            title: "移动文件夹",
                            phase: "复制、校验并删除源文件",
                            currentPath: item.sourcePath,
                            totalItems: job.totalFiles,
                            completedItems: min(completed, job.totalFiles),
                            message: "\(item.sourcePath) -> \(item.destinationPath)"
                        )
                    }
                }
                await MainActor.run {
                    self.blockingTask = BlockingTaskReport(
                        title: "移动文件夹",
                        phase: "更新索引",
                        currentPath: job.destinationPath,
                        totalItems: job.totalFiles,
                        completedItems: job.totalFiles,
                        message: "\(job.sourcePath) -> \(job.destinationPath)"
                    )
                }
                try database.rewriteFolderMovePaths(job: job, parentID: parentID)
                let sourceDirectories = try database.sourceDirectories()
                let indexedBrowseFolders = try database.browseFolders()
                let interruptedScanPath = try database.latestInterruptedScanPath()
                await MainActor.run {
                    self.sourceDirectories = sourceDirectories
                    self.indexedBrowseFolders = indexedBrowseFolders
                    self.interruptedScanPath = interruptedScanPath
                    self.blockingTask = nil
                    self.refresh()
                    self.startStartupLibraryOrganizationIfNeeded()
                }
            } catch {
                try? database.failFolderMoveJob(id: job.id, error: error)
                await MainActor.run {
                    self.blockingTask = nil
                    self.lastError = error.fullTrace
                }
            }
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

    private func parentSourceID(for path: String, excluding sourceID: UUID?) -> UUID? {
        let normalizedPath = Self.normalizedDirectoryPath(path)
        return sourceDirectories
            .filter { sourceID == nil || $0.id != sourceID }
            .filter { source in
                let sourcePath = Self.normalizedDirectoryPath(source.path)
                return normalizedPath == sourcePath || normalizedPath.hasPrefix(sourcePath + "/")
            }
            .max { $0.path.count < $1.path.count }?
            .id
    }

    private func isLibraryPath(_ path: String) -> Bool {
        let normalizedPath = Self.normalizedDirectoryPath(path)
        return sourceDirectories.contains { source in
            let sourcePath = Self.normalizedDirectoryPath(source.path)
            return normalizedPath == sourcePath || normalizedPath.hasPrefix(sourcePath + "/")
        }
    }

    private static func applicationSupport() throws -> URL {
        let root = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("PhotoAssetManager", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        return root
    }

    private static func normalizedDirectoryPath(_ path: String) -> String {
        guard path.count > 1 else { return path }
        return path.hasSuffix("/") ? String(path.dropLast()) : path
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

struct FolderSelectionLoadResult: Sendable {
    var selection: BrowseSelection
    var assets: [Asset]
    var selectedAssetID: UUID?
    var selectedFiles: [FileInstance]
    var hasMoreAssets: Bool
}
