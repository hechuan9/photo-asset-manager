import Foundation
import Testing

struct StartupPerformanceTests {
    @Test func libraryStartupSchedulesAvailabilityCheckWithoutBlockingRefresh() throws {
        let source = try sourceFile("Sources/PhotoAssetManager/LibraryStore.swift")

        #expect(source.contains("startAvailabilityRefreshInBackground()"))
        #expect(!functionBody(named: "refresh", in: source).contains("markMissingFiles"))
        #expect(source.contains("backgroundTask"))
    }

    @Test func assetQueriesDeriveStatusWithoutPerAssetFileLookups() throws {
        let source = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")

        #expect(source.contains("func countsByStatus() throws -> [AssetStatus: Int]"))
        #expect(!functionBody(named: "countsByStatus", in: source).contains("queryAssets"))
        #expect(!source.contains("return asset.withStatus(derivedStatus(for: asset.id))"))
        #expect(!source.contains("private func derivedStatus(for assetID: UUID)"))
    }

    @Test func contentViewHasBottomBackgroundTaskBar() throws {
        let source = try sourceFile("Sources/PhotoAssetManager/ContentView.swift")

        #expect(source.contains("BackgroundTaskBar()"))
        #expect(source.contains(".safeAreaInset(edge: .bottom)"))
    }

    @Test func startupRefreshUsesPagedAssetLoading() throws {
        let store = try sourceFile("Sources/PhotoAssetManager/LibraryStore.swift")
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")
        let content = try sourceFile("Sources/PhotoAssetManager/ContentView.swift")

        #expect(database.contains("func queryAssets(filter: LibraryFilter, limit: Int, offset: Int = 0) throws -> [Asset]"))
        #expect(database.contains("LIMIT ? OFFSET ?"))
        #expect(database.contains("WITH page AS"))
        #expect(database.contains("idx_assets_sort_time"))
        #expect(functionBody(named: "refresh", in: store).contains("assetPageSize + 1"))
        #expect(!functionBody(named: "refresh", in: store).contains("countsByStatus"))
        #expect(store.contains("func refreshCounts()"))
        #expect(store.contains("func loadMoreAssets()"))
        #expect(store.contains("private let assetPageSize = 96"))
        #expect(store.contains("func loadMoreAssetsIfNeeded(currentAssetID: UUID)"))
        #expect(content.contains(".onAppear"))
        #expect(content.contains("loadMore: { assetID in"))
        #expect(content.contains("library.loadMoreAssetsIfNeeded(currentAssetID: assetID)"))
        #expect(!content.contains("Button(\"加载更多\")"))
    }

    @Test func filteredAssetQueriesPageBeforeAggregatingFileInstances() throws {
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")
        let queryAssetsBody = functionBody(named: "queryAssets", in: database)

        #expect(queryAssetsBody.contains("WITH matching_assets AS"))
        #expect(queryAssetsBody.contains("page AS ("))
        #expect(queryAssetsBody.contains("JOIN matching_assets ma ON ma.asset_id = a.id"))
        #expect(queryAssetsBody.contains("FROM page p"))
        #expect(queryAssetsBody.contains("LEFT JOIN file_instances fi ON fi.asset_id = a.id"))
    }

    @Test func folderSelectionReadsAssetsOffMainActorWithTimingLogs() throws {
        let store = try sourceFile("Sources/PhotoAssetManager/LibraryStore.swift")
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")
        let selectFolderBody = functionBody(named: "selectFolder", in: store)
        let finishSelectingFolderBody = functionBody(named: "finishSelectingFolder", in: store)

        #expect(database.contains("final class SQLiteDatabase: @unchecked Sendable"))
        #expect(database.contains("readOnly: Bool = false"))
        #expect(store.contains("private let databasePath: URL"))
        #expect(store.contains("struct FolderSelectionLoadResult: Sendable"))
        #expect(selectFolderBody.contains("Task(priority: .userInitiated)"))
        #expect(finishSelectingFolderBody.contains("Task.detached(priority: .userInitiated)"))
        #expect(finishSelectingFolderBody.contains("SQLiteDatabase(path: databasePath, migrateSchema: false, readOnly: true)"))
        #expect(finishSelectingFolderBody.contains("PerformanceLog.measure"))
    }

    @Test func performanceLogsArePersistentAndIncludeFolderClickBoundary() throws {
        let log = try sourceFile("Sources/PhotoAssetManager/PerformanceLog.swift")
        let store = try sourceFile("Sources/PhotoAssetManager/LibraryStore.swift")
        let selectFolderBody = functionBody(named: "selectFolder", in: store)

        #expect(log.contains("logger.notice"))
        #expect(selectFolderBody.contains("PerformanceLog.event(\"folder-selection-click\""))
    }

    @Test func browseGraphPersistsFolderMembershipAndFiltersAssetQueries() throws {
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")

        #expect(database.contains("CREATE TABLE IF NOT EXISTS browse_nodes"))
        #expect(database.contains("CREATE TABLE IF NOT EXISTS browse_edges"))
        #expect(database.contains("CREATE TABLE IF NOT EXISTS browse_file_instances"))
        #expect(database.contains("UNIQUE(kind, canonical_key)"))
        #expect(database.contains("WITH RECURSIVE selected_browse_nodes"))
        #expect(database.contains("browse_file_instances bfi"))
        #expect(database.contains("func upsertBrowseFolderMembership(filePath: String, fileInstanceID: UUID, storageKind: StorageKind)"))
        #expect(database.contains("func backfillBrowseGraphFromFileInstances()"))
        #expect(database.contains("try backfillBrowseGraphFromFileInstances()"))
        #expect(database.contains("func browseNodeIDs(selection: BrowseSelection) throws -> [UUID]"))
        #expect(database.contains("upsertBrowseFolderMembership(filePath: scanned.url.path, fileInstanceID: fileID, storageKind: scanned.storageKind)"))
        #expect(database.contains("BrowseScope.direct"))
        #expect(database.contains("BrowseScope.recursive"))
    }

    @Test func databaseMigrationDoesNotRestoreInterruptedScanRootsAsSources() throws {
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")

        #expect(database.contains("WHERE status IN ('finished', 'finished_with_errors', 'resumed')"))
        #expect(!database.contains("CASE WHEN status IN ('finished', 'finished_with_errors', 'resumed') THEN imported_at ELSE NULL END"))
    }

    @Test func scannerSkipsRecycleDirectoriesByPathComponent() throws {
        let scanner = try sourceFile("Sources/PhotoAssetManager/PhotoScanner.swift")

        #expect(scanner.contains("private let skippedDirectoryNames"))
        #expect(scanner.contains("url.pathComponents.contains"))
        #expect(scanner.contains("\"#recycle\""))
        #expect(functionBody(named: "shouldSkipDirectory", in: scanner).contains("skippedDirectoryNames.contains"))
    }

    @Test func startupOrganizesUnscannedSourcesBehindBlockingDialog() throws {
        let store = try sourceFile("Sources/PhotoAssetManager/LibraryStore.swift")

        #expect(store.contains("startStartupLibraryOrganizationIfNeeded()"))
        #expect(store.contains("func sourcesNeedingStartupOrganization() throws"))
        #expect(store.contains("func startStartupLibraryOrganizationIfNeeded()"))
        #expect(store.contains("系统整理中"))
        #expect(store.contains("正在整理照片索引"))
        #expect(functionBody(named: "sourcesNeedingStartupOrganization", in: store).contains("lastScannedAt == nil"))
        #expect(functionBody(named: "sourcesNeedingStartupOrganization", in: store).contains("sourceDirectoryPathsNeedingBrowseGraphRepair"))
        #expect(functionBody(named: "startStartupLibraryOrganizationIfNeeded", in: store).contains("blockingTask = BlockingTaskReport"))
        #expect(functionBody(named: "startStartupLibraryOrganizationIfNeeded", in: store).contains("database.backfillBrowseGraphFromFileInstances()"))
        #expect(functionBody(named: "startStartupLibraryOrganizationIfNeeded", in: store).contains("scanner.scanDirectory"))
        #expect(functionBody(named: "startStartupLibraryOrganizationIfNeeded", in: store).contains("database.markSourceDirectoryScanned"))
        #expect(functionBody(named: "startStartupLibraryOrganizationIfNeeded", in: store).contains("indexedBrowseFolders = try database.browseFolders()"))
        #expect(functionBody(named: "startupOrganizationMessage", in: store).contains("已扫描"))
        #expect(functionBody(named: "startupOrganizationMessage", in: store).contains("候选文件"))
    }

    @Test func startupOrganizationRepairsIndexedSourcesMissingBrowseGraph() throws {
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")

        #expect(database.contains("func sourceDirectoryPathsNeedingBrowseGraphRepair() throws -> Set<String>"))
        #expect(functionBody(named: "sourceDirectoryPathsNeedingBrowseGraphRepair", in: database).contains("source_directories sd"))
        #expect(functionBody(named: "sourceDirectoryPathsNeedingBrowseGraphRepair", in: database).contains("EXISTS"))
        #expect(functionBody(named: "sourceDirectoryPathsNeedingBrowseGraphRepair", in: database).contains("file_instances fi"))
        #expect(functionBody(named: "sourceDirectoryPathsNeedingBrowseGraphRepair", in: database).contains("browse_nodes bn"))
        #expect(functionBody(named: "sourceDirectoryPathsNeedingBrowseGraphRepair", in: database).contains("browse_file_instances bfi"))
        #expect(functionBody(named: "sourceDirectoryPathsNeedingBrowseGraphRepair", in: database).contains("bn.id IS NULL"))
        #expect(functionBody(named: "sourceDirectoryPathsNeedingBrowseGraphRepair", in: database).contains("bfi.file_instance_id IS NULL"))
        #expect(database.contains("func backfillBrowseGraphFromFileInstances() throws"))
    }

    @Test func importBatchBackfillDoesNotCreateNestedTopLevelSources() throws {
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")

        #expect(database.contains("func pruneNestedImportBatchSourceDirectories() throws"))
        #expect(functionBody(named: "migrate", in: database).contains("try pruneNestedImportBatchSourceDirectories()"))
        #expect(database.contains("NOT EXISTS ("))
        #expect(database.contains("ancestor.path || '/%'"))
        #expect(functionBody(named: "pruneNestedImportBatchSourceDirectories", in: database).contains("DELETE FROM source_directories"))
        #expect(functionBody(named: "pruneNestedImportBatchSourceDirectories", in: database).contains("import_batches ib"))
    }

    @Test func rescanningUnchangedFilesRepairsBrowseMembership() throws {
        let scanner = try sourceFile("Sources/PhotoAssetManager/PhotoScanner.swift")
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")

        #expect(database.contains("func unchangedFileInstanceID(path: String, sizeBytes: Int64) throws -> UUID?"))
        #expect(scanner.contains("let unchangedFileInstanceID = try await MainActor.run"))
        #expect(functionBody(named: "scanDirectory", in: scanner).contains("database.upsertBrowseFolderMembership(filePath: url.path, fileInstanceID: unchangedFileInstanceID, storageKind: storageKind)"))
        #expect(!scanner.contains("try database.hasUnchangedFileInstance(path: url.path, sizeBytes: size)"))
    }

    @Test func scannerRegistersSameBasenameSidecarsWithOriginalAsset() throws {
        let scanner = try sourceFile("Sources/PhotoAssetManager/PhotoScanner.swift")
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")
        let scannedFile = structBody(named: "ScannedFile", in: scanner)
        let scanFileBody = functionBody(named: "scanFile", in: scanner)
        let upsertBody = functionBody(named: "upsertScannedFile", in: database)

        #expect(scanner.contains("struct ScannedSidecar"))
        #expect(scannedFile.contains("var sidecars: [ScannedSidecar]"))
        #expect(scanner.contains("func scanSidecars(for url: URL, storageKind: StorageKind) throws -> [ScannedSidecar]"))
        #expect(scanFileBody.contains("sidecars: try scanSidecars(for: url, storageKind: storageKind)"))
        #expect(database.contains("func upsertScannedSidecar"))
        #expect(upsertBody.contains("for sidecar in scanned.sidecars"))
        #expect(upsertBody.contains("try upsertScannedSidecar(sidecar, assetID: assetID)"))
        #expect(functionBody(named: "upsertScannedSidecar", in: database).contains(".text(FileRole.sidecar.rawValue)"))
        #expect(functionBody(named: "upsertScannedSidecar", in: database).contains("try upsertBrowseFolderMembership(filePath: sidecar.url.path"))
    }

    @Test func scannerPublishesProgressAfterEveryScannedPhoto() throws {
        let scanner = try sourceFile("Sources/PhotoAssetManager/PhotoScanner.swift")
        let scanDirectoryBody = functionBody(named: "scanDirectory", in: scanner)

        #expect(scanner.contains("func publishScanProgress"))
        #expect(scanDirectoryBody.contains("report.scannedFiles += 1\n                        await publishScanProgress(report, progress: progress)"))
        #expect(scanDirectoryBody.components(separatedBy: "await publishScanProgress(report, progress: progress)").count - 1 == 2)
        #expect(!scanDirectoryBody.contains("report.scannedFiles % 3"))
        #expect(!scanDirectoryBody.contains("report.scannedFiles % 25"))
    }

    @Test func thumbnailWriteFailuresDoNotFailPhotoScan() throws {
        let scanner = try sourceFile("Sources/PhotoAssetManager/PhotoScanner.swift")
        let scanFileBody = functionBody(named: "scanFile", in: scanner)

        #expect(scanFileBody.contains("try? generateThumbnail"))
        #expect(!scanFileBody.contains("let thumbnail = try generateThumbnail"))
    }

    @Test func rawAssetsCanRenderFromPrimaryPathWhenThumbnailIsMissing() throws {
        let content = try sourceFile("Sources/PhotoAssetManager/ContentView.swift")
        let scanner = try sourceFile("Sources/PhotoAssetManager/PhotoScanner.swift")

        #expect(scanner.contains("\"3fr\""))
        #expect(scanner.contains("\"arw\""))
        #expect(content.contains("AssetPreviewImage("))
        #expect(content.contains("ImageRenderer.renderableImage(url: URL(fileURLWithPath: primaryPath))"))
        #expect(content.contains("asset.primaryPath"))
    }

    @Test func thumbnailsAreAssetScopedInsteadOfOnePerOriginalFile() throws {
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")
        let scanner = try sourceFile("Sources/PhotoAssetManager/PhotoScanner.swift")

        #expect(database.contains("func upsertAssetThumbnail(assetID: UUID, url: URL, hash: String, sizeBytes: Int64) throws"))
        #expect(functionBody(named: "upsertScannedFile", in: database).contains("upsertAssetThumbnail(assetID: assetID"))
        #expect(!functionBody(named: "upsertScannedFile", in: database).contains("upsertDerivedFile(assetID: assetID, url: thumbnailURL, role: .thumbnail"))
        #expect(database.contains("CREATE UNIQUE INDEX IF NOT EXISTS idx_file_instances_one_thumbnail_per_asset"))
        #expect(scanner.contains("thumbnailURL: thumbnail"))
    }

    @Test func thumbnailMigrationDeduplicatesExistingRawJpegPairThumbnails() throws {
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")
        let migrateBody = functionBody(named: "migrate", in: database)

        #expect(database.contains("func deduplicateAssetThumbnails() throws"))
        #expect(migrateBody.contains("try deduplicateAssetThumbnails()"))
        #expect(functionBody(named: "deduplicateAssetThumbnails", in: database).contains("ROW_NUMBER() OVER"))
        #expect(functionBody(named: "deduplicateAssetThumbnails", in: database).contains("DELETE FROM file_instances"))
        #expect(functionBody(named: "deduplicateAssetThumbnails", in: database).contains("file_role = 'thumbnail'"))
    }

    @Test func captureTimeBackfillRunsFullLibraryScanFromMacMenuBarToolsMenu() throws {
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")
        let store = try sourceFile("Sources/PhotoAssetManager/LibraryStore.swift")
        let content = try sourceFile("Sources/PhotoAssetManager/ContentView.swift")
        let app = try sourceFile("Sources/PhotoAssetManager/PhotoAssetManagerApp.swift")
        let scanner = try sourceFile("Sources/PhotoAssetManager/PhotoScanner.swift")

        #expect(!database.contains("func backfillMissingCaptureTimesFromCreatedAt()"))
        #expect(database.contains("func applyScannedCaptureTimeIfEmpty(path: String, captureTime: Date?) throws"))
        #expect(functionBody(named: "applyScannedCaptureTimeIfEmpty", in: database).contains("WHERE capture_time IS NULL"))
        #expect(scanner.contains("func bestCaptureTime(metadata: ImageMetadata, url: URL) throws -> Date?"))
        #expect(functionBody(named: "scanDirectory", in: scanner).contains("database.applyScannedCaptureTimeIfEmpty(path: url.path, captureTime: captureTime)"))
        #expect(store.contains("func fillMissingCaptureTimes()"))
        #expect(functionBody(named: "fillMissingCaptureTimes", in: store).contains("sourceDirectories.filter(\\.isTracked)"))
        #expect(functionBody(named: "fillMissingCaptureTimes", in: store).contains("scanner.scanDirectory"))
        #expect(functionBody(named: "fillMissingCaptureTimes", in: store).contains("blockingTask = BlockingTaskReport"))
        #expect(!content.contains("Menu(\"工具\")"))
        #expect(app.contains("ToolCommands(library: library)"))
        #expect(app.contains("struct ToolCommands: Commands"))
        #expect(app.contains("CommandMenu(\"工具\")"))
        #expect(app.contains("Button(\"补齐拍摄时间\")"))
        #expect(app.contains("library.fillMissingCaptureTimes()"))
    }

    @Test func assetPreviewLoadsImagesAsynchronouslyWithCache() throws {
        let content = try sourceFile("Sources/PhotoAssetManager/ContentView.swift")
        let previewBody = structBody(named: "AssetPreviewImage", in: content)
        let loaderBody = classBody(named: "ImagePreviewLoader", in: content)

        #expect(content.contains("final class ImagePreviewCache"))
        #expect(content.contains("NSCache<NSString, NSImage>"))
        #expect(content.contains("final class ImagePreviewLoader: ObservableObject"))
        #expect(loaderBody.contains("decodeTask?.cancel()"))
        #expect(loaderBody.contains("Task.detached(priority: .utility)"))
        #expect(loaderBody.contains("Task.isCancelled"))
        #expect(loaderBody.contains("PerformanceLog.measure"))
        #expect(!loaderBody.contains("Task.detached(priority: .userInitiated)"))
        #expect(loaderBody.contains("ImagePreviewCache.shared"))
        #expect(previewBody.contains("@StateObject private var loader = ImagePreviewLoader()"))
        #expect(previewBody.contains(".task(id: cacheKey)"))
        #expect(previewBody.contains("await loader.load"))
        #expect(!previewBody.contains("NSImage(contentsOfFile:"))
        #expect(!previewBody.contains("ImageRenderer.renderableImage"))
    }

    @Test func availabilityRefreshUsesIndexedQueryAndGroupedWrites() throws {
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")

        #expect(database.contains("idx_file_instances_role_path"))
        #expect(functionBody(named: "updateFileAvailability", in: database).contains("Dictionary(grouping: updates, by: \\.availability)"))
        #expect(functionBody(named: "updateFileAvailability", in: database).contains("WHERE id IN"))
    }

    @Test func startupAvailabilityRefreshIsThrottledAndCanBeForced() throws {
        let store = try sourceFile("Sources/PhotoAssetManager/LibraryStore.swift")
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")

        #expect(store.contains("private let availabilityRefreshInterval: TimeInterval = 24 * 60 * 60"))
        #expect(store.contains("func startAvailabilityRefreshInBackground(force: Bool = false)"))
        #expect(functionBody(named: "startAvailabilityRefreshInBackground", in: store).contains("shouldRunAvailabilityRefresh(force: force)"))
        #expect(functionBody(named: "startAvailabilityRefreshInBackground", in: store).contains("database.markAvailabilityRefreshCompleted(at: Date())"))
        #expect(store.contains("func forceAvailabilityRefreshInBackground()"))
        #expect(functionBody(named: "forceAvailabilityRefreshInBackground", in: store).contains("startAvailabilityRefreshInBackground(force: true)"))
        #expect(database.contains("func lastAvailabilityRefreshAt() throws -> Date?"))
        #expect(database.contains("func markAvailabilityRefreshCompleted(at date: Date) throws"))
        #expect(database.contains("last_availability_refresh_at"))
        #expect(database.contains("INSERT OR IGNORE INTO app_settings (key, value)"))
    }

    @Test func startupAvailabilityRefreshMountsNASRootsBeforeFileChecks() throws {
        let store = try sourceFile("Sources/PhotoAssetManager/LibraryStore.swift")
        let mountManager = try sourceFile("Sources/PhotoAssetManager/NASMountManager.swift")

        #expect(store.contains("private let nasMountManager = NASMountManager()"))
        #expect(functionBody(named: "startStartupLibraryOrganizationIfNeeded", in: store).contains("mountNASRootsAtStartup"))
        #expect(functionBody(named: "startStartupLibraryOrganizationIfNeeded", in: store).contains("挂载 NAS 来源"))
        #expect(functionBody(named: "startAvailabilityRefreshInBackground", in: store).contains("guard startupNASMountSucceeded else"))
        #expect(mountManager.contains("struct NASMountManager"))
        #expect(mountManager.contains("func mountNASRootsIfNeeded(for sources: [SourceDirectory], derivativeStorageURL: URL?) async -> NASMountReport"))
        #expect(mountManager.contains("UserDefaults.standard.string(forKey: \"nasSMBHost\")"))
        #expect(mountManager.contains("smb://"))
        #expect(mountManager.contains("/usr/bin/osascript"))
        #expect(mountManager.contains("mount volume"))
        #expect(mountManager.contains("uniqueVolumeRoots"))
    }

    @Test func startupMountIncludesDerivativeStorageRoot() throws {
        let store = try sourceFile("Sources/PhotoAssetManager/LibraryStore.swift")
        let mountManager = try sourceFile("Sources/PhotoAssetManager/NASMountManager.swift")

        #expect(functionBody(named: "mountNASRootsAtStartup", in: store).contains("derivativeStorageURL: derivativeStorageURL"))
        #expect(mountManager.contains("func uniqueVolumeRoots(from sources: [SourceDirectory], derivativeStorageURL: URL?) -> [NASVolumeRoot]"))
        #expect(functionBody(named: "uniqueVolumeRoots", in: mountManager).contains("if let derivativeStorageURL"))
        #expect(functionBody(named: "uniqueVolumeRoots", in: mountManager).contains("NASVolumeRoot(path: derivativeStorageURL.path, host: host)"))
    }

    @Test func startupMountCleanupRunsWhenNoIndexOrganizationIsNeeded() throws {
        let store = try sourceFile("Sources/PhotoAssetManager/LibraryStore.swift")
        let startupBody = functionBody(named: "startStartupLibraryOrganizationIfNeeded", in: store)

        #expect(startupBody.contains("defer {"))
        #expect(startupBody.contains("isScanning = false"))
        #expect(startupBody.contains("blockingTask = nil"))
        #expect(startupBody.contains("startupOrganizationTask = nil"))
        #expect(startupBody.contains("guard !sources.isEmpty else"))
    }

    @Test func removingFolderRequiresConfirmationAndDeletesByStorageKind() throws {
        let content = try sourceFile("Sources/PhotoAssetManager/ContentView.swift")
        let store = try sourceFile("Sources/PhotoAssetManager/LibraryStore.swift")
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")
        let operations = try sourceFile("Sources/PhotoAssetManager/FileOperations.swift")

        #expect(content.contains("@State private var pendingFolderRemovalSource: FolderMoveSource?"))
        #expect(content.contains("FolderRemovalConfirmationDialog("))
        #expect(content.contains("openRemovalDialog(moveSource)"))
        #expect(content.contains("Button(\"仅移除\")"))
        #expect(content.contains("Button(\"彻底删除\", role: .destructive)"))
        #expect(store.contains("func removeFolder(_ source: FolderMoveSource, deleteEmptyFolder: Bool)"))
        #expect(store.contains("deleteEmptyFolderTree(at: URL(fileURLWithPath: source.path, isDirectory: true), storageKind: source.storageKind)"))
        #expect(store.contains("try database.removeBrowseFolderTree(path: source.path)"))
        #expect(database.contains("func removeBrowseFolderTree(path: String) throws"))
        #expect(functionBody(named: "removeBrowseFolderTree", in: database).contains("DELETE FROM browse_nodes"))
        #expect(functionBody(named: "removeBrowseFolderTree", in: database).contains("canonical_key = ? OR canonical_key LIKE ? || '/%'"))
        #expect(operations.contains("func deleteEmptyFolderTree(at url: URL, storageKind: StorageKind) throws"))
        #expect(functionBody(named: "deleteEmptyFolderTree", in: operations).contains("case .local:"))
        #expect(functionBody(named: "deleteEmptyFolderTree", in: operations).contains("case .nas, .externalDrive:"))
        #expect(functionBody(named: "deleteEmptyFolderTree", in: operations).contains("try trashEmptyFolderTree(at: url)"))
        #expect(functionBody(named: "deleteEmptyFolderTree", in: operations).contains("try removeEmptyDirectoryTree(at: url)"))
        #expect(functionBody(named: "ensureFolderTreeContainsOnlyDirectories", in: operations).contains("resourceValues(forKeys: [.isRegularFileKey, .isDirectoryKey, .isSymbolicLinkKey])"))
        #expect(functionBody(named: "ensureFolderTreeContainsOnlyDirectories", in: operations).contains("throw FileOperationError.folderContainsFiles"))
        #expect(functionBody(named: "trashEmptyFolderTree", in: operations).contains("fileManager.trashItem"))
        #expect(!functionBody(named: "trashEmptyFolderTree", in: operations).contains("removeItem"))
    }

    @Test func photoDeletionPrincipleRequiresTwoStepFilesystemDeletionWithoutRm() throws {
        let readme = try sourceFile("README.md")
        let feature = try sourceFile("feature.md")

        #expect(readme.contains("删除照片必须先从右键菜单发起，再在确认弹窗中确认"))
        #expect(feature.contains("照片和文件夹物理删除必须经过两步操作"))
        #expect(feature.contains("不能调用 rm"))
    }

    private func sourceFile(_ path: String) throws -> String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repositoryRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let file = repositoryRoot.appendingPathComponent(path)
        return try String(contentsOf: file, encoding: .utf8)
    }

    private func functionBody(named name: String, in source: String) -> String {
        guard let range = source.range(of: "func \(name)") else { return "" }
        let suffix = source[range.lowerBound...]
        guard let openBrace = suffix.firstIndex(of: "{") else { return "" }

        var depth = 0
        var body = ""
        for character in suffix[openBrace...] {
            if character == "{" {
                depth += 1
            } else if character == "}" {
                depth -= 1
            }
            body.append(character)
            if depth == 0 {
                return body
            }
        }
        return body
    }

    private func structBody(named name: String, in source: String) -> String {
        body(after: "struct \(name)", in: source)
    }

    private func classBody(named name: String, in source: String) -> String {
        body(after: "class \(name)", in: source)
    }

    private func body(after marker: String, in source: String) -> String {
        guard let range = source.range(of: marker) else { return "" }
        let suffix = source[range.lowerBound...]
        guard let openBrace = suffix.firstIndex(of: "{") else { return "" }

        var depth = 0
        var body = ""
        for character in suffix[openBrace...] {
            if character == "{" {
                depth += 1
            } else if character == "}" {
                depth -= 1
            }
            body.append(character)
            if depth == 0 {
                return body
            }
        }
        return body
    }
}
