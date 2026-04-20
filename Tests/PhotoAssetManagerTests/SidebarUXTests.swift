import Foundation
import Testing

struct SidebarUXTests {
    @Test func folderRowsReceiveInterruptedScanContextAndHideTrackingText() throws {
        let source = try contentViewSource()

        #expect(source.contains("interruptedScanPath: library.interruptedScanPath"))
        #expect(!source.contains("追踪中"))
    }

    @Test func foldersInSidebarAreAlwaysTrackedAndUseContextMenu() throws {
        let source = try contentViewSource()
        let store = try libraryStoreSource()
        let app = try appSource()

        #expect(source.contains(".contextMenu"))
        #expect(source.contains("Button(\"刷新\")"))
        #expect(!source.contains("Button(\"扫描\")"))
        #expect(!source.contains("停止追踪"))
        #expect(!source.contains("恢复"))
        #expect(!source.contains("已停止"))
        #expect(!source.contains("扫描本地目录"))
        #expect(!source.contains("扫描 NAS 目录"))
        #expect(!source.contains(".disabled(!source.isTracked || library.isBusy)"))

        #expect(functionBody(named: "scanTrackedSources", in: store).contains("sourceDirectories.filter(\\.isTracked)"))
        #expect(!store.contains("for source in sources where source.isTracked"))
        #expect(!store.contains("guard source.isTracked else { return }"))
        #expect(!store.contains("func chooseAndScan"))
        #expect(!app.contains("扫描本地目录"))
        #expect(!app.contains("扫描 NAS 目录"))
    }

    @Test func folderRowsDoNotShowTrailingEllipsisMenu() throws {
        let source = try contentViewSource()
        let rowBody = structBody(named: "SourceDirectoryNodeRow", in: source)

        #expect(rowBody.contains(".contextMenu"))
        #expect(!rowBody.contains("\n            Menu {"))
        #expect(!rowBody.contains("Image(systemName: \"ellipsis\")"))
        #expect(rowBody.contains("FolderActionMenuItems("))
    }

    @Test func thumbnailMaintenanceIsCollapsedInsideSidebar() throws {
        let source = try contentViewSource()

        #expect(!source.contains("Section(\"缩略图存储\")"))
        #expect(source.contains("ThumbnailStoragePopover()"))
    }

    @Test func sidebarDoesNotShowWorkflowStatusShortcuts() throws {
        let source = try contentViewSource()
        let sidebarBody = structBody(named: "SidebarView", in: source)
        let filterBarBody = structBody(named: "FilterBar", in: source)

        #expect(!sidebarBody.contains("Section(\"工作流\")"))
        #expect(!sidebarBody.contains("AssetStatus.allCases"))
        #expect(!source.contains("struct StatusRow"))
        #expect(filterBarBody.contains("library.filter = LibraryFilter()"))
        #expect(!filterBarBody.contains("LibraryFilter(status: status)"))
    }

    @Test func filterBarUsesLightroomStyleFirstThreeFilterGroupsAndSortMenu() throws {
        let source = try contentViewSource()
        let models = try sourceFile("Sources/PhotoAssetManager/Models.swift")
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")
        let filterBarBody = structBody(named: "FilterBar", in: source)

        #expect(models.contains("enum LibrarySortOrder"))
        #expect(models.contains("case captureTimeAscending"))
        #expect(models.contains("case captureTimeDescending"))
        #expect(models.contains("var sortOrder: LibrarySortOrder = .captureTimeDescending"))
        #expect(database.contains("filter.sortOrder.sqlDirection"))
        #expect(!database.contains("ORDER BY COALESCE(capture_time, created_at) DESC"))
        #expect(filterBarBody.contains("LightroomRatingFilterGroup("))
        #expect(filterBarBody.contains("LightroomFlagFilterGroup("))
        #expect(filterBarBody.contains("LightroomColorLabelFilterGroup("))
        #expect(filterBarBody.contains("Picker(\"整理顺序\""))
        #expect(filterBarBody.contains("library.setMinimumRatingFilter"))
        #expect(filterBarBody.contains("library.setFlaggedOnlyFilter"))
        #expect(filterBarBody.contains("library.setSortOrder"))
        #expect(!filterBarBody.contains("TextField(\"相机\""))
        #expect(!filterBarBody.contains("Stepper(\"最低"))
        #expect(!filterBarBody.contains("Button(\"记录导出\""))
        #expect(!filterBarBody.contains("library.recordExportForSelected"))
    }

    @Test func fileSearchFieldStaysCollapsedUntilExplicitlyOpened() throws {
        let source = try contentViewSource()
        let filterBarBody = structBody(named: "FilterBar", in: source)

        #expect(filterBarBody.contains("@State private var isFileSearchOpen = false"))
        #expect(filterBarBody.contains("@FocusState private var isFileSearchFocused"))
        #expect(filterBarBody.contains("if isFileSearchOpen"))
        #expect(filterBarBody.contains("TextField(\"文件搜索\""))
        #expect(filterBarBody.contains("Button(\"文件搜索\")"))
        #expect(filterBarBody.contains("isFileSearchOpen = true"))
        #expect(filterBarBody.contains(".focused($isFileSearchFocused)"))
        #expect(filterBarBody.contains("isFileSearchFocused = true"))
        #expect(!filterBarBody.contains("TextField(\"搜索\""))
    }

    @Test func folderSidebarUsesTreeWithChildDisclosureAndMoveMenu() throws {
        let source = try contentViewSource()
        let store = try libraryStoreSource()
        let models = try sourceFile("Sources/PhotoAssetManager/Models.swift")
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")

        #expect(models.contains("struct SourceDirectoryNode"))
        #expect(models.contains("var parentID: UUID?"))
        #expect(source.contains("@State private var expandedFolderNodeIDs: Set<String>"))
        #expect(source.contains("SourceDirectoryTreeBuilder.build"))
        #expect(source.contains("SourceDirectoryNodeRow("))
        #expect(source.contains("Image(systemName: isExpanded ? \"chevron.down\" : \"chevron.right\")"))
        #expect(source.contains("Button(action: select)"))
        #expect(source.contains("toggleExpansion()"))
        #expect(source.contains("Button(\"移动到...\")"))
        #expect(source.contains("FolderActionMenuItems("))
        #expect(store.contains("func moveSourceDirectory"))
        #expect(database.contains("parent_source_directory_id"))
        #expect(database.contains("func moveSourceDirectory"))
        #expect(database.contains("UPDATE file_instances SET path = replace(path, ?, ?)"))
        #expect(store.contains("startFolderMove"))
    }

    @Test func folderTreeUsesLightroomStyleClickableDisclosureRows() throws {
        let source = try contentViewSource()
        let rowBody = structBody(named: "SourceDirectoryNodeRow", in: source)

        #expect(rowBody.contains("Image(systemName: isExpanded ? \"chevron.down\" : \"chevron.right\")"))
        #expect(rowBody.contains(".contentShape(Rectangle())"))
        #expect(rowBody.contains("Button(action: select)"))
        #expect(rowBody.contains("toggleExpansion()"))
        #expect(rowBody.contains("node.hasChildren"))
        #expect(!source.contains("Button(\"展开\")"))
        #expect(!source.contains("Button(\"收缩\")"))
        #expect(!source.contains("node.depth > 0 && node.hasChildren"))
    }

    @Test func folderRowsFeelLikePressedButtons() throws {
        let source = try contentViewSource()
        let rowBody = structBody(named: "SourceDirectoryNodeRow", in: source)
        let styleBody = structBody(named: "FolderRowButtonStyle", in: source)

        #expect(rowBody.contains("@State private var isHovering"))
        #expect(rowBody.contains("Button(action: select)"))
        #expect(rowBody.contains("FolderRowButtonStyle(isSelected: isSelected, isHovering: isHovering)"))
        #expect(rowBody.contains(".onHover"))
        #expect(rowBody.contains(".disabled(library.pendingBrowseSelection != nil)"))
        #expect(styleBody.contains("configuration.isPressed"))
        #expect(styleBody.contains("Color.accentColor.opacity"))
        #expect(styleBody.contains("RoundedRectangle(cornerRadius: 6)"))
        #expect(styleBody.contains(".scaleEffect(configuration.isPressed ? 0.985 : 1.0)"))
    }

    @Test func folderTreeCanCollapseRootsAndUsesRelativeDisplayNames() throws {
        let content = try contentViewSource()
        let models = try sourceFile("Sources/PhotoAssetManager/Models.swift")

        #expect(models.contains("var displayName: String"))
        #expect(models.contains("displayNameOverride ?? displayName(for: source, parent: parent)"))
        #expect(models.contains("guard expandedNodeIDs.contains(nodeID) else { return nodes }"))
        #expect(!models.contains("guard depth == 0 || expandedIDs.contains(source.id) else { return nodes }"))
        #expect(models.contains("relativePath.hasPrefix(\"/\")"))
        #expect(content.contains("displayName: node.displayName"))
        #expect(content.contains("Text(displayName)"))
    }

    @Test func folderSidebarUsesCompactLightroomRowsWithoutPathMetadata() throws {
        let content = try contentViewSource()
        let models = try sourceFile("Sources/PhotoAssetManager/Models.swift")
        let rowBody = structBody(named: "SourceDirectoryRow", in: content)
        let nodeRowBody = structBody(named: "SourceDirectoryNodeRow", in: content)

        #expect(models.contains("guard let parent else { return URL(fileURLWithPath: sourcePath).lastPathComponent }"))
        #expect(rowBody.contains("HStack(spacing: 6)"))
        #expect(rowBody.contains(".lineLimit(1)"))
        #expect(rowBody.contains(".padding(.vertical, 1)"))
        #expect(nodeRowBody.contains(".frame(width: CGFloat(node.depth) * 10)"))
        #expect(nodeRowBody.contains(".frame(width: 12, height: 20)"))
        #expect(!rowBody.contains("lastScannedAt"))
        #expect(!rowBody.contains(".textSelection(.enabled)"))
    }

    @Test func folderTreeExpandsIndexedBrowseGraphSubdirectoriesWithoutPureBlackSidebar() throws {
        let content = try contentViewSource()
        let sidebarBody = structBody(named: "SidebarView", in: content)
        let models = try sourceFile("Sources/PhotoAssetManager/Models.swift")
        let store = try libraryStoreSource()

        #expect(content.contains("@State private var expandedFolderNodeIDs: Set<String>"))
        #expect(content.contains("expandedFolderNodeIDs.contains(node.id)"))
        #expect(models.contains("var id: String"))
        #expect(models.contains("var source: SourceDirectory?"))
        #expect(store.contains("@Published var indexedBrowseFolders: [BrowseNode] = []"))
        #expect(store.contains("indexedBrowseFolders = try database.browseFolders()"))
        #expect(models.contains("indexedBrowseFolders:"))
        #expect(!models.contains("contentsOfDirectory"))
        #expect(!models.contains("fileSystemChildNodes"))
        #expect(!models.contains("hasFileSystemSubdirectories"))
        #expect(content.contains("AppPalette.sidebarBackground"))
        #expect(content.contains("AppPalette.folderText"))
        #expect(!sidebarBody.contains("Color.black"))
        #expect(!sidebarBody.contains("NSColor.black"))
    }

    @Test func folderBrowsingUsesReusableBrowseGraphSelection() throws {
        let content = try contentViewSource()
        let models = try sourceFile("Sources/PhotoAssetManager/Models.swift")
        let store = try libraryStoreSource()
        let app = try appSource()
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")

        #expect(models.contains("enum BrowseNodeKind"))
        #expect(models.contains("case folder"))
        #expect(models.contains("enum BrowseScope"))
        #expect(models.contains("case direct"))
        #expect(models.contains("case recursive"))
        #expect(models.contains("struct BrowseSelection"))
        #expect(models.contains("var browseSelection: BrowseSelection?"))
        #expect(models.contains("struct IndexedFolderTree"))
        #expect(store.contains("func selectFolder(path: String)"))
        #expect(store.contains("try readDatabase.browseFolder(path: normalizedPath)"))
        #expect(!functionBody(named: "selectFolder", in: store).contains("upsertBrowseFolderNode"))
        #expect(store.contains("func clearBrowseSelection()"))
        #expect(store.contains("func setBrowseScope(_ scope: BrowseScope)"))
        #expect(database.contains("func browseFolders() throws -> [BrowseNode]"))
        #expect(content.contains("isSelected: library.filter.browseSelection?.path == node.path"))
        #expect(content.contains("select: {"))
        #expect(app.contains("Commands"))
        #expect(app.contains("FolderScopeCommands"))
        #expect(app.contains("仅当前文件夹"))
        #expect(app.contains("包含子文件夹"))
    }

    @Test func folderSelectionShowsImmediatePendingFeedbackBeforeAssetsFinishLoading() throws {
        let content = try contentViewSource()
        let store = try libraryStoreSource()
        let selectFolderBody = functionBody(named: "selectFolder", in: store)
        let finishSelectingFolderBody = functionBody(named: "finishSelectingFolder", in: store)

        #expect(store.contains("@Published var pendingBrowseSelection: BrowseSelection?"))
        #expect(selectFolderBody.contains("pendingBrowseSelection = selection"))
        #expect(selectFolderBody.contains("blockingTask = BlockingTaskReport"))
        #expect(selectFolderBody.contains("assets = []"))
        #expect(selectFolderBody.contains("selectedAssetID = nil"))
        #expect(selectFolderBody.contains("selectedFiles = []"))
        #expect(selectFolderBody.contains("Task(priority: .userInitiated) { [weak self] in"))
        #expect(selectFolderBody.contains("await Task.yield()"))
        #expect(!selectFolderBody.contains("refresh()"))
        #expect(finishSelectingFolderBody.contains("defer"))
        #expect(finishSelectingFolderBody.contains("blockingTask = nil"))
        #expect(content.contains("library.pendingBrowseSelection?.path == node.path"))
        #expect(store.contains("正在打开文件夹"))
    }

    @Test func assetBrowserUsesLightroomStyleFilledTilesWithoutMetadata() throws {
        let content = try contentViewSource()
        let browserBody = structBody(named: "AssetBrowserView", in: content)
        let tileBody = structBody(named: "AssetTile", in: content)

        #expect(content.contains("struct JustifiedAssetGrid"))
        #expect(content.contains("struct JustifiedAssetRow"))
        #expect(content.contains("enum JustifiedAssetGridLayout"))
        #expect(content.contains("JustifiedAssetGridLayout.rows"))
        #expect(content.contains("targetHeight: CGFloat = 168"))
        #expect(content.contains("rowHeight = availableImageWidth / aspectRatioSum"))
        #expect(content.contains("GeometryReader"))
        #expect(browserBody.contains("JustifiedAssetGrid("))
        #expect(browserBody.contains(".background(Color.black)"))
        #expect(!browserBody.contains("LazyVGrid"))
        #expect(!browserBody.contains("GridItem(.adaptive"))
        #expect(tileBody.contains(".clipped()"))
        #expect(tileBody.contains("AssetPreviewImage("))
        #expect(tileBody.contains("contentMode: .fit"))
        #expect(tileBody.contains("placeholderSize: 34"))
        #expect(tileBody.contains("onAspectRatioChange"))
        #expect(!tileBody.contains(".overlay"))
        #expect(!tileBody.contains(".stroke("))
        #expect(!tileBody.contains("Color.white.opacity"))
        #expect(!tileBody.contains("Color.clear"))
        #expect(!tileBody.contains("Color.accentColor"))
        #expect(!tileBody.contains("Text(asset.originalFilename)"))
        #expect(!tileBody.contains("asset.status.label"))
        #expect(!tileBody.contains("asset.rating"))
    }

    @Test func doubleClickOpensLightroomStyleLoupeWithFilmstrip() throws {
        let content = try contentViewSource()
        let browserBody = structBody(named: "AssetBrowserView", in: content)
        let gridBody = structBody(named: "JustifiedAssetGrid", in: content)
        let loupeBody = structBody(named: "LightroomLoupeView", in: content)
        let filmstripBody = structBody(named: "LoupeFilmstripView", in: content)

        #expect(content.contains("@State private var loupeAssetID: UUID?"))
        #expect(browserBody.contains("if let loupeAssetID, let loupeAsset = library.assets.first(where: { $0.id == loupeAssetID })"))
        #expect(browserBody.contains("LightroomLoupeView("))
        #expect(browserBody.contains("openLoupe: { asset in"))
        #expect(browserBody.contains("loupeAssetID = asset.id"))
        #expect(gridBody.contains("var openLoupe: (Asset) -> Void"))
        #expect(gridBody.contains("AssetMouseEventCatcher("))
        #expect(gridBody.contains("singleClick: {"))
        #expect(gridBody.contains("doubleClick: {"))
        #expect(gridBody.contains("dragPayload: assetDragPayload(for: asset)"))
        #expect(content.contains("struct AssetMouseEventCatcher: NSViewRepresentable"))
        #expect(content.contains("final class AssetMouseEventView: NSView, NSDraggingSource"))
        #expect(content.contains("event.clickCount >= 2"))
        #expect(content.contains("beginDraggingSession"))
        #expect(!gridBody.contains("TapGesture("))
        #expect(!gridBody.contains("ExclusiveGesture("))
        #expect(!gridBody.contains(".onDrag"))
        #expect(!gridBody.contains(".draggable("))
        #expect(!gridBody.contains(".onTapGesture {"))
        #expect(!gridBody.contains(".onTapGesture(count: 2)"))
        #expect(loupeBody.contains("AssetPreviewImage(asset: asset, contentMode: .fit, placeholderSize: 72)"))
        #expect(loupeBody.contains("LoupeFilmstripView("))
        #expect(loupeBody.contains("Button(\"返回图库\")"))
        #expect(loupeBody.contains(".keyboardShortcut(.escape, modifiers: [])"))
        #expect(loupeBody.contains(".background(Color.black)"))
        #expect(filmstripBody.contains("ScrollView(.horizontal"))
        #expect(filmstripBody.contains("ForEach(assets)"))
        #expect(filmstripBody.contains("AssetPreviewImage(asset: filmstripAsset, contentMode: .fit, placeholderSize: 18)"))
        #expect(!filmstripBody.contains("contentMode: .fill"))
        #expect(filmstripBody.contains("LoupeFilmstripMetrics.thumbnailWidth"))
        #expect(filmstripBody.contains("LoupeFilmstripMetrics.thumbnailHeight"))
        #expect(filmstripBody.contains("LoupeFilmstripMetrics.verticalPadding"))
        #expect(filmstripBody.contains("LoupeFilmstripMetrics.height"))
        #expect(filmstripBody.contains(".layoutPriority(1)"))
        #expect(!filmstripBody.contains(".frame(width: 70, height: 52)"))
        #expect(!filmstripBody.contains(".frame(height: 60)"))
        #expect(filmstripBody.contains("select(filmstripAsset)"))
    }

    @Test func detailHeaderDoesNotRepeatSelectedAssetThumbnail() throws {
        let content = try contentViewSource()
        let headerBody = structBody(named: "PreviewHeader", in: content)

        #expect(!headerBody.contains("AssetPreviewImage("))
        #expect(!headerBody.contains("placeholderSize: 46"))
        #expect(headerBody.contains("Text(asset.originalFilename)"))
        #expect(headerBody.contains("asset.primaryPath ?? \"当前没有可访问原片路径\""))
    }

    @Test func folderMoveUsesRecoverableBlockingFileMoveJobs() throws {
        let content = try contentViewSource()
        let store = try libraryStoreSource()
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")
        let operations = try sourceFile("Sources/PhotoAssetManager/FileOperations.swift")
        let rewriteBody = functionBody(named: "rewriteFolderMovePaths", in: database)
        let continueMoveBody = functionBody(named: "continueFolderMove", in: store)

        #expect(database.contains("CREATE TABLE IF NOT EXISTS folder_move_jobs"))
        #expect(database.contains("CREATE TABLE IF NOT EXISTS folder_move_items"))
        #expect(database.contains("func createFolderMoveJob"))
        #expect(database.contains("func unfinishedFolderMoveJob"))
        #expect(database.contains("func completeFolderMoveItem"))
        #expect(database.contains("func rewriteFolderMovePaths"))
        #expect(database.contains("func markInterruptedFolderMoveJobs"))
        #expect(operations.contains("func moveFolder"))
        #expect(operations.contains("copyItem(at: source, to: destination)"))
        #expect(operations.contains("FileHasher.sha256(url: destination)"))
        #expect(operations.contains("removeItem(at: source)"))
        #expect(store.contains("resumeInterruptedFolderMoveIfNeeded()"))
        #expect(store.contains("title: \"移动文件夹\""))
        #expect(content.contains("FolderMoveTargetDialog("))
        #expect(content.contains("FolderActionMenuItems("))
        #expect(database.contains("func refreshBrowseGraphForFolderMove"))
        #expect(rewriteBody.contains("refreshBrowseGraphForFolderMove(job: job)"))
        #expect(!rewriteBody.contains("rebuildBrowseGraph()"))
        #expect(continueMoveBody.contains("phase: \"更新索引\""))
        #expect(continueMoveBody.contains("Task.detached(priority: .userInitiated)"))
        #expect(continueMoveBody.contains("await MainActor.run"))
        #expect(continueMoveBody.contains("phase: \"复制、校验并删除源文件\""))
        #expect(!operations.contains("@MainActor\nstruct FileOperations"))
        #expect(operations.contains("@MainActor\n    func reveal"))
        #expect(operations.contains("@MainActor\n    func open"))
    }

    @Test func folderMoveOpensTargetDialogForRegisteredAndIndexedFolders() throws {
        let content = try contentViewSource()
        let rowBody = structBody(named: "SourceDirectoryNodeRow", in: content)
        let actionsBody = structBody(named: "FolderActionMenuItems", in: content)
        let dialogBody = structBody(named: "FolderMoveTargetDialog", in: content)
        let store = try libraryStoreSource()
        let models = try sourceFile("Sources/PhotoAssetManager/Models.swift")

        #expect(content.contains("FolderMoveSource"))
        #expect(models.contains("struct FolderMoveSource"))
        #expect(rowBody.contains("moveSource: FolderMoveSource"))
        #expect(rowBody.contains("FolderActionMenuItems("))
        #expect(rowBody.contains(".contextMenu"))
        #expect(actionsBody.contains("Button(\"移动到...\")"))
        #expect(actionsBody.contains("openMoveDialog(moveSource)"))
        #expect(!rowBody.contains("if let source = node.source"))
        #expect(content.contains("@State private var pendingMoveSource: FolderMoveSource?"))
        #expect(content.contains("FolderMoveTargetDialog("))
        #expect(dialogBody.contains("Button(\"进入\")"))
        #expect(dialogBody.contains("Button(\"移动到这里\")"))
        #expect(dialogBody.contains("library.moveFolder(source, to: target)"))
        #expect(dialogBody.contains("currentPath = target.path"))
        #expect(dialogBody.contains("childTargets"))
        #expect(!content.contains("Menu(\"移动到\")"))
        #expect(store.contains("func moveFolder(_ source: FolderMoveSource, to target: FolderMoveTarget)"))
    }

    @Test func folderMoveDialogCanCreateTargetFolderInCurrentDirectory() throws {
        let content = try contentViewSource()
        let dialogBody = structBody(named: "FolderMoveTargetDialog", in: content)
        let createDialogBody = structBody(named: "FolderCreateDialog", in: content)

        #expect(dialogBody.contains("@State private var createdTargets: [FolderMoveTarget]"))
        #expect(dialogBody.contains("@State private var pendingCreateFolderParentPath: String?"))
        #expect(dialogBody.contains("Button(\"添加文件夹\")"))
        #expect(dialogBody.contains("pendingCreateFolderParentPath = currentPath"))
        #expect(dialogBody.contains("FolderCreateDialog("))
        #expect(dialogBody.contains("try FileManager.default.createDirectory"))
        #expect(dialogBody.contains("createdTargets.append(created)"))
        #expect(dialogBody.contains("currentPath = created.path"))
        #expect(createDialogBody.contains("TextField(\"文件夹名称\""))
        #expect(createDialogBody.contains("Button(\"创建\")"))
        #expect(createDialogBody.contains("error.fullTrace"))
        #expect(createDialogBody.contains("cancel()"))
    }

    @Test func photoImportCopiesExternalFoldersIntoIndexedTargets() throws {
        let content = try contentViewSource()
        let store = try libraryStoreSource()
        let operations = try sourceFile("Sources/PhotoAssetManager/FileOperations.swift")
        let models = try sourceFile("Sources/PhotoAssetManager/Models.swift")
        let importBody = functionBody(named: "importPhotoFolder", in: store)
        let copyBody = functionBody(named: "copyImportedFolder", in: operations)

        #expect(models.contains("struct PhotoImportTarget"))
        #expect(models.contains("struct PhotoImportPlanItem"))
        #expect(content.contains("@State private var pendingImportSource: URL?"))
        #expect(content.contains("Button(\"导入照片\""))
        #expect(content.contains("PhotoImportTargetDialog("))
        #expect(content.contains("library.importPhotoFolder(source, to: target)"))
        #expect(store.contains("func choosePhotoImportSource() -> URL?"))
        #expect(store.contains("func availablePhotoImportTargets() -> [PhotoImportTarget]"))
        #expect(importBody.contains("FileOperations().buildPhotoImportPlan"))
        #expect(importBody.contains("FileOperations().copyImportedFolder"))
        #expect(importBody.contains("scanner.scanDirectory"))
        #expect(importBody.contains("database.markSourceDirectoryScanned(path: target.path)"))
        #expect(operations.contains("func buildPhotoImportPlan"))
        #expect(operations.contains("func copyImportedFolder"))
        #expect(copyBody.contains("copyItem(at: source, to: destination)"))
        #expect(copyBody.contains("FileHasher.sha256(url: destination)"))
        #expect(!copyBody.contains("removeItem(at: source)"))
        #expect(!copyBody.contains("emptySourceDirectoryTree"))
    }

    @Test func selectedAssetsCanBeDraggedToFolderAfterConfirmation() throws {
        let content = try contentViewSource()
        let store = try libraryStoreSource()
        let operations = try sourceFile("Sources/PhotoAssetManager/FileOperations.swift")
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")
        let models = try sourceFile("Sources/PhotoAssetManager/Models.swift")

        #expect(models.contains("struct AssetFileMoveRequest"))
        #expect(models.contains("struct AssetFileMovePlanItem"))
        #expect(store.contains("@Published var selectedAssetIDs: Set<UUID>"))
        #expect(store.contains("func selectAsset(_ asset: Asset, modifiers: EventModifiers)"))
        #expect(store.contains("func moveAssets(_ assetIDs: [UUID], to target: FolderMoveTarget)"))
        #expect(database.contains("func movableFileInstances(assetIDs: [UUID])"))
        #expect(database.contains("func completeAssetFileMoveItem"))
        #expect(database.contains("SET path = ?, storage_kind = ?"))
        #expect(operations.contains("func buildAssetFileMovePlan"))
        #expect(operations.contains("func moveAssetFiles"))
        #expect(operations.contains("copyItem(at: source, to: destination)"))
        #expect(operations.contains("removeItem(at: source)"))

        #expect(content.contains("@State private var pendingAssetFileMoveRequest: AssetFileMoveRequest?"))
        #expect(content.contains("AssetFileMoveConfirmationDialog("))
        #expect(content.contains("dragPayload: assetDragPayload(for: asset)"))
        #expect(content.contains("beginDraggingSession"))
        #expect(content.contains("NSDraggingSource"))
        #expect(content.contains(".dropDestination(for: String.self)"))
        #expect(content.contains("openAssetMoveConfirmation"))
        #expect(content.contains("library.moveAssets(request.assetIDs, to: request.target)"))
        #expect(content.contains("ModifierAwareClickView"))
        #expect(content.contains("select(asset, modifiers)"))
        #expect(!content.contains("NSEvent.modifierFlags"))

        let confirmationBody = structBody(named: "AssetFileMoveConfirmationDialog", in: content)
        #expect(confirmationBody.contains("Text(\"移动选中文件？\")"))
        #expect(confirmationBody.contains("Button(\"确认移动\")"))
        #expect(confirmationBody.contains("request.assetIDs.count"))
    }

    @Test func selectedAssetsCanBeDeletedFromContextMenuAfterConfirmation() throws {
        let content = try contentViewSource()
        let store = try libraryStoreSource()
        let operations = try sourceFile("Sources/PhotoAssetManager/FileOperations.swift")
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")

        #expect(content.contains("@State private var pendingAssetDeletionRequest: AssetDeletionRequest?"))
        #expect(content.contains("AssetDeletionConfirmationDialog("))
        #expect(content.contains("openAssetDeletionConfirmation"))
        #expect(content.contains("Button(\"删除照片\", role: .destructive)"))
        #expect(content.contains("override func rightMouseDown"))
        #expect(content.contains("NSMenuItem(title: \"删除照片\""))
        #expect(content.contains("library.deleteAssets(request.assetIDs)"))

        let confirmationBody = structBody(named: "AssetDeletionConfirmationDialog", in: content)
        #expect(confirmationBody.contains("Text(\"删除选中照片？\")"))
        #expect(confirmationBody.contains("request.assetIDs.count"))
        #expect(confirmationBody.contains("Button(\"确认删除\", role: .destructive)"))
        #expect(confirmationBody.contains("优先移入废纸篓"))
        #expect(confirmationBody.contains(".frame(minHeight: 72, alignment: .leading)"))
        #expect(confirmationBody.contains(".frame(width: 520, height: 190, alignment: .leading)"))

        #expect(store.contains("func deleteAssets(_ assetIDs: [UUID])"))
        #expect(store.contains("FileOperations().deleteAssetFiles"))
        #expect(store.contains("try database.deletableFileInstances(assetIDs: assetIDs)"))
        let deleteAssetsBody = functionBody(named: "deleteAssets", in: store)
        #expect(deleteAssetsBody.contains("let visibleDeletionFiles = files.filter { $0.fileRole != .thumbnail }"))
        #expect(deleteAssetsBody.contains("guard file.fileRole != .thumbnail else { return }"))
        #expect(deleteAssetsBody.contains("totalItems: visibleDeletionFiles.count"))
        #expect(!deleteAssetsBody.contains("totalItems: files.count"))

        #expect(database.contains("func deletableFileInstances(assetIDs: [UUID]) throws -> [FileInstance]"))
        #expect(database.contains("func removeDeletedFileInstance(_ file: FileInstance, deletionMethod: AssetFileDeletionMethod) throws"))
        #expect(functionBody(named: "removeDeletedFileInstance", in: database).contains("DELETE FROM file_instances WHERE id = ?"))
        #expect(functionBody(named: "removeDeletedFileInstance", in: database).contains("DELETE FROM assets"))
        #expect(functionBody(named: "removeDeletedFileInstance", in: database).contains("operation_logs"))

        #expect(operations.contains("enum AssetFileDeletionMethod"))
        #expect(operations.contains("func deleteAssetFiles"))
        #expect(operations.contains("try database.removeDeletedFileInstance"))
        #expect(functionBody(named: "deleteAssetFile", in: operations).contains("fileManager.trashItem"))
        #expect(functionBody(named: "deleteAssetFile", in: operations).contains("fileManager.removeItem"))
        #expect(!functionBody(named: "deleteAssetFile", in: operations).contains("Process("))
    }

    @Test func shiftSelectionUsesCurrentAnchorAndSelectsContiguousRange() throws {
        let store = try libraryStoreSource()
        let selectBody = functionBody(named: "selectAsset", in: store)
        let refreshBody = functionBody(named: "refresh", in: store)
        let finishSelectingFolderBody = functionBody(named: "finishSelectingFolder", in: store)

        #expect(refreshBody.contains("assetSelectionAnchorID = selectedAssetID"))
        #expect(finishSelectingFolderBody.contains("assetSelectionAnchorID = result.selectedAssetID"))
        #expect(selectBody.contains("let bounds = min(anchorIndex, selectedIndex)...max(anchorIndex, selectedIndex)"))
        #expect(selectBody.contains("selectedAssetIDs = Set(assets[bounds].map(\\.id))"))
        #expect(!selectBody.contains("selectedAssetIDs.formUnion(assets[bounds].map(\\.id))"))
    }

    @Test func lightroomKeyboardShortcutsNavigateRateAndFlagSelectedAsset() throws {
        let models = try sourceFile("Sources/PhotoAssetManager/Models.swift")
        let store = try libraryStoreSource()
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")
        let app = try appSource()

        #expect(models.contains("enum AssetFlagState"))
        #expect(models.contains("case picked"))
        #expect(models.contains("case rejected"))
        #expect(models.contains("case unflagged"))
        #expect(models.contains("var flagState: AssetFlagState"))

        #expect(database.contains("flag_state TEXT"))
        #expect(database.contains("CASE WHEN flag = 1 THEN 'picked' ELSE 'unflagged' END"))
        #expect(functionBody(named: "updateAssetMetadata", in: database).contains("flag_state = ?"))
        #expect(functionBody(named: "updateAssetMetadata", in: database).contains("asset.flagState.rawValue"))

        #expect(store.contains("func selectAdjacentAsset(_ direction: AssetSelectionDirection)"))
        #expect(store.contains("func setSelectedAssetRating(_ rating: Int)"))
        #expect(store.contains("func setSelectedAssetFlagState(_ flagState: AssetFlagState)"))

        #expect(app.contains("AssetSelectionCommands(library: library)"))
        #expect(app.contains(".keyboardShortcut(.leftArrow, modifiers: [])"))
        #expect(app.contains(".keyboardShortcut(.rightArrow, modifiers: [])"))
        #expect(app.contains(".keyboardShortcut(\"1\", modifiers: [])"))
        #expect(app.contains(".keyboardShortcut(\"5\", modifiers: [])"))
        #expect(app.contains(".keyboardShortcut(\"p\", modifiers: [])"))
        #expect(app.contains(".keyboardShortcut(\"x\", modifiers: [])"))
        #expect(app.contains(".keyboardShortcut(\"u\", modifiers: [])"))
    }

    private func contentViewSource() throws -> String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repositoryRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let contentView = repositoryRoot
            .appendingPathComponent("Sources/PhotoAssetManager/ContentView.swift")
        return try String(contentsOf: contentView, encoding: .utf8)
    }

    private func libraryStoreSource() throws -> String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repositoryRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let libraryStore = repositoryRoot
            .appendingPathComponent("Sources/PhotoAssetManager/LibraryStore.swift")
        return try String(contentsOf: libraryStore, encoding: .utf8)
    }

    private func appSource() throws -> String {
        try sourceFile("Sources/PhotoAssetManager/PhotoAssetManagerApp.swift")
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
