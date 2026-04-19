import Foundation
import Testing

struct SidebarUXTests {
    @Test func folderRowsReceiveInterruptedScanContextAndHideTrackingText() throws {
        let source = try contentViewSource()

        #expect(source.contains("interruptedScanPath: library.interruptedScanPath"))
        #expect(!source.contains("追踪中"))
    }

    @Test func foldersInSidebarAreAlwaysTrackedAndUseRowMenu() throws {
        let source = try contentViewSource()
        let store = try libraryStoreSource()
        let app = try appSource()

        #expect(source.contains("Menu"))
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

    @Test func thumbnailMaintenanceIsCollapsedInsideSidebar() throws {
        let source = try contentViewSource()

        #expect(!source.contains("Section(\"缩略图存储\")"))
        #expect(source.contains("ThumbnailStoragePopover()"))
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
        #expect(source.contains(".onTapGesture"))
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

        #expect(source.contains("Image(systemName: isExpanded ? \"chevron.down\" : \"chevron.right\")"))
        #expect(source.contains(".contentShape(Rectangle())"))
        #expect(source.contains(".onTapGesture"))
        #expect(source.contains("node.hasChildren"))
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
