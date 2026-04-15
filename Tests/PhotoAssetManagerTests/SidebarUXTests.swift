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

        #expect(!store.contains("sourceDirectories.filter(\\.isTracked)"))
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
        #expect(source.contains("MoveSourceDirectorySheet("))
        #expect(store.contains("func moveSourceDirectory"))
        #expect(database.contains("parent_source_directory_id"))
        #expect(database.contains("func moveSourceDirectory"))
        #expect(!database.contains("UPDATE file_instances SET path"))
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

    @Test func folderTreeExpandsRealFileSystemSubdirectoriesWithoutPureBlackSidebar() throws {
        let content = try contentViewSource()
        let models = try sourceFile("Sources/PhotoAssetManager/Models.swift")

        #expect(content.contains("@State private var expandedFolderNodeIDs: Set<String>"))
        #expect(content.contains("expandedFolderNodeIDs.contains(node.id)"))
        #expect(models.contains("var id: String"))
        #expect(models.contains("var source: SourceDirectory?"))
        #expect(models.contains("contentsOfDirectory"))
        #expect(models.contains("fileSystemChildNodes"))
        #expect(models.contains("hasFileSystemSubdirectories"))
        #expect(content.contains("AppPalette.sidebarBackground"))
        #expect(content.contains("AppPalette.folderText"))
        #expect(!content.contains("Color.black"))
        #expect(!content.contains("NSColor.black"))
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
}
