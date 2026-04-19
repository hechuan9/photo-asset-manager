import SwiftUI

private enum AppPalette {
    static let sidebarBackground = Color(nsColor: NSColor(name: nil) { appearance in
        let isDark = appearance.bestMatch(from: [.darkAqua, .aqua]) == .darkAqua
        return isDark
            ? NSColor(calibratedRed: 0.13, green: 0.15, blue: 0.16, alpha: 1)
            : NSColor(calibratedRed: 0.95, green: 0.96, blue: 0.96, alpha: 1)
    })

    static let folderText = Color(nsColor: NSColor(name: nil) { appearance in
        let isDark = appearance.bestMatch(from: [.darkAqua, .aqua]) == .darkAqua
        return isDark
            ? NSColor(calibratedWhite: 0.82, alpha: 1)
            : NSColor(calibratedWhite: 0.24, alpha: 1)
    })
}

struct ContentView: View {
    @EnvironmentObject private var library: LibraryStore
    @State private var pendingImportSource: URL?

    var body: some View {
        NavigationSplitView {
            SidebarView()
                .navigationSplitViewColumnWidth(min: 220, ideal: 250)
        } content: {
            AssetBrowserView()
                .navigationSplitViewColumnWidth(min: 520, ideal: 760)
        } detail: {
            DetailView()
                .navigationSplitViewColumnWidth(min: 320, ideal: 420)
        }
        .safeAreaInset(edge: .bottom) {
            BackgroundTaskBar()
        }
        .toolbar {
            ToolbarItemGroup {
                Button("导入照片", systemImage: "square.and.arrow.down") {
                    pendingImportSource = library.choosePhotoImportSource()
                }
                .disabled(library.isBusy)
                Button("添加文件夹", systemImage: "plus") {
                    library.chooseAndAddFolders(scanImmediately: false)
                }
                .disabled(library.isBusy)
                Button("校验文件状态") {
                    library.forceAvailabilityRefreshInBackground()
                }
                .disabled(library.isBusy)
            }
            ToolbarItemGroup {
                Button("归档到 NAS") {
                    library.archiveSelected()
                }
                .disabled(library.selectedAsset == nil || library.isBusy)
                Button("同步变更") {
                    library.syncSelected()
                }
                .disabled(library.selectedAsset == nil || library.isBusy)
            }
        }
        .sheet(isPresented: Binding(
            get: { pendingImportSource != nil },
            set: { if !$0 { pendingImportSource = nil } }
        )) {
            if let source = pendingImportSource {
                PhotoImportTargetDialog(
                    source: source,
                    close: {
                        pendingImportSource = nil
                    }
                )
            }
        }
        .sheet(isPresented: Binding(
            get: { library.blockingTask != nil },
            set: { _ in }
        )) {
            if let task = library.blockingTask {
                BlockingTaskProgressView(task: task)
                    .interactiveDismissDisabled(true)
            }
        }
        .alert("操作失败", isPresented: Binding(
            get: { library.lastError != nil },
            set: { if !$0 { library.lastError = nil } }
        )) {
            Button("关闭", role: .cancel) {}
        } message: {
            Text(library.lastError ?? "")
        }
    }
}

struct PhotoImportTargetDialog: View {
    @EnvironmentObject private var library: LibraryStore
    var source: URL
    var close: () -> Void
    @State private var currentPath: String?

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            Text("导入照片")
                .font(.headline)
            Text(source.path)
                .foregroundStyle(.secondary)
                .lineLimit(2)
                .truncationMode(.middle)
                .textSelection(.enabled)

            Divider()

            HStack(spacing: 8) {
                Button("上一级") {
                    currentPath = parentPath(of: currentPath ?? "")
                }
                .disabled(currentPath == nil)

                VStack(alignment: .leading, spacing: 2) {
                    Text("目标位置")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    Text(currentPath ?? "目标根目录")
                        .lineLimit(1)
                        .truncationMode(.middle)
                }

                Spacer()
            }

            if let currentTarget {
                Button("导入到这里") {
                    library.importPhotoFolder(source, to: currentTarget)
                    close()
                }
                .disabled(library.isBusy)
            }

            if childTargets.isEmpty {
                Text("没有可用的资料库目标。请先添加或刷新照片文件夹。")
                    .foregroundStyle(.secondary)
                    .frame(maxWidth: .infinity, minHeight: 160, alignment: .center)
            } else {
                List(childTargets) { target in
                    HStack(spacing: 10) {
                        Image(systemName: "folder")
                            .foregroundStyle(.secondary)
                        VStack(alignment: .leading, spacing: 2) {
                            Text(displayName(for: target.path))
                                .lineLimit(1)
                            Text(target.path)
                                .font(.caption)
                                .foregroundStyle(.secondary)
                                .lineLimit(1)
                                .truncationMode(.middle)
                        }
                        Spacer()
                        Button("导入到这里") {
                            library.importPhotoFolder(source, to: target)
                            close()
                        }
                        .disabled(library.isBusy)
                        Button("进入") {
                            currentPath = target.path
                        }
                        .disabled(immediateChildren(of: target.path).isEmpty)
                    }
                }
                .frame(minHeight: 260)
            }

            HStack {
                Spacer()
                Button("取消", role: .cancel) {
                    close()
                }
            }
        }
        .frame(width: 560, height: 420, alignment: .leading)
        .padding(18)
    }

    private var targets: [PhotoImportTarget] {
        library.availablePhotoImportTargets()
    }

    private var currentTarget: PhotoImportTarget? {
        guard let currentPath else { return nil }
        return targets.first { normalizedPath($0.path) == normalizedPath(currentPath) }
    }

    private var childTargets: [PhotoImportTarget] {
        immediateChildren(of: currentPath)
    }

    private func immediateChildren(of parent: String?) -> [PhotoImportTarget] {
        let targetPaths = Set(targets.map { normalizedPath($0.path) })
        return targets.filter { target in
            let path = normalizedPath(target.path)
            let targetParent = parentPath(of: path)
            if let parent {
                return targetParent == normalizedPath(parent)
            }
            guard let targetParent else { return true }
            return !targetPaths.contains(targetParent)
        }
        .sorted { $0.path.localizedStandardCompare($1.path) == .orderedAscending }
    }

    private func parentPath(of path: String) -> String? {
        let normalized = normalizedPath(path)
        guard !normalized.isEmpty, normalized != "/" else { return nil }
        let parent = URL(fileURLWithPath: normalized, isDirectory: true).deletingLastPathComponent().path
        return normalizedPath(parent)
    }

    private func displayName(for path: String) -> String {
        let normalized = normalizedPath(path)
        return normalized == "/" ? "/" : URL(fileURLWithPath: normalized, isDirectory: true).lastPathComponent
    }

    private func normalizedPath(_ path: String) -> String {
        guard path.count > 1 else { return path }
        return path.hasSuffix("/") ? String(path.dropLast()) : path
    }
}

struct BackgroundTaskBar: View {
    @EnvironmentObject private var library: LibraryStore

    var body: some View {
        VStack(spacing: 0) {
            Divider()
            HStack(spacing: 10) {
                if let task = library.backgroundTask {
                    if task.isFinished {
                        Image(systemName: "checkmark.circle")
                            .foregroundStyle(.secondary)
                    } else {
                        ProgressView()
                            .controlSize(.small)
                    }
                    Text(task.phase)
                        .lineLimit(1)
                    if task.totalItems > 0 {
                        ProgressView(value: Double(task.completedItems), total: Double(task.totalItems))
                            .frame(width: 160)
                        Text("\(task.completedItems) / \(task.totalItems)")
                            .foregroundStyle(.secondary)
                    }
                    if !task.message.isEmpty {
                        Text(task.message)
                            .foregroundStyle(.secondary)
                            .lineLimit(1)
                    }
                    if !task.currentPath.isEmpty {
                        Text(task.currentPath)
                            .foregroundStyle(.tertiary)
                            .lineLimit(1)
                            .truncationMode(.middle)
                    }
                } else {
                    Image(systemName: "checkmark.circle")
                        .foregroundStyle(.secondary)
                    Text("就绪")
                        .foregroundStyle(.secondary)
                }
                Spacer()
            }
            .font(.caption)
            .padding(.horizontal, 12)
            .padding(.vertical, 6)
            .frame(maxWidth: .infinity)
            .background(.bar)
        }
    }
}

struct BlockingTaskProgressView: View {
    var task: BlockingTaskReport

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            Text(task.title)
                .font(.title3)
                .fontWeight(.semibold)

            if task.totalItems > 0 {
                ProgressView(value: Double(task.completedItems), total: Double(task.totalItems))
                HStack {
                    Text("\(task.completedItems) / \(task.totalItems)")
                    Spacer()
                    Text(percentText)
                }
                .foregroundStyle(.secondary)
            } else {
                ProgressView()
                Text("正在准备...")
                    .foregroundStyle(.secondary)
            }

            if !task.phase.isEmpty {
                Text(task.phase)
                    .fontWeight(.medium)
            }

            if !task.currentPath.isEmpty {
                VStack(alignment: .leading, spacing: 4) {
                    Text("当前文件")
                        .foregroundStyle(.secondary)
                    Text(task.currentPath)
                        .lineLimit(3)
                        .textSelection(.enabled)
                }
            }

            if !task.message.isEmpty {
                Text(task.message)
                    .foregroundStyle(.secondary)
            }

            Text("任务进行中，请保持应用打开。其它操作会等这个任务结束后再继续。")
                .font(.caption)
                .foregroundStyle(.secondary)
        }
        .frame(width: 460)
        .padding(24)
    }

    private var percentText: String {
        guard task.totalItems > 0 else { return "0%" }
        let percent = Int((Double(task.completedItems) / Double(task.totalItems) * 100).rounded())
        return "\(percent)%"
    }
}

struct SidebarView: View {
    @EnvironmentObject private var library: LibraryStore
    @State private var expandedFolderNodeIDs: Set<String> = []
    @State private var pendingMoveSource: FolderMoveSource?
    @State private var pendingFolderRemovalSource: FolderMoveSource?
    @State private var pendingAssetFileMoveRequest: AssetFileMoveRequest?

    var body: some View {
        List(selection: Binding(
            get: { library.filter.status },
            set: { library.setStatusFilter($0) }
        )) {
            Button {
                library.setStatusFilter(nil)
                library.clearBrowseSelection()
            } label: {
                Label("全部资产", systemImage: "photo.on.rectangle")
            }
            .buttonStyle(.plain)

            Section("工作流") {
                ForEach(AssetStatus.allCases) { status in
                    StatusRow(status: status, count: library.counts[status] ?? 0)
                        .tag(Optional(status))
                }
            }

            Section {
                if library.sourceDirectories.isEmpty {
                    Text("还没有文件夹")
                        .foregroundStyle(.secondary)
                } else {
                    ForEach(SourceDirectoryTreeBuilder.build(
                        library.sourceDirectories,
                        indexedBrowseFolders: library.indexedBrowseFolders,
                        expandedNodeIDs: expandedFolderNodeIDs
                    )) { node in
                        SourceDirectoryNodeRow(
                            node: node,
                            interruptedScanPath: library.interruptedScanPath,
                            isExpanded: expandedFolderNodeIDs.contains(node.id),
                            isSelected: library.filter.browseSelection?.path == node.path || library.pendingBrowseSelection?.path == node.path,
                            toggleExpansion: {
                                if expandedFolderNodeIDs.contains(node.id) {
                                    expandedFolderNodeIDs.remove(node.id)
                                } else {
                                    expandedFolderNodeIDs.insert(node.id)
                                }
                            },
                            select: {
                                library.selectFolder(path: node.path)
                            },
                            openMoveDialog: { source in
                                pendingMoveSource = source
                            },
                            openRemovalDialog: { source in
                                pendingFolderRemovalSource = source
                            },
                            openAssetMoveConfirmation: { request in
                                pendingAssetFileMoveRequest = request
                            }
                        )
                    }
                }
            } header: {
                HStack {
                    Text("文件夹")
                    Spacer()
                    ThumbnailStoragePopover()
                    Button {
                        library.scanTrackedSources()
                    } label: {
                        Image(systemName: "arrow.clockwise")
                    }
                    .buttonStyle(.plain)
                    .disabled(library.isBusy || library.sourceDirectories.isEmpty)
                    .help("刷新所有文件夹")
                    Button {
                        library.chooseAndAddFolders(scanImmediately: false)
                    } label: {
                        Image(systemName: "plus")
                    }
                    .buttonStyle(.plain)
                    .disabled(library.isBusy)
                    .help("添加文件夹")
                }
            }

            if library.isScanning {
                Section("扫描中") {
                    VStack(alignment: .leading, spacing: 8) {
                        if library.scanReport.totalFiles > 0 {
                            ProgressView(value: Double(library.scanReport.scannedFiles), total: Double(library.scanReport.totalFiles))
                        } else {
                            ProgressView()
                        }
                        if !library.scanReport.phase.isEmpty {
                            Text(library.scanReport.phase)
                                .fontWeight(.medium)
                        }
                        if !library.scanReport.currentPath.isEmpty {
                            Text(library.scanReport.currentPath)
                                .lineLimit(2)
                                .foregroundStyle(.secondary)
                        }
                        Text("已发现 \(library.scanReport.discoveredFiles) 个候选文件")
                        if library.scanReport.totalFiles > 0 {
                            Text("已扫描 \(library.scanReport.scannedFiles) / \(library.scanReport.totalFiles)")
                        } else {
                            Text("已扫描 \(library.scanReport.scannedFiles) 个文件")
                        }
                        Text("新增 \(library.scanReport.importedAssets)，位置更新 \(library.scanReport.newLocations)")
                            .foregroundStyle(.secondary)
                        if library.scanReport.skippedExistingFiles > 0 {
                            Text("已跳过 \(library.scanReport.skippedExistingFiles) 个已入库文件")
                                .foregroundStyle(.secondary)
                        }
                    }
                    .font(.callout)
                    .padding(.vertical, 4)
                }
            }
        }
        .scrollContentBackground(.hidden)
        .background(AppPalette.sidebarBackground)
        .sheet(item: $pendingMoveSource) { source in
            FolderMoveTargetDialog(
                source: source,
                close: {
                    pendingMoveSource = nil
                }
            )
        }
        .sheet(item: $pendingFolderRemovalSource) { source in
            FolderRemovalConfirmationDialog(
                source: source,
                close: {
                    pendingFolderRemovalSource = nil
                }
            )
        }
        .sheet(item: $pendingAssetFileMoveRequest) { request in
            AssetFileMoveConfirmationDialog(
                request: request,
                close: {
                    pendingAssetFileMoveRequest = nil
                }
            )
        }
    }
}

struct ThumbnailStoragePopover: View {
    @EnvironmentObject private var library: LibraryStore
    @State private var isPresented = false

    var body: some View {
        Button {
            isPresented.toggle()
        } label: {
            Image(systemName: "rectangle.stack")
        }
        .buttonStyle(.plain)
        .disabled(library.isBusy)
        .help("缩略图维护")
        .popover(isPresented: $isPresented) {
            VStack(alignment: .leading, spacing: 12) {
                Text("缩略图存储")
                    .font(.headline)

                Text(library.derivativeStorageURL?.path ?? "未设置，不生成新缩略图")
                    .lineLimit(3)
                    .foregroundStyle(.secondary)
                    .textSelection(.enabled)

                if let migrationReport = library.migrationReport {
                    Text(migrationReport)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }

                HStack {
                    Button("迁移到...") {
                        library.chooseDerivativeMigrationLocation()
                        isPresented = false
                    }
                    .disabled(library.isBusy)

                    if library.derivativeStorageURL != nil {
                        Button("清除") {
                            library.clearDerivativeStorageLocation()
                            isPresented = false
                        }
                        .disabled(library.isBusy)
                    }
                }
            }
            .frame(width: 320, alignment: .leading)
            .padding(14)
        }
    }
}

struct StatusRow: View {
    var status: AssetStatus
    var count: Int

    var body: some View {
        HStack {
            Text(status.label)
            Spacer()
            Text("\(count)")
                .foregroundStyle(.secondary)
        }
    }
}

struct SourceDirectoryRow: View {
    @EnvironmentObject private var library: LibraryStore
    var source: SourceDirectory?
    var path: String
    var displayName: String
    var interruptedScanPath: String?
    var showsMenu = true
    var openRemovalDialog: ((FolderMoveSource) -> Void)?

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack {
                Text(displayName)
                    .lineLimit(2)
                    .foregroundStyle(AppPalette.folderText)
                    .textSelection(.enabled)
                Spacer()
                if showsMenu, let source {
                    Menu {
                        Button("刷新") {
                            library.scanSource(source)
                        }
                        if isInterruptedScanSource {
                            Button("继续扫描") {
                                library.resumeInterruptedScan()
                            }
                        }
                        Divider()
                        Button("移除", role: .destructive) {
                            openRemovalDialog?(FolderMoveSource(source: source))
                        }
                        .disabled(openRemovalDialog == nil)
                    } label: {
                        Image(systemName: "ellipsis")
                    }
                    .menuStyle(.borderlessButton)
                    .disabled(library.isBusy)
                    .help("文件夹操作")
                }
            }
            if let lastScannedAt = source?.lastScannedAt {
                Text("上次扫描 \(lastScannedAt.formatted(date: .abbreviated, time: .shortened))")
                    .font(.caption)
                    .foregroundStyle(.tertiary)
            }
        }
        .font(.callout)
        .padding(.vertical, 4)
    }

    private var isInterruptedScanSource: Bool {
        guard let interruptedScanPath else { return false }
        return interruptedScanPath == path || interruptedScanPath.hasPrefix(path + "/")
    }
}

struct SourceDirectoryNodeRow: View {
    @EnvironmentObject private var library: LibraryStore
    @State private var isHovering = false
    var node: SourceDirectoryNode
    var interruptedScanPath: String?
    var isExpanded: Bool
    var isSelected: Bool
    var toggleExpansion: () -> Void
    var select: () -> Void
    var openMoveDialog: (FolderMoveSource) -> Void
    var openRemovalDialog: (FolderMoveSource) -> Void
    var openAssetMoveConfirmation: (AssetFileMoveRequest) -> Void

    private var moveSource: FolderMoveSource {
        node.source.map(FolderMoveSource.init(source:)) ?? FolderMoveSource(path: node.path)
    }

    var body: some View {
        HStack(alignment: .top, spacing: 6) {
            Spacer()
                .frame(width: CGFloat(node.depth) * 14)
            if node.hasChildren {
                Button {
                    toggleExpansion()
                } label: {
                    Image(systemName: isExpanded ? "chevron.down" : "chevron.right")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .frame(width: 14, height: 24)
                        .contentShape(Rectangle())
                }
                .buttonStyle(.plain)
                .disabled(library.pendingBrowseSelection != nil)
            } else {
                Spacer()
                    .frame(width: 14)
            }

            Button(action: select) {
                SourceDirectoryRow(
                    source: node.source,
                    path: node.path,
                    displayName: node.displayName,
                    interruptedScanPath: interruptedScanPath,
                    showsMenu: false
                )
            }
            .buttonStyle(FolderRowButtonStyle(isSelected: isSelected, isHovering: isHovering))
            .disabled(library.pendingBrowseSelection != nil)
            .onHover { hovering in
                isHovering = hovering
            }

            Menu {
                FolderActionMenuItems(
                    source: node.source,
                    moveSource: moveSource,
                    interruptedScanPath: interruptedScanPath,
                    nodePath: node.path,
                    openMoveDialog: openMoveDialog,
                    openRemovalDialog: openRemovalDialog
                )
            } label: {
                Image(systemName: "ellipsis")
                    .frame(width: 22, height: 24)
            }
            .menuStyle(.borderlessButton)
            .disabled(library.isBusy)
        }
        .contextMenu {
            FolderActionMenuItems(
                source: node.source,
                moveSource: moveSource,
                interruptedScanPath: interruptedScanPath,
                nodePath: node.path,
                openMoveDialog: openMoveDialog,
                openRemovalDialog: openRemovalDialog
            )
        }
        .dropDestination(for: String.self) { items, _ in
            guard !library.isBusy,
                  let assetIDs = items.lazy.compactMap(AssetDragPayload.assetIDs).first,
                  !assetIDs.isEmpty else {
                return false
            }
            openAssetMoveConfirmation(AssetFileMoveRequest(
                assetIDs: assetIDs,
                target: FolderMoveTarget(path: node.path, displayName: node.displayName)
            ))
            return true
        } isTargeted: { targeted in
            isHovering = targeted
        }
    }
}

struct AssetFileMoveConfirmationDialog: View {
    @EnvironmentObject private var library: LibraryStore
    var request: AssetFileMoveRequest
    var close: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            Text("移动选中文件？")
                .font(.headline)
            Text("将移动 \(request.assetIDs.count) 个选中资产的在线原片、sidecar 和导出文件。移动会先复制并校验 hash，通过后才删除源文件。")
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
            Text(request.target.path)
                .font(.caption)
                .foregroundStyle(.secondary)
                .lineLimit(2)
                .truncationMode(.middle)
                .textSelection(.enabled)

            HStack {
                Spacer()
                Button("取消", role: .cancel) {
                    close()
                }
                Button("确认移动") {
                    library.moveAssets(request.assetIDs, to: request.target)
                    close()
                }
                .keyboardShortcut(.defaultAction)
                .disabled(library.isBusy)
            }
        }
        .frame(width: 440, alignment: .leading)
        .padding(18)
    }
}

struct FolderRemovalConfirmationDialog: View {
    @EnvironmentObject private var library: LibraryStore
    var source: FolderMoveSource
    var close: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            Text("移除文件夹？")
                .font(.headline)
            Text("仅移除会把这个文件夹从资料库列表中移除，不检查也不改动磁盘文件。彻底删除会先扫描文件夹；只要发现任何文件，就阻止删除。")
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
            Text(source.path)
                .font(.caption)
                .foregroundStyle(.secondary)
                .lineLimit(3)
                .truncationMode(.middle)
                .textSelection(.enabled)

            HStack {
                Spacer()
                Button("取消", role: .cancel) {
                    close()
                }
                Button("仅移除") {
                    library.removeFolder(source, deleteEmptyFolder: false)
                    close()
                }
                .disabled(library.isBusy || source.sourceDirectoryID == nil)
                Button("彻底删除", role: .destructive) {
                    library.removeFolder(source, deleteEmptyFolder: true)
                    close()
                }
                .keyboardShortcut(.defaultAction)
                .disabled(library.isBusy)
            }
        }
        .frame(width: 460, alignment: .leading)
        .padding(18)
    }
}

private enum AssetDragPayload {
    private static let prefix = "photo-asset-manager.assets"

    static func string(assetIDs: [UUID]) -> String {
        ([prefix] + assetIDs.map(\.uuidString)).joined(separator: "\n")
    }

    static func assetIDs(from payload: String) -> [UUID]? {
        let lines = payload.split(separator: "\n", omittingEmptySubsequences: true).map(String.init)
        guard lines.first == prefix else { return nil }
        let ids = lines.dropFirst().compactMap(UUID.init(uuidString:))
        return ids.isEmpty ? nil : ids
    }
}

struct FolderActionMenuItems: View {
    @EnvironmentObject private var library: LibraryStore
    var source: SourceDirectory?
    var moveSource: FolderMoveSource
    var interruptedScanPath: String?
    var nodePath: String
    var openMoveDialog: (FolderMoveSource) -> Void
    var openRemovalDialog: (FolderMoveSource) -> Void

    var body: some View {
        if let source {
            Button("刷新") {
                library.scanSource(source)
            }
            if isInterruptedScanSource {
                Button("继续扫描") {
                    library.resumeInterruptedScan()
                }
            }
            Divider()
        }

        Button("移动到...") {
            openMoveDialog(moveSource)
        }

        Divider()
        Button("移除", role: .destructive) {
            openRemovalDialog(moveSource)
        }
    }

    private var isInterruptedScanSource: Bool {
        guard let interruptedScanPath else { return false }
        return interruptedScanPath == nodePath || interruptedScanPath.hasPrefix(nodePath + "/")
    }
}

struct FolderMoveTargetDialog: View {
    @EnvironmentObject private var library: LibraryStore
    var source: FolderMoveSource
    var close: () -> Void
    @State private var currentPath: String?
    @State private var createdTargets: [FolderMoveTarget] = []
    @State private var pendingCreateFolderParentPath: String?

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            Text("移动文件夹")
                .font(.headline)
            Text(source.path)
                .foregroundStyle(.secondary)
                .lineLimit(2)
                .truncationMode(.middle)
                .textSelection(.enabled)

            Divider()

            HStack(spacing: 8) {
                Button("上一级") {
                    currentPath = parentPath(of: currentPath ?? "")
                }
                .disabled(currentPath == nil)

                VStack(alignment: .leading, spacing: 2) {
                    Text("当前位置")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    Text(currentPath ?? "目标根目录")
                        .lineLimit(1)
                        .truncationMode(.middle)
                }

                Spacer()

                Button("添加文件夹") {
                    pendingCreateFolderParentPath = currentPath
                }
                .disabled(currentPath == nil || library.isBusy)
            }

            if let currentTarget {
                Button("移动到这里") {
                    library.moveFolder(source, to: currentTarget)
                    close()
                }
                .disabled(library.isBusy)
            }

            if childTargets.isEmpty {
                Text("没有下一级可选目标。")
                    .foregroundStyle(.secondary)
                    .frame(maxWidth: .infinity, minHeight: 120, alignment: .center)
            } else {
                List(childTargets) { target in
                    HStack(spacing: 10) {
                        Image(systemName: "folder")
                            .foregroundStyle(.secondary)
                        VStack(alignment: .leading, spacing: 2) {
                            Text(displayName(for: target.path))
                                .lineLimit(1)
                            Text(target.path)
                                .font(.caption)
                                .foregroundStyle(.secondary)
                                .lineLimit(1)
                                .truncationMode(.middle)
                        }
                        Spacer()
                        Button("移动到这里") {
                            library.moveFolder(source, to: target)
                            close()
                        }
                        .disabled(library.isBusy)
                        Button("进入") {
                            currentPath = target.path
                        }
                        .disabled(immediateChildren(of: target.path).isEmpty)
                    }
                }
                .frame(minHeight: 240)
            }

            HStack {
                Spacer()
                Button("取消", role: .cancel) {
                    close()
                }
            }
        }
        .frame(width: 560, height: 420, alignment: .leading)
        .padding(18)
        .sheet(isPresented: Binding(
            get: { pendingCreateFolderParentPath != nil },
            set: { if !$0 { pendingCreateFolderParentPath = nil } }
        )) {
            if let parentPath = pendingCreateFolderParentPath {
                FolderCreateDialog(
                    parentPath: parentPath,
                    create: { name in
                        try createFolder(parentPath: parentPath, name: name)
                    },
                    cancel: {
                        pendingCreateFolderParentPath = nil
                    }
                )
            }
        }
    }

    private var targets: [FolderMoveTarget] {
        var targetsByPath: [String: FolderMoveTarget] = [:]
        for target in library.availableFolderMoveTargets(for: source) + createdTargets {
            let path = normalizedPath(target.path)
            targetsByPath[path] = FolderMoveTarget(path: path, displayName: target.displayName)
        }
        return targetsByPath.values.sorted { $0.path.localizedStandardCompare($1.path) == .orderedAscending }
    }

    private var currentTarget: FolderMoveTarget? {
        guard let currentPath else { return nil }
        return targets.first { normalizedPath($0.path) == normalizedPath(currentPath) }
    }

    private var childTargets: [FolderMoveTarget] {
        immediateChildren(of: currentPath)
    }

    private func immediateChildren(of parent: String?) -> [FolderMoveTarget] {
        let targetPaths = Set(targets.map { normalizedPath($0.path) })
        return targets.filter { target in
            let path = normalizedPath(target.path)
            guard path != normalizedPath(source.path) else { return false }
            let targetParent = parentPath(of: path)
            if let parent {
                return targetParent == normalizedPath(parent)
            }
            guard let targetParent else { return true }
            return !targetPaths.contains(targetParent)
        }
        .sorted { $0.path.localizedStandardCompare($1.path) == .orderedAscending }
    }

    private func parentPath(of path: String) -> String? {
        let normalized = normalizedPath(path)
        guard !normalized.isEmpty, normalized != "/" else { return nil }
        let parent = URL(fileURLWithPath: normalized, isDirectory: true).deletingLastPathComponent().path
        return normalizedPath(parent)
    }

    private func displayName(for path: String) -> String {
        let normalized = normalizedPath(path)
        return normalized == "/" ? "/" : URL(fileURLWithPath: normalized, isDirectory: true).lastPathComponent
    }

    private func normalizedPath(_ path: String) -> String {
        guard path.count > 1 else { return path }
        return path.hasSuffix("/") ? String(path.dropLast()) : path
    }

    private func createFolder(parentPath: String, name: String) throws {
        let trimmedName = name.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmedName.isEmpty else { throw FolderCreateError.emptyName }
        guard !trimmedName.contains("/") else { throw FolderCreateError.nameContainsSeparator }

        let parentURL = URL(fileURLWithPath: normalizedPath(parentPath), isDirectory: true)
        var isDirectory: ObjCBool = false
        guard FileManager.default.fileExists(atPath: parentURL.path, isDirectory: &isDirectory), isDirectory.boolValue else {
            throw FolderCreateError.parentUnavailable(parentURL.path)
        }
        guard FileManager.default.isWritableFile(atPath: parentURL.path) else {
            throw FileOperationError.cannotWrite(parentURL)
        }

        let destinationURL = parentURL.appendingPathComponent(trimmedName, isDirectory: true)
        if FileManager.default.fileExists(atPath: destinationURL.path) {
            throw FileOperationError.destinationExists(destinationURL)
        }

        try FileManager.default.createDirectory(at: destinationURL, withIntermediateDirectories: false)
        let created = FolderMoveTarget(path: normalizedPath(destinationURL.path), displayName: trimmedName)
        createdTargets.removeAll { normalizedPath($0.path) == created.path }
        createdTargets.append(created)
        currentPath = created.path
        pendingCreateFolderParentPath = nil
    }
}

struct FolderCreateDialog: View {
    var parentPath: String
    var create: (String) throws -> Void
    var cancel: () -> Void
    @State private var folderName = ""
    @State private var errorMessage: String?

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            Text("创建文件夹")
                .font(.headline)
            Text(parentPath)
                .foregroundStyle(.secondary)
                .lineLimit(2)
                .truncationMode(.middle)
                .textSelection(.enabled)

            TextField("文件夹名称", text: $folderName)
                .textFieldStyle(.roundedBorder)

            if let errorMessage {
                Text(errorMessage)
                    .foregroundStyle(.red)
                    .textSelection(.enabled)
            }

            HStack {
                Spacer()
                Button("取消", role: .cancel) {
                    cancel()
                }
                Button("创建") {
                    do {
                        try create(folderName)
                    } catch {
                        errorMessage = error.fullTrace
                    }
                }
                .keyboardShortcut(.defaultAction)
            }
        }
        .frame(width: 420, alignment: .leading)
        .padding(18)
    }
}

private enum FolderCreateError: LocalizedError {
    case emptyName
    case nameContainsSeparator
    case parentUnavailable(String)

    var errorDescription: String? {
        switch self {
        case .emptyName:
            "文件夹名称不能为空"
        case .nameContainsSeparator:
            "文件夹名称不能包含路径分隔符"
        case .parentUnavailable(let path):
            "当前目录不可用：\(path)"
        }
    }
}

struct FolderRowButtonStyle: ButtonStyle {
    var isSelected: Bool
    var isHovering: Bool

    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .background(backgroundColor(isPressed: configuration.isPressed))
            .overlay {
                RoundedRectangle(cornerRadius: 6)
                    .stroke(borderColor(isPressed: configuration.isPressed), lineWidth: isSelected || configuration.isPressed ? 1 : 0)
            }
            .clipShape(RoundedRectangle(cornerRadius: 6))
            .scaleEffect(configuration.isPressed ? 0.985 : 1.0)
            .animation(.easeOut(duration: 0.08), value: configuration.isPressed)
    }

    private func backgroundColor(isPressed: Bool) -> Color {
        if isPressed {
            return Color.accentColor.opacity(0.24)
        }
        if isSelected {
            return Color.accentColor.opacity(0.16)
        }
        if isHovering {
            return Color.primary.opacity(0.07)
        }
        return Color.clear
    }

    private func borderColor(isPressed: Bool) -> Color {
        if isPressed || isSelected {
            return Color.accentColor.opacity(0.55)
        }
        return Color.clear
    }
}

struct JustifiedAssetRow: Identifiable {
    let id: UUID
    let assets: [Asset]
    let height: CGFloat
    let aspectRatios: [UUID: CGFloat]
    let spacing: CGFloat

    func width(for asset: Asset) -> CGFloat {
        height * (aspectRatios[asset.id] ?? JustifiedAssetGridLayout.defaultAspectRatio)
    }
}

enum JustifiedAssetGridLayout {
    static let defaultAspectRatio: CGFloat = 1.5

    static func rows(
        assets: [Asset],
        aspectRatios: [UUID: CGFloat],
        availableWidth: CGFloat,
        targetHeight: CGFloat = 168,
        spacing: CGFloat = 1
    ) -> [JustifiedAssetRow] {
        let availableWidth = max(1, availableWidth)
        var rows: [JustifiedAssetRow] = []
        var pendingAssets: [Asset] = []
        var aspectRatioSum: CGFloat = 0

        func appendPendingRow() {
            guard !pendingAssets.isEmpty, aspectRatioSum > 0 else { return }
            let spacingWidth = spacing * CGFloat(max(0, pendingAssets.count - 1))
            let availableImageWidth = max(1, availableWidth - spacingWidth)
            let rowHeight = availableImageWidth / aspectRatioSum
            rows.append(JustifiedAssetRow(
                id: pendingAssets[0].id,
                assets: pendingAssets,
                height: rowHeight,
                aspectRatios: aspectRatios,
                spacing: spacing
            ))
            pendingAssets.removeAll(keepingCapacity: true)
            aspectRatioSum = 0
        }

        for asset in assets {
            let ratio = max(0.2, aspectRatios[asset.id] ?? defaultAspectRatio)
            pendingAssets.append(asset)
            aspectRatioSum += ratio

            let spacingWidth = spacing * CGFloat(max(0, pendingAssets.count - 1))
            let rowWidthAtTargetHeight = aspectRatioSum * targetHeight + spacingWidth
            if rowWidthAtTargetHeight >= availableWidth {
                appendPendingRow()
            }
        }

        appendPendingRow()
        return rows
    }
}

struct JustifiedAssetGrid: View {
    var assets: [Asset]
    var selectedAssetID: UUID?
    var selectedAssetIDs: Set<UUID>
    var aspectRatios: [UUID: CGFloat]
    var availableWidth: CGFloat
    var select: (Asset, EventModifiers) -> Void
    var openLoupe: (Asset) -> Void
    var loadMore: (UUID) -> Void
    var updateAspectRatio: (UUID, CGFloat) -> Void

    private let spacing: CGFloat = 1
    private let targetHeight: CGFloat = 168

    var body: some View {
        let rows = JustifiedAssetGridLayout.rows(
            assets: assets,
            aspectRatios: aspectRatios,
            availableWidth: max(1, availableWidth - spacing * 2),
            targetHeight: targetHeight,
            spacing: spacing
        )

        LazyVStack(spacing: spacing) {
            ForEach(rows) { row in
                HStack(spacing: spacing) {
                    ForEach(row.assets) { asset in
                        AssetTile(asset: asset, selected: selectedAssetIDs.contains(asset.id) || asset.id == selectedAssetID) { ratio in
                            updateAspectRatio(asset.id, ratio)
                        }
                        .frame(width: row.width(for: asset), height: row.height)
                        .highPriorityGesture(
                            ExclusiveGesture(
                                TapGesture(count: 2),
                                TapGesture(count: 1)
                            )
                            .onEnded { value in
                                switch value {
                                case .first:
                                    openLoupe(asset)
                                case .second:
                                    let modifiers = ModifierAwareClickView.currentModifiers()
                                    select(asset, modifiers)
                                }
                            }
                        )
                        .draggable(assetDragPayload(for: asset))
                        .onAppear {
                            loadMore(asset.id)
                        }
                    }
                }
                .frame(width: max(1, availableWidth - spacing * 2), height: row.height, alignment: .leading)
            }
        }
        .padding(spacing)
    }

    private func assetDragPayload(for asset: Asset) -> String {
        let draggedIDs = selectedAssetIDs.contains(asset.id) ? Array(selectedAssetIDs) : [asset.id]
        return AssetDragPayload.string(assetIDs: draggedIDs.sorted { $0.uuidString < $1.uuidString })
    }
}

private enum ModifierAwareClickView {
    @MainActor
    static func currentModifiers() -> EventModifiers {
        var modifiers: EventModifiers = []
        let flags = NSApp.currentEvent?.modifierFlags ?? []
        if flags.contains(.command) {
            modifiers.insert(.command)
        }
        if flags.contains(.shift) {
            modifiers.insert(.shift)
        }
        return modifiers
    }
}

struct AssetBrowserView: View {
    @EnvironmentObject private var library: LibraryStore
    @State private var aspectRatios: [UUID: CGFloat] = [:]
    @State private var loupeAssetID: UUID?

    var body: some View {
        Group {
            if let loupeAssetID, let loupeAsset = library.assets.first(where: { $0.id == loupeAssetID }) {
                LightroomLoupeView(
                    asset: loupeAsset,
                    assets: library.assets,
                    select: { asset in
                        library.selectAsset(asset, modifiers: [])
                        self.loupeAssetID = asset.id
                    },
                    close: {
                        self.loupeAssetID = nil
                    }
                )
            } else {
                VStack(spacing: 0) {
                    FilterBar()
                    Divider()
                    if library.assets.isEmpty {
                        EmptyLibraryView()
                    } else {
                        GeometryReader { proxy in
                            ScrollView {
                                JustifiedAssetGrid(
                                    assets: library.assets,
                                    selectedAssetID: library.selectedAssetID,
                                    selectedAssetIDs: library.selectedAssetIDs,
                                    aspectRatios: aspectRatios,
                                    availableWidth: proxy.size.width,
                                    select: { asset, modifiers in
                                        library.selectAsset(asset, modifiers: modifiers)
                                    },
                                    openLoupe: { asset in
                                        library.selectAsset(asset, modifiers: [])
                                        loupeAssetID = asset.id
                                    },
                                    loadMore: { assetID in
                                        library.loadMoreAssetsIfNeeded(currentAssetID: assetID)
                                    },
                                    updateAspectRatio: { assetID, ratio in
                                        aspectRatios[assetID] = ratio
                                    }
                                )
                            }
                        }
                        .background(Color.black)
                    }
                }
            }
        }
    }
}

struct LightroomLoupeView: View {
    var asset: Asset
    var assets: [Asset]
    var select: (Asset) -> Void
    var close: () -> Void

    var body: some View {
        VStack(spacing: 0) {
            HStack {
                Button("返回图库") {
                    close()
                }
                .keyboardShortcut(.escape, modifiers: [])
                .buttonStyle(.bordered)

                Spacer()

                Text(asset.originalFilename)
                    .font(.callout)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .truncationMode(.middle)
            }
            .padding(.horizontal, 12)
            .padding(.vertical, 8)

            ZStack {
                Color.black
                AssetPreviewImage(asset: asset, contentMode: .fit, placeholderSize: 72)
                    .padding(.horizontal, 24)
                    .padding(.vertical, 16)
            }

            LoupeFilmstripView(
                assets: assets,
                selectedAssetID: asset.id,
                select: select
            )
        }
        .background(Color.black)
    }
}

struct LoupeFilmstripView: View {
    var assets: [Asset]
    var selectedAssetID: UUID
    var select: (Asset) -> Void

    var body: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 2) {
                ForEach(assets) { filmstripAsset in
                    Button {
                        select(filmstripAsset)
                    } label: {
                        ZStack {
                            Color.black
                            AssetPreviewImage(asset: filmstripAsset, contentMode: .fill, placeholderSize: 18)
                        }
                        .frame(width: 70, height: 52)
                        .clipped()
                        .overlay {
                            Rectangle()
                                .stroke(
                                    filmstripAsset.id == selectedAssetID ? Color.white : Color.clear,
                                    lineWidth: 2
                                )
                        }
                    }
                    .buttonStyle(.plain)
                }
            }
            .padding(.horizontal, 2)
            .padding(.vertical, 3)
        }
        .frame(height: 60)
        .background(Color(nsColor: .windowBackgroundColor).opacity(0.35))
    }
}

struct FilterBar: View {
    @EnvironmentObject private var library: LibraryStore

    var body: some View {
        VStack(spacing: 10) {
            HStack {
                TextField("搜索文件名、标签、路径", text: $library.filter.searchText)
                    .textFieldStyle(.roundedBorder)
                TextField("相机", text: $library.filter.camera)
                    .textFieldStyle(.roundedBorder)
                    .frame(width: 140)
                TextField("扩展名", text: $library.filter.fileExtension)
                    .textFieldStyle(.roundedBorder)
                    .frame(width: 100)
                Stepper("最低 \(library.filter.minimumRating) 星", value: $library.filter.minimumRating, in: 0...5)
                    .frame(width: 130)
                Button("应用") {
                    library.refresh()
                }
                Button("重置") {
                    let status = library.filter.status
                    library.filter = LibraryFilter(status: status)
                    library.refresh()
                }
            }
            HStack {
                TextField("标签", text: $library.filter.tag)
                    .textFieldStyle(.roundedBorder)
                TextField("目录前缀", text: $library.filter.directory)
                    .textFieldStyle(.roundedBorder)
                Button("记录导出") {
                    library.recordExportForSelected()
                }
                .disabled(library.selectedAsset == nil)
            }
        }
        .padding(12)
    }
}

struct EmptyLibraryView: View {
    var body: some View {
        VStack(spacing: 14) {
            Image(systemName: "photo.stack")
                .font(.system(size: 48))
                .foregroundStyle(.secondary)
            Text("还没有资产")
                .font(.title3)
            Text("先添加文件夹，再用刷新扫描清单。原片不会被移动，索引会记录每个文件位置。")
                .foregroundStyle(.secondary)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}

struct AssetTile: View {
    var asset: Asset
    var selected: Bool
    var onAspectRatioChange: (CGFloat) -> Void = { _ in }

    var body: some View {
        ZStack {
            Rectangle()
                .fill(Color.black)
            AssetPreviewImage(
                asset: asset,
                contentMode: .fit,
                placeholderSize: 34,
                onAspectRatioChange: onAspectRatioChange
            )
        }
        .clipped()
        .clipShape(RoundedRectangle(cornerRadius: 0))
        .border(Color(nsColor: .selectedContentBackgroundColor), width: selected ? 3 : 0)
        .contentShape(Rectangle())
        .accessibilityLabel(asset.originalFilename)
    }
}

struct AssetPreviewImage: View {
    var asset: Asset
    var contentMode: ContentMode
    var placeholderSize: CGFloat
    var onAspectRatioChange: (CGFloat) -> Void = { _ in }
    @StateObject private var loader = ImagePreviewLoader()

    var body: some View {
        Group {
            if let image = loader.image {
                Image(nsImage: image)
                    .resizable()
                    .aspectRatio(contentMode: contentMode)
                    .onAppear {
                        reportAspectRatio(image.size)
                    }
            } else {
                Image(systemName: "photo")
                    .font(.system(size: placeholderSize))
                    .foregroundStyle(.secondary)
            }
        }
        .task(id: cacheKey) {
            await loader.load(thumbnailPath: asset.thumbnailPath, primaryPath: asset.primaryPath, cacheKey: cacheKey)
        }
    }

    private var cacheKey: String {
        asset.thumbnailPath ?? asset.primaryPath ?? asset.id.uuidString
    }

    private func reportAspectRatio(_ size: NSSize) {
        guard size.width > 0, size.height > 0 else { return }
        onAspectRatioChange(size.width / size.height)
    }
}

@MainActor
final class ImagePreviewCache {
    static let shared = ImagePreviewCache()

    private let cache = NSCache<NSString, NSImage>()

    private init() {
        cache.countLimit = 600
    }

    func image(forKey key: String) -> NSImage? {
        cache.object(forKey: key as NSString)
    }

    func insert(_ image: NSImage, forKey key: String) {
        cache.setObject(image, forKey: key as NSString)
    }
}

@MainActor
final class ImagePreviewLoader: ObservableObject {
    @Published var image: NSImage?
    private var loadedCacheKey: String?
    private var decodeTask: Task<NSImage?, Never>?

    deinit {
        decodeTask?.cancel()
    }

    func load(thumbnailPath: String?, primaryPath: String?, cacheKey: String) async {
        guard loadedCacheKey != cacheKey else { return }
        decodeTask?.cancel()
        loadedCacheKey = cacheKey

        if let cached = ImagePreviewCache.shared.image(forKey: cacheKey) {
            image = cached
            return
        }

        image = nil
        let task = Task.detached(priority: .utility) { () -> NSImage? in
            PerformanceLog.measure("image-preview-decode") {
                guard !Task.isCancelled else { return nil }
                if let thumbnailPath, let image = NSImage(contentsOfFile: thumbnailPath) {
                    return image
                }
                guard !Task.isCancelled else { return nil }
                if let primaryPath {
                    return ImageRenderer.renderableImage(url: URL(fileURLWithPath: primaryPath))
                }
                return nil
            }
        }
        decodeTask = task
        defer {
            if Task.isCancelled {
                task.cancel()
            }
        }
        let loaded = await task.value

        guard !Task.isCancelled, loadedCacheKey == cacheKey else { return }
        if let loaded {
            ImagePreviewCache.shared.insert(loaded, forKey: cacheKey)
        }
        image = loaded
    }
}

struct DetailView: View {
    @EnvironmentObject private var library: LibraryStore
    @State private var draftTags = ""

    var body: some View {
        Group {
            if let asset = library.selectedAsset {
                ScrollView {
                    VStack(alignment: .leading, spacing: 18) {
                        PreviewHeader(asset: asset)
                        AssetMetadataEditor(asset: asset, draftTags: $draftTags)
                        FileInstancesView(fileInstances: library.selectedFiles)
                    }
                    .padding(16)
                }
                .onAppear {
                    draftTags = asset.tags.joined(separator: ", ")
                }
                .onChange(of: asset.id) {
                    draftTags = asset.tags.joined(separator: ", ")
                }
            } else {
                Text("选择一个资产查看详情")
                    .foregroundStyle(.secondary)
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
            }
        }
    }
}

struct PreviewHeader: View {
    var asset: Asset

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            ZStack {
                Rectangle()
                    .fill(Color(nsColor: .controlBackgroundColor))
                AssetPreviewImage(asset: asset, contentMode: .fit, placeholderSize: 46)
            }
            .frame(height: 240)
            .clipShape(RoundedRectangle(cornerRadius: 8))

            Text(asset.originalFilename)
                .font(.title3)
                .fontWeight(.semibold)
            Text(asset.primaryPath ?? "当前没有可访问原片路径")
                .font(.caption)
                .foregroundStyle(.secondary)
                .textSelection(.enabled)
        }
    }
}

struct AssetMetadataEditor: View {
    @EnvironmentObject private var library: LibraryStore
    var asset: Asset
    @Binding var draftTags: String

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("元数据")
                .font(.headline)
            Grid(alignment: .leading, horizontalSpacing: 12, verticalSpacing: 8) {
                GridRow {
                    Text("状态")
                    Text(asset.status.label)
                }
                GridRow {
                    Text("拍摄时间")
                    Text(asset.captureTime.map { $0.formatted(date: .abbreviated, time: .shortened) } ?? "未知")
                }
                GridRow {
                    Text("相机")
                    Text([asset.cameraMake, asset.cameraModel].filter { !$0.isEmpty }.joined(separator: " "))
                }
                GridRow {
                    Text("镜头")
                    Text(asset.lensModel.isEmpty ? "未知" : asset.lensModel)
                }
                GridRow {
                    Text("文件数")
                    Text("\(asset.fileCount)")
                }
            }
            .font(.callout)

            HStack {
                Text("评分")
                Picker("", selection: Binding(
                    get: { asset.rating },
                    set: { value in
                        var copy = asset
                        copy.rating = value
                        library.update(asset: copy)
                    }
                )) {
                    ForEach(0...5, id: \.self) { value in
                        Text(value == 0 ? "未评分" : "\(value) 星").tag(value)
                    }
                }
                .labelsHidden()
            }

            Toggle("精选", isOn: Binding(
                get: { asset.flag },
                set: { value in
                    var copy = asset
                    copy.flag = value
                    library.update(asset: copy)
                }
            ))

            HStack {
                TextField("标签，用逗号分隔", text: $draftTags)
                    .textFieldStyle(.roundedBorder)
                Button("保存标签") {
                    var copy = asset
                    copy.tags = draftTags
                        .split(separator: ",")
                        .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
                        .filter { !$0.isEmpty }
                    library.update(asset: copy)
                }
            }
        }
    }
}

struct FileInstancesView: View {
    @EnvironmentObject private var library: LibraryStore
    var fileInstances: [FileInstance]

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("文件位置")
                .font(.headline)
            ForEach(Array(fileInstances.enumerated()), id: \.offset) { _, file in
                VStack(alignment: .leading, spacing: 6) {
                    HStack {
                        Text(file.fileRole.label)
                            .fontWeight(.medium)
                        Text(file.storageKind.label)
                            .foregroundStyle(.secondary)
                        Text(file.syncStatus.label)
                            .foregroundStyle(file.syncStatus == .synced ? Color.secondary : Color.orange)
                        Spacer()
                        Button("打开") {
                            library.open(file: file)
                        }
                        .disabled(file.availability != .online)
                        Button("定位") {
                            library.reveal(file: file)
                        }
                        .disabled(file.availability != .online)
                    }
                    Text(file.path)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .textSelection(.enabled)
                    Text("hash \(file.contentHash.isEmpty ? "未记录" : String(file.contentHash.prefix(16))) · \(file.availability.rawValue)")
                        .font(.caption2)
                        .foregroundStyle(.tertiary)
                }
                .padding(10)
                .background(Color(nsColor: .controlBackgroundColor))
                .clipShape(RoundedRectangle(cornerRadius: 8))
            }
        }
    }
}
