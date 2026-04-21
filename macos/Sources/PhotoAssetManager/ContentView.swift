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

private enum BackgroundTaskBarMetrics {
    static let height: CGFloat = 34
}

struct ContentView: View {
    @EnvironmentObject private var library: LibraryStore
    @State private var pendingImportSource: URL?

    var body: some View {
        VStack(spacing: 0) {
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
            .frame(maxWidth: .infinity, maxHeight: .infinity)

            BackgroundTaskBar()
                .frame(height: BackgroundTaskBarMetrics.height)
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
                    SyncStatusPopover()
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

struct SyncStatusPopover: View {
    @EnvironmentObject private var library: LibraryStore
    @AppStorage(SyncPreferenceKey.baseURL) private var baseURL = ""
    @AppStorage(SyncPreferenceKey.authMode) private var authModeRawValue = SyncAuthenticationMode.bearer.rawValue
    @AppStorage(SyncPreferenceKey.accessCredential) private var accessCredential = ""
    @AppStorage(SyncPreferenceKey.awsRegion) private var awsRegion = "us-east-1"
    @AppStorage(SyncPreferenceKey.awsAccessKeyID) private var awsAccessKeyID = ""
    @AppStorage(SyncPreferenceKey.awsSecretAccessKey) private var awsSecretAccessKey = ""
    @AppStorage(SyncPreferenceKey.awsSessionToken) private var awsSessionToken = ""
    @State private var isPresented = false

    private var authMode: SyncAuthenticationMode {
        get { SyncAuthenticationMode(rawValue: authModeRawValue) ?? .bearer }
        nonmutating set { authModeRawValue = newValue.rawValue }
    }

    var body: some View {
        Button {
            isPresented.toggle()
        } label: {
            Image(systemName: library.hasRemoteSyncConfiguration ? "icloud" : "icloud.slash")
        }
        .buttonStyle(.plain)
        .help("自动同步")
        .popover(isPresented: $isPresented) {
            VStack(alignment: .leading, spacing: 12) {
                Text("自动同步")
                    .font(.headline)

                Text(library.lastSyncSummary)
                    .font(.caption)
                    .foregroundStyle(.secondary)

                TextField("https://control-plane.example.com", text: $baseURL)
                    .textFieldStyle(.roundedBorder)
                Picker("认证方式", selection: Binding(
                    get: { authMode },
                    set: { authMode = $0 }
                )) {
                    ForEach(SyncAuthenticationMode.allCases) { mode in
                        Text(mode.displayName).tag(mode)
                    }
                }
                .pickerStyle(.segmented)

                if authMode == .bearer {
                    SecureField("Bearer token（可留空）", text: $accessCredential)
                        .textFieldStyle(.roundedBorder)
                } else {
                    TextField("AWS region", text: $awsRegion)
                        .textFieldStyle(.roundedBorder)
                    TextField("AWS access key ID", text: $awsAccessKeyID)
                        .textFieldStyle(.roundedBorder)
                    SecureField("AWS secret access key", text: $awsSecretAccessKey)
                        .textFieldStyle(.roundedBorder)
                    SecureField("AWS session token（可留空）", text: $awsSessionToken)
                        .textFieldStyle(.roundedBorder)
                }

                Text("macOS 会自动把 ledger 和缩略图上传到 control plane；不会把原图发给 iOS。若使用 AWS IAM，当前服务名固定为 execute-api。")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)

                HStack {
                    Button("保存") {
                        library.reloadSyncConfiguration()
                    }

                    Button("补齐 ledger") {
                        library.backfillSyncLedger()
                    }
                    .disabled(library.isBusy)

                    Button("立即同步") {
                        library.reloadSyncConfiguration(scheduleSync: false)
                        library.forceAutomaticSync()
                    }
                    .disabled(!library.hasRemoteSyncConfiguration || library.isSyncing)
                }
            }
            .frame(width: 340, alignment: .leading)
            .padding(14)
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
        HStack(spacing: 6) {
            Text(displayName)
                .lineLimit(1)
                .foregroundStyle(AppPalette.folderText)
            Spacer(minLength: 4)
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
        .font(.callout)
        .padding(.vertical, 1)
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
        HStack(alignment: .center, spacing: 4) {
            Spacer()
                .frame(width: CGFloat(node.depth) * 10)
            if node.hasChildren {
                Button {
                    toggleExpansion()
                } label: {
                    Image(systemName: isExpanded ? "chevron.down" : "chevron.right")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .frame(width: 12, height: 20)
                        .contentShape(Rectangle())
                }
                .buttonStyle(.plain)
                .disabled(library.pendingBrowseSelection != nil)
            } else {
                Spacer()
                    .frame(width: 12)
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

struct AssetDeletionConfirmationDialog: View {
    @EnvironmentObject private var library: LibraryStore
    var request: AssetDeletionRequest
    var close: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            Text("删除选中照片？")
                .font(.headline)
            Text("将把 \(request.assetIDs.count) 个选中资产移入共享回收站，并从默认视图隐藏。磁盘上的照片文件会保持原样，不会被删除、移动或覆盖。")
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
                .frame(minHeight: 72, alignment: .leading)

            HStack {
                Spacer()
                Button("取消", role: .cancel) {
                    close()
                }
                Button("确认删除", role: .destructive) {
                    library.deleteAssets(request.assetIDs)
                    close()
                }
                .keyboardShortcut(.defaultAction)
                .disabled(library.isBusy)
            }
        }
        .frame(width: 520, height: 190, alignment: .leading)
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
    var openAssetDeletionConfirmation: (AssetDeletionRequest) -> Void
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
                        .overlay {
                            AssetMouseEventCatcher(
                                singleClick: {
                                    let modifiers = ModifierAwareClickView.currentModifiers()
                                    select(asset, modifiers)
                                },
                                doubleClick: {
                                    openLoupe(asset)
                                },
                                deletionAction: {
                                    openAssetDeletionConfirmation(AssetDeletionRequest(assetIDs: deletionAssetIDs(for: asset)))
                                },
                                dragPayload: assetDragPayload(for: asset)
                            )
                        }
                        .onAppear {
                            loadMore(asset.id)
                        }
                        .contextMenu {
                            Button("删除照片", role: .destructive) {
                                openAssetDeletionConfirmation(AssetDeletionRequest(assetIDs: deletionAssetIDs(for: asset)))
                            }
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

    private func deletionAssetIDs(for asset: Asset) -> [UUID] {
        let ids = selectedAssetIDs.contains(asset.id) ? Array(selectedAssetIDs) : [asset.id]
        return ids.sorted { $0.uuidString < $1.uuidString }
    }
}

struct AssetMouseEventCatcher: NSViewRepresentable {
    var singleClick: () -> Void
    var doubleClick: () -> Void
    var deletionAction: () -> Void
    var dragPayload: String

    func makeNSView(context: Context) -> AssetMouseEventView {
        let view = AssetMouseEventView()
        view.singleClick = singleClick
        view.doubleClick = doubleClick
        view.deletionAction = deletionAction
        view.dragPayload = dragPayload
        return view
    }

    func updateNSView(_ nsView: AssetMouseEventView, context: Context) {
        nsView.singleClick = singleClick
        nsView.doubleClick = doubleClick
        nsView.deletionAction = deletionAction
        nsView.dragPayload = dragPayload
    }
}

final class AssetMouseEventView: NSView, NSDraggingSource {
    var singleClick: () -> Void = {}
    var doubleClick: () -> Void = {}
    var deletionAction: () -> Void = {}
    var dragPayload = ""
    private var mouseDownEvent: NSEvent?
    private var didStartDrag = false

    override func mouseDown(with event: NSEvent) {
        mouseDownEvent = event
        didStartDrag = false
        if event.clickCount >= 2 {
            doubleClick()
        }
    }

    override func mouseUp(with event: NSEvent) {
        if event.clickCount == 1, !didStartDrag {
            singleClick()
        }
    }

    override func mouseDragged(with event: NSEvent) {
        guard !didStartDrag, let mouseDownEvent else { return }
        didStartDrag = true

        let pasteboardItem = NSPasteboardItem()
        pasteboardItem.setString(dragPayload, forType: .string)

        let draggingItem = NSDraggingItem(pasteboardWriter: pasteboardItem)
        draggingItem.setDraggingFrame(bounds, contents: dragImage())
        beginDraggingSession(with: [draggingItem], event: mouseDownEvent, source: self)
    }

    override func rightMouseDown(with event: NSEvent) {
        let menu = NSMenu()
        let deleteItem = NSMenuItem(title: "删除照片", action: #selector(deleteFromContextMenu), keyEquivalent: "")
        deleteItem.target = self
        menu.addItem(deleteItem)
        NSMenu.popUpContextMenu(menu, with: event, for: self)
    }

    @objc private func deleteFromContextMenu() {
        deletionAction()
    }

    func draggingSession(_ session: NSDraggingSession, sourceOperationMaskFor context: NSDraggingContext) -> NSDragOperation {
        .move
    }

    func ignoreModifierKeys(for session: NSDraggingSession) -> Bool {
        true
    }

    private func dragImage() -> NSImage {
        let imageSize = NSSize(width: max(bounds.width, 1), height: max(bounds.height, 1))
        let image = NSImage(size: imageSize)
        image.lockFocus()
        NSColor.black.withAlphaComponent(0.3).setFill()
        NSBezierPath(rect: NSRect(origin: .zero, size: image.size)).fill()
        image.unlockFocus()
        return image
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
    @State private var pendingAssetDeletionRequest: AssetDeletionRequest?

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
                                    openAssetDeletionConfirmation: { request in
                                        pendingAssetDeletionRequest = request
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
        .sheet(item: $pendingAssetDeletionRequest) { request in
            AssetDeletionConfirmationDialog(
                request: request,
                close: {
                    pendingAssetDeletionRequest = nil
                }
            )
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

private enum LoupeFilmstripMetrics {
    static let thumbnailWidth: CGFloat = 104
    static let thumbnailHeight: CGFloat = 78
    static let verticalPadding: CGFloat = 6
    static let height = thumbnailHeight + verticalPadding * 2
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
                            AssetPreviewImage(asset: filmstripAsset, contentMode: .fit, placeholderSize: 18)
                        }
                        .frame(
                            width: LoupeFilmstripMetrics.thumbnailWidth,
                            height: LoupeFilmstripMetrics.thumbnailHeight
                        )
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
            .padding(.vertical, LoupeFilmstripMetrics.verticalPadding)
        }
        .frame(height: LoupeFilmstripMetrics.height)
        .layoutPriority(1)
        .background(Color(nsColor: .windowBackgroundColor).opacity(0.35))
    }
}

struct FilterBar: View {
    @EnvironmentObject private var library: LibraryStore
    @State private var isFileSearchOpen = false
    @FocusState private var isFileSearchFocused: Bool

    var body: some View {
        HStack(spacing: 0) {
            LightroomRatingFilterGroup(
                minimumRating: library.filter.minimumRating,
                setMinimumRating: library.setMinimumRatingFilter
            )
            LightroomFilterDivider()
            LightroomFlagFilterGroup(
                flaggedOnly: library.filter.flaggedOnly,
                setFlaggedOnly: library.setFlaggedOnlyFilter
            )
            LightroomFilterDivider()
            LightroomColorLabelFilterGroup(
                selectedLabels: library.filter.colorLabels,
                toggleColorLabel: library.toggleColorLabelFilter
            )
            LightroomFilterDivider()

            Spacer(minLength: 12)

            Picker("整理顺序", selection: Binding(
                get: { library.filter.sortOrder },
                set: { library.setSortOrder($0) }
            )) {
                ForEach(LibrarySortOrder.allCases) { sortOrder in
                    Text(sortOrder.label).tag(sortOrder)
                }
            }
            .pickerStyle(.menu)
            .frame(width: 150)

            if isFileSearchOpen {
                TextField("文件搜索", text: $library.filter.searchText)
                    .textFieldStyle(.plain)
                    .focused($isFileSearchFocused)
                    .onAppear {
                        isFileSearchFocused = true
                    }
                    .onSubmit {
                        library.refresh()
                    }
                    .onExitCommand {
                        isFileSearchFocused = false
                        isFileSearchOpen = false
                    }
                    .padding(.horizontal, 8)
                    .frame(width: 220, height: 28)
                    .background(Color.black.opacity(0.22))
                    .overlay(
                        RoundedRectangle(cornerRadius: 4)
                            .stroke(Color.white.opacity(0.12), lineWidth: 1)
                    )
            } else {
                Button("文件搜索") {
                    isFileSearchOpen = true
                    isFileSearchFocused = true
                }
                .buttonStyle(.borderless)
                .frame(width: 90, height: 28)
            }

            Button("应用") {
                library.refresh()
            }
            .buttonStyle(.borderless)

            Button("重置") {
                library.filter = LibraryFilter()
                isFileSearchFocused = false
                isFileSearchOpen = false
                library.refresh()
            }
            .buttonStyle(.borderless)
        }
        .padding(.horizontal, 10)
        .frame(height: 44)
        .background(Color(nsColor: NSColor(calibratedWhite: 0.12, alpha: 1)))
    }
}

struct LightroomRatingFilterGroup: View {
    var minimumRating: Int
    var setMinimumRating: (Int) -> Void

    var body: some View {
        HStack(spacing: 4) {
            Button {
                setMinimumRating(0)
            } label: {
                Image(systemName: "greaterthan.circle.fill")
                    .foregroundStyle(minimumRating == 0 ? Color.white : Color.secondary)
            }
            .buttonStyle(.plain)

            ForEach(1...5, id: \.self) { rating in
                Button {
                    setMinimumRating(rating)
                } label: {
                    Image(systemName: "star.fill")
                        .foregroundStyle(rating <= minimumRating ? Color.white : Color.secondary)
                }
                .buttonStyle(.plain)
            }
        }
        .frame(width: 142)
    }
}

struct LightroomFlagFilterGroup: View {
    var flaggedOnly: Bool
    var setFlaggedOnly: (Bool) -> Void

    var body: some View {
        HStack(spacing: 12) {
            Button {
                setFlaggedOnly(!flaggedOnly)
            } label: {
                Image(systemName: flaggedOnly ? "flag.fill" : "flag")
                    .foregroundStyle(flaggedOnly ? Color.white : Color.secondary)
            }
            .buttonStyle(.plain)

            Image(systemName: "flag")
                .foregroundStyle(Color.secondary.opacity(0.45))
            Image(systemName: "flag.slash")
                .foregroundStyle(Color.secondary.opacity(0.45))
        }
        .frame(width: 116)
    }
}

struct LightroomColorLabelFilterGroup: View {
    var selectedLabels: Set<AssetColorLabel>
    var toggleColorLabel: (AssetColorLabel) -> Void

    var body: some View {
        HStack(spacing: 7) {
            ForEach(AssetColorLabel.allCases) { label in
                Button {
                    toggleColorLabel(label)
                } label: {
                    Circle()
                        .fill(color(for: label))
                        .frame(width: 16, height: 16)
                        .overlay(
                            Circle()
                                .stroke(selectedLabels.contains(label) ? Color.white : Color.clear, lineWidth: 2)
                        )
                }
                .buttonStyle(.plain)
                .accessibilityLabel(label.label)
            }
        }
        .frame(width: 128)
    }

    private func color(for label: AssetColorLabel) -> Color {
        switch label {
        case .red: Color(red: 0.65, green: 0.22, blue: 0.19)
        case .yellow: Color(red: 0.66, green: 0.65, blue: 0.22)
        case .green: Color(red: 0.33, green: 0.55, blue: 0.31)
        case .blue: Color(red: 0.25, green: 0.42, blue: 0.63)
        case .purple: Color(red: 0.46, green: 0.29, blue: 0.61)
        }
    }
}

struct LightroomFilterDivider: View {
    var body: some View {
        Rectangle()
            .fill(Color.white.opacity(0.08))
            .frame(width: 1, height: 44)
            .padding(.horizontal, 10)
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
            .saturation(asset.flagState == .rejected ? 0.0 : 1.0)
            .brightness(asset.flagState == .rejected ? -0.18 : 0.0)
            if asset.flagState == .rejected {
                RejectedAssetOverlay()
            }
            AssetFlagBadge(flagState: asset.flagState)
        }
        .clipped()
        .clipShape(RoundedRectangle(cornerRadius: 0))
        .border(Color(nsColor: .selectedContentBackgroundColor), width: selected ? 3 : 0)
        .contentShape(Rectangle())
        .accessibilityLabel(asset.originalFilename)
    }
}

struct RejectedAssetOverlay: View {
    var body: some View {
        Rectangle()
            .fill(Color.gray.opacity(0.42))
    }
}

struct AssetFlagBadge: View {
    var flagState: AssetFlagState

    var body: some View {
        VStack {
            HStack {
                Spacer()
                if flagState != .unflagged {
                    Image(systemName: systemImage)
                        .font(.system(size: 10, weight: .bold))
                        .foregroundStyle(Color.white)
                        .frame(width: 20, height: 20)
                        .background(badgeColor)
                        .clipShape(RoundedRectangle(cornerRadius: 4))
                        .padding(5)
                }
            }
            Spacer()
        }
    }

    private var systemImage: String {
        switch flagState {
        case .unflagged: ""
        case .picked: "flag.fill"
        case .rejected: "xmark"
        }
    }

    private var badgeColor: Color {
        switch flagState {
        case .unflagged: Color.clear
        case .picked: Color(red: 0.12, green: 0.58, blue: 0.32)
        case .rejected: Color(red: 0.42, green: 0.42, blue: 0.42)
        }
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
                    set: { value in library.setSelectedAssetRating(value) }
                )) {
                    ForEach(0...5, id: \.self) { value in
                        Text(value == 0 ? "未评分" : "\(value) 星").tag(value)
                    }
                }
                .labelsHidden()
            }

            HStack {
                Text("标记")
                Picker("", selection: Binding(
                    get: { asset.flagState },
                    set: { value in library.setSelectedAssetFlagState(value) }
                )) {
                    ForEach(AssetFlagState.allCases) { flagState in
                        Text(flagState.label).tag(flagState)
                    }
                }
                .labelsHidden()
                .frame(width: 120)
            }

            HStack {
                Text("颜色")
                Picker("", selection: Binding(
                    get: { asset.colorLabel },
                    set: { value in library.setSelectedAssetColorLabel(value) }
                )) {
                    Text("无").tag(Optional<AssetColorLabel>.none)
                    ForEach(AssetColorLabel.allCases) { label in
                        Text(label.label).tag(Optional(label))
                    }
                }
                .labelsHidden()
                .frame(width: 120)
            }

            HStack {
                TextField("标签，用逗号分隔", text: $draftTags)
                    .textFieldStyle(.roundedBorder)
                Button("保存标签") {
                    library.setSelectedAssetTags(
                        draftTags
                            .split(separator: ",")
                            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
                    )
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
