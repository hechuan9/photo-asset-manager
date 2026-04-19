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
    @State private var sourcePendingMove: SourceDirectory?

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
                            isSelected: library.filter.browseSelection?.path == node.path,
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
                            move: {
                                sourcePendingMove = node.source
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
        .sheet(item: $sourcePendingMove) { source in
            MoveSourceDirectorySheet(
                source: source,
                targets: library.topLevelMoveTargets(excluding: source),
                move: { target in
                    library.moveSourceDirectory(source, to: target)
                    sourcePendingMove = nil
                },
                cancel: {
                    sourcePendingMove = nil
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

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack {
                Text(displayName)
                    .lineLimit(2)
                    .foregroundStyle(AppPalette.folderText)
                    .textSelection(.enabled)
                Spacer()
                if let source {
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
                            library.removeSourceDirectory(source)
                        }
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
    var node: SourceDirectoryNode
    var interruptedScanPath: String?
    var isExpanded: Bool
    var isSelected: Bool
    var toggleExpansion: () -> Void
    var select: () -> Void
    var move: () -> Void

    var body: some View {
        HStack(alignment: .top, spacing: 6) {
            Spacer()
                .frame(width: CGFloat(node.depth) * 14)
            if node.hasChildren {
                Image(systemName: isExpanded ? "chevron.down" : "chevron.right")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .frame(width: 12, height: 18)
                    .contentShape(Rectangle())
                    .onTapGesture {
                        toggleExpansion()
                    }
            } else {
                Spacer()
                    .frame(width: 12)
            }
            SourceDirectoryRow(
                source: node.source,
                path: node.path,
                displayName: node.displayName,
                interruptedScanPath: interruptedScanPath
            )
        }
        .contentShape(Rectangle())
        .background(isSelected ? Color.accentColor.opacity(0.14) : Color.clear)
        .clipShape(RoundedRectangle(cornerRadius: 6))
        .onTapGesture {
            select()
        }
        .contextMenu {
            if let source = node.source {
                Button("刷新") {
                    library.scanSource(source)
                }
                if node.depth > 0 {
                    Button("移动到...") {
                        move()
                    }
                }
                Button("移除", role: .destructive) {
                    library.removeSourceDirectory(source)
                }
            }
        }
    }
}

struct MoveSourceDirectorySheet: View {
    var source: SourceDirectory
    var targets: [SourceDirectory]
    var move: (SourceDirectory?) -> Void
    var cancel: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            Text("移动文件夹")
                .font(.headline)
            Text(source.path)
                .lineLimit(2)
                .foregroundStyle(.secondary)
                .textSelection(.enabled)
            Divider()
            Button("移到顶层") {
                move(nil)
            }
            ForEach(targets) { target in
                Button(target.path) {
                    move(target)
                }
                .lineLimit(1)
                .truncationMode(.middle)
            }
            HStack {
                Spacer()
                Button("取消", role: .cancel) {
                    cancel()
                }
            }
        }
        .frame(width: 420, alignment: .leading)
        .padding(18)
    }
}

struct AssetBrowserView: View {
    @EnvironmentObject private var library: LibraryStore
    private let columns = [GridItem(.adaptive(minimum: 150, maximum: 210), spacing: 14)]

    var body: some View {
        VStack(spacing: 0) {
            FilterBar()
            Divider()
            if library.assets.isEmpty {
                EmptyLibraryView()
            } else {
                ScrollView {
                    LazyVGrid(columns: columns, spacing: 14) {
                        ForEach(library.assets) { asset in
                            AssetTile(asset: asset, selected: asset.id == library.selectedAssetID)
                                .onTapGesture {
                                    library.selectedAssetID = asset.id
                                    library.loadSelectedFiles()
                                }
                                .onAppear {
                                    library.loadMoreAssetsIfNeeded(currentAssetID: asset.id)
                                }
                        }
                    }
                    .padding(16)
                }
            }
        }
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

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            ZStack {
                Rectangle()
                    .fill(Color(nsColor: .controlBackgroundColor))
                AssetPreviewImage(asset: asset, contentMode: .fill, placeholderSize: 34)
            }
            .aspectRatio(1.25, contentMode: .fit)
            .clipShape(RoundedRectangle(cornerRadius: 6))
            .overlay {
                RoundedRectangle(cornerRadius: 6)
                    .stroke(selected ? Color.accentColor : Color.clear, lineWidth: 3)
            }

            Text(asset.originalFilename)
                .font(.callout)
                .lineLimit(1)
            HStack {
                Text(asset.status.label)
                Spacer()
                Text(asset.rating > 0 ? String(repeating: "*", count: asset.rating) : "未评分")
            }
            .font(.caption)
            .foregroundStyle(.secondary)
        }
        .padding(8)
        .background(selected ? Color.accentColor.opacity(0.12) : Color.clear)
        .clipShape(RoundedRectangle(cornerRadius: 8))
    }
}

struct AssetPreviewImage: View {
    var asset: Asset
    var contentMode: ContentMode
    var placeholderSize: CGFloat
    @StateObject private var loader = ImagePreviewLoader()

    var body: some View {
        Group {
            if let image = loader.image {
                Image(nsImage: image)
                    .resizable()
                    .aspectRatio(contentMode: contentMode)
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

    func load(thumbnailPath: String?, primaryPath: String?, cacheKey: String) async {
        guard loadedCacheKey != cacheKey else { return }
        loadedCacheKey = cacheKey

        if let cached = ImagePreviewCache.shared.image(forKey: cacheKey) {
            image = cached
            return
        }

        image = nil
        let loaded = await Task.detached(priority: .userInitiated) { () -> NSImage? in
            if let thumbnailPath, let image = NSImage(contentsOfFile: thumbnailPath) {
                return image
            }
            if let primaryPath {
                return ImageRenderer.renderableImage(url: URL(fileURLWithPath: primaryPath))
            }
            return nil
        }.value

        guard loadedCacheKey == cacheKey else { return }
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
