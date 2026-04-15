import SwiftUI

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
        .toolbar {
            ToolbarItemGroup {
                Button("添加文件夹", systemImage: "plus") {
                    library.chooseAndAddFolders(scanImmediately: false)
                }
                Button("添加并扫描", systemImage: "plus.viewfinder") {
                    library.chooseAndAddFolders(scanImmediately: true)
                }
                Button("扫描所有来源") {
                    library.scanTrackedSources()
                }
                .disabled(library.isScanning)
            }
            ToolbarItemGroup {
                Button("归档到 NAS") {
                    library.archiveSelected()
                }
                .disabled(library.selectedAsset == nil)
                Button("同步变更") {
                    library.syncSelected()
                }
                .disabled(library.selectedAsset == nil)
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

struct SidebarView: View {
    @EnvironmentObject private var library: LibraryStore

    var body: some View {
        List(selection: Binding(
            get: { library.filter.status },
            set: { library.setStatusFilter($0) }
        )) {
            Button {
                library.setStatusFilter(nil)
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
                    ForEach(library.sourceDirectories) { source in
                        SourceDirectoryRow(source: source)
                    }
                }
            } header: {
                HStack {
                    Text("文件夹")
                    Spacer()
                    Button {
                        library.chooseAndAddFolders(scanImmediately: false)
                    } label: {
                        Image(systemName: "plus")
                    }
                    .buttonStyle(.plain)
                    .disabled(library.isScanning)
                    .help("添加文件夹")
                }
            }

            Section("缩略图位置") {
                Text(library.derivativeStorageURL?.path ?? "未设置，不生成新缩略图")
                    .lineLimit(2)
                    .foregroundStyle(.secondary)
                    .textSelection(.enabled)
                HStack {
                    Button("修改") {
                        library.chooseDerivativeStorageLocation()
                    }
                    .disabled(library.isScanning)
                    if library.derivativeStorageURL != nil {
                        Button("清除") {
                            library.clearDerivativeStorageLocation()
                        }
                        .disabled(library.isScanning)
                    }
                }
            }

            if let path = library.interruptedScanPath {
                Section("断点续扫") {
                    Text(path)
                        .lineLimit(2)
                        .foregroundStyle(.secondary)
                    Button("继续上次扫描") {
                        library.resumeInterruptedScan()
                    }
                    .disabled(library.isScanning)
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
    var source: SourceDirectory

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack {
                Text(source.isTracked ? "追踪中" : "已停止")
                    .foregroundStyle(source.isTracked ? Color.secondary : Color.orange)
                Spacer()
                Button("扫描") {
                    library.scanSource(source)
                }
                .disabled(!source.isTracked || library.isScanning)
                if source.isTracked {
                    Button("停止追踪") {
                        library.stopTrackingSource(source)
                    }
                    .disabled(library.isScanning)
                } else {
                    Button("恢复") {
                        library.resumeTrackingSource(source)
                    }
                    .disabled(library.isScanning)
                }
                Button("移除") {
                    library.removeSourceDirectory(source)
                }
                .disabled(library.isScanning)
            }
            Text(source.path)
                .lineLimit(2)
                .foregroundStyle(.secondary)
                .textSelection(.enabled)
            if let lastScannedAt = source.lastScannedAt {
                Text("上次扫描 \(lastScannedAt.formatted(date: .abbreviated, time: .shortened))")
                    .font(.caption)
                    .foregroundStyle(.tertiary)
            }
        }
        .font(.callout)
        .padding(.vertical, 4)
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
            Text("先扫描本地目录或 NAS 目录。原片不会被移动，索引会记录每个文件位置。")
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
                if let path = asset.thumbnailPath, let image = NSImage(contentsOfFile: path) {
                    Image(nsImage: image)
                        .resizable()
                        .scaledToFill()
                } else {
                    Image(systemName: "photo")
                        .font(.system(size: 34))
                        .foregroundStyle(.secondary)
                }
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
                if let path = asset.thumbnailPath, let image = NSImage(contentsOfFile: path) {
                    Image(nsImage: image)
                        .resizable()
                        .scaledToFit()
                } else {
                    Image(systemName: "photo")
                        .font(.system(size: 46))
                        .foregroundStyle(.secondary)
                }
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
