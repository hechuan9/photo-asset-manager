import SwiftUI
import KeepsAPI

@main
struct KeepsIOSApp: App {
    @StateObject private var library = IOSLibraryStore()
    var body: some Scene {
        WindowGroup { IOSRootView().environmentObject(library) }
    }
}

struct IOSRootView: View {
    @EnvironmentObject private var library: IOSLibraryStore
    @Environment(\.scenePhase) private var scenePhase
    @State private var showingSettings = false

    var body: some View {
        NavigationStack {
            VStack(spacing: 0) {
                if let error = library.lastError {
                    Text(error).font(.footnote).foregroundStyle(.red).textSelection(.enabled).padding()
                }
                if library.configuration == nil {
                    ContentUnavailableView {
                        Label("连接 NAS 图库", systemImage: "externaldrive.connected.to.line.below")
                    } description: { Text("填写 NAS 服务地址和访问令牌，即可浏览照片。") }
                    actions: { Button("设置") { showingSettings = true } }
                } else if library.assets.isEmpty && !library.isLoading {
                    ContentUnavailableView(library.showingTrash ? "回收站为空" : "没有照片", systemImage: "photo.on.rectangle", description: Text("可以刷新图库，或在 NAS 中添加追踪目录。"))
                } else {
                    IOSWaterfallGallery()
                }
                if library.isLoading { ProgressView("正在加载 NAS 图库…").padding() }
            }
            .navigationTitle(library.showingTrash ? "回收站" : "图库 · \(library.total)")
            .searchable(text: $library.search, prompt: "搜索照片")
            .onSubmit(of: .search) { Task { await library.refresh() } }
            .onChange(of: library.search) { _, value in
                if value.isEmpty { Task { await library.refresh() } }
            }
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    Button { library.showingTrash.toggle(); Task { await library.refresh() } } label: {
                        Label(library.showingTrash ? "图库" : "回收站", systemImage: library.showingTrash ? "photo.on.rectangle" : "trash")
                    }
                }
                ToolbarItemGroup(placement: .topBarTrailing) {
                    Menu {
                        Picker("排序", selection: $library.sort) {
                            Text("拍摄时间降序").tag("capture_desc")
                            Text("拍摄时间升序").tag("capture_asc")
                            Text("文件名").tag("filename")
                            Text("评分").tag("rating_desc")
                        }
                    } label: { Image(systemName: "arrow.up.arrow.down") }
                    Button { Task { await library.refresh() } } label: { Image(systemName: "arrow.clockwise") }
                    Button { showingSettings = true } label: { Image(systemName: "gearshape") }
                }
            }
            .onChange(of: library.sort) { _, _ in Task { await library.refresh() } }
            .sheet(isPresented: $showingSettings) {
                IOSSettingsView(configuration: library.configuration) {
                    library.reloadConfiguration()
                    Task { await library.refresh() }
                }
            }
            .task { await library.refresh() }
            .onChange(of: scenePhase) { _, phase in
                if phase == .active && library.assets.isEmpty { Task { await library.refresh() } }
            }
        }
    }
}

struct IOSSettingsView: View {
    @Environment(\.dismiss) private var dismiss
    @State private var baseURL: String
    @State private var libraryID: String
    @State private var token: String
    @State private var error: String?
    var didSave: () -> Void

    init(configuration: KeepsConfiguration?, didSave: @escaping () -> Void) {
        _baseURL = State(initialValue: configuration?.baseURL.absoluteString ?? "http://192.168.0.50:2283")
        _libraryID = State(initialValue: configuration?.libraryID ?? "local-library")
        _token = State(initialValue: configuration?.accessCredential ?? "")
        self.didSave = didSave
    }
    var body: some View {
        NavigationStack {
            Form {
                Section("NAS 服务") {
                    TextField("服务地址", text: $baseURL).keyboardType(.URL)
                    TextField("图库 ID", text: $libraryID)
                    SecureField("访问令牌", text: $token)
                }.textInputAutocapitalization(.never).autocorrectionDisabled()
                Section {
                    Text("照片、评分、标签和回收站状态由 NAS 保存。此设备仅保存连接设置和可清理的图片缓存。")
                    Text("移入回收站只隐藏图库记录，原片保持不变。")
                }
                if let error { Text(error).foregroundStyle(.red).textSelection(.enabled) }
            }
            .navigationTitle("连接设置")
            .toolbar {
                ToolbarItem(placement: .cancellationAction) { Button("取消") { dismiss() } }
                ToolbarItem(placement: .confirmationAction) {
                    Button("保存") {
                        do {
                            _ = try KeepsSettings.save(baseURLString: baseURL, libraryID: libraryID, accessCredential: token)
                            didSave(); dismiss()
                        } catch { self.error = String(reflecting: error) }
                    }
                }
            }
        }
    }
}
