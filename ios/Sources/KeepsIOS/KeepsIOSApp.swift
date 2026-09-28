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
    @State private var showingSearch = false
    @State private var collections = false
    @State private var collectionOpen = false
    @State private var selecting = false
    @State private var selectedIDs: Set<UUID> = []
    @State private var visibleDates = ""
    @State private var changing = false
    @State private var confirmTrash = false
    @AppStorage("galleryDensity") private var density: KeepsGalleryDensity = .large

    private var showsGallery: Bool { !collections || collectionOpen }
    private var title: String {
        if !showsGallery { return "精选集" }
        if library.showingTrash { return "回收站" }
        return library.showingPicked ? "留用" : "图库"
    }

    var body: some View {
        ZStack {
            Color.black.ignoresSafeArea()
            if !showsGallery {
                collectionsView
            } else if library.configuration == nil {
                ContentUnavailableView("连接 NAS 图库", systemImage: "externaldrive", description: Text("在连接设置中填写服务地址和访问令牌。"))
            } else if library.assets.isEmpty {
                if library.isLoading { ProgressView("正在载入图库") }
                else { ContentUnavailableView(library.lastError == nil ? "没有照片" : "无法加载图库", systemImage: library.lastError == nil ? "photo" : "wifi.exclamationmark") }
            } else {
                IOSWaterfallGallery(selecting: $selecting, selectedIDs: $selectedIDs, density: $density, visibleDates: $visibleDates)
                    .id("\(library.showingTrash)|\(library.showingPicked)|\(library.search)")
                    .ignoresSafeArea().allowsHitTesting(!changing)
            }
            VStack(spacing: 0) {
                header.disabled(changing)
                if let error = library.lastError {
                    Text(error).font(.caption).lineLimit(3).padding(10)
                        .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 12))
                        .padding(.horizontal)
                }
                if library.isLoading && library.assets.isEmpty { ProgressView().padding() }
                Spacer(minLength: 0)
                bottomBar
            }
        }
        .preferredColorScheme(.dark)
        .tint(.white)
        .sheet(isPresented: $showingSettings) {
            IOSSettingsView(configuration: library.configuration) {
                library.reloadConfiguration(); Task { await library.refresh() }
            }
        }
        .sheet(isPresented: $showingSearch) {
            IOSSearchSheet(query: library.search) { query in
                collections = false; collectionOpen = false
                library.showingTrash = false; library.showingPicked = false
                library.search = query; library.followsLatest = true; resetSelection()
                Task { await library.refresh() }
            }
        }
        .confirmationDialog("将所选照片移入回收站？原片不会删除。", isPresented: $confirmTrash, titleVisibility: .visible) {
            Button("移入回收站", role: .destructive) { Task { await changeSelection(trash: true) } }
        }
        .task { await library.refresh() }
        .onChange(of: scenePhase, initial: true) { _, phase in library.setActive(phase == .active) }
        .onChange(of: library.assets) { _, assets in
            selectedIDs.formIntersection(Set(assets.map(\.id)))
            if assets.isEmpty { visibleDates = "" }
        }
    }

    private var header: some View {
        VStack(alignment: .leading, spacing: 5) {
            HStack {
                Text(selecting ? "已选 \(selectedIDs.count) 张" : title).font(.largeTitle.bold()).lineLimit(1).minimumScaleFactor(0.6)
                Spacer()
                if showsGallery {
                    Menu {
                        Picker("照片大小", selection: $density) {
                            Text("大图 · 完整比例").tag(KeepsGalleryDensity.large)
                            Text("中图 · 完整比例").tag(KeepsGalleryDensity.medium)
                            Text("密集 · 小方块").tag(KeepsGalleryDensity.compact)
                        }
                        Divider()
                        Button("刷新", systemImage: "arrow.clockwise") { Task { await library.refresh() } }
                        Button("连接设置", systemImage: "gearshape") { showingSettings = true }
                    } label: { Image(systemName: "line.3.horizontal.decrease").frame(width: 44, height: 44) }
                        .glassEffect(.regular.interactive(), in: Circle()).accessibilityLabel("图库选项")
                    Button(selecting ? "取消" : "选择") {
                        selecting.toggle(); selectedIDs.removeAll()
                    }.padding(.horizontal, 18).frame(height: 44)
                        .glassEffect(.regular.interactive(), in: Capsule()).disabled(changing)
                } else {
                    Button { showingSettings = true } label: { Image(systemName: "gearshape").frame(width: 44, height: 44) }
                        .glassEffect(.regular.interactive(), in: Circle()).accessibilityLabel("连接设置")
                }
            }
            if showsGallery && !visibleDates.isEmpty { Text(visibleDates).font(.subheadline.weight(.semibold)) }
            if !library.search.isEmpty && showsGallery { Text("搜索：\(library.search)").font(.caption) }
        }
        .foregroundStyle(.white).padding(.horizontal, 16).padding(.top, 8).padding(.bottom, 18)
        .background {
            LinearGradient(colors: [.black.opacity(0.65), .clear], startPoint: .top, endPoint: .bottom)
                .ignoresSafeArea(edges: .top)
        }
    }

    private var bottomBar: some View {
        GlassEffectContainer(spacing: 20) {
            HStack(spacing: 16) {
                if selecting {
                    HStack(spacing: 22) {
                        Button("全选已加载") { selectedIDs = Set(library.assets.map(\.id)) }
                        Spacer(minLength: 0)
                        Button { Task { await changeSelection(trash: false) } } label: {
                            Image(systemName: library.showingTrash ? "arrow.uturn.backward" : "heart")
                        }.accessibilityLabel(library.showingTrash ? "恢复所选照片" : "留用所选照片")
                        if !library.showingTrash {
                            Button { confirmTrash = true } label: { Image(systemName: "trash") }
                                .accessibilityLabel("移入回收站").disabled(selectedIDs.isEmpty)
                        }
                    }.disabled(changing).padding(20).glassEffect(.regular, in: Capsule())
                } else {
                    HStack(spacing: 4) {
                        tabButton("图库", icon: "photo.on.rectangle.fill", active: !collections) { openScope(picked: false, trash: false, inCollections: false) }
                        tabButton("精选集", icon: "rectangle.stack.fill", active: collections) {
                            collections = true; collectionOpen = false; resetSelection()
                        }
                    }.padding(5).glassEffect(.regular, in: Capsule())
                    Spacer(minLength: 0)
                    Button { showingSearch = true } label: {
                        Image(systemName: "magnifyingglass").font(.title2).frame(width: 60, height: 60)
                    }.glassEffect(.regular.interactive(), in: Circle()).accessibilityLabel("搜索")
                }
            }
        }.padding(.horizontal, 20).padding(.bottom, 10)
    }

    private func tabButton(_ label: String, icon: String, active: Bool, action: @escaping () -> Void) -> some View {
        Button(action: action) {
            VStack(spacing: 3) { Image(systemName: icon).font(.title3); Text(label).font(.caption.weight(.semibold)) }
                .frame(width: 86, height: 50).foregroundStyle(active ? Color.cyan : .white)
                .background(active ? Color.white.opacity(0.15) : .clear, in: Capsule())
        }.buttonStyle(.plain).accessibilityAddTraits(active ? .isSelected : [])
    }

    private var collectionsView: some View {
        VStack(spacing: 14) {
            Button { openScope(picked: true, trash: false, inCollections: true) } label: {
                Label("留用", systemImage: "heart.fill").frame(maxWidth: .infinity, alignment: .leading).padding(24)
            }
            Button { openScope(picked: false, trash: true, inCollections: true) } label: {
                Label("回收站", systemImage: "trash").frame(maxWidth: .infinity, alignment: .leading).padding(24)
            }
            Spacer()
        }.buttonStyle(.bordered).padding(.horizontal, 20).padding(.top, 100)
    }

    private func resetSelection() { selecting = false; selectedIDs.removeAll(); visibleDates = "" }
    private func openScope(picked: Bool, trash: Bool, inCollections: Bool) {
        collections = inCollections; collectionOpen = inCollections
        library.showingPicked = picked; library.showingTrash = trash; library.search = ""; library.followsLatest = true
        resetSelection(); Task { await library.refresh() }
    }

    private func changeSelection(trash: Bool) async {
        guard let configuration = library.configuration, !selectedIDs.isEmpty else { return }
        changing = true
        defer { changing = false }
        let client = KeepsClient(configuration: configuration)
        do {
            for id in Array(selectedIDs) {
                let asset: KeepsAsset
                if trash { asset = try await client.trashAsset(id: id) }
                else if library.showingTrash { asset = try await client.restoreAsset(id: id) }
                else { asset = try await client.updateAsset(id: id, patch: KeepsAssetPatch(flagState: "picked")) }
                library.update(asset); selectedIDs.remove(id)
            }
            resetSelection()
        } catch { library.lastError = String(reflecting: error) }
    }
}

struct IOSSearchSheet: View {
    @Environment(\.dismiss) private var dismiss
    @State var query: String
    @FocusState private var focused: Bool
    let submit: (String) -> Void
    var body: some View {
        NavigationStack {
            Form {
                TextField("文件名或相机", text: $query).focused($focused).submitLabel(.search)
                    .onSubmit { submit(query); dismiss() }
                Button("显示全部照片") { submit(""); dismiss() }
            }
            .navigationTitle("搜索")
            .toolbar {
                ToolbarItem(placement: .cancellationAction) { Button("取消") { dismiss() } }
                ToolbarItem(placement: .confirmationAction) { Button("搜索") { submit(query); dismiss() } }
            }.onAppear { focused = true }
        }.presentationDetents([.medium, .large])
    }
}

struct IOSSettingsView: View {
    @Environment(\.dismiss) private var dismiss
    @State private var baseURL: String
    @State private var libraryID: String
    @State private var token: String
    @State private var error: String?
    @State private var isChecking = false
    var didSave: () -> Void

    init(configuration: KeepsConfiguration?, didSave: @escaping () -> Void) {
        _baseURL = State(initialValue: configuration?.baseURL.absoluteString ?? "https://keeps.hechuannas.synology.me")
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
                }.textInputAutocapitalization(.never).autocorrectionDisabled().disabled(isChecking)
                Section {
                    Text("照片、评分、标签和回收站状态由 NAS 保存。此设备仅保存连接设置和可清理的图片缓存。")
                    Text("移入回收站只隐藏图库记录，原片保持不变。")
                    Text("使用 HTTPS 服务地址时，Wi-Fi 和蜂窝网络均可连接。使用局域网地址时，请允许本地网络访问并连接 NAS 所在网络。")
                }
                if isChecking { ProgressView("正在验证 NAS 连接…") }
                if let error { Text(error).foregroundStyle(.red).textSelection(.enabled) }
            }
            .interactiveDismissDisabled(isChecking)
            .navigationTitle("连接设置")
            .toolbar {
                ToolbarItem(placement: .cancellationAction) { Button("取消") { dismiss() }.disabled(isChecking) }
                ToolbarItem(placement: .confirmationAction) {
                    Button("验证并保存") { Task { await verifyAndSave() } }
                        .disabled(isChecking)
                }
            }
        }
    }

    @MainActor
    private func verifyAndSave() async {
        guard !isChecking else { return }
        isChecking = true
        error = nil
        defer { isChecking = false }
        do {
            let base = baseURL.trimmingCharacters(in: .whitespacesAndNewlines)
            let library = libraryID.trimmingCharacters(in: .whitespacesAndNewlines)
            let credential = token.trimmingCharacters(in: .whitespacesAndNewlines)
            guard let url = URL(string: base), ["http", "https"].contains(url.scheme?.lowercased() ?? ""),
                  url.host != nil, url.user == nil, url.password == nil, url.query == nil,
                  url.fragment == nil, !library.isEmpty else { throw KeepsAPIError.invalidConfiguration }
            let client = KeepsClient(configuration: KeepsConfiguration(baseURL: url, libraryID: library,
                                                                       accessCredential: credential.isEmpty ? nil : credential))
            _ = try await client.counts()
            var probe = KeepsAssetQuery()
            probe.limit = 1
            _ = try await client.assets(query: probe)
            try Task.checkCancellation()
            try KeepsSettings.save(baseURLString: base, libraryID: library, accessCredential: credential)
            didSave()
            dismiss()
        } catch { self.error = error.localizedDescription + "\n" + String(reflecting: error) }
    }

}
