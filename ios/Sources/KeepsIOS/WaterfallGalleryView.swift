import SwiftUI
import KeepsAPI

struct IOSWaterfallGallery: View {
    @EnvironmentObject private var library: IOSLibraryStore
    @Binding var selecting: Bool
    @Binding var selectedIDs: Set<UUID>
    @Binding var density: KeepsGalleryDensity
    @Binding var visibleDates: String
    @State private var selected: KeepsAsset?
    @State private var information: KeepsAsset?
    @State private var olderVisible = false
    @State private var hasScrolled = false
    @State private var focusedAssetID: UUID?

    var body: some View {
        GeometryReader { geometry in
            let ratios = library.assets.map { JustifiedAssetGridLayout.aspectRatio($0.preview) }
            let rows = KeepsPhotoGrid.rows(aspectRatios: ratios, width: geometry.size.width, density: density)
            ScrollViewReader { proxy in
            ScrollView {
                VStack(alignment: .leading, spacing: 1) {
                    if library.canLoadMore {
                        Button("载入更早的照片") { Task { await library.loadMore() } }
                            .frame(maxWidth: .infinity).padding().disabled(library.isLoading)
                            .onScrollVisibilityChange(threshold: 0.01) { visible in
                                olderVisible = visible
                                if visible { loadOlderIfNeeded() }
                            }
                    }
                    // 从最新一端组行再反向展示，向顶部插入旧页时已有行的身份和顺序保持稳定。
                    ForEach(Array(rows.reversed()), id: \.indices.lowerBound) { row in
                        IOSPhotoGridRow(
                            assets: row.indices.reversed().map { library.assets[$0] },
                            sizes: Array(row.sizes.reversed()), width: geometry.size.width,
                            selecting: $selecting, selectedIDs: $selectedIDs, compact: density == .compact,
                            open: open, showInformation: { information = $0 }
                        ).id(library.assets[row.indices.lowerBound].id)
                    }
                }.scrollTargetLayout()
            }
            .contentMargins(.bottom, 0, for: .scrollContent)
            .defaultScrollAnchor(.bottom)
            .defaultScrollAnchor(.top, for: .alignment)
            .onScrollGeometryChange(for: Bool.self) { geometry in
                geometry.contentOffset.y + geometry.containerSize.height >= geometry.contentSize.height - 40
            } action: { _, atBottom in
                if hasScrolled { library.followsLatest = atBottom }
            }
            .simultaneousGesture(MagnifyGesture().onEnded { value in
                let next = density.pinched(magnification: value.magnification)
                if next != density { density = next }
            })
            .onChange(of: density) { _, _ in
                if let focusedAssetID, let index = library.assets.firstIndex(where: { $0.id == focusedAssetID }),
                   let row = rows.first(where: { $0.indices.contains(index) }) {
                    proxy.scrollTo(library.assets[row.indices.lowerBound].id, anchor: .center)
                }
            }
            .onScrollPhaseChange { _, phase in
                if phase == .interacting { hasScrolled = true; loadOlderIfNeeded() }
            }
            .onChange(of: library.isLoading) { _, loading in if !loading { loadOlderIfNeeded() } }
            .onScrollTargetVisibilityChange(idType: UUID.self, threshold: 0.2) { ids in
                let visible = Set(ids)
                let visibleAssets = rows.filter { visible.contains(library.assets[$0.indices.lowerBound].id) }
                    .flatMap { row in row.indices.map { library.assets[$0] } }
                if !visibleAssets.isEmpty { focusedAssetID = visibleAssets[visibleAssets.count / 2].id }
                let dates = visibleAssets.map { String(($0.captureTime ?? $0.createdAt).prefix(10)) }.sorted()
                if let first = dates.first, let last = dates.last {
                    visibleDates = first == last ? first : "\(first) – \(last)"
                }
            }
            }
        }
        .fullScreenCover(item: $selected) { asset in IOSPhotoViewer(initialAsset: asset).environmentObject(library) }
        .sheet(item: $information) { asset in
            NavigationStack {
                IOSAssetDetail(asset: asset)
                    .toolbar { ToolbarItem(placement: .confirmationAction) { Button("完成") { information = nil } } }
            }.environmentObject(library)
        }
    }

    private func open(_ asset: KeepsAsset) {
        if selecting {
            if !selectedIDs.insert(asset.id).inserted { selectedIDs.remove(asset.id) }
        } else { selected = asset }
    }

    private func loadOlderIfNeeded() {
        guard hasScrolled, olderVisible, library.lastError == nil else { return }
        Task { await library.loadMore() }
    }
}

private struct IOSPhotoGridRow: View {
    @EnvironmentObject private var library: IOSLibraryStore
    let assets: [KeepsAsset]
    let sizes: [CGSize]
    let width: CGFloat
    @Binding var selecting: Bool
    @Binding var selectedIDs: Set<UUID>
    let compact: Bool
    let open: (KeepsAsset) -> Void
    let showInformation: (KeepsAsset) -> Void
    @State private var visible = false

    var body: some View {
        // 行的占位尺寸始终确定，图片仅在可见时解码，避免长图库持有所有预览。
        Color.clear.frame(width: width, height: sizes.map(\.height).max() ?? 0)
            .overlay(alignment: .leading) {
                if visible {
                    HStack(spacing: 1) {
                        ForEach(Array(assets.enumerated()), id: \.element.id) { index, asset in
                            Button { open(asset) } label: {
                                IOSPreviewImage(asset: asset, configuration: library.configuration, contentMode: compact ? .fill : .fit)
                                    .frame(width: sizes[index].width, height: sizes[index].height)
                                    .overlay(alignment: .bottomTrailing) {
                                        if selecting {
                                            Image(systemName: selectedIDs.contains(asset.id) ? "checkmark.circle.fill" : "circle")
                                                .font(compact ? .caption : .title3)
                                                .foregroundStyle(selectedIDs.contains(asset.id) ? .cyan : .white)
                                                .shadow(radius: 2).padding(3)
                                        } else if asset.flagState == "picked" {
                                            Image(systemName: "heart.fill").font(compact ? .caption2 : .body)
                                                .foregroundStyle(.white).shadow(radius: 2).padding(3)
                                        }
                                    }
                            }.buttonStyle(.plain).accessibilityLabel(asset.originalFilename)
                                .accessibilityAddTraits(selectedIDs.contains(asset.id) ? .isSelected : [])
                                .contextMenu {
                                    Button("查看照片", systemImage: "photo") { open(asset) }
                                    Button("选择", systemImage: "checkmark.circle") { selecting = true; selectedIDs.insert(asset.id) }
                                    Button("信息与整理", systemImage: "info.circle") { showInformation(asset) }
                                }
                        }
                    }
                }
            }
            .onScrollVisibilityChange(threshold: 0.01) { visible = $0 }
    }
}

struct IOSPreviewImage: View {
    let asset: KeepsAsset
    let configuration: KeepsConfiguration?
    var contentMode: ContentMode = .fit
    var body: some View { KeepsPreviewImage(asset: asset, configuration: configuration, contentMode: contentMode) }
}

struct IOSPhotoViewer: View {
    @EnvironmentObject private var library: IOSLibraryStore
    @Environment(\.dismiss) private var dismiss
    @State private var selection: UUID
    @State private var photos: [KeepsAsset] = []
    @State private var controlsVisible = true
    @State private var information: KeepsAsset?
    let initialAsset: KeepsAsset

    init(initialAsset: KeepsAsset) {
        self.initialAsset = initialAsset
        _selection = State(initialValue: initialAsset.id)
    }

    private var current: KeepsAsset { photos.first { $0.id == selection } ?? initialAsset }

    var body: some View {
        ZStack {
            Color.black.ignoresSafeArea()
            TabView(selection: $selection) {
                ForEach(photos) { asset in
                    IOSZoomablePhoto(asset: asset, configuration: library.configuration,
                                     toggleControls: { withAnimation { controlsVisible.toggle() } },
                                     close: { dismiss() })
                        .tag(asset.id)
                }
            }
            .tabViewStyle(.page(indexDisplayMode: .never))
            .overlay(alignment: .top) {
                if controlsVisible {
                    HStack {
                        Button { dismiss() } label: { Image(systemName: "chevron.down").padding(12) }
                            .accessibilityLabel("返回图库")
                        Spacer()
                        VStack(spacing: 4) {
                            Text(String((current.captureTime ?? current.createdAt).prefix(10))).font(.headline)
                            Text(current.originalFilename).font(.caption).lineLimit(1)
                        }
                        Spacer()
                        Button { information = current } label: { Image(systemName: "info.circle").padding(12) }
                            .accessibilityLabel("照片信息与整理")
                    }.padding(.horizontal, 8).padding(.vertical, 6).background(.ultraThinMaterial)
                }
            }
            .overlay(alignment: .bottom) {
                if controlsVisible {
                    ScrollViewReader { proxy in
                        ScrollView(.horizontal) {
                            LazyHStack(spacing: 3) {
                                ForEach(photos) { asset in
                                    Button { selection = asset.id } label: {
                                        IOSPreviewImage(asset: asset, configuration: library.configuration)
                                            .frame(width: 48 * JustifiedAssetGridLayout.aspectRatio(asset.preview), height: 48)
                                            .overlay { if selection == asset.id { Rectangle().stroke(.white, lineWidth: 2) } }
                                    }.buttonStyle(.plain).id(asset.id)
                                        .accessibilityLabel(asset.originalFilename)
                                        .accessibilityAddTraits(selection == asset.id ? .isSelected : [])
                                }
                            }.padding(8)
                        }.frame(height: 64).scrollIndicators(.hidden).background(.ultraThinMaterial)
                            .onAppear { proxy.scrollTo(selection, anchor: .center) }
                            .onChange(of: selection) { _, id in withAnimation { proxy.scrollTo(id, anchor: .center) } }
                    }.frame(height: 64)
                }
            }
        }
        .preferredColorScheme(.dark)
        .statusBarHidden(!controlsVisible)
        .onAppear { photos = library.assets.isEmpty ? [initialAsset] : library.chronologicalAssets }
        .onChange(of: library.assets) { _, updated in
            photos = updated.reversed()
            if !photos.contains(where: { $0.id == selection }) { dismiss() }
        }
        .task(id: selection) {
            if photos.prefix(3).contains(where: { $0.id == selection }), library.canLoadMore { await library.loadMore() }
        }
        .sheet(item: $information) { asset in
            NavigationStack {
                IOSAssetDetail(asset: asset, onRemoved: { id in
                    information = nil
                    let index = photos.firstIndex { $0.id == id } ?? 0
                    photos.removeAll { $0.id == id }
                    if photos.isEmpty { dismiss() }
                    else { selection = photos[min(index, photos.count - 1)].id }
                })
                    .toolbar { ToolbarItem(placement: .confirmationAction) { Button("完成") { information = nil } } }
            }.environmentObject(library)
        }
    }
}

struct IOSAssetDetail: View {
    @EnvironmentObject private var library: IOSLibraryStore
    @Environment(\.dismiss) private var dismiss
    @State var asset: KeepsAsset
    var onRemoved: (UUID) -> Void = { _ in }
    @State private var rating = 0
    @State private var flag = "unflagged"
    @State private var color = ""
    @State private var tags = ""
    @State private var busy = false
    @State private var error: String?
    @State private var saved = false

    var body: some View {
        Form {
            Section {
                IOSPreviewImage(asset: asset, configuration: library.configuration)
                    .frame(minHeight: 240, maxHeight: 420)
                LabeledContent("文件名", value: asset.originalFilename)
                if let time = asset.captureTime { LabeledContent("拍摄时间", value: time) }
                if !asset.cameraModel.isEmpty { LabeledContent("相机", value: asset.cameraModel) }
                if !asset.lensModel.isEmpty { LabeledContent("镜头", value: asset.lensModel) }
            }
            if let configuration = library.configuration {
                Section {
                    KeepsAssetVersionsView(assetID: asset.id, revision: asset.updatedAt, configuration: configuration) { updated in
                        guard library.configuration == configuration else { return }
                        asset = updated
                        library.update(updated)
                    }.id(asset.id.uuidString + configuration.baseURL.absoluteString + configuration.libraryID)
                }
            }
            Section("整理") {
                Stepper("评分：\(rating)", value: $rating, in: 0...5)
                Picker("标记", selection: $flag) {
                    Text("未标记").tag("unflagged")
                    Text("留用").tag("picked")
                    Text("排除").tag("rejected")
                }
                Picker("颜色", selection: $color) {
                    Text("无").tag("")
                    Text("红色").tag("red"); Text("黄色").tag("yellow")
                    Text("绿色").tag("green"); Text("蓝色").tag("blue"); Text("紫色").tag("purple")
                }
                TextField("标签，以逗号分隔", text: $tags)
                Button("保存到 NAS") { Task { await save() } }.disabled(busy)
                if saved { Text("已保存").foregroundStyle(.secondary) }
            }
            Section {
                Button(asset.trashed ? "恢复到图库" : "移入回收站", role: asset.trashed ? nil : .destructive) {
                    Task { await toggleTrash() }
                }.disabled(busy)
            } footer: { Text("此操作只改变 NAS 图库状态，不会删除或移动原片。") }
            if busy { ProgressView() }
            if let error { Text(error).foregroundStyle(.red).textSelection(.enabled) }
        }
        .onChange(of: library.assets) { _, assets in
            if let updated = assets.first(where: { $0.id == asset.id }) { asset = updated }
        }
        .navigationTitle("照片详情")
        .navigationBarTitleDisplayMode(.inline)
        .task {
            applyFields()
            guard let configuration = library.configuration else { return }
            do { asset = try await KeepsClient(configuration: configuration).asset(id: asset.id); applyFields() }
            catch { self.error = String(reflecting: error) }
        }
    }
    private func applyFields() {
        rating = asset.rating; flag = asset.flagState; color = asset.colorLabel ?? ""; tags = asset.tags.joined(separator: ", ")
    }
    private func save() async {
        guard let configuration = library.configuration else { return }
        busy = true; error = nil; saved = false
        defer { busy = false }
        let tagList = tags.replacingOccurrences(of: "，", with: ",").split(separator: ",").map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }.filter { !$0.isEmpty }
        do {
            asset = try await KeepsClient(configuration: configuration).updateAsset(id: asset.id, patch: KeepsAssetPatch(rating: rating, flagState: flag, colorLabel: color.isEmpty ? nil : color, clearColorLabel: color.isEmpty, tags: tagList))
            library.update(asset); applyFields(); saved = true
        } catch { self.error = String(reflecting: error) }
    }
    private func toggleTrash() async {
        guard let configuration = library.configuration else { return }
        busy = true; error = nil
        defer { busy = false }
        do {
            let client = KeepsClient(configuration: configuration)
            asset = try await (asset.trashed ? client.restoreAsset(id: asset.id) : client.trashAsset(id: asset.id))
            library.update(asset); onRemoved(asset.id); dismiss()
        } catch { self.error = String(reflecting: error) }
    }
}
