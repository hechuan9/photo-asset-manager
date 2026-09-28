import SwiftUI
import KeepsAPI

struct IOSWaterfallGallery: View {
    @EnvironmentObject private var library: IOSLibraryStore
    var body: some View {
        GeometryReader { geometry in
            let count = geometry.size.width > 700 ? 4 : 2
            let columns = columns(count: count)
            ScrollView {
                HStack(alignment: .top, spacing: 8) {
                    ForEach(columns.indices, id: \.self) { index in
                        LazyVStack(spacing: 8) {
                            ForEach(columns[index]) { asset in
                                NavigationLink {
                                    IOSAssetDetail(asset: asset).environmentObject(library)
                                } label: {
                                    VStack(alignment: .leading, spacing: 5) {
                                        IOSPreviewImage(asset: asset, configuration: library.configuration)
                                            .aspectRatio(ratio(asset), contentMode: .fit)
                                            .clipShape(RoundedRectangle(cornerRadius: 12))
                                        Text(asset.originalFilename).font(.caption).lineLimit(1)
                                        if asset.rating > 0 { Text(String(repeating: "★", count: asset.rating)).font(.caption).foregroundStyle(.yellow) }
                                    }
                                }.buttonStyle(.plain)
                            }
                        }.frame(maxWidth: .infinity)
                    }
                }.padding(8)
                if library.canLoadMore {
                    Button("加载更多") { Task { await library.loadMore() } }
                        .disabled(library.isLoading)
                        .padding()
                        .onAppear { Task { await library.loadMore() } }
                }
            }.refreshable { await library.refresh() }
        }
    }
    private func ratio(_ asset: KeepsAsset) -> CGFloat {
        guard let preview = asset.preview, preview.height > 0 else { return 0.82 }
        return max(0.35, CGFloat(preview.width) / CGFloat(preview.height))
    }
    private func columns(count: Int) -> [[KeepsAsset]] {
        var result = Array(repeating: [KeepsAsset](), count: count)
        var heights = Array(repeating: CGFloat.zero, count: count)
        for asset in library.assets {
            let index = heights.indices.min { heights[$0] < heights[$1] } ?? 0
            result[index].append(asset)
            heights[index] += 1 / ratio(asset) + 0.2
        }
        return result
    }
}

struct IOSPreviewImage: View {
    let asset: KeepsAsset
    let configuration: KeepsConfiguration?

    var body: some View {
        KeepsPreviewImage(asset: asset, configuration: configuration)
    }
}

struct IOSAssetDetail: View {
    @EnvironmentObject private var library: IOSLibraryStore
    @Environment(\.dismiss) private var dismiss
    @State var asset: KeepsAsset
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
            library.update(asset); dismiss()
        } catch { self.error = String(reflecting: error) }
    }
}
