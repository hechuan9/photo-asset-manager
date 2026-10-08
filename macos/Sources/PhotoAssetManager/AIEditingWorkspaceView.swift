import SwiftUI

struct AIEditingWorkspaceView: View {
    @EnvironmentObject private var library: LibraryStore
    @EnvironmentObject private var store: AIEditingBatchStore
    @Environment(\.openSettings) private var openSettings

    var body: some View {
        VStack(spacing: 0) {
            header.padding(20)
            Divider()
            if let batch = store.batch {
                ScrollView {
                    LazyVStack(spacing: 20) {
                        ForEach(batch.items) { item in
                            AIEditingPhotoRow(item: item, store: store)
                        }
                    }.padding(20)
                }
            } else {
                ContentUnavailableView("从图库选择照片", systemImage: "photo.on.rectangle", description: Text("选择照片后，点击 AI 调色进入工作台。"))
            }
            Divider()
            footer.padding(16)
        }
        .background(WorkspaceStyle.canvas)
        .preferredColorScheme(.dark)
        .tint(WorkspaceStyle.accent)
        .task { await store.loadWorkspace(library: library) }
    }

    private var header: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text("AI 调色工作台").font(.title2.bold())
                Spacer()
                if store.isRunning || store.isSyncing { ProgressView().controlSize(.small) }
                Text(store.status).font(.callout).foregroundStyle(.secondary)
            }
            DisclosureGroup("我的审美与本批要求") {
                HStack(alignment: .top, spacing: 20) {
                    VStack(alignment: .leading) {
                        Text("长期审美偏好").font(.headline)
                        TextField("描述你希望之后的照片参考的审美", text: $store.preferencesText, axis: .vertical)
                            .lineLimit(3...6).textFieldStyle(.roundedBorder)
                        Button("保存长期偏好") { Task { await store.savePreferences() } }.disabled(store.isSyncing)
                    }
                    VStack(alignment: .leading) {
                        Text("本批要求").font(.headline)
                        TextField("仅用于这批照片", text: $store.batchInstruction, axis: .vertical)
                            .lineLimit(3...6).textFieldStyle(.roundedBorder)
                            .disabled(!store.isAwaitingConfirmation || store.isSyncing)
                        Text("单张对话不会自动改变长期偏好。")
                            .font(.caption).foregroundStyle(.secondary)
                    }
                }.padding(.top, 8)
            }
            if let batch = store.batch, batch.confirmed {
                Text("本批使用的审美偏好：\((batch.preferences ?? "").isEmpty ? "未设置" : batch.preferences!)")
                    .font(.caption).foregroundStyle(.secondary).textSelection(.enabled)
            }
            Text("比较后确认所选版本，才会发布新的展示图和缩略图。关闭窗口可稍后继续。")
                .font(.caption).foregroundStyle(.secondary)
        }
    }

    private var footer: some View {
        VStack(alignment: .leading, spacing: 8) {
            if let error = store.errorMessage {
                Text(error).foregroundStyle(.red).textSelection(.enabled)
            }
            HStack {
                Button("AI 设置") { openSettings() }.disabled(store.isRunning || store.isSyncing)
                if store.errorMessage != nil {
                    Button("重试同步工作台") { Task { await store.loadWorkspace(library: library) } }.disabled(store.isSyncing || store.isRunning)
                    Button("保留本机副本并载入 NAS 草稿") { Task { await store.reloadRemoteWorkspace() } }
                        .disabled(store.isRunning || store.isSyncing)
                }
                if store.isAwaitingConfirmation {
                    Button("取消本批", action: store.dismiss).disabled(store.isSyncing)
                } else if store.isRunning {
                    Button("暂停", action: store.cancel)
                } else if store.batch?.cancelled == true {
                    Button("继续处理", action: store.retry).disabled(store.isSyncing)
                }
                if !store.failedItems.isEmpty || store.errorMessage != nil {
                    Button("重试未完成任务", action: store.retry).disabled(store.isRunning || store.isSyncing)
                }
                Spacer()
                if !store.isRunning, store.batch?.confirmed == true,
                   store.batch?.items.contains(where: { $0.phase != .done }) == true {
                    Button("保留本机草稿副本并结束本批", action: store.archiveAndDismiss)
                        .disabled(store.isSyncing)
                }
                if store.isAwaitingConfirmation {
                    Button("开始调色（\(store.totalCount) 张）", action: store.start)
                        .buttonStyle(.borderedProminent).disabled(store.isSyncing)
                } else if let items = store.batch?.items {
                    let ready = items.filter { store.canPublish($0) && $0.failure == nil }.count
                    if items.allSatisfy({ $0.phase == .done }) {
                        Button("完成本批", action: store.closeCompletedWorkspace)
                    } else {
                        Button("确认并发布这 \(ready) 张", action: store.publishReady)
                            .buttonStyle(.borderedProminent).disabled(ready == 0 || store.isRunning || store.isSyncing)
                    }
                }
            }
        }
    }
}

private struct AIEditingPhotoRow: View {
    let item: AIEditingBatch.Item
    @ObservedObject var store: AIEditingBatchStore
    @State private var instruction = ""
    @State private var expanded = false
    @State private var zoom = 1.0
    @State private var offset = CGSize.zero
    @State private var dragOrigin = CGSize.zero

    private var candidates: [AIEditingBatch.Candidate] { item.candidates ?? [] }
    private var selected: AIEditingBatch.Candidate? { candidates.first { $0.id == item.selectedCandidateID } }
    private var displayed: AIEditingBatch.Candidate? { selected ?? candidates.last }
    private var editable: Bool { item.phase == .review && !store.isRunning && !store.isSyncing }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text(item.name).font(.headline).lineLimit(1)
                Spacer()
                Text(phaseTitle).foregroundStyle(.secondary)
            }
            HStack(spacing: 16) {
                comparisonPane(url: item.originalPreview ?? displayed?.result.originalPreview, title: "调整前", selected: item.selectedCandidateID == nil) {
                    store.selectCandidate(itemID: item.id, candidateID: nil)
                }
                comparisonPane(url: displayed?.result.preview ?? displayed?.result.fullSize, title: "AI 结果", selected: item.selectedCandidateID != nil) {
                    if let candidate = displayed { store.selectCandidate(itemID: item.id, candidateID: candidate.id) }
                }
            }
            HStack {
                Image(systemName: "minus.magnifyingglass")
                Slider(value: $zoom, in: 1...4).frame(width: 150).accessibilityLabel("同步缩放")
                Image(systemName: "plus.magnifyingglass")
                Button("重置视图") { zoom = 1; offset = .zero; dragOrigin = .zero }
                Spacer()
                if !candidates.isEmpty {
                    Menu("版本记录（\(candidates.count)）") {
                        ForEach(Array(candidates.enumerated()), id: \.element.id) { index, candidate in
                            Button("第 \(index + 1) 版：\(candidate.instruction.isEmpty ? "首次调色" : candidate.instruction)") {
                                store.selectCandidate(itemID: item.id, candidateID: candidate.id)
                            }
                        }
                    }.disabled(!editable)
                }
            }.font(.caption)
            if let result = displayed?.result {
                Text("调色意见：\(result.reason)").textSelection(.enabled)
            }
            if let failure = item.failure {
                Text(failure).foregroundStyle(.red).textSelection(.enabled)
                if let details = item.failureDetails {
                    DisclosureGroup("诊断详情") { Text(details).font(.caption).textSelection(.enabled) }
                }
            }
            DisclosureGroup("继续调色", isExpanded: $expanded) {
                VStack(alignment: .leading, spacing: 10) {
                    ForEach(Array(candidates.enumerated()), id: \.element.id) { index, candidate in
                        VStack(alignment: .leading, spacing: 4) {
                            Text("第 \(index + 1) 版 · \(candidate.instruction.isEmpty ? "首次调色" : candidate.instruction)").font(.subheadline.bold())
                            Text(candidate.result.reason).font(.callout).foregroundStyle(.secondary).textSelection(.enabled)
                        }
                    }
                    HStack(alignment: .bottom) {
                        TextField("描述这张照片还需要怎样调整…", text: $instruction, axis: .vertical)
                            .lineLimit(2...5).textFieldStyle(.roundedBorder)
                        Button("生成下一版") {
                            store.refine(itemID: item.id, instruction: instruction)
                            instruction = ""
                        }.disabled(!editable || instruction.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                    }
                }.padding(.top, 8)
            }
            HStack {
                if let activity = store.activities[item.id] {
                    ProgressView().controlSize(.small)
                    Text(activity.title).font(.caption).foregroundStyle(.secondary)
                }
                Spacer()
                Button(item.selectedCandidateID == nil ? "确认保留调整前版本" : "确认并发布") { store.publish(itemID: item.id) }
                    .buttonStyle(.borderedProminent).disabled(!editable || !store.canPublish(item))
            }
        }
        .padding(18)
        .background(WorkspaceStyle.panel, in: RoundedRectangle(cornerRadius: 12))
    }

    private var phaseTitle: String {
        if item.failure != nil { return "需要处理" }
        switch item.phase {
        case .preparing: return "等待调色"
        case .downloading: return "准备照片"
        case .grading: return "正在调色"
        case .uploading: return "正在发布"
        case .review: return "待确认"
        case .done: return "已确认"
        }
    }

    private func comparisonPane(url: URL?, title: String, selected: Bool, action: @escaping () -> Void) -> some View {
        VStack(spacing: 8) {
            GeometryReader { geometry in
                ZStack {
                    Color.black.opacity(0.35)
                    if let url, let image = NSImage(contentsOf: url) {
                        Image(nsImage: image).resizable().scaledToFit()
                            .frame(width: geometry.size.width, height: geometry.size.height)
                            .scaleEffect(zoom).offset(offset)
                    } else {
                        Text("等待预览").foregroundStyle(.secondary)
                    }
                }
                .clipped().contentShape(Rectangle())
                .gesture(DragGesture().onChanged { value in
                    offset = CGSize(width: dragOrigin.width + value.translation.width, height: dragOrigin.height + value.translation.height)
                }.onEnded { _ in dragOrigin = offset })
            }.frame(height: 270)
            Button(action: action) {
                Label(title, systemImage: selected ? "largecircle.fill.circle" : "circle")
                    .frame(maxWidth: .infinity)
            }.buttonStyle(.plain).disabled(!editable || url == nil)
        }.frame(maxWidth: .infinity)
    }
}
