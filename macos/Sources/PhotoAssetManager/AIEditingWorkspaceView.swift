import SwiftUI

struct AIEditingWorkspaceView: View {
    @EnvironmentObject private var library: LibraryStore
    @EnvironmentObject private var store: AIEditingBatchStore
    @Environment(\.openSettings) private var openSettings

    var autoStart = false
    var returnToLibrary: () -> Void
    @State private var showsPreferences = false
    @State private var preferencesDraft = ""

    var body: some View {
        VStack(spacing: 0) {
            header.padding(20)
            Divider()
            if store.batch != nil, store.workspaceItems.isEmpty {
                Spacer()
            } else if store.batch != nil {
                ScrollView {
                    LazyVStack(spacing: 20) {
                        ForEach(store.workspaceItems) { item in
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
        .task {
            await store.loadWorkspace(library: library)
            if autoStart && store.isAwaitingConfirmation && store.errorMessage == nil { store.start() }
        }
        .sheet(isPresented: $showsPreferences) { preferencesEditor }
    }

    private var header: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Button(action: returnToLibrary) { Label("返回图库", systemImage: "chevron.left") }
                Text("AI 调色工作台").font(.title2.bold())
                Spacer()
            }
            VStack(alignment: .leading, spacing: 6) {
                Text("我的审美偏好").font(.headline)
                Text(store.preferencesText.isEmpty ? "尚未设置，双击添加审美偏好" : store.preferencesText)
                    .foregroundStyle(.secondary).lineLimit(3)
                Text("双击编辑，保存后用于下一批照片").font(.caption).foregroundStyle(.secondary)
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(12).background(WorkspaceStyle.panel, in: RoundedRectangle(cornerRadius: 8))
            .contentShape(Rectangle())
            .onTapGesture(count: 2) { preferencesDraft = store.preferencesText; showsPreferences = true }
            .accessibilityAction(named: Text("编辑审美偏好")) { preferencesDraft = store.preferencesText; showsPreferences = true }
            if store.isAwaitingConfirmation {
                TextField("本批要求", text: $store.batchInstruction, axis: .vertical)
                    .lineLimit(1...3).textFieldStyle(.roundedBorder)
            }
            if let batch = store.batch, batch.confirmed {
                Text("本批使用的审美偏好：\((batch.preferences ?? "").isEmpty ? "未设置" : batch.preferences!)")
                    .font(.caption).foregroundStyle(.secondary).textSelection(.enabled)
            }
            Text("选择采用版本后，该照片立即移出工作台并在后台发布；进度显示在底部状态栏。")
                .font(.caption).foregroundStyle(.secondary)
        }
    }

    private var preferencesEditor: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("编辑审美偏好").font(.title2.bold())
            TextEditor(text: $preferencesDraft).frame(minHeight: 160)
            Text("偏好会保存到资料库；当前已开始的照片保留启动时的偏好。")
                .font(.caption).foregroundStyle(.secondary)
            if let error = store.errorMessage { Text(error).foregroundStyle(.red) }
            HStack {
                Spacer()
                Button("取消") { showsPreferences = false }
                Button("保存") {
                    Task {
                        if await store.savePreferences(text: preferencesDraft) { showsPreferences = false }
                    }
                }.buttonStyle(.borderedProminent).disabled(store.isSyncing)
            }
        }.padding(24).frame(width: 520)
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
                    if ready > 0 {
                        Button("确认并发布这 \(ready) 张", action: store.publishReady)
                            .buttonStyle(.borderedProminent).disabled(ready == 0 || store.isSyncing)
                    }
                }
            }
        }
    }
}

private struct AIEditingPhotoRow: View {
    @EnvironmentObject private var library: LibraryStore
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
    private var editable: Bool { store.canInteract(item) }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text(item.name).font(.headline).lineLimit(1)
                Spacer()
                Text(phaseTitle).foregroundStyle(.secondary)
            }
            HStack(spacing: 16) {
                comparisonPane(url: item.originalPreview ?? displayed?.result.originalPreview, title: "采用调整前版本", original: true, selected: item.selectedCandidateID == nil) {
                    store.choose(itemID: item.id, candidateID: nil)
                }
                comparisonPane(url: displayed?.result.preview ?? displayed?.result.fullSize, title: "采用 AI 结果", selected: item.selectedCandidateID != nil) {
                    if let candidate = displayed { store.choose(itemID: item.id, candidateID: candidate.id) }
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
                if item.isBackgroundDecision {
                    Button("重试后台处理") { store.retryDecision(itemID: item.id) }
                }
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
                Button("弃用") { store.reject(itemID: item.id) }
                    .disabled(!editable)
                Button(item.selectedCandidateID == nil ? "确认保留调整前版本" : "采用当前版本") { store.publish(itemID: item.id) }
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
        case .discarding: return "正在弃用"
        case .review: return "待确认"
        case .done: return "已确认"
        }
    }

    private func comparisonPane(url: URL?, title: String, original: Bool = false, selected: Bool, action: @escaping () -> Void) -> some View {
        VStack(spacing: 8) {
            GeometryReader { geometry in
                ZStack {
                    Color.black.opacity(0.35)
                    AIEditingComparisonImage(url: url,
                        asset: original ? library.assets.first(where: { $0.id == item.assetID }) : nil,
                        configuration: library.configuration)
                        .frame(width: geometry.size.width, height: geometry.size.height)
                        .scaleEffect(zoom).offset(offset)
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

struct AIEditingBackgroundStatus: View {
    @ObservedObject var store: AIEditingBatchStore
    let showWorkspace: () -> Void

    var body: some View {
        if store.backgroundDecisionCount > 0 || store.failedDecisionCount > 0 {
            VStack(spacing: 0) {
                Divider()
                HStack(spacing: 10) {
                    if store.backgroundDecisionCount > 0 {
                        if store.batch?.cancelled != true { ProgressView().controlSize(.small) }
                        Text("后台处理 \(store.backgroundDecisionCount) 张\(store.batch?.cancelled == true ? " · 已暂停" : "")")
                            .foregroundStyle(.secondary)
                    }
                    if store.failedDecisionCount > 0 {
                        Button("\(store.failedDecisionCount) 张处理失败 · 查看") { showWorkspace() }
                            .foregroundStyle(.red).buttonStyle(.plain)
                    }
                    Spacer()
                }.font(.caption).padding(.horizontal, 16).padding(.vertical, 8)
            }.background(WorkspaceStyle.canvas)
        }
    }
}
