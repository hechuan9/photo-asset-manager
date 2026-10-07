import SwiftUI
import KeepsAPI

struct DirectoryMoveProgressView: View {
    @ObservedObject var library: LibraryStore

    private var operation: String { library.directoryMove?.name == nil ? "移动" : "重命名" }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text(library.directoryMoveFinished ? "文件夹\(operation)未完成" : "正在\(operation)文件夹")
                .font(.headline)
            if let pending = library.directoryMove {
                Text((pending.path as NSString).lastPathComponent)
                    .lineLimit(2).truncationMode(.middle)
                Text(pending.name.map { "新名称：\($0)" } ?? "移入：\(pending.parentPath)")
                    .font(.caption).foregroundStyle(.secondary)
                    .lineLimit(2).truncationMode(.middle)
                if !library.directoryMoveFinished {
                    ProgressView().progressViewStyle(.linear)
                        .accessibilityLabel(Self.phaseLabel(library.directoryMovePhase).replacingOccurrences(of: "移动", with: operation))
                    Text(Self.phaseLabel(library.directoryMovePhase).replacingOccurrences(of: "移动", with: operation))
                    TimelineView(.periodic(from: .now, by: 1)) { context in
                        Text("已用时 \(max(0, Int(context.date.timeIntervalSince(pending.startedAt)))) 秒")
                            .font(.caption).monospacedDigit().foregroundStyle(.secondary)
                    }
                    Text("任务由 NAS 执行，关闭 App 后仍会继续。")
                        .font(.caption).foregroundStyle(.secondary)
                }
            }
            if let message = library.directoryMoveMessage {
                ScrollView { Text(message).font(.caption).textSelection(.enabled) }
                    .frame(maxHeight: 110)
                    .foregroundStyle(library.directoryMoveFinished ? .red : .secondary)
            }
            if library.directoryMoveFinished {
                Button("确认并刷新目录") { library.acknowledgeDirectoryMoveFailure() }
            }
        }
        .padding(20).frame(width: 380)
        .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 10))
    }

    static func phaseLabel(_ phase: String) -> String {
        switch phase {
        case "waiting": "等待 NAS 开始处理…"
        case "validating": "检查来源和目标文件夹…"
        case "moving": "移动 NAS 文件夹…"
        case "catalog": "更新照片索引…"
        case "tracking": "更新目录追踪和扫描任务…"
        case "completed": "移动完成"
        case "failed": "移动失败"
        default: "NAS 正在处理…"
        }
    }
}


struct DirectoryRenameSheet: View {
    @ObservedObject var library: LibraryStore
    let directory: KeepsNavigationDirectory
    @Environment(\.dismiss) private var dismiss
    @State private var name = ""
    @FocusState private var nameFocused: Bool

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("重命名文件夹").font(.headline)
            Text(directory.path).font(.caption).foregroundStyle(.secondary).textSelection(.enabled)
            TextField("新名称", text: $name).focused($nameFocused)
            Text("名称不能以 . 开头，不能包含斜杠、控制字符或使用系统保留名称。同名文件夹不会被覆盖。")
                .font(.caption).foregroundStyle(.secondary)
            HStack {
                Spacer()
                Button("取消") { dismiss() }.keyboardShortcut(.cancelAction)
                Button("重命名") {
                    let newName = name
                    dismiss()
                    Task { await library.renameDirectory(directory.path, name: newName) }
                }
                .keyboardShortcut(.defaultAction)
                .disabled(!library.canRenameDirectory(directory.path, name: name))
            }
        }
        .padding(24).frame(width: 420)
        .onAppear { name = directory.name; nameFocused = true }
    }
}
