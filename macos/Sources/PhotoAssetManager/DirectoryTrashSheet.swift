import SwiftUI
import KeepsAPI

struct DirectoryTrashSheet: View {
    @ObservedObject var library: LibraryStore
    let directory: KeepsNavigationDirectory
    @Environment(\.dismiss) private var dismiss
    @State private var confirmationName = ""
    private var isDeleting: Bool { library.isDirectoryTrashBlocking }
    @State private var error: String?

    private var phaseTitle: String {
        switch library.directoryTrashPhase {
        case "recycling": "正在移入 NAS 回收站"
        case "reconciling": "正在更新资料库索引"
        case "failed": "删除任务需要确认"
        default: "等待服务器执行"
        }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("删除文件夹").font(.headline)
            Text("文件夹及其所有内容将移入 NAS 回收站。需要恢复时，请在 NAS 的 File Station 回收站中操作。")
            Text(directory.path).font(.callout).textSelection(.enabled)
            Text("请输入完整目录名“\(directory.name)”以确认：")
            TextField("目录名", text: $confirmationName)
                .textFieldStyle(.roundedBorder)
                .disabled(isDeleting)
            if let error {
                Text(error).foregroundStyle(.red).textSelection(.enabled)
            }
            if let pending = library.directoryTrash {
                TimelineView(.periodic(from: .now, by: 1)) { context in
                    VStack(alignment: .leading, spacing: 8) {
                        if !library.directoryTrashFinished { ProgressView().controlSize(.small) }
                        Text(phaseTitle)
                        Text("已用时 \(max(0, Int(context.date.timeIntervalSince(pending.startedAt)))) 秒").foregroundStyle(.secondary)
                        if let message = library.directoryTrashMessage {
                            Text(message).foregroundStyle(library.directoryTrashFinished ? .red : .secondary).textSelection(.enabled)
                        }
                        Text("处理期间其他操作已暂停。退出应用不会取消后台任务，下次启动将继续查询。")
                            .font(.caption).foregroundStyle(.secondary)
                    }
                }
            }
            HStack {
                Spacer()
                Button(library.directoryTrashFinished ? "关闭" : "取消") {
                    if library.directoryTrashFinished { library.acknowledgeDirectoryTrashFailure() }
                    dismiss()
                }.keyboardShortcut(.cancelAction).disabled(isDeleting && !library.directoryTrashFinished)
                Button("移入 NAS 回收站", role: .destructive) {
                    error = nil
                    Task {
                        do {
                            try await library.trashDirectory(directory, confirmationName: confirmationName)
                            if !library.isDirectoryTrashBlocking { dismiss() }
                        } catch {
                            self.error = error.localizedDescription
                        }
                    }
                }
                .disabled(confirmationName != directory.name || isDeleting)
            }
        }
        .padding(24)
        .frame(width: 440)
        .interactiveDismissDisabled(isDeleting)
    }
}
