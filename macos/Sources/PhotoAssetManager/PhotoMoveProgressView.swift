import SwiftUI
import KeepsAPI

struct PhotoMoveProgressView: View {
    @ObservedObject var library: LibraryStore

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text(library.photoMoveFinished ? "照片移动未完成" : "正在移动照片")
                .font(.headline)
            if let pending = library.photoMove {
                Text("\(pending.payload.assetIDs.count) 张照片")
                    .lineLimit(2).truncationMode(.middle)
                Text("移入：\(pending.parentPath)")
                    .font(.caption).foregroundStyle(.secondary)
                    .lineLimit(2).truncationMode(.middle)
                if !library.photoMoveFinished {
                    ProgressView().progressViewStyle(.linear)
                        .accessibilityLabel(Self.phaseLabel(library.photoMovePhase))
                    Text(Self.phaseLabel(library.photoMovePhase))
                    TimelineView(.periodic(from: .now, by: 1)) { context in
                        Text("已用时 \(max(0, Int(context.date.timeIntervalSince(pending.startedAt)))) 秒")
                            .font(.caption).monospacedDigit().foregroundStyle(.secondary)
                    }
                    Text("任务由 NAS 执行，关闭 App 后仍会继续。")
                        .font(.caption).foregroundStyle(.secondary)
                }
            }
            if let message = library.photoMoveMessage {
                ScrollView { Text(message).font(.caption).textSelection(.enabled) }
                    .frame(maxHeight: 110)
                    .foregroundStyle(library.photoMoveFinished ? .red : .secondary)
            }
            if library.photoMoveFinished {
                Button("关闭") { library.acknowledgePhotoMoveFailure() }
            }
        }
        .padding(20).frame(width: 380)
        .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 10))
    }

    static func phaseLabel(_ phase: String) -> String {
        switch phase {
        case "waiting": "等待 NAS 开始处理…"
        case "validating": "检查来源和目标文件夹…"
        case "moving": "移动照片及关联文件…"
        case "catalog": "更新照片索引…"
        case "tracking": "更新目录追踪和扫描任务…"
        case "completed": "移动完成"
        case "failed": "移动失败"
        default: "NAS 正在处理…"
        }
    }
}
