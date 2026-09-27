import SwiftUI
import KeepsAPI

struct NASTasksView: View {
    let client: KeepsClient
    @Environment(\.dismiss) private var dismiss
    @State private var rootPath = ""
    @State private var folders: [KeepsFolder] = []
    @State private var jobs: [KeepsJob] = []
    @State private var newPath = "."
    @State private var isBusy = false
    @State private var errorMessage: String?

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            HStack {
                Text("来源与任务").font(.title2)
                Spacer()
                if isBusy { ProgressView().controlSize(.small) }
                Button("刷新") { perform { try await refresh() } }.disabled(isBusy)
                Button("完成") { dismiss() }.keyboardShortcut(.cancelAction)
            }
            Text("服务器原片根目录：\(rootPath.isEmpty ? "等待连接" : rootPath)")
                .font(.caption).foregroundStyle(.secondary).textSelection(.enabled)
            Text("此处管理服务器目录，无需在本机挂载 NAS。输入相对于服务器原片根目录的路径；“.” 表示整个根目录。停止追踪只修改索引配置，保留所有原片。")
                .font(.caption).foregroundStyle(.secondary)
            HStack {
                TextField("例如：2026/旅行，或 .", text: $newPath).textFieldStyle(.roundedBorder)
                    .accessibilityIdentifier("nas-folder-path")
                Button("添加追踪") {
                    perform {
                        _ = try await client.addFolder(path: newPath.trimmingCharacters(in: .whitespacesAndNewlines))
                        try await refresh()
                    }
                }
                .disabled(isBusy || newPath.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
            }
            if let errorMessage {
                Text(errorMessage).foregroundStyle(.red).textSelection(.enabled)
                    .font(.caption).accessibilityIdentifier("nas-task-error")
            }
            ScrollView {
                VStack(alignment: .leading, spacing: 12) {
                    Text("服务器来源").font(.headline)
                    if folders.isEmpty { Text("尚未添加服务端目录").foregroundStyle(.secondary) }
                    ForEach(folders) { folder in
                        HStack {
                            Text(folder.path).textSelection(.enabled)
                            Spacer()
                            Text(folder.active ? "追踪中" : "已停止").foregroundStyle(.secondary)
                            if folder.active {
                                Button("扫描") { perform {
                                    _ = try await client.scanFolder(id: folder.id)
                                    try await refresh()
                                } }
                                Button("停止追踪") { perform {
                                    try await client.removeFolder(id: folder.id)
                                    try await refresh()
                                } }
                            }
                        }.disabled(isBusy)
                    }
                    Divider()
                    Text("扫描任务").font(.headline)
                    if jobs.isEmpty { Text("暂无任务").foregroundStyle(.secondary) }
                    ForEach(jobs) { job in
                        VStack(alignment: .leading, spacing: 4) {
                            HStack {
                                Text(job.path)
                                Spacer()
                                Text(statusLabel(job.status)).foregroundStyle(.secondary)
                                if job.status == "failed" {
                                    Button("重试") { perform {
                                        _ = try await client.retryJob(id: job.id)
                                        try await refresh()
                                    } }.disabled(isBusy)
                                }
                            }
                            HStack(spacing: 14) {
                                if let processed = job.processed { Text("已处理 \(processed)") }
                                if let skipped = job.skipped { Text("已跳过 \(skipped)") }
                                if let failed = job.failed { Text("失败 \(failed)") }
                            }
                            .font(.caption).monospacedDigit().foregroundStyle(.secondary)
                            if let path = job.currentPath {
                                Text("当前文件：\(path)").font(.caption).lineLimit(2)
                                    .truncationMode(.middle).textSelection(.enabled)
                                    .help(path)
                            }
                            HStack(spacing: 14) {
                                if let startedAt = job.startedAt {
                                    Text("开始：\(Date(timeIntervalSince1970: TimeInterval(startedAt)).formatted(date: .abbreviated, time: .standard))")
                                }
                                if let finishedAt = job.finishedAt {
                                    Text("结束：\(Date(timeIntervalSince1970: TimeInterval(finishedAt)).formatted(date: .abbreviated, time: .standard))")
                                }
                            }.font(.caption2).foregroundStyle(.secondary)
                            if let error = job.error { Text(error).font(.caption).foregroundStyle(.red).textSelection(.enabled) }
                        }
                    }
                }.frame(maxWidth: .infinity, alignment: .leading)
            }
        }
        .padding(20).frame(width: 680, height: 520)
        .task {
            await run { try await refresh() }
            while !Task.isCancelled {
                do { try await Task.sleep(for: .seconds(5)) } catch { return }
                if !isBusy { await run { try await refresh() } }
            }
        }
    }

    private func statusLabel(_ status: String) -> String {
        switch status {
        case "pending", "queued": "等待中"
        case "running": "扫描中"
        case "completed": "已完成"
        case "failed": "失败"
        case "cancelled": "已停止"
        default: status
        }
    }

    private func refresh() async throws {
        let response = try await client.folders()
        let tasks = try await client.jobs()
        rootPath = response.rootPath
        folders = response.folders
        jobs = tasks.jobs
    }

    private func perform(_ action: @escaping @MainActor () async throws -> Void) {
        Task { await run(action) }
    }

    @MainActor private func run(_ action: @MainActor () async throws -> Void) async {
        guard !isBusy else { return }
        isBusy = true
        defer { isBusy = false }
        do {
            try await action()
            errorMessage = nil
        } catch is CancellationError {
        } catch {
            errorMessage = String(reflecting: error) + "\n" + error.localizedDescription
        }
    }
}
