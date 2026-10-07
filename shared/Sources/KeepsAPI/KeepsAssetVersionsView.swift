import SwiftUI

public struct KeepsAssetVersionsView: View {
    public let assetID: UUID
    public let revision: String
    public let configuration: KeepsConfiguration
    public var onSelection: (KeepsAsset) -> Void
    @State private var versions: [KeepsAssetVersion] = []
    @State private var deprecatedFiles: [KeepsAssetVersions.DeprecatedFile] = []
    @State private var busy = false
    @State private var error: String?

    public init(assetID: UUID, revision: String, configuration: KeepsConfiguration, onSelection: @escaping (KeepsAsset) -> Void) {
        self.assetID = assetID
        self.revision = revision
        self.configuration = configuration
        self.onSelection = onSelection
    }

    public var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("文件版本").font(.headline)
            ForEach(versions) { version in
                VStack(alignment: .leading, spacing: 4) {
                    Text(version.filename).textSelection(.enabled)
                    Text("\(version.width) × \(version.height) · \(version.kind)").font(.caption).foregroundStyle(.secondary)
                    if version.isDefault {
                        Text(version.userSelected ? "默认展示 · 已手动指定" : "默认展示").font(.caption)
                    } else {
                        Button("设为默认展示") { Task { await select(version) } }
                            .disabled(busy || !version.available)
                    }
                    if !version.available { Text("文件暂不可用").font(.caption).foregroundStyle(.secondary) }
                }
            }
            if !deprecatedFiles.isEmpty {
                DisclosureGroup("已弃用的重复文件") {
                    VStack(alignment: .leading, spacing: 8) {
                        Text("以下文件仅标记为弃用，磁盘文件未删除。").foregroundStyle(.secondary)
                        ForEach(deprecatedFiles, id: \.path) { file in
                            VStack(alignment: .leading, spacing: 4) {
                                Text(file.path)
                                Text("保留位置：\(file.retainedPath)")
                                Text("弃用原因：\(file.reason)").foregroundStyle(.secondary)
                            }
                            .textSelection(.enabled)
                        }
                    }
                    .font(.caption)
                }
            }
            if versions.isEmpty && !busy && error == nil {
                Text("暂无版本信息，保留当前预览。").font(.caption).foregroundStyle(.secondary)
            }
            if busy { ProgressView() }
            if let error { Text(error).font(.caption).foregroundStyle(.red).textSelection(.enabled) }
        }
        .task(id: assetID.uuidString + ":" + revision) { await load() }
    }

    private func load() async {
        versions = []; deprecatedFiles = []; error = nil; busy = true
        defer { busy = false }
        do {
            let result = try await KeepsClient(configuration: configuration).versionDetails(assetID: assetID)
            try Task.checkCancellation()
            versions = result.items
            deprecatedFiles = result.deprecatedFiles ?? []
        } catch { if !Task.isCancelled { self.error = String(reflecting: error) } }
    }

    private func select(_ version: KeepsAssetVersion) async {
        busy = true; error = nil
        defer { busy = false }
        do {
            let client = KeepsClient(configuration: configuration)
            versions = try await client.setDefaultVersion(assetID: assetID, contentHash: version.contentHash)
            let updated = try await client.asset(id: assetID)
            try Task.checkCancellation()
            onSelection(updated)
        } catch { if !Task.isCancelled { self.error = String(reflecting: error) } }
    }
}
