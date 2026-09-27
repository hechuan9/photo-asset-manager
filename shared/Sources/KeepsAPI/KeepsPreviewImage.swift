import SwiftUI

public struct KeepsPreviewImage: View {
    public let asset: KeepsAsset
    public let configuration: KeepsConfiguration?
    @Environment(\.displayScale) private var displayScale
    @State private var image: CGImage?
    @State private var error: String?
    @State private var retry = 0

    public init(asset: KeepsAsset, configuration: KeepsConfiguration?) {
        self.asset = asset
        self.configuration = configuration
    }

    public var body: some View {
        GeometryReader { geometry in
            let maxPixelSize = max(1, Int(ceil(max(geometry.size.width, geometry.size.height) * displayScale)))
            ZStack {
                Color.secondary.opacity(0.12)
                if let image {
                    Image(decorative: image, scale: displayScale)
                        .resizable().scaledToFit()
                } else if let error {
                    VStack(spacing: 6) {
                        Image(systemName: "photo.badge.exclamationmark")
                        Button("重新载入预览") { retry += 1 }.buttonStyle(.plain)
                        Text(error).font(.caption2).lineLimit(3)
                    }.font(.caption).padding(8)
                } else if asset.preview == nil {
                    VStack {
                        Image(systemName: "photo")
                        Text("预览尚未生成").font(.caption)
                    }.foregroundStyle(.secondary)
                } else if configuration == nil {
                    Text("请先连接资料库").font(.caption).foregroundStyle(.secondary)
                } else {
                    ProgressView()
                }
            }
            .task(id: taskID(maxPixelSize: maxPixelSize)) {
                await load(maxPixelSize: maxPixelSize)
            }
        }
    }

    private func taskID(maxPixelSize: Int) -> String {
        guard let preview = asset.preview, let configuration else { return asset.id.uuidString }
        return "\(PreviewCache.key(assetID: asset.id, preview: preview, configuration: configuration))|\(maxPixelSize)|\(retry)"
    }

    @MainActor
    private func load(maxPixelSize: Int) async {
        image = nil
        error = nil
        guard let preview = asset.preview, let configuration else { return }
        do {
            let loaded = try await PreviewCache.shared.image(
                assetID: asset.id, preview: preview, configuration: configuration, maxPixelSize: maxPixelSize
            )
            try Task.checkCancellation()
            image = loaded
        } catch {
            guard !Task.isCancelled, !(error is CancellationError) else { return }
            self.error = String(reflecting: error)
        }
    }
}
