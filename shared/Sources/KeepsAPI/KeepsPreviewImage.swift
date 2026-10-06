import SwiftUI

public struct KeepsPreviewImage: View {
    public let asset: KeepsAsset
    public let configuration: KeepsConfiguration?
    private let contentMode: ContentMode
    private let loadStandard: Bool
    @Environment(\.displayScale) private var displayScale
    @State private var image: CGImage?
    @State private var error: String?
    @State private var retry = 0

    public init(asset: KeepsAsset, configuration: KeepsConfiguration?, contentMode: ContentMode = .fit, loadStandard: Bool = false) {
        self.asset = asset
        self.configuration = configuration
        self.contentMode = contentMode
        self.loadStandard = loadStandard
    }

    public var body: some View {
        GeometryReader { geometry in
            let maxPixelSize = max(1, Int(ceil(max(geometry.size.width, geometry.size.height) * displayScale)))
            ZStack {
                Color.secondary.opacity(0.12)
                if let image {
                    Image(decorative: image, scale: displayScale)
                        .resizable().aspectRatio(contentMode: contentMode)
                        .frame(width: geometry.size.width, height: geometry.size.height).clipped()
                        .overlay(alignment: .bottom) {
                            if let error {
                                VStack {
                                    Button("重新载入照片") { retry += 1 }
                                    Text(error).font(.caption2).lineLimit(3)
                                }.padding(8).background(.regularMaterial)
                            }
                        }
                } else if let error {
                    VStack(spacing: 6) {
                        Image(systemName: "photo.badge.exclamationmark")
                        Button("重新载入预览") { retry += 1 }.buttonStyle(.plain)
                        Text(error).font(.caption2).lineLimit(3)
                    }.font(.caption).padding(8)
                } else if asset.gridPreview == nil && (!loadStandard || asset.standard == nil) {
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
        guard let preview = asset.gridPreview ?? (loadStandard ? asset.standard : nil), let configuration else { return asset.id.uuidString }
        return "\(PreviewCache.key(assetID: asset.id, preview: preview, configuration: configuration))|\(asset.standard?.version ?? "")|\(loadStandard)|\(maxPixelSize)|\(retry)"
    }

    @MainActor
    private func load(maxPixelSize: Int) async {
        image = nil
        error = nil
        guard let configuration else { return }
        if let preview = asset.gridPreview {
            await display(preview, role: asset.thumbnail == nil ? .preview : .thumbnail, configuration: configuration, pixels: maxPixelSize)
        }
        guard !Task.isCancelled, loadStandard, let standard = asset.standard else { return }
        await display(standard, role: .standard, configuration: configuration, pixels: maxPixelSize)
    }

    @MainActor
    private func display(_ descriptor: KeepsPreview, role: KeepsMediaRole, configuration: KeepsConfiguration, pixels: Int) async {
        do {
            let loaded = try await PreviewCache.cache(for: role).image(
                assetID: asset.id, preview: descriptor, configuration: configuration, maxPixelSize: pixels
            )
            try Task.checkCancellation()
            image = loaded
            error = nil
        } catch {
            guard !Task.isCancelled, !(error is CancellationError) else { return }
            self.error = String(reflecting: error)
        }
    }
}
