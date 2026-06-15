import CryptoKit
import ImageIO
import SwiftUI
import UIKit
import UniformTypeIdentifiers

struct IOSWaterfallGallery: View {
    @EnvironmentObject private var library: IOSLibraryStore
    @Environment(\.horizontalSizeClass) private var horizontalSizeClass
    @State private var measuredAspectRatios: [UUID: CGFloat] = [:]

    var body: some View {
        GeometryReader { proxy in
            let metrics = IOSWaterfallMetrics(
                containerWidth: proxy.size.width,
                isCompact: horizontalSizeClass == .compact
            )
            let aspectRatios = mergedAspectRatios()
            let columns = IOSWaterfallLayout.columns(
                assets: library.assets,
                columnCount: metrics.columnCount,
                aspectRatios: aspectRatios,
                columnWidth: metrics.columnWidth,
                spacing: metrics.spacing
            )

            ScrollView {
                HStack(alignment: .top, spacing: metrics.spacing) {
                    ForEach(Array(columns.enumerated()), id: \.offset) { _, column in
                        LazyVStack(spacing: metrics.spacing) {
                            ForEach(column.items) { asset in
                                IOSAssetCard(
                                    asset: asset,
                                    ratioHint: aspectRatios[asset.id] ?? IOSWaterfallLayout.defaultAspectRatio,
                                    remoteHint: library.derivativeHint(for: asset.id),
                                    configuration: library.configuration
                                ) { ratio in
                                    measuredAspectRatios[asset.id] = ratio
                                }
                                .frame(height: metrics.columnWidth / max(0.35, aspectRatios[asset.id] ?? IOSWaterfallLayout.defaultAspectRatio))
                            }
                        }
                        .frame(width: metrics.columnWidth)
                    }
                }
                .padding(.horizontal, metrics.outerPadding)
                .padding(.bottom, 24)
            }
            .refreshable {
                await library.syncNow(statusPrefix: "立即刷新")
            }
        }
    }

    private func mergedAspectRatios() -> [UUID: CGFloat] {
        var merged = measuredAspectRatios
        for asset in library.assets where merged[asset.id] == nil {
            merged[asset.id] = library.preferredAspectRatio(for: asset.id)
        }
        return merged
    }
}

struct IOSWaterfallLayout {
    static let defaultAspectRatio: CGFloat = 0.82

    struct Column {
        var items: [Asset] = []
        var totalHeight: CGFloat = 0
    }

    static func columns(
        assets: [Asset],
        columnCount: Int,
        aspectRatios: [UUID: CGFloat],
        columnWidth: CGFloat,
        spacing: CGFloat
    ) -> [Column] {
        guard columnCount > 0 else { return [] }
        var columns = Array(repeating: Column(), count: columnCount)

        for asset in assets {
            let ratio = max(0.35, aspectRatios[asset.id] ?? defaultAspectRatio)
            let itemHeight = columnWidth / ratio
            let nextIndex = columns.enumerated().min(by: { $0.element.totalHeight < $1.element.totalHeight })?.offset ?? 0
            columns[nextIndex].items.append(asset)
            columns[nextIndex].totalHeight += itemHeight + (columns[nextIndex].items.count > 1 ? spacing : 0)
        }

        return columns
    }
}

private struct IOSWaterfallMetrics {
    var containerWidth: CGFloat
    var isCompact: Bool

    var columnCount: Int {
        if containerWidth >= 1080 { return 5 }
        if containerWidth >= 820 { return 4 }
        return isCompact ? 2 : 3
    }

    let spacing: CGFloat = 8
    let outerPadding: CGFloat = 12

    var columnWidth: CGFloat {
        let totalSpacing = outerPadding * 2 + CGFloat(max(0, columnCount - 1)) * spacing
        return max(80, (containerWidth - totalSpacing) / CGFloat(columnCount))
    }
}

struct IOSAssetCard: View {
    var asset: Asset
    var ratioHint: CGFloat
    var remoteHint: RemoteDerivativeHint?
    var configuration: SyncClientConfiguration
    var onAspectRatioChange: (CGFloat) -> Void

    var body: some View {
        ZStack(alignment: .bottomLeading) {
            RoundedRectangle(cornerRadius: 18, style: .continuous)
                .fill(Color.white.opacity(0.06))

            IOSAssetPreviewImage(
                asset: asset,
                remoteHint: remoteHint,
                configuration: configuration,
                onAspectRatioChange: onAspectRatioChange
            )
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .background(Color.black.opacity(0.28))
            .clipShape(RoundedRectangle(cornerRadius: 18, style: .continuous))

            LinearGradient(
                colors: [Color.clear, Color.black.opacity(0.75)],
                startPoint: .top,
                endPoint: .bottom
            )
            .clipShape(RoundedRectangle(cornerRadius: 18, style: .continuous))

            VStack(alignment: .leading, spacing: 3) {
                Text(asset.originalFilename)
                    .font(.system(size: 12, weight: .semibold))
                    .foregroundStyle(.white)
                    .lineLimit(1)

                Text(captureTimeLabel)
                    .font(.system(size: 11, weight: .medium))
                    .foregroundStyle(.white.opacity(0.75))
                    .lineLimit(1)
            }
            .padding(12)

            VStack {
                HStack {
                    Spacer()
                    if asset.flagState != .unflagged {
                        Image(systemName: asset.flagState == .picked ? "flag.fill" : "xmark")
                            .font(.system(size: 11, weight: .bold))
                            .foregroundStyle(.white)
                            .frame(width: 24, height: 24)
                            .background(flagBadgeColor)
                            .clipShape(Circle())
                            .padding(10)
                    }
                }
                Spacer()
            }
        }
        .clipShape(RoundedRectangle(cornerRadius: 18, style: .continuous))
        .overlay(
            RoundedRectangle(cornerRadius: 18, style: .continuous)
                .stroke(Color.white.opacity(0.06), lineWidth: 1)
        )
        .shadow(color: Color.black.opacity(0.18), radius: 14, x: 0, y: 8)
        .accessibilityLabel(asset.originalFilename)
    }

    private var flagBadgeColor: Color {
        switch asset.flagState {
        case .unflagged:
            return .clear
        case .picked:
            return Color(red: 0.16, green: 0.60, blue: 0.32)
        case .rejected:
            return Color(red: 0.47, green: 0.47, blue: 0.49)
        }
    }

    private var captureTimeLabel: String {
        IOSDateFormatters.captureTime.string(from: asset.captureTime ?? asset.createdAt)
    }
}

struct IOSAssetPreviewImage: View {
    var asset: Asset
    var remoteHint: RemoteDerivativeHint?
    var configuration: SyncClientConfiguration
    var onAspectRatioChange: (CGFloat) -> Void
    @StateObject private var loader = IOSImagePreviewLoader()

    var body: some View {
        Group {
            if let image = loader.image {
                Image(uiImage: image)
                    .resizable()
                    .aspectRatio(contentMode: .fill)
            } else {
                VStack(spacing: 8) {
                    Image(systemName: "photo")
                        .font(.system(size: 28))
                        .foregroundStyle(.white.opacity(0.72))
                    Text("等待预览图")
                        .font(.system(size: 11, weight: .medium))
                        .foregroundStyle(.white.opacity(0.50))
                }
            }
        }
        .clipped()
        .task(id: loader.cacheKey(asset: asset, remoteHint: remoteHint, configuration: configuration)) {
            await loader.load(
                asset: asset,
                remoteHint: remoteHint,
                configuration: configuration,
                onAspectRatioChange: onAspectRatioChange
            )
        }
    }
}

@MainActor
private final class IOSImagePreviewCache {
    static let shared = IOSImagePreviewCache()

    private let cache = NSCache<NSString, UIImage>()

    private init() {
        cache.countLimit = 800
    }

    func image(forKey key: String) -> UIImage? {
        cache.object(forKey: key as NSString)
    }

    func insert(_ image: UIImage, forKey key: String) {
        cache.setObject(image, forKey: key as NSString)
    }
}

// 磁盘预览图缓存：支持后台全量预取（即便 10万+ 张）。文件放 Caches/Previews/，下次启动可直接命中，不重复下载。
// 命中检查优先内存 -> 磁盘 -> 网络。prefetchIfNeeded 只填充缓存，不触碰 UI 绑定。
private enum DiskPreviewCache {
    static let directory: URL = {
        let caches = FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask).first!
        let dir = caches.appendingPathComponent("Previews", isDirectory: true)
        try? FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }()

    static func url(for cacheKey: String) -> URL {
        let digest = SHA256.hash(data: Data(cacheKey.utf8))
        let hex = digest.map { String(format: "%02x", $0) }.joined()
        return directory.appendingPathComponent("\(hex).heic")
    }

    static func load(for cacheKey: String) -> UIImage? {
        let url = url(for: cacheKey)
        guard let data = try? Data(contentsOf: url) else { return nil }
        return UIImage(data: data)
    }

    static func save(_ image: UIImage, for cacheKey: String) {
        let url = url(for: cacheKey)
        guard let data = heifData(for: image) else { return }
        try? data.write(to: url, options: [.atomic])
    }

    private static func heifData(for image: UIImage) -> Data? {
        guard let cgImage = image.cgImage else { return nil }
        let data = NSMutableData()
        guard let destination = CGImageDestinationCreateWithData(data, UTType.heic.identifier as CFString, 1, nil) else {
            return nil
        }
        CGImageDestinationAddImage(
            destination,
            cgImage,
            [kCGImageDestinationLossyCompressionQuality: 0.72] as CFDictionary
        )
        guard CGImageDestinationFinalize(destination) else { return nil }
        return data as Data
    }
}

@MainActor
final class IOSImagePreviewLoader: ObservableObject {
    @Published var image: UIImage?
    private var loadedCacheKey: String?
    private var loadTask: Task<Void, Never>?

    deinit {
        loadTask?.cancel()
    }

    static func cacheKey(asset: Asset, remoteHint: RemoteDerivativeHint?, configuration: SyncClientConfiguration) -> String {
        let localKey = asset.previewPath ?? asset.id.uuidString
        let remoteKey = remoteHint.map { "\($0.role.rawValue):\($0.pixelSize.width)x\($0.pixelSize.height)" } ?? "none"
        return "\(localKey)|\(remoteKey)|\(configuration.trimmedBaseURLString)"
    }

    // 实例方法保持兼容调用点（.task id 等）
    func cacheKey(asset: Asset, remoteHint: RemoteDerivativeHint?, configuration: SyncClientConfiguration) -> String {
        Self.cacheKey(asset: asset, remoteHint: remoteHint, configuration: configuration)
    }

    func load(
        asset: Asset,
        remoteHint: RemoteDerivativeHint?,
        configuration: SyncClientConfiguration,
        onAspectRatioChange: @escaping (CGFloat) -> Void
    ) async {
        let cacheKey = cacheKey(asset: asset, remoteHint: remoteHint, configuration: configuration)
        guard loadedCacheKey != cacheKey else { return }

        loadTask?.cancel()
        loadedCacheKey = cacheKey

        if let cached = IOSImagePreviewCache.shared.image(forKey: cacheKey) {
            image = cached
            reportAspectRatio(for: cached, onAspectRatioChange: onAspectRatioChange)
            return
        }

        // 磁盘命中：后台预取或上次会话保存的预览图直接使用，跳过网络
        if let disk = DiskPreviewCache.load(for: cacheKey) {
            IOSImagePreviewCache.shared.insert(disk, forKey: cacheKey)
            image = disk
            reportAspectRatio(for: disk, onAspectRatioChange: onAspectRatioChange)
            return
        }

        image = nil
        let task = Task(priority: .utility) {
            let loaded = await Self.loadImage(asset: asset, remoteHint: remoteHint, configuration: configuration)
            guard !Task.isCancelled else { return }
            await MainActor.run {
                guard self.loadedCacheKey == cacheKey else { return }
                if let loaded {
                    IOSImagePreviewCache.shared.insert(loaded, forKey: cacheKey)
                    DiskPreviewCache.save(loaded, for: cacheKey)
                    self.reportAspectRatio(for: loaded, onAspectRatioChange: onAspectRatioChange)
                }
                self.image = loaded
            }
        }
        loadTask = task
        await task.value
    }

    private func reportAspectRatio(for image: UIImage, onAspectRatioChange: (CGFloat) -> Void) {
        guard image.size.width > 0, image.size.height > 0 else { return }
        onAspectRatioChange(image.size.width / image.size.height)
    }

    private static func loadImage(
        asset: Asset,
        remoteHint: RemoteDerivativeHint?,
        configuration: SyncClientConfiguration
    ) async -> UIImage? {
        if let previewPath = asset.previewPath, let image = loadLocalImage(path: previewPath) {
            return image
        }
        guard let remoteHint, let baseURL = configuration.baseURL else {
            return nil
        }
        let client = SyncControlPlaneHTTPClient(
            baseURL: baseURL,
            authentication: configuration.requestAuthentication
        )
        guard let metadata = try? await client.fetchDerivativeMetadata(
            libraryID: configuration.libraryID,
            assetID: asset.id,
            role: remoteHint.role
        ) else {
            return nil
        }
        guard let (data, _) = try? await URLSession.shared.data(from: metadata.downloadURL) else {
            return nil
        }
        return UIImage(data: data)
    }

    private static func loadLocalImage(path: String) -> UIImage? {
        UIImage(contentsOfFile: path)
    }

    // 后台预取入口：为 ledger 中所有资产的预览图拉取并落盘。
    // 由 store 在首次有数据时用 detached task 调用。内部快速跳过已缓存项，只在 miss 时做网络。
    // 即便资产很多，也只在后台低优先级缓慢进行，不阻塞 UI 或前台同步。
    static func prefetchIfNeeded(
        asset: Asset,
        remoteHint: RemoteDerivativeHint?,
        configuration: SyncClientConfiguration
    ) async {
        let key = Self.cacheKey(asset: asset, remoteHint: remoteHint, configuration: configuration)

        if IOSImagePreviewCache.shared.image(forKey: key) != nil {
            return
        }
        if FileManager.default.fileExists(atPath: DiskPreviewCache.url(for: key).path) {
            return
        }

        print("[Prefetch] miss for \(asset.id), will fetch derivative meta role=\(remoteHint?.role.rawValue ?? "nil")")

        guard let remoteHint, let baseURL = configuration.baseURL else {
            return
        }

        let client = SyncControlPlaneHTTPClient(
            baseURL: baseURL,
            authentication: configuration.requestAuthentication
        )
        guard let metadata = try? await client.fetchDerivativeMetadata(
            libraryID: configuration.libraryID,
            assetID: asset.id,
            role: remoteHint.role
        ) else {
            print("[Prefetch] meta fetch failed for \(asset.id)")
            return
        }
        guard let (data, _) = try? await URLSession.shared.data(from: metadata.downloadURL),
              let ui = UIImage(data: data) else {
            return
        }

        DiskPreviewCache.save(ui, for: key)
        await MainActor.run {
            IOSImagePreviewCache.shared.insert(ui, forKey: key)
        }
        print("[Prefetch] saved to disk for \(asset.id)")
    }
}

private enum IOSDateFormatters {
    static let captureTime: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateStyle = .medium
        formatter.timeStyle = .short
        return formatter
    }()
}
