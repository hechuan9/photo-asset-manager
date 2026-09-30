import Foundation
import CryptoKit
import OSLog
import SwiftUI

/// Downloads only small derivatives; pagination never mutates the visible catalog.
public actor ThumbnailPrefetch {
    public static let shared = ThumbnailPrefetch()
    private var running: Set<String> = []

    public func run(configuration: KeepsConfiguration) async {
        let identity = [configuration.baseURL.absoluteString, configuration.libraryID].map { "\($0.utf8.count):\($0)" }.joined()
        let checkpoint = "thumbnail-prefetch-v1-" + SHA256.hash(data: Data(identity.utf8)).map { String(format: "%02x", $0) }.joined()
        while running.contains(checkpoint) {
            do { try await Task.sleep(for: .milliseconds(250)) } catch { return }
        }
        guard !Task.isCancelled else { return }
        running.insert(checkpoint)
        defer { running.remove(checkpoint) }
        let client = KeepsClient(configuration: configuration)
        while !Task.isCancelled {
            do {
                var query = KeepsAssetQuery()
                query.showHidden = true
                query.limit = 100
                query.cursor = UserDefaults.standard.string(forKey: checkpoint + "-cursor")
                query.trashed = UserDefaults.standard.bool(forKey: checkpoint + "-trash")
                let page = try await client.assets(query: query)
                for asset in page.items {
                    try Task.checkCancellation()
                    guard let thumbnail = asset.thumbnail else { continue }
                    do {
                        while await PreviewCache.standards.isDownloading {
                            try await Task.sleep(for: .seconds(1))
                        }
                        while !(try await PreviewCache.thumbnails.prefetch(assetID: asset.id, preview: thumbnail, configuration: configuration)) {
                            try await Task.sleep(for: .seconds(5))
                        }
                    } catch {
                        try Task.checkCancellation()
                        Logger(subsystem: "local.keeps", category: "thumbnail-prefetch").error("Thumbnail \(asset.id) failed: \(String(reflecting: error), privacy: .public)")
                    }
                    try await Task.sleep(for: .milliseconds(250))
                }
                // A failed asset is retried on the next pass without blocking the rest of the library.
                UserDefaults.standard.set(page.nextCursor, forKey: checkpoint + "-cursor")
                if page.nextCursor == nil {
                    UserDefaults.standard.set(!query.trashed, forKey: checkpoint + "-trash")
                    if query.trashed { try await Task.sleep(for: .seconds(300)) }
                }
                try await Task.sleep(for: .seconds(2))
            } catch {
                if Task.isCancelled { return }
                Logger(subsystem: "local.keeps", category: "thumbnail-prefetch").error("Thumbnail prefetch failed: \(String(reflecting: error), privacy: .public)")
                do { try await Task.sleep(for: .seconds(60)) } catch { return }
            }
        }
    }
}

private struct ThumbnailPrefetchModifier: ViewModifier {
    let configuration: KeepsConfiguration?
    @Environment(\.scenePhase) private var scenePhase
    private var identity: String {
        "\(String(describing: configuration))|\(scenePhase == .active)"
    }
    func body(content: Content) -> some View {
        content.task(id: identity, priority: .background) {
            guard scenePhase == .active, let configuration else { return }
            await ThumbnailPrefetch.shared.run(configuration: configuration)
        }
    }
}

extension View {
    public func prefetchKeepsThumbnails(configuration: KeepsConfiguration?) -> some View {
        modifier(ThumbnailPrefetchModifier(configuration: configuration))
    }
}
