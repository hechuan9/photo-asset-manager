import Foundation
import SwiftUI

struct RemoteDerivativeHint: Equatable, Sendable {
    var role: DerivativeRole
    var pixelSize: PixelSize

    var aspectRatio: CGFloat {
        guard pixelSize.height > 0 else { return IOSWaterfallLayout.defaultAspectRatio }
        return CGFloat(pixelSize.width) / CGFloat(pixelSize.height)
    }
}

private struct AssetGallerySnapshot {
    var assets: [Asset]
    var derivativeHints: [UUID: RemoteDerivativeHint]
}

@MainActor
final class IOSLibraryStore: ObservableObject {
    @Published private(set) var assets: [Asset] = []
    @Published private(set) var derivativeHints: [UUID: RemoteDerivativeHint] = [:]
    @Published private(set) var isSyncing = false
    @Published private(set) var didLoadInitialSnapshot = false
    @Published var lastError: String?
    @Published var lastSyncSummary = "尚未同步"
    @Published var configuration = SyncClientConfiguration.load()

    private let databasePath: URL
    private let database: SQLiteDatabase
    private let assetPageSize = 240
    private let automaticSyncIntervalNanoseconds: UInt64 = 15_000_000_000
    private var automaticSyncTask: Task<Void, Never>?
    private var automaticSyncActive = false

    init(databasePath: URL? = nil) {
        do {
            let path = try databasePath ?? Self.defaultDatabasePath()
            self.databasePath = path
            self.database = try SQLiteDatabase(path: path)
        } catch {
            fatalError(error.diagnosticDescription)
        }
    }

    var databasePathDisplayValue: String {
        databasePath.path
    }

    var hasRemoteSyncConfiguration: Bool {
        configuration.hasRemoteSync
    }

    func loadIfNeeded() {
        guard !didLoadInitialSnapshot else { return }
        didLoadInitialSnapshot = true
        refreshLocalProjection(statusOverride: "已加载本地缓存")
        restartAutomaticSyncLoop(immediate: false)
    }

    func reloadConfiguration() {
        configuration = SyncClientConfiguration.load()
        restartAutomaticSyncLoop(immediate: automaticSyncActive)
    }

    func setAutomaticSyncActive(_ isActive: Bool) {
        automaticSyncActive = isActive
        restartAutomaticSyncLoop(immediate: isActive)
    }

    func syncNow(statusPrefix: String = "同步") async {
        guard !isSyncing else { return }
        guard let baseURL = configuration.baseURL else {
            refreshLocalProjection(statusOverride: assets.isEmpty ? "未配置 control plane" : "未配置 control plane，当前显示本地缓存")
            return
        }

        let currentConfiguration = configuration
        let database = self.database
        let assetPageSize = self.assetPageSize

        isSyncing = true
        lastError = nil

        do {
            let snapshot = try await Task.detached(priority: .userInitiated) {
                let client = SyncControlPlaneHTTPClient(
                    baseURL: baseURL,
                    authentication: currentConfiguration.requestAuthentication
                )
                let service = SyncService(
                    libraryID: currentConfiguration.libraryID,
                    peerID: currentConfiguration.peerID,
                    database: database,
                    client: client
                )
                try await service.sync()
                return try Self.loadSnapshot(database: database, limit: assetPageSize)
            }.value

            apply(snapshot)
            lastSyncSummary = "\(statusPrefix)完成 · \(assets.count) 张"
        } catch {
            lastError = error.diagnosticDescription
            refreshLocalProjection(statusOverride: "\(statusPrefix)失败，已回退到本地缓存")
        }

        isSyncing = false
    }

    func derivativeHint(for assetID: UUID) -> RemoteDerivativeHint? {
        derivativeHints[assetID]
    }

    func preferredAspectRatio(for assetID: UUID) -> CGFloat {
        derivativeHints[assetID]?.aspectRatio ?? IOSWaterfallLayout.defaultAspectRatio
    }

    private func refreshLocalProjection(statusOverride: String? = nil) {
        do {
            let snapshot = try Self.loadSnapshot(database: database, limit: assetPageSize)
            apply(snapshot)
            if let statusOverride {
                lastSyncSummary = statusOverride
            } else {
                lastSyncSummary = assets.isEmpty ? "本地缓存为空" : "已加载本地缓存"
            }
        } catch {
            lastError = error.diagnosticDescription
        }
    }

    private func apply(_ snapshot: AssetGallerySnapshot) {
        assets = snapshot.assets
        derivativeHints = snapshot.derivativeHints
    }

    nonisolated private static func loadSnapshot(database: SQLiteDatabase, limit: Int) throws -> AssetGallerySnapshot {
        let assets = try database.queryAssets(filter: LibraryFilter(), limit: limit)
        var hints: [UUID: RemoteDerivativeHint] = [:]
        for asset in assets {
            let derivatives = try database.derivatives(assetID: asset.id)
            if let thumbnail = derivatives.first(where: { $0.role == .thumbnail }) {
                hints[asset.id] = RemoteDerivativeHint(role: .thumbnail, pixelSize: thumbnail.pixelSize)
            } else if let preview = derivatives.first(where: { $0.role == .preview }) {
                hints[asset.id] = RemoteDerivativeHint(role: .preview, pixelSize: preview.pixelSize)
            }
        }
        return AssetGallerySnapshot(assets: assets, derivativeHints: hints)
    }

    private static func defaultDatabasePath() throws -> URL {
        let support = try FileManager.default.url(
            for: .applicationSupportDirectory,
            in: .userDomainMask,
            appropriateFor: nil,
            create: true
        )
        let directory = support.appendingPathComponent("PhotoAssetManagerIOS", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return directory.appendingPathComponent("Library.sqlite")
    }

    private func restartAutomaticSyncLoop(immediate: Bool) {
        automaticSyncTask?.cancel()
        guard automaticSyncActive, didLoadInitialSnapshot, configuration.hasRemoteSync else { return }

        automaticSyncTask = Task { [weak self] in
            guard let self else { return }
            if immediate {
                await self.syncNow(statusPrefix: "自动同步")
            }
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: automaticSyncIntervalNanoseconds)
                guard !Task.isCancelled else { return }
                await self.syncNow(statusPrefix: "自动同步")
            }
        }
    }

    deinit {
        automaticSyncTask?.cancel()
    }
}

private extension Error {
    var diagnosticDescription: String {
        let message = (self as NSError).localizedDescription
        let reflected = String(reflecting: self)
        return reflected == message ? message : "\(message)\n\(reflected)"
    }
}
