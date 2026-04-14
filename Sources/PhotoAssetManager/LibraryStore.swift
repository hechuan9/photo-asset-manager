import AppKit
import Foundation

@MainActor
final class LibraryStore: ObservableObject {
    @Published var assets: [Asset] = []
    @Published var selectedAssetID: UUID?
    @Published var selectedFiles: [FileInstance] = []
    @Published var filter = LibraryFilter()
    @Published var counts: [AssetStatus: Int] = [:]
    @Published var isScanning = false
    @Published var scanReport = ScanReport()
    @Published var lastError: String?
    @Published var nasRoot: URL?
    @Published var interruptedScanPath: String?

    private let database: SQLiteDatabase
    private let scanner: PhotoScanner
    private let fileOperations = FileOperations()

    init() {
        do {
            let support = try Self.applicationSupport()
            database = try SQLiteDatabase(path: support.appendingPathComponent("Library.sqlite"))
            scanner = PhotoScanner(cacheRoot: support.appendingPathComponent("Cache", isDirectory: true))
            try database.markInterruptedImportBatches()
            interruptedScanPath = try database.latestInterruptedScanPath()
            try database.markMissingFiles()
            refresh()
        } catch {
            fatalError(error.fullTrace)
        }
    }

    var selectedAsset: Asset? {
        assets.first { $0.id == selectedAssetID }
    }

    func chooseAndScan(storageKind: StorageKind) {
        let panel = NSOpenPanel()
        panel.canChooseDirectories = true
        panel.canChooseFiles = false
        panel.allowsMultipleSelection = false
        panel.message = storageKind == .nas ? "选择 NAS 根目录或照片目录" : "选择要扫描的本地照片目录"
        if panel.runModal() == .OK, let url = panel.url {
            if storageKind == .nas {
                nasRoot = url
            }
            scan(url, storageKind: storageKind)
        }
    }

    func chooseNASRoot() {
        let panel = NSOpenPanel()
        panel.canChooseDirectories = true
        panel.canChooseFiles = false
        panel.allowsMultipleSelection = false
        panel.message = "选择 NAS 权威根目录"
        if panel.runModal() == .OK {
            nasRoot = panel.url
        }
    }

    func scan(_ url: URL, storageKind: StorageKind) {
        isScanning = true
        scanReport = ScanReport()
        lastError = nil
        Task {
            let report = await scanner.scanDirectory(url, storageKind: storageKind, database: database) { [weak self] report in
                self?.scanReport = report
            }
            scanReport = report
            isScanning = false
            if !report.errors.isEmpty {
                lastError = report.errors.joined(separator: "\n\n")
            }
            try? database.clearInterruptedBatches(sourcePath: url.path)
            interruptedScanPath = try? database.latestInterruptedScanPath()
            refresh()
        }
    }

    func resumeInterruptedScan() {
        guard let interruptedScanPath else { return }
        let url = URL(fileURLWithPath: interruptedScanPath)
        let storageKind: StorageKind = interruptedScanPath.hasPrefix("/Volumes/") ? .nas : .local
        if storageKind == .nas {
            nasRoot = url
        }
        scan(url, storageKind: storageKind)
    }

    func refresh() {
        do {
            try database.markMissingFiles()
            assets = try database.queryAssets(filter: filter)
            counts = try database.countsByStatus()
            if selectedAssetID == nil {
                selectedAssetID = assets.first?.id
            }
            loadSelectedFiles()
        } catch {
            lastError = error.fullTrace
        }
    }

    func setStatusFilter(_ status: AssetStatus?) {
        filter.status = status
        refresh()
    }

    func loadSelectedFiles() {
        guard let selectedAssetID else {
            selectedFiles = []
            return
        }
        do {
            selectedFiles = try database.fileInstances(assetID: selectedAssetID)
        } catch {
            lastError = error.fullTrace
        }
    }

    func update(asset: Asset) {
        do {
            try database.updateAssetMetadata(asset: asset)
            refresh()
        } catch {
            lastError = error.fullTrace
        }
    }

    func archiveSelected() {
        guard let asset = selectedAsset else { return }
        guard let nasRoot else {
            chooseNASRoot()
            guard let nasRoot else { return }
            archive(asset: asset, nasRoot: nasRoot)
            return
        }
        archive(asset: asset, nasRoot: nasRoot)
    }

    func syncSelected() {
        guard let asset = selectedAsset else { return }
        guard let nasRoot else {
            chooseNASRoot()
            guard let nasRoot else { return }
            sync(asset: asset, nasRoot: nasRoot)
            return
        }
        sync(asset: asset, nasRoot: nasRoot)
    }

    func recordExportForSelected() {
        guard let asset = selectedAsset else { return }
        let panel = NSOpenPanel()
        panel.canChooseDirectories = false
        panel.canChooseFiles = true
        panel.allowsMultipleSelection = true
        panel.message = "选择这个资产导出的 JPEG、PNG 或 TIFF"
        if panel.runModal() == .OK {
            do {
                for url in panel.urls {
                    try database.insertExport(assetID: asset.id, exportURL: url, sourceVersionID: nil)
                    try database.writeOperation(action: "record_export", source: asset.primaryPath, destination: url.path, status: "success", detail: "记录导出文件")
                }
                refresh()
            } catch {
                lastError = error.fullTrace
            }
        }
    }

    func reveal(file: FileInstance) {
        fileOperations.reveal(file)
    }

    func open(file: FileInstance) {
        fileOperations.open(file)
    }

    private func archive(asset: Asset, nasRoot: URL) {
        do {
            try fileOperations.archive(asset: asset, files: selectedFiles, nasRoot: nasRoot, database: database)
            refresh()
        } catch {
            lastError = error.fullTrace
        }
    }

    private func sync(asset: Asset, nasRoot: URL) {
        do {
            try fileOperations.syncChanges(asset: asset, files: selectedFiles, nasRoot: nasRoot, database: database)
            refresh()
        } catch {
            lastError = error.fullTrace
        }
    }

    private static func applicationSupport() throws -> URL {
        let root = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("PhotoAssetManager", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        return root
    }
}
