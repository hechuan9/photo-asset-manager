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
    @Published var sourceDirectories: [SourceDirectory] = []

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
            sourceDirectories = try database.sourceDirectories()
            refresh()
        } catch {
            fatalError(error.fullTrace)
        }
    }

    var selectedAsset: Asset? {
        assets.first { $0.id == selectedAssetID }
    }

    func chooseAndScan(storageKind: StorageKind) {
        chooseAndAddFolders(scanImmediately: true)
    }

    func chooseAndAddFolders(scanImmediately: Bool) {
        let panel = NSOpenPanel()
        panel.canChooseDirectories = true
        panel.canChooseFiles = false
        panel.allowsMultipleSelection = true
        panel.message = "添加一个或多个照片文件夹"
        if panel.runModal() == .OK {
            addSourceDirectories(panel.urls, scanImmediately: scanImmediately)
        }
    }

    func addSourceDirectories(_ urls: [URL], scanImmediately: Bool) {
        guard !urls.isEmpty else { return }
        do {
            for url in urls {
                let storageKind = storageKind(for: url)
                try database.upsertSourceDirectory(path: url.path, storageKind: storageKind)
                if storageKind == .nas, nasRoot == nil {
                    nasRoot = nasRootURL(for: url)
                }
            }
            sourceDirectories = try database.sourceDirectories()
            if scanImmediately {
                scanSources(sourceDirectories.filter { source in
                    urls.contains { $0.path == source.path }
                })
            }
        } catch {
            lastError = error.fullTrace
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
            try? database.markSourceDirectoryScanned(path: url.path)
            try? database.clearInterruptedBatches(sourcePath: url.path)
            interruptedScanPath = try? database.latestInterruptedScanPath()
            sourceDirectories = (try? database.sourceDirectories()) ?? sourceDirectories
            refresh()
        }
    }

    func scanSource(_ source: SourceDirectory) {
        guard source.isTracked else { return }
        scan(URL(fileURLWithPath: source.path), storageKind: source.storageKind)
    }

    func scanTrackedSources() {
        scanSources(sourceDirectories.filter(\.isTracked))
    }

    func stopTrackingSource(_ source: SourceDirectory) {
        do {
            try database.setSourceDirectoryTracked(id: source.id, isTracked: false)
            sourceDirectories = try database.sourceDirectories()
        } catch {
            lastError = error.fullTrace
        }
    }

    func resumeTrackingSource(_ source: SourceDirectory) {
        do {
            try database.setSourceDirectoryTracked(id: source.id, isTracked: true)
            sourceDirectories = try database.sourceDirectories()
        } catch {
            lastError = error.fullTrace
        }
    }

    func removeSourceDirectory(_ source: SourceDirectory) {
        do {
            try database.removeSourceDirectory(id: source.id)
            sourceDirectories = try database.sourceDirectories()
        } catch {
            lastError = error.fullTrace
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
            sourceDirectories = try database.sourceDirectories()
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
        guard let root = preferredNASRoot() else {
            lastError = "没有可用的 NAS 文件夹。请先添加一个 /Volumes 下的文件夹。"
            return
        }
        archive(asset: asset, nasRoot: root)
    }

    func syncSelected() {
        guard let asset = selectedAsset else { return }
        guard let root = preferredNASRoot() else {
            lastError = "没有可用的 NAS 文件夹。请先添加一个 /Volumes 下的文件夹。"
            return
        }
        sync(asset: asset, nasRoot: root)
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

    private func scanSources(_ sources: [SourceDirectory]) {
        guard !sources.isEmpty else { return }
        Task {
            for source in sources where source.isTracked {
                await MainActor.run {
                    scan(URL(fileURLWithPath: source.path), storageKind: source.storageKind)
                }
                while await MainActor.run(body: { isScanning }) {
                    try? await Task.sleep(nanoseconds: 200_000_000)
                }
            }
        }
    }

    private func preferredNASRoot() -> URL? {
        if let nasRoot {
            return nasRoot
        }
        return sourceDirectories
            .filter { $0.isTracked && $0.storageKind == .nas }
            .map { nasRootURL(for: URL(fileURLWithPath: $0.path)) }
            .first
    }

    private func storageKind(for url: URL) -> StorageKind {
        url.path.hasPrefix("/Volumes/") ? .nas : .local
    }

    private func nasRootURL(for url: URL) -> URL {
        let components = url.pathComponents
        guard components.count >= 3, components[1] == "Volumes" else {
            return url
        }
        return URL(fileURLWithPath: "/" + components[1] + "/" + components[2], isDirectory: true)
    }

    private static func applicationSupport() throws -> URL {
        let root = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("PhotoAssetManager", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        return root
    }
}
