import SwiftUI
import BackgroundTasks
import OSLog
import KeepsAPI

@MainActor
final class IOSThumbnailDownload: ObservableObject {
    static let shared = IOSThumbnailDownload()
    private static let identifier = "com.hechuan.Keeps.thumbnail-download"
    private static let logger = Logger(subsystem: "local.keeps", category: "thumbnail-download")
    @Published private(set) var isRunning = false
    @Published private(set) var isComplete = false
    @Published private(set) var progress: ThumbnailPrefetch.Progress?
    @Published private(set) var status = "正在准备离线图库。" {
        didSet { systemTask?.updateTitle("准备离线图库", subtitle: status) }
    }
    @Published private(set) var error: String?
    @Published private(set) var cloudStatus = "数据库快照和缩略图会同步到 Keeps 的独立 iCloud 空间。"
    private var registered = false
    private var configuration: KeepsConfiguration?
    private var work: Task<Void, Never>?
    private var runID: UUID?
    private var systemTask: BGContinuedProcessingTask?
    @Published private(set) var systemProgress = IOSOfflineProgress()
    private var backgroundTask = UIBackgroundTaskIdentifier.invalid

    func prepare(configuration: KeepsConfiguration, refreshLocal: @escaping @MainActor () async -> Void,
                 synchronize: @escaping @MainActor (@escaping IOSOfflineProgress.Reporter) async throws -> Void) async {
        guard !isRunning else { return }
        configurationChanged(configuration)
        let checkID = UUID()
        runID = checkID
        isRunning = true
        isComplete = false
        error = nil
        systemProgress = IOSOfflineProgress()
        status = "正在检查本地图库和缩略图…"
        let complete = await ThumbnailPrefetch.shared.runLocal(configuration: configuration, downloadMissing: false) { update in
            await self.reportLocalCheck(update, configuration: configuration)
        }
        guard !Task.isCancelled, self.configuration == configuration, runID == checkID else {
            if runID == checkID { runID = nil; isRunning = false; status = "准备已暂停，点击继续。" }
            return
        }
        runID = nil
        isRunning = false
        if complete {
            isComplete = true
            systemProgress.complete()
            status = "本地图库已准备完成。"
        } else {
            start(configuration: configuration, restoring: true, refreshLocal: refreshLocal, synchronize: synchronize)
        }
    }

    func update(configuration: KeepsConfiguration, synchronize: @escaping @MainActor (@escaping IOSOfflineProgress.Reporter) async throws -> Void) async {
        await work?.value
        guard !Task.isCancelled, self.configuration == configuration, !isRunning else { return }
        start(configuration: configuration, restoring: false, refreshLocal: {}, synchronize: synchronize)
        await work?.value
    }

    private func reportLocalCheck(_ update: ThumbnailPrefetch.Progress, configuration: KeepsConfiguration) {
        guard self.configuration == configuration else { return }
        progress = update
        status = "正在检查本地缩略图：\(update.processed) / \(update.total)"
    }

    private func start(configuration: KeepsConfiguration, restoring: Bool,
                       refreshLocal: @escaping @MainActor () async -> Void,
                       synchronize: @escaping @MainActor (@escaping IOSOfflineProgress.Reporter) async throws -> Void) {
        guard !isRunning else { return }
        error = nil
        self.configuration = configuration
        let id = UUID()
        runID = id
        progress = nil
        systemProgress = IOSOfflineProgress()
        isRunning = true
        status = "正在准备图库…"
        if restoring && !registered {
            registered = BGTaskScheduler.shared.register(forTaskWithIdentifier: Self.identifier, using: .main) { task in
                MainActor.assumeIsolated {
                    guard let continued = task as? BGContinuedProcessingTask else {
                        task.setTaskCompleted(success: false)
                        return
                    }
                    self.begin(continued)
                }
            }
        }
        backgroundTask = UIApplication.shared.beginBackgroundTask(withName: "Keeps library update") { [weak self] in
            Task { @MainActor in
                guard let self, self.systemTask == nil else { return }
                self.work?.cancel()
                self.endBackgroundTime()
            }
        }
        if restoring {
            let request = BGContinuedProcessingTaskRequest(identifier: Self.identifier,
                title: "准备离线图库", subtitle: "数据库、缩略图与 iCloud 副本")
            request.strategy = .fail
            do {
                if registered { try BGTaskScheduler.shared.submit(request) }
                else { Self.logger.error("Continued task registration failed") }
            } catch {
                Self.logger.error("Continued task submission failed: \(String(reflecting: error), privacy: .public)")
            }
        }
        let previous = ThumbnailBackgroundDelegate.activeWork
        previous?.cancel()
        work = Task { @MainActor in
            await previous?.value
            guard !Task.isCancelled, self.runID == id else { self.finish(id: id, completed: false); return }
            await self.run(configuration: configuration, id: id, restoring: restoring,
                           refreshLocal: refreshLocal, synchronize: synchronize)
        }
    }

    private func begin(_ task: BGContinuedProcessingTask) {
        guard isRunning, runID != nil else {
            task.setTaskCompleted(success: isComplete)
            return
        }
        systemTask = task
        endBackgroundTime()
        task.progress.totalUnitCount = systemProgress.totalUnitCount
        task.progress.completedUnitCount = systemProgress.completedUnitCount
        task.updateTitle("准备离线图库", subtitle: status)
        task.expirationHandler = { [weak self] in
            Task { @MainActor in
                guard let self else { return }
                Self.logger.notice("Continued task expired at progress \(self.systemProgress.completedUnitCount)/\(self.systemProgress.totalUnitCount)")
                self.work?.cancel()
            }
        }
    }

    private func run(configuration: KeepsConfiguration, id: UUID, restoring: Bool,
                     refreshLocal: @MainActor () async -> Void, synchronize: @MainActor (@escaping IOSOfflineProgress.Reporter) async throws -> Void) async {
        if restoring {
            do {
                _ = try await KeepsCloudReplica.shared.restore(configuration: configuration, progress: { message in
                    await self.reportCloud(message, id: id)
                }, workProgress: { completed, total in
                    await self.reportWork(.restore, completed: completed, total: total, id: id)
                })
                guard !Task.isCancelled, runID == id else { finish(id: id, completed: false); return }
                await refreshLocal()
            } catch {
                reportCloud("iCloud 恢复未完成：" + String(reflecting: error), id: id)
                Self.logger.error("Cloud restore failed: \(String(reflecting: error), privacy: .public)")
            }
        }
        guard !Task.isCancelled, runID == id else { finish(id: id, completed: false); return }
        var completed = false
        if restoring {
            completed = await ThumbnailPrefetch.shared.runLocal(configuration: configuration, downloadMissing: false) { update in
                await self.reportWork(.check, completed: Int64(update.processed), total: Int64(max(1, update.total)), id: id)
            }
        }
        guard !Task.isCancelled, runID == id else { finish(id: id, completed: false); return }
        if !completed {
            status = "正在更新照片数据库…"
            do {
                try await synchronize { stage, completed, total in
                    self.reportWork(stage, completed: completed, total: total, id: id)
                }
            } catch {
                self.error = String(reflecting: error)
                Self.logger.error("Offline rebuild failed: \(String(reflecting: error), privacy: .public)")
                finish(id: id, completed: false)
                return
            }
            guard !Task.isCancelled, runID == id else { finish(id: id, completed: false); return }
            completed = await ThumbnailPrefetch.shared.runLocal(configuration: configuration, downloadMissing: !restoring) { update in
                await self.report(update, id: id)
            }
        }
        guard !Task.isCancelled, runID == id else { finish(id: id, completed: false); return }
        if completed { isComplete = true }
        var cloudFailed = false
        do {
            try await KeepsCloudReplica.shared.backup(configuration: configuration, progress: { message in
                await self.reportCloud(message, id: id)
            }, workProgress: { completed, total in
                await self.reportWork(.backup, completed: completed, total: total, id: id)
            })
        } catch {
            cloudFailed = true
            reportCloud("iCloud 副本待继续同步：" + String(reflecting: error), id: id)
            Self.logger.error("Cloud backup failed: \(String(reflecting: error), privacy: .public)")
        }
        finish(id: id, completed: completed, cloudFailed: cloudFailed)
    }

    private func finish(id: UUID, completed: Bool, cloudFailed: Bool = false) {
        guard runID == id else { return }
        let cancelled = Task.isCancelled
        let succeeded = completed && !cancelled && !cloudFailed
        if succeeded { systemProgress.complete() }
        if cancelled { status = "准备已暂停，已缓存的文件会保留；点击继续。" }
        else if cloudFailed { status = "本地检查已结束，iCloud 副本待继续同步。" }
        else if completed { status = "离线图库准备完成。" }
        else { status = "尚未全部准备完成，已缓存的文件会保留；点击重试。" }
        systemTask?.progress.completedUnitCount = systemProgress.completedUnitCount
        systemTask?.setTaskCompleted(success: succeeded)
        endBackgroundTime()
        isRunning = false
        work = nil
        systemTask = nil
        runID = nil
    }

    private func reportCloud(_ message: String, id: UUID) {
        guard runID == id else { return }
        cloudStatus = message
        status = message
    }

    private func report(_ update: ThumbnailPrefetch.Progress, id: UUID) {
        guard runID == id else { return }
        progress = update
        error = update.lastError
        reportWork(.thumbnails, completed: Int64(update.processed), total: Int64(max(1, update.total)), id: id)
        let text = "已缓存 \(update.cached) / \(update.total) 张缩略图"
        status = text
    }

    private func reportWork(_ stage: IOSOfflineProgress.Stage, completed: Int64, total: Int64, id: UUID) {
        guard runID == id, !Task.isCancelled else { return }
        systemProgress.record(stage, completed: completed, total: total)
        switch stage {
        case .check: status = "正在检查本地缩略图：\(completed) / \(total)"
        case .snapshot: status = "NAS 正在创建数据库快照…"
        case .catalog: status = "NAS 正在生成离线数据库：\(completed) / \(total)"
        case .navigation: status = "NAS 正在打包相册目录：\(completed) / \(total)"
        case .archive: status = "NAS 正在打包离线图库…"
        case .verifying: status = "NAS 正在校验离线图库包…"
        case .download: status = "正在下载离线图库包：\(ByteCountFormatter.string(fromByteCount: completed, countStyle: .file)) / \(ByteCountFormatter.string(fromByteCount: total, countStyle: .file))"
        case .verifyDownload: status = "正在校验下载的离线图库包…"
        case .importing: status = "正在导入离线图库包：\(completed) / \(total)"
        case .restore, .thumbnails, .backup: break
        }
        systemTask?.progress.completedUnitCount = systemProgress.completedUnitCount
    }

    private func endBackgroundTime() {
        guard backgroundTask != .invalid else { return }
        UIApplication.shared.endBackgroundTask(backgroundTask)
        backgroundTask = .invalid
    }

    func configurationChanged(_ configuration: KeepsConfiguration?) {
        guard self.configuration != configuration else { return }
        if self.configuration != nil { ThumbnailBackgroundDelegate.activeWork?.cancel() }
        BGTaskScheduler.shared.cancel(taskRequestWithIdentifier: Self.identifier)
        work?.cancel()
        systemTask?.setTaskCompleted(success: false)
        endBackgroundTime()
        runID = nil
        work = nil
        systemTask = nil
        isRunning = false
        isComplete = false
        progress = nil
        systemProgress = IOSOfflineProgress()
        error = nil
        status = "正在准备离线图库。"
        cloudStatus = "数据库快照和缩略图会同步到 Keeps 的独立 iCloud 空间。"
        self.configuration = configuration
    }
}
