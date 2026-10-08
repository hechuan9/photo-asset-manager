import Combine
import Foundation
import KeepsAPI

struct AIEditingBatch: Codable {
    struct Item: Codable, Identifiable {
        enum Phase: String, Codable { case preparing, downloading, grading, uploading, done, review }
        let id: UUID
        let assetID: UUID
        let name: String
        var phase: Phase = .preparing
        var source: KeepsEditState?
        var result: AIGradeResult?
        var failure: String?
        var failureDetails: String?
        var phaseSeconds: [String: Double]?
        var terminal: Bool { phase == .done || phase == .review }
    }
    let id: UUID
    let baseURL: String
    let libraryID: String
    var confirmed = false
    var cancelled = false
    var items: [Item]
}

@MainActor final class AIEditingBatchStore: ObservableObject {
    let editor: AIEditingSettingsStore
    @Published private(set) var batch: AIEditingBatch?
    @Published private(set) var status = "准备 AI 调色"
    @Published private(set) var errorMessage: String?
    @Published private(set) var errorDetails: String?
    @Published private(set) var diagnosticDirectory: URL?
    @Published private(set) var isRunning = false
    var isBlocking: Bool { batch != nil }
    var isAwaitingUpload: Bool { batch?.items.first(where: { !$0.terminal })?.phase == .uploading }
    var pendingResultURL: URL? { isAwaitingUpload ? batch?.items.first(where: { !$0.terminal })?.result?.fullSize : nil }
    var isAwaitingConfirmation: Bool { batch != nil && batch?.confirmed == false && batch?.cancelled == false }
    var totalCount: Int { batch?.items.count ?? 0 }
    var completedCount: Int { batch?.items.filter(\.terminal).count ?? 0 }
    var isFinished: Bool { batch != nil && !isRunning && (batch!.cancelled || batch!.items.allSatisfy(\.terminal)) }
    private let root: URL
    private var manifest: URL { root.appendingPathComponent("batch.json") }
    private weak var library: LibraryStore?
    private var worker: Task<Void, Never>?
    struct Activity: Identifiable {
        let id: UUID
        let name: String
        var title = "等待处理"
        var progress = 0.0
        var ceiling = 0.0
        var elapsedSeconds = 0
    }
    @Published private(set) var activities: [UUID: Activity] = [:]
    @Published private(set) var elapsedSeconds = 0
    var activeItems: [Activity] { batch?.items.compactMap { activities[$0.id] } ?? [] }
    var activeCount: Int { activities.count }
    var failedItems: [AIEditingBatch.Item] { batch?.items.filter { $0.failure != nil } ?? [] }
    var waitingCount: Int { max(0, totalCount - completedCount - activeCount - failedItems.count) }
    var overallProgress: Double {
        min(1, (Double(completedCount) + activities.values.reduce(0) { $0 + $1.progress }) / Double(max(1, totalCount)))
    }
    private var progressTicker: Task<Void, Never>?
    private var gates: [String: Int] = [:]
    private var limits = AIEditingLimits()
    private var aiResumeAfter = Date.distantPast
    private var storageError: Error?
    private var restored = false
    var retryInterval: Duration = .seconds(5)

    init(root: URL? = nil, editor: AIEditingSettingsStore = AIEditingSettingsStore()) {
        self.editor = editor
        self.root = root ?? editor.root.appendingPathComponent("batches", isDirectory: true)
        do {
            try FileManager.default.createDirectory(at: self.root, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
            if FileManager.default.fileExists(atPath: manifest.path) {
                batch = try JSONDecoder().decode(AIEditingBatch?.self, from: Data(contentsOf: manifest))
            }
        } catch { storageError = error; recordFailure(error) }
    }

    func prepare(library: LibraryStore) {
        guard !isBlocking, !library.isOperationBlocking, !library.isMutating, !library.isSelectingAll,
              !library.isCheckingConnection, !library.isUpdatingHiddenDirectory, !library.isImportingPhotos,
              library.directoryToRename == nil, library.directoryToTrash == nil,
              !editor.isBusy, !library.selectedIDs.isEmpty, let configuration = library.configuration else { return }
        do {
            if let storageError { throw storageError }
            let selected = library.selectedIDs
            let names = Dictionary(uniqueKeysWithValues: library.assets.map { ($0.id, $0.originalFilename) })
            batch = AIEditingBatch(id: UUID(), baseURL: configuration.baseURL.absoluteString, libraryID: configuration.libraryID,
                items: selected.sorted { $0.uuidString < $1.uuidString }.map { .init(id: UUID(), assetID: $0, name: names[$0] ?? $0.uuidString) })
            try persist()
            self.library = library
            library.isAIEditingBlocking = true
            library.pauseLibraryForDirectoryOperation()
            errorMessage = nil; errorDetails = nil; diagnosticDirectory = nil
            status = "确认对所选 \(totalCount) 张照片进行 AI 调色"
        } catch { batch = nil; library.lastError = String(reflecting: error) }
    }

    func restore(library: LibraryStore) {
        guard !restored else { return }
        restored = true
        self.library = library
        guard let batch else { return }
        library.isAIEditingBlocking = true
        library.pauseLibraryForDirectoryOperation()
        if batch.confirmed && !isFinished { start() }
    }

    func start() {
        guard worker == nil, !editor.isBusy, batch != nil, !isFinished else { return }
        do {
            guard let client = library?.client, matches(client) else { throw AIEditingFailure("请使用创建此任务时的资料库连接。") }
            for index in batch!.items.indices { batch!.items[index].failure = nil; batch!.items[index].failureDetails = nil }
            batch!.confirmed = true
            try persist()
            errorMessage = nil; errorDetails = nil; diagnosticDirectory = nil
            limits = editor.limits.bounded
            editor.batchActive = true
            isRunning = true
            elapsedSeconds = 0
            progressTicker = Task { [weak self] in
                while !Task.isCancelled {
                    do { try await Task.sleep(for: .seconds(1)) } catch { return }
                    self?.tickProgress()
                }
            }
            worker = Task { await run(client: client) }
        } catch { recordFailure(error); status = "任务已暂停，请按下方提示处理后重试" }
    }

    func retry() { start() }

    func stopForExit() {
        worker?.cancel()
        editor.stopForExit()
    }

    func cancel() {
        guard batch != nil else { return }
        batch!.cancelled = true
        editor.cancel()
        worker?.cancel()
        status = "正在停止；已提交的调色结果保留"
        do { try persist() } catch { recordFailure(error) }
        if worker == nil { status = "已取消；已提交的结果保留" }
    }

    func dismiss() {
        guard !isRunning else { return }
        let previous = batch
        batch = nil
        do { try persist() }
        catch { batch = previous; recordFailure(error); return }
        library?.isAIEditingBlocking = false
        library?.refresh(force: true)
        library?.refreshNavigation()
    }

    private func recordFailure(_ error: Error, context: String? = nil) {
        errorMessage = [context, AIEditingFailure.userMessage(error)].compactMap { $0 }.joined(separator: "\n")
        errorDetails = AIEditingSettingsStore.redacted(String(reflecting: error))
        diagnosticDirectory = (error as? AIEditingFailure)?.logDirectory ?? root
        do {
            try Data((errorDetails ?? "").utf8).write(to: root.appendingPathComponent("failure.log"), options: .atomic)
        } catch {
            errorDetails = (errorDetails ?? "") + "\n诊断日志无法写入：" + error.localizedDescription
        }
    }

    private func matches(_ client: KeepsClient) -> Bool {
        batch?.baseURL == client.configuration.baseURL.absoluteString && batch?.libraryID == client.configuration.libraryID
    }

    private func persist() throws { try JSONEncoder().encode(batch).write(to: manifest, options: .atomic) }

    private func save(_ item: AIEditingBatch.Item, index: Int) throws {
        batch!.items[index] = item
        try persist()
    }

    private func run(client: KeepsClient) async {
        defer {
            progressTicker?.cancel(); progressTicker = nil
            activities = [:]
            isRunning = false; worker = nil
            editor.batchActive = false
            if batch?.cancelled == true { status = "已取消；已提交的结果保留" }
        }
        let pending = batch!.items.filter { !$0.terminal }.map(\.id)
        let limit = max(1, limits.photos)
        await withTaskGroup(of: Void.self) { group in
            var next = 0
            for _ in 0..<min(limit, pending.count) {
                let id = pending[next]; next += 1
                group.addTask { await self.process(id: id, client: client) }
            }
            while await group.next() != nil {
                if Task.isCancelled { group.cancelAll(); continue }
                if next < pending.count {
                    let id = pending[next]; next += 1
                    group.addTask { await self.process(id: id, client: client) }
                }
            }
        }
        if batch?.cancelled == false {
            status = failedItems.isEmpty
                ? "已处理 \(completedCount) 张；\(batch!.items.filter { $0.phase == .review }.count) 张需要检查"
                : "已完成 \(completedCount) 张，\(failedItems.count) 张待重试；已调好的结果保留在本机"
        }
    }

    private func process(id: UUID, client: KeepsClient) async {
        guard let index = batch?.items.firstIndex(where: { $0.id == id }) else { return }
        activities[id] = Activity(id: id, name: batch!.items[index].name)
        defer { activities[id] = nil }
        var aiRetries = 0
        while !Task.isCancelled, batch?.cancelled == false {
            var item = batch!.items[index]
            guard !item.terminal else { return }
            let phase = item.phase.rawValue
            let started = Date()
            do {
                try await advance(&item, client: client)
                item.phaseSeconds = item.phaseSeconds ?? [:]
                item.phaseSeconds![phase, default: 0] += Date().timeIntervalSince(started)
                try save(item, index: index)
            } catch {
                item.phaseSeconds = item.phaseSeconds ?? [:]
                item.phaseSeconds![phase, default: 0] += Date().timeIntervalSince(started)
                if Task.isCancelled || batch?.cancelled == true {
                    do { try save(item, index: index) } catch { recordFailure(error) }
                    return
                }
                if Self.isTransient(error) {
                    if item.phase == .grading {
                        aiRetries += 1
                        aiResumeAfter = max(aiResumeAfter, Date().addingTimeInterval(min(120, 30 * pow(2, Double(min(aiRetries - 1, 2))))))
                    }
                    stage(id, item.phase == .grading ? "AI 暂时不可用，等待后重试" : "连接暂时不可用，等待重连", activities[id]?.progress ?? 0, activities[id]?.progress ?? 0)
                    do { try save(item, index: index); try await Task.sleep(for: retryInterval) }
                    catch { if !Task.isCancelled { recordFailure(error) }; return }
                } else {
                    item.failure = AIEditingFailure.userMessage(error)
                    item.failureDetails = AIEditingSettingsStore.redacted(String(reflecting: error))
                    do { try save(item, index: index) } catch { recordFailure(error); return }
                    recordFailure(error, context: item.name + "：" + (activities[id]?.title ?? ""))
                    return
                }
            }
        }
    }

    private func stage(_ id: UUID, _ title: String, _ floor: Double, _ ceiling: Double) {
        guard var activity = activities[id] else { return }
        activity.title = title
        activity.progress = max(activity.progress, floor)
        activity.ceiling = max(activity.progress, ceiling)
        activities[id] = activity
        status = "正在处理 \(activeCount) 张，等待 \(waitingCount) 张，待重试 \(failedItems.count) 张"
    }

    func tickProgress() {
        guard isRunning else { return }
        elapsedSeconds += 1
        for id in Array(activities.keys) {
            activities[id]!.elapsedSeconds += 1
            activities[id]!.progress += (activities[id]!.ceiling - activities[id]!.progress) * 0.035
        }
    }

    private func acquire(_ name: String, limit: Int) async throws {
        while gates[name, default: 0] >= max(1, limit) || (name == "ai" && Date() < aiResumeAfter) {
            try await Task.sleep(for: .milliseconds(100))
        }
        try Task.checkCancellation()
        gates[name, default: 0] += 1
    }

    private func advance(_ item: inout AIEditingBatch.Item, client: KeepsClient) async throws {
        let directory = root.appendingPathComponent(item.id.uuidString.lowercased(), isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        if item.phase == .preparing {
            stage(item.id, "读取底片（\(completedCount + 1)/\(totalCount)）", 0.01, 0.04)
            let state = try await client.editState(assetID: item.assetID)
            if state.lastRequestID == item.id.uuidString.lowercased() { item.phase = .done; return }
            guard state.sourceAvailable, state.negativeContentHash != nil, state.sourceFilename != nil else {
                throw AIEditingFailure("底片不可用，请先在信息面板检查底片版本。")
            }
            item.source = state
            item.phase = .downloading
            return
        }
        guard let sourceState = item.source, let hash = sourceState.negativeContentHash,
              let filename = sourceState.sourceFilename else { throw AIEditingFailure("任务缺少底片信息。") }
        let source = directory.appendingPathComponent("source").appendingPathExtension(URL(fileURLWithPath: filename).pathExtension)
        switch item.phase {
        case .downloading:
            stage(item.id, "等待下载", 0.04, 0.04)
            try await acquire("download", limit: limits.downloads)
            defer { gates["download", default: 0] -= 1 }
            stage(item.id, "下载底片（\(completedCount + 1)/\(totalCount)）", 0.04, 0.15)
            try await client.downloadNegative(assetID: item.assetID, contentHash: hash, to: source)
            let info = try await AIEditingImages.inspect(source, image: false)
            guard info.hash == (sourceState.sourceFileHash ?? hash) else { throw AIEditingFailure("底片下载校验失败，请重新扫描后再试。") }
            item.phase = .grading
        case .grading:
            stage(item.id, "等待 AI 会话", 0.15, 0.15)
            try await acquire("ai", limit: limits.ai)
            defer { gates["ai", default: 0] -= 1 }
            let itemID = item.id
            stage(item.id, "连接 AI，准备观察照片", 0.15, 0.25)
            item.result = try await editor.gradePhoto(source, job: directory.appendingPathComponent("ai", isDirectory: true), progress: { [weak self] title, floor, ceiling in
                self?.stage(itemID, title, floor, ceiling)
            })
            item.phase = item.result?.status == "selected" ? .uploading : .review
        case .uploading:
            stage(item.id, "调色已完成，等待保存", 0.85, 0.85)
            try await acquire("upload", limit: limits.uploads)
            defer { gates["upload", default: 0] -= 1 }
            stage(item.id, "调色已完成，准备保存到 NAS", 0.85, 0.88)
            let state = try await client.editState(assetID: item.assetID)
            if state.lastRequestID == item.id.uuidString.lowercased() { item.phase = .done; return }
            guard state.revision == sourceState.revision else { throw AIEditingFailure("照片已被其它客户端修改，本次结果未覆盖。请取消后重新开始。") }
            guard let result = item.result, let full = result.fullSize, let recipe = result.recipeJSON, let xmp = result.xmp else {
                throw AIEditingFailure("完整调色结果或配方缺失。")
            }
            stage(item.id, "生成展示图和缩略图", 0.87, 0.90)
            let files = try await editor.withRenderSlot {
                try await AIEditingImages.derivatives(from: full, directory: directory)
            }
            let upload = try await client.prepareEditUploads(assetID: item.assetID, requestID: item.id)
            guard Set(upload.objects.map(\.role)) == Set(files.keys), upload.objects.count == 3 else { throw KeepsAPIError.invalidResponse }
            var outputs: [KeepsEditOutput] = []
            for (index, target) in upload.objects.enumerated() {
                let names = ["standard": "全尺寸展示图", "thumbnail": "缩略图", "browse": "浏览预览"]
                stage(item.id, "保存到 NAS：\(names[target.role] ?? target.role)（\(index + 1)/3）", 0.90 + Double(index) * 0.025, 0.925 + Double(index) * 0.025)
                try Task.checkCancellation()
                let file = files[target.role]!
                let info = try await AIEditingImages.inspect(file, image: true)
                try await client.uploadEditImage(target: target, file: file)
                outputs.append(.init(role: target.role, objectRef: target.objectRef, contentHash: info.hash,
                    width: info.width, height: info.height, sizeBytes: info.size))
            }
            try Task.checkCancellation()
            stage(item.id, "NAS 正在校验并保存调色结果", 0.98, 0.995)
            _ = try await client.commitEdit(assetID: item.assetID, edit: .init(requestID: item.id.uuidString.lowercased(),
                expectedRevision: sourceState.revision, negativeContentHash: hash,
                algorithmVersion: "keeps-ai-v1", rendererVersion: "darktable-5.6.2", outputs: outputs,
                recipe: .init(recipeJSON: recipe, xmp: xmp)))
            item.phase = .done
        case .preparing, .done, .review: break
        }
    }

    static func isTransient(_ error: Error) -> Bool {
        if let failure = error as? AIEditingFailure { return failure.retryable }
        if let error = error as? URLError { return error.code != .cancelled }
        if case KeepsAPIError.http(let code, _) = error { return code >= 500 || code == 408 || code == 429 }
        return false
    }
}
