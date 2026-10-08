import Combine
import Foundation
import KeepsAPI

struct AIEditingBatch: Codable {
    struct Candidate: Codable, Identifiable {
        let id: UUID
        let result: AIGradeResult
        let instruction: String
        let parentID: UUID?
        var preferences: String = ""
        var preferenceRevision: Int64 = 0
        var batchInstruction: String = ""
    }
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
        var candidates: [Candidate]?
        var selectedCandidateID: UUID?
        var originalPreview: URL?
        var pendingInstruction: String?
        var pendingCandidateID: UUID?
        var confirmedAt: String?
        var terminal: Bool { phase == .done || phase == .review }
    }
    let id: UUID
    let baseURL: String
    let libraryID: String
    var confirmed = false
    var cancelled = false
    var items: [Item]
    var preferences: String?
    var preferenceRevision: Int64?
    var instruction: String?
    var ownerID: String?
}

@MainActor final class AIEditingBatchStore: ObservableObject {
    let editor: AIEditingSettingsStore
    @Published private(set) var batch: AIEditingBatch?
    @Published private(set) var status = "准备 AI 调色"
    @Published private(set) var errorMessage: String?
    @Published private(set) var errorDetails: String?
    @Published private(set) var diagnosticDirectory: URL?
    @Published private(set) var isRunning = false
    var isBlocking: Bool { false }
    @Published var preferencesText = ""
    @Published var batchInstruction = "" {
        didSet {
            guard batch?.confirmed == false else { return }
            batch?.instruction = batchInstruction
            do { try persist() } catch { recordFailure(error) }
        }
    }
    @Published private(set) var isSyncing = false
    private var preferencesRevision: Int64 = 0
    private var workspaceRevision: Int64 = 0
    private var workspaceLoaded = false
    private var loadedConfiguration: KeepsConfiguration?
    private var ownerID = ""
    private var checkpoint: URL { root.appendingPathComponent("workspace-revision.json") }
    private var syncFailure: Error?
    private var syncTask: Task<Void, Never>?
    private var syncDirty = false
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
            let ownerFile = self.root.appendingPathComponent("owner-id")
            if FileManager.default.fileExists(atPath: ownerFile.path) { ownerID = try String(contentsOf: ownerFile, encoding: .utf8) }
            else { ownerID = UUID().uuidString; try ownerID.write(to: ownerFile, atomically: true, encoding: .utf8) }
            if FileManager.default.fileExists(atPath: manifest.path) {
                batch = try JSONDecoder().decode(AIEditingBatch?.self, from: Data(contentsOf: manifest))
                if let saved = batch {
                    for index in saved.items.indices where saved.items[index].phase == .uploading && saved.items[index].confirmedAt == nil {
                        batch!.items[index].phase = .review
                        if var result = saved.items[index].result {
                            let job = self.root.appendingPathComponent(saved.items[index].id.uuidString.lowercased()).appendingPathComponent("ai")
                            if FileManager.default.fileExists(atPath: job.appendingPathComponent("result.json").path) {
                                do { result = try AIEditingSettingsStore.gradeResult(in: job) }
                                catch {
                                    batch!.items[index].failure = "旧草稿的对比预览不可用，请继续调色生成新版本。"
                                    batch!.items[index].failureDetails = String(reflecting: error)
                                }
                            }
                            result.preview = result.preview ?? result.fullSize
                            batch!.items[index].originalPreview = result.originalPreview
                            batch!.items[index].result = result
                            let candidate = AIEditingBatch.Candidate(id: UUID(), result: result, instruction: "", parentID: nil)
                            batch!.items[index].candidates = [candidate]
                            batch!.items[index].selectedCandidateID = result.status == "selected" ? candidate.id : nil
                        }
                    }
                }
            }
        } catch { storageError = error; recordFailure(error) }
    }

    func prepare(library: LibraryStore) {
        guard batch == nil, !library.isOperationBlocking, !library.isMutating, !library.isSelectingAll,
              !library.isCheckingConnection, !library.isUpdatingHiddenDirectory, !library.isImportingPhotos,
              library.directoryToRename == nil, library.directoryToTrash == nil,
              !editor.isBusy, !library.selectedIDs.isEmpty, let configuration = library.configuration else { return }
        do {
            if let storageError { throw storageError }
            if loadedConfiguration != configuration { workspaceLoaded = false; workspaceRevision = 0 }
            let selected = library.selectedIDs
            let names = Dictionary(uniqueKeysWithValues: library.assets.map { ($0.id, $0.originalFilename) })
            batch = AIEditingBatch(id: UUID(), baseURL: configuration.baseURL.absoluteString, libraryID: configuration.libraryID,
                items: selected.sorted { $0.uuidString < $1.uuidString }.map { .init(id: UUID(), assetID: $0, name: names[$0] ?? $0.uuidString) })
            batch!.ownerID = ownerID
            self.library = library
            try persist()

            errorMessage = nil; errorDetails = nil; diagnosticDirectory = nil
            status = "确认对所选 \(totalCount) 张照片进行 AI 调色"
        } catch { batch = nil; library.lastError = String(reflecting: error) }
    }

    func restore(library: LibraryStore) {
        guard !restored else { return }
        restored = true
        self.library = library
        guard let batch else { return }

        preferencesText = batch.preferences ?? ""
        batchInstruction = batch.instruction ?? ""
        if batch.confirmed && !batch.cancelled && !isFinished { start() }
    }

    func start() {
        guard worker == nil, !editor.isBusy, batch != nil,
              batch!.items.contains(where: { !$0.terminal }) else { return }
        isRunning = true
        worker = Task {
            do {
                guard let library, let client = library.client, matches(client) else { throw AIEditingFailure("请使用创建此任务时的资料库连接。") }
                if !workspaceLoaded { await loadWorkspace(library: library) }
                guard workspaceLoaded, batch?.ownerID == nil || batch?.ownerID == ownerID else {
                    throw AIEditingFailure("工作台尚未同步，或由另一台 Mac 创建。请在原 Mac 继续。")
                }
                if !batch!.confirmed {
                    let saved = try await client.saveAIPreferences(text: preferencesText, expectedRevision: preferencesRevision)
                    preferencesRevision = saved.revision
                    batch!.preferences = saved.text
                    batch!.preferenceRevision = saved.revision
                    batch!.instruction = batchInstruction
                }
                for index in batch!.items.indices { batch!.items[index].failure = nil; batch!.items[index].failureDetails = nil }
                batch!.confirmed = true; batch!.cancelled = false; batch!.ownerID = ownerID
                try persist()
                try await awaitWorkspaceSync()
                errorMessage = nil; errorDetails = nil; diagnosticDirectory = nil
                limits = editor.limits.bounded
                editor.batchActive = true
                elapsedSeconds = 0
                progressTicker = Task { [weak self = self] in
                    while !Task.isCancelled {
                        do { try await Task.sleep(for: .seconds(1)) } catch { return }
                        self?.tickProgress()
                    }
                }
                try Task.checkCancellation()
                await run(client: client)
            } catch {
                isRunning = false; worker = nil; editor.batchActive = false
                progressTicker?.cancel(); progressTicker = nil
                recordFailure(error); status = "任务已暂停，请处理后重试"
            }
        }
    }

    func loadWorkspace(library: LibraryStore) async {
        self.library = library
        if loadedConfiguration != library.configuration { workspaceLoaded = false }
        guard !isSyncing, let client = library.client else { return }
        if batch != nil, !matches(client) { recordFailure(AIEditingFailure("请切回创建工作台的资料库。")); return }
        isSyncing = true
        defer { isSyncing = false }
        do {
            if workspaceLoaded {
                scheduleSync()
                try await awaitWorkspaceSync()
                errorMessage = nil
                return
            }
            let prefs = try await client.aiPreferences()
            let remote = try await client.aiWorkspace()
            guard library.client?.configuration == client.configuration else { return }
            preferencesText = prefs.text; preferencesRevision = prefs.revision
            if let document = remote.document {
                let saved = try JSONDecoder().decode(AIEditingBatch.self, from: Data(document.utf8))
                if let batch, batch.id != saved.id { throw AIEditingFailure("NAS 上存在另一份工作台草稿；本机草稿已保留，请先完成当前工作台。") }
                if let local = batch {
                    let known = (try? Data(contentsOf: checkpoint)).flatMap { try? JSONDecoder().decode(Int64.self, from: $0) }
                    let encoder = JSONEncoder(); encoder.outputFormatting = [.sortedKeys]
                    let same = try encoder.encode(local) == encoder.encode(saved)
                    if local.confirmed && known != remote.revision && !same {
                        throw AIEditingFailure("NAS 草稿版本已改变，本机进度已保留，不能覆盖远端草稿。")
                    }
                } else { batch = saved; batchInstruction = saved.instruction ?? "" }
                guard saved.ownerID == nil || saved.ownerID == ownerID else {
                    throw AIEditingFailure("此工作台由另一台 Mac 创建，调色缓存保存在原 Mac，请在原 Mac 继续。")
                }
            }
            workspaceRevision = remote.revision
            try JSONEncoder().encode(workspaceRevision).write(to: checkpoint, options: .atomic)
            loadedConfiguration = client.configuration
            workspaceLoaded = true
            try persist()
            if batch?.confirmed == true, batch?.cancelled == false, worker == nil,
               batch?.items.contains(where: { !$0.terminal }) == true { start() }
        } catch { recordFailure(error) }
    }

    func reloadRemoteWorkspace() async {
        guard !isRunning, !isSyncing, let library else { return }
        do {
            if let syncTask { await syncTask.value }
            if let batch {
                let copy = root.appendingPathComponent("recovery-" + UUID().uuidString + ".json")
                try JSONEncoder().encode(batch).write(to: copy, options: .atomic)
            }
            batch = nil; workspaceLoaded = false; syncDirty = false; syncFailure = nil
            try JSONEncoder().encode(batch).write(to: manifest, options: .atomic)
            await loadWorkspace(library: library)
        } catch { recordFailure(error) }
    }

    func savePreferences() async {
        guard !isSyncing, let client = library?.client, workspaceLoaded, client.configuration == loadedConfiguration else { return }
        isSyncing = true
        defer { isSyncing = false }
        do {
            let saved = try await client.saveAIPreferences(text: preferencesText, expectedRevision: preferencesRevision)
            preferencesRevision = saved.revision
            preferencesText = saved.text
            status = "长期审美偏好已保存；当前批次继续使用开始时的偏好"
        } catch { recordFailure(error) }
    }

    private func scheduleSync() {
        guard workspaceLoaded else { return }
        syncDirty = true
        guard syncTask == nil else { return }
        syncTask = Task { [weak self] in
            guard let self else { return }
            self.syncFailure = nil
            defer { self.syncTask = nil }
            do { try await self.flushWorkspace(); self.syncFailure = nil }
            catch { self.syncFailure = error; self.recordFailure(error) }
        }
    }

    private func flushWorkspace() async throws {
        guard let client = library?.client, workspaceLoaded, client.configuration == loadedConfiguration,
              batch == nil || matches(client) else { throw AIEditingFailure("资料库连接已变化，草稿保留在本机，请切回原连接后同步。") }
        while syncDirty {
            syncDirty = false
            let encoder = JSONEncoder(); encoder.outputFormatting = [.sortedKeys]
            let document = try batch.map { String(decoding: try encoder.encode($0), as: UTF8.self) }
            do {
                let saved = try await client.saveAIWorkspace(document: document, expectedRevision: workspaceRevision)
                workspaceRevision = saved.revision
                try JSONEncoder().encode(workspaceRevision).write(to: checkpoint, options: .atomic)
            } catch { syncDirty = true; throw error }
        }
    }

    private func awaitWorkspaceSync() async throws {
        if let syncTask { await syncTask.value }
        if let syncFailure { throw syncFailure }
        if syncDirty { scheduleSync(); if let syncTask { await syncTask.value } }
        if let syncFailure { throw syncFailure }
    }

    func selectCandidate(itemID: UUID, candidateID: UUID?) {
        guard let index = batch?.items.firstIndex(where: { $0.id == itemID }),
              batch!.items[index].phase == .review else { return }
        if let candidateID, !(batch!.items[index].candidates ?? []).contains(where: { $0.id == candidateID && $0.result.status == "selected" }) { return }
        batch!.items[index].failure = nil; batch!.items[index].failureDetails = nil
        batch!.items[index].selectedCandidateID = candidateID
        batch!.items[index].result = batch!.items[index].candidates?.first { $0.id == candidateID }?.result
        do { try persist() } catch { recordFailure(error) }
    }

    func refine(itemID: UUID, instruction: String) {
        let instruction = instruction.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !isRunning, !instruction.isEmpty, let index = batch?.items.firstIndex(where: { $0.id == itemID }),
              batch!.items[index].phase == .review else { return }
        batch!.items[index].pendingInstruction = instruction
        batch!.items[index].pendingCandidateID = UUID()
        batch!.items[index].phase = .grading
        batch!.items[index].failure = nil
        batch!.cancelled = false
        do { try persist(); start() } catch { recordFailure(error) }
    }

    func canPublish(_ item: AIEditingBatch.Item) -> Bool {
        guard item.phase == .review else { return false }
        guard let selected = item.selectedCandidateID else { return true }
        guard let candidate = item.candidates?.first(where: { $0.id == selected }),
              let before = item.originalPreview ?? candidate.result.originalPreview,
              let after = candidate.result.preview ?? candidate.result.fullSize else { return false }
        return FileManager.default.isReadableFile(atPath: before.path) && FileManager.default.isReadableFile(atPath: after.path)
    }

    func publish(itemID: UUID) {
        guard !isRunning, workspaceLoaded, batch?.ownerID == nil || batch?.ownerID == ownerID else { return }
        do { try confirm(itemID: itemID); start() } catch { recordFailure(error) }
    }

    func publishReady() {
        guard !isRunning, workspaceLoaded, batch?.ownerID == nil || batch?.ownerID == ownerID else { return }
        do {
            for item in batch?.items ?? [] where canPublish(item) && item.failure == nil { try confirm(itemID: item.id) }
            start()
        } catch { recordFailure(error) }
    }

    private func confirm(itemID: UUID) throws {
        guard let index = batch?.items.firstIndex(where: { $0.id == itemID }), batch!.items[index].phase == .review else { return }
        guard canPublish(batch!.items[index]) else { throw AIEditingFailure("对比预览不完整，请继续调色生成新版本后确认。") }
        let previous = batch
        batch!.items[index].confirmedAt = ISO8601DateFormatter().string(from: Date())
        batch!.items[index].phase = .uploading
        batch!.items[index].failure = nil
        batch!.cancelled = false
        do { try persist() } catch { batch = previous; throw error }
    }

    func archiveAndDismiss() {
        guard !isRunning, let batch else { return }
        do {
            let copy = root.appendingPathComponent("recovery-" + batch.id.uuidString + ".json")
            try JSONEncoder().encode(batch).write(to: copy, options: .atomic)
            dismiss()
        } catch { recordFailure(error) }
    }

    func closeCompletedWorkspace() {
        guard !isRunning, batch?.items.allSatisfy({ $0.phase == .done }) == true else { return }
        dismiss()
    }

    private func editMetadata(_ item: AIEditingBatch.Item) throws -> String {
        let candidate = item.candidates?.first { $0.id == item.selectedCandidateID }
        let object: [String: Any] = ["reason": candidate?.result.reason ?? item.candidates?.last?.result.reason ?? "保留调整前版本",
            "instruction": candidate?.instruction ?? "", "preferences": candidate?.preferences ?? batch?.preferences ?? "",
            "preferenceRevision": candidate?.preferenceRevision ?? batch?.preferenceRevision ?? 0,
            "batchInstruction": candidate?.batchInstruction ?? batch?.instruction ?? "",
            "selection": item.selectedCandidateID?.uuidString ?? "original", "confirmedAt": item.confirmedAt ?? "",
            "history": (item.candidates ?? []).map { ["id": $0.id.uuidString, "reason": $0.result.reason,
                "instruction": $0.instruction, "parentID": $0.parentID?.uuidString ?? "", "status": $0.result.status, "recipeJSON": $0.result.recipeJSON ?? ""] }]
        return String(decoding: try JSONSerialization.data(withJSONObject: object, options: [.sortedKeys]), as: UTF8.self)
    }

    func retry() {
        if batch?.cancelled == true { batch!.cancelled = false }
        if let items = batch?.items {
            for index in items.indices where items[index].failure != nil && items[index].phase == .review && items[index].pendingCandidateID != nil {
                batch!.items[index].phase = .grading
            }
        }
        syncFailure = nil
        do { try persist() } catch { recordFailure(error); return }
        start()
    }

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

    private func persist() throws {
        try JSONEncoder().encode(batch).write(to: manifest, options: .atomic)
        scheduleSync()
    }

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
        do { try await awaitWorkspaceSync() } catch { recordFailure(error) }
        library?.refresh(force: true)
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
                    if item.phase == .grading, !(item.candidates ?? []).isEmpty {
                        item.phase = .review
                        item.result = item.candidates?.first { $0.id == item.selectedCandidateID }?.result
                    }
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
            if state.hasEdit {
                let descriptor = try await client.refreshPreviewDescriptor(assetID: item.assetID, role: .standard)
                let (data, response) = try await URLSession.shared.data(from: descriptor.downloadURL)
                guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else { throw KeepsAPIError.invalidResponse }
                let preview = directory.appendingPathComponent("before.heic")
                try data.write(to: preview, options: .atomic)
                item.originalPreview = preview
            }
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
            if !FileManager.default.fileExists(atPath: source.path) { item.phase = .downloading; return }
            stage(item.id, "等待 AI 会话", 0.15, 0.15)
            try await acquire("ai", limit: limits.ai)
            defer { gates["ai", default: 0] -= 1 }
            let itemID = item.id
            stage(item.id, "连接 AI，准备观察照片", 0.15, 0.25)
            let selected = item.candidates?.first { $0.id == item.selectedCandidateID }
            let context = AIGradeContext(preferences: batch?.preferences ?? "", batchInstruction: batch?.instruction ?? "",
                instruction: item.pendingInstruction ?? "", baseRecipeJSON: selected?.result.recipeJSON ?? item.source?.recipe?.recipeJSON,
                history: (item.candidates ?? []).map { $0.instruction + "\n" + $0.result.reason })
            let jobName = item.pendingCandidateID.map { "ai-" + $0.uuidString.lowercased() } ?? "ai"
            item.result = try await editor.gradePhoto(source, job: directory.appendingPathComponent(jobName, isDirectory: true), context: context, progress: { [weak self] title, floor, ceiling in
                self?.stage(itemID, title, floor, ceiling)
            })
                        let candidate = AIEditingBatch.Candidate(id: item.pendingCandidateID ?? UUID(), result: item.result!,
                instruction: item.pendingInstruction ?? "", parentID: item.selectedCandidateID,
                preferences: batch?.preferences ?? "", preferenceRevision: batch?.preferenceRevision ?? 0,
                batchInstruction: batch?.instruction ?? "")
            item.candidates = (item.candidates ?? []) + [candidate]
            if item.result?.status == "selected" { item.selectedCandidateID = candidate.id }
            if item.originalPreview == nil { item.originalPreview = item.result?.originalPreview }
            item.result = item.candidates?.first { $0.id == item.selectedCandidateID }?.result
            item.pendingInstruction = nil; item.pendingCandidateID = nil
            item.phase = .review
        case .uploading:
            guard item.confirmedAt != nil else { item.phase = .review; return }
            try await awaitWorkspaceSync()
            stage(item.id, "调色已完成，等待保存", 0.85, 0.85)
            try await acquire("upload", limit: limits.uploads)
            defer { gates["upload", default: 0] -= 1 }
            stage(item.id, "调色已完成，准备保存到 NAS", 0.85, 0.88)
            let state = try await client.editState(assetID: item.assetID)
            if state.lastRequestID == item.id.uuidString.lowercased() { item.phase = .done; return }
            guard state.revision == sourceState.revision else { throw AIEditingFailure("照片已被其它客户端修改，本次结果未覆盖。请取消后重新开始。") }
            if item.selectedCandidateID == nil {
                _ = try await client.confirmOriginalEdit(assetID: item.assetID, requestID: item.id,
                    expectedRevision: sourceState.revision, metadata: try editMetadata(item))
                item.phase = .done
                return
            }
            guard let result = item.result, let full = result.fullSize, let recipe = result.recipeJSON, let xmp = result.xmp else {
                throw AIEditingFailure("完整调色结果或配方缺失。")
            }
            guard FileManager.default.fileExists(atPath: full.path) else {
                item.phase = .review
                throw AIEditingFailure("本机完整调色图已丢失；配方和意见仍在。请继续调色重新生成一版，再确认发布。")
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
                recipe: .init(recipeJSON: recipe, xmp: xmp, metadata: try editMetadata(item))))
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
