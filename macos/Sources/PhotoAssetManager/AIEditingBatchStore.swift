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
    @Published private(set) var currentName = ""
    @Published private(set) var errorMessage: String?
    @Published private(set) var errorDetails: String?
    @Published private(set) var diagnosticDirectory: URL?
    @Published private(set) var isRunning = false
    var isBlocking: Bool { batch != nil }
    var isAwaitingConfirmation: Bool { batch != nil && batch?.confirmed == false && batch?.cancelled == false }
    var totalCount: Int { batch?.items.count ?? 0 }
    var completedCount: Int { batch?.items.filter(\.terminal).count ?? 0 }
    var isFinished: Bool { batch != nil && !isRunning && (batch!.cancelled || batch!.items.allSatisfy(\.terminal)) }
    private let root: URL
    private var manifest: URL { root.appendingPathComponent("batch.json") }
    private weak var library: LibraryStore?
    private var worker: Task<Void, Never>?
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
            try editor.validateEditingAccount()
            guard let client = library?.client, matches(client) else { throw AIEditingFailure("请使用创建此任务时的资料库连接。") }
            batch!.confirmed = true
            try persist()
            errorMessage = nil; errorDetails = nil; diagnosticDirectory = nil
            isRunning = true
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
            isRunning = false; worker = nil
            if batch?.cancelled == true { status = "已取消；已提交的结果保留" }
        }
        while !Task.isCancelled, batch?.cancelled == false,
              let index = batch?.items.firstIndex(where: { !$0.terminal }) {
            var item = batch!.items[index]
            currentName = item.name
            do {
                try await advance(&item, client: client)
                try save(item, index: index)
                errorMessage = nil; errorDetails = nil; diagnosticDirectory = nil
            } catch {
                if Task.isCancelled || batch?.cancelled == true {
                    status = "已取消；已提交的结果保留。若提交时断开，重新调色前会读取服务器状态。"
                    return
                }
                recordFailure(error, context: status)
                if Self.isTransient(error) {
                    status = "连接暂时不可用，等待重连（\(completedCount)/\(totalCount)）"
                    do { try await Task.sleep(for: retryInterval) } catch { return }
                } else {
                    status = "当前照片未完成，可重试或取消；已完成照片保持保存"
                    return
                }
            }
        }
        if batch?.cancelled == false {
            let review = batch!.items.filter { $0.phase == .review }.count
            status = "已处理 \(totalCount) 张；\(review) 张未自动应用、需要检查"
        }
    }

    private func advance(_ item: inout AIEditingBatch.Item, client: KeepsClient) async throws {
        let directory = root.appendingPathComponent(item.id.uuidString.lowercased(), isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        if item.phase == .preparing {
            status = "读取底片（\(completedCount + 1)/\(totalCount)）"
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
            status = "下载底片（\(completedCount + 1)/\(totalCount)）"
            try await client.downloadNegative(assetID: item.assetID, contentHash: hash, to: source)
            let info = try await AIEditingImages.inspect(source, image: false)
            guard info.hash == (sourceState.sourceFileHash ?? hash) else { throw AIEditingFailure("底片下载校验失败，请重新扫描后再试。") }
            item.phase = .grading
        case .grading:
            status = "AI 调色（\(completedCount + 1)/\(totalCount)）"
            item.result = try await editor.gradePhoto(source, job: directory.appendingPathComponent("ai", isDirectory: true))
            item.phase = item.result?.status == "selected" ? .uploading : .review
        case .uploading:
            status = "保存结果与缩略图（\(completedCount + 1)/\(totalCount)）"
            let state = try await client.editState(assetID: item.assetID)
            if state.lastRequestID == item.id.uuidString.lowercased() { item.phase = .done; return }
            guard state.revision == sourceState.revision else { throw AIEditingFailure("照片已被其它客户端修改，本次结果未覆盖。请取消后重新开始。") }
            guard let result = item.result, let full = result.fullSize, let recipe = result.recipeJSON, let xmp = result.xmp else {
                throw AIEditingFailure("完整调色结果或配方缺失。")
            }
            let files = try await AIEditingImages.derivatives(from: full, directory: directory)
            let upload = try await client.prepareEditUploads(assetID: item.assetID, requestID: item.id)
            guard Set(upload.objects.map(\.role)) == Set(files.keys), upload.objects.count == 3 else { throw KeepsAPIError.invalidResponse }
            var outputs: [KeepsEditOutput] = []
            for target in upload.objects {
                try Task.checkCancellation()
                let file = files[target.role]!
                let info = try await AIEditingImages.inspect(file, image: true)
                try await client.uploadEditImage(target: target, file: file)
                outputs.append(.init(role: target.role, objectRef: target.objectRef, contentHash: info.hash,
                    width: info.width, height: info.height, sizeBytes: info.size))
            }
            try Task.checkCancellation()
            _ = try await client.commitEdit(assetID: item.assetID, edit: .init(requestID: item.id.uuidString.lowercased(),
                expectedRevision: sourceState.revision, negativeContentHash: hash,
                algorithmVersion: "keeps-ai-v1", rendererVersion: "darktable-5.6.2", outputs: outputs,
                recipe: .init(recipeJSON: recipe, xmp: xmp)))
            item.phase = .done
        case .preparing, .done, .review: break
        }
    }

    static func isTransient(_ error: Error) -> Bool {
        if let error = error as? URLError { return error.code != .cancelled }
        if case KeepsAPIError.http(let code, _) = error { return code >= 500 || code == 408 || code == 429 }
        return false
    }
}
