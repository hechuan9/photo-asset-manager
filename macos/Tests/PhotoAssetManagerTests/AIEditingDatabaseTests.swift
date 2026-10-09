import Foundation
import SQLite3
import Testing
@testable import PhotoAssetManager

@MainActor struct AIEditingDatabaseTests {
    @Test func restartRestoresOrderPhasesCandidatesAndPreferences() throws {
        let root = temporaryRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let candidate = AIEditingBatch.Candidate(id: UUID(), result: .init(status: "selected", reason: "保留肤色", recipeJSON: "recipe"), instruction: "暖一点", parentID: nil)
        let first = AIEditingBatch.Item(id: UUID(), assetID: UUID(), name: "first.arw", phase: .review, candidates: [candidate], selectedCandidateID: candidate.id)
        let second = AIEditingBatch.Item(id: UUID(), assetID: UUID(), name: "second.arw", phase: .downloading)
        let batch = AIEditingBatch(id: UUID(), baseURL: "https://nas.invalid", libraryID: "local", items: [second, first])
        do {
            let database = try AIEditingDatabase(root: root)
            #expect(try database.load() == nil)
            try database.save(batch: batch, preferences: .init(text: "自然", revision: 7))
        }
        let restored = try #require(try AIEditingDatabase(root: root).load())
        #expect(restored.batch?.items.map(\.id) == [second.id, first.id])
        #expect(restored.batch?.items.map(\.phase) == [.downloading, .review])
        #expect(restored.batch?.items[1].candidates?.first?.id == candidate.id)
        #expect(restored.batch?.items[1].candidates?.first?.result.recipeJSON == "recipe")
        #expect(restored.batch?.items[1].selectedCandidateID == candidate.id)
        #expect(restored.preferences.text == "自然")
        #expect(restored.preferences.revision == 7)
        try sql(root, "UPDATE items SET phase='grading',position=2 WHERE id='\(second.id.uuidString)'")
        let sqlRestored = try #require(try AIEditingDatabase(root: root).load())
        #expect(sqlRestored.batch?.items.map(\.id) == [first.id, second.id])
        #expect(sqlRestored.batch?.items.last?.phase == .grading)
    }

    @Test func itemCheckpointIsAtomicAndDoesNotChangeOtherItems() throws {
        let root = temporaryRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let database = try AIEditingDatabase(root: root)
        var first = AIEditingBatch.Item(id: UUID(), assetID: UUID(), name: "first", phase: .grading)
        let second = AIEditingBatch.Item(id: UUID(), assetID: UUID(), name: "second", phase: .downloading)
        let batch = AIEditingBatch(id: UUID(), baseURL: "https://nas.invalid", libraryID: "local", items: [first, second])
        try database.save(batch: batch, preferences: .init(text: "natural", revision: 1))
        let candidate = AIEditingBatch.Candidate(id: UUID(), result: .init(status: "selected", reason: "done", recipeJSON: "recipe"), instruction: "", parentID: nil)
        first.phase = .review; first.candidates = [candidate]; first.result = candidate.result
        try sql(root, "CREATE TRIGGER fail_candidate BEFORE INSERT ON candidates BEGIN SELECT RAISE(ABORT,'candidate checkpoint failure'); END")
        #expect(throws: (any Error).self) { try database.save(item: first, position: 0) }
        var saved = try #require(try database.load())
        #expect(saved.batch?.items[0].phase == .grading)
        #expect(saved.batch?.items[0].result == nil)
        #expect(saved.batch?.items[0].candidates?.isEmpty == true)
        #expect(saved.batch?.items[1].phase == .downloading)
        try sql(root, "DROP TRIGGER fail_candidate")
        try database.save(item: first, position: 0)
        saved = try #require(try database.load())
        #expect(saved.batch?.items[0].phase == .review)
        #expect(saved.batch?.items[0].candidates?.first?.id == candidate.id)
        #expect(saved.batch?.items[1].id == second.id)
        #expect(saved.batch?.items[1].phase == .downloading)
        #expect(saved.preferences.revision == 1)
    }

    @Test func clearingWorkspaceDoesNotReviveItemsOrRemoveCandidateFiles() throws {
        let root = temporaryRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let database = try AIEditingDatabase(root: root)
        let preview = root.appendingPathComponent("candidate.jpg")
        try Data("candidate file".utf8).write(to: preview)
        let item = AIEditingBatch.Item(id: UUID(), assetID: UUID(), name: "photo")
        let batch = AIEditingBatch(id: UUID(), baseURL: "https://nas.invalid", libraryID: "local", items: [item])
        try database.save(batch: batch, preferences: .init(text: "saved", revision: 2))
        try database.save(batch: nil, preferences: .init(text: "saved", revision: 2))
        #expect(throws: (any Error).self) { try database.save(item: item, position: 0) }
        let restored = try #require(try AIEditingDatabase(root: root).load())
        #expect(restored.batch == nil)
        #expect(restored.preferences.text == "saved")
        #expect(FileManager.default.fileExists(atPath: preview.path))
    }

    @Test func wholeSaveRollbackPreservesPreferencesAndWorkspace() throws {
        let root = temporaryRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let database = try AIEditingDatabase(root: root)
        let item = AIEditingBatch.Item(id: UUID(), assetID: UUID(), name: "photo")
        let batch = AIEditingBatch(id: UUID(), baseURL: "https://nas.invalid", libraryID: "local", items: [item])
        try database.save(batch: batch, preferences: .init(text: "before", revision: 1))
        try sql(root, "CREATE TRIGGER fail_delete BEFORE DELETE ON items BEGIN SELECT RAISE(ABORT,'delete failure'); END")
        #expect(throws: (any Error).self) { try database.save(batch: nil, preferences: .init(text: "after", revision: 2)) }
        let saved = try #require(try database.load())
        #expect(saved.preferences.text == "before")
        #expect(saved.batch?.items.first?.id == item.id)
    }

    @Test func rejectsFutureDatabaseVersion() throws {
        let root = temporaryRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        do { _ = try AIEditingDatabase(root: root) }
        try sql(root, "PRAGMA user_version=2")
        #expect(throws: (any Error).self) { _ = try AIEditingDatabase(root: root) }
    }

    private func temporaryRoot() -> URL { FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString) }

    private func sql(_ root: URL, _ statement: String) throws {
        var handle: OpaquePointer?
        guard sqlite3_open(root.appendingPathComponent("workspace.sqlite").path, &handle) == SQLITE_OK else {
            sqlite3_close(handle)
            throw AIEditingFailure("测试数据库打开失败")
        }
        defer { sqlite3_close(handle) }
        let code = sqlite3_exec(handle, statement, nil, nil, nil)
        guard code == SQLITE_OK else { throw AIEditingFailure(String(cString: sqlite3_errmsg(handle))) }
    }
}
