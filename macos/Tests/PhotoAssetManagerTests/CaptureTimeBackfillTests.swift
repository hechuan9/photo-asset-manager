import Foundation
import Testing
@testable import PhotoAssetManager

struct CaptureTimeBackfillTests {
    @Test func scannerStoresFileCreationDateForNewImportedPhotoWhenMetadataHasNoCaptureTime() async throws {
        let root = FileManager.default.temporaryDirectory
            .resolvingSymlinksInPath()
            .appendingPathComponent("PhotoAssetManagerTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer {
            try? FileManager.default.removeItem(at: root)
        }

        let photo = root.appendingPathComponent("new-import.jpg")
        try Data("not-an-image".utf8).write(to: photo)
        let fileCreationDate = Date(timeIntervalSince1970: 1_704_247_200)
        try FileManager.default.setAttributes(
            [
                .creationDate: fileCreationDate,
                .modificationDate: fileCreationDate
            ],
            ofItemAtPath: photo.path
        )

        let database = try SQLiteDatabase(path: root.appendingPathComponent("Library.sqlite"))
        let report = await PhotoScanner().scanDirectory(root, storageKind: .local, derivativeRoot: nil, database: database) { _ in }
        let assets = try database.queryAssets(filter: LibraryFilter(sortOrder: .captureTimeAscending), limit: 10)

        #expect(report.errors.isEmpty)
        #expect(report.importedAssets == 1)
        #expect(assets.count == 1)
        #expect(abs((assets.first?.captureTime?.timeIntervalSince1970 ?? 0) - fileCreationDate.timeIntervalSince1970) < 1)
    }

    @Test func rescanningExistingAssetAtNewLocationBackfillsMissingCaptureTime() throws {
        let root = FileManager.default.temporaryDirectory
            .resolvingSymlinksInPath()
            .appendingPathComponent("PhotoAssetManagerTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer {
            try? FileManager.default.removeItem(at: root)
        }

        let firstPath = root.appendingPathComponent("first.jpg")
        let secondPath = root.appendingPathComponent("second.jpg")
        let captureTime = Date(timeIntervalSince1970: 1_704_333_600)
        let database = try SQLiteDatabase(path: root.appendingPathComponent("Library.sqlite"))
        let batchID = try database.createImportBatch(sourcePath: root.path, deviceID: "test")

        let firstScan = ScannedFile(
            url: firstPath,
            deviceID: "test",
            storageKind: .local,
            fileRole: .jpegOriginal,
            authorityRole: .workingCopy,
            syncStatus: .needsArchive,
            sizeBytes: 1,
            contentHash: "same-content",
            metadataFingerprint: "same-metadata",
            captureTime: nil,
            cameraMake: "",
            cameraModel: "",
            lensModel: "",
            rating: 0,
            sidecars: []
        )
        let secondScan = ScannedFile(
            url: secondPath,
            deviceID: "test",
            storageKind: .local,
            fileRole: .jpegOriginal,
            authorityRole: .workingCopy,
            syncStatus: .needsArchive,
            sizeBytes: 1,
            contentHash: "same-content",
            metadataFingerprint: "same-metadata",
            captureTime: captureTime,
            cameraMake: "",
            cameraModel: "",
            lensModel: "",
            rating: 0,
            sidecars: []
        )

        _ = try database.upsertScannedFile(firstScan, batchID: batchID)
        _ = try database.upsertScannedFile(secondScan, batchID: batchID)
        let assets = try database.queryAssets(filter: LibraryFilter(), limit: 10)

        #expect(assets.count == 1)
        #expect(assets.first?.captureTime == captureTime)
    }

    @Test func scannerBackfillsMissingCaptureTimesFromFileCreationDateWithoutOverwritingExistingCaptureTimes() async throws {
        let root = FileManager.default.temporaryDirectory
            .resolvingSymlinksInPath()
            .appendingPathComponent("PhotoAssetManagerTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer {
            try? FileManager.default.removeItem(at: root)
        }

        let database = try SQLiteDatabase(path: root.appendingPathComponent("Library.sqlite"))
        let photo = root.appendingPathComponent("missing.jpg")
        try Data("not-an-image".utf8).write(to: photo)
        let fileCreationDate = Date(timeIntervalSince1970: 1_704_160_800)
        try FileManager.default.setAttributes(
            [
                .creationDate: fileCreationDate,
                .modificationDate: fileCreationDate
            ],
            ofItemAtPath: photo.path
        )
        let scannedPhoto = try #require(
            FileManager.default
                .enumerator(at: root, includingPropertiesForKeys: [.fileSizeKey])?
                .compactMap { $0 as? URL }
                .first { $0.lastPathComponent == "missing.jpg" }
        )
        let size = Int64((try scannedPhoto.resourceValues(forKeys: [.fileSizeKey]).fileSize) ?? 0)
        let missingID = UUID().uuidString
        let existingID = UUID().uuidString
        let existingCaptureTime = "2023-05-06T07:08:09.000Z"
        let existingCreatedAt = "2024-02-03T04:05:06.000Z"

        try database.execute(
            """
            INSERT INTO assets (
                id, capture_time, camera_make, camera_model, lens_model, original_filename,
                content_fingerprint, metadata_fingerprint, rating, flag, color_label, tags, created_at, updated_at
            ) VALUES
                ('\(missingID)', NULL, '', '', '', 'missing.jpg', 'hash-missing', 'meta-missing', 0, 0, NULL, '[]', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z'),
                ('\(existingID)', '\(existingCaptureTime)', '', '', '', 'existing.jpg', 'hash-existing', 'meta-existing', 0, 0, NULL, '[]', '\(existingCreatedAt)', '\(existingCreatedAt)')
            """
        )
        try database.execute(
            """
            INSERT INTO file_instances (
                id, asset_id, path, device_id, storage_kind, file_role, authority_role,
                sync_status, size_bytes, content_hash, last_seen_at, availability
            ) VALUES (
                '\(UUID().uuidString)', '\(missingID)', '\(scannedPhoto.path)', 'test', 'local', 'jpeg_original', 'working_copy',
                'needs_archive', \(size), 'hash-missing', '2026-01-01T00:00:00.000Z', 'online'
            )
            """
        )

        let report = await PhotoScanner().scanDirectory(root, storageKind: .local, derivativeRoot: nil, database: database) { _ in }
        let assets = try database.queryAssets(filter: LibraryFilter(sortOrder: .captureTimeAscending), limit: 10)

        #expect(report.errors.isEmpty)
        #expect(report.scannedFiles == 1)
        #expect(report.skippedExistingFiles == 1)
        #expect(abs((assets.first(where: { $0.id.uuidString == missingID })?.captureTime?.timeIntervalSince1970 ?? 0) - fileCreationDate.timeIntervalSince1970) < 1)
        #expect(assets.first(where: { $0.id.uuidString == existingID })?.captureTime == DateCoding.decode(existingCaptureTime))
    }

    @Test func migrateDoesNotResurrectUserRemovedSourceDirectorySeededFromImportBatches() throws {
        // TDD RED: 模拟用户通过 import 历史拥有来源 P，启动时 seed 添加 source，
        // 用户执行 removeSourceDirectory 删除之（"删除文件夹"），
        // 重启（新 SQLiteDatabase 触发 migrate）后不应复活。
        // 此测试当前必须失败，证明根因；修复 seed guard 后变绿。
        let root = FileManager.default.temporaryDirectory
            .resolvingSymlinksInPath()
            .appendingPathComponent("PhotoAssetManagerTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer {
            try? FileManager.default.removeItem(at: root)
        }

        let dbPath = root.appendingPathComponent("Library.sqlite")
        let removedPath = "/tmp/user-removed-folder-for-test-\(UUID().uuidString)"
        let ibID = UUID().uuidString  // import_batches.id 必须是 UUID 字符串格式，否则 sourceDirectories 加载时 id 回退随机 UUID，导致 remove 按错 id 无法命中

        // 第一次打开：建库 + migrate（此时无 ib）。注意：migrate 可能已消费 one-time seed 标记，
        // 为模拟“ib 在考虑播种时已存在”的真实场景，手动清除标记后再插入 ib。
        var db: SQLiteDatabase? = try SQLiteDatabase(path: dbPath)
        try db!.execute("DELETE FROM app_settings WHERE key = 'import_batch_sources_seeded'")

        // 插入历史 import_batch（模拟旧库的 import 记录，使用合法 UUID id）
        try db!.execute(
            """
            INSERT INTO import_batches (id, source_path, device_id, imported_at, imported_by, status)
            VALUES ('\(ibID)', '\(removedPath)', 'test-dev', '2024-01-01T00:00:00Z', 'tester', 'finished')
            """
        )

        // 关闭连接，重新打开以触发带 ib 的 migrate -> 此时 flag 已清，应执行 seed 添加 source
        db = nil
        var db2: SQLiteDatabase? = try SQLiteDatabase(path: dbPath)
        var sources = try db2!.sourceDirectories()
        let seeded = sources.first { $0.path == removedPath }
        #expect(seeded != nil, "测试设置：带 ib 的 migrate 应该播种 source（当前行为）")

        // 用户"删除文件夹"：仅移除追踪记录
        if let s = seeded {
            try db2!.removeSourceDirectory(id: s.id)
        }
        sources = try db2!.sourceDirectories()
        #expect(!sources.contains { $0.path == removedPath }, "remove 后当前会话应无该 source")

        // 模拟重启：释放连接，新建连接触发 migrate
        db2 = nil
        let db3 = try SQLiteDatabase(path: dbPath)
        let sourcesAfterRestart = try db3.sourceDirectories()

        // 关键断言：不应被 import_batches 复活
        #expect(
            !sourcesAfterRestart.contains { $0.path == removedPath },
            "移除 source 后重启（migrate 重跑）不应从 import_batches 复活该文件夹记录"
        )
    }
}
