import Foundation
import Testing
@testable import PhotoAssetManager

struct CaptureTimeBackfillTests {
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
}
