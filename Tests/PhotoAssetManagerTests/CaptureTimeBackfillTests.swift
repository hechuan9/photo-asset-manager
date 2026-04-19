import Foundation
import Testing
@testable import PhotoAssetManager

struct CaptureTimeBackfillTests {
    @Test func backfillMissingCaptureTimesCopiesCreatedAtWithoutOverwritingExistingCaptureTimes() throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("PhotoAssetManagerTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer {
            try? FileManager.default.removeItem(at: root)
        }

        let database = try SQLiteDatabase(path: root.appendingPathComponent("Library.sqlite"))
        let missingID = UUID().uuidString
        let existingID = UUID().uuidString
        let missingCreatedAt = "2024-01-02T03:04:05.000Z"
        let existingCaptureTime = "2023-05-06T07:08:09.000Z"
        let existingCreatedAt = "2024-02-03T04:05:06.000Z"

        try database.execute(
            """
            INSERT INTO assets (
                id, capture_time, camera_make, camera_model, lens_model, original_filename,
                content_fingerprint, metadata_fingerprint, rating, flag, color_label, tags, created_at, updated_at
            ) VALUES
                ('\(missingID)', NULL, '', '', '', 'missing.jpg', 'hash-missing', 'meta-missing', 0, 0, NULL, '[]', '\(missingCreatedAt)', '\(missingCreatedAt)'),
                ('\(existingID)', '\(existingCaptureTime)', '', '', '', 'existing.jpg', 'hash-existing', 'meta-existing', 0, 0, NULL, '[]', '\(existingCreatedAt)', '\(existingCreatedAt)')
            """
        )

        let updatedCount = try database.backfillMissingCaptureTimesFromCreatedAt()
        let assets = try database.queryAssets(filter: LibraryFilter(sortOrder: .captureTimeAscending), limit: 10)

        #expect(updatedCount == 1)
        #expect(assets.first(where: { $0.id.uuidString == missingID })?.captureTime == DateCoding.decode(missingCreatedAt))
        #expect(assets.first(where: { $0.id.uuidString == existingID })?.captureTime == DateCoding.decode(existingCaptureTime))
    }
}
