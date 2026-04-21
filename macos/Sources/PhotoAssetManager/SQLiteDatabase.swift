import Foundation
#if os(iOS)
import UIKit
#endif
import SQLite3

enum DatabaseError: LocalizedError {
    case openFailed(String)
    case prepareFailed(String)
    case stepFailed(String)
    case bindFailed(String)

    var errorDescription: String? {
        switch self {
        case .openFailed(let message): "数据库打开失败：\(message)"
        case .prepareFailed(let message): "SQL 准备失败：\(message)"
        case .stepFailed(let message): "SQL 执行失败：\(message)"
        case .bindFailed(let message): "SQL 绑定失败：\(message)"
        }
    }
}

final class SQLiteDatabase: @unchecked Sendable {
    private var db: OpaquePointer?

    init(path: URL, migrateSchema: Bool = true, readOnly: Bool = false) throws {
        let directory = path.deletingLastPathComponent()
        if !readOnly {
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        }
        let flags = readOnly
            ? SQLITE_OPEN_READONLY | SQLITE_OPEN_FULLMUTEX
            : SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX
        if sqlite3_open_v2(path.path, &db, flags, nil) != SQLITE_OK {
            throw DatabaseError.openFailed(lastError)
        }
        try execute("PRAGMA foreign_keys = ON")
        if !readOnly {
            try execute("PRAGMA journal_mode = WAL")
        }
        if migrateSchema {
            try migrate()
        }
    }

    deinit {
        sqlite3_close(db)
    }

    var lastError: String {
        guard let db else { return "数据库句柄不可用" }
        return String(cString: sqlite3_errmsg(db))
    }

    func execute(_ sql: String) throws {
        var error: UnsafeMutablePointer<CChar>?
        if sqlite3_exec(db, sql, nil, nil, &error) != SQLITE_OK {
            let message = error.map { String(cString: $0) } ?? lastError
            sqlite3_free(error)
            throw DatabaseError.stepFailed(message)
        }
    }

    func transaction<T>(_ body: () throws -> T) throws -> T {
        try execute("BEGIN IMMEDIATE TRANSACTION")
        do {
            let result = try body()
            try execute("COMMIT")
            return result
        } catch {
            try? execute("ROLLBACK")
            throw error
        }
    }

    func queryAssets(filter: LibraryFilter, limit: Int, offset: Int = 0, includeTrashed: Bool = false) throws -> [Asset] {
        guard limit > 0 else { return [] }
        guard offset >= 0 else { return [] }

        var conditions: [String] = []
        var values: [SQLiteValue] = []

        if !includeTrashed {
            conditions.append("NOT EXISTS (SELECT 1 FROM asset_trash_states ats WHERE ats.asset_id = a.id AND ats.state = 'trashed')")
        }

        if !filter.searchText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            conditions.append("(a.original_filename LIKE ? OR a.tags LIKE ? OR fi.path LIKE ?)")
            let value = "%\(filter.searchText)%"
            values.append(.text(value))
            values.append(.text(value))
            values.append(.text(value))
        }
        if !filter.camera.isEmpty {
            conditions.append("(a.camera_make LIKE ? OR a.camera_model LIKE ?)")
            values.append(.text("%\(filter.camera)%"))
            values.append(.text("%\(filter.camera)%"))
        }
        if !filter.fileExtension.isEmpty {
            conditions.append("LOWER(fi.path) LIKE ?")
            values.append(.text("%.\(filter.fileExtension.lowercased())"))
        }
        if filter.minimumRating > 0 {
            conditions.append("a.rating >= ?")
            values.append(.int(Int64(filter.minimumRating)))
        }
        if filter.flaggedOnly {
            conditions.append("COALESCE(a.flag_state, CASE WHEN flag = 1 THEN 'picked' ELSE 'unflagged' END) = 'picked'")
        }
        if !filter.colorLabels.isEmpty {
            let labels = filter.colorLabels.sorted { $0.rawValue < $1.rawValue }
            let placeholders = Array(repeating: "?", count: labels.count).joined(separator: ", ")
            conditions.append("a.color_label IN (\(placeholders))")
            values.append(contentsOf: labels.map { .text($0.rawValue) })
        }
        if !filter.tag.isEmpty {
            conditions.append("a.tags LIKE ?")
            values.append(.text("%\(filter.tag)%"))
        }
        if !filter.directory.isEmpty {
            conditions.append("fi.path LIKE ?")
            values.append(.text("\(filter.directory)%"))
        }
        if let browseSelection = filter.browseSelection {
            let nodeIDs = try browseNodeIDs(selection: browseSelection)
            if nodeIDs.isEmpty {
                conditions.append("0 = 1")
            } else {
                let placeholders = Array(repeating: "?", count: nodeIDs.count).joined(separator: ", ")
                conditions.append(
                    """
                    a.id IN (
                        SELECT DISTINCT bfi_file.asset_id
                        FROM file_instances bfi_file
                        JOIN browse_file_instances bfi ON bfi.file_instance_id = bfi_file.id
                        WHERE bfi.node_id IN (\(placeholders))
                          AND bfi.membership_kind = 'direct_file_instance'
                    )
                    """
                )
                values.append(contentsOf: nodeIDs.map { .text($0.uuidString) })
            }
        }

        let statusCondition = statusSQL(filter.status)
        if !statusCondition.isEmpty {
            conditions.append(statusCondition)
        }
        let sortDirection = filter.sortOrder.sqlDirection

        let sql: String
        if conditions.isEmpty {
            sql = """
            WITH page AS (
                SELECT id
                FROM assets
                ORDER BY COALESCE(capture_time, created_at) \(sortDirection)
                LIMIT ? OFFSET ?
            )
            SELECT
                a.id, a.capture_time, a.camera_make, a.camera_model, a.lens_model,
                a.original_filename, a.content_fingerprint, a.metadata_fingerprint,
                a.rating, COALESCE(a.flag_state, CASE WHEN flag = 1 THEN 'picked' ELSE 'unflagged' END) AS flag_state, a.color_label, a.tags, a.created_at, a.updated_at,
                COUNT(fi.id) AS file_count,
                MIN(CASE WHEN fi.file_role IN ('raw_original','jpeg_original') THEN fi.path ELSE NULL END) AS primary_path,
                COALESCE(
                    MIN(CASE WHEN fi.file_role = 'thumbnail' THEN fi.path ELSE NULL END),
                    MIN(CASE WHEN fi.file_role = 'jpeg_original' THEN fi.path ELSE NULL END)
                ) AS thumbnail_path,
                MAX(CASE WHEN fi.file_role IN ('raw_original','jpeg_original') AND fi.availability = 'online' THEN 1 ELSE 0 END) AS has_online_original,
                MAX(CASE WHEN fi.sync_status = 'needs_sync' THEN 1 ELSE 0 END) AS has_needs_sync,
                MAX(CASE WHEN fi.sync_status = 'needs_archive' THEN 1 ELSE 0 END) AS has_needs_archive,
                MAX(CASE WHEN fi.storage_kind = 'nas' AND fi.authority_role = 'canonical' THEN 1 ELSE 0 END) AS has_archived_copy,
                MAX(CASE WHEN fi.authority_role = 'working_copy' THEN 1 ELSE 0 END) AS has_working_copy
            FROM page p
            JOIN assets a ON a.id = p.id
            LEFT JOIN file_instances fi ON fi.asset_id = a.id
            GROUP BY a.id
            ORDER BY COALESCE(a.capture_time, a.created_at) \(sortDirection)
            """
        } else {
            let whereClause = "WHERE " + conditions.joined(separator: " AND ")
            sql = """
            WITH matching_assets AS (
                SELECT DISTINCT a.id AS asset_id
                FROM assets a
                LEFT JOIN file_instances fi ON fi.asset_id = a.id
                \(whereClause)
            ),
            page AS (
                SELECT a.id
                FROM assets a
                JOIN matching_assets ma ON ma.asset_id = a.id
                ORDER BY COALESCE(a.capture_time, a.created_at) \(sortDirection)
                LIMIT ? OFFSET ?
            )
            SELECT
                a.id, a.capture_time, a.camera_make, a.camera_model, a.lens_model,
                a.original_filename, a.content_fingerprint, a.metadata_fingerprint,
                a.rating, COALESCE(a.flag_state, CASE WHEN flag = 1 THEN 'picked' ELSE 'unflagged' END) AS flag_state, a.color_label, a.tags, a.created_at, a.updated_at,
                COUNT(fi.id) AS file_count,
                MIN(CASE WHEN fi.file_role IN ('raw_original','jpeg_original') THEN fi.path ELSE NULL END) AS primary_path,
                COALESCE(
                    MIN(CASE WHEN fi.file_role = 'thumbnail' THEN fi.path ELSE NULL END),
                    MIN(CASE WHEN fi.file_role = 'jpeg_original' THEN fi.path ELSE NULL END)
                ) AS thumbnail_path,
                MAX(CASE WHEN fi.file_role IN ('raw_original','jpeg_original') AND fi.availability = 'online' THEN 1 ELSE 0 END) AS has_online_original,
                MAX(CASE WHEN fi.sync_status = 'needs_sync' THEN 1 ELSE 0 END) AS has_needs_sync,
                MAX(CASE WHEN fi.sync_status = 'needs_archive' THEN 1 ELSE 0 END) AS has_needs_archive,
                MAX(CASE WHEN fi.storage_kind = 'nas' AND fi.authority_role = 'canonical' THEN 1 ELSE 0 END) AS has_archived_copy,
                MAX(CASE WHEN fi.authority_role = 'working_copy' THEN 1 ELSE 0 END) AS has_working_copy
            FROM page p
            JOIN assets a ON a.id = p.id
            LEFT JOIN file_instances fi ON fi.asset_id = a.id
            GROUP BY a.id
            ORDER BY COALESCE(a.capture_time, a.created_at) \(sortDirection)
            """
        }
        values.append(.int(Int64(limit)))
        values.append(.int(Int64(offset)))

        return try prepare(sql, values) { statement in
            let asset = Asset(
                id: UUID(uuidString: statement.text(0)) ?? UUID(),
                captureTime: DateCoding.decode(statement.optionalText(1)),
                cameraMake: statement.text(2),
                cameraModel: statement.text(3),
                lensModel: statement.text(4),
                originalFilename: statement.text(5),
                contentFingerprint: statement.text(6),
                metadataFingerprint: statement.text(7),
                rating: Int(statement.int(8)),
                flagState: AssetFlagState(rawValue: statement.text(9)) ?? .unflagged,
                colorLabel: statement.optionalText(10).flatMap(AssetColorLabel.init(rawValue:)),
                tags: decodeTags(statement.text(11)),
                createdAt: DateCoding.decode(statement.text(12)) ?? Date(),
                updatedAt: DateCoding.decode(statement.text(13)) ?? Date(),
                status: assetStatus(
                    hasOnlineOriginal: statement.int(17) == 1,
                    hasNeedsSync: statement.int(18) == 1,
                    hasNeedsArchive: statement.int(19) == 1,
                    hasArchivedCopy: statement.int(20) == 1,
                    hasWorkingCopy: statement.int(21) == 1
                ),
                fileCount: Int(statement.int(14)),
                primaryPath: statement.optionalText(15),
                thumbnailPath: statement.optionalText(16)
            )
            return asset
        }
    }

    func trashedAssets() throws -> [TrashedAssetRecord] {
        try prepare(
            """
            SELECT asset_id, reason, changed_at_hlc, changed_by
            FROM asset_trash_states
            WHERE state = ?
            ORDER BY updated_at DESC, changed_at_hlc DESC, asset_id ASC
            """,
            [.text(ProjectedTrashState.trashed.rawValue)]
        ) { statement in
            guard let assetID = UUID(uuidString: statement.text(0)) else {
                throw DatabaseError.stepFailed("asset_trash_states.asset_id 不是 UUID：\(statement.text(0))")
            }
            return TrashedAssetRecord(
                assetID: assetID,
                reason: statement.text(1),
                movedAt: try LedgerSQLiteCoding.decodeTime(statement.text(2)),
                movedBy: statement.text(3)
            )
        }
    }

    func archiveReceipts(assetID: UUID) throws -> [ArchiveReceiptRecord] {
        try prepare(
            """
            SELECT op_id, actor_id, hybrid_logical_time, payload_json
            FROM operation_ledger
            WHERE op_type = ?
              AND entity_type = ?
              AND entity_id = ?
            ORDER BY hybrid_logical_time ASC, device_id ASC, op_id ASC
            """,
            [
                .text(LedgerOperationType.originalArchiveReceiptRecorded.rawValue),
                .text(LedgerEntityType.filePlacement.rawValue),
                .text(assetID.uuidString)
            ]
        ) { statement in
            guard let opID = UUID(uuidString: statement.text(0)) else {
                throw DatabaseError.stepFailed("operation_ledger.op_id 不是 UUID：\(statement.text(0))")
            }
            let payload = try LedgerSQLiteCoding.decodePayload(statement.text(3))
            guard case let .originalArchiveReceiptRecorded(recordedAssetID, fileObject, serverPlacement) = payload,
                  recordedAssetID == assetID else {
                throw DatabaseError.stepFailed("operation_ledger.payload 与 assetID 不匹配：\(assetID.uuidString)")
            }
            return ArchiveReceiptRecord(
                opID: opID,
                assetID: recordedAssetID,
                fileObject: fileObject,
                serverPlacement: serverPlacement,
                movedAt: try LedgerSQLiteCoding.decodeTime(statement.text(2)),
                movedBy: statement.text(1)
            )
        }
    }

    func canonicalPlacements(assetID: UUID) throws -> [FilePlacement] {
        let receipts = try archiveReceipts(assetID: assetID)
        guard !receipts.isEmpty else { return [] }

        let fileObjectByStableKey = Dictionary(uniqueKeysWithValues: receipts.map { ($0.fileObject.stableKey, $0.fileObject) })
        let stableKeys = Array(fileObjectByStableKey.keys).sorted()
        let placeholders = Array(repeating: "?", count: stableKeys.count).joined(separator: ", ")
        let values = stableKeys.map(SQLiteValue.text) + [.text(AuthorityRole.canonical.rawValue)]
        return try prepare(
            """
            SELECT file_object_id, holder_id, storage_kind, authority_role, availability
            FROM file_placements
            WHERE file_object_id IN (\(placeholders))
              AND authority_role = ?
            ORDER BY holder_id ASC, storage_kind ASC, file_object_id ASC
            """,
            values
        ) { statement in
            let stableKey = statement.text(0)
            guard let fileObject = fileObjectByStableKey[stableKey] else {
                throw DatabaseError.stepFailed("file_placements.file_object_id 找不到对应的 ledger 记录：\(stableKey)")
            }
            return FilePlacement(
                fileObjectID: fileObject,
                holderID: statement.text(1),
                storageKind: StorageKind(rawValue: statement.text(2)) ?? .local,
                authorityRole: AuthorityRole(rawValue: statement.text(3)) ?? .canonical,
                availability: Availability(rawValue: statement.text(4)) ?? .online
            )
        }
    }

    func derivatives(assetID: UUID) throws -> [DerivativeObject] {
        try prepare(
            """
            SELECT d.role, d.file_object_id, fo.content_hash, fo.size_bytes, fo.file_role,
                   d.s3_bucket, d.s3_key, d.s3_etag, d.pixel_width, d.pixel_height
            FROM derivative_objects d
            JOIN file_objects fo ON fo.id = d.file_object_id
            WHERE d.asset_id = ?
            ORDER BY d.role ASC
            """,
            [.text(assetID.uuidString)]
        ) { statement in
            guard let role = DerivativeRole(rawValue: statement.text(0)) else {
                throw DatabaseError.stepFailed("derivative_objects.role 无效：\(statement.text(0))")
            }
            guard let fileRole = FileRole(rawValue: statement.text(4)) else {
                throw DatabaseError.stepFailed("file_objects.file_role 无效：\(statement.text(4))")
            }
            let fileObject = FileObjectID(
                contentHash: statement.text(2),
                sizeBytes: statement.int64(3),
                role: fileRole
            )
            guard fileObject.stableKey == statement.text(1) else {
                throw DatabaseError.stepFailed("derivative_objects.file_object_id 与 file_objects 不匹配：\(statement.text(1))")
            }
            return DerivativeObject(
                assetID: assetID,
                role: role,
                fileObject: fileObject,
                s3Object: S3ObjectRef(
                    bucket: statement.text(5),
                    key: statement.text(6),
                    eTag: statement.optionalText(7)
                ),
                pixelSize: PixelSize(width: Int(statement.int(8)), height: Int(statement.int(9)))
            )
        }
    }

    func bootstrapAssetSnapshots() throws -> [AssetSnapshot] {
        try prepare(
            """
            SELECT id, capture_time, camera_make, camera_model, lens_model, original_filename,
                   content_fingerprint, metadata_fingerprint, rating,
                   COALESCE(flag_state, CASE WHEN flag = 1 THEN 'picked' ELSE 'unflagged' END),
                   color_label, tags, created_at, updated_at
            FROM assets
            ORDER BY id ASC
            """,
            []
        ) { statement in
            guard let assetID = UUID(uuidString: statement.text(0)) else {
                throw DatabaseError.stepFailed("assets.id 不是 UUID：\(statement.text(0))")
            }
            guard let createdAt = DateCoding.decode(statement.text(12)) else {
                throw DatabaseError.stepFailed("assets.created_at 无效：\(statement.text(12))")
            }
            guard let updatedAt = DateCoding.decode(statement.text(13)) else {
                throw DatabaseError.stepFailed("assets.updated_at 无效：\(statement.text(13))")
            }
            return AssetSnapshot(
                assetID: assetID,
                captureTime: statement.optionalText(1).flatMap(DateCoding.decode),
                cameraMake: statement.text(2),
                cameraModel: statement.text(3),
                lensModel: statement.text(4),
                originalFilename: statement.text(5),
                contentFingerprint: statement.text(6),
                metadataFingerprint: statement.text(7),
                rating: Int(statement.int(8)),
                flagState: AssetFlagState(rawValue: statement.text(9)) ?? .unflagged,
                colorLabel: statement.optionalText(10).flatMap(AssetColorLabel.init(rawValue:)),
                tags: decodeTags(statement.text(11)),
                createdAt: createdAt,
                updatedAt: updatedAt
            )
        }
    }

    func bootstrapAssetSnapshotCount() throws -> Int {
        let rows = try prepare("SELECT COUNT(*) FROM assets", []) { statement in
            Int(statement.int(0))
        }
        return rows.first ?? 0
    }

    func bootstrapFileInstances() throws -> [FileInstance] {
        try prepare(
            """
            SELECT id, asset_id, path, device_id, storage_kind, file_role, authority_role,
                   sync_status, size_bytes, content_hash, last_seen_at, availability
            FROM file_instances
            ORDER BY asset_id ASC, file_role ASC, path ASC
            """,
            []
        ) { statement in
            FileInstance(
                id: UUID(uuidString: statement.text(0)) ?? UUID(),
                assetID: UUID(uuidString: statement.text(1)) ?? UUID(),
                path: statement.text(2),
                deviceID: statement.text(3),
                storageKind: StorageKind(rawValue: statement.text(4)) ?? .local,
                fileRole: FileRole(rawValue: statement.text(5)) ?? .rawOriginal,
                authorityRole: AuthorityRole(rawValue: statement.text(6)) ?? .sourceCopy,
                syncStatus: SyncStatus(rawValue: statement.text(7)) ?? .synced,
                sizeBytes: statement.int64(8),
                contentHash: statement.text(9),
                lastSeenAt: DateCoding.decode(statement.text(10)) ?? Date(),
                availability: Availability(rawValue: statement.text(11)) ?? .missing
            )
        }
    }

    func bootstrapFileInstanceCount() throws -> Int {
        let rows = try prepare("SELECT COUNT(*) FROM file_instances", []) { statement in
            Int(statement.int(0))
        }
        return rows.first ?? 0
    }

    func syncMigrationState(libraryID: String) throws -> SyncMigrationState? {
        let rows = try prepare(
            """
            SELECT library_id, status, source_database_fingerprint, started_at, completed_at,
                   ledger_high_watermark, projection_verified
            FROM sync_migration_state
            WHERE library_id = ?
            LIMIT 1
            """,
            [.text(libraryID)]
        ) { statement in
            guard let status = SyncMigrationStatus(rawValue: statement.text(1)) else {
                throw DatabaseError.stepFailed("sync_migration_state.status 无效：\(statement.text(1))")
            }
            guard let startedAt = DateCoding.decode(statement.text(3)) else {
                throw DatabaseError.stepFailed("sync_migration_state.started_at 无效：\(statement.text(3))")
            }
            return SyncMigrationState(
                libraryID: statement.text(0),
                status: status,
                sourceDatabaseFingerprint: statement.text(2),
                startedAt: startedAt,
                completedAt: statement.optionalText(4).flatMap(DateCoding.decode),
                ledgerHighWatermark: Int(statement.int(5)),
                projectionVerified: statement.int(6) == 1
            )
        }
        return rows.first ?? nil
    }

    func recordSyncMigrationStarted(libraryID: String, sourceDatabaseFingerprint: String, startedAt: Date) throws {
        try execute(
            """
            INSERT INTO sync_migration_state (
                library_id, status, source_database_fingerprint, started_at,
                completed_at, ledger_high_watermark, projection_verified
            ) VALUES (?, ?, ?, ?, NULL, 0, 0)
            ON CONFLICT(library_id) DO UPDATE SET
                status = excluded.status,
                source_database_fingerprint = excluded.source_database_fingerprint,
                started_at = excluded.started_at,
                completed_at = NULL,
                projection_verified = 0
            """,
            [
                .text(libraryID),
                .text(SyncMigrationStatus.started.rawValue),
                .text(sourceDatabaseFingerprint),
                .text(DateCoding.encode(startedAt))
            ]
        )
    }

    func recordSyncMigrationCompleted(libraryID: String, completedAt: Date, ledgerHighWatermark: Int, projectionVerified: Bool) throws {
        try execute(
            """
            UPDATE sync_migration_state
            SET status = ?, completed_at = ?, ledger_high_watermark = ?, projection_verified = ?
            WHERE library_id = ?
            """,
            [
                .text(SyncMigrationStatus.completed.rawValue),
                .text(DateCoding.encode(completedAt)),
                .int(Int64(ledgerHighWatermark)),
                .int(projectionVerified ? 1 : 0),
                .text(libraryID)
            ]
        )
    }

    func countsByStatus() throws -> [AssetStatus: Int] {
        let sql = """
        SELECT status, COUNT(*)
        FROM (
            SELECT
                CASE
                    WHEN MAX(CASE WHEN fi.file_role IN ('raw_original','jpeg_original') AND fi.availability = 'online' THEN 1 ELSE 0 END) = 0 THEN 'missingOriginal'
                    WHEN MAX(CASE WHEN fi.sync_status = 'needs_sync' THEN 1 ELSE 0 END) = 1 THEN 'needsSync'
                    WHEN MAX(CASE WHEN fi.sync_status = 'needs_archive' THEN 1 ELSE 0 END) = 1 THEN 'needsArchive'
                    WHEN MAX(CASE WHEN fi.storage_kind = 'nas' AND fi.authority_role = 'canonical' THEN 1 ELSE 0 END) = 1 THEN 'archived'
                    WHEN MAX(CASE WHEN fi.authority_role = 'working_copy' THEN 1 ELSE 0 END) = 1 THEN 'working'
                    ELSE 'inbox'
                END AS status
            FROM assets a
            LEFT JOIN file_instances fi ON fi.asset_id = a.id
            GROUP BY a.id
        )
        GROUP BY status
        """
        let rows = try prepare(sql, []) { statement in
            (statement.text(0), Int(statement.int(1)))
        }
        return rows.reduce(into: [:]) { result, row in
            guard let status = AssetStatus(rawValue: row.0) else { return }
            result[status] = row.1
        }
    }

    func sourceDirectories() throws -> [SourceDirectory] {
        try prepare(
            """
            SELECT id, path, storage_kind, is_tracked, parent_source_directory_id, created_at, last_scanned_at
            FROM source_directories
            ORDER BY is_tracked DESC, path ASC
            """,
            []
        ) { statement in
            SourceDirectory(
                id: UUID(uuidString: statement.text(0)) ?? UUID(),
                path: statement.text(1),
                storageKind: StorageKind(rawValue: statement.text(2)) ?? .local,
                isTracked: statement.int(3) == 1,
                parentID: statement.optionalText(4).flatMap(UUID.init(uuidString:)),
                createdAt: DateCoding.decode(statement.text(5)) ?? Date(),
                lastScannedAt: DateCoding.decode(statement.optionalText(6))
            )
        }
    }

    func sourceDirectoryPathsNeedingBrowseGraphRepair() throws -> Set<String> {
        let paths = try prepare(
            """
            SELECT sd.path
            FROM source_directories sd
            LEFT JOIN browse_nodes bn
              ON bn.kind = ?
             AND bn.canonical_key = sd.path
            WHERE sd.is_tracked = 1
              AND (
                  (
                      bn.id IS NULL
                      AND EXISTS (
                          SELECT 1
                          FROM file_instances fi
                          WHERE (fi.path = sd.path OR fi.path LIKE sd.path || '/%')
                            AND fi.file_role IN ('raw_original', 'jpeg_original', 'sidecar', 'export')
                      )
                  )
                  OR EXISTS (
                      SELECT 1
                      FROM file_instances fi
                      LEFT JOIN browse_file_instances bfi
                        ON bfi.file_instance_id = fi.id
                       AND bfi.membership_kind = ?
                      WHERE (fi.path = sd.path OR fi.path LIKE sd.path || '/%')
                        AND fi.file_role IN ('raw_original', 'jpeg_original', 'sidecar', 'export')
                        AND bfi.file_instance_id IS NULL
                  )
              )
            ORDER BY sd.path
            """,
            [
                .text(BrowseNodeKind.folder.rawValue),
                .text(BrowseMembershipKind.directFileInstance.rawValue)
            ]
        ) { statement in
            statement.text(0)
        }
        return Set(paths)
    }

    func upsertBrowseFolderNode(path: String, storageKind: StorageKind) throws -> BrowseNode {
        let normalizedPath = Self.normalizedDirectoryPath(path)
        let id = UUID()
        let displayName = Self.lastPathComponent(of: normalizedPath)
        try execute(
            """
            INSERT INTO browse_nodes (id, kind, canonical_key, display_name, display_path, storage_kind)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(kind, canonical_key) DO UPDATE SET
                display_name = excluded.display_name,
                display_path = excluded.display_path,
                storage_kind = excluded.storage_kind
            """,
            [
                .text(id.uuidString),
                .text(BrowseNodeKind.folder.rawValue),
                .text(normalizedPath),
                .text(displayName),
                .text(normalizedPath),
                .text(storageKind.rawValue)
            ]
        )
        return try browseNode(kind: .folder, canonicalKey: normalizedPath)
    }

    func browseFolder(path: String) throws -> BrowseNode? {
        let normalizedPath = Self.normalizedDirectoryPath(path)
        return try prepare(
            """
            SELECT id, kind, canonical_key, display_name, display_path, storage_kind
            FROM browse_nodes
            WHERE kind = ? AND canonical_key = ?
            LIMIT 1
            """,
            [.text(BrowseNodeKind.folder.rawValue), .text(normalizedPath)]
        ) { statement in
            BrowseNode(
                id: UUID(uuidString: statement.text(0)) ?? UUID(),
                kind: BrowseNodeKind(rawValue: statement.text(1)) ?? .folder,
                canonicalKey: statement.text(2),
                displayName: statement.text(3),
                displayPath: statement.text(4),
                storageKind: StorageKind(rawValue: statement.text(5)) ?? .local
            )
        }.first
    }

    func browseFolders() throws -> [BrowseNode] {
        try prepare(
            """
            SELECT id, kind, canonical_key, display_name, display_path, storage_kind
            FROM browse_nodes
            WHERE kind = ?
            ORDER BY display_path
            """,
            [.text(BrowseNodeKind.folder.rawValue)]
        ) { statement in
            BrowseNode(
                id: UUID(uuidString: statement.text(0)) ?? UUID(),
                kind: BrowseNodeKind(rawValue: statement.text(1)) ?? .folder,
                canonicalKey: statement.text(2),
                displayName: statement.text(3),
                displayPath: statement.text(4),
                storageKind: StorageKind(rawValue: statement.text(5)) ?? .local
            )
        }
    }

    func removeBrowseFolderTree(path: String) throws {
        let normalizedPath = Self.normalizedDirectoryPath(path)
        try execute(
            """
            DELETE FROM browse_nodes
            WHERE kind = ?
              AND (canonical_key = ? OR canonical_key LIKE ? || '/%')
            """,
            [
                .text(BrowseNodeKind.folder.rawValue),
                .text(normalizedPath),
                .text(normalizedPath)
            ]
        )
    }

    func browseNodeIDs(selection: BrowseSelection) throws -> [UUID] {
        switch selection.scope {
        case BrowseScope.direct:
            return [selection.nodeID]
        case BrowseScope.recursive:
            return try prepare(
                """
                WITH RECURSIVE selected_browse_nodes(id) AS (
                    SELECT ?
                    UNION
                    SELECT be.child_node_id
                    FROM browse_edges be
                    JOIN selected_browse_nodes selected ON selected.id = be.parent_node_id
                    WHERE be.kind = 'filesystem_containment'
                )
                SELECT id FROM selected_browse_nodes
                """,
                [.text(selection.nodeID.uuidString)]
            ) { statement in
                UUID(uuidString: statement.text(0)) ?? selection.nodeID
            }
        }
    }

    func upsertBrowseFolderMembership(filePath: String, fileInstanceID: UUID, storageKind: StorageKind) throws {
        let folderPath = Self.parentDirectoryPath(ofFilePath: filePath)
        let folders = Self.ancestorDirectoryPaths(to: folderPath)
        guard !folders.isEmpty else { return }

        var previousNode: BrowseNode?
        var directNode: BrowseNode?
        for folder in folders {
            let node = try upsertBrowseFolderNode(path: folder, storageKind: storageKind)
            if let previousNode {
                try execute(
                    """
                    INSERT OR IGNORE INTO browse_edges (parent_node_id, child_node_id, kind)
                    VALUES (?, ?, ?)
                    """,
                    [
                        .text(previousNode.id.uuidString),
                        .text(node.id.uuidString),
                        .text(BrowseEdgeKind.filesystemContainment.rawValue)
                    ]
                )
            }
            previousNode = node
            directNode = node
        }

        guard let directNode else { return }
        try execute(
            "DELETE FROM browse_file_instances WHERE file_instance_id = ? AND membership_kind = ?",
            [.text(fileInstanceID.uuidString), .text(BrowseMembershipKind.directFileInstance.rawValue)]
        )
        try execute(
            """
            INSERT OR IGNORE INTO browse_file_instances (node_id, file_instance_id, membership_kind)
            VALUES (?, ?, ?)
            """,
            [
                .text(directNode.id.uuidString),
                .text(fileInstanceID.uuidString),
                .text(BrowseMembershipKind.directFileInstance.rawValue)
            ]
        )
    }

    func fileInstances(assetID: UUID) throws -> [FileInstance] {
        let sql = """
        SELECT id, asset_id, path, device_id, storage_kind, file_role, authority_role,
               sync_status, size_bytes, content_hash, last_seen_at, availability
        FROM file_instances
        WHERE asset_id = ?
        ORDER BY authority_role, file_role, path
        """
        return try prepare(sql, [.text(assetID.uuidString)]) { statement in
            FileInstance(
                id: UUID(uuidString: statement.text(0)) ?? UUID(),
                assetID: UUID(uuidString: statement.text(1)) ?? assetID,
                path: statement.text(2),
                deviceID: statement.text(3),
                storageKind: StorageKind(rawValue: statement.text(4)) ?? .local,
                fileRole: FileRole(rawValue: statement.text(5)) ?? .jpegOriginal,
                authorityRole: AuthorityRole(rawValue: statement.text(6)) ?? .sourceCopy,
                syncStatus: SyncStatus(rawValue: statement.text(7)) ?? .needsArchive,
                sizeBytes: statement.int64(8),
                contentHash: statement.text(9),
                lastSeenAt: DateCoding.decode(statement.text(10)) ?? Date(),
                availability: Availability(rawValue: statement.text(11)) ?? .missing
            )
        }
    }

    func movableFileInstances(assetIDs: [UUID]) throws -> [FileInstance] {
        let ids = Array(Set(assetIDs)).sorted { $0.uuidString < $1.uuidString }
        guard !ids.isEmpty else { return [] }
        let placeholders = Array(repeating: "?", count: ids.count).joined(separator: ", ")
        let sql = """
        SELECT id, asset_id, path, device_id, storage_kind, file_role, authority_role,
               sync_status, size_bytes, content_hash, last_seen_at, availability
        FROM file_instances
        WHERE asset_id IN (\(placeholders))
          AND availability = ?
          AND file_role IN ('raw_original', 'jpeg_original', 'sidecar', 'export')
        ORDER BY asset_id, authority_role, file_role, path
        """
        let values = ids.map { SQLiteValue.text($0.uuidString) } + [.text(Availability.online.rawValue)]
        return try prepare(sql, values) { statement in
            FileInstance(
                id: UUID(uuidString: statement.text(0)) ?? UUID(),
                assetID: UUID(uuidString: statement.text(1)) ?? UUID(),
                path: statement.text(2),
                deviceID: statement.text(3),
                storageKind: StorageKind(rawValue: statement.text(4)) ?? .local,
                fileRole: FileRole(rawValue: statement.text(5)) ?? .jpegOriginal,
                authorityRole: AuthorityRole(rawValue: statement.text(6)) ?? .sourceCopy,
                syncStatus: SyncStatus(rawValue: statement.text(7)) ?? .needsArchive,
                sizeBytes: statement.int64(8),
                contentHash: statement.text(9),
                lastSeenAt: DateCoding.decode(statement.text(10)) ?? Date(),
                availability: Availability(rawValue: statement.text(11)) ?? .missing
            )
        }
    }

    func deletableFileInstances(assetIDs: [UUID]) throws -> [FileInstance] {
        let ids = Array(Set(assetIDs)).sorted { $0.uuidString < $1.uuidString }
        guard !ids.isEmpty else { return [] }
        let placeholders = Array(repeating: "?", count: ids.count).joined(separator: ", ")
        let sql = """
        SELECT id, asset_id, path, device_id, storage_kind, file_role, authority_role,
               sync_status, size_bytes, content_hash, last_seen_at, availability
        FROM file_instances
        WHERE asset_id IN (\(placeholders))
          AND availability = ?
        ORDER BY asset_id, authority_role, file_role, path
        """
        let values = ids.map { SQLiteValue.text($0.uuidString) } + [.text(Availability.online.rawValue)]
        return try prepare(sql, values) { statement in
            FileInstance(
                id: UUID(uuidString: statement.text(0)) ?? UUID(),
                assetID: UUID(uuidString: statement.text(1)) ?? UUID(),
                path: statement.text(2),
                deviceID: statement.text(3),
                storageKind: StorageKind(rawValue: statement.text(4)) ?? .local,
                fileRole: FileRole(rawValue: statement.text(5)) ?? .jpegOriginal,
                authorityRole: AuthorityRole(rawValue: statement.text(6)) ?? .sourceCopy,
                syncStatus: SyncStatus(rawValue: statement.text(7)) ?? .needsArchive,
                sizeBytes: statement.int64(8),
                contentHash: statement.text(9),
                lastSeenAt: DateCoding.decode(statement.text(10)) ?? Date(),
                availability: Availability(rawValue: statement.text(11)) ?? .missing
            )
        }
    }

    #if os(macOS)
    func upsertScannedFile(
        _ scanned: ScannedFile,
        batchID: UUID,
        ledgerContext: ScannedFileLedgerContext? = nil
    ) throws -> ScannedFileUpsertResult {
        try transaction {
            let existingAssetID = try assetID(contentHash: scanned.contentHash, metadataFingerprint: scanned.metadataFingerprint)
            let assetID = existingAssetID ?? UUID()
            let now = Date()
            let isNewAsset = existingAssetID == nil

            if isNewAsset {
                try execute(
                    """
                    INSERT INTO assets (
                        id, capture_time, camera_make, camera_model, lens_model, original_filename,
                        content_fingerprint, metadata_fingerprint, rating, flag, flag_state, color_label, tags, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'unflagged', NULL, '[]', ?, ?)
                    """,
                    [
                        .text(assetID.uuidString),
                        .nullableText(scanned.captureTime.map(DateCoding.encode)),
                        .text(scanned.cameraMake),
                        .text(scanned.cameraModel),
                        .text(scanned.lensModel),
                        .text(scanned.url.lastPathComponent),
                        .text(scanned.contentHash),
                        .text(scanned.metadataFingerprint),
                        .int(Int64(scanned.rating)),
                        .text(DateCoding.encode(now)),
                        .text(DateCoding.encode(now))
                    ]
                )
                try insertVersion(assetID: assetID, name: "Original", kind: .original, parentID: nil, notes: "导入批次 \(batchID.uuidString)")
            } else {
                try execute(
                    """
                    UPDATE assets
                    SET rating = CASE WHEN rating = 0 AND ? > 0 THEN ? ELSE rating END,
                        capture_time = CASE WHEN capture_time IS NULL THEN ? ELSE capture_time END,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    [
                        .int(Int64(scanned.rating)),
                        .int(Int64(scanned.rating)),
                        .nullableText(scanned.captureTime.map(DateCoding.encode)),
                        .text(DateCoding.encode(now)),
                        .text(assetID.uuidString)
                    ]
                )
            }

            let existingFileID = try fileInstanceID(path: scanned.url.path)
            let fileID = existingFileID ?? UUID()
            try execute(
                """
                INSERT INTO file_instances (
                    id, asset_id, path, device_id, storage_kind, file_role, authority_role,
                    sync_status, size_bytes, content_hash, last_seen_at, availability
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(path) DO UPDATE SET
                    asset_id = excluded.asset_id,
                    device_id = excluded.device_id,
                    storage_kind = excluded.storage_kind,
                    file_role = excluded.file_role,
                    authority_role = excluded.authority_role,
                    sync_status = excluded.sync_status,
                    size_bytes = excluded.size_bytes,
                    content_hash = excluded.content_hash,
                    last_seen_at = excluded.last_seen_at,
                    availability = excluded.availability
                """,
                [
                    .text(fileID.uuidString),
                    .text(assetID.uuidString),
                    .text(scanned.url.path),
                    .text(scanned.deviceID),
                    .text(scanned.storageKind.rawValue),
                    .text(scanned.fileRole.rawValue),
                    .text(scanned.authorityRole.rawValue),
                    .text(scanned.syncStatus.rawValue),
                    .int(scanned.sizeBytes),
                    .text(scanned.contentHash),
                    .text(DateCoding.encode(now)),
                    .text(Availability.online.rawValue)
                ]
            )
            try upsertBrowseFolderMembership(filePath: scanned.url.path, fileInstanceID: fileID, storageKind: scanned.storageKind)

            if let thumbnailURL = scanned.thumbnailURL {
                try upsertAssetThumbnail(assetID: assetID, url: thumbnailURL, hash: scanned.thumbnailHash ?? "", sizeBytes: scanned.thumbnailSize)
            }
            if let previewURL = scanned.previewURL {
                try upsertAssetDerivative(assetID: assetID, role: .preview, url: previewURL, hash: scanned.previewHash ?? "", sizeBytes: scanned.previewSize)
            }

            for sidecar in scanned.sidecars {
                try upsertScannedSidecar(sidecar, assetID: assetID)
            }

            if let ledgerContext {
                try appendScannedSnapshotAndPlacement(assetID: assetID, scanned: scanned, ledgerContext: ledgerContext)
            }

            let derivativeCandidates = scanned.thumbnailURL.map {
                [ScannedDerivativeUploadCandidate(assetID: assetID, role: .thumbnail, fileURL: $0)]
            } ?? []
            return ScannedFileUpsertResult(
                assetID: assetID,
                insertedAsset: isNewAsset,
                insertedLocation: existingFileID == nil,
                derivativeCandidates: derivativeCandidates
            )
        }
    }

    func upsertSidecarsForFileInstance(_ sidecars: [ScannedSidecar], fileInstanceID: UUID) throws {
        guard !sidecars.isEmpty else { return }
        guard let assetID = try assetID(fileInstanceID: fileInstanceID) else { return }
        for sidecar in sidecars {
            try upsertScannedSidecar(sidecar, assetID: assetID)
        }
    }

    func upsertScannedSidecar(_ sidecar: ScannedSidecar, assetID: UUID) throws {
        let fileID = try fileInstanceID(path: sidecar.url.path) ?? UUID()
        try execute(
            """
            INSERT INTO file_instances (
                id, asset_id, path, device_id, storage_kind, file_role, authority_role,
                sync_status, size_bytes, content_hash, last_seen_at, availability
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                asset_id = excluded.asset_id,
                device_id = excluded.device_id,
                storage_kind = excluded.storage_kind,
                file_role = excluded.file_role,
                authority_role = excluded.authority_role,
                sync_status = excluded.sync_status,
                size_bytes = excluded.size_bytes,
                content_hash = excluded.content_hash,
                last_seen_at = excluded.last_seen_at,
                availability = excluded.availability
            """,
            [
                .text(fileID.uuidString),
                .text(assetID.uuidString),
                .text(sidecar.url.path),
                .text(sidecar.deviceID),
                .text(sidecar.storageKind.rawValue),
                .text(FileRole.sidecar.rawValue),
                .text(sidecar.authorityRole.rawValue),
                .text(sidecar.syncStatus.rawValue),
                .int(sidecar.sizeBytes),
                .text(sidecar.contentHash),
                .text(DateCoding.encode(Date())),
                .text(Availability.online.rawValue)
            ]
        )
        try upsertBrowseFolderMembership(filePath: sidecar.url.path, fileInstanceID: fileID, storageKind: sidecar.storageKind)
    }

    private func appendScannedSnapshotAndPlacement(
        assetID: UUID,
        scanned: ScannedFile,
        ledgerContext: ScannedFileLedgerContext
    ) throws {
        guard let snapshot = try currentAssetSnapshot(assetID: assetID) else { return }
        let clock = try reserveLedgerClock(
            libraryID: ledgerContext.libraryID,
            deviceID: ledgerContext.deviceID,
            currentWallTimeMilliseconds: ledgerContext.currentWallTimeMilliseconds()
        )
        let fileObject = FileObjectID(
            contentHash: scanned.contentHash,
            sizeBytes: scanned.sizeBytes,
            role: scanned.fileRole
        )
        let placement = FilePlacement(
            fileObjectID: fileObject,
            holderID: scanned.deviceID,
            storageKind: scanned.storageKind,
            authorityRole: scanned.authorityRole,
            availability: .online
        )
        let entries = [
            OperationLedgerEntry.assetSnapshotDeclared(
                libraryID: ledgerContext.libraryID,
                deviceID: ledgerContext.deviceID,
                deviceSequence: clock.deviceSequence,
                time: clock.hybridLogicalTime,
                actorID: ledgerContext.actorID,
                snapshot: snapshot
            ),
            OperationLedgerEntry.filePlacementSnapshotDeclared(
                libraryID: ledgerContext.libraryID,
                deviceID: ledgerContext.deviceID,
                deviceSequence: clock.deviceSequence + 1,
                time: clock.hybridLogicalTime,
                actorID: ledgerContext.actorID,
                assetID: assetID,
                fileObject: fileObject,
                placement: placement
            )
        ]
        for entry in entries {
            try appendLedgerEntryBody(entry, uploadStatus: .pending)
        }
    }

    private func appendScannedAssetSnapshot(
        assetID: UUID,
        ledgerContext: ScannedFileLedgerContext
    ) throws {
        guard let snapshot = try currentAssetSnapshot(assetID: assetID) else { return }
        let clock = try reserveLedgerClock(
            libraryID: ledgerContext.libraryID,
            deviceID: ledgerContext.deviceID,
            currentWallTimeMilliseconds: ledgerContext.currentWallTimeMilliseconds()
        )
        try appendLedgerEntryBody(
            .assetSnapshotDeclared(
                libraryID: ledgerContext.libraryID,
                deviceID: ledgerContext.deviceID,
                deviceSequence: clock.deviceSequence,
                time: clock.hybridLogicalTime,
                actorID: ledgerContext.actorID,
                snapshot: snapshot
            ),
            uploadStatus: .pending
        )
    }

    private func currentAssetSnapshot(assetID: UUID) throws -> AssetSnapshot? {
        let rows = try prepare(
            """
            SELECT id, capture_time, camera_make, camera_model, lens_model, original_filename,
                   content_fingerprint, metadata_fingerprint, rating,
                   COALESCE(flag_state, CASE WHEN flag = 1 THEN 'picked' ELSE 'unflagged' END),
                   color_label, tags, created_at, updated_at
            FROM assets
            WHERE id = ?
            LIMIT 1
            """,
            [.text(assetID.uuidString)]
        ) { statement in
            guard let assetID = UUID(uuidString: statement.text(0)) else {
                throw DatabaseError.stepFailed("assets.id 不是 UUID：\(statement.text(0))")
            }
            guard let createdAt = DateCoding.decode(statement.text(12)) else {
                throw DatabaseError.stepFailed("assets.created_at 无效：\(statement.text(12))")
            }
            guard let updatedAt = DateCoding.decode(statement.text(13)) else {
                throw DatabaseError.stepFailed("assets.updated_at 无效：\(statement.text(13))")
            }
            return AssetSnapshot(
                assetID: assetID,
                captureTime: statement.optionalText(1).flatMap(DateCoding.decode),
                cameraMake: statement.text(2),
                cameraModel: statement.text(3),
                lensModel: statement.text(4),
                originalFilename: statement.text(5),
                contentFingerprint: statement.text(6),
                metadataFingerprint: statement.text(7),
                rating: Int(statement.int(8)),
                flagState: AssetFlagState(rawValue: statement.text(9)) ?? .unflagged,
                colorLabel: statement.optionalText(10).flatMap(AssetColorLabel.init(rawValue:)),
                tags: decodeTags(statement.text(11)),
                createdAt: createdAt,
                updatedAt: updatedAt
            )
        }
        return rows.first
    }
    #endif

    func derivativeStoragePath() throws -> String? {
        try prepare("SELECT value FROM app_settings WHERE key = 'derivative_storage_path'", []) { statement in
            statement.text(0)
        }.first ?? nil
    }

    func setDerivativeStoragePath(_ path: String?) throws {
        if let path, !path.isEmpty {
            try execute(
                """
                INSERT INTO app_settings (key, value)
                VALUES ('derivative_storage_path', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                [.text(path)]
            )
        } else {
            try execute("DELETE FROM app_settings WHERE key = 'derivative_storage_path'", [])
        }
    }

    func lastAvailabilityRefreshAt() throws -> Date? {
        let value = try prepare("SELECT value FROM app_settings WHERE key = 'last_availability_refresh_at'", []) { statement in
            statement.text(0)
        }.first
        return DateCoding.decode(value)
    }

    func markAvailabilityRefreshCompleted(at date: Date) throws {
        try execute(
            """
            INSERT INTO app_settings (key, value)
            VALUES ('last_availability_refresh_at', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            [.text(DateCoding.encode(date))]
        )
    }

    func createImportBatch(sourcePath: String, deviceID: String) throws -> UUID {
        let id = UUID()
        try execute(
            """
            INSERT INTO import_batches (id, source_path, device_id, imported_at, imported_by, status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                .text(id.uuidString),
                .text(sourcePath),
                .text(deviceID),
                .text(DateCoding.encode(Date())),
                .text(NSUserName()),
                .text("scanning")
            ]
        )
        return id
    }

    func upsertSourceDirectory(path: String, storageKind: StorageKind) throws {
        try execute(
            """
            INSERT INTO source_directories (id, path, storage_kind, is_tracked, created_at, last_scanned_at)
            VALUES (?, ?, ?, 1, ?, NULL)
            ON CONFLICT(path) DO UPDATE SET
                storage_kind = excluded.storage_kind,
                is_tracked = 1
            """,
            [
                .text(UUID().uuidString),
                .text(path),
                .text(storageKind.rawValue),
                .text(DateCoding.encode(Date()))
            ]
        )
    }

    func setSourceDirectoryTracked(id: UUID, isTracked: Bool) throws {
        try execute(
            "UPDATE source_directories SET is_tracked = ? WHERE id = ?",
            [.int(isTracked ? 1 : 0), .text(id.uuidString)]
        )
    }

    func removeSourceDirectory(id: UUID) throws {
        try execute("UPDATE source_directories SET parent_source_directory_id = NULL WHERE parent_source_directory_id = ?", [.text(id.uuidString)])
        try execute("DELETE FROM source_directories WHERE id = ?", [.text(id.uuidString)])
    }

    func moveSourceDirectory(id: UUID, parentID: UUID?) throws {
        guard parentID != id else {
            throw DatabaseError.stepFailed("不能把文件夹移动到自身下面。")
        }
        try execute(
            "UPDATE source_directories SET parent_source_directory_id = ? WHERE id = ?",
            [.nullableText(parentID?.uuidString), .text(id.uuidString)]
        )
    }

    func fileInstancesForFolderMove(sourcePath: String) throws -> [String: (UUID, String)] {
        let normalizedPath = Self.normalizedDirectoryPath(sourcePath)
        let rows = try prepare(
            """
            SELECT id, path, content_hash
            FROM file_instances
            WHERE path = ? OR path LIKE ? || '/%'
            """,
            [.text(normalizedPath), .text(normalizedPath)]
        ) { statement in
            (
                path: statement.text(1),
                id: UUID(uuidString: statement.text(0)),
                hash: statement.text(2)
            )
        }
        return rows.reduce(into: [:]) { result, row in
            guard let id = row.id else { return }
            result[row.path] = (id, row.hash)
        }
    }

    func createFolderMoveJob(
        source: FolderMoveSource,
        destinationParentPath: String,
        destinationPath: String,
        items: [FolderMovePlanItem]
    ) throws -> FolderMoveJob {
        guard let sourceDirectoryID = source.sourceDirectoryID else {
            throw DatabaseError.stepFailed("移动源不属于任何已登记文件夹：\(source.path)")
        }
        let jobID = UUID()
        let now = DateCoding.encode(Date())
        try execute("BEGIN IMMEDIATE TRANSACTION")
        do {
            try execute(
                """
                INSERT INTO folder_move_jobs (
                    id, source_directory_id, source_path, destination_parent_path, destination_path,
                    storage_kind, status, total_files, completed_files, created_at, updated_at, error_detail
                ) VALUES (?, ?, ?, ?, ?, ?, 'running', ?, 0, ?, ?, '')
                """,
                [
                    .text(jobID.uuidString),
                    .text(sourceDirectoryID.uuidString),
                    .text(source.path),
                    .text(destinationParentPath),
                    .text(destinationPath),
                    .text(source.storageKind.rawValue),
                    .int(Int64(items.count)),
                    .text(now),
                    .text(now)
                ]
            )
            for item in items {
                try execute(
                    """
                    INSERT INTO folder_move_items (
                        id, job_id, file_instance_id, source_path, destination_path,
                        content_hash, status, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
                    """,
                    [
                        .text(UUID().uuidString),
                        .text(jobID.uuidString),
                        .nullableText(item.fileInstanceID?.uuidString),
                        .text(item.sourcePath),
                        .text(item.destinationPath),
                        .text(item.contentHash),
                        .text(now)
                    ]
                )
            }
            try execute("COMMIT")
        } catch {
            try? execute("ROLLBACK")
            throw error
        }
        return try folderMoveJob(id: jobID)
    }

    func unfinishedFolderMoveJob() throws -> FolderMoveJob? {
        try prepare(
            """
            SELECT id, source_directory_id, source_path, destination_parent_path, destination_path,
                   storage_kind, status, total_files, completed_files
            FROM folder_move_jobs
            WHERE status IN ('running', 'interrupted')
            ORDER BY created_at ASC
            LIMIT 1
            """,
            []
        ) { statement in
            FolderMoveJob(
                id: UUID(uuidString: statement.text(0)) ?? UUID(),
                sourceDirectoryID: UUID(uuidString: statement.text(1)) ?? UUID(),
                sourcePath: statement.text(2),
                destinationParentPath: statement.text(3),
                destinationPath: statement.text(4),
                storageKind: StorageKind(rawValue: statement.text(5)) ?? .local,
                status: statement.text(6),
                totalFiles: Int(statement.int(7)),
                completedFiles: Int(statement.int(8))
            )
        }.first
    }

    func pendingFolderMoveItems(jobID: UUID) throws -> [FolderMoveItem] {
        try prepare(
            """
            SELECT id, job_id, file_instance_id, source_path, destination_path, content_hash, status
            FROM folder_move_items
            WHERE job_id = ? AND status <> 'completed'
            ORDER BY source_path
            """,
            [.text(jobID.uuidString)]
        ) { statement in
            FolderMoveItem(
                id: UUID(uuidString: statement.text(0)) ?? UUID(),
                jobID: UUID(uuidString: statement.text(1)) ?? jobID,
                fileInstanceID: statement.optionalText(2).flatMap(UUID.init(uuidString:)),
                sourcePath: statement.text(3),
                destinationPath: statement.text(4),
                contentHash: statement.text(5),
                status: statement.text(6)
            )
        }
    }

    func markFolderMoveJobRunning(id: UUID) throws {
        try execute(
            "UPDATE folder_move_jobs SET status = 'running', updated_at = ? WHERE id = ?",
            [.text(DateCoding.encode(Date())), .text(id.uuidString)]
        )
    }

    func markInterruptedFolderMoveJobs() throws {
        try execute("UPDATE folder_move_jobs SET status = 'interrupted', updated_at = ? WHERE status = 'running'", [.text(DateCoding.encode(Date()))])
    }

    func completeFolderMoveItem(_ item: FolderMoveItem, hash: String, sizeBytes: Int64) throws {
        let now = DateCoding.encode(Date())
        try execute("BEGIN IMMEDIATE TRANSACTION")
        do {
            if let fileInstanceID = item.fileInstanceID {
                try execute(
                    """
                    UPDATE file_instances
                    SET path = ?, content_hash = ?, size_bytes = ?, last_seen_at = ?, availability = ?
                    WHERE id = ?
                    """,
                    [
                        .text(item.destinationPath),
                        .text(hash),
                        .int(sizeBytes),
                        .text(now),
                        .text(Availability.online.rawValue),
                        .text(fileInstanceID.uuidString)
                    ]
                )
                try upsertBrowseFolderMembership(filePath: item.destinationPath, fileInstanceID: fileInstanceID, storageKind: storageKindForPath(item.destinationPath))
            }
            try execute(
                "UPDATE folder_move_items SET status = 'completed', updated_at = ? WHERE id = ?",
                [.text(now), .text(item.id.uuidString)]
            )
            try execute(
                "UPDATE folder_move_jobs SET completed_files = completed_files + 1, updated_at = ? WHERE id = ?",
                [.text(now), .text(item.jobID.uuidString)]
            )
            try execute("COMMIT")
        } catch {
            try? execute("ROLLBACK")
            throw error
        }
    }

    func completeAssetFileMoveItem(_ item: AssetFileMovePlanItem, hash: String, sizeBytes: Int64) throws {
        let now = DateCoding.encode(Date())
        try execute("BEGIN IMMEDIATE TRANSACTION")
        do {
            try execute(
                """
                UPDATE file_instances
                SET path = ?, storage_kind = ?, content_hash = ?, size_bytes = ?, last_seen_at = ?, availability = ?
                WHERE id = ?
                """,
                [
                    .text(item.destinationPath),
                    .text(storageKindForPath(item.destinationPath).rawValue),
                    .text(hash),
                    .int(sizeBytes),
                    .text(now),
                    .text(Availability.online.rawValue),
                    .text(item.fileInstanceID.uuidString)
                ]
            )
            try execute(
                "UPDATE assets SET updated_at = ? WHERE id = (SELECT asset_id FROM file_instances WHERE id = ?)",
                [.text(now), .text(item.fileInstanceID.uuidString)]
            )
            try upsertBrowseFolderMembership(
                filePath: item.destinationPath,
                fileInstanceID: item.fileInstanceID,
                storageKind: storageKindForPath(item.destinationPath)
            )
            try execute(
                """
                INSERT INTO operation_logs (id, action, source_path, destination_path, status, detail, created_at)
                VALUES (?, 'move_asset_file', ?, ?, 'success', ?, ?)
                """,
                [
                    .text(UUID().uuidString),
                    .text(item.sourcePath),
                    .text(item.destinationPath),
                    .text("file_instance_id=\(item.fileInstanceID.uuidString)"),
                    .text(now)
                ]
            )
            try execute("COMMIT")
        } catch {
            try? execute("ROLLBACK")
            throw error
        }
    }

    #if os(macOS)
    func removeDeletedFileInstance(_ file: FileInstance, deletionMethod: AssetFileDeletionMethod) throws {
        let now = DateCoding.encode(Date())
        try execute("BEGIN IMMEDIATE TRANSACTION")
        do {
            try execute(
                "DELETE FROM file_instances WHERE id = ?",
                [.text(file.id.uuidString)]
            )
            let remainingFiles = try prepare(
                "SELECT COUNT(*) FROM file_instances WHERE asset_id = ?",
                [.text(file.assetID.uuidString)]
            ) { statement in
                Int(statement.int(0))
            }.first ?? 0
            if remainingFiles == 0 {
                try execute("DELETE FROM assets WHERE id = ?", [.text(file.assetID.uuidString)])
            } else {
                try execute(
                    "UPDATE assets SET updated_at = ? WHERE id = ?",
                    [.text(now), .text(file.assetID.uuidString)]
                )
            }
            try execute(
                """
                INSERT INTO operation_logs (id, action, source_path, destination_path, status, detail, created_at)
                VALUES (?, 'delete_asset_file', ?, NULL, 'success', ?, ?)
                """,
                [
                    .text(UUID().uuidString),
                    .text(file.path),
                    .text("asset_id=\(file.assetID.uuidString); file_instance_id=\(file.id.uuidString); method=\(deletionMethod.rawValue)"),
                    .text(now)
                ]
            )
            try execute("COMMIT")
        } catch {
            try? execute("ROLLBACK")
            throw error
        }
    }
    #endif

    func rewriteFolderMovePaths(job: FolderMoveJob, parentID: UUID?) throws {
        let now = DateCoding.encode(Date())
        try execute("BEGIN IMMEDIATE TRANSACTION")
        do {
            try execute(
                """
                UPDATE source_directories
                SET path = replace(path, ?, ?),
                    parent_source_directory_id = CASE WHEN id = ? THEN ? ELSE parent_source_directory_id END,
                    last_scanned_at = NULL
                WHERE path = ? OR path LIKE ? || '/%'
                """,
                [
                    .text(job.sourcePath),
                    .text(job.destinationPath),
                    .text(job.sourceDirectoryID.uuidString),
                    .nullableText(parentID?.uuidString),
                    .text(job.sourcePath),
                    .text(job.sourcePath)
                ]
            )
            try execute(
                """
                UPDATE file_instances SET path = replace(path, ?, ?)
                WHERE (path = ? OR path LIKE ? || '/%')
                  AND path NOT IN (SELECT destination_path FROM folder_move_items WHERE job_id = ? AND status = 'completed')
                """,
                [
                    .text(job.sourcePath),
                    .text(job.destinationPath),
                    .text(job.sourcePath),
                    .text(job.sourcePath),
                    .text(job.id.uuidString)
                ]
            )
            try execute(
                "UPDATE folder_move_jobs SET status = 'completed', completed_files = total_files, updated_at = ? WHERE id = ?",
                [.text(now), .text(job.id.uuidString)]
            )
            try execute(
                """
                UPDATE assets
                SET updated_at = ?
                WHERE id IN (
                    SELECT fi.asset_id
                    FROM file_instances fi
                    JOIN folder_move_items fmi ON fmi.file_instance_id = fi.id
                    WHERE fmi.job_id = ?
                )
                """,
                [.text(now), .text(job.id.uuidString)]
            )
            try execute(
                """
                INSERT INTO operation_logs (id, action, source_path, destination_path, status, detail, created_at)
                VALUES (?, 'move_folder', ?, ?, 'success', ?, ?)
                """,
                [
                    .text(UUID().uuidString),
                    .text(job.sourcePath),
                    .text(job.destinationPath),
                    .text("files=\(job.totalFiles)"),
                    .text(now)
                ]
            )
            try execute("COMMIT")
        } catch {
            try? execute("ROLLBACK")
            throw error
        }
        try refreshBrowseGraphForFolderMove(job: job)
    }

    func refreshBrowseGraphForFolderMove(job: FolderMoveJob) throws {
        let rows = try prepare(
            """
            SELECT fi.id, fi.path, fi.storage_kind
            FROM file_instances fi
            JOIN folder_move_items fmi ON fmi.file_instance_id = fi.id
            WHERE fmi.job_id = ?
              AND fi.file_role IN ('raw_original', 'jpeg_original', 'sidecar', 'export')
            ORDER BY fi.path
            """,
            [.text(job.id.uuidString)]
        ) { statement in
            (
                UUID(uuidString: statement.text(0)),
                statement.text(1),
                StorageKind(rawValue: statement.text(2)) ?? .local
            )
        }

        guard !rows.isEmpty else { return }
        try execute("BEGIN IMMEDIATE TRANSACTION")
        do {
            try execute(
                """
                DELETE FROM browse_nodes
                WHERE kind = ?
                  AND (canonical_key = ? OR canonical_key LIKE ? || '/%')
                """,
                [
                    .text(BrowseNodeKind.folder.rawValue),
                    .text(job.sourcePath),
                    .text(job.sourcePath)
                ]
            )
            for row in rows {
                guard let fileInstanceID = row.0 else { continue }
                try upsertBrowseFolderMembership(filePath: row.1, fileInstanceID: fileInstanceID, storageKind: row.2)
            }
            try execute("COMMIT")
        } catch {
            try? execute("ROLLBACK")
            throw error
        }
    }

    func failFolderMoveJob(id: UUID, error: Error) throws {
        #if os(macOS)
        let detail = error.fullTrace
        #else
        let detail = String(reflecting: error)
        #endif
        try execute(
            "UPDATE folder_move_jobs SET status = 'failed', error_detail = ?, updated_at = ? WHERE id = ?",
            [.text(detail), .text(DateCoding.encode(Date())), .text(id.uuidString)]
        )
    }

    func markSourceDirectoryScanned(path: String) throws {
        try execute(
            "UPDATE source_directories SET last_scanned_at = ? WHERE path = ?",
            [.text(DateCoding.encode(Date())), .text(path)]
        )
    }

    func finishImportBatch(_ id: UUID, status: String) throws {
        try execute("UPDATE import_batches SET status = ? WHERE id = ?", [.text(status), .text(id.uuidString)])
    }

    func markInterruptedImportBatches() throws {
        try execute("UPDATE import_batches SET status = 'interrupted' WHERE status = 'scanning'")
    }

    func clearInterruptedBatches(sourcePath: String) throws {
        try execute("UPDATE import_batches SET status = 'resumed' WHERE source_path = ? AND status = 'interrupted'", [.text(sourcePath)])
    }

    func latestInterruptedScanPath() throws -> String? {
        try prepare(
            """
            SELECT source_path
            FROM import_batches
            WHERE status = 'interrupted'
            ORDER BY imported_at DESC
            LIMIT 1
            """,
            []
        ) { statement in
            statement.text(0)
        }.first ?? nil
    }

    func unchangedFileInstanceID(path: String, sizeBytes: Int64) throws -> UUID? {
        try prepare(
            """
            SELECT id
            FROM file_instances
            WHERE path = ? AND size_bytes = ? AND availability = 'online'
            LIMIT 1
            """,
            [.text(path), .int(sizeBytes)]
        ) { statement in
            UUID(uuidString: statement.text(0))
        }.first ?? nil
    }

    func applyScannedRatingIfEmpty(path: String, rating: Int) throws {
        guard rating > 0 else { return }
        try execute(
            """
            UPDATE assets
            SET rating = ?, updated_at = ?
            WHERE rating = 0
              AND id IN (SELECT asset_id FROM file_instances WHERE path = ?)
            """,
            [.int(Int64(rating)), .text(DateCoding.encode(Date())), .text(path)]
        )
    }

    func applyScannedCaptureTimeIfEmpty(path: String, captureTime: Date?) throws {
        guard let captureTime else { return }
        try execute(
            """
            UPDATE assets
            SET capture_time = ?,
                updated_at = ?
            WHERE capture_time IS NULL
              AND id IN (SELECT asset_id FROM file_instances WHERE path = ?)
            """,
            [
                .text(DateCoding.encode(captureTime)),
                .text(DateCoding.encode(Date())),
                .text(path)
            ]
        )
    }

    #if os(macOS)
    func applyScannedMetadataBackfillIfNeeded(
        fileInstanceID: UUID,
        rating: Int,
        captureTime: Date?,
        ledgerContext: ScannedFileLedgerContext? = nil
    ) throws -> Bool {
        try transaction {
            guard let assetID = try assetID(fileInstanceID: fileInstanceID) else { return false }
            let rows = try prepare(
                """
                SELECT rating, capture_time
                FROM assets
                WHERE id = ?
                LIMIT 1
                """,
                [.text(assetID.uuidString)]
            ) { statement in
                (
                    rating: Int(statement.int(0)),
                    captureTime: statement.optionalText(1).flatMap(DateCoding.decode)
                )
            }
            guard let row = rows.first else { return false }

            var changed = false
            let now = DateCoding.encode(Date())
            if row.rating == 0, rating > 0 {
                try execute(
                    "UPDATE assets SET rating = ?, updated_at = ? WHERE id = ?",
                    [.int(Int64(rating)), .text(now), .text(assetID.uuidString)]
                )
                changed = true
            }
            if row.captureTime == nil, let captureTime {
                try execute(
                    "UPDATE assets SET capture_time = ?, updated_at = ? WHERE id = ?",
                    [.text(DateCoding.encode(captureTime)), .text(now), .text(assetID.uuidString)]
                )
                changed = true
            }

            if changed, let ledgerContext {
                try appendScannedAssetSnapshot(assetID: assetID, ledgerContext: ledgerContext)
            }
            return changed
        }
    }
    #endif

    func thumbnailFileInstances() throws -> [FileInstance] {
        let sql = """
        SELECT id, asset_id, path, device_id, storage_kind, file_role, authority_role,
               sync_status, size_bytes, content_hash, last_seen_at, availability
        FROM file_instances
        WHERE file_role = 'thumbnail'
        ORDER BY path
        """
        return try prepare(sql, []) { statement in
            FileInstance(
                id: UUID(uuidString: statement.text(0)) ?? UUID(),
                assetID: UUID(uuidString: statement.text(1)) ?? UUID(),
                path: statement.text(2),
                deviceID: statement.text(3),
                storageKind: StorageKind(rawValue: statement.text(4)) ?? .local,
                fileRole: FileRole(rawValue: statement.text(5)) ?? .thumbnail,
                authorityRole: AuthorityRole(rawValue: statement.text(6)) ?? .cache,
                syncStatus: SyncStatus(rawValue: statement.text(7)) ?? .cacheOnly,
                sizeBytes: statement.int64(8),
                contentHash: statement.text(9),
                lastSeenAt: DateCoding.decode(statement.text(10)) ?? Date(),
                availability: Availability(rawValue: statement.text(11)) ?? .missing
            )
        }
    }

    func thumbnailsNeedingDerivativeUpload() throws -> [FileInstance] {
        let sql = """
        SELECT file_instances.id, file_instances.asset_id, file_instances.path, file_instances.device_id,
               file_instances.storage_kind, file_instances.file_role, file_instances.authority_role,
               file_instances.sync_status, file_instances.size_bytes, file_instances.content_hash,
               file_instances.last_seen_at, file_instances.availability
        FROM file_instances
        LEFT JOIN derivative_objects d
          ON d.asset_id = file_instances.asset_id
         AND d.role = 'thumbnail'
        LEFT JOIN file_objects fo
          ON fo.id = d.file_object_id
        WHERE file_instances.file_role = 'thumbnail'
          AND (
            d.asset_id IS NULL
            OR fo.content_hash IS NULL
            OR fo.content_hash != file_instances.content_hash
            OR fo.size_bytes != file_instances.size_bytes
            OR fo.file_role != file_instances.file_role
          )
        ORDER BY path
        """
        return try prepare(sql, []) { statement in
            FileInstance(
                id: UUID(uuidString: statement.text(0)) ?? UUID(),
                assetID: UUID(uuidString: statement.text(1)) ?? UUID(),
                path: statement.text(2),
                deviceID: statement.text(3),
                storageKind: StorageKind(rawValue: statement.text(4)) ?? .local,
                fileRole: FileRole(rawValue: statement.text(5)) ?? .thumbnail,
                authorityRole: AuthorityRole(rawValue: statement.text(6)) ?? .cache,
                syncStatus: SyncStatus(rawValue: statement.text(7)) ?? .cacheOnly,
                sizeBytes: statement.int64(8),
                contentHash: statement.text(9),
                lastSeenAt: DateCoding.decode(statement.text(10)) ?? Date(),
                availability: Availability(rawValue: statement.text(11)) ?? .missing
            )
        }
    }

    func updateFileInstanceLocation(id: UUID, path: String, hash: String, sizeBytes: Int64) throws {
        try execute(
            """
            UPDATE file_instances
            SET path = ?, content_hash = ?, size_bytes = ?, last_seen_at = ?, availability = ?
            WHERE id = ?
            """,
            [
                .text(path),
                .text(hash),
                .int(sizeBytes),
                .text(DateCoding.encode(Date())),
                .text(Availability.online.rawValue),
                .text(id.uuidString)
            ]
        )
    }

    func tableExists(_ tableName: String) throws -> Bool {
        let rows = try prepare(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name = ?
            LIMIT 1
            """,
            [.text(tableName)]
        ) { statement in
            statement.text(0)
        }
        return !rows.isEmpty
    }

    func appendLedgerEntry(_ entry: OperationLedgerEntry, uploadStatus: LedgerUploadStatus) throws {
        try transaction {
            if let existing = try ledgerEntry(opID: entry.opID) {
                guard Self.isSameLedgerPayload(existing, entry) else {
                    throw DatabaseError.stepFailed("operation_ledger.op_id 冲突：\(entry.opID.uuidString)")
                }
                if uploadStatus == .pending {
                    try execute(
                        "UPDATE operation_ledger SET upload_status = ? WHERE op_id = ?",
                        [.text(uploadStatus.rawValue), .text(entry.opID.uuidString)]
                    )
                    try execute(
                        """
                        INSERT OR IGNORE INTO sync_upload_queue (op_id, status, attempt_count, last_error, updated_at)
                        VALUES (?, ?, 0, '', ?)
                        """,
                        [.text(entry.opID.uuidString), .text(LedgerUploadStatus.pending.rawValue), .text(DateCoding.encode(Date()))]
                    )
                } else {
                    try execute(
                        "UPDATE operation_ledger SET upload_status = ? WHERE op_id = ?",
                        [.text(uploadStatus.rawValue), .text(entry.opID.uuidString)]
                    )
                    try execute("DELETE FROM sync_upload_queue WHERE op_id = ?", [.text(entry.opID.uuidString)])
                }
                return
            }
            try appendLedgerEntryBody(entry, uploadStatus: uploadStatus)
        }
    }

    func appendLedgerEntries(_ entries: [OperationLedgerEntry], uploadStatus: LedgerUploadStatus) throws -> Int {
        try transaction {
            var insertedCount = 0
            for entry in entries {
                if let existing = try ledgerEntry(opID: entry.opID) {
                    guard Self.isSameLedgerPayload(existing, entry) else {
                        throw DatabaseError.stepFailed("operation_ledger.op_id 冲突：\(entry.opID.uuidString)")
                    }
                    if uploadStatus == .pending {
                        try execute(
                            "UPDATE operation_ledger SET upload_status = ? WHERE op_id = ?",
                            [.text(uploadStatus.rawValue), .text(entry.opID.uuidString)]
                        )
                        try execute(
                            """
                            INSERT OR IGNORE INTO sync_upload_queue (op_id, status, attempt_count, last_error, updated_at)
                            VALUES (?, ?, 0, '', ?)
                            """,
                            [.text(entry.opID.uuidString), .text(LedgerUploadStatus.pending.rawValue), .text(DateCoding.encode(Date()))]
                        )
                    } else {
                        try execute(
                            "UPDATE operation_ledger SET upload_status = ? WHERE op_id = ?",
                            [.text(uploadStatus.rawValue), .text(entry.opID.uuidString)]
                        )
                        try execute("DELETE FROM sync_upload_queue WHERE op_id = ?", [.text(entry.opID.uuidString)])
                    }
                    continue
                }
                try appendLedgerEntryBody(entry, uploadStatus: uploadStatus)
                insertedCount += 1
            }
            return insertedCount
        }
    }

    func recordLedgerOperation(
        libraryID: String,
        deviceID: SyncDeviceID,
        currentWallTimeMilliseconds: Int64,
        uploadStatus: LedgerUploadStatus,
        buildEntry: (Int64, HybridLogicalTime) throws -> OperationLedgerEntry
    ) throws {
        try transaction {
            let clock = try reserveLedgerClock(
                libraryID: libraryID,
                deviceID: deviceID,
                currentWallTimeMilliseconds: currentWallTimeMilliseconds
            )
            let entry = try buildEntry(clock.deviceSequence, clock.hybridLogicalTime)
            try appendLedgerEntryBody(entry, uploadStatus: uploadStatus)
        }
    }

    func recordLedgerOperations(
        libraryID: String,
        deviceID: SyncDeviceID,
        currentWallTimeMilliseconds: Int64,
        uploadStatus: LedgerUploadStatus,
        buildEntries: (Int64, HybridLogicalTime) throws -> [OperationLedgerEntry]
    ) throws {
        try transaction {
            let clock = try reserveLedgerClock(
                libraryID: libraryID,
                deviceID: deviceID,
                currentWallTimeMilliseconds: currentWallTimeMilliseconds
            )
            let entries = try buildEntries(clock.deviceSequence, clock.hybridLogicalTime)
            for entry in entries {
                try appendLedgerEntryBody(entry, uploadStatus: uploadStatus)
            }
        }
    }

    func updateAssetMetadataAndAppendLedger(
        asset: Asset,
        libraryID: String,
        deviceID: SyncDeviceID,
        currentWallTimeMilliseconds: Int64,
        uploadStatus: LedgerUploadStatus = .pending,
        buildEntry: (Int64, HybridLogicalTime) throws -> OperationLedgerEntry,
        appendEntry: ((OperationLedgerEntry, LedgerUploadStatus) throws -> Void)? = nil
    ) throws {
        try transaction {
            try updateAssetMetadata(asset: asset)
            let clock = try reserveLedgerClock(
                libraryID: libraryID,
                deviceID: deviceID,
                currentWallTimeMilliseconds: currentWallTimeMilliseconds
            )
            let entry = try buildEntry(clock.deviceSequence, clock.hybridLogicalTime)
            if let appendEntry {
                try appendEntry(entry, uploadStatus)
            } else {
                try appendLedgerEntryBody(entry, uploadStatus: uploadStatus)
            }
        }
    }

    func ledgerEntries(libraryID: String) throws -> [OperationLedgerEntry] {
        try prepare(
            """
            SELECT op_id, library_id, device_id, device_seq, hybrid_logical_time,
                   actor_id, entity_type, entity_id, op_type, payload_json,
                   base_version, created_at, global_seq
            FROM operation_ledger
            WHERE library_id = ?
            ORDER BY global_seq IS NULL ASC, global_seq ASC, hybrid_logical_time ASC, device_id ASC, op_id ASC
            """,
            [.text(libraryID)]
        ) { statement in
            guard let opID = UUID(uuidString: statement.text(0)) else {
                throw DatabaseError.stepFailed("operation_ledger.op_id 不是 UUID：\(statement.text(0))")
            }
            let entityTypeRaw = statement.text(6)
            guard let entityType = LedgerEntityType(rawValue: entityTypeRaw) else {
                throw DatabaseError.stepFailed("operation_ledger.entity_type 无效：\(entityTypeRaw)")
            }
            let opTypeRaw = statement.text(8)
            guard let opType = LedgerOperationType(rawValue: opTypeRaw) else {
                throw DatabaseError.stepFailed("operation_ledger.op_type 无效：\(opTypeRaw)")
            }
            let createdAtRaw = statement.text(11)
            guard let createdAt = DateCoding.decode(createdAtRaw) else {
                throw DatabaseError.stepFailed("operation_ledger.created_at 无效：\(createdAtRaw)")
            }
            return OperationLedgerEntry(
                opID: opID,
                globalSeq: statement.optionalInt64(12),
                libraryID: statement.text(1),
                deviceID: SyncDeviceID(statement.text(2)),
                deviceSequence: statement.int64(3),
                hybridLogicalTime: try LedgerSQLiteCoding.decodeTime(statement.text(4)),
                actorID: statement.text(5),
                entityType: entityType,
                entityID: statement.text(7),
                opType: opType,
                payload: try LedgerSQLiteCoding.decodePayload(statement.text(9)),
                baseVersion: statement.optionalText(10),
                createdAt: createdAt
            )
        }
    }

    func pendingLedgerUploadEntries(libraryID: String) throws -> [OperationLedgerEntry] {
        try prepare(
            """
            SELECT ol.op_id, ol.library_id, ol.device_id, ol.device_seq, ol.hybrid_logical_time,
                   ol.actor_id, ol.entity_type, ol.entity_id, ol.op_type, ol.payload_json,
                   ol.base_version, ol.created_at, ol.global_seq
            FROM sync_upload_queue q
            JOIN operation_ledger ol ON ol.op_id = q.op_id
            WHERE q.status = ?
              AND ol.library_id = ?
            ORDER BY ol.hybrid_logical_time ASC, ol.device_id ASC, ol.op_id ASC
            """,
            [.text(LedgerUploadStatus.pending.rawValue), .text(libraryID)]
        ) { statement in
            try decodeLedgerEntry(statement)
        }
    }

    func claimPendingLedgerUploadEntries(libraryID: String, limit: Int) throws -> [OperationLedgerEntry] {
        precondition(limit > 0, "ledger upload claim limit must be positive")
        try recoverStaleLedgerUploads(olderThan: Date().addingTimeInterval(-Self.ledgerUploadClaimLeaseDuration))
        return try transaction {
            let pendingEntries = try prepare(
                """
                SELECT ol.op_id, ol.library_id, ol.device_id, ol.device_seq, ol.hybrid_logical_time,
                       ol.actor_id, ol.entity_type, ol.entity_id, ol.op_type, ol.payload_json,
                       ol.base_version, ol.created_at, ol.global_seq
                FROM sync_upload_queue q
                JOIN operation_ledger ol ON ol.op_id = q.op_id
                WHERE q.status = ?
                  AND ol.library_id = ?
                ORDER BY ol.hybrid_logical_time ASC, ol.device_id ASC, ol.op_id ASC
                LIMIT ?
                """,
                [.text(LedgerUploadStatus.pending.rawValue), .text(libraryID), .int(Int64(limit))]
            ) { statement in
                try decodeLedgerEntry(statement)
            }
            guard !pendingEntries.isEmpty else { return [] }

            let ids = pendingEntries.map(\.opID)
            let placeholders = Array(repeating: "?", count: ids.count).joined(separator: ", ")
            let values = ids.map { SQLiteValue.text($0.uuidString) }
            let now = DateCoding.encode(Date())

            try execute(
                "UPDATE sync_upload_queue SET status = ?, updated_at = ? WHERE status = ? AND op_id IN (\(placeholders))",
                [.text(LedgerUploadStatus.uploading.rawValue), .text(now), .text(LedgerUploadStatus.pending.rawValue)] + values
            )
            try execute(
                "UPDATE operation_ledger SET upload_status = ? WHERE op_id IN (\(placeholders))",
                [.text(LedgerUploadStatus.uploading.rawValue)] + values
            )
            return pendingEntries
        }
    }

    func recoverStaleLedgerUploads(olderThan cutoff: Date) throws {
        let cutoffValue = DateCoding.encode(cutoff)
        try transaction {
            let staleIDs = try prepare(
                """
                SELECT op_id
                FROM sync_upload_queue
                WHERE status = ?
                  AND updated_at < ?
                ORDER BY op_id ASC
                """,
                [.text(LedgerUploadStatus.uploading.rawValue), .text(cutoffValue)]
            ) { statement in
                statement.text(0)
            }
            guard !staleIDs.isEmpty else { return }

            let placeholders = Array(repeating: "?", count: staleIDs.count).joined(separator: ", ")
            let values = staleIDs.map { SQLiteValue.text($0) }
            let now = DateCoding.encode(Date())
            try execute(
                "UPDATE sync_upload_queue SET status = ?, updated_at = ? WHERE op_id IN (\(placeholders))",
                [.text(LedgerUploadStatus.pending.rawValue), .text(now)] + values
            )
            try execute(
                "UPDATE operation_ledger SET upload_status = ? WHERE op_id IN (\(placeholders))",
                [.text(LedgerUploadStatus.pending.rawValue)] + values
            )
        }
    }

    func pendingLedgerUploadCount() throws -> Int {
        let rows = try prepare(
            "SELECT COUNT(*) FROM sync_upload_queue WHERE status = ?",
            [.text(LedgerUploadStatus.pending.rawValue)]
        ) { statement in
            Int(statement.int(0))
        }
        return rows.first ?? 0
    }

    func ledgerUploadStatus(opID: UUID) throws -> LedgerUploadStatus? {
        let rows = try prepare(
            "SELECT upload_status FROM operation_ledger WHERE op_id = ? LIMIT 1",
            [.text(opID.uuidString)]
        ) { statement -> LedgerUploadStatus? in
            guard let rawValue = statement.optionalText(0) else { return nil }
            guard let status = LedgerUploadStatus(rawValue: rawValue) else {
                throw DatabaseError.stepFailed("operation_ledger.upload_status 无效：\(rawValue)")
            }
            return status
        }
        return rows.first ?? nil
    }

    func ledgerGlobalSeq(opID: UUID) throws -> Int64? {
        let rows = try prepare(
            "SELECT global_seq FROM operation_ledger WHERE op_id = ? LIMIT 1",
            [.text(opID.uuidString)]
        ) { statement in
            statement.optionalInt64(0)
        }
        return rows.first ?? nil
    }

    func ledgerEntry(opID: UUID) throws -> OperationLedgerEntry? {
        let rows = try prepare(
            """
            SELECT op_id, library_id, device_id, device_seq, hybrid_logical_time,
                   actor_id, entity_type, entity_id, op_type, payload_json,
                   base_version, created_at, global_seq
            FROM operation_ledger
            WHERE op_id = ?
            LIMIT 1
            """,
            [.text(opID.uuidString)]
        ) { statement in
            try decodeLedgerEntry(statement)
        }
        return rows.first
    }

    func markLedgerEntriesAcknowledged(_ opIDs: [UUID]) throws {
        let ids = Array(Set(opIDs)).sorted { $0.uuidString < $1.uuidString }
        guard !ids.isEmpty else { return }
        try transaction {
            let placeholders = Array(repeating: "?", count: ids.count).joined(separator: ", ")
            let values = ids.map { SQLiteValue.text($0.uuidString) }
            try execute(
                "UPDATE operation_ledger SET upload_status = ? WHERE op_id IN (\(placeholders))",
                [.text(LedgerUploadStatus.acknowledged.rawValue)] + values
            )
            try execute(
                "DELETE FROM sync_upload_queue WHERE op_id IN (\(placeholders))",
                values
            )
        }
    }

    func markLedgerEntriesAcknowledged(_ accepted: [SyncOpsAcceptedOperation], cursor: String) throws {
        try transaction {
            for acceptedOperation in accepted {
                try execute(
                    """
                    UPDATE operation_ledger
                    SET upload_status = ?, global_seq = ?, remote_cursor = ?
                    WHERE op_id = ?
                    """,
                    [
                        .text(LedgerUploadStatus.acknowledged.rawValue),
                        .int(acceptedOperation.globalSeq),
                        .text(cursor),
                        .text(acceptedOperation.opID.uuidString)
                    ]
                )
                try execute("DELETE FROM sync_upload_queue WHERE op_id = ?", [.text(acceptedOperation.opID.uuidString)])
            }
        }
    }

    func restoreClaimedLedgerUploadEntries(_ opIDs: [UUID], lastError: String) throws {
        let ids = Array(Set(opIDs)).sorted { $0.uuidString < $1.uuidString }
        guard !ids.isEmpty else { return }
        try transaction {
            let placeholders = Array(repeating: "?", count: ids.count).joined(separator: ", ")
            let values = ids.map { SQLiteValue.text($0.uuidString) }
            let now = DateCoding.encode(Date())
            try execute(
                """
                UPDATE sync_upload_queue
                SET status = ?, attempt_count = attempt_count + 1, last_error = ?, updated_at = ?
                WHERE status = ? AND op_id IN (\(placeholders))
                """,
                [.text(LedgerUploadStatus.pending.rawValue), .text(lastError), .text(now), .text(LedgerUploadStatus.uploading.rawValue)] + values
            )
            try execute(
                "UPDATE operation_ledger SET upload_status = ? WHERE op_id IN (\(placeholders))",
                [.text(LedgerUploadStatus.pending.rawValue)] + values
            )
        }
    }

    func syncCursor(peerID: String) throws -> String? {
        let rows = try prepare(
            "SELECT cursor FROM sync_cursors WHERE peer_id = ? LIMIT 1",
            [.text(peerID)]
        ) { statement in
            statement.optionalText(0)
        }
        return rows.first ?? nil
    }

    func setSyncCursor(peerID: String, cursor: String) throws {
        try execute(
            """
            INSERT INTO sync_cursors (peer_id, cursor, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(peer_id) DO UPDATE SET
                cursor = excluded.cursor,
                updated_at = excluded.updated_at
            """,
            [
                .text(peerID),
                .text(cursor),
                .text(DateCoding.encode(Date()))
            ]
        )
    }

    func appendAcknowledgedRemoteLedgerPage(_ entries: [OperationLedgerEntry], peerID: String, cursor: String) throws {
        try transaction {
            for entry in entries {
                if let existing = try ledgerEntry(opID: entry.opID) {
                    guard Self.isSameLedgerPayload(existing, entry) else {
                        throw DatabaseError.stepFailed("operation_ledger.op_id 冲突：\(entry.opID.uuidString)")
                    }
                    try execute(
                        "UPDATE operation_ledger SET upload_status = ?, global_seq = COALESCE(?, global_seq), remote_cursor = ? WHERE op_id = ?",
                        [
                            .text(LedgerUploadStatus.acknowledged.rawValue),
                            .nullableInt(entry.globalSeq),
                            .text(cursor),
                            .text(entry.opID.uuidString)
                        ]
                    )
                    try execute("DELETE FROM sync_upload_queue WHERE op_id = ?", [.text(entry.opID.uuidString)])
                    continue
                }
                try appendLedgerEntryBody(entry, uploadStatus: .acknowledged, remoteCursor: cursor)
            }
            try setSyncCursor(peerID: peerID, cursor: cursor)
        }
    }

    private static func isSameLedgerPayload(_ lhs: OperationLedgerEntry, _ rhs: OperationLedgerEntry) -> Bool {
        lhs.opID == rhs.opID &&
        lhs.libraryID == rhs.libraryID &&
        lhs.deviceID == rhs.deviceID &&
        lhs.deviceSequence == rhs.deviceSequence &&
        lhs.hybridLogicalTime == rhs.hybridLogicalTime &&
        lhs.actorID == rhs.actorID &&
        lhs.entityType == rhs.entityType &&
        lhs.entityID == rhs.entityID &&
        lhs.opType == rhs.opType &&
        lhs.payload == rhs.payload &&
        lhs.baseVersion == rhs.baseVersion
    }

    private func decodeLedgerEntry(_ statement: SQLiteStatement) throws -> OperationLedgerEntry {
        guard let opID = UUID(uuidString: statement.text(0)) else {
            throw DatabaseError.stepFailed("operation_ledger.op_id 不是 UUID：\(statement.text(0))")
        }
        let entityTypeRaw = statement.text(6)
        guard let entityType = LedgerEntityType(rawValue: entityTypeRaw) else {
            throw DatabaseError.stepFailed("operation_ledger.entity_type 无效：\(entityTypeRaw)")
        }
        let opTypeRaw = statement.text(8)
        guard let opType = LedgerOperationType(rawValue: opTypeRaw) else {
            throw DatabaseError.stepFailed("operation_ledger.op_type 无效：\(opTypeRaw)")
        }
        let createdAtRaw = statement.text(11)
        guard let createdAt = DateCoding.decode(createdAtRaw) else {
            throw DatabaseError.stepFailed("operation_ledger.created_at 无效：\(createdAtRaw)")
        }
        return OperationLedgerEntry(
            opID: opID,
            globalSeq: statement.optionalInt64(12),
            libraryID: statement.text(1),
            deviceID: SyncDeviceID(statement.text(2)),
            deviceSequence: statement.int64(3),
            hybridLogicalTime: try LedgerSQLiteCoding.decodeTime(statement.text(4)),
            actorID: statement.text(5),
            entityType: entityType,
            entityID: statement.text(7),
            opType: opType,
            payload: try LedgerSQLiteCoding.decodePayload(statement.text(9)),
            baseVersion: statement.optionalText(10),
            createdAt: createdAt
        )
    }

    func nextLedgerDeviceSequence(libraryID: String, deviceID: SyncDeviceID) throws -> Int64 {
        let rows = try prepare(
            """
            SELECT COALESCE(MAX(device_seq), 0) + 1
            FROM operation_ledger
            WHERE library_id = ?
              AND device_id = ?
            """,
            [.text(libraryID), .text(deviceID.rawValue)]
        ) { statement in
            statement.int64(0)
        }
        return rows.first ?? 1
    }

    private func appendLedgerEntryBody(_ entry: OperationLedgerEntry, uploadStatus: LedgerUploadStatus, remoteCursor: String? = nil) throws {
        let payloadJSON = try LedgerSQLiteCoding.encodePayload(entry.payload)
        let now = DateCoding.encode(Date())
        try execute(
            """
            INSERT INTO operation_ledger (
                op_id, library_id, device_id, device_seq, hybrid_logical_time,
                actor_id, entity_type, entity_id, op_type, payload_json,
                base_version, created_at, upload_status, remote_cursor, global_seq
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                .text(entry.opID.uuidString),
                .text(entry.libraryID),
                .text(entry.deviceID.rawValue),
                .int(entry.deviceSequence),
                .text(LedgerSQLiteCoding.encodeTime(entry.hybridLogicalTime)),
                .text(entry.actorID),
                .text(entry.entityType.rawValue),
                .text(entry.entityID),
                .text(entry.opType.rawValue),
                .text(payloadJSON),
                .nullableText(entry.baseVersion),
                .text(DateCoding.encode(entry.createdAt)),
                .text(uploadStatus.rawValue),
                .nullableText(remoteCursor),
                .nullableInt(entry.globalSeq)
            ]
        )
        if uploadStatus == .pending {
            try execute(
                """
                INSERT OR IGNORE INTO sync_upload_queue (op_id, status, attempt_count, last_error, updated_at)
                VALUES (?, ?, 0, '', ?)
                """,
                [.text(entry.opID.uuidString), .text(LedgerUploadStatus.pending.rawValue), .text(now)]
            )
        } else {
            try execute(
                "UPDATE operation_ledger SET upload_status = ? WHERE op_id = ?",
                [.text(uploadStatus.rawValue), .text(entry.opID.uuidString)]
            )
            try execute("DELETE FROM sync_upload_queue WHERE op_id = ?", [.text(entry.opID.uuidString)])
        }
        try applyLedgerSideTables(entry)
        try recordLedgerClockState(libraryID: entry.libraryID, deviceID: entry.deviceID, time: entry.hybridLogicalTime)
    }

    private func reserveLedgerClock(
        libraryID: String,
        deviceID: SyncDeviceID,
        currentWallTimeMilliseconds: Int64
    ) throws -> (deviceSequence: Int64, hybridLogicalTime: HybridLogicalTime) {
        let sequence = try nextLedgerDeviceSequence(libraryID: libraryID, deviceID: deviceID)
        let last = try fetchLedgerClockState(libraryID: libraryID, deviceID: deviceID)
        let wallTime = max(currentWallTimeMilliseconds, last.wallTimeMilliseconds)
        let counter: Int64 = currentWallTimeMilliseconds <= last.wallTimeMilliseconds ? last.counter + 1 : 0
        let time = HybridLogicalTime(wallTimeMilliseconds: wallTime, counter: counter, nodeID: deviceID.rawValue)
        try recordLedgerClockState(libraryID: libraryID, deviceID: deviceID, time: time)
        return (sequence, time)
    }

    private func fetchLedgerClockState(libraryID: String, deviceID: SyncDeviceID) throws -> HybridLogicalTime {
        let rows = try prepare(
            """
            SELECT last_wall_time_ms, last_counter
            FROM sync_hlc_state
            WHERE library_id = ? AND device_id = ?
            """,
            [.text(libraryID), .text(deviceID.rawValue)]
        ) { statement in
            HybridLogicalTime(
                wallTimeMilliseconds: statement.int64(0),
                counter: statement.int64(1),
                nodeID: deviceID.rawValue
            )
        }
        return rows.first ?? HybridLogicalTime(wallTimeMilliseconds: 0, counter: 0, nodeID: deviceID.rawValue)
    }

    private func recordLedgerClockState(
        libraryID: String,
        deviceID: SyncDeviceID,
        time: HybridLogicalTime
    ) throws {
        let now = DateCoding.encode(Date())
        try execute(
            """
            INSERT INTO sync_hlc_state (
                library_id, device_id, last_wall_time_ms, last_counter, updated_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(library_id, device_id) DO UPDATE SET
                last_wall_time_ms = CASE
                    WHEN excluded.last_wall_time_ms > sync_hlc_state.last_wall_time_ms THEN excluded.last_wall_time_ms
                    ELSE sync_hlc_state.last_wall_time_ms
                END,
                last_counter = CASE
                    WHEN excluded.last_wall_time_ms > sync_hlc_state.last_wall_time_ms THEN excluded.last_counter
                    WHEN excluded.last_wall_time_ms = sync_hlc_state.last_wall_time_ms AND excluded.last_counter > sync_hlc_state.last_counter THEN excluded.last_counter
                    ELSE sync_hlc_state.last_counter
                END,
                updated_at = excluded.updated_at
            """,
            [
                .text(libraryID),
                .text(deviceID.rawValue),
                .int(time.wallTimeMilliseconds),
                .int(time.counter),
                .text(now)
            ]
        )
    }

    func updateAssetMetadata(asset: Asset) throws {
        try execute(
            """
            UPDATE assets
            SET rating = ?, flag_state = ?, flag = ?, color_label = ?, tags = ?, updated_at = ?
            WHERE id = ?
            """,
            [
                .int(Int64(asset.rating)),
                .text(asset.flagState.rawValue),
                .int(asset.flagState == .picked ? 1 : 0),
                .nullableText(asset.colorLabel?.rawValue),
                .text(encodeTags(asset.tags)),
                .text(DateCoding.encode(Date())),
                .text(asset.id.uuidString)
            ]
        )
    }

    private func applyLedgerSideTables(_ entry: OperationLedgerEntry) throws {
        let now = DateCoding.encode(Date())
        switch entry.payload {
        case let .assetSnapshotDeclared(snapshot):
            try upsertProjectedAssetSnapshot(snapshot)
        case let .filePlacementSnapshotDeclared(_, fileObject, placement):
            try upsertFileObject(fileObject, now: now)
            try upsertFilePlacement(placement, now: now)
        case let .moveToTrash(assetID, reason):
            try execute(
                """
                INSERT INTO asset_trash_states (
                    asset_id, state, reason, changed_by, changed_at_hlc, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(asset_id) DO UPDATE SET
                    state = excluded.state,
                    reason = excluded.reason,
                    changed_by = excluded.changed_by,
                    changed_at_hlc = excluded.changed_at_hlc,
                    updated_at = excluded.updated_at
                """,
                [
                    .text(assetID.uuidString),
                    .text(ProjectedTrashState.trashed.rawValue),
                    .text(reason),
                    .text(entry.actorID),
                    .text(LedgerSQLiteCoding.encodeTime(entry.hybridLogicalTime)),
                    .text(now)
                ]
            )
        case let .restoreFromTrash(assetID):
            try execute(
                """
                INSERT INTO asset_trash_states (
                    asset_id, state, reason, changed_by, changed_at_hlc, updated_at
                ) VALUES (?, ?, '', ?, ?, ?)
                ON CONFLICT(asset_id) DO UPDATE SET
                    state = excluded.state,
                    reason = excluded.reason,
                    changed_by = excluded.changed_by,
                    changed_at_hlc = excluded.changed_at_hlc,
                    updated_at = excluded.updated_at
                """,
                [
                    .text(assetID.uuidString),
                    .text(ProjectedTrashState.active.rawValue),
                    .text(entry.actorID),
                    .text(LedgerSQLiteCoding.encodeTime(entry.hybridLogicalTime)),
                    .text(now)
                ]
            )
        case let .importedOriginalDeclared(_, fileObject, placement):
            try upsertFileObject(fileObject, now: now)
            try upsertFilePlacement(placement, now: now)
        case let .originalArchiveReceiptRecorded(_, fileObject, serverPlacement):
            try upsertFileObject(fileObject, now: now)
            try upsertFilePlacement(serverPlacement, now: now)
        case let .derivativeDeclared(_, derivative):
            try upsertFileObject(derivative.fileObject, now: now)
            try upsertDerivative(derivative, now: now)
            try upsertFilePlacement(
                FilePlacement(
                    fileObjectID: derivative.fileObject,
                    holderID: derivative.s3Object.bucket,
                    storageKind: .cloudPreview,
                    authorityRole: .canonical,
                    availability: .online
                ),
                now: now
            )
        case let .metadataSet(assetID, field, value):
            try updateProjectedAssetMetadata(assetID: assetID, field: field, value: value, now: now)
        case let .tagsUpdated(assetID, add, remove):
            try updateProjectedAssetTags(assetID: assetID, add: add, remove: remove, now: now)
        case .archiveRequested:
            break
        }
    }

    private static let ledgerUploadClaimLeaseDuration: TimeInterval = 5 * 60

    private func upsertFileObject(_ fileObject: FileObjectID, now: String) throws {
        try execute(
            """
            INSERT OR IGNORE INTO file_objects (id, content_hash, size_bytes, file_role, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                .text(fileObject.stableKey),
                .text(fileObject.contentHash),
                .int(fileObject.sizeBytes),
                .text(fileObject.role.rawValue),
                .text(now)
            ]
        )
    }

    private func upsertFilePlacement(_ placement: FilePlacement, now: String) throws {
        try execute(
            """
            INSERT INTO file_placements (
                file_object_id, holder_id, storage_kind, authority_role, availability, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(file_object_id, holder_id, storage_kind, authority_role) DO UPDATE SET
                availability = excluded.availability,
                updated_at = excluded.updated_at
            """,
            [
                .text(placement.fileObjectID.stableKey),
                .text(placement.holderID),
                .text(placement.storageKind.rawValue),
                .text(placement.authorityRole.rawValue),
                .text(placement.availability.rawValue),
                .text(now)
            ]
        )
    }

    private func upsertDerivative(_ derivative: DerivativeObject, now: String) throws {
        try execute(
            """
            INSERT INTO derivative_objects (
                asset_id, role, file_object_id, s3_bucket, s3_key, s3_etag,
                pixel_width, pixel_height, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(asset_id, role) DO UPDATE SET
                file_object_id = excluded.file_object_id,
                s3_bucket = excluded.s3_bucket,
                s3_key = excluded.s3_key,
                s3_etag = excluded.s3_etag,
                pixel_width = excluded.pixel_width,
                pixel_height = excluded.pixel_height,
                updated_at = excluded.updated_at
            """,
            [
                .text(derivative.assetID.uuidString),
                .text(derivative.role.rawValue),
                .text(derivative.fileObject.stableKey),
                .text(derivative.s3Object.bucket),
                .text(derivative.s3Object.key),
                .nullableText(derivative.s3Object.eTag),
                .int(Int64(derivative.pixelSize.width)),
                .int(Int64(derivative.pixelSize.height)),
                .text(now)
            ]
        )
    }

    private func upsertProjectedAssetSnapshot(_ snapshot: AssetSnapshot) throws {
        try execute(
            """
            INSERT INTO assets (
                id, capture_time, camera_make, camera_model, lens_model, original_filename,
                content_fingerprint, metadata_fingerprint, rating, flag, flag_state, color_label,
                tags, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                capture_time = excluded.capture_time,
                camera_make = excluded.camera_make,
                camera_model = excluded.camera_model,
                lens_model = excluded.lens_model,
                original_filename = excluded.original_filename,
                content_fingerprint = excluded.content_fingerprint,
                metadata_fingerprint = excluded.metadata_fingerprint,
                rating = excluded.rating,
                flag = excluded.flag,
                flag_state = excluded.flag_state,
                color_label = excluded.color_label,
                tags = excluded.tags,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at
            """,
            [
                .text(snapshot.assetID.uuidString),
                .nullableText(snapshot.captureTime.map(DateCoding.encode)),
                .text(snapshot.cameraMake),
                .text(snapshot.cameraModel),
                .text(snapshot.lensModel),
                .text(snapshot.originalFilename),
                .text(snapshot.contentFingerprint),
                .text(snapshot.metadataFingerprint),
                .int(Int64(snapshot.rating)),
                .int(snapshot.flagState == .picked ? 1 : 0),
                .text(snapshot.flagState.rawValue),
                .nullableText(snapshot.colorLabel?.rawValue),
                .text(encodeTags(snapshot.tags)),
                .text(DateCoding.encode(snapshot.createdAt)),
                .text(DateCoding.encode(snapshot.updatedAt))
            ]
        )
    }

    private func updateProjectedAssetMetadata(
        assetID: UUID,
        field: AssetMetadataField,
        value: LedgerValue,
        now: String
    ) throws {
        switch field {
        case .rating:
            guard case let .int(rating) = value else { return }
            try execute(
                "UPDATE assets SET rating = ?, updated_at = ? WHERE id = ?",
                [.int(Int64(rating)), .text(now), .text(assetID.uuidString)]
            )
        case .flagState:
            guard case let .string(rawValue) = value, let flagState = AssetFlagState(rawValue: rawValue) else {
                return
            }
            try execute(
                "UPDATE assets SET flag_state = ?, flag = ?, updated_at = ? WHERE id = ?",
                [
                    .text(flagState.rawValue),
                    .int(flagState == .picked ? 1 : 0),
                    .text(now),
                    .text(assetID.uuidString)
                ]
            )
        case .colorLabel:
            let colorLabelRaw: String?
            switch value {
            case let .string(rawValue):
                guard let colorLabel = AssetColorLabel(rawValue: rawValue) else {
                    throw DatabaseError.stepFailed("不支持的 color_label：\(rawValue)")
                }
                colorLabelRaw = colorLabel.rawValue
            case .null:
                colorLabelRaw = nil
            case .int:
                return
            }
            try execute(
                "UPDATE assets SET color_label = ?, updated_at = ? WHERE id = ?",
                [.nullableText(colorLabelRaw), .text(now), .text(assetID.uuidString)]
            )
        case .caption:
            break
        }
    }

    private func updateProjectedAssetTags(
        assetID: UUID,
        add: Set<String>,
        remove: Set<String>,
        now: String
    ) throws {
        let rows = try prepare(
            "SELECT tags FROM assets WHERE id = ? LIMIT 1",
            [.text(assetID.uuidString)]
        ) { statement in
            decodeTags(statement.text(0))
        }
        guard let existingTags = rows.first else { return }

        var nextTags = Set(existingTags)
        nextTags.subtract(remove)
        nextTags.formUnion(add)

        try execute(
            "UPDATE assets SET tags = ?, updated_at = ? WHERE id = ?",
            [.text(encodeTags(nextTags.sorted())), .text(now), .text(assetID.uuidString)]
        )
    }

    func markFileStatus(id: UUID, syncStatus: SyncStatus, authorityRole: AuthorityRole? = nil) throws {
        if let authorityRole {
            try execute(
                "UPDATE file_instances SET sync_status = ?, authority_role = ? WHERE id = ?",
                [.text(syncStatus.rawValue), .text(authorityRole.rawValue), .text(id.uuidString)]
            )
        } else {
            try execute("UPDATE file_instances SET sync_status = ? WHERE id = ?", [.text(syncStatus.rawValue), .text(id.uuidString)])
        }
    }

    func insertCopiedFile(assetID: UUID, source: FileInstance, destination: URL, storageKind: StorageKind, authorityRole: AuthorityRole, syncStatus: SyncStatus, hash: String, sizeBytes: Int64) throws {
        try execute(
            """
            INSERT INTO file_instances (
                id, asset_id, path, device_id, storage_kind, file_role, authority_role,
                sync_status, size_bytes, content_hash, last_seen_at, availability
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                content_hash = excluded.content_hash,
                size_bytes = excluded.size_bytes,
                last_seen_at = excluded.last_seen_at,
                availability = excluded.availability,
                sync_status = excluded.sync_status,
                authority_role = excluded.authority_role
            """,
            [
                .text(UUID().uuidString),
                .text(assetID.uuidString),
                .text(destination.path),
                .text(currentDeviceID()),
                .text(storageKind.rawValue),
                .text(source.fileRole.rawValue),
                .text(authorityRole.rawValue),
                .text(syncStatus.rawValue),
                .int(sizeBytes),
                .text(hash),
                .text(DateCoding.encode(Date())),
                .text(Availability.online.rawValue)
            ]
        )
    }

    #if os(macOS)
    func insertExport(assetID: UUID, exportURL: URL, sourceVersionID: UUID?) throws {
        let hash = (try? FileHasher.sha256(url: exportURL)) ?? ""
        let size = (try? exportURL.resourceValues(forKeys: [.fileSizeKey]).fileSize).map(Int64.init) ?? 0
        let versionID = UUID()
        try insertVersion(assetID: assetID, name: exportURL.deletingPathExtension().lastPathComponent, kind: .export, parentID: sourceVersionID, notes: exportURL.path)
        try execute(
            """
            INSERT INTO file_instances (
                id, asset_id, path, device_id, storage_kind, file_role, authority_role,
                sync_status, size_bytes, content_hash, last_seen_at, availability
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                .text(versionID.uuidString),
                .text(assetID.uuidString),
                .text(exportURL.path),
                .text(currentDeviceID()),
                .text(StorageKind.local.rawValue),
                .text(FileRole.export.rawValue),
                .text(AuthorityRole.workingCopy.rawValue),
                .text(SyncStatus.needsSync.rawValue),
                .int(size),
                .text(hash),
                .text(DateCoding.encode(Date())),
                .text(Availability.online.rawValue)
            ]
        )
    }
    #endif

    func writeOperation(action: String, source: String?, destination: String?, status: String, detail: String) throws {
        try execute(
            """
            INSERT INTO operation_logs (id, action, source_path, destination_path, status, detail, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                .text(UUID().uuidString),
                .text(action),
                .nullableText(source),
                .nullableText(destination),
                .text(status),
                .text(detail),
                .text(DateCoding.encode(Date()))
            ]
        )
    }

    func markMissingFiles() throws {
        let rows = try prepare("SELECT id, path FROM file_instances WHERE file_role IN ('raw_original','jpeg_original','sidecar','export')", []) { statement in
            (UUID(uuidString: statement.text(0)), statement.text(1))
        }
        for row in rows {
            guard let id = row.0 else { continue }
            let available = FileManager.default.fileExists(atPath: row.1)
            try execute("UPDATE file_instances SET availability = ? WHERE id = ?", [.text(available ? Availability.online.rawValue : Availability.missing.rawValue), .text(id.uuidString)])
        }
    }

    func availabilityCheckTargets() throws -> [AvailabilityCheckTarget] {
        try prepare(
            "SELECT id, path FROM file_instances WHERE file_role IN ('raw_original','jpeg_original','sidecar','export') ORDER BY path",
            []
        ) { statement in
            guard let id = UUID(uuidString: statement.text(0)) else {
                throw DatabaseError.stepFailed("文件实例 ID 格式无效：\(statement.text(0))")
            }
            return AvailabilityCheckTarget(
                id: id,
                path: statement.text(1)
            )
        }
    }

    func updateFileAvailability(_ updates: [FileAvailabilityUpdate]) throws {
        guard !updates.isEmpty else { return }
        try execute("BEGIN IMMEDIATE TRANSACTION")
        do {
            let groupedUpdates = Dictionary(grouping: updates, by: \.availability)
            for (availability, group) in groupedUpdates {
                let placeholders = Array(repeating: "?", count: group.count).joined(separator: ", ")
                let values = [.text(availability.rawValue)] + group.map { SQLiteValue.text($0.id.uuidString) }
                try execute(
                    "UPDATE file_instances SET availability = ? WHERE id IN (\(placeholders))",
                    values
                )
            }
            try execute("COMMIT")
        } catch {
            try? execute("ROLLBACK")
            throw error
        }
    }

    private func migrate() throws {
        try execute(
            """
            CREATE TABLE IF NOT EXISTS assets (
                id TEXT PRIMARY KEY,
                capture_time TEXT,
                camera_make TEXT NOT NULL,
                camera_model TEXT NOT NULL,
                lens_model TEXT NOT NULL,
                original_filename TEXT NOT NULL,
                content_fingerprint TEXT NOT NULL,
                metadata_fingerprint TEXT NOT NULL,
                rating INTEGER NOT NULL DEFAULT 0,
                flag INTEGER NOT NULL DEFAULT 0,
                flag_state TEXT NOT NULL DEFAULT 'unflagged',
                color_label TEXT,
                tags TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS file_instances (
                id TEXT PRIMARY KEY,
                asset_id TEXT NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
                path TEXT NOT NULL UNIQUE,
                device_id TEXT NOT NULL,
                storage_kind TEXT NOT NULL,
                file_role TEXT NOT NULL,
                authority_role TEXT NOT NULL,
                sync_status TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                content_hash TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                availability TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_file_instances_asset ON file_instances(asset_id);
            CREATE INDEX IF NOT EXISTS idx_file_instances_hash ON file_instances(content_hash);
            CREATE INDEX IF NOT EXISTS idx_file_instances_asset_role_availability ON file_instances(asset_id, file_role, availability);
            CREATE INDEX IF NOT EXISTS idx_file_instances_asset_sync ON file_instances(asset_id, sync_status);
            CREATE INDEX IF NOT EXISTS idx_file_instances_asset_storage_authority ON file_instances(asset_id, storage_kind, authority_role);
            CREATE INDEX IF NOT EXISTS idx_file_instances_role_path ON file_instances(file_role, path);
            CREATE INDEX IF NOT EXISTS idx_assets_sort_time ON assets(COALESCE(capture_time, created_at) DESC);

            CREATE TABLE IF NOT EXISTS versions (
                id TEXT PRIMARY KEY,
                asset_id TEXT NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                version_kind TEXT NOT NULL,
                parent_version_id TEXT,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                notes TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS collections (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                collection_kind TEXT NOT NULL,
                description TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS collection_assets (
                collection_id TEXT NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
                asset_id TEXT NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
                PRIMARY KEY (collection_id, asset_id)
            );

            CREATE TABLE IF NOT EXISTS import_batches (
                id TEXT PRIMARY KEY,
                source_path TEXT NOT NULL,
                device_id TEXT NOT NULL,
                imported_at TEXT NOT NULL,
                imported_by TEXT NOT NULL,
                status TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS source_directories (
                id TEXT PRIMARY KEY,
                path TEXT NOT NULL UNIQUE,
                storage_kind TEXT NOT NULL,
                is_tracked INTEGER NOT NULL,
                parent_source_directory_id TEXT,
                created_at TEXT NOT NULL,
                last_scanned_at TEXT
            );

            CREATE TABLE IF NOT EXISTS operation_logs (
                id TEXT PRIMARY KEY,
                action TEXT NOT NULL,
                source_path TEXT,
                destination_path TEXT,
                status TEXT NOT NULL,
                detail TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS operation_ledger (
                op_id TEXT PRIMARY KEY,
                library_id TEXT NOT NULL,
                device_id TEXT NOT NULL,
                device_seq INTEGER NOT NULL,
                hybrid_logical_time TEXT NOT NULL,
                actor_id TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                op_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                base_version TEXT,
                created_at TEXT NOT NULL,
                upload_status TEXT NOT NULL DEFAULT 'pending',
                remote_cursor TEXT,
                global_seq INTEGER
            );

            CREATE TABLE IF NOT EXISTS sync_cursors (
                peer_id TEXT PRIMARY KEY,
                cursor TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sync_hlc_state (
                library_id TEXT NOT NULL,
                device_id TEXT NOT NULL,
                last_wall_time_ms INTEGER NOT NULL,
                last_counter INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(library_id, device_id)
            );

            CREATE TABLE IF NOT EXISTS sync_upload_queue (
                op_id TEXT PRIMARY KEY REFERENCES operation_ledger(op_id) ON DELETE CASCADE,
                status TEXT NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS file_objects (
                id TEXT PRIMARY KEY,
                content_hash TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                file_role TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS file_placements (
                file_object_id TEXT NOT NULL REFERENCES file_objects(id) ON DELETE CASCADE,
                holder_id TEXT NOT NULL,
                storage_kind TEXT NOT NULL,
                authority_role TEXT NOT NULL,
                availability TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(file_object_id, holder_id, storage_kind, authority_role)
            );

            CREATE TABLE IF NOT EXISTS derivative_objects (
                asset_id TEXT NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
                role TEXT NOT NULL,
                file_object_id TEXT NOT NULL REFERENCES file_objects(id) ON DELETE CASCADE,
                s3_bucket TEXT NOT NULL,
                s3_key TEXT NOT NULL,
                s3_etag TEXT,
                pixel_width INTEGER NOT NULL,
                pixel_height INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(asset_id, role)
            );

            CREATE TABLE IF NOT EXISTS asset_trash_states (
                asset_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                reason TEXT NOT NULL,
                changed_by TEXT NOT NULL,
                changed_at_hlc TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sync_migration_state (
                library_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                source_database_fingerprint TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                ledger_high_watermark INTEGER NOT NULL DEFAULT 0,
                projection_verified INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS browse_nodes (
                id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                canonical_key TEXT NOT NULL,
                display_name TEXT NOT NULL,
                display_path TEXT NOT NULL,
                storage_kind TEXT NOT NULL,
                UNIQUE(kind, canonical_key)
            );

            CREATE TABLE IF NOT EXISTS browse_edges (
                parent_node_id TEXT NOT NULL REFERENCES browse_nodes(id) ON DELETE CASCADE,
                child_node_id TEXT NOT NULL REFERENCES browse_nodes(id) ON DELETE CASCADE,
                kind TEXT NOT NULL,
                PRIMARY KEY(parent_node_id, child_node_id, kind)
            );

            CREATE TABLE IF NOT EXISTS browse_file_instances (
                node_id TEXT NOT NULL REFERENCES browse_nodes(id) ON DELETE CASCADE,
                file_instance_id TEXT NOT NULL REFERENCES file_instances(id) ON DELETE CASCADE,
                membership_kind TEXT NOT NULL,
                PRIMARY KEY(node_id, file_instance_id, membership_kind)
            );

            CREATE TABLE IF NOT EXISTS folder_move_jobs (
                id TEXT PRIMARY KEY,
                source_directory_id TEXT NOT NULL REFERENCES source_directories(id),
                source_path TEXT NOT NULL,
                destination_parent_path TEXT NOT NULL,
                destination_path TEXT NOT NULL,
                storage_kind TEXT NOT NULL,
                status TEXT NOT NULL,
                total_files INTEGER NOT NULL,
                completed_files INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                error_detail TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS folder_move_items (
                id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL REFERENCES folder_move_jobs(id) ON DELETE CASCADE,
                file_instance_id TEXT,
                source_path TEXT NOT NULL,
                destination_path TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_browse_edges_parent ON browse_edges(parent_node_id, kind);
            CREATE INDEX IF NOT EXISTS idx_browse_edges_child ON browse_edges(child_node_id, kind);
            CREATE INDEX IF NOT EXISTS idx_browse_file_instances_node ON browse_file_instances(node_id, membership_kind);
            CREATE INDEX IF NOT EXISTS idx_browse_file_instances_file ON browse_file_instances(file_instance_id);
            CREATE INDEX IF NOT EXISTS idx_folder_move_jobs_status ON folder_move_jobs(status, created_at);
            CREATE INDEX IF NOT EXISTS idx_folder_move_items_job_status ON folder_move_items(job_id, status);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_operation_ledger_device_seq ON operation_ledger(library_id, device_id, device_seq);
            CREATE INDEX IF NOT EXISTS idx_operation_ledger_entity ON operation_ledger(entity_type, entity_id, hybrid_logical_time);
            CREATE INDEX IF NOT EXISTS idx_operation_ledger_upload ON operation_ledger(upload_status, created_at);
            CREATE INDEX IF NOT EXISTS idx_file_placements_holder ON file_placements(holder_id, availability);
            CREATE INDEX IF NOT EXISTS idx_derivative_objects_file_object ON derivative_objects(file_object_id);
            """
        )

        try execute(
            """
            INSERT OR IGNORE INTO app_settings (key, value)
            VALUES ('last_availability_refresh_at', ?)
            """,
            [.text(DateCoding.encode(Date()))]
        )

        try addColumnIfNeeded(
            table: "source_directories",
            column: "parent_source_directory_id",
            definition: "TEXT"
        )
        try addColumnIfNeeded(table: "assets", column: "color_label", definition: "TEXT")
        try addColumnIfNeeded(table: "assets", column: "flag_state", definition: "TEXT")
        try addColumnIfNeeded(table: "operation_ledger", column: "global_seq", definition: "INTEGER")
        try execute(
            """
            UPDATE assets
            SET flag_state = CASE WHEN flag = 1 THEN 'picked' ELSE 'unflagged' END
            WHERE flag_state IS NULL OR flag_state = ''
            """
        )

        try execute(
            """
            INSERT OR IGNORE INTO source_directories (id, path, storage_kind, is_tracked, created_at, last_scanned_at)
            SELECT
                id,
                source_path,
                CASE WHEN source_path LIKE '/Volumes/%' THEN 'nas' ELSE 'local' END,
                1,
                imported_at,
                imported_at
            FROM import_batches
            WHERE status IN ('finished', 'finished_with_errors', 'resumed')
              AND NOT EXISTS (
                  SELECT 1
                  FROM source_directories ancestor
                  WHERE ancestor.is_tracked = 1
                    AND source_path LIKE ancestor.path || '/%'
              )
            """
        )
        try pruneNestedImportBatchSourceDirectories()
        try deduplicateAssetThumbnails()
        try execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_file_instances_one_thumbnail_per_asset
            ON file_instances(asset_id, file_role)
            WHERE file_role = 'thumbnail'
            """
        )
        try backfillBrowseGraphFromFileInstances()
    }

    func pruneNestedImportBatchSourceDirectories() throws {
        try execute(
            """
            DELETE FROM source_directories
            WHERE id IN (
                SELECT sd.id
                FROM source_directories sd
                JOIN import_batches ib
                  ON ib.id = sd.id
                 AND ib.source_path = sd.path
                WHERE EXISTS (
                    SELECT 1
                    FROM source_directories ancestor
                    WHERE ancestor.id <> sd.id
                      AND ancestor.is_tracked = 1
                      AND sd.path LIKE ancestor.path || '/%'
                )
            )
            """
        )
    }

    func deduplicateAssetThumbnails() throws {
        try execute(
            """
            DELETE FROM file_instances
            WHERE id IN (
                SELECT id
                FROM (
                    SELECT
                        id,
                        ROW_NUMBER() OVER (
                            PARTITION BY asset_id
                            ORDER BY
                                CASE
                                    WHEN EXISTS (
                                        SELECT 1
                                        FROM file_instances original
                                        WHERE original.asset_id = ranked.asset_id
                                          AND original.file_role = 'jpeg_original'
                                          AND ranked.path LIKE '%' || original.content_hash || '-320.jpg'
                                    ) THEN 0
                                    ELSE 1
                                END,
                                path
                        ) AS rank
                    FROM file_instances ranked
                    WHERE file_role = 'thumbnail'
                )
                WHERE rank > 1
            )
            """
        )
    }

    func backfillBrowseGraphFromFileInstances() throws {
        let rows = try prepare(
            """
            SELECT fi.id, fi.path, fi.storage_kind
            FROM file_instances fi
            LEFT JOIN browse_file_instances bfi
              ON bfi.file_instance_id = fi.id
             AND bfi.membership_kind = 'direct_file_instance'
            WHERE bfi.file_instance_id IS NULL
              AND fi.file_role IN ('raw_original', 'jpeg_original', 'sidecar', 'export')
            ORDER BY fi.path
            """,
            []
        ) { statement in
            (
                UUID(uuidString: statement.text(0)),
                statement.text(1),
                StorageKind(rawValue: statement.text(2)) ?? .local
            )
        }

        guard !rows.isEmpty else { return }
        try execute("BEGIN IMMEDIATE TRANSACTION")
        do {
            for row in rows {
                guard let fileInstanceID = row.0 else { continue }
                try upsertBrowseFolderMembership(filePath: row.1, fileInstanceID: fileInstanceID, storageKind: row.2)
            }
            try execute("COMMIT")
        } catch {
            try? execute("ROLLBACK")
            throw error
        }
    }

    func rebuildBrowseGraph() throws {
        try execute("BEGIN IMMEDIATE TRANSACTION")
        do {
            try execute("DELETE FROM browse_file_instances")
            try execute("DELETE FROM browse_edges")
            try execute("DELETE FROM browse_nodes")
            try execute("COMMIT")
        } catch {
            try? execute("ROLLBACK")
            throw error
        }
        try backfillBrowseGraphFromFileInstances()
    }

    private func assetID(contentHash: String, metadataFingerprint: String) throws -> UUID? {
        let sql = "SELECT id FROM assets WHERE content_fingerprint = ? OR metadata_fingerprint = ? LIMIT 1"
        return try prepare(sql, [.text(contentHash), .text(metadataFingerprint)]) { statement in
            UUID(uuidString: statement.text(0))
        }.first ?? nil
    }

    private func assetID(fileInstanceID: UUID) throws -> UUID? {
        try prepare(
            "SELECT asset_id FROM file_instances WHERE id = ? LIMIT 1",
            [.text(fileInstanceID.uuidString)]
        ) { statement in
            UUID(uuidString: statement.text(0))
        }.first ?? nil
    }

    private func folderMoveJob(id: UUID) throws -> FolderMoveJob {
        let rows = try prepare(
            """
            SELECT id, source_directory_id, source_path, destination_parent_path, destination_path,
                   storage_kind, status, total_files, completed_files
            FROM folder_move_jobs
            WHERE id = ?
            LIMIT 1
            """,
            [.text(id.uuidString)]
        ) { statement in
            FolderMoveJob(
                id: UUID(uuidString: statement.text(0)) ?? id,
                sourceDirectoryID: UUID(uuidString: statement.text(1)) ?? UUID(),
                sourcePath: statement.text(2),
                destinationParentPath: statement.text(3),
                destinationPath: statement.text(4),
                storageKind: StorageKind(rawValue: statement.text(5)) ?? .local,
                status: statement.text(6),
                totalFiles: Int(statement.int(7)),
                completedFiles: Int(statement.int(8))
            )
        }
        guard let job = rows.first else {
            throw DatabaseError.stepFailed("文件夹移动任务不存在：\(id.uuidString)")
        }
        return job
    }

    private func storageKindForPath(_ path: String) -> StorageKind {
        path.hasPrefix("/Volumes/") ? .nas : .local
    }

    private func browseNode(kind: BrowseNodeKind, canonicalKey: String) throws -> BrowseNode {
        let rows = try prepare(
            """
            SELECT id, kind, canonical_key, display_name, display_path, storage_kind
            FROM browse_nodes
            WHERE kind = ? AND canonical_key = ?
            LIMIT 1
            """,
            [.text(kind.rawValue), .text(canonicalKey)]
        ) { statement in
            BrowseNode(
                id: UUID(uuidString: statement.text(0)) ?? UUID(),
                kind: BrowseNodeKind(rawValue: statement.text(1)) ?? .folder,
                canonicalKey: statement.text(2),
                displayName: statement.text(3),
                displayPath: statement.text(4),
                storageKind: StorageKind(rawValue: statement.text(5)) ?? .local
            )
        }
        guard let node = rows.first else {
            throw DatabaseError.stepFailed("浏览节点不存在：\(kind.rawValue) \(canonicalKey)")
        }
        return node
    }

    private func fileInstanceID(path: String) throws -> UUID? {
        let sql = "SELECT id FROM file_instances WHERE path = ? LIMIT 1"
        return try prepare(sql, [.text(path)]) { statement in
            UUID(uuidString: statement.text(0))
        }.first ?? nil
    }

    private func upsertAssetThumbnail(assetID: UUID, url: URL, hash: String, sizeBytes: Int64) throws {
        try upsertAssetDerivative(assetID: assetID, role: .thumbnail, url: url, hash: hash, sizeBytes: sizeBytes)
    }

    private func upsertAssetDerivative(assetID: UUID, role: FileRole, url: URL, hash: String, sizeBytes: Int64) throws {
        precondition(role == .thumbnail || role == .preview)
        try execute(
            """
            DELETE FROM file_instances
            WHERE asset_id = ?
              AND file_role = ?
              AND path <> ?
            """,
            [.text(assetID.uuidString), .text(role.rawValue), .text(url.path)]
        )
        try execute(
            """
            INSERT INTO file_instances (
                id, asset_id, path, device_id, storage_kind, file_role, authority_role,
                sync_status, size_bytes, content_hash, last_seen_at, availability
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                asset_id = excluded.asset_id,
                size_bytes = excluded.size_bytes,
                content_hash = excluded.content_hash,
                last_seen_at = excluded.last_seen_at,
                availability = excluded.availability
            """,
            [
                .text(UUID().uuidString),
                .text(assetID.uuidString),
                .text(url.path),
                .text(currentDeviceID()),
                .text(StorageKind.local.rawValue),
                .text(role.rawValue),
                .text(AuthorityRole.cache.rawValue),
                .text(SyncStatus.cacheOnly.rawValue),
                .int(sizeBytes),
                .text(hash),
                .text(DateCoding.encode(Date())),
                .text(Availability.online.rawValue)
            ]
        )
    }

    private func insertVersion(assetID: UUID, name: String, kind: VersionKind, parentID: UUID?, notes: String) throws {
        try execute(
            """
            INSERT INTO versions (id, asset_id, name, version_kind, parent_version_id, created_by, created_at, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                .text(UUID().uuidString),
                .text(assetID.uuidString),
                .text(name),
                .text(kind.rawValue),
                .nullableText(parentID?.uuidString),
                .text(NSUserName()),
                .text(DateCoding.encode(Date())),
                .text(notes)
            ]
        )
    }

    private func statusSQL(_ status: AssetStatus?) -> String {
        guard let status else { return "" }
        switch status {
        case .inbox:
            return "NOT EXISTS (SELECT 1 FROM file_instances s WHERE s.asset_id = a.id AND s.storage_kind = 'nas')"
        case .working:
            return "EXISTS (SELECT 1 FROM file_instances s WHERE s.asset_id = a.id AND s.authority_role = 'working_copy')"
        case .needsArchive:
            return "EXISTS (SELECT 1 FROM file_instances s WHERE s.asset_id = a.id AND s.sync_status = 'needs_archive')"
        case .needsSync:
            return "EXISTS (SELECT 1 FROM file_instances s WHERE s.asset_id = a.id AND s.sync_status = 'needs_sync')"
        case .archived:
            return "EXISTS (SELECT 1 FROM file_instances s WHERE s.asset_id = a.id AND s.storage_kind = 'nas' AND s.authority_role = 'canonical')"
        case .missingOriginal:
            return "NOT EXISTS (SELECT 1 FROM file_instances s WHERE s.asset_id = a.id AND s.file_role IN ('raw_original','jpeg_original') AND s.availability = 'online')"
        }
    }

    private func assetStatus(
        hasOnlineOriginal: Bool,
        hasNeedsSync: Bool,
        hasNeedsArchive: Bool,
        hasArchivedCopy: Bool,
        hasWorkingCopy: Bool
    ) -> AssetStatus {
        if !hasOnlineOriginal {
            return .missingOriginal
        }
        if hasNeedsSync {
            return .needsSync
        }
        if hasNeedsArchive {
            return .needsArchive
        }
        if hasArchivedCopy {
            return .archived
        }
        if hasWorkingCopy {
            return .working
        }
        return .inbox
    }

    private func prepare<T>(_ sql: String, _ values: [SQLiteValue], row: (SQLiteStatement) throws -> T) throws -> [T] {
        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &statement, nil) == SQLITE_OK else {
            throw DatabaseError.prepareFailed(lastError)
        }
        defer { sqlite3_finalize(statement) }

        for (index, value) in values.enumerated() {
            guard value.bind(to: statement, index: Int32(index + 1)) == SQLITE_OK else {
                throw DatabaseError.bindFailed(lastError)
            }
        }

        var rows: [T] = []
        while true {
            let result = sqlite3_step(statement)
            if result == SQLITE_ROW {
                rows.append(try row(SQLiteStatement(statement: statement)))
            } else if result == SQLITE_DONE {
                return rows
            } else {
                throw DatabaseError.stepFailed(lastError)
            }
        }
    }

    private func execute(_ sql: String, _ values: [SQLiteValue]) throws {
        _ = try prepare(sql, values) { _ in () }
    }

    private func addColumnIfNeeded(table: String, column: String, definition: String) throws {
        let columns = try prepare("PRAGMA table_info(\(table))", []) { statement in
            statement.text(1)
        }
        guard !columns.contains(column) else { return }
        try execute("ALTER TABLE \(table) ADD COLUMN \(column) \(definition)", [])
    }
}

private enum LedgerSQLiteCoding {
    private static func makeEncoder() -> JSONEncoder {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        return encoder
    }

    private static func makeDecoder() -> JSONDecoder {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return decoder
    }

    static func encodePayload(_ payload: OperationPayload) throws -> String {
        let data = try makeEncoder().encode(payload)
        guard let json = String(data: data, encoding: .utf8) else {
            throw DatabaseError.bindFailed("ledger payload 不能编码为 UTF-8")
        }
        return json
    }

    static func decodePayload(_ json: String) throws -> OperationPayload {
        guard let data = json.data(using: .utf8) else {
            throw DatabaseError.stepFailed("ledger payload 不是 UTF-8")
        }
        return try makeDecoder().decode(OperationPayload.self, from: data)
    }

    static func encodeTime(_ time: HybridLogicalTime) -> String {
        let wall = String(format: "%020lld", time.wallTimeMilliseconds)
        let counter = String(format: "%020lld", time.counter)
        return "\(wall):\(counter):\(time.nodeID)"
    }

    static func decodeTime(_ value: String) throws -> HybridLogicalTime {
        let parts = value.split(separator: ":", maxSplits: 2).map(String.init)
        guard parts.count == 3,
              let wallTime = Int64(parts[0]),
              let counter = Int64(parts[1]) else {
            throw DatabaseError.stepFailed("operation_ledger.hybrid_logical_time 无效：\(value)")
        }
        return HybridLogicalTime(wallTimeMilliseconds: wallTime, counter: counter, nodeID: parts[2])
    }
}

enum SQLiteValue {
    case text(String)
    case nullableText(String?)
    case nullableInt(Int64?)
    case int(Int64)

    func bind(to statement: OpaquePointer?, index: Int32) -> Int32 {
        switch self {
        case .text(let value):
            return sqlite3_bind_text(statement, index, value, -1, SQLITE_TRANSIENT)
        case .nullableText(let value):
            guard let value else { return sqlite3_bind_null(statement, index) }
            return sqlite3_bind_text(statement, index, value, -1, SQLITE_TRANSIENT)
        case .nullableInt(let value):
            guard let value else { return sqlite3_bind_null(statement, index) }
            return sqlite3_bind_int64(statement, index, value)
        case .int(let value):
            return sqlite3_bind_int64(statement, index, value)
        }
    }
}

struct SQLiteStatement {
    let statement: OpaquePointer?

    func text(_ index: Int32) -> String {
        guard let cString = sqlite3_column_text(statement, index) else { return "" }
        return String(cString: cString)
    }

    func optionalText(_ index: Int32) -> String? {
        guard sqlite3_column_type(statement, index) != SQLITE_NULL else { return nil }
        return text(index)
    }

    func int(_ index: Int32) -> Int32 {
        sqlite3_column_int(statement, index)
    }

    func int64(_ index: Int32) -> Int64 {
        sqlite3_column_int64(statement, index)
    }

    func optionalInt64(_ index: Int32) -> Int64? {
        guard sqlite3_column_type(statement, index) != SQLITE_NULL else { return nil }
        return int64(index)
    }
}

private let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

func currentDeviceID() -> String {
    #if os(iOS)
    let defaults = UserDefaults.standard
    let key = "photo_asset_manager.installation_device_id"
    if let stored = defaults.string(forKey: key), !stored.isEmpty {
        return stored
    }
    let generated = "ios-\(UUID().uuidString)"
    defaults.set(generated, forKey: key)
    return generated
    #else
    Host.current().localizedName ?? Host.current().name ?? "mac"
    #endif
}

func encodeTags(_ tags: [String]) -> String {
    guard let data = try? JSONEncoder().encode(tags), let value = String(data: data, encoding: .utf8) else {
        return "[]"
    }
    return value
}

func decodeTags(_ value: String) -> [String] {
    guard let data = value.data(using: .utf8), let tags = try? JSONDecoder().decode([String].self, from: data) else {
        return []
    }
    return tags
}

private extension SQLiteDatabase {
    static func normalizedDirectoryPath(_ path: String) -> String {
        guard path.count > 1 else { return path }
        return path.hasSuffix("/") ? String(path.dropLast()) : path
    }

    static func parentDirectoryPath(ofFilePath path: String) -> String {
        let normalizedPath = normalizedDirectoryPath(path)
        guard normalizedPath != "/" else { return "/" }
        guard let separator = normalizedPath.lastIndex(of: "/") else { return "/" }
        if separator == normalizedPath.startIndex {
            return "/"
        }
        return String(normalizedPath[..<separator])
    }

    static func lastPathComponent(of path: String) -> String {
        let normalizedPath = normalizedDirectoryPath(path)
        guard normalizedPath != "/" else { return "/" }
        guard let separator = normalizedPath.lastIndex(of: "/") else { return normalizedPath }
        return String(normalizedPath[normalizedPath.index(after: separator)...])
    }

    static func ancestorDirectoryPaths(to folderPath: String) -> [String] {
        let normalizedPath = normalizedDirectoryPath(folderPath)
        guard normalizedPath.hasPrefix("/") else { return [normalizedPath] }
        let components = normalizedPath.split(separator: "/", omittingEmptySubsequences: true)

        var paths = ["/"]
        var current = ""
        for component in components {
            current += "/" + component
            paths.append(current)
        }
        return paths
    }
}
