import Foundation
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

@MainActor
final class SQLiteDatabase {
    private var db: OpaquePointer?

    init(path: URL) throws {
        let directory = path.deletingLastPathComponent()
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        if sqlite3_open(path.path, &db) != SQLITE_OK {
            throw DatabaseError.openFailed(lastError)
        }
        try execute("PRAGMA foreign_keys = ON")
        try execute("PRAGMA journal_mode = WAL")
        try migrate()
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

    func queryAssets(filter: LibraryFilter) throws -> [Asset] {
        var conditions: [String] = []
        var values: [SQLiteValue] = []

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
        if !filter.tag.isEmpty {
            conditions.append("a.tags LIKE ?")
            values.append(.text("%\(filter.tag)%"))
        }
        if !filter.directory.isEmpty {
            conditions.append("fi.path LIKE ?")
            values.append(.text("\(filter.directory)%"))
        }

        let statusCondition = statusSQL(filter.status)
        if !statusCondition.isEmpty {
            conditions.append(statusCondition)
        }

        let whereClause = conditions.isEmpty ? "" : "WHERE " + conditions.joined(separator: " AND ")
        let sql = """
        SELECT
            a.id, a.capture_time, a.camera_make, a.camera_model, a.lens_model,
            a.original_filename, a.content_fingerprint, a.metadata_fingerprint,
            a.rating, a.flag, a.tags, a.created_at, a.updated_at,
            COUNT(fi.id) AS file_count,
            MIN(CASE WHEN fi.file_role IN ('raw_original','jpeg_original') THEN fi.path ELSE NULL END) AS primary_path,
            MAX(CASE WHEN fi.file_role = 'thumbnail' THEN fi.path ELSE NULL END) AS thumbnail_path
        FROM assets a
        LEFT JOIN file_instances fi ON fi.asset_id = a.id
        \(whereClause)
        GROUP BY a.id
        ORDER BY COALESCE(a.capture_time, a.created_at) DESC
        """

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
                flag: statement.int(9) == 1,
                tags: decodeTags(statement.text(10)),
                createdAt: DateCoding.decode(statement.text(11)) ?? Date(),
                updatedAt: DateCoding.decode(statement.text(12)) ?? Date(),
                status: .inbox,
                fileCount: Int(statement.int(13)),
                primaryPath: statement.optionalText(14),
                thumbnailPath: statement.optionalText(15)
            )
            return asset.withStatus(derivedStatus(for: asset.id))
        }
    }

    func countsByStatus() throws -> [AssetStatus: Int] {
        let all = try queryAssets(filter: LibraryFilter())
        return Dictionary(grouping: all, by: \.status).mapValues(\.count)
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

    func upsertScannedFile(_ scanned: ScannedFile, batchID: UUID) throws -> Bool {
        let existingAssetID = try assetID(contentHash: scanned.contentHash, metadataFingerprint: scanned.metadataFingerprint)
        let assetID = existingAssetID ?? UUID()
        let now = Date()
        let isNewAsset = existingAssetID == nil

        if isNewAsset {
            try execute(
                """
                INSERT INTO assets (
                    id, capture_time, camera_make, camera_model, lens_model, original_filename,
                    content_fingerprint, metadata_fingerprint, rating, flag, tags, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, '[]', ?, ?)
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
                    .text(DateCoding.encode(now)),
                    .text(DateCoding.encode(now))
                ]
            )
            try insertVersion(assetID: assetID, name: "Original", kind: .original, parentID: nil, notes: "导入批次 \(batchID.uuidString)")
        } else {
            try execute("UPDATE assets SET updated_at = ? WHERE id = ?", [.text(DateCoding.encode(now)), .text(assetID.uuidString)])
        }

        let fileID = try fileInstanceID(path: scanned.url.path) ?? UUID()
        let exists = try fileInstanceID(path: scanned.url.path) != nil
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

        if let thumbnailURL = scanned.thumbnailURL {
            try upsertDerivedFile(assetID: assetID, url: thumbnailURL, role: .thumbnail, hash: scanned.thumbnailHash ?? "", sizeBytes: scanned.thumbnailSize)
        }
        if let previewURL = scanned.previewURL {
            try upsertDerivedFile(assetID: assetID, url: previewURL, role: .preview, hash: scanned.previewHash ?? "", sizeBytes: scanned.previewSize)
        }

        return isNewAsset || !exists
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

    func hasUnchangedFileInstance(path: String, sizeBytes: Int64) throws -> Bool {
        let count = try prepare(
            """
            SELECT COUNT(*)
            FROM file_instances
            WHERE path = ? AND size_bytes = ? AND availability = 'online'
            """,
            [.text(path), .int(sizeBytes)]
        ) { statement in
            statement.int64(0)
        }.first ?? 0
        return count > 0
    }

    func updateAssetMetadata(asset: Asset) throws {
        try execute(
            """
            UPDATE assets
            SET rating = ?, flag = ?, tags = ?, updated_at = ?
            WHERE id = ?
            """,
            [
                .int(Int64(asset.rating)),
                .int(asset.flag ? 1 : 0),
                .text(encodeTags(asset.tags)),
                .text(DateCoding.encode(Date())),
                .text(asset.id.uuidString)
            ]
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

            CREATE TABLE IF NOT EXISTS operation_logs (
                id TEXT PRIMARY KEY,
                action TEXT NOT NULL,
                source_path TEXT,
                destination_path TEXT,
                status TEXT NOT NULL,
                detail TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )
    }

    private func assetID(contentHash: String, metadataFingerprint: String) throws -> UUID? {
        let sql = "SELECT id FROM assets WHERE content_fingerprint = ? OR metadata_fingerprint = ? LIMIT 1"
        return try prepare(sql, [.text(contentHash), .text(metadataFingerprint)]) { statement in
            UUID(uuidString: statement.text(0))
        }.first ?? nil
    }

    private func fileInstanceID(path: String) throws -> UUID? {
        let sql = "SELECT id FROM file_instances WHERE path = ? LIMIT 1"
        return try prepare(sql, [.text(path)]) { statement in
            UUID(uuidString: statement.text(0))
        }.first ?? nil
    }

    private func upsertDerivedFile(assetID: UUID, url: URL, role: FileRole, hash: String, sizeBytes: Int64) throws {
        try execute(
            """
            INSERT INTO file_instances (
                id, asset_id, path, device_id, storage_kind, file_role, authority_role,
                sync_status, size_bytes, content_hash, last_seen_at, availability
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET last_seen_at = excluded.last_seen_at, availability = excluded.availability
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

    private func derivedStatus(for assetID: UUID) -> AssetStatus {
        let files = (try? fileInstances(assetID: assetID)) ?? []
        if !files.contains(where: { ($0.fileRole == .rawOriginal || $0.fileRole == .jpegOriginal) && $0.availability == .online }) {
            return .missingOriginal
        }
        if files.contains(where: { $0.syncStatus == .needsSync }) {
            return .needsSync
        }
        if files.contains(where: { $0.syncStatus == .needsArchive }) {
            return .needsArchive
        }
        if files.contains(where: { $0.storageKind == .nas && $0.authorityRole == .canonical }) {
            return .archived
        }
        if files.contains(where: { $0.authorityRole == .workingCopy }) {
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
}

enum SQLiteValue {
    case text(String)
    case nullableText(String?)
    case int(Int64)

    func bind(to statement: OpaquePointer?, index: Int32) -> Int32 {
        switch self {
        case .text(let value):
            return sqlite3_bind_text(statement, index, value, -1, SQLITE_TRANSIENT)
        case .nullableText(let value):
            guard let value else { return sqlite3_bind_null(statement, index) }
            return sqlite3_bind_text(statement, index, value, -1, SQLITE_TRANSIENT)
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
}

private let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

func currentDeviceID() -> String {
    Host.current().localizedName ?? Host.current().name ?? "mac"
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

private extension Asset {
    func withStatus(_ status: AssetStatus) -> Asset {
        var copy = self
        copy.status = status
        return copy
    }
}
