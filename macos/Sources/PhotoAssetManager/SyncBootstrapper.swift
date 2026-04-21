import CryptoKit
import Foundation

struct SyncBootstrapper: Sendable {
    var libraryID: String
    var deviceID: SyncDeviceID
    var actorID: String
    let database: SQLiteDatabase
    var nowProvider: @Sendable () -> Date = Date.init
    var progressReporter: (@Sendable (SyncBootstrapProgress) -> Void)?

    func bootstrapExistingLibraryToLedger() throws -> SyncBootstrapResult {
        progressReporter?(.countingSourceFacts)
        let assetCount = try database.bootstrapAssetSnapshotCount()
        let fileCount = try database.bootstrapFileInstanceCount()

        progressReporter?(.loadingSnapshots(assetCount: assetCount, fileCount: fileCount))
        let snapshots = try database.bootstrapAssetSnapshots()
        let files = try database.bootstrapFileInstances()
        let fingerprint = Self.sourceDatabaseFingerprint(snapshots: snapshots, files: files)
        let startedAt = nowProvider()
        try database.recordSyncMigrationStarted(
            libraryID: libraryID,
            sourceDatabaseFingerprint: fingerprint,
            startedAt: startedAt
        )

        let entries = try buildBootstrapEntries(snapshots: snapshots, files: files, createdAt: startedAt)
        progressReporter?(.writingLedger(totalOperations: entries.count))
        let insertedCount = try database.appendLedgerEntries(entries, uploadStatus: .pending)

        progressReporter?(.verifyingProjection(totalOperations: entries.count))
        let ledgerEntries = try database.ledgerEntries(libraryID: libraryID)
        let projectionVerified = try verifyBootstrapProjection(snapshots: snapshots, ledgerEntries: ledgerEntries)
        let highWatermark = ledgerEntries.count
        try database.recordSyncMigrationCompleted(
            libraryID: libraryID,
            completedAt: nowProvider(),
            ledgerHighWatermark: highWatermark,
            projectionVerified: projectionVerified
        )

        return SyncBootstrapResult(
            createdOperationCount: insertedCount,
            ledgerHighWatermark: highWatermark,
            projectionVerified: projectionVerified
        )
    }

    func verifyBootstrapProjection() throws -> Bool {
        try verifyBootstrapProjection(
            snapshots: database.bootstrapAssetSnapshots(),
            ledgerEntries: database.ledgerEntries(libraryID: libraryID)
        )
    }

    private func buildBootstrapEntries(snapshots: [AssetSnapshot], files: [FileInstance], createdAt: Date) throws -> [OperationLedgerEntry] {
        let baseTime = Int64(createdAt.timeIntervalSince1970 * 1000)
        var entries: [OperationLedgerEntry] = []
        var sequence: Int64 = 1

        for snapshot in snapshots {
            entries.append(.assetSnapshotDeclared(
                opID: Self.stableUUID("bootstrap:\(libraryID):asset:\(snapshot.assetID.uuidString)"),
                libraryID: libraryID,
                deviceID: deviceID,
                deviceSequence: sequence,
                time: HybridLogicalTime(wallTimeMilliseconds: baseTime, counter: sequence, nodeID: deviceID.rawValue),
                actorID: actorID,
                snapshot: snapshot,
                createdAt: createdAt
            ))
            sequence += 1
        }

        for file in files {
            let fileObject = FileObjectID(contentHash: file.contentHash, sizeBytes: file.sizeBytes, role: file.fileRole)
            let placement = FilePlacement(
                fileObjectID: fileObject,
                holderID: file.deviceID,
                storageKind: file.storageKind,
                authorityRole: file.authorityRole,
                availability: file.availability
            )
            entries.append(.filePlacementSnapshotDeclared(
                opID: Self.stableUUID("bootstrap:\(libraryID):placement:\(file.id.uuidString)"),
                libraryID: libraryID,
                deviceID: deviceID,
                deviceSequence: sequence,
                time: HybridLogicalTime(wallTimeMilliseconds: baseTime, counter: sequence, nodeID: deviceID.rawValue),
                actorID: actorID,
                assetID: file.assetID,
                fileObject: fileObject,
                placement: placement,
                createdAt: createdAt
            ))
            sequence += 1
        }

        return entries
    }

    private func verifyBootstrapProjection(snapshots: [AssetSnapshot], ledgerEntries: [OperationLedgerEntry]) throws -> Bool {
        let projection = try SyncLedgerProjector.project(ledgerEntries)
        for snapshot in snapshots {
            guard let asset = projection.assets[snapshot.assetID] else { return false }
            guard asset.rating == snapshot.rating else { return false }
            guard asset.flagState == snapshot.flagState else { return false }
            guard asset.colorLabel == snapshot.colorLabel else { return false }
            guard asset.tags == snapshot.tags.sorted() else { return false }
        }
        return true
    }
    private static func sourceDatabaseFingerprint(snapshots: [AssetSnapshot], files: [FileInstance]) -> String {
        let value = snapshots.map { "\($0.assetID.uuidString):\($0.rating):\($0.flagState.rawValue):\($0.tags.joined(separator: ","))" }
            .joined(separator: "|") + "#" + files.map { "\($0.id.uuidString):\($0.contentHash):\($0.sizeBytes):\($0.fileRole.rawValue)" }
            .joined(separator: "|")
        return SHA256.hash(data: Data(value.utf8)).map { String(format: "%02x", $0) }.joined()
    }

    private static func stableUUID(_ key: String) -> UUID {
        var bytes = Array(SHA256.hash(data: Data(key.utf8)).prefix(16))
        bytes[6] = (bytes[6] & 0x0f) | 0x50
        bytes[8] = (bytes[8] & 0x3f) | 0x80
        return UUID(uuid: (
            bytes[0], bytes[1], bytes[2], bytes[3],
            bytes[4], bytes[5],
            bytes[6], bytes[7],
            bytes[8], bytes[9],
            bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
        ))
    }
}

enum SyncBootstrapProgress: Sendable {
    case countingSourceFacts
    case loadingSnapshots(assetCount: Int, fileCount: Int)
    case writingLedger(totalOperations: Int)
    case verifyingProjection(totalOperations: Int)
}

protocol DerivativeDataFetching: Sendable {
    func fetchDerivative(_ object: S3ObjectRef) async throws -> Data
}

protocol DerivativeDataUploading: Sendable {
    func uploadDerivativeData(_ data: Data, to uploadURL: URL) async throws
}

struct URLSessionDerivativeDataUploader: DerivativeDataUploading {
    var session: URLSession = .shared

    func uploadDerivativeData(_ data: Data, to uploadURL: URL) async throws {
        var request = URLRequest(url: uploadURL)
        request.httpMethod = "PUT"
        let (_, response) = try await session.upload(for: request, from: data)
        guard let httpResponse = response as? HTTPURLResponse else {
            throw SyncControlPlaneHTTPError.invalidHTTPResponse
        }
        guard 200..<300 ~= httpResponse.statusCode else {
            throw SyncControlPlaneHTTPError.unexpectedStatusCode(httpResponse.statusCode)
        }
    }
}

struct DerivativeUploadService<CommandLayer: SyncCommandWriting & Sendable>: Sendable {
    var libraryID: String
    var commandLayer: CommandLayer
    let controlPlane: SyncControlPlaneClient
    let uploader: DerivativeDataUploading

    func uploadDerivative(assetID: UUID, role: DerivativeRole, localFile: URL, pixelSize: PixelSize) async throws {
        let data = try Data(contentsOf: localFile)
        let fileObject = FileObjectID(
            contentHash: try FileHasher.sha256(url: localFile),
            sizeBytes: Int64(data.count),
            role: role.fileRole
        )
        let uploadRequest = DerivativeUploadRequest(
            libraryID: libraryID,
            assetID: assetID,
            role: role,
            fileObject: fileObject,
            pixelSize: pixelSize
        )
        let upload = try await controlPlane.createDerivativeUpload(uploadRequest)
        try await uploader.uploadDerivativeData(data, to: upload.uploadURL)
        try commandLayer.declareDerivative(
            assetID: assetID,
            role: role,
            fileObject: fileObject,
            s3Object: upload.s3Object,
            pixelSize: pixelSize
        )
    }
}

struct DerivativeCacheStore: Sendable {
    var cacheRoot: URL
    let fetcher: DerivativeDataFetching

    func cacheDerivative(assetID: UUID, role: DerivativeRole, s3Object: S3ObjectRef) async throws -> URL {
        let data = try await fetcher.fetchDerivative(s3Object)
        let directory = cacheRoot
            .appendingPathComponent(assetID.uuidString, isDirectory: true)
            .appendingPathComponent(role.rawValue, isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let destination = directory.appendingPathComponent(Self.safeFilename(for: s3Object), isDirectory: false)
        try data.write(to: destination, options: [.atomic])
        return destination
    }

    private static func safeFilename(for object: S3ObjectRef) -> String {
        let digest = SHA256.hash(data: Data("\(object.bucket):\(object.key):\(object.eTag ?? "")".utf8))
            .map { String(format: "%02x", $0) }
            .joined()
        return "\(digest).jpg"
    }
}
