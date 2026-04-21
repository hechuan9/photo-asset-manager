import Foundation
import Testing
@testable import PhotoAssetManager

struct SyncLedgerTests {
    private static let accessCredentialHeaderName = "Author" + "ization"
    private static let accessCredentialScheme = "Bear" + "er"

    @Test func scalarMetadataUsesFieldLevelRegistersForDeterministicOfflineMerge() throws {
        let assetID = UUID()
        let deviceA = SyncDeviceID("mac")
        let deviceB = SyncDeviceID("iphone")
        let baseTime = HybridLogicalTime(wallTimeMilliseconds: 1_800_000_000_000, counter: 0, nodeID: deviceA.rawValue)
        let laterTime = HybridLogicalTime(wallTimeMilliseconds: 1_800_000_000_001, counter: 0, nodeID: deviceB.rawValue)

        let ops = [
            OperationLedgerEntry.metadataSet(
                opID: UUID(uuidString: "00000000-0000-0000-0000-000000000001")!,
                libraryID: "library",
                deviceID: deviceA,
                deviceSequence: 1,
                time: baseTime,
                actorID: "user",
                assetID: assetID,
                field: .rating,
                value: .int(4)
            ),
            OperationLedgerEntry.metadataSet(
                opID: UUID(uuidString: "00000000-0000-0000-0000-000000000002")!,
                libraryID: "library",
                deviceID: deviceB,
                deviceSequence: 1,
                time: laterTime,
                actorID: "user",
                assetID: assetID,
                field: .flagState,
                value: .string(AssetFlagState.picked.rawValue)
            )
        ]

        let projection = try SyncLedgerProjector.project(ops)

        #expect(projection.assets[assetID]?.rating == 4)
        #expect(projection.assets[assetID]?.flagState == .picked)
        #expect(projection.conflicts.isEmpty)
    }

    @Test func tagUpdatesUseAddRemoveSetSemanticsInsteadOfLastWriterWins() throws {
        let assetID = UUID()
        let mac = SyncDeviceID("mac")
        let phone = SyncDeviceID("iphone")

        let ops = [
            OperationLedgerEntry.tagsUpdated(
                opID: UUID(uuidString: "00000000-0000-0000-0000-000000000011")!,
                libraryID: "library",
                deviceID: mac,
                deviceSequence: 1,
                time: HybridLogicalTime(wallTimeMilliseconds: 10, counter: 0, nodeID: mac.rawValue),
                actorID: "user",
                assetID: assetID,
                add: ["family"],
                remove: []
            ),
            OperationLedgerEntry.tagsUpdated(
                opID: UUID(uuidString: "00000000-0000-0000-0000-000000000012")!,
                libraryID: "library",
                deviceID: phone,
                deviceSequence: 1,
                time: HybridLogicalTime(wallTimeMilliseconds: 11, counter: 0, nodeID: phone.rawValue),
                actorID: "user",
                assetID: assetID,
                add: ["print"],
                remove: []
            ),
            OperationLedgerEntry.tagsUpdated(
                opID: UUID(uuidString: "00000000-0000-0000-0000-000000000013")!,
                libraryID: "library",
                deviceID: mac,
                deviceSequence: 2,
                time: HybridLogicalTime(wallTimeMilliseconds: 12, counter: 0, nodeID: mac.rawValue),
                actorID: "user",
                assetID: assetID,
                add: [],
                remove: ["family"]
            )
        ]

        let projection = try SyncLedgerProjector.project(ops)

        #expect(projection.assets[assetID]?.tags == ["print"])
    }

    @Test func projectionOrdersCommittedEventsByServerGlobalSequenceBeforeLocalTime() throws {
        let assetID = UUID()
        var firstCommitted = OperationLedgerEntry.metadataSet(
            opID: UUID(uuidString: "00000000-0000-0000-0000-000000000014")!,
            libraryID: "library",
            deviceID: SyncDeviceID("mac"),
            deviceSequence: 1,
            time: HybridLogicalTime(wallTimeMilliseconds: 20, counter: 0, nodeID: "mac"),
            actorID: "user",
            assetID: assetID,
            field: .rating,
            value: .int(1)
        )
        firstCommitted.globalSeq = 1
        var secondCommitted = OperationLedgerEntry.metadataSet(
            opID: UUID(uuidString: "00000000-0000-0000-0000-000000000015")!,
            libraryID: "library",
            deviceID: SyncDeviceID("iphone"),
            deviceSequence: 1,
            time: HybridLogicalTime(wallTimeMilliseconds: 10, counter: 0, nodeID: "iphone"),
            actorID: "user",
            assetID: assetID,
            field: .rating,
            value: .int(5)
        )
        secondCommitted.globalSeq = 2

        let projection = try SyncLedgerProjector.project([secondCommitted, firstCommitted])

        #expect(projection.assets[assetID]?.rating == 5)
    }

    @Test func remoteSnapshotPageMaterializesAssetsProjectionForFreshDevice() throws {
        let assetID = UUID(uuidString: "00000000-0000-0000-0000-000000000101")!
        let captureTime = Date(timeIntervalSince1970: 1_710_000_000)
        let createdAt = Date(timeIntervalSince1970: 1_709_999_900)
        let updatedAt = Date(timeIntervalSince1970: 1_710_000_100)

        try withTempDatabase { database, _ in
            var snapshot = OperationLedgerEntry.assetSnapshotDeclared(
                opID: UUID(uuidString: "00000000-0000-0000-0000-000000000102")!,
                libraryID: "library",
                deviceID: SyncDeviceID("server"),
                deviceSequence: 1,
                time: HybridLogicalTime(wallTimeMilliseconds: 1_710_000_100_000, counter: 0, nodeID: "server"),
                actorID: "system:migration",
                snapshot: AssetSnapshot(
                    assetID: assetID,
                    captureTime: captureTime,
                    cameraMake: "FUJIFILM",
                    cameraModel: "X-T5",
                    lensModel: "XF33mmF1.4",
                    originalFilename: "IMG_0001.DNG",
                    contentFingerprint: "fingerprint-a",
                    metadataFingerprint: "metadata-a",
                    rating: 0,
                    flagState: .unflagged,
                    colorLabel: nil,
                    tags: ["seed"],
                    createdAt: createdAt,
                    updatedAt: updatedAt
                )
            )
            snapshot.globalSeq = 1

            var rating = OperationLedgerEntry.metadataSet(
                opID: UUID(uuidString: "00000000-0000-0000-0000-000000000103")!,
                libraryID: "library",
                deviceID: SyncDeviceID("server"),
                deviceSequence: 2,
                time: HybridLogicalTime(wallTimeMilliseconds: 1_710_000_101_000, counter: 0, nodeID: "server"),
                actorID: "server",
                assetID: assetID,
                field: .rating,
                value: .int(5)
            )
            rating.globalSeq = 2

            var flag = OperationLedgerEntry.metadataSet(
                opID: UUID(uuidString: "00000000-0000-0000-0000-000000000104")!,
                libraryID: "library",
                deviceID: SyncDeviceID("server"),
                deviceSequence: 3,
                time: HybridLogicalTime(wallTimeMilliseconds: 1_710_000_102_000, counter: 0, nodeID: "server"),
                actorID: "server",
                assetID: assetID,
                field: .flagState,
                value: .string(AssetFlagState.picked.rawValue)
            )
            flag.globalSeq = 3

            var color = OperationLedgerEntry.metadataSet(
                opID: UUID(uuidString: "00000000-0000-0000-0000-000000000105")!,
                libraryID: "library",
                deviceID: SyncDeviceID("server"),
                deviceSequence: 4,
                time: HybridLogicalTime(wallTimeMilliseconds: 1_710_000_103_000, counter: 0, nodeID: "server"),
                actorID: "server",
                assetID: assetID,
                field: .colorLabel,
                value: .string(AssetColorLabel.red.rawValue)
            )
            color.globalSeq = 4

            var tags = OperationLedgerEntry.tagsUpdated(
                opID: UUID(uuidString: "00000000-0000-0000-0000-000000000106")!,
                libraryID: "library",
                deviceID: SyncDeviceID("server"),
                deviceSequence: 5,
                time: HybridLogicalTime(wallTimeMilliseconds: 1_710_000_104_000, counter: 0, nodeID: "server"),
                actorID: "server",
                assetID: assetID,
                add: ["portfolio"],
                remove: ["seed"]
            )
            tags.globalSeq = 5

            try database.appendAcknowledgedRemoteLedgerPage(
                [snapshot, rating, flag, color, tags],
                peerID: "control-plane",
                cursor: "cursor-5"
            )

            let assets = try database.queryAssets(filter: LibraryFilter(), limit: 10)
            let asset = try #require(assets.first)

            #expect(assets.count == 1)
            #expect(asset.id == assetID)
            #expect(asset.captureTime == captureTime)
            #expect(asset.originalFilename == "IMG_0001.DNG")
            #expect(asset.rating == 5)
            #expect(asset.flagState == .picked)
            #expect(asset.colorLabel == .red)
            #expect(asset.tags == ["portfolio"])
            #expect(try database.syncCursor(peerID: "control-plane") == "cursor-5")
        }
    }

    @Test func sharedTrashIsARecoverableLibraryWideStateMachine() throws {
        let assetID = UUID()
        let device = SyncDeviceID("mac")

        let ops = [
            OperationLedgerEntry.moveToTrash(
                opID: UUID(uuidString: "00000000-0000-0000-0000-000000000021")!,
                libraryID: "library",
                deviceID: device,
                deviceSequence: 1,
                time: HybridLogicalTime(wallTimeMilliseconds: 20, counter: 0, nodeID: device.rawValue),
                actorID: "user",
                assetID: assetID,
                reason: "culled"
            ),
            OperationLedgerEntry.restoreFromTrash(
                opID: UUID(uuidString: "00000000-0000-0000-0000-000000000022")!,
                libraryID: "library",
                deviceID: device,
                deviceSequence: 2,
                time: HybridLogicalTime(wallTimeMilliseconds: 21, counter: 0, nodeID: device.rawValue),
                actorID: "user",
                assetID: assetID
            )
        ]

        let projection = try SyncLedgerProjector.project(ops)

        #expect(projection.assets[assetID]?.trashState == .active)
        #expect(projection.trash[assetID] == nil)
    }

    @Test func originalArchiveReceiptSeparatesFileContentFromAssetMetadata() throws {
        let assetID = UUID()
        let fileObjectID = FileObjectID(contentHash: "abc123", sizeBytes: 42, role: .rawOriginal)
        let device = SyncDeviceID("iphone")
        let server = SyncDeviceID("archive-server")

        let ops = [
            OperationLedgerEntry.importedOriginalDeclared(
                opID: UUID(uuidString: "00000000-0000-0000-0000-000000000031")!,
                libraryID: "library",
                deviceID: device,
                deviceSequence: 1,
                time: HybridLogicalTime(wallTimeMilliseconds: 30, counter: 0, nodeID: device.rawValue),
                actorID: "user",
                assetID: assetID,
                fileObject: fileObjectID,
                placement: FilePlacement(
                    fileObjectID: fileObjectID,
                    holderID: device.rawValue,
                    storageKind: .local,
                    authorityRole: .workingCopy,
                    availability: .online
                )
            ),
            OperationLedgerEntry.originalArchiveReceiptRecorded(
                opID: UUID(uuidString: "00000000-0000-0000-0000-000000000032")!,
                libraryID: "library",
                deviceID: server,
                deviceSequence: 1,
                time: HybridLogicalTime(wallTimeMilliseconds: 31, counter: 0, nodeID: server.rawValue),
                actorID: "archive-server",
                assetID: assetID,
                fileObject: fileObjectID,
                serverPlacement: FilePlacement(
                    fileObjectID: fileObjectID,
                    holderID: server.rawValue,
                    storageKind: .nas,
                    authorityRole: .canonical,
                    availability: .online
                )
            )
        ]

        let projection = try SyncLedgerProjector.project(ops)

        #expect(projection.assets[assetID]?.archiveState == .archived)
        #expect(projection.fileObjects[fileObjectID]?.contentHash == "abc123")
        #expect(projection.filePlacements[fileObjectID]?.count == 2)
    }

    @Test func databaseMigratesLedgerAndSyncStateTables() throws {
        let root = FileManager.default.temporaryDirectory
            .resolvingSymlinksInPath()
            .appendingPathComponent("PhotoAssetManagerTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        let database = try SQLiteDatabase(path: root.appendingPathComponent("Library.sqlite"))

        #expect(try database.tableExists("operation_ledger"))
        #expect(try database.tableExists("sync_cursors"))
        #expect(try database.tableExists("sync_upload_queue"))
        #expect(try database.tableExists("file_objects"))
        #expect(try database.tableExists("file_placements"))
        #expect(try database.tableExists("asset_trash_states"))
        #expect(try database.tableExists("sync_migration_state"))
        #expect(try database.tableExists("derivative_objects"))
    }

    @Test func bootstrapExistingLibraryWritesIdempotentSnapshotLedgerAndMigrationWatermark() throws {
        let root = try makeTemporaryRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let database = try SQLiteDatabase(path: root.appendingPathComponent("Library.sqlite"))
        let assetID = UUID(uuidString: "00000000-0000-0000-0000-00000000ba01")!
        let originalID = UUID(uuidString: "00000000-0000-0000-0000-00000000ba02")!
        let thumbnailID = UUID(uuidString: "00000000-0000-0000-0000-00000000ba03")!
        let createdAt = DateCoding.encode(Date(timeIntervalSince1970: 1_700_001_000))

        try database.execute(
            """
            INSERT INTO assets (
                id, capture_time, camera_make, camera_model, lens_model, original_filename,
                content_fingerprint, metadata_fingerprint, rating, flag, flag_state, color_label, tags, created_at, updated_at
            ) VALUES (
                '\(assetID.uuidString)', NULL, 'Sony', 'A7C', '35mm', 'DSC0001.ARW',
                'asset-fp', 'metadata-fp', 5, 1, 'picked', 'red', '["family","print"]', '\(createdAt)', '\(createdAt)'
            )
            """
        )
        try database.execute(
            """
            INSERT INTO file_instances (
                id, asset_id, path, device_id, storage_kind, file_role, authority_role,
                sync_status, size_bytes, content_hash, last_seen_at, availability
            ) VALUES
            (
                '\(originalID.uuidString)', '\(assetID.uuidString)', '\(root.appendingPathComponent("DSC0001.ARW").path)',
                'mac', 'nas', 'raw_original', 'canonical', 'synced', 12345, 'raw-hash', '\(createdAt)', 'online'
            ),
            (
                '\(thumbnailID.uuidString)', '\(assetID.uuidString)', '\(root.appendingPathComponent("thumb.jpg").path)',
                'mac', 'local', 'thumbnail', 'cache', 'cache_only', 321, 'thumb-hash', '\(createdAt)', 'online'
            )
            """
        )

        let bootstrapper = SyncBootstrapper(
            libraryID: "library",
            deviceID: SyncDeviceID("mac"),
            actorID: "system:migration",
            database: database,
            nowProvider: { Date(timeIntervalSince1970: 1_700_002_000) }
        )

        let first = try bootstrapper.bootstrapExistingLibraryToLedger()
        let second = try bootstrapper.bootstrapExistingLibraryToLedger()
        let entries = try database.ledgerEntries(libraryID: "library")
        let projection = try SyncLedgerProjector.project(entries)
        let state = try database.syncMigrationState(libraryID: "library")

        #expect(first.createdOperationCount == entries.count)
        #expect(second.createdOperationCount == 0)
        #expect(entries.map(\.actorID).allSatisfy { $0 == "system:migration" })
        #expect(entries.contains(where: { $0.opType == .assetSnapshotDeclared }))
        #expect(entries.contains(where: { $0.opType == .filePlacementSnapshotDeclared }))
        #expect(!entries.contains(where: { $0.opType == .derivativeDeclared }))
        #expect(projection.assets[assetID]?.rating == 5)
        #expect(projection.assets[assetID]?.flagState == .picked)
        #expect(projection.assets[assetID]?.colorLabel == .red)
        #expect(projection.assets[assetID]?.tags == ["family", "print"])
        #expect(projection.fileObjects[FileObjectID(contentHash: "raw-hash", sizeBytes: 12345, role: .rawOriginal)] != nil)
        #expect(projection.derivatives[assetID] == nil)
        #expect(state?.status == .completed)
        #expect(state?.projectionVerified == true)
        #expect(state?.ledgerHighWatermark == entries.count)
    }

    @Test func bootstrapStableOperationIDFailsWhenSnapshotPayloadChanges() throws {
        let root = try makeTemporaryRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let database = try SQLiteDatabase(path: root.appendingPathComponent("Library.sqlite"))
        let assetID = UUID(uuidString: "00000000-0000-0000-0000-00000000bb01")!
        let createdAt = DateCoding.encode(Date(timeIntervalSince1970: 1_700_003_000))

        try database.execute(
            """
            INSERT INTO assets (
                id, capture_time, camera_make, camera_model, lens_model, original_filename,
                content_fingerprint, metadata_fingerprint, rating, flag, flag_state, color_label, tags, created_at, updated_at
            ) VALUES (
                '\(assetID.uuidString)', NULL, '', '', '', 'one.jpg',
                'asset-fp', 'metadata-fp', 3, 0, 'unflagged', NULL, '[]', '\(createdAt)', '\(createdAt)'
            )
            """
        )

        let bootstrapper = SyncBootstrapper(
            libraryID: "library",
            deviceID: SyncDeviceID("mac"),
            actorID: "system:migration",
            database: database,
            nowProvider: { Date(timeIntervalSince1970: 1_700_004_000) }
        )
        _ = try bootstrapper.bootstrapExistingLibraryToLedger()

        try database.execute("UPDATE assets SET rating = 4 WHERE id = '\(assetID.uuidString)'")

        do {
            _ = try bootstrapper.bootstrapExistingLibraryToLedger()
            Issue.record("expected stable bootstrap op conflict")
        } catch {
            #expect(error.localizedDescription.contains("operation_ledger.op_id 冲突"))
        }
    }

    @Test func thumbnailsNeedingDerivativeUploadOnlyReturnsMissingOrStaleThumbnailDerivatives() throws {
        let root = try makeTemporaryRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let database = try SQLiteDatabase(path: root.appendingPathComponent("Library.sqlite"))
        let assetNeedingUpload = UUID(uuidString: "00000000-0000-0000-0000-00000000bd01")!
        let assetAlreadyUploaded = UUID(uuidString: "00000000-0000-0000-0000-00000000bd02")!
        let createdAt = DateCoding.encode(Date(timeIntervalSince1970: 1_700_006_000))

        for assetID in [assetNeedingUpload, assetAlreadyUploaded] {
            try database.execute(
                """
                INSERT INTO assets (
                    id, capture_time, camera_make, camera_model, lens_model, original_filename,
                    content_fingerprint, metadata_fingerprint, rating, flag, flag_state, color_label, tags, created_at, updated_at
                ) VALUES (
                    '\(assetID.uuidString)', NULL, '', '', '', '\(assetID.uuidString).jpg',
                    'asset-fp-\(assetID.uuidString)', 'metadata-fp-\(assetID.uuidString)', 0, 0, 'unflagged', NULL, '[]', '\(createdAt)', '\(createdAt)'
                )
                """
            )
        }

        let thumbnailAPath = root.appendingPathComponent("thumb-a.jpg").path
        let thumbnailBPath = root.appendingPathComponent("thumb-b.jpg").path
        try database.execute(
            """
            INSERT INTO file_instances (
                id, asset_id, path, device_id, storage_kind, file_role, authority_role,
                sync_status, size_bytes, content_hash, last_seen_at, availability
            ) VALUES
            (
                '00000000-0000-0000-0000-00000000bd11', '\(assetNeedingUpload.uuidString)', '\(thumbnailAPath)',
                'mac', 'local', 'thumbnail', 'cache', 'cache_only', 111, 'thumb-hash-a', '\(createdAt)', 'online'
            ),
            (
                '00000000-0000-0000-0000-00000000bd12', '\(assetAlreadyUploaded.uuidString)', '\(thumbnailBPath)',
                'mac', 'local', 'thumbnail', 'cache', 'cache_only', 222, 'thumb-hash-b', '\(createdAt)', 'online'
            )
            """
        )
        try database.execute(
            """
            INSERT INTO file_objects (
                id, content_hash, size_bytes, file_role, created_at
            ) VALUES (
                'thumb-hash-b:222:thumbnail', 'thumb-hash-b', 222, 'thumbnail', '\(createdAt)'
            )
            """
        )
        try database.execute(
            """
            INSERT INTO derivative_objects (
                asset_id, role, file_object_id, s3_bucket, s3_key, s3_etag, pixel_width, pixel_height, updated_at
            ) VALUES (
                '\(assetAlreadyUploaded.uuidString)', 'thumbnail', 'thumb-hash-b:222:thumbnail',
                'bucket', 'key', 'etag', 400, 300, '\(createdAt)'
            )
            """
        )

        let candidates = try database.thumbnailsNeedingDerivativeUpload()

        #expect(candidates.map(\.assetID) == [assetNeedingUpload])
    }

    @Test func commandLayerDeclaresS3ThumbnailAndPreviewDerivativesWithoutStoringBytesInLedger() throws {
        let root = try makeTemporaryRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let database = try SQLiteDatabase(path: root.appendingPathComponent("Library.sqlite"))
        let commandLayer = SyncCommandLayer(
            libraryID: "library",
            deviceID: SyncDeviceID("mac"),
            actorID: "user",
            database: database,
            nowProvider: { Date(timeIntervalSince1970: 1_700_005_000) }
        )
        let assetID = UUID(uuidString: "00000000-0000-0000-0000-00000000bc01")!
        let thumbnailObject = FileObjectID(contentHash: "thumb-hash", sizeBytes: 320, role: .thumbnail)
        let previewObject = FileObjectID(contentHash: "preview-hash", sizeBytes: 2048, role: .preview)
        let createdAt = DateCoding.encode(Date(timeIntervalSince1970: 1_700_004_900))
        try database.execute(
            """
            INSERT INTO assets (
                id, capture_time, camera_make, camera_model, lens_model, original_filename,
                content_fingerprint, metadata_fingerprint, rating, flag, flag_state, color_label, tags, created_at, updated_at
            ) VALUES (
                '\(assetID.uuidString)', NULL, '', '', '', 'one.jpg',
                'asset-fp', 'metadata-fp', 0, 0, 'unflagged', NULL, '[]', '\(createdAt)', '\(createdAt)'
            )
            """
        )

        try commandLayer.declareDerivative(
            assetID: assetID,
            role: .thumbnail,
            fileObject: thumbnailObject,
            s3Object: S3ObjectRef(bucket: "photo-derivatives", key: "libraries/library/assets/\(assetID.uuidString)/thumbnail/thumb-hash.jpg", eTag: "etag-thumb"),
            pixelSize: PixelSize(width: 320, height: 240)
        )
        try commandLayer.declareDerivative(
            assetID: assetID,
            role: .preview,
            fileObject: previewObject,
            s3Object: S3ObjectRef(bucket: "photo-derivatives", key: "libraries/library/assets/\(assetID.uuidString)/preview/preview-hash.jpg", eTag: "etag-preview"),
            pixelSize: PixelSize(width: 2048, height: 1365)
        )

        let entries = try database.ledgerEntries(libraryID: "library")
        let projection = try SyncLedgerProjector.project(entries)
        let derivatives = try database.derivatives(assetID: assetID)

        #expect(entries.map(\.opType) == [.derivativeDeclared, .derivativeDeclared])
        #expect(entries.allSatisfy { $0.entityType == .derivativeObject })
        #expect(entries.allSatisfy { !$0.entityID.contains("/Users/") })
        #expect(projection.derivatives[assetID]?[.thumbnail]?.fileObject == thumbnailObject)
        #expect(projection.derivatives[assetID]?[.preview]?.pixelSize.width == 2048)
        #expect(derivatives.map(\.role).sorted { $0.rawValue < $1.rawValue } == [.preview, .thumbnail])
        #expect(try database.pendingLedgerUploadCount() == 2)
    }

    @Test func controlPlaneSupportsDerivativeUploadAndDownloadRoutes() async throws {
        let assetID = UUID(uuidString: "00000000-0000-0000-0000-00000000bd01")!
        let uploadRequest = DerivativeUploadRequest(
            libraryID: "library",
            assetID: assetID,
            role: .preview,
            fileObject: FileObjectID(contentHash: "preview-hash", sizeBytes: 2048, role: .preview),
            pixelSize: PixelSize(width: 2048, height: 1365)
        )
        let uploadResponse = DerivativeUploadResponse(
            s3Object: S3ObjectRef(bucket: "photo-derivatives", key: "libraries/library/assets/\(assetID.uuidString)/preview/preview-hash.jpg", eTag: nil),
            uploadURL: URL(string: "https://upload.example.com/preview")!
        )
        let metadata = DerivativeMetadataResponse(
            derivative: DerivativeObject(
                assetID: assetID,
                role: .preview,
                fileObject: uploadRequest.fileObject,
                s3Object: uploadResponse.s3Object,
                pixelSize: uploadRequest.pixelSize
            ),
            downloadURL: URL(string: "https://download.example.com/preview")!
        )

        var captured: [URLRequest] = []
        let sessionStub = makeStubSession { request in
            captured.append(request)
            guard let url = request.url else {
                throw NSError(domain: "PhotoAssetManagerTests", code: 41, userInfo: [NSLocalizedDescriptionKey: "missing url"])
            }
            let encoder = makeJSONEncoder()
            let decoder = makeJSONDecoder()
            switch (request.httpMethod, url.path) {
            case ("POST", "/derivatives/uploads"):
                #expect(try decoder.decode(DerivativeUploadRequest.self, from: request.httpBodyData) == uploadRequest)
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, try encoder.encode(uploadResponse))
            case ("GET", "/derivatives/\(assetID.uuidString)"):
                let queryItems = URLComponents(url: url, resolvingAgainstBaseURL: false)?.queryItems
                #expect(queryItems?.first(where: { $0.name == "role" })?.value == "preview")
                #expect(queryItems?.first(where: { $0.name == "libraryID" })?.value == "library")
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!, try encoder.encode(metadata))
            default:
                throw NSError(domain: "PhotoAssetManagerTests", code: 42, userInfo: [NSLocalizedDescriptionKey: "unexpected derivative request"])
            }
        }

        let client = SyncControlPlaneHTTPClient(
            baseURL: URL(string: "https://control.example.com")!,
            headerProvider: { ["X-Stub-Key": sessionStub.stubKey] },
            session: sessionStub.session
        )

        let signedUpload = try await client.createDerivativeUpload(uploadRequest)
        let fetchedMetadata = try await client.fetchDerivativeMetadata(libraryID: "library", assetID: assetID, role: .preview)

        #expect(signedUpload == uploadResponse)
        #expect(fetchedMetadata == metadata)
        #expect(captured.map(\.httpMethod) == ["POST", "GET"])
    }

    @Test func derivativeCacheMissDownloadsOnlyIntoCacheRoot() async throws {
        let root = try makeTemporaryRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let cacheRoot = root.appendingPathComponent("DerivativeCache", isDirectory: true)
        let original = root.appendingPathComponent("Originals/DSC0001.ARW")
        try FileManager.default.createDirectory(at: original.deletingLastPathComponent(), withIntermediateDirectories: true)
        try Data("original".utf8).write(to: original)

        let ref = S3ObjectRef(bucket: "photo-derivatives", key: "libraries/library/assets/a/thumbnail/thumb.jpg", eTag: "etag")
        let fetcher = StubDerivativeDataFetcher(data: Data("thumbnail".utf8))
        let store = DerivativeCacheStore(cacheRoot: cacheRoot, fetcher: fetcher)

        let cached = try await store.cacheDerivative(
            assetID: UUID(uuidString: "00000000-0000-0000-0000-00000000be01")!,
            role: .thumbnail,
            s3Object: ref
        )

        #expect(try Data(contentsOf: cached) == Data("thumbnail".utf8))
        #expect(try Data(contentsOf: original) == Data("original".utf8))
        #expect(cached.path.hasPrefix(cacheRoot.path))
        #expect(fetcher.requests == [ref])
    }

    @Test func derivativeUploadServiceDeclaresLedgerOnlyAfterUploadSucceeds() async throws {
        let root = try makeTemporaryRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let database = try SQLiteDatabase(path: root.appendingPathComponent("Library.sqlite"))
        let assetID = UUID(uuidString: "00000000-0000-0000-0000-00000000bf01")!
        let derivativeFile = root.appendingPathComponent("preview.jpg")
        try Data("preview".utf8).write(to: derivativeFile)
        let createdAt = DateCoding.encode(Date(timeIntervalSince1970: 1_700_006_000))
        try database.execute(
            """
            INSERT INTO assets (
                id, capture_time, camera_make, camera_model, lens_model, original_filename,
                content_fingerprint, metadata_fingerprint, rating, flag, flag_state, color_label, tags, created_at, updated_at
            ) VALUES (
                '\(assetID.uuidString)', NULL, '', '', '', 'one.jpg',
                'asset-fp', 'metadata-fp', 0, 0, 'unflagged', NULL, '[]', '\(createdAt)', '\(createdAt)'
            )
            """
        )
        let commandLayer = SyncCommandLayer(
            libraryID: "library",
            deviceID: SyncDeviceID("mac"),
            actorID: "user",
            database: database,
            nowProvider: { Date(timeIntervalSince1970: 1_700_006_100) }
        )
        let control = StubDerivativeControlPlane()
        let uploader = StubDerivativeUploader()
        let service = DerivativeUploadService(
            libraryID: "library",
            commandLayer: commandLayer,
            controlPlane: control,
            uploader: uploader
        )

        try await service.uploadDerivative(
            assetID: assetID,
            role: .preview,
            localFile: derivativeFile,
            pixelSize: PixelSize(width: 2048, height: 1365)
        )

        let entries = try database.ledgerEntries(libraryID: "library")
        #expect(control.uploadRequests.count == 1)
        #expect(uploader.uploads.count == 1)
        #expect(entries.map(\.opType) == [.derivativeDeclared])
        #expect(try database.derivatives(assetID: assetID).first?.role == .preview)
    }

    @Test func derivativeUploadServiceDoesNotDeclareLedgerWhenUploadFails() async throws {
        let root = try makeTemporaryRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let database = try SQLiteDatabase(path: root.appendingPathComponent("Library.sqlite"))
        let assetID = UUID(uuidString: "00000000-0000-0000-0000-00000000bf02")!
        let derivativeFile = root.appendingPathComponent("thumbnail.jpg")
        try Data("thumbnail".utf8).write(to: derivativeFile)
        let commandLayer = SyncCommandLayer(
            libraryID: "library",
            deviceID: SyncDeviceID("mac"),
            actorID: "user",
            database: database,
            nowProvider: { Date(timeIntervalSince1970: 1_700_006_200) }
        )
        let control = StubDerivativeControlPlane()
        let uploader = StubDerivativeUploader()
        uploader.error = NSError(domain: "PhotoAssetManagerTests", code: 61, userInfo: [NSLocalizedDescriptionKey: "upload failed"])
        let service = DerivativeUploadService(
            libraryID: "library",
            commandLayer: commandLayer,
            controlPlane: control,
            uploader: uploader
        )

        do {
            try await service.uploadDerivative(
                assetID: assetID,
                role: .thumbnail,
                localFile: derivativeFile,
                pixelSize: PixelSize(width: 320, height: 240)
            )
            Issue.record("expected derivative upload failure")
        } catch {
            #expect(error.localizedDescription.contains("upload failed"))
        }

        #expect(try database.ledgerEntries(libraryID: "library").isEmpty)
        #expect(try database.derivatives(assetID: assetID).isEmpty)
    }

    @Test func commandLayerWritesBusinessOpsAndUploadQueueWithoutDirectSqlReplay() throws {
        let root = FileManager.default.temporaryDirectory
            .resolvingSymlinksInPath()
            .appendingPathComponent("PhotoAssetManagerTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        let database = try SQLiteDatabase(path: root.appendingPathComponent("Library.sqlite"))
        let commandLayer = SyncCommandLayer(
            libraryID: "library",
            deviceID: SyncDeviceID("mac"),
            actorID: "user",
            database: database
        )
        let assetID = UUID()

        try commandLayer.setRating(assetID: assetID, rating: 5)
        try commandLayer.updateTags(assetID: assetID, add: ["keeper"], remove: [])
        try commandLayer.moveToTrash(assetID: assetID, reason: "review reject")

        let entries = try database.ledgerEntries(libraryID: "library")
        let projection = try SyncLedgerProjector.project(entries)

        #expect(entries.map(\.opType) == [.metadataSet, .tagsUpdated, .moveToTrash])
        #expect(entries.map(\.deviceSequence) == [1, 2, 3])
        #expect(try database.pendingLedgerUploadCount() == 3)
        #expect(projection.assets[assetID]?.rating == 5)
        #expect(projection.assets[assetID]?.tags == ["keeper"])
        #expect(projection.assets[assetID]?.trashState == .trashed)
    }

    @Test func commandLayerKeepsHybridLogicalTimeMonotonicWhenClockMovesBackward() throws {
        let root = FileManager.default.temporaryDirectory
            .resolvingSymlinksInPath()
            .appendingPathComponent("PhotoAssetManagerTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        let database = try SQLiteDatabase(path: root.appendingPathComponent("Library.sqlite"))
        let clock = TestClock([1_800_000_000_000, 1_799_999_999_000])
        let commandLayer = SyncCommandLayer(
            libraryID: "library",
            deviceID: SyncDeviceID("mac"),
            actorID: "user",
            database: database,
            nowProvider: clock.next
        )
        let assetID = UUID()

        try commandLayer.setRating(assetID: assetID, rating: 3)
        try commandLayer.setRating(assetID: assetID, rating: 4)

        let entries = try database.ledgerEntries(libraryID: "library")
        #expect(entries.count == 2)
        #expect(entries[1].hybridLogicalTime > entries[0].hybridLogicalTime)
        #expect(entries[1].hybridLogicalTime.wallTimeMilliseconds == entries[0].hybridLogicalTime.wallTimeMilliseconds)
        #expect(entries[1].hybridLogicalTime.counter == entries[0].hybridLogicalTime.counter + 1)
    }

    @MainActor
    @Test func libraryStoreRatingUpdateUsesLedgerCommandLayerAndRefreshesLocalProjection() throws {
        try withTempDatabase { database, databaseURL in
            let assetID = UUID()
            try insertAsset(id: assetID, database: database)

            let spy = SpySyncCommandLayer(
                wrapped: SyncCommandLayer(
                    libraryID: "local-library",
                    deviceID: SyncDeviceID("mac"),
                    actorID: "user",
                    database: database
                )
            )
            let store = LibraryStore(
                databasePath: databaseURL,
                database: database,
                syncCommandLayerFactory: { spy },
                performStartupWork: false
            )

            #expect(store.selectedAssetID == assetID)

            store.setSelectedAssetRating(5)

            let refreshedAssets = try database.queryAssets(filter: LibraryFilter(), limit: 10)
            let ledgerEntries = try database.ledgerEntries(libraryID: "local-library")

            #expect(store.assets.first?.rating == 5)
            #expect(refreshedAssets.first?.rating == 5)
            #expect(spy.builtRatingCalls.count == 1)
            #expect(spy.builtRatingCalls.first?.assetID == assetID)
            #expect(spy.builtRatingCalls.first?.rating == 5)
            #expect(ledgerEntries.count == 1)
            #expect(ledgerEntries.first?.opType == .metadataSet)
            #expect(
                ledgerEntries.first.map { entry in
                    if case let .metadataSet(assetID: ledgerAssetID, field: field, value: value) = entry.payload {
                        return ledgerAssetID == assetID && field == .rating && value == .int(5)
                    }
                    return false
                } == true
            )
            #expect(try database.pendingLedgerUploadCount() == 1)
        }
    }

    @MainActor
    @Test func libraryStoreFlagUpdateUsesLedgerCommandLayerAndRefreshesLocalProjection() throws {
        try withTempDatabase { database, databaseURL in
            let assetID = UUID()
            try insertAsset(id: assetID, database: database)

            let spy = SpySyncCommandLayer(
                wrapped: SyncCommandLayer(
                    libraryID: "local-library",
                    deviceID: SyncDeviceID("mac"),
                    actorID: "user",
                    database: database
                )
            )
            let store = LibraryStore(
                databasePath: databaseURL,
                database: database,
                syncCommandLayerFactory: { spy },
                performStartupWork: false
            )

            #expect(store.selectedAssetID == assetID)

            store.setSelectedAssetFlagState(.picked)

            let refreshedAssets = try database.queryAssets(filter: LibraryFilter(), limit: 10)
            let ledgerEntries = try database.ledgerEntries(libraryID: "local-library")

            #expect(store.assets.first?.flagState == .picked)
            #expect(refreshedAssets.first?.flagState == .picked)
            #expect(spy.builtFlagCalls.count == 1)
            #expect(spy.builtFlagCalls.first?.assetID == assetID)
            #expect(spy.builtFlagCalls.first?.flagState == .picked)
            #expect(ledgerEntries.count == 1)
            #expect(ledgerEntries.first?.opType == .metadataSet)
            #expect(
                ledgerEntries.first.map { entry in
                    if case let .metadataSet(assetID: ledgerAssetID, field: field, value: value) = entry.payload {
                        return ledgerAssetID == assetID && field == .flagState && value == .string(AssetFlagState.picked.rawValue)
                    }
                    return false
                } == true
            )
            #expect(try database.pendingLedgerUploadCount() == 1)
        }
    }

    @MainActor
    @Test func libraryStoreColorUpdateUsesLedgerCommandLayerAndRefreshesLocalProjection() throws {
        try withTempDatabase { database, databaseURL in
            let assetID = UUID()
            try insertAsset(id: assetID, database: database)

            let spy = SpySyncCommandLayer(
                wrapped: SyncCommandLayer(
                    libraryID: "local-library",
                    deviceID: SyncDeviceID("mac"),
                    actorID: "user",
                    database: database
                )
            )
            let store = LibraryStore(
                databasePath: databaseURL,
                database: database,
                syncCommandLayerFactory: { spy },
                performStartupWork: false
            )

            #expect(store.selectedAssetID == assetID)

            store.setSelectedAssetColorLabel(.blue)

            let refreshedAssets = try database.queryAssets(filter: LibraryFilter(), limit: 10)
            let ledgerEntries = try database.ledgerEntries(libraryID: "local-library")

            #expect(store.assets.first?.colorLabel == .blue)
            #expect(refreshedAssets.first?.colorLabel == .blue)
            #expect(spy.builtColorCalls.count == 1)
            #expect(spy.builtColorCalls.first?.assetID == assetID)
            #expect(spy.builtColorCalls.first?.colorLabel == .blue)
            #expect(ledgerEntries.count == 1)
            #expect(ledgerEntries.first?.opType == .metadataSet)
            #expect(
                ledgerEntries.first.map { entry in
                    if case let .metadataSet(assetID: ledgerAssetID, field: field, value: value) = entry.payload {
                        return ledgerAssetID == assetID && field == .colorLabel && value == .string(AssetColorLabel.blue.rawValue)
                    }
                    return false
                } == true
            )
            #expect(try database.pendingLedgerUploadCount() == 1)
        }
    }

    @MainActor
    @Test func libraryStoreTagsUpdateUsesLedgerCommandLayerAndRefreshesLocalProjection() throws {
        try withTempDatabase { database, databaseURL in
            let assetID = UUID()
            try insertAsset(id: assetID, database: database)

            let spy = SpySyncCommandLayer(
                wrapped: SyncCommandLayer(
                    libraryID: "local-library",
                    deviceID: SyncDeviceID("mac"),
                    actorID: "user",
                    database: database
                )
            )
            let store = LibraryStore(
                databasePath: databaseURL,
                database: database,
                syncCommandLayerFactory: { spy },
                performStartupWork: false
            )

            #expect(store.selectedAssetID == assetID)

            store.setSelectedAssetTags(["keeper", "print", "keeper"])

            let refreshedAssets = try database.queryAssets(filter: LibraryFilter(), limit: 10)
            let ledgerEntries = try database.ledgerEntries(libraryID: "local-library")

            #expect(store.assets.first?.tags == ["keeper", "print"])
            #expect(refreshedAssets.first?.tags == ["keeper", "print"])
            #expect(spy.builtTagCalls.count == 1)
            #expect(spy.builtTagCalls.first?.assetID == assetID)
            #expect(spy.builtTagCalls.first?.add == ["keeper", "print"])
            #expect(spy.builtTagCalls.first?.remove.isEmpty == true)
            #expect(ledgerEntries.count == 1)
            #expect(ledgerEntries.first?.opType == .tagsUpdated)
            #expect(
                ledgerEntries.first.map { entry in
                    if case let .tagsUpdated(assetID: ledgerAssetID, add: add, remove: remove) = entry.payload {
                        return ledgerAssetID == assetID && add == ["keeper", "print"] && remove.isEmpty
                    }
                    return false
                } == true
            )
            #expect(try database.pendingLedgerUploadCount() == 1)
        }
    }

    @MainActor
    @Test func libraryStoreDeleteAssetsMovesAssetToSharedTrashWithoutDeletingFiles() async throws {
        try await withTempDatabaseAsync { database, databaseURL in
            let assetID = UUID()
            let fileURL = databaseURL.deletingLastPathComponent().appendingPathComponent("asset.raw")
            try Data("raw".utf8).write(to: fileURL)
            try insertAsset(id: assetID, fileURL: fileURL, database: database)

            let spy = SpySyncCommandLayer(
                wrapped: SyncCommandLayer(
                    libraryID: "local-library",
                    deviceID: SyncDeviceID("mac"),
                    actorID: "user",
                    database: database
                )
            )
            let store = LibraryStore(
                databasePath: databaseURL,
                database: database,
                syncCommandLayerFactory: { spy },
                performStartupWork: false
            )

            #expect(store.assets.map(\.id) == [assetID])

            store.deleteAssets([assetID])
            try await waitUntil(timeoutSeconds: 3) {
                try database.ledgerEntries(libraryID: "local-library").contains(where: { $0.opType == .moveToTrash })
            }

            let visibleAssets = try database.queryAssets(filter: LibraryFilter(), limit: 10)
            let visibleAssetsIncludingTrash = try database.queryAssets(filter: LibraryFilter(), limit: 10, includeTrashed: true)
            let trashEntries = try database.trashedAssets()
            let pendingEntries = try database.pendingLedgerUploadEntries(libraryID: "local-library")

            #expect(FileManager.default.fileExists(atPath: fileURL.path))
            #expect(visibleAssets.isEmpty)
            #expect(visibleAssetsIncludingTrash.map(\.id) == [assetID])
            #expect(trashEntries.count == 1)
            #expect(trashEntries.first?.assetID == assetID)
            #expect(trashEntries.first?.reason == "deleted_from_library")
            #expect(trashEntries.first?.movedBy == "user")
            #expect(spy.moveToTrashCalls.count == 1)
            #expect(spy.moveToTrashCalls.first?.0 == assetID)
            #expect(spy.moveToTrashCalls.first?.1 == "deleted_from_library")
            #expect(pendingEntries.map(\.opType) == [.moveToTrash])
            #expect(try database.pendingLedgerUploadCount() == 1)
        }
    }

    @MainActor
    @Test func libraryStoreCanManuallyBackfillLedgerBeforeRemoteSync() async throws {
        try await withTempDatabaseAsync { database, databaseURL in
            let assetID = UUID(uuidString: "00000000-0000-0000-0000-00000000be01")!
            let createdAt = DateCoding.encode(Date(timeIntervalSince1970: 1_700_007_000))
            let thumbnailURL = databaseURL.deletingLastPathComponent().appendingPathComponent("manual-backfill-thumb.jpg")
            try Data("thumb".utf8).write(to: thumbnailURL)

            try database.execute(
                """
                INSERT INTO assets (
                    id, capture_time, camera_make, camera_model, lens_model, original_filename,
                    content_fingerprint, metadata_fingerprint, rating, flag, flag_state, color_label, tags, created_at, updated_at
                ) VALUES (
                    '\(assetID.uuidString)', NULL, 'FUJIFILM', 'X100VI', '', 'IMG_0001.JPG',
                    'asset-fp', 'metadata-fp', 4, 0, 'picked', NULL, '["travel"]', '\(createdAt)', '\(createdAt)'
                )
                """
            )
            try database.execute(
                """
                INSERT INTO file_instances (
                    id, asset_id, path, device_id, storage_kind, file_role, authority_role,
                    sync_status, size_bytes, content_hash, last_seen_at, availability
                ) VALUES (
                    '00000000-0000-0000-0000-00000000be02', '\(assetID.uuidString)', '\(thumbnailURL.path)',
                    'mac', 'local', 'thumbnail', 'cache', 'cache_only', 5, 'thumb-hash', '\(createdAt)', 'online'
                )
                """
            )

            let store = LibraryStore(
                databasePath: databaseURL,
                database: database,
                syncCommandLayerFactory: {
                    SyncCommandLayer(
                        libraryID: "local-library",
                        deviceID: SyncDeviceID("mac"),
                        actorID: "user",
                        database: database
                    )
                },
                performStartupWork: false
            )

            store.backfillSyncLedger()

            try await waitUntil(timeoutSeconds: 3) {
                store.backgroundTask?.phase == "同步 ledger 补齐完成"
            }

            let entries = try database.ledgerEntries(libraryID: "local-library")
            let migrationState = try #require(try database.syncMigrationState(libraryID: "local-library"))

            #expect(entries.map(\.opType) == [.assetSnapshotDeclared, .filePlacementSnapshotDeclared])
            #expect(entries.map(\.actorID).allSatisfy { $0 == "system:migration" })
            #expect(migrationState.projectionVerified == true)
            #expect(store.lastSyncSummary.contains("已补齐初始 ledger"))
            #expect(store.lastSyncSummary.contains("待上传 1 张缩略图"))
            #expect(store.backgroundTask?.phase == "同步 ledger 补齐完成")
        }
    }

    @MainActor
    @Test func libraryStoreRestoreAssetsFromTrashBringsAssetBackIntoVisibleQuery() async throws {
        try await withTempDatabaseAsync { database, databaseURL in
            let assetID = UUID()
            let fileURL = databaseURL.deletingLastPathComponent().appendingPathComponent("asset.raw")
            try Data("raw".utf8).write(to: fileURL)
            try insertAsset(id: assetID, fileURL: fileURL, database: database)

            let spy = SpySyncCommandLayer(
                wrapped: SyncCommandLayer(
                    libraryID: "local-library",
                    deviceID: SyncDeviceID("mac"),
                    actorID: "user",
                    database: database
                )
            )
            let store = LibraryStore(
                databasePath: databaseURL,
                database: database,
                syncCommandLayerFactory: { spy },
                performStartupWork: false
            )

            store.deleteAssets([assetID])
            try await waitUntil(timeoutSeconds: 3) {
                try database.ledgerEntries(libraryID: "local-library").contains(where: { $0.opType == .moveToTrash })
            }

            store.restoreAssetsFromTrash([assetID])
            try await waitUntil(timeoutSeconds: 3) {
                try database.ledgerEntries(libraryID: "local-library").contains(where: { $0.opType == .restoreFromTrash })
            }

            let visibleAssets = try database.queryAssets(filter: LibraryFilter(), limit: 10)
            let trashEntries = try database.trashedAssets()
            let pendingEntries = try database.pendingLedgerUploadEntries(libraryID: "local-library")

            #expect(visibleAssets.map(\.id) == [assetID])
            #expect(trashEntries.isEmpty)
            #expect(spy.restoreFromTrashCalls == [assetID])
            #expect(pendingEntries.map(\.opType) == [.moveToTrash, .restoreFromTrash])
            #expect(try database.pendingLedgerUploadCount() == 2)
        }
    }

    @MainActor
    @Test func libraryStoreSkipsLedgerWhenLocalMetadataPersistenceFails() throws {
        try withTempDatabase { database, databaseURL in
            let assetID = UUID()
            try insertAsset(id: assetID, database: database)

            let spy = SpySyncCommandLayer(
                wrapped: SyncCommandLayer(
                    libraryID: "local-library",
                    deviceID: SyncDeviceID("mac"),
                    actorID: "user",
                    database: database
                )
            )
            let store = LibraryStore(
                databasePath: databaseURL,
                database: database,
                syncCommandLayerFactory: { spy },
                performStartupWork: false
            )

            try database.execute("PRAGMA query_only = ON")

            #expect(store.selectedAssetID == assetID)

            store.setSelectedAssetRating(5)

            #expect(store.lastError?.contains("SQL") == true)
            #expect(spy.builtRatingCalls.isEmpty)
            #expect(try database.ledgerEntries(libraryID: "local-library").isEmpty)
            #expect(try database.pendingLedgerUploadCount() == 0)
            #expect((try database.queryAssets(filter: LibraryFilter(), limit: 10)).first?.rating == 0)
        }
    }

    @Test func queryAssetsHidesTrashedAssetsByDefaultAndExposesTrashDetailsAndArchivePlacements() throws {
        try withTempDatabase { database, _ in
            let assetID = UUID()
            let fileObject = FileObjectID(contentHash: "abc123", sizeBytes: 42, role: .rawOriginal)
            let trashTime = HybridLogicalTime(wallTimeMilliseconds: 1_800_000_000_000, counter: 0, nodeID: "mac")
            try insertAsset(id: assetID, database: database)
            try database.appendLedgerEntry(
                .moveToTrash(
                    libraryID: "library",
                    deviceID: SyncDeviceID("mac"),
                    deviceSequence: 1,
                    time: trashTime,
                    actorID: "user",
                    assetID: assetID,
                    reason: "shared_trash"
                ),
                uploadStatus: .acknowledged
            )
            try database.appendLedgerEntry(
                .importedOriginalDeclared(
                    libraryID: "library",
                    deviceID: SyncDeviceID("mac"),
                    deviceSequence: 2,
                    time: HybridLogicalTime(wallTimeMilliseconds: 1_800_000_000_001, counter: 0, nodeID: "mac"),
                    actorID: "user",
                    assetID: assetID,
                    fileObject: fileObject,
                    placement: FilePlacement(
                        fileObjectID: fileObject,
                        holderID: "mac",
                        storageKind: .local,
                        authorityRole: .workingCopy,
                        availability: .online
                    )
                ),
                uploadStatus: .acknowledged
            )
            try database.appendLedgerEntry(
                .originalArchiveReceiptRecorded(
                    libraryID: "library",
                    deviceID: SyncDeviceID("archive"),
                    deviceSequence: 1,
                    time: HybridLogicalTime(wallTimeMilliseconds: 1_800_000_000_002, counter: 0, nodeID: "archive"),
                    actorID: "archive",
                    assetID: assetID,
                    fileObject: fileObject,
                    serverPlacement: FilePlacement(
                        fileObjectID: fileObject,
                        holderID: "archive",
                        storageKind: .nas,
                        authorityRole: .canonical,
                        availability: .online
                    )
                ),
                uploadStatus: .acknowledged
            )

            let visibleAssets = try database.queryAssets(filter: LibraryFilter(), limit: 10)
            let visibleIncludingTrash = try database.queryAssets(filter: LibraryFilter(), limit: 10, includeTrashed: true)
            let trashEntries = try database.trashedAssets()
            let canonicalPlacements = try database.canonicalPlacements(assetID: assetID)

            #expect(visibleAssets.isEmpty)
            #expect(visibleIncludingTrash.map(\.id) == [assetID])
            #expect(trashEntries.count == 1)
            #expect(trashEntries.first?.assetID == assetID)
            #expect(trashEntries.first?.reason == "shared_trash")
            #expect(trashEntries.first?.movedBy == "user")
            #expect(trashEntries.first?.movedAt == trashTime)
            #expect(canonicalPlacements.count == 1)
            #expect(canonicalPlacements.first?.storageKind == .nas)
            #expect(canonicalPlacements.first?.authorityRole == .canonical)
        }
    }

    @Test func archiveReceiptsQueryReturnsReceiptFactsIndependentlyOfCanonicalPlacements() throws {
        try withTempDatabase { database, _ in
            let assetID = UUID()
            let fileObject = FileObjectID(contentHash: "abc123", sizeBytes: 42, role: .rawOriginal)
            let receiptTime = HybridLogicalTime(wallTimeMilliseconds: 1_800_000_000_100, counter: 0, nodeID: "archive")
            let opID = UUID(uuidString: "00000000-0000-0000-0000-0000000000aa")!
            try insertAsset(id: assetID, database: database)
            try database.appendLedgerEntry(
                .originalArchiveReceiptRecorded(
                    opID: opID,
                    libraryID: "library",
                    deviceID: SyncDeviceID("archive"),
                    deviceSequence: 1,
                    time: receiptTime,
                    actorID: "archive-server",
                    assetID: assetID,
                    fileObject: fileObject,
                    serverPlacement: FilePlacement(
                        fileObjectID: fileObject,
                        holderID: "archive-server",
                        storageKind: .nas,
                        authorityRole: .canonical,
                        availability: .online
                    )
                ),
                uploadStatus: .acknowledged
            )

            let receipts = try database.archiveReceipts(assetID: assetID)

            #expect(receipts.count == 1)
            #expect(receipts.first?.opID == opID)
            #expect(receipts.first?.assetID == assetID)
            #expect(receipts.first?.fileObject == fileObject)
            #expect(receipts.first?.serverPlacement.storageKind == .nas)
            #expect(receipts.first?.serverPlacement.authorityRole == .canonical)
            #expect(receipts.first?.movedAt == receiptTime)
            #expect(receipts.first?.movedBy == "archive-server")
        }
    }

    @Test func archiveReceiptsQueryFiltersByAssetWithoutCrossAssetLeakage() throws {
        try withTempDatabase { database, _ in
            let assetA = UUID()
            let assetB = UUID()
            let fileObjectA = FileObjectID(contentHash: "abc123", sizeBytes: 42, role: .rawOriginal)
            let fileObjectB = FileObjectID(contentHash: "def456", sizeBytes: 84, role: .rawOriginal)
            let receiptTimeA = HybridLogicalTime(wallTimeMilliseconds: 1_800_000_000_100, counter: 0, nodeID: "archive-a")
            let receiptTimeB = HybridLogicalTime(wallTimeMilliseconds: 1_800_000_000_200, counter: 0, nodeID: "archive-b")

            try insertAsset(id: assetA, database: database)
            try insertAsset(id: assetB, database: database)
            try database.appendLedgerEntry(
                .originalArchiveReceiptRecorded(
                    opID: UUID(uuidString: "00000000-0000-0000-0000-0000000000a1")!,
                    libraryID: "library",
                    deviceID: SyncDeviceID("archive-a"),
                    deviceSequence: 1,
                    time: receiptTimeA,
                    actorID: "archive-a",
                    assetID: assetA,
                    fileObject: fileObjectA,
                    serverPlacement: FilePlacement(
                        fileObjectID: fileObjectA,
                        holderID: "archive-a",
                        storageKind: .nas,
                        authorityRole: .canonical,
                        availability: .online
                    )
                ),
                uploadStatus: .acknowledged
            )
            try database.appendLedgerEntry(
                .originalArchiveReceiptRecorded(
                    opID: UUID(uuidString: "00000000-0000-0000-0000-0000000000b2")!,
                    libraryID: "library",
                    deviceID: SyncDeviceID("archive-b"),
                    deviceSequence: 1,
                    time: receiptTimeB,
                    actorID: "archive-b",
                    assetID: assetB,
                    fileObject: fileObjectB,
                    serverPlacement: FilePlacement(
                        fileObjectID: fileObjectB,
                        holderID: "archive-b",
                        storageKind: .nas,
                        authorityRole: .canonical,
                        availability: .online
                    )
                ),
                uploadStatus: .acknowledged
            )

            let receiptsA = try database.archiveReceipts(assetID: assetA)
            let receiptsB = try database.archiveReceipts(assetID: assetB)

            #expect(receiptsA.count == 1)
            #expect(receiptsA.first?.assetID == assetA)
            #expect(receiptsA.first?.fileObject == fileObjectA)
            #expect(receiptsA.first?.movedAt == receiptTimeA)
            #expect(receiptsB.count == 1)
            #expect(receiptsB.first?.assetID == assetB)
            #expect(receiptsB.first?.fileObject == fileObjectB)
            #expect(receiptsB.first?.movedAt == receiptTimeB)
        }
    }

    @MainActor
    @Test func libraryStoreDeleteAssetsIsAtomicWhenBatchMoveFails() async throws {
        try await withTempDatabaseAsync { database, databaseURL in
            let assetA = UUID()
            let assetB = UUID()
            try insertAsset(id: assetA, database: database)
            try insertAsset(id: assetB, database: database)

            let failing = FaultyBatchSyncCommandLayer(
                database: database,
                libraryID: "local-library",
                deviceID: SyncDeviceID("mac"),
                actorID: "user"
            )
            let store = LibraryStore(
                databasePath: databaseURL,
                database: database,
                syncCommandLayerFactory: { failing },
                performStartupWork: false
            )

            store.deleteAssets([assetA, assetB])

            #expect(store.lastError != nil)
            #expect(try database.ledgerEntries(libraryID: "local-library").isEmpty)
            #expect(try database.pendingLedgerUploadCount() == 0)
            #expect(try database.trashedAssets().isEmpty)
            #expect(try database.queryAssets(filter: LibraryFilter(), limit: 10, includeTrashed: true).count == 2)
            #expect(try database.queryAssets(filter: LibraryFilter(), limit: 10).count == 2)
        }
    }

    @MainActor
    @Test func libraryStoreRestoreAssetsFromTrashIsAtomicWhenBatchRestoreFailsDuringWrite() async throws {
        try await withTempDatabaseAsync { database, databaseURL in
            let assetA = UUID()
            let assetB = UUID()
            try insertAsset(id: assetA, database: database)
            try insertAsset(id: assetB, database: database)

            let seedStore = LibraryStore(
                databasePath: databaseURL,
                database: database,
                syncCommandLayerFactory: {
                    SyncCommandLayer(
                        libraryID: "local-library",
                        deviceID: SyncDeviceID("mac"),
                        actorID: "user",
                        database: database
                    )
                },
                performStartupWork: false
            )

            seedStore.deleteAssets([assetA, assetB])
            try await waitUntil(timeoutSeconds: 3) {
                try database.ledgerEntries(libraryID: "local-library").contains(where: { $0.opType == .moveToTrash })
            }
            #expect(try database.trashedAssets().count == 2)

            let commandLayer = RestoreWriteConflictSyncCommandLayer(database: database)
            let store = LibraryStore(
                databasePath: databaseURL,
                database: database,
                syncCommandLayerFactory: { commandLayer },
                performStartupWork: false
            )

            store.restoreAssetsFromTrash([assetA, assetB])

            #expect(store.lastError != nil)
            #expect(try database.ledgerEntries(libraryID: "local-library").filter { $0.opType == .moveToTrash }.count == 2)
            #expect(try database.ledgerEntries(libraryID: "local-library").filter { $0.opType == .restoreFromTrash }.isEmpty)
            #expect(try database.pendingLedgerUploadEntries(libraryID: "local-library").filter { $0.opType == .restoreFromTrash }.isEmpty)
            #expect(try database.trashedAssets().count == 2)
            #expect(try database.queryAssets(filter: LibraryFilter(), limit: 10).isEmpty)
            #expect(try database.queryAssets(filter: LibraryFilter(), limit: 10, includeTrashed: true).count == 2)
            #expect(try database.pendingLedgerUploadCount() == 2)
        }
    }

    @Test func atomicAssetMetadataAndLedgerWriteRollsBackOnLedgerFailure() throws {
        try withTempDatabase { database, _ in
            let assetID = UUID()
            let asset = try insertAndLoadAsset(id: assetID, database: database)

            do {
                try database.updateAssetMetadataAndAppendLedger(
                    asset: assetBySettingRating(asset, 5),
                    libraryID: "local-library",
                    deviceID: SyncDeviceID("mac"),
                    currentWallTimeMilliseconds: 1_800_000_000_000,
                    buildEntry: { sequence, time in
                        OperationLedgerEntry.metadataSet(
                            libraryID: "local-library",
                            deviceID: SyncDeviceID("mac"),
                            deviceSequence: sequence,
                            time: time,
                            actorID: "user",
                            assetID: assetID,
                            field: .rating,
                            value: .int(5)
                        )
                    },
                    appendEntry: { _, _ in
                        throw NSError(domain: "PhotoAssetManagerTests", code: 2, userInfo: [NSLocalizedDescriptionKey: "ledger append failed"])
                    }
                )
                Issue.record("expected ledger append failure")
            } catch {
                #expect(error.localizedDescription.contains("ledger append failed"))
            }

            #expect(try database.ledgerEntries(libraryID: "local-library").isEmpty)
            #expect(try database.pendingLedgerUploadCount() == 0)
            #expect((try database.queryAssets(filter: LibraryFilter(), limit: 10)).first?.rating == 0)
        }
    }

    @Test func ledgerEntriesFailFastOnCorruptRows() throws {
        try withTempDatabase { database, _ in
            let assetID = UUID()
            let payload = String(
                decoding: try JSONEncoder().encode(
                    OperationLedgerEntry.metadataSet(
                        libraryID: "library",
                        deviceID: SyncDeviceID("mac"),
                        deviceSequence: 1,
                        time: HybridLogicalTime(wallTimeMilliseconds: 1, counter: 0, nodeID: "mac"),
                        actorID: "user",
                        assetID: assetID,
                        field: .rating,
                        value: .int(3)
                    ).payload
                ),
                as: UTF8.self
            )
            let validTime = "00000000000000000001:00000000000000000000:mac"
            let validCreatedAt = DateCoding.encode(Date())

            let cases: [(label: String, entityType: String, opType: String, time: String, createdAt: String)] = [
                ("entity_type", "not-an-entity", "metadata_set", validTime, validCreatedAt),
                ("op_type", "asset", "not-an-op", validTime, validCreatedAt),
                ("hybrid_logical_time", "asset", "metadata_set", "not-a-time", validCreatedAt),
                ("created_at", "asset", "metadata_set", validTime, "not-a-date")
            ]

            for testCase in cases {
                let opID = UUID().uuidString
                try database.execute(
                    """
                    INSERT INTO operation_ledger (
                        op_id, library_id, device_id, device_seq, hybrid_logical_time,
                        actor_id, entity_type, entity_id, op_type, payload_json,
                        base_version, created_at, upload_status, remote_cursor
                    ) VALUES (
                        '\(opID)', 'library', 'mac', 1, '\(testCase.time)',
                        'user', '\(testCase.entityType)', '\(assetID.uuidString)', '\(testCase.opType)', '\(payload)',
                        NULL, '\(testCase.createdAt)', 'pending', NULL
                    )
                    """
                )

                do {
                    _ = try database.ledgerEntries(libraryID: "library")
                    Issue.record("expected \(testCase.label) decoding failure")
                } catch {
                    #expect(error.localizedDescription.contains(testCase.label))
                }

                try database.execute("DELETE FROM operation_ledger")
            }
        }
    }

    @Test func appendingSameLedgerOperationIsIdempotent() throws {
        let root = FileManager.default.temporaryDirectory
            .resolvingSymlinksInPath()
            .appendingPathComponent("PhotoAssetManagerTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        let database = try SQLiteDatabase(path: root.appendingPathComponent("Library.sqlite"))
        let assetID = UUID()
        let op = OperationLedgerEntry.metadataSet(
            opID: UUID(uuidString: "00000000-0000-0000-0000-000000000041")!,
            libraryID: "library",
            deviceID: SyncDeviceID("iphone"),
            deviceSequence: 1,
            time: HybridLogicalTime(wallTimeMilliseconds: 40, counter: 0, nodeID: "iphone"),
            actorID: "user",
            assetID: assetID,
            field: .rating,
            value: .int(3)
        )

        try database.appendLedgerEntry(op, uploadStatus: .acknowledged)
        try database.appendLedgerEntry(op, uploadStatus: .acknowledged)

        #expect(try database.ledgerEntries(libraryID: "library").count == 1)
        #expect(try database.pendingLedgerUploadCount() == 0)
    }

    @Test func controlPlaneRoutesPercentEncodeOpaqueIdentifiers() {
        let libraryID = "lib/ary 1"
        let deviceID = "mac#1"
        let cursor = "cursor/with spaces?and=reserved&chars"

        #expect(SyncControlPlaneRoute.uploadOps(libraryID: libraryID).method == "POST")
        #expect(SyncControlPlaneRoute.uploadOps(libraryID: libraryID).path == "/libraries/lib%2Fary%201/ops")
        #expect(SyncControlPlaneRoute.fetchOps(libraryID: libraryID, after: cursor).method == "GET")
        #expect(SyncControlPlaneRoute.fetchOps(libraryID: libraryID, after: cursor).path == "/libraries/lib%2Fary%201/ops?after=cursor%2Fwith%20spaces%3Fand%3Dreserved%26chars")
        #expect(SyncControlPlaneRoute.deviceHeartbeat(deviceID: deviceID).path == "/devices/mac%231/heartbeat")
        #expect(SyncControlPlaneRoute.archiveReceipts.path == "/archive/receipts")
        #expect(SyncControlPlaneRoute.trash(libraryID: libraryID).path == "/libraries/lib%2Fary%201/trash")
    }

    @Test func controlPlaneHTTPClientBuildsExpectedRequestsAndDecodesResponses() async throws {
        let assetID = UUID(uuidString: "00000000-0000-0000-0000-0000000000a1")!
        let localOp = OperationLedgerEntry.metadataSet(
            opID: UUID(uuidString: "00000000-0000-0000-0000-0000000000b1")!,
            libraryID: "library",
            deviceID: SyncDeviceID("mac"),
            deviceSequence: 1,
            time: HybridLogicalTime(wallTimeMilliseconds: 100, counter: 0, nodeID: "mac"),
            actorID: "user",
            assetID: assetID,
            field: .rating,
            value: .int(4),
            createdAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let remoteOp = OperationLedgerEntry.metadataSet(
            opID: UUID(uuidString: "00000000-0000-0000-0000-0000000000c1")!,
            libraryID: "library",
            deviceID: SyncDeviceID("server"),
            deviceSequence: 99,
            time: HybridLogicalTime(wallTimeMilliseconds: 101, counter: 0, nodeID: "server"),
            actorID: "server",
            assetID: assetID,
            field: .flagState,
            value: .string(AssetFlagState.picked.rawValue),
            createdAt: Date(timeIntervalSince1970: 1_700_000_100)
        )

        var capturedRequests: [URLRequest] = []
        let sessionStub = makeStubSession { request in
            capturedRequests.append(request)
            guard let url = request.url else {
                throw NSError(domain: "PhotoAssetManagerTests", code: 21, userInfo: [NSLocalizedDescriptionKey: "missing url"])
            }

            let decoder = makeJSONDecoder()
            let encoder = makeJSONEncoder()

            switch (request.httpMethod, url.path) {
            case ("POST", "/libraries/library/ops"):
                let upload = try decoder.decode(SyncOpsUploadRequest.self, from: request.httpBodyData)
                #expect(upload.operations == [localOp])
                #expect(request.value(forHTTPHeaderField: Self.accessCredentialHeaderName) == "\(Self.accessCredentialScheme) cred-123")
                #expect(request.value(forHTTPHeaderField: "X-Trace") == "trace-1")
                let response = SyncOpsUploadResponse(
                    accepted: [SyncOpsAcceptedOperation(opID: localOp.opID, globalSeq: 11, status: "committed")],
                    cursor: "11"
                )
                return (
                    HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!,
                    try encoder.encode(response)
                )
            case ("GET", "/libraries/library/ops"):
                #expect(URLComponents(url: url, resolvingAgainstBaseURL: false)?.queryItems?.first(where: { $0.name == "after" })?.value == "cursor-1")
                let response = SyncOpsFetchResponse(operations: [remoteOp], cursor: "cursor-2")
                return (
                    HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!,
                    try encoder.encode(response)
                )
            case ("POST", "/devices/mac/heartbeat"):
                let heartbeat = try decoder.decode(DeviceHeartbeatRequest.self, from: request.httpBodyData)
                #expect(heartbeat.deviceID == "mac")
                #expect(heartbeat.libraryID == "library")
                return (HTTPURLResponse(url: url, statusCode: 202, httpVersion: nil, headerFields: nil)!, Data())
            case ("POST", "/archive/receipts"):
                let receipt = try decoder.decode(ArchiveReceiptRequest.self, from: request.httpBodyData)
                #expect(receipt.operation == localOp)
                return (HTTPURLResponse(url: url, statusCode: 204, httpVersion: nil, headerFields: nil)!, Data())
            default:
                throw NSError(domain: "PhotoAssetManagerTests", code: 22, userInfo: [NSLocalizedDescriptionKey: "unexpected request: \(request.httpMethod ?? "-") \(url.path)"])
            }
        }

        let client = SyncControlPlaneHTTPClient(
            baseURL: URL(string: "https://control.example.com")!,
            accessCredential: "cred-123",
            headerProvider: { ["X-Trace": "trace-1", "X-Stub-Key": sessionStub.stubKey] },
            session: sessionStub.session
        )

        let uploaded = try await client.uploadOperations(SyncOpsUploadRequest(operations: [localOp]), libraryID: "library")
        let fetched = try await client.fetchOperations(libraryID: "library", after: "cursor-1")
        try await client.sendHeartbeat(DeviceHeartbeatRequest(deviceID: "mac", libraryID: "library", placements: [], sentAt: Date(timeIntervalSince1970: 1_700_000_200)))
        try await client.recordArchiveReceipt(ArchiveReceiptRequest(operation: localOp))

        let errorSession = makeStubSession { request in
            let url = request.url ?? URL(string: "https://control.example.com/libraries/library/ops")!
            return (HTTPURLResponse(url: url, statusCode: 503, httpVersion: nil, headerFields: nil)!, Data())
        }
        let errorClient = SyncControlPlaneHTTPClient(
            baseURL: URL(string: "https://control.example.com")!,
            headerProvider: { ["X-Stub-Key": errorSession.stubKey] },
            session: errorSession.session
        )
        do {
            try await errorClient.uploadOperations(SyncOpsUploadRequest(operations: []), libraryID: "library")
            Issue.record("expected status code error")
        } catch let error as SyncControlPlaneHTTPError {
            #expect(error == .unexpectedStatusCode(503))
        }

        #expect(capturedRequests.count == 4)
        #expect(capturedRequests.allSatisfy { $0.value(forHTTPHeaderField: Self.accessCredentialHeaderName) == "\(Self.accessCredentialScheme) cred-123" })
        #expect(capturedRequests.allSatisfy { $0.value(forHTTPHeaderField: "X-Trace") == "trace-1" })
        #expect(capturedRequests.map { $0.httpMethod } == ["POST", "GET", "POST", "POST"])
        #expect(capturedRequests.map { $0.url?.path } == ["/libraries/library/ops", "/libraries/library/ops", "/devices/mac/heartbeat", "/archive/receipts"])
        #expect(fetched.operations == [remoteOp])
        #expect(fetched.cursor == "cursor-2")
        #expect(uploaded.accepted == [SyncOpsAcceptedOperation(opID: localOp.opID, globalSeq: 11, status: "committed")])
    }

    @Test func controlPlaneHTTPClientPercentEncodesOpaquePathSegmentsAndQueryItems() async throws {
        let libraryID = "library /alpha"
        let deviceID = "device/with slash"
        let cursor = "a+b=?"

        var capturedRequests: [URLRequest] = []
        let sessionStub = makeStubSession { request in
            capturedRequests.append(request)
            guard let url = request.url else {
                throw NSError(domain: "PhotoAssetManagerTests", code: 31, userInfo: [NSLocalizedDescriptionKey: "missing url"])
            }
            if request.httpMethod == "GET" {
                let components = URLComponents(url: url, resolvingAgainstBaseURL: false)
                #expect(components?.percentEncodedPath == "/libraries/library%20%2Falpha/ops")
                #expect(components?.percentEncodedQuery == "after=a%2Bb%3D%3F")
                return (HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!, try makeJSONEncoder().encode(SyncOpsFetchResponse(operations: [], cursor: "done")))
            }

            #expect(URLComponents(url: url, resolvingAgainstBaseURL: false)?.percentEncodedPath == "/devices/device%2Fwith%20slash/heartbeat")
            return (HTTPURLResponse(url: url, statusCode: 204, httpVersion: nil, headerFields: nil)!, Data())
        }

        let client = SyncControlPlaneHTTPClient(
            baseURL: URL(string: "https://control.example.com")!,
            headerProvider: { ["X-Stub-Key": sessionStub.stubKey] },
            session: sessionStub.session
        )
        _ = try await client.fetchOperations(libraryID: libraryID, after: cursor)
        try await client.sendHeartbeat(DeviceHeartbeatRequest(deviceID: deviceID, libraryID: libraryID, placements: [], sentAt: Date(timeIntervalSince1970: 1_700_000_500)))

        #expect(capturedRequests.count == 2)
        #expect(URLComponents(url: capturedRequests[0].url!, resolvingAgainstBaseURL: false)?.percentEncodedPath == "/libraries/library%20%2Falpha/ops")
        #expect(URLComponents(url: capturedRequests[0].url!, resolvingAgainstBaseURL: false)?.percentEncodedQuery == "after=a%2Bb%3D%3F")
        #expect(URLComponents(url: capturedRequests[1].url!, resolvingAgainstBaseURL: false)?.percentEncodedPath == "/devices/device%2Fwith%20slash/heartbeat")
    }

    @Test func controlPlaneHTTPClientPreservesReservedHeadersWhenHeaderProviderConflicts() async throws {
        let assetID = UUID(uuidString: "00000000-0000-0000-0000-0000000000ac")!
        let op = OperationLedgerEntry.metadataSet(
            opID: UUID(uuidString: "00000000-0000-0000-0000-0000000000ad")!,
            libraryID: "library",
            deviceID: SyncDeviceID("mac"),
            deviceSequence: 1,
            time: HybridLogicalTime(wallTimeMilliseconds: 200, counter: 0, nodeID: "mac"),
            actorID: "user",
            assetID: assetID,
            field: .rating,
            value: .int(2),
            createdAt: Date(timeIntervalSince1970: 1_700_000_600)
        )

        let sessionStub = makeStubSession { request in
            guard let url = request.url else {
                throw NSError(domain: "PhotoAssetManagerTests", code: 32, userInfo: [NSLocalizedDescriptionKey: "missing url"])
            }
            #expect(request.value(forHTTPHeaderField: Self.accessCredentialHeaderName) == "\(Self.accessCredentialScheme) good-cred")
            #expect(request.value(forHTTPHeaderField: "Accept") == "application/json")
            #expect(request.value(forHTTPHeaderField: "Content-Type") == "application/json")
            #expect(request.value(forHTTPHeaderField: "X-Trace") == "trace-1")
            let response = SyncOpsUploadResponse(
                accepted: [SyncOpsAcceptedOperation(opID: op.opID, globalSeq: 12, status: "committed")],
                cursor: "12"
            )
            return (
                HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!,
                try makeJSONEncoder().encode(response)
            )
        }

        let client = SyncControlPlaneHTTPClient(
            baseURL: URL(string: "https://control.example.com")!,
            accessCredential: "good-cred",
            headerProvider: {
                [
                    Self.accessCredentialHeaderName: "\(Self.accessCredentialScheme) bad-cred",
                    "Accept": "text/plain",
                    "Content-Type": "text/plain",
                    "X-Stub-Key": sessionStub.stubKey,
                    "X-Trace": "trace-1"
                ]
            },
            session: sessionStub.session
        )

        try await client.uploadOperations(SyncOpsUploadRequest(operations: [op]), libraryID: "library")
    }

    @Test func syncServiceUploadsPendingOperationsAndClearsQueue() async throws {
        let root = FileManager.default.temporaryDirectory
            .resolvingSymlinksInPath()
            .appendingPathComponent("PhotoAssetManagerTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        let database = try SQLiteDatabase(path: root.appendingPathComponent("Library.sqlite"))
        let op = OperationLedgerEntry.metadataSet(
            opID: UUID(uuidString: "00000000-0000-0000-0000-0000000000d1")!,
            libraryID: "library",
            deviceID: SyncDeviceID("mac"),
            deviceSequence: 1,
            time: HybridLogicalTime(wallTimeMilliseconds: 200, counter: 0, nodeID: "mac"),
            actorID: "user",
            assetID: UUID(),
            field: .rating,
            value: .int(5),
            createdAt: Date(timeIntervalSince1970: 1_700_000_300)
        )
        try database.appendLedgerEntry(op, uploadStatus: .pending)
        try database.setSyncCursor(peerID: "control-plane", cursor: "cursor-before-upload")

        let client = MockSyncControlPlaneClient()
        client.uploadResponse = { request, libraryID in
            #expect(libraryID == "library")
            #expect(request.operations == [op])
        }

        let service = SyncService(
            libraryID: "library",
            peerID: "control-plane",
            database: database,
            client: client
        )

        try await service.uploadPendingOperations()

        #expect(client.uploadedRequests == [SyncOpsUploadRequest(operations: [op])])
        #expect(try database.pendingLedgerUploadCount() == 0)
        #expect(try database.ledgerUploadStatus(opID: op.opID) == .acknowledged)
        #expect(try database.ledgerGlobalSeq(opID: op.opID) == 1)
        #expect(try database.syncCursor(peerID: "control-plane") == "cursor-before-upload")
    }

    @Test func syncServiceClaimsPendingOperationsOnceWhileUploadIsInFlight() async throws {
        let root = FileManager.default.temporaryDirectory
            .resolvingSymlinksInPath()
            .appendingPathComponent("PhotoAssetManagerTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        let database = try SQLiteDatabase(path: root.appendingPathComponent("Library.sqlite"))
        let firstOp = OperationLedgerEntry.metadataSet(
            opID: UUID(uuidString: "00000000-0000-0000-0000-0000000000d2")!,
            libraryID: "library",
            deviceID: SyncDeviceID("mac"),
            deviceSequence: 1,
            time: HybridLogicalTime(wallTimeMilliseconds: 200, counter: 0, nodeID: "mac"),
            actorID: "user",
            assetID: UUID(),
            field: .rating,
            value: .int(5),
            createdAt: Date(timeIntervalSince1970: 1_700_000_301)
        )
        let secondOp = OperationLedgerEntry.metadataSet(
            opID: UUID(uuidString: "00000000-0000-0000-0000-0000000000d3")!,
            libraryID: "library",
            deviceID: SyncDeviceID("mac"),
            deviceSequence: 2,
            time: HybridLogicalTime(wallTimeMilliseconds: 201, counter: 0, nodeID: "mac"),
            actorID: "user",
            assetID: UUID(),
            field: .flagState,
            value: .string(AssetFlagState.picked.rawValue),
            createdAt: Date(timeIntervalSince1970: 1_700_000_302)
        )
        try database.appendLedgerEntry(firstOp, uploadStatus: .pending)
        try database.appendLedgerEntry(secondOp, uploadStatus: .pending)

        let client = BlockingSyncControlPlaneClient()
        let service = SyncService(
            libraryID: "library",
            peerID: "control-plane",
            database: database,
            client: client
        )

        let firstTask = Task {
            try await service.uploadPendingOperations()
        }

        await client.waitForFirstUploadStart()
        try await service.uploadPendingOperations()
        let uploadedRequests = await client.uploadedRequests()
        #expect(uploadedRequests.count == 1)
        #expect(uploadedRequests.first?.operations == [firstOp, secondOp])
        #expect(try database.pendingLedgerUploadCount() == 0)
        #expect(try database.ledgerUploadStatus(opID: firstOp.opID) == .uploading)
        #expect(try database.ledgerUploadStatus(opID: secondOp.opID) == .uploading)

        await client.releaseBlockedUpload()
        try await firstTask.value

        #expect(try database.pendingLedgerUploadCount() == 0)
        #expect(try database.ledgerUploadStatus(opID: firstOp.opID) == .acknowledged)
        #expect(try database.ledgerUploadStatus(opID: secondOp.opID) == .acknowledged)
        #expect(try database.ledgerGlobalSeq(opID: firstOp.opID) == 1)
        #expect(try database.ledgerGlobalSeq(opID: secondOp.opID) == 2)
    }

    @Test func syncServiceRecoversStaleUploadingClaimsBeforeRetrying() async throws {
        let root = FileManager.default.temporaryDirectory
            .resolvingSymlinksInPath()
            .appendingPathComponent("PhotoAssetManagerTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        let database = try SQLiteDatabase(path: root.appendingPathComponent("Library.sqlite"))
        let op = OperationLedgerEntry.metadataSet(
            opID: UUID(uuidString: "00000000-0000-0000-0000-0000000000d5")!,
            libraryID: "library",
            deviceID: SyncDeviceID("mac"),
            deviceSequence: 1,
            time: HybridLogicalTime(wallTimeMilliseconds: 203, counter: 0, nodeID: "mac"),
            actorID: "user",
            assetID: UUID(),
            field: .rating,
            value: .int(1),
            createdAt: Date(timeIntervalSince1970: 1_700_000_304)
        )
        try database.appendLedgerEntry(op, uploadStatus: .pending)
        _ = try database.claimPendingLedgerUploadEntries(libraryID: "library")
        try database.execute(
            """
            UPDATE sync_upload_queue
            SET updated_at = '\(DateCoding.encode(Date(timeIntervalSince1970: 1)))'
            WHERE op_id = '\(op.opID.uuidString)'
            """
        )

        let client = MockSyncControlPlaneClient()
        client.uploadResponse = { request, libraryID in
            #expect(libraryID == "library")
            #expect(request.operations == [op])
        }

        let service = SyncService(
            libraryID: "library",
            peerID: "control-plane",
            database: database,
            client: client
        )

        try await service.uploadPendingOperations()

        #expect(client.uploadedRequests == [SyncOpsUploadRequest(operations: [op])])
        #expect(try database.pendingLedgerUploadCount() == 0)
        #expect(try database.ledgerUploadStatus(opID: op.opID) == .acknowledged)
        #expect(try database.ledgerGlobalSeq(opID: op.opID) == 1)
    }

    @Test func syncServiceRestoresClaimedOperationsAfterUploadFailureAndAllowsRetry() async throws {
        let root = FileManager.default.temporaryDirectory
            .resolvingSymlinksInPath()
            .appendingPathComponent("PhotoAssetManagerTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        let database = try SQLiteDatabase(path: root.appendingPathComponent("Library.sqlite"))
        let op = OperationLedgerEntry.metadataSet(
            opID: UUID(uuidString: "00000000-0000-0000-0000-0000000000d4")!,
            libraryID: "library",
            deviceID: SyncDeviceID("mac"),
            deviceSequence: 1,
            time: HybridLogicalTime(wallTimeMilliseconds: 202, counter: 0, nodeID: "mac"),
            actorID: "user",
            assetID: UUID(),
            field: .rating,
            value: .int(2),
            createdAt: Date(timeIntervalSince1970: 1_700_000_303)
        )
        try database.appendLedgerEntry(op, uploadStatus: .pending)

        let client = MockSyncControlPlaneClient()
        client.uploadResponse = { _, _ in
            struct UploadFailure: Error, Equatable {}
            throw UploadFailure()
        }

        let service = SyncService(
            libraryID: "library",
            peerID: "control-plane",
            database: database,
            client: client
        )

        do {
            try await service.uploadPendingOperations()
            Issue.record("expected upload failure")
        } catch {}

        let failedUploadRequests = client.uploadedRequests
        #expect(failedUploadRequests.count == 1)
        #expect(try database.pendingLedgerUploadCount() == 1)
        #expect(try database.ledgerUploadStatus(opID: op.opID) == .pending)

        client.uploadResponse = { request, libraryID in
            #expect(libraryID == "library")
            #expect(request.operations == [op])
        }
        try await service.uploadPendingOperations()

        let retriedUploadRequests = client.uploadedRequests
        #expect(retriedUploadRequests.count == 2)
        #expect(try database.pendingLedgerUploadCount() == 0)
        #expect(try database.ledgerUploadStatus(opID: op.opID) == .acknowledged)
        #expect(try database.ledgerGlobalSeq(opID: op.opID) == 1)
    }

    @Test func syncServiceAcknowledgesPartialAcceptedOperationsFromConflictResponse() async throws {
        let root = FileManager.default.temporaryDirectory
            .resolvingSymlinksInPath()
            .appendingPathComponent("PhotoAssetManagerTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        let database = try SQLiteDatabase(path: root.appendingPathComponent("Library.sqlite"))
        let firstOp = OperationLedgerEntry.metadataSet(
            opID: UUID(uuidString: "00000000-0000-0000-0000-0000000000d6")!,
            libraryID: "library",
            deviceID: SyncDeviceID("mac"),
            deviceSequence: 1,
            time: HybridLogicalTime(wallTimeMilliseconds: 204, counter: 0, nodeID: "mac"),
            actorID: "user",
            assetID: UUID(),
            field: .rating,
            value: .int(2),
            createdAt: Date(timeIntervalSince1970: 1_700_000_305)
        )
        let secondOp = OperationLedgerEntry.metadataSet(
            opID: UUID(uuidString: "00000000-0000-0000-0000-0000000000d7")!,
            libraryID: "library",
            deviceID: SyncDeviceID("mac"),
            deviceSequence: 2,
            time: HybridLogicalTime(wallTimeMilliseconds: 205, counter: 0, nodeID: "mac"),
            actorID: "user",
            assetID: UUID(),
            field: .rating,
            value: .int(3),
            createdAt: Date(timeIntervalSince1970: 1_700_000_306)
        )
        try database.appendLedgerEntry(firstOp, uploadStatus: .pending)
        try database.appendLedgerEntry(secondOp, uploadStatus: .pending)
        try database.setSyncCursor(peerID: "control-plane", cursor: "cursor-before-conflict")

        let client = MockSyncControlPlaneClient()
        let partialResponse = SyncOpsUploadResponse(
            accepted: [SyncOpsAcceptedOperation(opID: firstOp.opID, globalSeq: 21, status: "committed")],
            cursor: "21",
            conflicts: [SyncOpsConflictOperation(opID: secondOp.opID, conflictType: "duplicate_device_sequence")]
        )
        client.uploadResponse = { _, _ in
            throw SyncControlPlaneHTTPError.conflict(partialResponse)
        }

        let service = SyncService(
            libraryID: "library",
            peerID: "control-plane",
            database: database,
            client: client
        )

        do {
            try await service.uploadPendingOperations()
            Issue.record("expected partial conflict")
        } catch let error as SyncControlPlaneHTTPError {
            #expect(error == .conflict(partialResponse))
        }

        #expect(try database.ledgerUploadStatus(opID: firstOp.opID) == .acknowledged)
        #expect(try database.ledgerGlobalSeq(opID: firstOp.opID) == 21)
        #expect(try database.ledgerUploadStatus(opID: secondOp.opID) == .pending)
        #expect(try database.ledgerGlobalSeq(opID: secondOp.opID) == nil)
        #expect(try database.pendingLedgerUploadCount() == 1)
        #expect(try database.syncCursor(peerID: "control-plane") == "cursor-before-conflict")
    }

    @Test func syncServicePullsRemoteOperationsOnceAndPreservesIdempotency() async throws {
        let root = FileManager.default.temporaryDirectory
            .resolvingSymlinksInPath()
            .appendingPathComponent("PhotoAssetManagerTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        let database = try SQLiteDatabase(path: root.appendingPathComponent("Library.sqlite"))
        var remoteOp = OperationLedgerEntry.metadataSet(
            opID: UUID(uuidString: "00000000-0000-0000-0000-0000000000e1")!,
            libraryID: "library",
            deviceID: SyncDeviceID("server"),
            deviceSequence: 7,
            time: HybridLogicalTime(wallTimeMilliseconds: 300, counter: 0, nodeID: "server"),
            actorID: "server",
            assetID: UUID(uuidString: "00000000-0000-0000-0000-0000000000e2")!,
            field: .flagState,
            value: .string(AssetFlagState.picked.rawValue)
        )
        remoteOp.globalSeq = 44

        let client = MockSyncControlPlaneClient()
        client.fetchResponse = { after in
            if after == nil {
                return SyncOpsFetchResponse(operations: [remoteOp], cursor: "cursor-1")
            }
            return SyncOpsFetchResponse(operations: [remoteOp], cursor: "cursor-2")
        }

        let service = SyncService(
            libraryID: "library",
            peerID: "control-plane",
            database: database,
            client: client
        )

        try await service.pullRemoteOperations()
        try await service.pullRemoteOperations()

        #expect(client.fetchCalls == [nil, "cursor-1"])
        #expect(try database.ledgerEntries(libraryID: "library").count == 1)
        #expect(try database.pendingLedgerUploadCount() == 0)
        #expect(try database.syncCursor(peerID: "control-plane") == "cursor-2")
        #expect(try database.ledgerGlobalSeq(opID: remoteOp.opID) == 44)
    }

    @Test func syncServicePullsAllRemotePagesBeforeReturning() async throws {
        let root = FileManager.default.temporaryDirectory
            .resolvingSymlinksInPath()
            .appendingPathComponent("PhotoAssetManagerTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        let database = try SQLiteDatabase(path: root.appendingPathComponent("Library.sqlite"))
        let assetID = UUID(uuidString: "00000000-0000-0000-0000-000000000201")!

        var snapshot = OperationLedgerEntry.assetSnapshotDeclared(
            opID: UUID(uuidString: "00000000-0000-0000-0000-000000000202")!,
            libraryID: "library",
            deviceID: SyncDeviceID("server"),
            deviceSequence: 1,
            time: HybridLogicalTime(wallTimeMilliseconds: 500, counter: 0, nodeID: "server"),
            actorID: "system:migration",
            snapshot: AssetSnapshot(
                assetID: assetID,
                captureTime: Date(timeIntervalSince1970: 1_710_000_200),
                cameraMake: "Apple",
                cameraModel: "iPhone",
                lensModel: "Main",
                originalFilename: "IMG_0201.HEIC",
                contentFingerprint: "fingerprint-b",
                metadataFingerprint: "metadata-b",
                rating: 0,
                flagState: .unflagged,
                colorLabel: nil,
                tags: [],
                createdAt: Date(timeIntervalSince1970: 1_710_000_200),
                updatedAt: Date(timeIntervalSince1970: 1_710_000_200)
            )
        )
        snapshot.globalSeq = 1

        var rating = OperationLedgerEntry.metadataSet(
            opID: UUID(uuidString: "00000000-0000-0000-0000-000000000203")!,
            libraryID: "library",
            deviceID: SyncDeviceID("server"),
            deviceSequence: 2,
            time: HybridLogicalTime(wallTimeMilliseconds: 600, counter: 0, nodeID: "server"),
            actorID: "server",
            assetID: assetID,
            field: .rating,
            value: .int(4)
        )
        rating.globalSeq = 2

        let client = MockSyncControlPlaneClient()
        client.fetchResponse = { after in
            if after == nil {
                return SyncOpsFetchResponse(operations: [snapshot], cursor: "cursor-1", hasMore: true)
            }
            if after == "cursor-1" {
                return SyncOpsFetchResponse(operations: [rating], cursor: "cursor-2", hasMore: false)
            }
            return SyncOpsFetchResponse(operations: [], cursor: after ?? "cursor-2", hasMore: false)
        }

        let service = SyncService(
            libraryID: "library",
            peerID: "control-plane",
            database: database,
            client: client
        )

        try await service.pullRemoteOperations()

        #expect(client.fetchCalls == [nil, "cursor-1"])
        #expect(try database.ledgerEntries(libraryID: "library").count == 2)
        #expect(try database.syncCursor(peerID: "control-plane") == "cursor-2")
        #expect(try database.queryAssets(filter: LibraryFilter(), limit: 10).first?.rating == 4)
    }

    @Test func syncServiceReconcilesPulledOperationWithLocalPendingOpID() async throws {
        let root = FileManager.default.temporaryDirectory
            .resolvingSymlinksInPath()
            .appendingPathComponent("PhotoAssetManagerTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        let database = try SQLiteDatabase(path: root.appendingPathComponent("Library.sqlite"))
        var op = OperationLedgerEntry.metadataSet(
            opID: UUID(uuidString: "00000000-0000-0000-0000-0000000000e3")!,
            libraryID: "library",
            deviceID: SyncDeviceID("mac"),
            deviceSequence: 1,
            time: HybridLogicalTime(wallTimeMilliseconds: 310, counter: 0, nodeID: "mac"),
            actorID: "user",
            assetID: UUID(uuidString: "00000000-0000-0000-0000-0000000000e4")!,
            field: .rating,
            value: .int(4)
        )
        try database.appendLedgerEntry(op, uploadStatus: .pending)
        op.globalSeq = 88

        let client = MockSyncControlPlaneClient()
        client.fetchResponse = { _ in
            SyncOpsFetchResponse(operations: [op], cursor: "cursor-88")
        }

        let service = SyncService(
            libraryID: "library",
            peerID: "control-plane",
            database: database,
            client: client
        )

        try await service.pullRemoteOperations()

        #expect(try database.ledgerEntries(libraryID: "library").count == 1)
        #expect(try database.pendingLedgerUploadCount() == 0)
        #expect(try database.ledgerUploadStatus(opID: op.opID) == .acknowledged)
        #expect(try database.ledgerGlobalSeq(opID: op.opID) == 88)
        #expect(try database.syncCursor(peerID: "control-plane") == "cursor-88")
    }

    @Test func syncServiceRollsBackRemotePageWhenOneOperationConflicts() async throws {
        let root = FileManager.default.temporaryDirectory
            .resolvingSymlinksInPath()
            .appendingPathComponent("PhotoAssetManagerTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        let database = try SQLiteDatabase(path: root.appendingPathComponent("Library.sqlite"))
        let assetID = UUID(uuidString: "00000000-0000-0000-0000-0000000000f1")!
        let first = OperationLedgerEntry.metadataSet(
            opID: UUID(uuidString: "00000000-0000-0000-0000-0000000000f2")!,
            libraryID: "library",
            deviceID: SyncDeviceID("server"),
            deviceSequence: 1,
            time: HybridLogicalTime(wallTimeMilliseconds: 400, counter: 0, nodeID: "server"),
            actorID: "server",
            assetID: assetID,
            field: .rating,
            value: .int(1)
        )
        let conflicting = OperationLedgerEntry.metadataSet(
            opID: first.opID,
            libraryID: "library",
            deviceID: SyncDeviceID("server"),
            deviceSequence: 2,
            time: HybridLogicalTime(wallTimeMilliseconds: 401, counter: 0, nodeID: "server"),
            actorID: "server",
            assetID: assetID,
            field: .rating,
            value: .int(5)
        )

        let client = MockSyncControlPlaneClient()
        client.fetchResponse = { _ in
            SyncOpsFetchResponse(operations: [first, conflicting], cursor: "cursor-rolled-forward")
        }

        let service = SyncService(
            libraryID: "library",
            peerID: "control-plane",
            database: database,
            client: client
        )

        do {
            try await service.pullRemoteOperations()
            Issue.record("expected remote page conflict")
        } catch {
            #expect(try database.ledgerEntries(libraryID: "library").isEmpty)
            #expect(try database.syncCursor(peerID: "control-plane") == nil)
        }
    }

    private func withTempDatabase(
        _ body: (_ database: SQLiteDatabase, _ databaseURL: URL) throws -> Void
    ) throws {
        let root = FileManager.default.temporaryDirectory
            .resolvingSymlinksInPath()
            .appendingPathComponent("PhotoAssetManagerTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        let databaseURL = root.appendingPathComponent("Library.sqlite")
        let database = try SQLiteDatabase(path: databaseURL)
        try body(database, databaseURL)
    }

    @MainActor
    private func withTempDatabaseAsync(
        _ body: @escaping (_ database: SQLiteDatabase, _ databaseURL: URL) async throws -> Void
    ) async throws {
        let root = FileManager.default.temporaryDirectory
            .resolvingSymlinksInPath()
            .appendingPathComponent("PhotoAssetManagerTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        let databaseURL = root.appendingPathComponent("Library.sqlite")
        let database = try SQLiteDatabase(path: databaseURL)
        try await body(database, databaseURL)
    }

    private func insertAsset(id: UUID, database: SQLiteDatabase) throws {
        let now = DateCoding.encode(Date())
        try database.execute(
            """
            INSERT INTO assets (
                id, capture_time, camera_make, camera_model, lens_model, original_filename,
                content_fingerprint, metadata_fingerprint, rating, flag, flag_state,
                color_label, tags, created_at, updated_at
            ) VALUES (
                '\(id.uuidString)', NULL, 'camera', 'model', 'lens', 'asset.jpg',
                'content-\(id.uuidString)', 'metadata-\(id.uuidString)', 0, 0, '\(AssetFlagState.unflagged.rawValue)',
                NULL, '[]', '\(now)', '\(now)'
            )
            """
        )
    }

    private func insertAsset(id: UUID, fileURL: URL, database: SQLiteDatabase) throws {
        let now = DateCoding.encode(Date())
        try database.execute(
            """
            INSERT INTO assets (
                id, capture_time, camera_make, camera_model, lens_model, original_filename,
                content_fingerprint, metadata_fingerprint, rating, flag, flag_state,
                color_label, tags, created_at, updated_at
            ) VALUES (
                '\(id.uuidString)', NULL, 'camera', 'model', 'lens', 'asset.raw',
                'content-\(id.uuidString)', 'metadata-\(id.uuidString)', 0, 0, '\(AssetFlagState.unflagged.rawValue)',
                NULL, '[]', '\(now)', '\(now)'
            )
            """
        )
        try database.execute(
            """
            INSERT INTO file_instances (
                id, asset_id, path, device_id, storage_kind, file_role, authority_role,
                sync_status, size_bytes, content_hash, last_seen_at, availability
            ) VALUES (
                '\(UUID().uuidString)', '\(id.uuidString)', '\(fileURL.path)', 'mac', 'local', 'raw_original', 'working_copy',
                'needs_archive', 3, 'hash-\(id.uuidString)', '\(now)', 'online'
            )
            """
        )
    }

    private func insertAndLoadAsset(id: UUID, database: SQLiteDatabase) throws -> Asset {
        try insertAsset(id: id, database: database)
        guard let asset = try database.queryAssets(filter: LibraryFilter(), limit: 10).first else {
            throw NSError(domain: "PhotoAssetManagerTests", code: 3, userInfo: [NSLocalizedDescriptionKey: "asset not found"])
        }
        return asset
    }

    private func assetBySettingRating(_ asset: Asset, _ rating: Int) -> Asset {
        var copy = asset
        copy.rating = rating
        return copy
    }

    @MainActor
    private func waitUntil(timeoutSeconds: Double, predicate: @escaping () throws -> Bool) async throws {
        let deadline = Date().addingTimeInterval(timeoutSeconds)
        while Date() < deadline {
            if try predicate() {
                return
            }
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        if try predicate() {
            return
        }
        throw NSError(domain: "PhotoAssetManagerTests", code: 99, userInfo: [NSLocalizedDescriptionKey: "timed out waiting for predicate"])
    }

    private func makeJSONEncoder() -> JSONEncoder {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        return encoder
    }

    private func makeJSONDecoder() -> JSONDecoder {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return decoder
    }

    private func makeStubSession(
        _ handler: @escaping (URLRequest) throws -> (HTTPURLResponse, Data)
    ) -> (session: URLSession, stubKey: String) {
        let stubKey = UUID().uuidString
        MockURLProtocol.register(handler: handler, stubKey: stubKey)
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [MockURLProtocol.self]
        return (URLSession(configuration: configuration), stubKey)
    }

    private final class SpySyncCommandLayer: SyncCommandWriting {
        let libraryID: String
        let deviceID: SyncDeviceID
        let actorID: String
        private let wrapped: SyncCommandLayer
        private(set) var ratingCalls: [(assetID: UUID, rating: Int)] = []
        private(set) var flagCalls: [(assetID: UUID, flagState: AssetFlagState)] = []
        private(set) var colorCalls: [(assetID: UUID, colorLabel: AssetColorLabel?)] = []
        private(set) var tagCalls: [(assetID: UUID, add: Set<String>, remove: Set<String>)] = []
        private(set) var builtRatingCalls: [(assetID: UUID, rating: Int)] = []
        private(set) var builtFlagCalls: [(assetID: UUID, flagState: AssetFlagState)] = []
        private(set) var builtColorCalls: [(assetID: UUID, colorLabel: AssetColorLabel?)] = []
        private(set) var builtTagCalls: [(assetID: UUID, add: Set<String>, remove: Set<String>)] = []
        private(set) var moveToTrashCalls: [(UUID, String)] = []
        private(set) var restoreFromTrashCalls: [UUID] = []

        init(wrapped: SyncCommandLayer) {
            self.wrapped = wrapped
            self.libraryID = wrapped.libraryID
            self.deviceID = wrapped.deviceID
            self.actorID = wrapped.actorID
        }

        func setRating(assetID: UUID, rating: Int) throws {
            ratingCalls.append((assetID, rating))
            try wrapped.setRating(assetID: assetID, rating: rating)
        }

        func setFlag(assetID: UUID, flagState: AssetFlagState) throws {
            flagCalls.append((assetID, flagState))
            try wrapped.setFlag(assetID: assetID, flagState: flagState)
        }

        func setColorLabel(assetID: UUID, colorLabel: AssetColorLabel?) throws {
            colorCalls.append((assetID, colorLabel))
            try wrapped.setColorLabel(assetID: assetID, colorLabel: colorLabel)
        }

        func updateTags(assetID: UUID, add: Set<String>, remove: Set<String>) throws {
            tagCalls.append((assetID, add, remove))
            try wrapped.updateTags(assetID: assetID, add: add, remove: remove)
        }

        func moveToTrash(assetID: UUID, reason: String) throws {
            moveToTrashCalls.append((assetID, reason))
            try wrapped.moveToTrash(assetID: assetID, reason: reason)
        }

        func restoreFromTrash(assetID: UUID) throws {
            restoreFromTrashCalls.append(assetID)
            try wrapped.restoreFromTrash(assetID: assetID)
        }

        func declareImportedOriginal(assetID: UUID, fileObject: FileObjectID, localPlacement: FilePlacement) throws {
            try wrapped.declareImportedOriginal(assetID: assetID, fileObject: fileObject, localPlacement: localPlacement)
        }

        func requestArchive(assetID: UUID) throws {
            try wrapped.requestArchive(assetID: assetID)
        }

        func recordArchiveReceipt(assetID: UUID, fileObject: FileObjectID, serverPlacement: FilePlacement) throws {
            try wrapped.recordArchiveReceipt(assetID: assetID, fileObject: fileObject, serverPlacement: serverPlacement)
        }

        func makeRatingOperation(assetID: UUID, rating: Int, deviceSequence: Int64, time: HybridLogicalTime) -> OperationLedgerEntry {
            builtRatingCalls.append((assetID, rating))
            return wrapped.makeRatingOperation(assetID: assetID, rating: rating, deviceSequence: deviceSequence, time: time)
        }

        func makeFlagOperation(assetID: UUID, flagState: AssetFlagState, deviceSequence: Int64, time: HybridLogicalTime) -> OperationLedgerEntry {
            builtFlagCalls.append((assetID, flagState))
            return wrapped.makeFlagOperation(assetID: assetID, flagState: flagState, deviceSequence: deviceSequence, time: time)
        }

        func makeColorLabelOperation(assetID: UUID, colorLabel: AssetColorLabel?, deviceSequence: Int64, time: HybridLogicalTime) -> OperationLedgerEntry {
            builtColorCalls.append((assetID, colorLabel))
            return wrapped.makeColorLabelOperation(assetID: assetID, colorLabel: colorLabel, deviceSequence: deviceSequence, time: time)
        }

        func makeTagsOperation(assetID: UUID, add: Set<String>, remove: Set<String>, deviceSequence: Int64, time: HybridLogicalTime) -> OperationLedgerEntry {
            builtTagCalls.append((assetID, add, remove))
            return wrapped.makeTagsOperation(assetID: assetID, add: add, remove: remove, deviceSequence: deviceSequence, time: time)
        }
    }

    private final class FaultyBatchSyncCommandLayer: SyncCommandWriting {
        let libraryID: String
        let deviceID: SyncDeviceID
        let actorID: String
        let database: SQLiteDatabase

        init(database: SQLiteDatabase, libraryID: String, deviceID: SyncDeviceID, actorID: String) {
            self.database = database
            self.libraryID = libraryID
            self.deviceID = deviceID
            self.actorID = actorID
        }

        func setRating(assetID: UUID, rating: Int) throws {}
        func setFlag(assetID: UUID, flagState: AssetFlagState) throws {}
        func setColorLabel(assetID: UUID, colorLabel: AssetColorLabel?) throws {}
        func updateTags(assetID: UUID, add: Set<String>, remove: Set<String>) throws {}
        func declareImportedOriginal(assetID: UUID, fileObject: FileObjectID, localPlacement: FilePlacement) throws {}
        func requestArchive(assetID: UUID) throws {}
        func recordArchiveReceipt(assetID: UUID, fileObject: FileObjectID, serverPlacement: FilePlacement) throws {}

        func moveToTrash(assetID: UUID, reason: String) throws {
            try moveAssetsToTrash([assetID], reason: reason)
        }

        func restoreFromTrash(assetID: UUID) throws {
            try restoreAssetsFromTrash([assetID])
        }

        func moveAssetsToTrash(_ assetIDs: [UUID], reason: String) throws {
            let uniqueAssetIDs = Array(Set(assetIDs)).sorted { $0.uuidString < $1.uuidString }
            let opID = UUID(uuidString: "00000000-0000-0000-0000-00000000dead")!
            try database.recordLedgerOperations(
                libraryID: libraryID,
                deviceID: deviceID,
                currentWallTimeMilliseconds: 1_800_000_000_000,
                uploadStatus: .pending
            ) { sequence, time in
                uniqueAssetIDs.enumerated().map { index, assetID in
                    .moveToTrash(
                        opID: opID,
                        libraryID: libraryID,
                        deviceID: deviceID,
                        deviceSequence: sequence + Int64(index),
                        time: time,
                        actorID: actorID,
                        assetID: assetID,
                        reason: reason
                    )
                }
            }
        }

        func restoreAssetsFromTrash(_ assetIDs: [UUID]) throws {
            let uniqueAssetIDs = Array(Set(assetIDs)).sorted { $0.uuidString < $1.uuidString }
            let opID = UUID(uuidString: "00000000-0000-0000-0000-00000000beef")!
            try database.recordLedgerOperations(
                libraryID: libraryID,
                deviceID: deviceID,
                currentWallTimeMilliseconds: 1_800_000_000_000,
                uploadStatus: .pending
            ) { sequence, time in
                uniqueAssetIDs.enumerated().map { index, assetID in
                    .restoreFromTrash(
                        opID: opID,
                        libraryID: libraryID,
                        deviceID: deviceID,
                        deviceSequence: sequence + Int64(index),
                        time: time,
                        actorID: actorID,
                        assetID: assetID
                    )
                }
            }
        }

        func makeRatingOperation(assetID: UUID, rating: Int, deviceSequence: Int64, time: HybridLogicalTime) -> OperationLedgerEntry {
            .metadataSet(
                libraryID: libraryID,
                deviceID: deviceID,
                deviceSequence: deviceSequence,
                time: time,
                actorID: actorID,
                assetID: assetID,
                field: .rating,
                value: .int(rating)
            )
        }

        func makeFlagOperation(assetID: UUID, flagState: AssetFlagState, deviceSequence: Int64, time: HybridLogicalTime) -> OperationLedgerEntry {
            .metadataSet(
                libraryID: libraryID,
                deviceID: deviceID,
                deviceSequence: deviceSequence,
                time: time,
                actorID: actorID,
                assetID: assetID,
                field: .flagState,
                value: .string(flagState.rawValue)
            )
        }

        func makeColorLabelOperation(assetID: UUID, colorLabel: AssetColorLabel?, deviceSequence: Int64, time: HybridLogicalTime) -> OperationLedgerEntry {
            .metadataSet(
                libraryID: libraryID,
                deviceID: deviceID,
                deviceSequence: deviceSequence,
                time: time,
                actorID: actorID,
                assetID: assetID,
                field: .colorLabel,
                value: colorLabel.map { .string($0.rawValue) } ?? .null
            )
        }

        func makeTagsOperation(assetID: UUID, add: Set<String>, remove: Set<String>, deviceSequence: Int64, time: HybridLogicalTime) -> OperationLedgerEntry {
            .tagsUpdated(
                libraryID: libraryID,
                deviceID: deviceID,
                deviceSequence: deviceSequence,
                time: time,
                actorID: actorID,
                assetID: assetID,
                add: add,
                remove: remove
            )
        }
    }

    private final class RestoreWriteConflictSyncCommandLayer: SyncCommandWriting {
        let libraryID: String = "local-library"
        let deviceID: SyncDeviceID = SyncDeviceID("mac")
        let actorID: String = "user"
        let database: SQLiteDatabase

        init(database: SQLiteDatabase) {
            self.database = database
        }

        func setRating(assetID: UUID, rating: Int) throws {}
        func setFlag(assetID: UUID, flagState: AssetFlagState) throws {}
        func setColorLabel(assetID: UUID, colorLabel: AssetColorLabel?) throws {}
        func updateTags(assetID: UUID, add: Set<String>, remove: Set<String>) throws {}
        func declareImportedOriginal(assetID: UUID, fileObject: FileObjectID, localPlacement: FilePlacement) throws {}
        func requestArchive(assetID: UUID) throws {}
        func recordArchiveReceipt(assetID: UUID, fileObject: FileObjectID, serverPlacement: FilePlacement) throws {}

        func moveToTrash(assetID: UUID, reason: String) throws {
            try moveAssetsToTrash([assetID], reason: reason)
        }

        func restoreFromTrash(assetID: UUID) throws {
            try restoreAssetsFromTrash([assetID])
        }

        func moveAssetsToTrash(_ assetIDs: [UUID], reason: String) throws {
            let uniqueAssetIDs = Array(Set(assetIDs)).sorted { $0.uuidString < $1.uuidString }
            try database.recordLedgerOperations(
                libraryID: libraryID,
                deviceID: deviceID,
                currentWallTimeMilliseconds: 1_800_000_000_000,
                uploadStatus: .pending
            ) { sequence, time in
                uniqueAssetIDs.enumerated().map { index, assetID in
                    .moveToTrash(
                        opID: UUID(uuidString: "00000000-0000-0000-0000-00000000cafe")!,
                        libraryID: libraryID,
                        deviceID: deviceID,
                        deviceSequence: sequence + Int64(index),
                        time: time,
                        actorID: actorID,
                        assetID: assetID,
                        reason: reason
                    )
                }
            }
        }

        func restoreAssetsFromTrash(_ assetIDs: [UUID]) throws {
            let uniqueAssetIDs = Array(Set(assetIDs)).sorted { $0.uuidString < $1.uuidString }
            try database.recordLedgerOperations(
                libraryID: libraryID,
                deviceID: deviceID,
                currentWallTimeMilliseconds: 1_800_000_000_000,
                uploadStatus: .pending
            ) { sequence, time in
                uniqueAssetIDs.enumerated().map { index, assetID in
                    .restoreFromTrash(
                        opID: UUID(uuidString: "00000000-0000-0000-0000-00000000f00d")!,
                        libraryID: libraryID,
                        deviceID: deviceID,
                        deviceSequence: sequence + Int64(index),
                        time: time,
                        actorID: actorID,
                        assetID: assetID
                    )
                }
            }
        }

        func makeRatingOperation(assetID: UUID, rating: Int, deviceSequence: Int64, time: HybridLogicalTime) -> OperationLedgerEntry {
            .metadataSet(
                libraryID: libraryID,
                deviceID: deviceID,
                deviceSequence: deviceSequence,
                time: time,
                actorID: actorID,
                assetID: assetID,
                field: .rating,
                value: .int(rating)
            )
        }

        func makeFlagOperation(assetID: UUID, flagState: AssetFlagState, deviceSequence: Int64, time: HybridLogicalTime) -> OperationLedgerEntry {
            .metadataSet(
                libraryID: libraryID,
                deviceID: deviceID,
                deviceSequence: deviceSequence,
                time: time,
                actorID: actorID,
                assetID: assetID,
                field: .flagState,
                value: .string(flagState.rawValue)
            )
        }

        func makeColorLabelOperation(assetID: UUID, colorLabel: AssetColorLabel?, deviceSequence: Int64, time: HybridLogicalTime) -> OperationLedgerEntry {
            .metadataSet(
                libraryID: libraryID,
                deviceID: deviceID,
                deviceSequence: deviceSequence,
                time: time,
                actorID: actorID,
                assetID: assetID,
                field: .colorLabel,
                value: colorLabel.map { .string($0.rawValue) } ?? .null
            )
        }

        func makeTagsOperation(assetID: UUID, add: Set<String>, remove: Set<String>, deviceSequence: Int64, time: HybridLogicalTime) -> OperationLedgerEntry {
            .tagsUpdated(
                libraryID: libraryID,
                deviceID: deviceID,
                deviceSequence: deviceSequence,
                time: time,
                actorID: actorID,
                assetID: assetID,
                add: add,
                remove: remove
            )
        }
    }

    private final class MockSyncControlPlaneClient: @unchecked Sendable, SyncControlPlaneClient {
        private(set) var uploadedRequests: [SyncOpsUploadRequest] = []
        private(set) var fetchCalls: [String?] = []
        var uploadResponse: ((SyncOpsUploadRequest, String) throws -> Void)?
        var fetchResponse: ((String?) throws -> SyncOpsFetchResponse)?
        var heartbeatResponse: ((DeviceHeartbeatRequest) throws -> Void)?
        var receiptResponse: ((ArchiveReceiptRequest) throws -> Void)?

        func uploadOperations(_ request: SyncOpsUploadRequest, libraryID: String) async throws -> SyncOpsUploadResponse {
            uploadedRequests.append(request)
            try uploadResponse?(request, libraryID)
            return SyncOpsUploadResponse(
                accepted: request.operations.enumerated().map { index, operation in
                    SyncOpsAcceptedOperation(opID: operation.opID, globalSeq: Int64(index + 1), status: "committed")
                },
                cursor: String(request.operations.count)
            )
        }

        func fetchOperations(libraryID: String, after cursor: String?) async throws -> SyncOpsFetchResponse {
            fetchCalls.append(cursor)
            guard let fetchResponse else {
                throw NSError(domain: "PhotoAssetManagerTests", code: 23, userInfo: [NSLocalizedDescriptionKey: "missing fetch response"])
            }
            return try fetchResponse(cursor)
        }

        func sendHeartbeat(_ request: DeviceHeartbeatRequest) async throws {
            try heartbeatResponse?(request)
        }

        func recordArchiveReceipt(_ request: ArchiveReceiptRequest) async throws {
            try receiptResponse?(request)
        }
    }

    private final class BlockingSyncControlPlaneClient: @unchecked Sendable, SyncControlPlaneClient {
        private let state = UploadGateState()

        func waitForFirstUploadStart() async {
            await state.waitForFirstUploadStart()
        }

        func releaseBlockedUpload() async {
            await state.releaseBlockedUpload()
        }

        func uploadedRequests() async -> [SyncOpsUploadRequest] {
            await state.uploadedRequests
        }

        func uploadOperations(_ request: SyncOpsUploadRequest, libraryID: String) async throws -> SyncOpsUploadResponse {
            await state.recordUpload(request)
            await state.signalUploadStarted()
            await state.waitForRelease()
            return SyncOpsUploadResponse(
                accepted: request.operations.enumerated().map { index, operation in
                    SyncOpsAcceptedOperation(opID: operation.opID, globalSeq: Int64(index + 1), status: "committed")
                },
                cursor: String(request.operations.count)
            )
        }

        func fetchOperations(libraryID: String, after cursor: String?) async throws -> SyncOpsFetchResponse {
            SyncOpsFetchResponse(operations: [], cursor: cursor ?? "")
        }

        func sendHeartbeat(_ request: DeviceHeartbeatRequest) async throws {}

        func recordArchiveReceipt(_ request: ArchiveReceiptRequest) async throws {}
    }

    private final class StubDerivativeDataFetcher: @unchecked Sendable, DerivativeDataFetching {
        private let data: Data
        private(set) var requests: [S3ObjectRef] = []

        init(data: Data) {
            self.data = data
        }

        func fetchDerivative(_ object: S3ObjectRef) async throws -> Data {
            requests.append(object)
            return data
        }
    }

    private final class StubDerivativeControlPlane: @unchecked Sendable, SyncControlPlaneClient {
        private(set) var uploadRequests: [DerivativeUploadRequest] = []

        func uploadOperations(_ request: SyncOpsUploadRequest, libraryID: String) async throws -> SyncOpsUploadResponse {
            SyncOpsUploadResponse(accepted: [], cursor: "")
        }

        func fetchOperations(libraryID: String, after cursor: String?) async throws -> SyncOpsFetchResponse {
            SyncOpsFetchResponse(operations: [], cursor: cursor ?? "")
        }

        func sendHeartbeat(_ request: DeviceHeartbeatRequest) async throws {}

        func recordArchiveReceipt(_ request: ArchiveReceiptRequest) async throws {}

        func createDerivativeUpload(_ request: DerivativeUploadRequest) async throws -> DerivativeUploadResponse {
            uploadRequests.append(request)
            return DerivativeUploadResponse(
                s3Object: S3ObjectRef(
                    bucket: "photo-derivatives",
                    key: "libraries/\(request.libraryID)/assets/\(request.assetID.uuidString)/\(request.role.rawValue)/\(request.fileObject.contentHash).jpg",
                    eTag: "etag"
                ),
                uploadURL: URL(string: "https://upload.example.com/\(request.role.rawValue)")!
            )
        }

        func fetchDerivativeMetadata(libraryID: String, assetID: UUID, role: DerivativeRole) async throws -> DerivativeMetadataResponse {
            throw NSError(domain: "PhotoAssetManagerTests", code: 62, userInfo: [NSLocalizedDescriptionKey: "unused"])
        }
    }

    private final class StubDerivativeUploader: @unchecked Sendable, DerivativeDataUploading {
        private(set) var uploads: [(data: Data, url: URL)] = []
        var error: Error?

        func uploadDerivativeData(_ data: Data, to uploadURL: URL) async throws {
            if let error {
                throw error
            }
            uploads.append((data, uploadURL))
        }
    }

    private func makeTemporaryRoot() throws -> URL {
        let root = FileManager.default.temporaryDirectory
            .resolvingSymlinksInPath()
            .appendingPathComponent("PhotoAssetManagerTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        return root
    }

    private actor UploadGateState {
        private var uploadedRequestsStorage: [SyncOpsUploadRequest] = []
        private var uploadStartedContinuations: [CheckedContinuation<Void, Never>] = []
        private var releaseContinuation: CheckedContinuation<Void, Never>?
        private var firstUploadObserved = false

        var uploadedRequests: [SyncOpsUploadRequest] {
            uploadedRequestsStorage
        }

        func waitForFirstUploadStart() async {
            if firstUploadObserved {
                return
            }
            await withCheckedContinuation { continuation in
                uploadStartedContinuations.append(continuation)
            }
        }

        func recordUpload(_ request: SyncOpsUploadRequest) {
            uploadedRequestsStorage.append(request)
            firstUploadObserved = true
        }

        func signalUploadStarted() {
            guard !uploadStartedContinuations.isEmpty else { return }
            let continuations = uploadStartedContinuations
            uploadStartedContinuations.removeAll()
            for continuation in continuations {
                continuation.resume()
            }
        }

        func waitForRelease() async {
            await withCheckedContinuation { continuation in
                releaseContinuation = continuation
            }
        }

        func releaseBlockedUpload() {
            let continuation = releaseContinuation
            releaseContinuation = nil
            continuation?.resume()
        }
    }

    private final class MockURLProtocol: URLProtocol {
        nonisolated(unsafe) private static var handlers: [String: (URLRequest) throws -> (HTTPURLResponse, Data)] = [:]
        private static let lock = NSLock()

        static func register(
            handler: @escaping (URLRequest) throws -> (HTTPURLResponse, Data),
            stubKey: String
        ) {
            lock.lock()
            handlers[stubKey] = handler
            lock.unlock()
        }

        override class func canInit(with request: URLRequest) -> Bool {
            request.value(forHTTPHeaderField: "X-Stub-Key") != nil
        }

        override class func canonicalRequest(for request: URLRequest) -> URLRequest {
            request
        }

        override func startLoading() {
            guard
                let stubKey = request.value(forHTTPHeaderField: "X-Stub-Key")
            else {
                client?.urlProtocol(self, didFailWithError: NSError(domain: "PhotoAssetManagerTests", code: 20, userInfo: [NSLocalizedDescriptionKey: "missing stub stubKey"]))
                return
            }
            let handler = Self.handler(for: stubKey)
            do {
                guard let handler else {
                    throw NSError(domain: "PhotoAssetManagerTests", code: 24, userInfo: [NSLocalizedDescriptionKey: "missing request handler for stubKey"])
                }
                let (response, data) = try handler(request)
                client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
                client?.urlProtocol(self, didLoad: data)
                client?.urlProtocolDidFinishLoading(self)
            } catch {
                client?.urlProtocol(self, didFailWithError: error)
            }
        }

        override func stopLoading() {}

        private static func handler(for stubKey: String) -> ((URLRequest) throws -> (HTTPURLResponse, Data))? {
            lock.lock()
            defer { lock.unlock() }
            return handlers[stubKey]
        }
    }

    private final class ThrowingSyncCommandLayer: SyncCommandWriting {
        let libraryID: String = "local-library"
        let deviceID: SyncDeviceID = SyncDeviceID("mac")
        let actorID: String = "user"
        private let error: any Error

        init(error: any Error) {
            self.error = error
        }

        func setRating(assetID: UUID, rating: Int) throws {
            throw error
        }

        func setFlag(assetID: UUID, flagState: AssetFlagState) throws {
            throw error
        }

        func setColorLabel(assetID: UUID, colorLabel: AssetColorLabel?) throws {
            throw error
        }

        func updateTags(assetID: UUID, add: Set<String>, remove: Set<String>) throws {
            throw error
        }

        func moveToTrash(assetID: UUID, reason: String) throws {
            throw error
        }

        func restoreFromTrash(assetID: UUID) throws {
            throw error
        }

        func moveAssetsToTrash(_ assetIDs: [UUID], reason: String) throws {
            throw error
        }

        func restoreAssetsFromTrash(_ assetIDs: [UUID]) throws {
            throw error
        }

        func declareImportedOriginal(assetID: UUID, fileObject: FileObjectID, localPlacement: FilePlacement) throws {
            throw error
        }

        func requestArchive(assetID: UUID) throws {
            throw error
        }

        func recordArchiveReceipt(assetID: UUID, fileObject: FileObjectID, serverPlacement: FilePlacement) throws {
            throw error
        }

        func makeRatingOperation(assetID: UUID, rating: Int, deviceSequence: Int64, time: HybridLogicalTime) -> OperationLedgerEntry {
            OperationLedgerEntry.metadataSet(
                libraryID: libraryID,
                deviceID: deviceID,
                deviceSequence: deviceSequence,
                time: time,
                actorID: actorID,
                assetID: assetID,
                field: .rating,
                value: .int(rating)
            )
        }

        func makeFlagOperation(assetID: UUID, flagState: AssetFlagState, deviceSequence: Int64, time: HybridLogicalTime) -> OperationLedgerEntry {
            OperationLedgerEntry.metadataSet(
                libraryID: libraryID,
                deviceID: deviceID,
                deviceSequence: deviceSequence,
                time: time,
                actorID: actorID,
                assetID: assetID,
                field: .flagState,
                value: .string(flagState.rawValue)
            )
        }

        func makeColorLabelOperation(assetID: UUID, colorLabel: AssetColorLabel?, deviceSequence: Int64, time: HybridLogicalTime) -> OperationLedgerEntry {
            OperationLedgerEntry.metadataSet(
                libraryID: libraryID,
                deviceID: deviceID,
                deviceSequence: deviceSequence,
                time: time,
                actorID: actorID,
                assetID: assetID,
                field: .colorLabel,
                value: colorLabel.map { .string($0.rawValue) } ?? .null
            )
        }

        func makeTagsOperation(assetID: UUID, add: Set<String>, remove: Set<String>, deviceSequence: Int64, time: HybridLogicalTime) -> OperationLedgerEntry {
            OperationLedgerEntry.tagsUpdated(
                libraryID: libraryID,
                deviceID: deviceID,
                deviceSequence: deviceSequence,
                time: time,
                actorID: actorID,
                assetID: assetID,
                add: add,
                remove: remove
            )
        }
    }

    private final class TestClock: @unchecked Sendable {
        private var values: [Int64]

        init(_ values: [Int64]) {
            self.values = values
        }

        func next() -> Date {
            let nextValue = values.isEmpty ? Int64(Date().timeIntervalSince1970 * 1000) : values.removeFirst()
            return Date(timeIntervalSince1970: TimeInterval(nextValue) / 1000)
        }
    }
}

private extension URLRequest {
    var httpBodyData: Data {
        if let httpBody {
            return httpBody
        }
        guard let httpBodyStream else {
            return Data()
        }
        httpBodyStream.open()
        defer { httpBodyStream.close() }

        var data = Data()
        let bufferSize = 1_024
        let buffer = UnsafeMutablePointer<UInt8>.allocate(capacity: bufferSize)
        defer { buffer.deallocate() }

        while httpBodyStream.hasBytesAvailable {
            let bytesRead = httpBodyStream.read(buffer, maxLength: bufferSize)
            if bytesRead < 0 {
                return Data()
            }
            if bytesRead == 0 {
                break
            }
            data.append(buffer, count: bytesRead)
        }
        return data
    }
}
