import Foundation

struct SyncDeviceID: RawRepresentable, Hashable, Codable, Sendable {
    let rawValue: String

    init(_ rawValue: String) {
        self.rawValue = rawValue
    }

    init(rawValue: String) {
        self.rawValue = rawValue
    }
}

struct HybridLogicalTime: Comparable, Codable, Hashable, Sendable {
    var wallTimeMilliseconds: Int64
    var counter: Int64
    var nodeID: String

    static func < (lhs: HybridLogicalTime, rhs: HybridLogicalTime) -> Bool {
        if lhs.wallTimeMilliseconds != rhs.wallTimeMilliseconds {
            return lhs.wallTimeMilliseconds < rhs.wallTimeMilliseconds
        }
        if lhs.counter != rhs.counter {
            return lhs.counter < rhs.counter
        }
        return lhs.nodeID < rhs.nodeID
    }
}

enum LedgerEntityType: String, Codable, Sendable {
    case asset
    case fileObject = "file_object"
    case filePlacement = "file_placement"
}

enum LedgerOperationType: String, Codable, Sendable {
    case metadataSet = "metadata_set"
    case tagsUpdated = "tags_updated"
    case moveToTrash = "move_to_trash"
    case restoreFromTrash = "restore_from_trash"
    case importedOriginalDeclared = "imported_original_declared"
    case archiveRequested = "archive_requested"
    case originalArchiveReceiptRecorded = "original_archive_receipt_recorded"
}

enum AssetMetadataField: String, Codable, Hashable, Sendable {
    case rating
    case flagState = "flag_state"
    case colorLabel = "color_label"
    case caption
}

enum LedgerValue: Codable, Equatable, Sendable {
    case int(Int)
    case string(String)
    case null
}

struct FileObjectID: Hashable, Codable, Sendable {
    var contentHash: String
    var sizeBytes: Int64
    var role: FileRole

    var stableKey: String {
        "\(role.rawValue):\(sizeBytes):\(contentHash)"
    }
}

struct FileObject: Hashable, Codable, Sendable {
    var id: FileObjectID
    var contentHash: String
    var sizeBytes: Int64
    var role: FileRole
}

struct FilePlacement: Hashable, Codable, Sendable {
    var fileObjectID: FileObjectID
    var holderID: String
    var storageKind: StorageKind
    var authorityRole: AuthorityRole
    var availability: Availability
}

enum ProjectedTrashState: String, Codable, Sendable {
    case active
    case trashed
}

enum ProjectedArchiveState: String, Codable, Sendable {
    case pendingOriginalUpload = "pending_original_upload"
    case archived
}

struct ProjectedAsset: Hashable, Sendable {
    var id: UUID
    var rating: Int = 0
    var flagState: AssetFlagState = .unflagged
    var colorLabel: AssetColorLabel?
    var caption: String?
    var tags: [String] = []
    var trashState: ProjectedTrashState = .active
    var archiveState: ProjectedArchiveState?
}

struct ProjectedTrashEntry: Hashable, Sendable {
    var assetID: UUID
    var reason: String
    var movedAt: HybridLogicalTime
    var movedBy: String
}

struct TrashedAssetRecord: Hashable, Sendable {
    var assetID: UUID
    var reason: String
    var movedAt: HybridLogicalTime
    var movedBy: String
}

struct ArchiveReceiptRecord: Hashable, Sendable {
    var opID: UUID
    var assetID: UUID
    var fileObject: FileObjectID
    var serverPlacement: FilePlacement
    var movedAt: HybridLogicalTime
    var movedBy: String
}

struct SyncConflict: Hashable, Sendable {
    var assetID: UUID
    var field: AssetMetadataField
    var leftOperationID: UUID
    var rightOperationID: UUID
    var message: String
}

struct SyncLedgerProjection: Sendable {
    var assets: [UUID: ProjectedAsset] = [:]
    var trash: [UUID: ProjectedTrashEntry] = [:]
    var fileObjects: [FileObjectID: FileObject] = [:]
    var filePlacements: [FileObjectID: [FilePlacement]] = [:]
    var conflicts: [SyncConflict] = []
}

enum OperationPayload: Codable, Equatable, Sendable {
    case metadataSet(assetID: UUID, field: AssetMetadataField, value: LedgerValue)
    case tagsUpdated(assetID: UUID, add: Set<String>, remove: Set<String>)
    case moveToTrash(assetID: UUID, reason: String)
    case restoreFromTrash(assetID: UUID)
    case importedOriginalDeclared(assetID: UUID, fileObject: FileObjectID, placement: FilePlacement)
    case archiveRequested(assetID: UUID)
    case originalArchiveReceiptRecorded(assetID: UUID, fileObject: FileObjectID, serverPlacement: FilePlacement)
}

struct OperationLedgerEntry: Identifiable, Codable, Equatable, Sendable {
    var id: UUID { opID }

    var opID: UUID
    var libraryID: String
    var deviceID: SyncDeviceID
    var deviceSequence: Int64
    var hybridLogicalTime: HybridLogicalTime
    var actorID: String
    var entityType: LedgerEntityType
    var entityID: String
    var opType: LedgerOperationType
    var payload: OperationPayload
    var baseVersion: String?
    var createdAt: Date

    static func metadataSet(
        opID: UUID = UUID(),
        libraryID: String,
        deviceID: SyncDeviceID,
        deviceSequence: Int64,
        time: HybridLogicalTime,
        actorID: String,
        assetID: UUID,
        field: AssetMetadataField,
        value: LedgerValue,
        baseVersion: String? = nil,
        createdAt: Date = Date()
    ) -> OperationLedgerEntry {
        OperationLedgerEntry(
            opID: opID,
            libraryID: libraryID,
            deviceID: deviceID,
            deviceSequence: deviceSequence,
            hybridLogicalTime: time,
            actorID: actorID,
            entityType: .asset,
            entityID: assetID.uuidString,
            opType: .metadataSet,
            payload: .metadataSet(assetID: assetID, field: field, value: value),
            baseVersion: baseVersion,
            createdAt: createdAt
        )
    }

    static func tagsUpdated(
        opID: UUID = UUID(),
        libraryID: String,
        deviceID: SyncDeviceID,
        deviceSequence: Int64,
        time: HybridLogicalTime,
        actorID: String,
        assetID: UUID,
        add: Set<String>,
        remove: Set<String>,
        baseVersion: String? = nil,
        createdAt: Date = Date()
    ) -> OperationLedgerEntry {
        OperationLedgerEntry(
            opID: opID,
            libraryID: libraryID,
            deviceID: deviceID,
            deviceSequence: deviceSequence,
            hybridLogicalTime: time,
            actorID: actorID,
            entityType: .asset,
            entityID: assetID.uuidString,
            opType: .tagsUpdated,
            payload: .tagsUpdated(assetID: assetID, add: add, remove: remove),
            baseVersion: baseVersion,
            createdAt: createdAt
        )
    }

    static func moveToTrash(
        opID: UUID = UUID(),
        libraryID: String,
        deviceID: SyncDeviceID,
        deviceSequence: Int64,
        time: HybridLogicalTime,
        actorID: String,
        assetID: UUID,
        reason: String,
        baseVersion: String? = nil,
        createdAt: Date = Date()
    ) -> OperationLedgerEntry {
        OperationLedgerEntry(
            opID: opID,
            libraryID: libraryID,
            deviceID: deviceID,
            deviceSequence: deviceSequence,
            hybridLogicalTime: time,
            actorID: actorID,
            entityType: .asset,
            entityID: assetID.uuidString,
            opType: .moveToTrash,
            payload: .moveToTrash(assetID: assetID, reason: reason),
            baseVersion: baseVersion,
            createdAt: createdAt
        )
    }

    static func restoreFromTrash(
        opID: UUID = UUID(),
        libraryID: String,
        deviceID: SyncDeviceID,
        deviceSequence: Int64,
        time: HybridLogicalTime,
        actorID: String,
        assetID: UUID,
        baseVersion: String? = nil,
        createdAt: Date = Date()
    ) -> OperationLedgerEntry {
        OperationLedgerEntry(
            opID: opID,
            libraryID: libraryID,
            deviceID: deviceID,
            deviceSequence: deviceSequence,
            hybridLogicalTime: time,
            actorID: actorID,
            entityType: .asset,
            entityID: assetID.uuidString,
            opType: .restoreFromTrash,
            payload: .restoreFromTrash(assetID: assetID),
            baseVersion: baseVersion,
            createdAt: createdAt
        )
    }

    static func importedOriginalDeclared(
        opID: UUID = UUID(),
        libraryID: String,
        deviceID: SyncDeviceID,
        deviceSequence: Int64,
        time: HybridLogicalTime,
        actorID: String,
        assetID: UUID,
        fileObject: FileObjectID,
        placement: FilePlacement,
        baseVersion: String? = nil,
        createdAt: Date = Date()
    ) -> OperationLedgerEntry {
        OperationLedgerEntry(
            opID: opID,
            libraryID: libraryID,
            deviceID: deviceID,
            deviceSequence: deviceSequence,
            hybridLogicalTime: time,
            actorID: actorID,
            entityType: .fileObject,
            entityID: fileObject.stableKey,
            opType: .importedOriginalDeclared,
            payload: .importedOriginalDeclared(assetID: assetID, fileObject: fileObject, placement: placement),
            baseVersion: baseVersion,
            createdAt: createdAt
        )
    }

    static func originalArchiveReceiptRecorded(
        opID: UUID = UUID(),
        libraryID: String,
        deviceID: SyncDeviceID,
        deviceSequence: Int64,
        time: HybridLogicalTime,
        actorID: String,
        assetID: UUID,
        fileObject: FileObjectID,
        serverPlacement: FilePlacement,
        baseVersion: String? = nil,
        createdAt: Date = Date()
    ) -> OperationLedgerEntry {
        OperationLedgerEntry(
            opID: opID,
            libraryID: libraryID,
            deviceID: deviceID,
            deviceSequence: deviceSequence,
            hybridLogicalTime: time,
            actorID: actorID,
            entityType: .filePlacement,
            entityID: assetID.uuidString,
            opType: .originalArchiveReceiptRecorded,
            payload: .originalArchiveReceiptRecorded(assetID: assetID, fileObject: fileObject, serverPlacement: serverPlacement),
            baseVersion: baseVersion,
            createdAt: createdAt
        )
    }

    static func archiveRequested(
        opID: UUID = UUID(),
        libraryID: String,
        deviceID: SyncDeviceID,
        deviceSequence: Int64,
        time: HybridLogicalTime,
        actorID: String,
        assetID: UUID,
        baseVersion: String? = nil,
        createdAt: Date = Date()
    ) -> OperationLedgerEntry {
        OperationLedgerEntry(
            opID: opID,
            libraryID: libraryID,
            deviceID: deviceID,
            deviceSequence: deviceSequence,
            hybridLogicalTime: time,
            actorID: actorID,
            entityType: .asset,
            entityID: assetID.uuidString,
            opType: .archiveRequested,
            payload: .archiveRequested(assetID: assetID),
            baseVersion: baseVersion,
            createdAt: createdAt
        )
    }
}

enum LedgerUploadStatus: String, Codable, Sendable {
    case pending
    case uploading
    case acknowledged
}

protocol SyncCommandWriting {
    var libraryID: String { get }
    var deviceID: SyncDeviceID { get }
    var actorID: String { get }

    func setRating(assetID: UUID, rating: Int) throws
    func setFlag(assetID: UUID, flagState: AssetFlagState) throws
    func setColorLabel(assetID: UUID, colorLabel: AssetColorLabel?) throws
    func updateTags(assetID: UUID, add: Set<String>, remove: Set<String>) throws
    func moveToTrash(assetID: UUID, reason: String) throws
    func restoreFromTrash(assetID: UUID) throws
    func moveAssetsToTrash(_ assetIDs: [UUID], reason: String) throws
    func restoreAssetsFromTrash(_ assetIDs: [UUID]) throws
    func declareImportedOriginal(assetID: UUID, fileObject: FileObjectID, localPlacement: FilePlacement) throws
    func requestArchive(assetID: UUID) throws
    func recordArchiveReceipt(assetID: UUID, fileObject: FileObjectID, serverPlacement: FilePlacement) throws

    func makeRatingOperation(assetID: UUID, rating: Int, deviceSequence: Int64, time: HybridLogicalTime) -> OperationLedgerEntry
    func makeFlagOperation(assetID: UUID, flagState: AssetFlagState, deviceSequence: Int64, time: HybridLogicalTime) -> OperationLedgerEntry
    func makeColorLabelOperation(assetID: UUID, colorLabel: AssetColorLabel?, deviceSequence: Int64, time: HybridLogicalTime) -> OperationLedgerEntry
    func makeTagsOperation(assetID: UUID, add: Set<String>, remove: Set<String>, deviceSequence: Int64, time: HybridLogicalTime) -> OperationLedgerEntry
}

struct SyncCommandLayer: SyncCommandWriting, Sendable {
    var libraryID: String
    var deviceID: SyncDeviceID
    var actorID: String
    let database: SQLiteDatabase
    var nowProvider: @Sendable () -> Date = Date.init

    func setRating(assetID: UUID, rating: Int) throws {
        guard (0...5).contains(rating) else {
            throw SyncCommandError.invalidRating(rating)
        }
        let wallTime = Int64(nowProvider().timeIntervalSince1970 * 1000)
        try database.recordLedgerOperation(
            libraryID: libraryID,
            deviceID: deviceID,
            currentWallTimeMilliseconds: wallTime,
            uploadStatus: .pending
        ) { sequence, time in
            makeRatingOperation(assetID: assetID, rating: rating, deviceSequence: sequence, time: time)
        }
    }

    func setFlag(assetID: UUID, flagState: AssetFlagState) throws {
        let wallTime = Int64(nowProvider().timeIntervalSince1970 * 1000)
        try database.recordLedgerOperation(
            libraryID: libraryID,
            deviceID: deviceID,
            currentWallTimeMilliseconds: wallTime,
            uploadStatus: .pending
        ) { sequence, time in
            makeFlagOperation(assetID: assetID, flagState: flagState, deviceSequence: sequence, time: time)
        }
    }

    func setColorLabel(assetID: UUID, colorLabel: AssetColorLabel?) throws {
        let wallTime = Int64(nowProvider().timeIntervalSince1970 * 1000)
        try database.recordLedgerOperation(
            libraryID: libraryID,
            deviceID: deviceID,
            currentWallTimeMilliseconds: wallTime,
            uploadStatus: .pending
        ) { sequence, time in
            makeColorLabelOperation(assetID: assetID, colorLabel: colorLabel, deviceSequence: sequence, time: time)
        }
    }

    func updateTags(assetID: UUID, add: Set<String>, remove: Set<String>) throws {
        let wallTime = Int64(nowProvider().timeIntervalSince1970 * 1000)
        try database.recordLedgerOperation(
            libraryID: libraryID,
            deviceID: deviceID,
            currentWallTimeMilliseconds: wallTime,
            uploadStatus: .pending
        ) { sequence, time in
            makeTagsOperation(assetID: assetID, add: add, remove: remove, deviceSequence: sequence, time: time)
        }
    }

    func moveToTrash(assetID: UUID, reason: String) throws {
        try moveAssetsToTrash([assetID], reason: reason)
    }

    func restoreFromTrash(assetID: UUID) throws {
        try restoreAssetsFromTrash([assetID])
    }

    func moveAssetsToTrash(_ assetIDs: [UUID], reason: String) throws {
        let uniqueAssetIDs = Array(Set(assetIDs)).sorted { $0.uuidString < $1.uuidString }
        guard !uniqueAssetIDs.isEmpty else { return }
        let wallTime = Int64(nowProvider().timeIntervalSince1970 * 1000)
        try database.recordLedgerOperations(
            libraryID: libraryID,
            deviceID: deviceID,
            currentWallTimeMilliseconds: wallTime,
            uploadStatus: .pending
        ) { sequence, time in
            uniqueAssetIDs.enumerated().map { index, assetID in
                .moveToTrash(
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
        guard !uniqueAssetIDs.isEmpty else { return }
        let wallTime = Int64(nowProvider().timeIntervalSince1970 * 1000)
        try database.recordLedgerOperations(
            libraryID: libraryID,
            deviceID: deviceID,
            currentWallTimeMilliseconds: wallTime,
            uploadStatus: .pending
        ) { sequence, time in
            uniqueAssetIDs.enumerated().map { index, assetID in
                .restoreFromTrash(
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

    func declareImportedOriginal(assetID: UUID, fileObject: FileObjectID, localPlacement: FilePlacement) throws {
        let wallTime = Int64(nowProvider().timeIntervalSince1970 * 1000)
        try database.recordLedgerOperation(
            libraryID: libraryID,
            deviceID: deviceID,
            currentWallTimeMilliseconds: wallTime,
            uploadStatus: .pending
        ) { sequence, time in
            .importedOriginalDeclared(
                libraryID: libraryID,
                deviceID: deviceID,
                deviceSequence: sequence,
                time: time,
                actorID: actorID,
                assetID: assetID,
                fileObject: fileObject,
                placement: localPlacement
            )
        }
    }

    func requestArchive(assetID: UUID) throws {
        let wallTime = Int64(nowProvider().timeIntervalSince1970 * 1000)
        try database.recordLedgerOperation(
            libraryID: libraryID,
            deviceID: deviceID,
            currentWallTimeMilliseconds: wallTime,
            uploadStatus: .pending
        ) { sequence, time in
            .archiveRequested(
                libraryID: libraryID,
                deviceID: deviceID,
                deviceSequence: sequence,
                time: time,
                actorID: actorID,
                assetID: assetID
            )
        }
    }

    func recordArchiveReceipt(assetID: UUID, fileObject: FileObjectID, serverPlacement: FilePlacement) throws {
        let wallTime = Int64(nowProvider().timeIntervalSince1970 * 1000)
        try database.recordLedgerOperation(
            libraryID: libraryID,
            deviceID: deviceID,
            currentWallTimeMilliseconds: wallTime,
            uploadStatus: .pending
        ) { sequence, time in
            .originalArchiveReceiptRecorded(
                libraryID: libraryID,
                deviceID: deviceID,
                deviceSequence: sequence,
                time: time,
                actorID: actorID,
                assetID: assetID,
                fileObject: fileObject,
                serverPlacement: serverPlacement
            )
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

extension SyncCommandWriting {
    func moveAssetsToTrash(_ assetIDs: [UUID], reason: String) throws {
        for assetID in assetIDs {
            try moveToTrash(assetID: assetID, reason: reason)
        }
    }

    func restoreAssetsFromTrash(_ assetIDs: [UUID]) throws {
        for assetID in assetIDs {
            try restoreFromTrash(assetID: assetID)
        }
    }
}

enum SyncCommandError: LocalizedError {
    case invalidRating(Int)

    var errorDescription: String? {
        switch self {
        case .invalidRating(let rating): "评分必须在 0 到 5 之间，实际值：\(rating)"
        }
    }
}

struct SyncControlPlaneRoute: Equatable, Sendable {
    var method: String
    var path: String

    static func uploadOps(libraryID: String) -> SyncControlPlaneRoute {
        SyncControlPlaneRoute(method: "POST", path: makePath(segments: ["libraries", libraryID, "ops"]))
    }

    static func fetchOps(libraryID: String, after cursor: String?) -> SyncControlPlaneRoute {
        let queryItems = cursor.map { [("after", $0)] } ?? []
        return SyncControlPlaneRoute(method: "GET", path: makePath(segments: ["libraries", libraryID, "ops"], queryItems: queryItems))
    }

    static func deviceHeartbeat(deviceID: String) -> SyncControlPlaneRoute {
        SyncControlPlaneRoute(method: "POST", path: makePath(segments: ["devices", deviceID, "heartbeat"]))
    }

    static var archiveReceipts: SyncControlPlaneRoute {
        SyncControlPlaneRoute(method: "POST", path: makePath(segments: ["archive", "receipts"]))
    }

    static func trash(libraryID: String) -> SyncControlPlaneRoute {
        SyncControlPlaneRoute(method: "GET", path: makePath(segments: ["libraries", libraryID, "trash"]))
    }

    private static func makePath(segments: [String], queryItems: [(String, String)] = []) -> String {
        let encodedPath = segments.map(percentEncodePathSegment).joined(separator: "/")
        let path = "/" + encodedPath
        guard !queryItems.isEmpty else {
            return path
        }
        let encodedQuery = queryItems.map { name, value in
            "\(percentEncodeQueryComponent(name))=\(percentEncodeQueryComponent(value))"
        }.joined(separator: "&")
        return path + "?" + encodedQuery
    }

    private static func percentEncodePathSegment(_ segment: String) -> String {
        let allowed = CharacterSet.urlPathAllowed.subtracting(CharacterSet(charactersIn: "/?#"))
        return segment.addingPercentEncoding(withAllowedCharacters: allowed) ?? segment
    }

    private static func percentEncodeQueryComponent(_ component: String) -> String {
        let allowed = CharacterSet.urlQueryAllowed.subtracting(CharacterSet(charactersIn: "+&=?/#"))
        return component.addingPercentEncoding(withAllowedCharacters: allowed) ?? component
    }
}

struct SyncOpsUploadRequest: Codable, Equatable, Sendable {
    var operations: [OperationLedgerEntry]
}

struct SyncOpsFetchResponse: Codable, Equatable, Sendable {
    var operations: [OperationLedgerEntry]
    var cursor: String
}

struct DeviceHeartbeatRequest: Codable, Equatable, Sendable {
    var deviceID: String
    var libraryID: String
    var placements: [FilePlacement]
    var sentAt: Date
}

struct ArchiveReceiptRequest: Codable, Equatable, Sendable {
    var operation: OperationLedgerEntry
}

protocol SyncControlPlaneClient: Sendable {
    func uploadOperations(_ request: SyncOpsUploadRequest, libraryID: String) async throws
    func fetchOperations(libraryID: String, after cursor: String?) async throws -> SyncOpsFetchResponse
    func sendHeartbeat(_ request: DeviceHeartbeatRequest) async throws
    func recordArchiveReceipt(_ request: ArchiveReceiptRequest) async throws
}

enum SyncLedgerProjector {
    static func project(_ entries: [OperationLedgerEntry]) throws -> SyncLedgerProjection {
        var state = ProjectorState()
        for entry in entries.sorted(by: ledgerOrder) {
            state.apply(entry)
        }
        return state.projection()
    }

    private static func ledgerOrder(lhs: OperationLedgerEntry, rhs: OperationLedgerEntry) -> Bool {
        if lhs.hybridLogicalTime != rhs.hybridLogicalTime {
            return lhs.hybridLogicalTime < rhs.hybridLogicalTime
        }
        if lhs.deviceID.rawValue != rhs.deviceID.rawValue {
            return lhs.deviceID.rawValue < rhs.deviceID.rawValue
        }
        return lhs.opID.uuidString < rhs.opID.uuidString
    }
}

private struct ProjectorState {
    private struct Register<Value: Equatable> {
        var value: Value
        var time: HybridLogicalTime
        var opID: UUID
    }

    private struct MutableAsset {
        var asset = ProjectedAsset(id: UUID())
        var rating: Register<Int>?
        var flagState: Register<AssetFlagState>?
        var colorLabel: Register<AssetColorLabel?>?
        var caption: Register<String?>?
        var tagRegisters: [String: Register<Bool>] = [:]
        var trashRegister: Register<ProjectedTrashState>?
    }

    private var assets: [UUID: MutableAsset] = [:]
    private var trash: [UUID: ProjectedTrashEntry] = [:]
    private var fileObjects: [FileObjectID: FileObject] = [:]
    private var placements: [FileObjectID: Set<FilePlacement>] = [:]
    private var conflicts: [SyncConflict] = []

    mutating func apply(_ entry: OperationLedgerEntry) {
        switch entry.payload {
        case let .metadataSet(assetID, field, value):
            applyMetadata(assetID: assetID, field: field, value: value, entry: entry)
        case let .tagsUpdated(assetID, add, remove):
            var asset = mutableAsset(assetID)
            for tag in add {
                asset.tagRegisters[tag] = newer(current: asset.tagRegisters[tag], value: true, entry: entry)
            }
            for tag in remove {
                asset.tagRegisters[tag] = newer(current: asset.tagRegisters[tag], value: false, entry: entry)
            }
            assets[assetID] = asset
        case let .moveToTrash(assetID, reason):
            var asset = mutableAsset(assetID)
            asset.trashRegister = newer(current: asset.trashRegister, value: .trashed, entry: entry)
            assets[assetID] = asset
            trash[assetID] = ProjectedTrashEntry(assetID: assetID, reason: reason, movedAt: entry.hybridLogicalTime, movedBy: entry.actorID)
        case let .restoreFromTrash(assetID):
            var asset = mutableAsset(assetID)
            asset.trashRegister = newer(current: asset.trashRegister, value: .active, entry: entry)
            assets[assetID] = asset
            trash.removeValue(forKey: assetID)
        case let .importedOriginalDeclared(assetID, fileObjectID, placement):
            var asset = mutableAsset(assetID)
            if asset.asset.archiveState == nil {
                asset.asset.archiveState = .pendingOriginalUpload
            }
            assets[assetID] = asset
            record(fileObjectID, placement: placement)
        case let .archiveRequested(assetID):
            var asset = mutableAsset(assetID)
            if asset.asset.archiveState == nil {
                asset.asset.archiveState = .pendingOriginalUpload
            }
            assets[assetID] = asset
        case let .originalArchiveReceiptRecorded(assetID, fileObjectID, serverPlacement):
            var asset = mutableAsset(assetID)
            asset.asset.archiveState = .archived
            assets[assetID] = asset
            record(fileObjectID, placement: serverPlacement)
        }
    }

    func projection() -> SyncLedgerProjection {
        var projectedAssets: [UUID: ProjectedAsset] = [:]
        for (id, mutable) in assets {
            var asset = mutable.asset
            asset.rating = mutable.rating?.value ?? 0
            asset.flagState = mutable.flagState?.value ?? .unflagged
            asset.colorLabel = mutable.colorLabel?.value ?? nil
            asset.caption = mutable.caption?.value ?? nil
            asset.tags = mutable.tagRegisters
                .filter { $0.value.value }
                .map(\.key)
                .sorted()
            asset.trashState = mutable.trashRegister?.value ?? .active
            projectedAssets[id] = asset
        }
        return SyncLedgerProjection(
            assets: projectedAssets,
            trash: trash,
            fileObjects: fileObjects,
            filePlacements: placements.mapValues { Array($0).sorted { $0.holderID < $1.holderID } },
            conflicts: conflicts
        )
    }

    private mutating func mutableAsset(_ id: UUID) -> MutableAsset {
        if let existing = assets[id] {
            return existing
        }
        var asset = MutableAsset()
        asset.asset.id = id
        return asset
    }

    private mutating func applyMetadata(assetID: UUID, field: AssetMetadataField, value: LedgerValue, entry: OperationLedgerEntry) {
        var asset = mutableAsset(assetID)
        switch field {
        case .rating:
            if case let .int(rating) = value {
                asset.rating = newer(current: asset.rating, value: rating, entry: entry, conflict: { previous in
                    SyncConflict(assetID: assetID, field: field, leftOperationID: previous.opID, rightOperationID: entry.opID, message: "并发 rating 修改")
                })
            }
        case .flagState:
            if case let .string(rawValue) = value, let flagState = AssetFlagState(rawValue: rawValue) {
                asset.flagState = newer(current: asset.flagState, value: flagState, entry: entry, conflict: { previous in
                    SyncConflict(assetID: assetID, field: field, leftOperationID: previous.opID, rightOperationID: entry.opID, message: "并发 flag 修改")
                })
            }
        case .colorLabel:
            let colorLabel: AssetColorLabel?
            if case let .string(rawValue) = value {
                colorLabel = AssetColorLabel(rawValue: rawValue)
            } else {
                colorLabel = nil
            }
            asset.colorLabel = newer(current: asset.colorLabel, value: colorLabel, entry: entry)
        case .caption:
            let caption: String?
            if case let .string(value) = value {
                caption = value
            } else {
                caption = nil
            }
            asset.caption = newer(current: asset.caption, value: caption, entry: entry)
        }
        assets[assetID] = asset
    }

    private mutating func record(_ fileObjectID: FileObjectID, placement: FilePlacement) {
        fileObjects[fileObjectID] = FileObject(
            id: fileObjectID,
            contentHash: fileObjectID.contentHash,
            sizeBytes: fileObjectID.sizeBytes,
            role: fileObjectID.role
        )
        placements[fileObjectID, default: []].insert(placement)
    }

    private mutating func newer<Value: Equatable>(
        current: Register<Value>?,
        value: Value,
        entry: OperationLedgerEntry,
        conflict: ((Register<Value>) -> SyncConflict)? = nil
    ) -> Register<Value> {
        guard let current else {
            return Register(value: value, time: entry.hybridLogicalTime, opID: entry.opID)
        }
        if current.time == entry.hybridLogicalTime, current.value != value, let conflict {
            conflicts.append(conflict(current))
        }
        if current.time < entry.hybridLogicalTime ||
            (current.time == entry.hybridLogicalTime && current.opID.uuidString < entry.opID.uuidString) {
            return Register(value: value, time: entry.hybridLogicalTime, opID: entry.opID)
        }
        return current
    }
}
