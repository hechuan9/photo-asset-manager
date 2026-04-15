import Foundation

enum StorageKind: String, CaseIterable, Codable, Identifiable {
    case local
    case nas
    case externalDrive = "external_drive"
    case cloudPreview = "cloud_preview"

    var id: String { rawValue }

    var label: String {
        switch self {
        case .local: "本地"
        case .nas: "NAS"
        case .externalDrive: "外接盘"
        case .cloudPreview: "云预览"
        }
    }
}

enum FileRole: String, CaseIterable, Codable, Identifiable {
    case rawOriginal = "raw_original"
    case jpegOriginal = "jpeg_original"
    case sidecar
    case preview
    case thumbnail
    case export

    var id: String { rawValue }

    var label: String {
        switch self {
        case .rawOriginal: "RAW 原片"
        case .jpegOriginal: "JPEG 原片"
        case .sidecar: "Sidecar"
        case .preview: "预览"
        case .thumbnail: "缩略图"
        case .export: "导出"
        }
    }
}

enum AuthorityRole: String, CaseIterable, Codable, Identifiable {
    case canonical
    case workingCopy = "working_copy"
    case sourceCopy = "source_copy"
    case cache

    var id: String { rawValue }

    var label: String {
        switch self {
        case .canonical: "权威副本"
        case .workingCopy: "工作副本"
        case .sourceCopy: "导入来源"
        case .cache: "缓存"
        }
    }
}

enum SyncStatus: String, CaseIterable, Codable, Identifiable {
    case synced
    case needsArchive = "needs_archive"
    case needsSync = "needs_sync"
    case conflict
    case cacheOnly = "cache_only"

    var id: String { rawValue }

    var label: String {
        switch self {
        case .synced: "已同步"
        case .needsArchive: "待归档"
        case .needsSync: "待同步"
        case .conflict: "冲突"
        case .cacheOnly: "仅缓存"
        }
    }
}

enum Availability: String, Codable, Sendable {
    case online
    case offline
    case missing
}

enum AssetStatus: String, CaseIterable, Identifiable {
    case inbox
    case working
    case needsArchive
    case needsSync
    case archived
    case missingOriginal

    var id: String { rawValue }

    var label: String {
        switch self {
        case .inbox: "Inbox"
        case .working: "Working"
        case .needsArchive: "Needs Archive"
        case .needsSync: "Needs Sync"
        case .archived: "Archived"
        case .missingOriginal: "Missing Original"
        }
    }
}

enum VersionKind: String, Codable {
    case original
    case editRecipe = "edit_recipe"
    case export
}

enum CollectionKind: String, Codable {
    case project
    case album
    case smartAlbum = "smart_album"
}

struct Asset: Identifiable, Hashable {
    let id: UUID
    var captureTime: Date?
    var cameraMake: String
    var cameraModel: String
    var lensModel: String
    var originalFilename: String
    var contentFingerprint: String
    var metadataFingerprint: String
    var rating: Int
    var flag: Bool
    var tags: [String]
    var createdAt: Date
    var updatedAt: Date
    var status: AssetStatus
    var fileCount: Int
    var primaryPath: String?
    var thumbnailPath: String?
}

struct FileInstance: Identifiable, Hashable {
    let id: UUID
    var assetID: UUID
    var path: String
    var deviceID: String
    var storageKind: StorageKind
    var fileRole: FileRole
    var authorityRole: AuthorityRole
    var syncStatus: SyncStatus
    var sizeBytes: Int64
    var contentHash: String
    var lastSeenAt: Date
    var availability: Availability
}

struct AssetVersion: Identifiable, Hashable {
    let id: UUID
    var assetID: UUID
    var name: String
    var versionKind: VersionKind
    var parentVersionID: UUID?
    var createdBy: String
    var createdAt: Date
    var notes: String
}

struct PhotoCollection: Identifiable, Hashable {
    let id: UUID
    var name: String
    var collectionKind: CollectionKind
    var description: String
}

struct ImportBatch: Identifiable, Hashable {
    let id: UUID
    var sourcePath: String
    var deviceID: String
    var importedAt: Date
    var importedBy: String
    var status: String
}

struct SourceDirectory: Identifiable, Hashable {
    let id: UUID
    var path: String
    var storageKind: StorageKind
    var isTracked: Bool
    var parentID: UUID?
    var createdAt: Date
    var lastScannedAt: Date?
}

struct SourceDirectoryNode: Identifiable, Hashable {
    var id: UUID { source.id }
    var source: SourceDirectory
    var depth: Int
    var hasChildren: Bool
}

enum SourceDirectoryTreeBuilder {
    static func build(_ sources: [SourceDirectory], expandedIDs: Set<UUID>) -> [SourceDirectoryNode] {
        let children = childMap(for: sources)
        let roots = rootSources(in: sources, children: children)
        return roots.flatMap { flatten(source: $0, depth: 0, children: children, expandedIDs: expandedIDs) }
    }

    static func topLevelSources(in sources: [SourceDirectory]) -> [SourceDirectory] {
        let children = childMap(for: sources)
        return rootSources(in: sources, children: children)
    }

    private static func flatten(
        source: SourceDirectory,
        depth: Int,
        children: [UUID: [SourceDirectory]],
        expandedIDs: Set<UUID>
    ) -> [SourceDirectoryNode] {
        let childSources = children[source.id] ?? []
        var nodes = [
            SourceDirectoryNode(
                source: source,
                depth: depth,
                hasChildren: !childSources.isEmpty
            )
        ]
        guard depth == 0 || expandedIDs.contains(source.id) else { return nodes }
        nodes.append(contentsOf: childSources.flatMap {
            flatten(source: $0, depth: depth + 1, children: children, expandedIDs: expandedIDs)
        })
        return nodes
    }

    private static func rootSources(
        in sources: [SourceDirectory],
        children: [UUID: [SourceDirectory]]
    ) -> [SourceDirectory] {
        let childIDs = Set(children.values.flatMap { $0.map(\.id) })
        return sources
            .filter { !childIDs.contains($0.id) }
            .sorted { $0.path.localizedStandardCompare($1.path) == .orderedAscending }
    }

    private static func childMap(for sources: [SourceDirectory]) -> [UUID: [SourceDirectory]] {
        let ids = Set(sources.map(\.id))
        var children: [UUID: [SourceDirectory]] = [:]
        for source in sources {
            guard let parentID = explicitOrPathParentID(for: source, in: sources, validIDs: ids) else { continue }
            children[parentID, default: []].append(source)
        }
        for parentID in children.keys {
            children[parentID]?.sort { $0.path.localizedStandardCompare($1.path) == .orderedAscending }
        }
        return children
    }

    private static func explicitOrPathParentID(
        for source: SourceDirectory,
        in sources: [SourceDirectory],
        validIDs: Set<UUID>
    ) -> UUID? {
        if let parentID = source.parentID, validIDs.contains(parentID), parentID != source.id {
            return parentID
        }
        return sources
            .filter { candidate in
                candidate.id != source.id && source.path.hasPrefix(normalizedDirectoryPath(candidate.path) + "/")
            }
            .max { $0.path.count < $1.path.count }?
            .id
    }

    private static func normalizedDirectoryPath(_ path: String) -> String {
        guard path.count > 1 else { return path }
        return path.hasSuffix("/") ? String(path.dropLast()) : path
    }
}

struct LibraryFilter: Equatable {
    var status: AssetStatus?
    var searchText = ""
    var camera = ""
    var fileExtension = ""
    var minimumRating = 0
    var tag = ""
    var directory = ""
}

struct ScanReport {
    var phase = ""
    var currentPath = ""
    var discoveredFiles = 0
    var totalFiles = 0
    var scannedFiles = 0
    var importedAssets = 0
    var newLocations = 0
    var skippedExistingFiles = 0
    var skippedFiles = 0
    var errors: [String] = []
}

struct BlockingTaskReport {
    var title = ""
    var phase = ""
    var currentPath = ""
    var totalItems = 0
    var completedItems = 0
    var skippedItems = 0
    var message = ""
}

struct BackgroundTaskReport: Sendable {
    var title = ""
    var phase = ""
    var currentPath = ""
    var totalItems = 0
    var completedItems = 0
    var message = ""
    var isFinished = false
}

struct AvailabilityCheckTarget: Sendable {
    let id: UUID
    let path: String
}

struct FileAvailabilityUpdate: Sendable {
    let id: UUID
    let availability: Availability
}
