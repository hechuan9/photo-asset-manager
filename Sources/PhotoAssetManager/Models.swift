import Foundation

enum StorageKind: String, CaseIterable, Codable, Identifiable, Sendable {
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

enum FileRole: String, CaseIterable, Codable, Identifiable, Sendable {
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

enum AuthorityRole: String, CaseIterable, Codable, Identifiable, Sendable {
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

enum SyncStatus: String, CaseIterable, Codable, Identifiable, Sendable {
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

enum AssetStatus: String, CaseIterable, Identifiable, Sendable {
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

enum VersionKind: String, Codable, Sendable {
    case original
    case editRecipe = "edit_recipe"
    case export
}

enum CollectionKind: String, Codable, Sendable {
    case project
    case album
    case smartAlbum = "smart_album"
}

enum BrowseNodeKind: String, CaseIterable, Codable, Identifiable, Sendable {
    case folder

    var id: String { rawValue }
}

enum BrowseEdgeKind: String, Codable, Sendable {
    case filesystemContainment = "filesystem_containment"
}

enum BrowseMembershipKind: String, Codable, Sendable {
    case directFileInstance = "direct_file_instance"
}

enum BrowseScope: String, CaseIterable, Codable, Identifiable {
    case direct
    case recursive

    var id: String { rawValue }

    var label: String {
        switch self {
        case .direct: "仅当前文件夹"
        case .recursive: "包含子文件夹"
        }
    }
}

struct BrowseNode: Identifiable, Hashable, Sendable {
    let id: UUID
    var kind: BrowseNodeKind
    var canonicalKey: String
    var displayName: String
    var displayPath: String
    var storageKind: StorageKind
}

struct BrowseSelection: Equatable, Hashable, Sendable {
    var nodeID: UUID
    var kind: BrowseNodeKind
    var path: String
    var displayName: String
    var scope: BrowseScope
}

struct Asset: Identifiable, Hashable, Sendable {
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

struct FileInstance: Identifiable, Hashable, Sendable {
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

struct AssetVersion: Identifiable, Hashable, Sendable {
    let id: UUID
    var assetID: UUID
    var name: String
    var versionKind: VersionKind
    var parentVersionID: UUID?
    var createdBy: String
    var createdAt: Date
    var notes: String
}

struct PhotoCollection: Identifiable, Hashable, Sendable {
    let id: UUID
    var name: String
    var collectionKind: CollectionKind
    var description: String
}

struct ImportBatch: Identifiable, Hashable, Sendable {
    let id: UUID
    var sourcePath: String
    var deviceID: String
    var importedAt: Date
    var importedBy: String
    var status: String
}

struct SourceDirectory: Identifiable, Hashable, Sendable {
    let id: UUID
    var path: String
    var storageKind: StorageKind
    var isTracked: Bool
    var parentID: UUID?
    var createdAt: Date
    var lastScannedAt: Date?
}

struct SourceDirectoryNode: Identifiable, Hashable, Sendable {
    var id: String
    var source: SourceDirectory?
    var path: String
    var depth: Int
    var displayName: String
    var hasChildren: Bool
}

struct FolderMoveSource: Identifiable, Hashable, Sendable {
    var id: String { path }
    var sourceDirectoryID: UUID?
    var path: String
    var storageKind: StorageKind

    init(source: SourceDirectory) {
        sourceDirectoryID = source.id
        path = source.path
        storageKind = source.storageKind
    }

    init(path: String) {
        sourceDirectoryID = nil
        self.path = SourceDirectoryTreeBuilder.normalizedDirectoryPath(path)
        storageKind = path.hasPrefix("/Volumes/") ? .nas : .local
    }
}

struct FolderMoveTarget: Identifiable, Hashable, Sendable {
    var id: String { path }
    var path: String
    var displayName: String
}

struct PhotoImportTarget: Identifiable, Hashable, Sendable {
    var id: String { path }
    var path: String
    var displayName: String
    var storageKind: StorageKind
}

struct FolderMoveJob: Identifiable, Hashable, Sendable {
    let id: UUID
    var sourceDirectoryID: UUID
    var sourcePath: String
    var destinationParentPath: String
    var destinationPath: String
    var storageKind: StorageKind
    var status: String
    var totalFiles: Int
    var completedFiles: Int
}

struct FolderMoveItem: Identifiable, Hashable, Sendable {
    let id: UUID
    var jobID: UUID
    var fileInstanceID: UUID?
    var sourcePath: String
    var destinationPath: String
    var contentHash: String
    var status: String
}

struct FolderMovePlanItem: Hashable, Sendable {
    var sourcePath: String
    var destinationPath: String
    var fileInstanceID: UUID?
    var contentHash: String
}

struct PhotoImportPlanItem: Hashable, Sendable {
    var sourcePath: String
    var destinationPath: String
    var contentHash: String
}

struct AssetFileMoveRequest: Identifiable, Hashable, Sendable {
    var id: String {
        (assetIDs.map(\.uuidString).sorted() + [target.path]).joined(separator: "|")
    }

    var assetIDs: [UUID]
    var target: FolderMoveTarget
}

struct AssetFileMovePlanItem: Hashable, Sendable {
    var fileInstanceID: UUID
    var sourcePath: String
    var destinationPath: String
    var contentHash: String
}

struct IndexedFolderTree: Sendable {
    private let childrenByParentPath: [String: [BrowseNode]]

    init(_ folders: [BrowseNode]) {
        var children: [String: [BrowseNode]] = [:]
        let foldersByPath = Dictionary(grouping: folders, by: { SourceDirectoryTreeBuilder.normalizedDirectoryPath($0.displayPath) })
        for folder in folders {
            let folderPath = SourceDirectoryTreeBuilder.normalizedDirectoryPath(folder.displayPath)
            guard let parentPath = Self.parentPath(of: folderPath), foldersByPath[parentPath] != nil else { continue }
            children[parentPath, default: []].append(folder)
        }
        for parentPath in children.keys {
            children[parentPath]?.sort { $0.displayPath.localizedStandardCompare($1.displayPath) == .orderedAscending }
        }
        childrenByParentPath = children
    }

    func immediateChildren(of path: String, excluding excludedPaths: Set<String> = []) -> [BrowseNode] {
        let normalizedPath = SourceDirectoryTreeBuilder.normalizedDirectoryPath(path)
        return (childrenByParentPath[normalizedPath] ?? [])
            .filter { !excludedPaths.contains(SourceDirectoryTreeBuilder.normalizedDirectoryPath($0.displayPath)) }
    }

    func hasImmediateChildren(of path: String, excluding excludedPaths: Set<String> = []) -> Bool {
        !immediateChildren(of: path, excluding: excludedPaths).isEmpty
    }

    private static func parentPath(of path: String) -> String? {
        guard path != "/" else { return nil }
        let parent = URL(fileURLWithPath: path, isDirectory: true).deletingLastPathComponent().path
        return SourceDirectoryTreeBuilder.normalizedDirectoryPath(parent)
    }
}

enum SourceDirectoryTreeBuilder {
    static func build(_ sources: [SourceDirectory], expandedIDs: Set<UUID>) -> [SourceDirectoryNode] {
        build(sources, expandedNodeIDs: Set(expandedIDs.map { "source:\($0.uuidString)" }))
    }

    static func build(
        _ sources: [SourceDirectory],
        indexedBrowseFolders: [BrowseNode] = [],
        expandedNodeIDs: Set<String>
    ) -> [SourceDirectoryNode] {
        let children = childMap(for: sources)
        let indexedFolderTree = IndexedFolderTree(indexedBrowseFolders)
        let roots = rootSources(in: sources, children: children)
        return roots.flatMap {
            flatten(
                source: $0,
                parent: nil,
                displayNameOverride: nil,
                depth: 0,
                children: children,
                indexedFolderTree: indexedFolderTree,
                expandedNodeIDs: expandedNodeIDs
            )
        }
    }

    static func topLevelSources(in sources: [SourceDirectory]) -> [SourceDirectory] {
        let children = childMap(for: sources)
        return rootSources(in: sources, children: children)
    }

    static func moveTargets(
        for sourcePath: String,
        sources: [SourceDirectory],
        indexedBrowseFolders: [BrowseNode]
    ) -> [FolderMoveTarget] {
        let sourcePath = normalizedDirectoryPath(sourcePath)
        let blockedPrefix = sourcePath == "/" ? "/" : sourcePath + "/"
        var targetsByPath: [String: FolderMoveTarget] = [:]

        func insert(path rawPath: String) {
            let path = normalizedDirectoryPath(rawPath)
            guard path != sourcePath, !path.hasPrefix(blockedPrefix) else { return }
            targetsByPath[path] = FolderMoveTarget(
                path: path,
                displayName: path
            )
        }

        for candidate in sources where normalizedDirectoryPath(candidate.path) != sourcePath {
            insert(path: candidate.path)
        }
        for folder in indexedBrowseFolders {
            insert(path: folder.displayPath)
        }

        return targetsByPath.values.sorted {
            $0.path.localizedStandardCompare($1.path) == .orderedAscending
        }
    }

    private static func flatten(
        source: SourceDirectory,
        parent: SourceDirectory?,
        displayNameOverride: String?,
        depth: Int,
        children: [UUID: [SourceDirectory]],
        indexedFolderTree: IndexedFolderTree,
        expandedNodeIDs: Set<String>
    ) -> [SourceDirectoryNode] {
        let childSources = children[source.id] ?? []
        let sourcePath = normalizedDirectoryPath(source.path)
        let sourceChildPaths = Set(childSources.map { normalizedDirectoryPath($0.path) })
        let indexedChildren = indexedFolderTree.immediateChildren(of: sourcePath, excluding: sourceChildPaths)
        let nodeID = sourceNodeID(source.id)
        var nodes = [
            SourceDirectoryNode(
                id: nodeID,
                source: source,
                path: sourcePath,
                depth: depth,
                displayName: displayNameOverride ?? displayName(for: source, parent: parent),
                hasChildren: !childSources.isEmpty || !indexedChildren.isEmpty
            )
        ]
        guard expandedNodeIDs.contains(nodeID) else { return nodes }
        nodes.append(contentsOf: childSources.flatMap {
            flatten(
                source: $0,
                parent: source,
                displayNameOverride: nil,
                depth: depth + 1,
                children: children,
                indexedFolderTree: indexedFolderTree,
                expandedNodeIDs: expandedNodeIDs
            )
        })
        nodes.append(contentsOf: indexedChildren.flatMap {
            flattenIndexedFolder(
                folder: $0,
                depth: depth + 1,
                indexedFolderTree: indexedFolderTree,
                expandedNodeIDs: expandedNodeIDs
            )
        })
        return nodes
    }

    private static func flattenIndexedFolder(
        folder: BrowseNode,
        depth: Int,
        indexedFolderTree: IndexedFolderTree,
        expandedNodeIDs: Set<String>
    ) -> [SourceDirectoryNode] {
        let path = normalizedDirectoryPath(folder.displayPath)
        let nodeID = fileSystemNodeID(path)
        let children = indexedFolderTree.immediateChildren(of: path)
        var nodes = [
            SourceDirectoryNode(
                id: nodeID,
                source: nil,
                path: path,
                depth: depth,
                displayName: folder.displayName,
                hasChildren: !children.isEmpty
            )
        ]
        guard expandedNodeIDs.contains(nodeID) else { return nodes }
        nodes.append(contentsOf: children.flatMap {
            flattenIndexedFolder(
                folder: $0,
                depth: depth + 1,
                indexedFolderTree: indexedFolderTree,
                expandedNodeIDs: expandedNodeIDs
            )
        })
        return nodes
    }

    private static func displayName(for source: SourceDirectory, parent: SourceDirectory?) -> String {
        let sourcePath = normalizedDirectoryPath(source.path)
        guard let parent else { return URL(fileURLWithPath: sourcePath).lastPathComponent }

        let parentPath = normalizedDirectoryPath(parent.path)
        let prefix = parentPath == "/" ? "/" : parentPath + "/"
        guard sourcePath.hasPrefix(prefix) else {
            return URL(fileURLWithPath: sourcePath).lastPathComponent
        }

        var relativePath = String(sourcePath.dropFirst(prefix.count))
        if relativePath.hasPrefix("/") {
            relativePath.removeFirst()
        }
        return relativePath.isEmpty ? URL(fileURLWithPath: sourcePath).lastPathComponent : relativePath
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
            guard let parentID = explicitOrImmediatePathParentID(for: source, in: sources, validIDs: ids) else { continue }
            children[parentID, default: []].append(source)
        }
        for parentID in children.keys {
            children[parentID]?.sort { $0.path.localizedStandardCompare($1.path) == .orderedAscending }
        }
        return children
    }

    private static func explicitOrImmediatePathParentID(
        for source: SourceDirectory,
        in sources: [SourceDirectory],
        validIDs: Set<UUID>
    ) -> UUID? {
        if let parentID = source.parentID, validIDs.contains(parentID), parentID != source.id {
            return parentID
        }
        return sources
            .filter { candidate in
                candidate.id != source.id && isImmediateChild(source.path, of: candidate.path)
            }
            .max { $0.path.count < $1.path.count }?
            .id
    }

    private static func isImmediateChild(_ childPath: String, of parentPath: String) -> Bool {
        let parent = normalizedDirectoryPath(parentPath)
        let child = normalizedDirectoryPath(childPath)
        let prefix = parent == "/" ? "/" : parent + "/"
        guard child.hasPrefix(prefix) else { return false }
        return !String(child.dropFirst(prefix.count)).contains("/")
    }

    private static func sourceNodeID(_ id: UUID) -> String {
        "source:\(id.uuidString)"
    }

    private static func fileSystemNodeID(_ path: String) -> String {
        "folder:\(path)"
    }

    static func normalizedDirectoryPath(_ path: String) -> String {
        guard path.count > 1 else { return path }
        return path.hasSuffix("/") ? String(path.dropLast()) : path
    }
}

struct LibraryFilter: Equatable, Sendable {
    var status: AssetStatus?
    var browseSelection: BrowseSelection?
    var searchText = ""
    var camera = ""
    var fileExtension = ""
    var minimumRating = 0
    var tag = ""
    var directory = ""
}

struct ScanReport: Sendable {
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

struct BlockingTaskReport: Sendable {
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
