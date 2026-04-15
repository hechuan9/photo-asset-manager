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

enum BrowseNodeKind: String, CaseIterable, Codable, Identifiable {
    case folder

    var id: String { rawValue }
}

enum BrowseEdgeKind: String, Codable {
    case filesystemContainment = "filesystem_containment"
}

enum BrowseMembershipKind: String, Codable {
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

struct BrowseNode: Identifiable, Hashable {
    let id: UUID
    var kind: BrowseNodeKind
    var canonicalKey: String
    var displayName: String
    var displayPath: String
    var storageKind: StorageKind
}

struct BrowseSelection: Equatable, Hashable {
    var nodeID: UUID
    var kind: BrowseNodeKind
    var path: String
    var displayName: String
    var scope: BrowseScope
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
    var id: String
    var source: SourceDirectory?
    var path: String
    var depth: Int
    var displayName: String
    var hasChildren: Bool
}

enum SourceDirectoryTreeBuilder {
    static func build(_ sources: [SourceDirectory], expandedIDs: Set<UUID>) -> [SourceDirectoryNode] {
        build(sources, expandedNodeIDs: Set(expandedIDs.map { "source:\($0.uuidString)" }))
    }

    static func build(_ sources: [SourceDirectory], expandedNodeIDs: Set<String>) -> [SourceDirectoryNode] {
        let children = childMap(for: sources)
        let sourceByPath = sourceMapByPath(for: sources)
        let roots = rootSources(in: sources, children: children)
        return roots.flatMap {
            flatten(
                source: $0,
                parent: nil,
                displayNameOverride: nil,
                depth: 0,
                children: children,
                sourceByPath: sourceByPath,
                expandedNodeIDs: expandedNodeIDs
            )
        }
    }

    static func topLevelSources(in sources: [SourceDirectory]) -> [SourceDirectory] {
        let children = childMap(for: sources)
        return rootSources(in: sources, children: children)
    }

    private static func flatten(
        source: SourceDirectory,
        parent: SourceDirectory?,
        displayNameOverride: String?,
        depth: Int,
        children: [UUID: [SourceDirectory]],
        sourceByPath: [String: SourceDirectory],
        expandedNodeIDs: Set<String>
    ) -> [SourceDirectoryNode] {
        let childSources = children[source.id] ?? []
        let sourcePath = normalizedDirectoryPath(source.path)
        let sourceChildPaths = Set(childSources.map { normalizedDirectoryPath($0.path) })
        let filesystemChildren = fileSystemChildNodes(
            parentPath: sourcePath,
            depth: depth + 1,
            excludedPaths: sourceChildPaths
        )
        let nodeID = sourceNodeID(source.id)
        var nodes = [
            SourceDirectoryNode(
                id: nodeID,
                source: source,
                path: sourcePath,
                depth: depth,
                displayName: displayNameOverride ?? displayName(for: source, parent: parent),
                hasChildren: !childSources.isEmpty || !filesystemChildren.isEmpty
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
                sourceByPath: sourceByPath,
                expandedNodeIDs: expandedNodeIDs
            )
        })
        nodes.append(contentsOf: filesystemChildren.flatMap {
            flattenFileSystemDirectory(
                path: $0.path,
                depth: $0.depth,
                sourceByPath: sourceByPath,
                expandedNodeIDs: expandedNodeIDs
            )
        })
        return nodes
    }

    private static func flattenFileSystemDirectory(
        path: String,
        depth: Int,
        sourceByPath: [String: SourceDirectory],
        expandedNodeIDs: Set<String>
    ) -> [SourceDirectoryNode] {
        let nodeID = fileSystemNodeID(path)
        if let source = sourceByPath[path] {
            return flatten(
                source: source,
                parent: nil,
                displayNameOverride: URL(fileURLWithPath: path).lastPathComponent,
                depth: depth,
                children: [:],
                sourceByPath: sourceByPath,
                expandedNodeIDs: expandedNodeIDs
            )
        }

        var nodes = [
            SourceDirectoryNode(
                id: nodeID,
                source: nil,
                path: path,
                depth: depth,
                displayName: URL(fileURLWithPath: path).lastPathComponent,
                hasChildren: hasFileSystemSubdirectories(path)
            )
        ]
        guard expandedNodeIDs.contains(nodeID) else { return nodes }
        nodes.append(contentsOf: fileSystemChildNodes(parentPath: path, depth: depth + 1).flatMap {
            flattenFileSystemDirectory(
                path: $0.path,
                depth: $0.depth,
                sourceByPath: sourceByPath,
                expandedNodeIDs: expandedNodeIDs
            )
        })
        return nodes
    }

    private static func displayName(for source: SourceDirectory, parent: SourceDirectory?) -> String {
        let sourcePath = normalizedDirectoryPath(source.path)
        guard let parent else { return sourcePath }

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

    private static func sourceMapByPath(for sources: [SourceDirectory]) -> [String: SourceDirectory] {
        var sourceByPath: [String: SourceDirectory] = [:]
        for source in sources {
            sourceByPath[normalizedDirectoryPath(source.path)] = source
        }
        return sourceByPath
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

    private static func fileSystemChildNodes(
        parentPath: String,
        depth: Int,
        excludedPaths: Set<String> = []
    ) -> [SourceDirectoryNode] {
        let parentURL = URL(fileURLWithPath: parentPath, isDirectory: true)
        let childURLs = (try? FileManager.default.contentsOfDirectory(
            at: parentURL,
            includingPropertiesForKeys: [.isDirectoryKey],
            options: [.skipsHiddenFiles]
        )) ?? []

        return childURLs
            .filter { isDirectory($0) }
            .map { normalizedDirectoryPath($0.path) }
            .filter { !excludedPaths.contains($0) }
            .sorted { $0.localizedStandardCompare($1) == .orderedAscending }
            .map { path in
                SourceDirectoryNode(
                    id: fileSystemNodeID(path),
                    source: nil,
                    path: path,
                    depth: depth,
                    displayName: URL(fileURLWithPath: path).lastPathComponent,
                    hasChildren: hasFileSystemSubdirectories(path)
                )
            }
    }

    private static func hasFileSystemSubdirectories(_ path: String) -> Bool {
        let url = URL(fileURLWithPath: path, isDirectory: true)
        guard let childURLs = try? FileManager.default.contentsOfDirectory(
            at: url,
            includingPropertiesForKeys: [.isDirectoryKey],
            options: [.skipsHiddenFiles]
        ) else {
            return false
        }
        return childURLs.contains { isDirectory($0) }
    }

    private static func isDirectory(_ url: URL) -> Bool {
        (try? url.resourceValues(forKeys: [.isDirectoryKey]).isDirectory) == true
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

    private static func normalizedDirectoryPath(_ path: String) -> String {
        guard path.count > 1 else { return path }
        return path.hasSuffix("/") ? String(path.dropLast()) : path
    }
}

struct LibraryFilter: Equatable {
    var status: AssetStatus?
    var browseSelection: BrowseSelection?
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
