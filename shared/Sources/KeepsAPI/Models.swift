import Foundation

public struct KeepsConfiguration: Equatable, Sendable {
    public var baseURL: URL
    public var libraryID: String
    public var accessCredential: String?
    public init(baseURL: URL, libraryID: String, accessCredential: String? = nil) {
        self.baseURL = baseURL
        self.libraryID = libraryID
        self.accessCredential = accessCredential
    }
}

public struct KeepsPreview: Codable, Equatable, Sendable {
    public var downloadURL: URL
    public var width: Int
    public var height: Int
    public var version: String
}

public struct KeepsAsset: Codable, Identifiable, Equatable, Sendable {
    public var id: UUID
    public var captureTime: String?
    public var cameraMake: String
    public var cameraModel: String
    public var lensModel: String
    public var originalFilename: String
    public var contentFingerprint: String
    public var metadataFingerprint: String
    public var rating: Int
    public var flagState: String
    public var colorLabel: String?
    public var tags: [String]
    public var createdAt: String
    public var updatedAt: String
    public var trashed: Bool
    public var preview: KeepsPreview?
}

public struct KeepsAssetPage: Decodable, Sendable {
    public var items: [KeepsAsset]
    public var total: Int
    public var nextCursor: String?
}

public struct KeepsHiddenDirectories: Decodable, Equatable, Sendable {
    public var paths: [String]
}

public struct KeepsCounts: Decodable, Equatable, Sendable {
    public var all: Int
    public var trashed: Int
    public var picked: Int
}

public struct KeepsDirectory: Decodable, Identifiable, Equatable, Sendable {
    public var path: String
    public var count: Int
    public var id: String { path }
}

public struct KeepsNavigationDirectory: Decodable, Identifiable, Equatable, Sendable {
    public var path: String
    public var name: String
    public var photoCount: Int
    public var hasChildren: Bool
    public var id: String { path }
}

public struct KeepsNavigation: Decodable, Sendable {
    public var path: String?
    public var directories: [KeepsNavigationDirectory]
}

public struct KeepsFolder: Decodable, Identifiable, Equatable, Sendable {
    public var id: String
    public var libraryID: String
    public var path: String
    public var active: Bool
}

public struct KeepsFoldersResponse: Decodable, Sendable {
    public var rootPath: String
    public var folders: [KeepsFolder]
}

public struct KeepsJob: Decodable, Identifiable, Equatable, Sendable {
    public var id: String
    public var folderID: String
    public var libraryID: String
    public var path: String
    public var status: String
    public var error: String?
    public var processed: Int?
    public var skipped: Int?
    public var failed: Int?
    public var currentPath: String?
    public var startedAt: Int64?
    public var finishedAt: Int64?
}

public struct KeepsJobsResponse: Decodable, Sendable {
    public var jobs: [KeepsJob]
}

public struct KeepsAssetQuery: Equatable, Sendable {
    public var q = ""
    public var minRating = 0
    public var flagState: String?
    public var colorLabel: String?
    public var tag: String?
    public var trashed = false
    public var sort = "capture_desc"
    public var folderID: String?
    public var directory: String?
    public var recursive = true
    public var showHidden = false
    public var cursor: String?
    public var limit = 100
    public init() {}

    var queryItems: [URLQueryItem] {
        var values = ["q": q, "minRating": String(minRating), "trashed": String(trashed), "sort": sort, "recursive": String(recursive), "showHidden": String(showHidden), "limit": String(limit)]
        for (key, value) in [("flagState", flagState), ("colorLabel", colorLabel), ("tag", tag), ("folderID", folderID), ("directory", directory), ("cursor", cursor)] {
            if let value { values[key] = value }
        }
        return values.sorted { $0.key < $1.key }.map { URLQueryItem(name: $0.key, value: $0.value) }
    }
}

public struct KeepsAssetPatch: Encodable, Sendable {
    public var rating: Int?
    public var flagState: String?
    public var colorLabel: String?
    public var clearColorLabel: Bool
    public var tags: [String]?
    public init(rating: Int? = nil, flagState: String? = nil, colorLabel: String? = nil, clearColorLabel: Bool = false, tags: [String]? = nil) {
        self.rating = rating; self.flagState = flagState; self.colorLabel = colorLabel
        self.clearColorLabel = clearColorLabel; self.tags = tags
    }
    enum CodingKeys: String, CodingKey { case rating, flagState, colorLabel, tags }
    public func encode(to encoder: Encoder) throws {
        var values = encoder.container(keyedBy: CodingKeys.self)
        try values.encodeIfPresent(rating, forKey: .rating)
        try values.encodeIfPresent(flagState, forKey: .flagState)
        if clearColorLabel { try values.encodeNil(forKey: .colorLabel) }
        else { try values.encodeIfPresent(colorLabel, forKey: .colorLabel) }
        try values.encodeIfPresent(tags, forKey: .tags)
    }
}

public struct KeepsAssetVersion: Decodable, Identifiable, Sendable {
    public var contentHash: String
    public var width: Int
    public var height: Int
    public var priority: Int
    public var isDefault: Bool
    public var userSelected: Bool
    public var available: Bool
    public var paths: [Location]
    public var id: String { contentHash }
    public var filename: String { paths.first.map { URL(fileURLWithPath: $0.path).lastPathComponent } ?? contentHash }
    public var kind: String {
        switch priority {
        case 3: "编辑成片"
        case 2: "成片"
        default: "RAW"
        }
    }
    public struct Location: Decodable, Sendable {
        public var path: String
        public var available: Bool
    }
}
