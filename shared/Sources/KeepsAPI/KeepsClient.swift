import Foundation

public enum KeepsAPIError: Error, LocalizedError, Sendable {
    case invalidConfiguration
    case invalidResponse
    case http(Int, String)
    public var errorDescription: String? {
        switch self {
        case .invalidConfiguration: "请填写有效的 NAS HTTP/HTTPS 地址和资料库名称。"
        case .invalidResponse: "NAS 返回了非 HTTP 响应。"
        case let .http(status, body): "NAS HTTP \(status)：\(body)"
        }
    }
}

public final class KeepsClient: Sendable {
    // Keep interactive API requests out of the shared preview-download session.
    public static let apiSession = URLSession(configuration: .default)
    public let configuration: KeepsConfiguration
    private let session: URLSession
    public init(configuration: KeepsConfiguration, session: URLSession = KeepsClient.apiSession) {
        self.configuration = configuration
        self.session = session
    }
    private var library: [String] { ["libraries", configuration.libraryID] }

    public func revision(path: String? = nil, includeChildren: Bool = false) async throws -> KeepsCatalogRevision {
        var query: [URLQueryItem] = []
        if let path { query.append(URLQueryItem(name: "path", value: path)) }
        if includeChildren { query.append(URLQueryItem(name: "includeChildren", value: "true")) }
        return try await request("GET", library + ["revision"], query: query)
    }
    public func assets(query: KeepsAssetQuery = KeepsAssetQuery()) async throws -> KeepsAssetPage {
        try await request("GET", library + ["assets"], query: query.queryItems)
    }
    public func asset(id: UUID) async throws -> KeepsAsset {
        try await request("GET", library + ["assets", id.uuidString])
    }
    public func versions(assetID: UUID) async throws -> [KeepsAssetVersion] {
        try await versionDetails(assetID: assetID).items
    }
    public func versionDetails(assetID: UUID) async throws -> KeepsAssetVersions {
        try await request("GET", library + ["assets", assetID.uuidString, "versions"])
    }
    public func setDefaultVersion(assetID: UUID, contentHash: String) async throws -> [KeepsAssetVersion] {
        let response: KeepsAssetVersions = try await request("PUT", library + ["assets", assetID.uuidString, "default-version"], body: JSONEncoder().encode(["contentHash": contentHash]))
        return response.items
    }
    public func updateAsset(id: UUID, patch: KeepsAssetPatch) async throws -> KeepsAsset {
        try await request("PATCH", library + ["assets", id.uuidString], body: JSONEncoder().encode(patch))
    }
    public func trashAsset(id: UUID) async throws -> KeepsAsset {
        try await request("POST", library + ["assets", id.uuidString, "trash"])
    }
    public func restoreAsset(id: UUID) async throws -> KeepsAsset {
        try await request("POST", library + ["assets", id.uuidString, "restore"])
    }
    public func counts(showHidden: Bool = false) async throws -> KeepsCounts {
        try await request("GET", library + ["counts"], query: [URLQueryItem(name: "showHidden", value: String(showHidden))])
    }
    public func hiddenDirectories() async throws -> KeepsHiddenDirectories {
        try await request("GET", library + ["hidden-directories"])
    }
    public func setDirectoryHidden(path: String, hidden: Bool) async throws -> KeepsHiddenDirectories {
        struct Change: Encodable { var path: String; var hidden: Bool }
        return try await request("PUT", library + ["hidden-directories"], body: JSONEncoder().encode(Change(path: path, hidden: hidden)))
    }
    public func directories() async throws -> [KeepsDirectory] {
        let response: Directories = try await request("GET", library + ["directories"])
        return response.directories
    }
    public func folders() async throws -> KeepsFoldersResponse { try await request("GET", library + ["folders"]) }
    public func navigation(path: String? = nil) async throws -> KeepsNavigation {
        try await request("GET", library + ["navigation"], query: path.map { [URLQueryItem(name: "path", value: $0)] } ?? [])
    }
    public func createDirectory(parentPath: String, name: String) async throws -> String {
        struct CreatedDirectory: Decodable { var path: String }
        let response: CreatedDirectory = try await request("POST", library + ["directories"], body: JSONEncoder().encode(["parentPath": parentPath, "name": name]))
        return response.path
    }
    public func trashDirectory(path: String, confirmationName: String, requestID: UUID) async throws -> KeepsDirectoryTrashTask {
        try await request("POST", library + ["directories", "trash"], body: JSONEncoder().encode(["path": path, "confirmationName": confirmationName, "requestID": requestID.uuidString.lowercased()]))
    }
    public func directoryTrashTask(id: UUID) async throws -> KeepsDirectoryTrashTask {
        try await request("GET", library + ["directories", "trash", id.uuidString.lowercased()])
    }
    public func addFolder(path: String) async throws -> KeepsFolder {
        try await request("POST", library + ["folders"], body: JSONEncoder().encode(["path": path]))
    }
    public func removeFolder(id: String) async throws {
        _ = try await send("DELETE", library + ["folders", id])
    }
    public func scanFolder(id: String) async throws -> KeepsJob {
        try await request("POST", library + ["folders", id, "scan"])
    }
    public func taskStatus() async throws -> KeepsTaskStatus {
        try await request("GET", library + ["task-status"])
    }
    public func jobs() async throws -> KeepsJobsResponse { try await request("GET", library + ["jobs"]) }
    public func retryJob(id: String) async throws -> KeepsJob {
        try await request("POST", library + ["jobs", id, "retry"])
    }
    public func prepareImport(_ manifest: KeepsImportManifest) async throws -> KeepsImportBatch {
        var request = try makeRequest("POST", library + ["imports"], body: JSONEncoder().encode(manifest))
        if manifest.deduplicate { request.timeoutInterval = 3600 }
        let (data, response) = try await session.data(for: request)
        return try JSONDecoder().decode(KeepsImportBatch.self, from: checkedData(data, response))
    }
    public func uploadImportFile(batchID: UUID, fileID: UUID, source: URL, progress: @escaping @Sendable (Int64) -> Void) async throws {
        var request = try makeRequest("PUT", library + ["imports", batchID.uuidString, "files", fileID.uuidString])
        request.setValue("application/octet-stream", forHTTPHeaderField: "Content-Type")
        request.timeoutInterval = 3600
        let (data, response) = try await session.upload(for: request, fromFile: source, delegate: KeepsUploadProgress(progress))
        _ = try checkedData(data, response)
    }
    public func finishImport(id: UUID) async throws -> KeepsJob {
        struct Finished: Decodable { var job: KeepsJob }
        let response: Finished = try await request("POST", library + ["imports", id.uuidString, "finish"])
        return response.job
    }
    public func refreshPreview(assetID: UUID) async throws -> URL {
        try await refreshPreviewDescriptor(assetID: assetID).downloadURL
    }
    public func refreshPreviewDescriptor(assetID: UUID, role: KeepsMediaRole = .preview) async throws -> KeepsPreview {
        let response: KeepsPreview = try await request("GET", ["derivatives", assetID.uuidString], query: [URLQueryItem(name: "role", value: role.rawValue), URLQueryItem(name: "libraryID", value: configuration.libraryID)])
        return response
    }
    private struct Directories: Decodable { var directories: [KeepsDirectory] }
    private func request<T: Decodable>(_ method: String, _ segments: [String], query: [URLQueryItem] = [], body: Data? = nil) async throws -> T {
        try JSONDecoder().decode(T.self, from: await send(method, segments, query: query, body: body))
    }
    private func send(_ method: String, _ segments: [String], query: [URLQueryItem] = [], body: Data? = nil) async throws -> Data {
        let request = try makeRequest(method, segments, query: query, body: body)
        let (data, response) = try await session.data(for: request)
        return try checkedData(data, response)
    }
    private func makeRequest(_ method: String, _ segments: [String], query: [URLQueryItem] = [], body: Data? = nil) throws -> URLRequest {
        guard ["http", "https"].contains(configuration.baseURL.scheme?.lowercased() ?? ""),
              configuration.baseURL.host != nil, !configuration.libraryID.isEmpty,
              var url = URLComponents(url: configuration.baseURL, resolvingAgainstBaseURL: false) else { throw KeepsAPIError.invalidConfiguration }
        let allowed = CharacterSet.urlPathAllowed.subtracting(CharacterSet(charactersIn: "/?#%"))
        let suffix = segments.map { $0.addingPercentEncoding(withAllowedCharacters: allowed)! }.joined(separator: "/")
        let prefix = url.percentEncodedPath.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
        url.percentEncodedPath = "/" + ([prefix, suffix].filter { !$0.isEmpty }.joined(separator: "/"))
        url.queryItems = query.isEmpty ? nil : query
        guard let endpoint = url.url else { throw KeepsAPIError.invalidConfiguration }
        var request = URLRequest(url: endpoint)
        request.cachePolicy = .reloadIgnoringLocalCacheData
        request.httpMethod = method
        request.httpBody = body
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        if body != nil { request.setValue("application/json", forHTTPHeaderField: "Content-Type") }
        if let credential = configuration.accessCredential, !credential.isEmpty { request.setValue("Bearer \(credential)", forHTTPHeaderField: "Authorization") }
        return request
    }
    private func checkedData(_ data: Data, _ response: URLResponse) throws -> Data {
        guard let http = response as? HTTPURLResponse else { throw KeepsAPIError.invalidResponse }
        guard 200..<300 ~= http.statusCode else { throw KeepsAPIError.http(http.statusCode, String(data: data, encoding: .utf8) ?? "响应体无法解码") }
        return data
    }
}
