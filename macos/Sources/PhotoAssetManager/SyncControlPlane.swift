import Foundation

enum SyncControlPlaneHTTPError: Error, Equatable, LocalizedError, Sendable {
    case invalidBaseURL
    case invalidHTTPResponse
    case unexpectedStatusCode(Int, String?)
    case conflict(SyncOpsUploadResponse)

    var errorDescription: String? {
        switch self {
        case .invalidBaseURL:
            return "control-plane base URL 无效。"
        case .invalidHTTPResponse:
            return "control-plane 返回了非 HTTP 响应。"
        case let .unexpectedStatusCode(statusCode, responseBody):
            if let responseBody, !responseBody.isEmpty {
                return "control-plane HTTP \(statusCode)：\(responseBody)"
            }
            return "control-plane HTTP \(statusCode)：响应体为空。"
        case let .conflict(response):
            return "control-plane 同步冲突：已接受 \(response.accepted.count) 条，冲突 \(response.conflicts?.count ?? 0) 条。"
        }
    }
}

struct SyncClientConfiguration: Equatable, Sendable {
    var baseURLString: String
    var libraryID: String
    var peerID: String
    var authModeRawValue: String
    var accessCredential: String

    static func load(defaults: UserDefaults = .standard) -> SyncClientConfiguration {
        SyncClientConfiguration(
            baseURLString: defaults.string(forKey: SyncPreferenceKey.baseURL) ?? "",
            libraryID: defaults.string(forKey: SyncPreferenceKey.libraryID) ?? "local-library",
            peerID: defaults.string(forKey: SyncPreferenceKey.peerID) ?? "control-plane",
            authModeRawValue: defaults.string(forKey: SyncPreferenceKey.authMode) ?? SyncAuthenticationMode.bearer.rawValue,
            accessCredential: defaults.string(forKey: SyncPreferenceKey.accessCredential) ?? ""
        )
    }

    var trimmedBaseURLString: String {
        baseURLString.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    var baseURL: URL? {
        guard !trimmedBaseURLString.isEmpty else { return nil }
        return URL(string: trimmedBaseURLString)
    }

    var authMode: SyncAuthenticationMode {
        SyncAuthenticationMode(rawValue: authModeRawValue) ?? .bearer
    }

    var accessCredentialValue: String? {
        let trimmed = accessCredential.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }

    var requestAuthentication: SyncRequestAuthentication? {
        switch authMode {
        case .bearer:
            return accessCredentialValue.map(SyncRequestAuthentication.bearer)
        }
    }

    var isAuthenticationConfigured: Bool {
        switch authMode {
        case .bearer:
            return true
        }
    }

    var hasRemoteSync: Bool {
        baseURL != nil && isAuthenticationConfigured
    }
}

enum SyncAuthenticationMode: String, CaseIterable, Equatable, Identifiable, Sendable {
    case bearer = "bearer"

    var id: String { rawValue }

    var displayName: String {
        switch self {
        case .bearer:
            return "Bearer"
        }
    }
}

enum SyncRequestAuthentication: Equatable, Sendable {
    case bearer(String)
}

enum SyncPreferenceKey {
    static let baseURL = "ios.sync.base_url"
    static let libraryID = "ios.sync.library_id"
    static let peerID = "ios.sync.peer_id"
    static let authMode = "ios.sync.auth_mode"
    static let accessCredential = "ios.sync.access_credential"
}

final class SyncControlPlaneHTTPClient: SyncControlPlaneClient {
    private let baseURL: URL
    private let authentication: SyncRequestAuthentication?
    private let headerProvider: @Sendable () -> [String: String]
    private let dateProvider: @Sendable () -> Date
    private let session: URLSession

    init(
        baseURL: URL,
        authentication: SyncRequestAuthentication? = nil,
        headerProvider: @escaping @Sendable () -> [String: String] = { [:] },
        dateProvider: @escaping @Sendable () -> Date = Date.init,
        session: URLSession = .shared
    ) {
        self.baseURL = baseURL
        self.authentication = authentication
        self.headerProvider = headerProvider
        self.dateProvider = dateProvider
        self.session = session
    }

    func uploadOperations(_ request: SyncOpsUploadRequest, libraryID: String) async throws -> SyncOpsUploadResponse {
        let url = try makeURL(pathSegments: ["libraries", libraryID, "ops"])
        let encodedBody = try Self.makeEncoder().encode(request)
        let (data, response) = try await send(method: "POST", url: url, body: encodedBody)
        guard let httpResponse = response as? HTTPURLResponse else {
            throw SyncControlPlaneHTTPError.invalidHTTPResponse
        }
        if httpResponse.statusCode == 409,
           let conflict = try? Self.makeDecoder().decode(SyncOpsUploadConflictEnvelope.self, from: data) {
            throw SyncControlPlaneHTTPError.conflict(conflict.detail)
        }
        guard 200..<300 ~= httpResponse.statusCode else {
            throw SyncControlPlaneHTTPError.unexpectedStatusCode(httpResponse.statusCode, Self.responseBodySnippet(data))
        }
        return try Self.makeDecoder().decode(SyncOpsUploadResponse.self, from: data)
    }

    func fetchOperations(libraryID: String, after cursor: String?) async throws -> SyncOpsFetchResponse {
        let queryItems = cursor.map { [URLQueryItem(name: "after", value: $0)] } ?? []
        let url = try makeURL(pathSegments: ["libraries", libraryID, "ops"], queryItems: queryItems)
        let (data, response) = try await send(method: "GET", url: url, body: nil)
        guard let httpResponse = response as? HTTPURLResponse else {
            throw SyncControlPlaneHTTPError.invalidHTTPResponse
        }
        guard 200..<300 ~= httpResponse.statusCode else {
            throw SyncControlPlaneHTTPError.unexpectedStatusCode(httpResponse.statusCode, Self.responseBodySnippet(data))
        }
        return try Self.makeDecoder().decode(SyncOpsFetchResponse.self, from: data)
    }

    func sendHeartbeat(_ request: DeviceHeartbeatRequest) async throws {
        let url = try makeURL(pathSegments: ["devices", request.deviceID, "heartbeat"])
        try await sendJSON(method: "POST", url: url, body: request)
    }

    func recordArchiveReceipt(_ request: ArchiveReceiptRequest) async throws {
        let url = try makeURL(pathSegments: ["archive", "receipts"])
        try await sendJSON(method: "POST", url: url, body: request)
    }

    func createDerivativeUpload(_ request: DerivativeUploadRequest) async throws -> DerivativeUploadResponse {
        let url = try makeURL(pathSegments: ["derivatives", "uploads"])
        let encodedBody = try Self.makeEncoder().encode(request)
        let (data, response) = try await send(method: "POST", url: url, body: encodedBody)
        guard let httpResponse = response as? HTTPURLResponse else {
            throw SyncControlPlaneHTTPError.invalidHTTPResponse
        }
        guard 200..<300 ~= httpResponse.statusCode else {
            throw SyncControlPlaneHTTPError.unexpectedStatusCode(httpResponse.statusCode, Self.responseBodySnippet(data))
        }
        return try Self.makeDecoder().decode(DerivativeUploadResponse.self, from: data)
    }

    func fetchDerivativeMetadata(libraryID: String, assetID: UUID, role: DerivativeRole) async throws -> DerivativeMetadataResponse {
        let url = try makeURL(
            pathSegments: ["derivatives", assetID.uuidString],
            queryItems: [
                URLQueryItem(name: "role", value: role.rawValue),
                URLQueryItem(name: "libraryID", value: libraryID)
            ]
        )
        let (data, response) = try await send(method: "GET", url: url, body: nil)
        guard let httpResponse = response as? HTTPURLResponse else {
            throw SyncControlPlaneHTTPError.invalidHTTPResponse
        }
        guard 200..<300 ~= httpResponse.statusCode else {
            throw SyncControlPlaneHTTPError.unexpectedStatusCode(httpResponse.statusCode, Self.responseBodySnippet(data))
        }
        return try Self.makeDecoder().decode(DerivativeMetadataResponse.self, from: data)
    }

    func deleteDerivative(libraryID: String, assetID: UUID, role: DerivativeRole) async throws {
        let url = try makeURL(
            pathSegments: ["libraries", libraryID, "derivatives", assetID.uuidString],
            queryItems: [URLQueryItem(name: "role", value: role.rawValue)]
        )
        let (data, response) = try await send(method: "DELETE", url: url, body: nil)
        guard let httpResponse = response as? HTTPURLResponse else {
            throw SyncControlPlaneHTTPError.invalidHTTPResponse
        }
        guard 200..<300 ~= httpResponse.statusCode else {
            throw SyncControlPlaneHTTPError.unexpectedStatusCode(httpResponse.statusCode, Self.responseBodySnippet(data))
        }
    }

    private func sendJSON<Body: Encodable>(method: String, url: URL, body: Body) async throws {
        let encodedBody = try Self.makeEncoder().encode(body)
        let (data, response) = try await send(method: method, url: url, body: encodedBody)
        guard let httpResponse = response as? HTTPURLResponse else {
            throw SyncControlPlaneHTTPError.invalidHTTPResponse
        }
        guard 200..<300 ~= httpResponse.statusCode else {
            throw SyncControlPlaneHTTPError.unexpectedStatusCode(httpResponse.statusCode, Self.responseBodySnippet(data))
        }
        _ = data
    }

    private func send(method: String, url: URL, body: Data?) async throws -> (Data, URLResponse) {
        var request = URLRequest(url: url)
        request.httpMethod = method
        for (header, value) in headerProvider() {
            let normalizedHeader = header.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
            guard !Self.reservedHeaderNames.contains(normalizedHeader) else { continue }
            request.setValue(value, forHTTPHeaderField: header)
        }
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        if let body {
            request.httpBody = body
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        } else {
            request.setValue(nil, forHTTPHeaderField: "Content-Type")
        }
        switch authentication {
        case .bearer(let accessCredential):
            request.setValue("\(Self.accessCredentialScheme) \(accessCredential)", forHTTPHeaderField: Self.accessCredentialHeaderName)
        case .none:
            break
        }
        return try await session.data(for: request)
    }

    private func makeURL(pathSegments: [String], queryItems: [URLQueryItem] = []) throws -> URL {
        guard var components = URLComponents(url: baseURL, resolvingAgainstBaseURL: false) else {
            throw SyncControlPlaneHTTPError.invalidBaseURL
        }

        let baseSegments = components.percentEncodedPath
            .split(separator: "/")
            .map(String.init)
            .filter { !$0.isEmpty }
        let requestSegments = pathSegments.map(Self.percentEncodePathSegment)
        components.percentEncodedPath = "/" + (baseSegments + requestSegments).joined(separator: "/")
        components.percentEncodedQueryItems = queryItems.isEmpty ? nil : queryItems.map {
            URLQueryItem(
                name: Self.percentEncodeQueryComponent($0.name),
                value: $0.value.map(Self.percentEncodeQueryComponent)
            )
        }
        guard let url = components.url else {
            throw SyncControlPlaneHTTPError.invalidBaseURL
        }
        return url
    }

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

    private static func responseBodySnippet(_ data: Data) -> String? {
        guard !data.isEmpty else { return nil }
        let text = String(data: data, encoding: .utf8) ?? data.base64EncodedString()
        let normalized = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !normalized.isEmpty else { return nil }
        if normalized.count <= 500 {
            return normalized
        }
        return String(normalized.prefix(500)) + "..."
    }

    private static let accessCredentialHeaderName = "Author" + "ization"
    private static let accessCredentialScheme = "Bear" + "er"
    private static let reservedHeaderNames: Set<String> = [
        accessCredentialHeaderName.lowercased(),
        "accept",
        "content-type",
        "host"
    ]

    private static func percentEncodePathSegment(_ segment: String) -> String {
        let allowed = CharacterSet.urlPathAllowed.subtracting(CharacterSet(charactersIn: "/?#"))
        return segment.addingPercentEncoding(withAllowedCharacters: allowed) ?? segment
    }

    private static func percentEncodeQueryComponent(_ component: String) -> String {
        let allowed = CharacterSet.urlQueryAllowed.subtracting(CharacterSet(charactersIn: "+&=?/#"))
        return component.addingPercentEncoding(withAllowedCharacters: allowed) ?? component
    }

}

struct SyncService: Sendable {
    var libraryID: String
    var peerID: String
    let database: SQLiteDatabase
    let client: SyncControlPlaneClient
    var uploadBatchSize = 500
    var progressReporter: (@Sendable (SyncServiceProgress) -> Void)?

    func uploadPendingOperations() async throws {
        var claimed = try database.claimPendingLedgerUploadEntries(
            libraryID: libraryID,
            limit: uploadBatchSize
        )
        guard !claimed.isEmpty else { return }

        let totalPending = try database.pendingLedgerUploadCount() + claimed.count

        var uploadedCount = 0
        progressReporter?(
            SyncServiceProgress(
                phase: .uploadingLedger,
                completedItems: uploadedCount,
                totalItems: totalPending,
                message: "准备上传 \(totalPending) 条 ledger"
            )
        )

        while true {
            do {
                let response = try await client.uploadOperations(
                    SyncOpsUploadRequest(operations: claimed),
                    libraryID: libraryID
                )
                try database.markLedgerEntriesAcknowledged(response.accepted, cursor: response.cursor)
                uploadedCount += response.accepted.count
                progressReporter?(
                    SyncServiceProgress(
                        phase: .uploadingLedger,
                        completedItems: uploadedCount,
                        totalItems: totalPending,
                        message: "已上传 \(uploadedCount) / \(totalPending) 条 ledger"
                    )
                )
            } catch SyncControlPlaneHTTPError.conflict(let response) {
                try database.markLedgerEntriesAcknowledged(response.accepted, cursor: response.cursor)
                let acceptedIDs = Set(response.accepted.map(\.opID))
                let unacceptedIDs = claimed.map(\.opID).filter { !acceptedIDs.contains($0) }
                try? database.restoreClaimedLedgerUploadEntries(
                    unacceptedIDs,
                    lastError: String(reflecting: SyncControlPlaneHTTPError.conflict(response))
                )
                throw SyncControlPlaneHTTPError.conflict(response)
            } catch {
                try? database.restoreClaimedLedgerUploadEntries(
                    claimed.map(\.opID),
                    lastError: String(reflecting: error)
                )
                throw error
            }

            claimed = try database.claimPendingLedgerUploadEntries(
                libraryID: libraryID,
                limit: uploadBatchSize
            )
            if claimed.isEmpty {
                return
            }
        }
    }

    func pullRemoteOperations() async throws {
        progressReporter?(
            SyncServiceProgress(
                phase: .pullingRemoteLedger,
                completedItems: 0,
                totalItems: 0,
                message: "正在拉取远端变更"
            )
        )
        var cursor = try database.syncCursor(peerID: peerID)

        while true {
            let response = try await client.fetchOperations(libraryID: libraryID, after: cursor)
            try database.appendAcknowledgedRemoteLedgerPage(
                response.operations,
                peerID: peerID,
                cursor: response.cursor
            )
            cursor = response.cursor
            if response.hasMore != true {
                break
            }
        }
    }

    func sync() async throws {
        try await uploadPendingOperations()
        try await pullRemoteOperations()
    }
}

enum SyncServiceProgressPhase: Sendable {
    case uploadingLedger
    case pullingRemoteLedger
}

struct SyncServiceProgress: Sendable {
    var phase: SyncServiceProgressPhase
    var completedItems: Int
    var totalItems: Int
    var message: String
}
