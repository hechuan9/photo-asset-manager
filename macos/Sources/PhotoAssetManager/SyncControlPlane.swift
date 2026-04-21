import CryptoKit
import Foundation

enum SyncControlPlaneHTTPError: Error, Equatable, Sendable {
    case invalidBaseURL
    case invalidHTTPResponse
    case unexpectedStatusCode(Int)
    case conflict(SyncOpsUploadResponse)
}

struct SyncClientConfiguration: Equatable, Sendable {
    var baseURLString: String
    var libraryID: String
    var peerID: String
    var authModeRawValue: String
    var accessCredential: String
    var awsRegion: String
    var awsAccessKeyID: String
    var awsSecretAccessKey: String
    var awsSessionToken: String

    static func load(defaults: UserDefaults = .standard) -> SyncClientConfiguration {
        SyncClientConfiguration(
            baseURLString: defaults.string(forKey: SyncPreferenceKey.baseURL) ?? "",
            libraryID: defaults.string(forKey: SyncPreferenceKey.libraryID) ?? "local-library",
            peerID: defaults.string(forKey: SyncPreferenceKey.peerID) ?? "control-plane",
            authModeRawValue: defaults.string(forKey: SyncPreferenceKey.authMode) ?? SyncAuthenticationMode.bearer.rawValue,
            accessCredential: defaults.string(forKey: SyncPreferenceKey.accessCredential) ?? "",
            awsRegion: defaults.string(forKey: SyncPreferenceKey.awsRegion) ?? "",
            awsAccessKeyID: defaults.string(forKey: SyncPreferenceKey.awsAccessKeyID) ?? "",
            awsSecretAccessKey: defaults.string(forKey: SyncPreferenceKey.awsSecretAccessKey) ?? "",
            awsSessionToken: defaults.string(forKey: SyncPreferenceKey.awsSessionToken) ?? ""
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

    var awsRegionValue: String? {
        let trimmed = awsRegion.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }

    var awsAccessKeyIDValue: String? {
        let trimmed = awsAccessKeyID.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }

    var awsSecretAccessKeyValue: String? {
        let trimmed = awsSecretAccessKey.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }

    var awsSessionTokenValue: String? {
        let trimmed = awsSessionToken.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }

    var requestAuthentication: SyncRequestAuthentication? {
        switch authMode {
        case .bearer:
            return accessCredentialValue.map(SyncRequestAuthentication.bearer)
        case .awsIAM:
            guard
                let region = awsRegionValue,
                let accessKeyID = awsAccessKeyIDValue,
                let secretAccessKey = awsSecretAccessKeyValue
            else {
                return nil
            }
            return .awsIAM(
                SyncAWSIAMCredentials(
                    region: region,
                    accessKeyID: accessKeyID,
                    secretAccessKey: secretAccessKey,
                    sessionToken: awsSessionTokenValue
                )
            )
        }
    }

    var isAuthenticationConfigured: Bool {
        switch authMode {
        case .bearer:
            return true
        case .awsIAM:
            return requestAuthentication != nil
        }
    }

    var hasRemoteSync: Bool {
        baseURL != nil && isAuthenticationConfigured
    }
}

enum SyncAuthenticationMode: String, CaseIterable, Equatable, Identifiable, Sendable {
    case bearer = "bearer"
    case awsIAM = "aws_iam"

    var id: String { rawValue }

    var displayName: String {
        switch self {
        case .bearer:
            return "Bearer"
        case .awsIAM:
            return "AWS IAM"
        }
    }
}

struct SyncAWSIAMCredentials: Equatable, Sendable {
    var region: String
    var accessKeyID: String
    var secretAccessKey: String
    var sessionToken: String?
}

enum SyncRequestAuthentication: Equatable, Sendable {
    case bearer(String)
    case awsIAM(SyncAWSIAMCredentials)
}

enum SyncPreferenceKey {
    static let baseURL = "ios.sync.base_url"
    static let libraryID = "ios.sync.library_id"
    static let peerID = "ios.sync.peer_id"
    static let authMode = "ios.sync.auth_mode"
    static let accessCredential = "ios.sync.access_credential"
    static let awsRegion = "ios.sync.aws_region"
    static let awsAccessKeyID = "ios.sync.aws_access_key_id"
    static let awsSecretAccessKey = "ios.sync.aws_secret_access_key"
    static let awsSessionToken = "ios.sync.aws_session_token"
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
            throw SyncControlPlaneHTTPError.unexpectedStatusCode(httpResponse.statusCode)
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
            throw SyncControlPlaneHTTPError.unexpectedStatusCode(httpResponse.statusCode)
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
            throw SyncControlPlaneHTTPError.unexpectedStatusCode(httpResponse.statusCode)
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
            throw SyncControlPlaneHTTPError.unexpectedStatusCode(httpResponse.statusCode)
        }
        return try Self.makeDecoder().decode(DerivativeMetadataResponse.self, from: data)
    }

    private func sendJSON<Body: Encodable>(method: String, url: URL, body: Body) async throws {
        let encodedBody = try Self.makeEncoder().encode(body)
        let (data, response) = try await send(method: method, url: url, body: encodedBody)
        guard let httpResponse = response as? HTTPURLResponse else {
            throw SyncControlPlaneHTTPError.invalidHTTPResponse
        }
        guard 200..<300 ~= httpResponse.statusCode else {
            throw SyncControlPlaneHTTPError.unexpectedStatusCode(httpResponse.statusCode)
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
        case .awsIAM(let credentials):
            try Self.signAWSIAMRequest(
                &request,
                credentials: credentials,
                timestamp: dateProvider()
            )
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

    private static let accessCredentialHeaderName = "Author" + "ization"
    private static let accessCredentialScheme = "Bear" + "er"
    private static let reservedHeaderNames: Set<String> = [
        accessCredentialHeaderName.lowercased(),
        "accept",
        "content-type",
        "host",
        "x-amz-content-sha256",
        "x-amz-date",
        "x-amz-security-token"
    ]

    private static func percentEncodePathSegment(_ segment: String) -> String {
        let allowed = CharacterSet.urlPathAllowed.subtracting(CharacterSet(charactersIn: "/?#"))
        return segment.addingPercentEncoding(withAllowedCharacters: allowed) ?? segment
    }

    private static func percentEncodeQueryComponent(_ component: String) -> String {
        let allowed = CharacterSet.urlQueryAllowed.subtracting(CharacterSet(charactersIn: "+&=?/#"))
        return component.addingPercentEncoding(withAllowedCharacters: allowed) ?? component
    }

    private static func signAWSIAMRequest(
        _ request: inout URLRequest,
        credentials: SyncAWSIAMCredentials,
        timestamp: Date
    ) throws {
        guard let url = request.url else {
            throw SyncControlPlaneHTTPError.invalidBaseURL
        }
        guard let host = url.host?.lowercased(), !host.isEmpty else {
            throw SyncControlPlaneHTTPError.invalidBaseURL
        }

        let body = request.httpBody ?? Data()
        let payloadHash = sha256Hex(body)
        let amzDate = awsTimestampFormatter.string(from: timestamp)
        let dateStamp = awsDateFormatter.string(from: timestamp)
        request.setValue(host, forHTTPHeaderField: "Host")
        request.setValue(payloadHash, forHTTPHeaderField: "X-Amz-Content-Sha256")
        request.setValue(amzDate, forHTTPHeaderField: "X-Amz-Date")
        if let sessionToken = credentials.sessionToken, !sessionToken.isEmpty {
            request.setValue(sessionToken, forHTTPHeaderField: "X-Amz-Security-Token")
        } else {
            request.setValue(nil, forHTTPHeaderField: "X-Amz-Security-Token")
        }

        let canonicalHeaders = canonicalAWSHeaders(from: request)
        let signedHeaders = canonicalHeaders.map(\.name).joined(separator: ";")
        let canonicalRequest = [
            request.httpMethod ?? "GET",
            canonicalAWSPath(from: url),
            canonicalAWSQuery(from: url),
            canonicalHeaders.map { "\($0.name):\($0.value)\n" }.joined(),
            signedHeaders,
            payloadHash
        ].joined(separator: "\n")

        let credentialScope = "\(dateStamp)/\(credentials.region)/execute-api/aws4_request"
        let stringToSign = [
            "AWS4-HMAC-SHA256",
            amzDate,
            credentialScope,
            sha256Hex(Data(canonicalRequest.utf8))
        ].joined(separator: "\n")
        let signingKey = awsSigningKey(secretAccessKey: credentials.secretAccessKey, dateStamp: dateStamp, region: credentials.region)
        let signature = hmacSHA256Hex(key: signingKey, message: stringToSign)
        let authorization = "AWS4-HMAC-SHA256 Credential=\(credentials.accessKeyID)/\(credentialScope), SignedHeaders=\(signedHeaders), Signature=\(signature)"
        request.setValue(authorization, forHTTPHeaderField: accessCredentialHeaderName)
    }

    private static func canonicalAWSPath(from url: URL) -> String {
        let components = URLComponents(url: url, resolvingAgainstBaseURL: false)
        let path = (components?.percentEncodedPath.isEmpty == false ? components?.percentEncodedPath : nil) ?? "/"
        return path.split(separator: "/", omittingEmptySubsequences: false)
            .map { percentEncodeCanonicalURIComponent(String($0)) }
            .joined(separator: "/")
    }

    private static func canonicalAWSQuery(from url: URL) -> String {
        guard let percentEncodedQuery = URLComponents(url: url, resolvingAgainstBaseURL: false)?.percentEncodedQuery,
              !percentEncodedQuery.isEmpty else {
            return ""
        }

        return percentEncodedQuery
            .split(separator: "&")
            .map(String.init)
            .map { component -> (String, String) in
                let parts = component.split(separator: "=", maxSplits: 1, omittingEmptySubsequences: false).map(String.init)
                let name = percentEncodeCanonicalQueryComponent(parts[0])
                let value = parts.count == 2 ? percentEncodeCanonicalQueryComponent(parts[1]) : ""
                return (name, value)
            }
            .sorted {
                if $0.0 == $1.0 {
                    return $0.1 < $1.1
                }
                return $0.0 < $1.0
            }
            .map { "\($0.0)=\($0.1)" }
            .joined(separator: "&")
    }

    private static func canonicalAWSHeaders(from request: URLRequest) -> [(name: String, value: String)] {
        (request.allHTTPHeaderFields ?? [:])
            .map { header, value in
                (
                    name: header.trimmingCharacters(in: .whitespacesAndNewlines).lowercased(),
                    value: normalizeAWSHeaderValue(value)
                )
            }
            .sorted { $0.name < $1.name }
    }

    private static func normalizeAWSHeaderValue(_ value: String) -> String {
        value
            .split(whereSeparator: \.isWhitespace)
            .joined(separator: " ")
    }

    private static func percentEncodeCanonicalURIComponent(_ component: String) -> String {
        let allowed = CharacterSet(charactersIn: "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")
        return component.addingPercentEncoding(withAllowedCharacters: allowed) ?? component
    }

    private static func percentEncodeCanonicalQueryComponent(_ component: String) -> String {
        let decoded = component.removingPercentEncoding ?? component
        let allowed = CharacterSet(charactersIn: "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")
        return decoded.addingPercentEncoding(withAllowedCharacters: allowed) ?? component
    }

    private static func awsSigningKey(secretAccessKey: String, dateStamp: String, region: String) -> SymmetricKey {
        let secret = "AWS4\(secretAccessKey)"
        let dateKey = hmacSHA256(keyData: Data(secret.utf8), message: dateStamp)
        let regionKey = hmacSHA256(keyData: dateKey, message: region)
        let serviceKey = hmacSHA256(keyData: regionKey, message: "execute-api")
        let signingKeyData = hmacSHA256(keyData: serviceKey, message: "aws4_request")
        return SymmetricKey(data: signingKeyData)
    }

    private static func hmacSHA256(keyData: Data, message: String) -> Data {
        let key = SymmetricKey(data: keyData)
        return Data(HMAC<SHA256>.authenticationCode(for: Data(message.utf8), using: key))
    }

    private static func hmacSHA256Hex(key: SymmetricKey, message: String) -> String {
        let signature = HMAC<SHA256>.authenticationCode(for: Data(message.utf8), using: key)
        return Data(signature).map { String(format: "%02x", $0) }.joined()
    }

    private static func sha256Hex(_ data: Data) -> String {
        SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
    }

    private static let awsTimestampFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyyMMdd'T'HHmmss'Z'"
        return formatter
    }()

    private static let awsDateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyyMMdd"
        return formatter
    }()
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
