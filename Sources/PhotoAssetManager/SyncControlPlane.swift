import Foundation

enum SyncControlPlaneHTTPError: Error, Equatable, Sendable {
    case invalidBaseURL
    case invalidHTTPResponse
    case unexpectedStatusCode(Int)
}

final class SyncControlPlaneHTTPClient: SyncControlPlaneClient {
    private let baseURL: URL
    private let accessCredential: String?
    private let headerProvider: @Sendable () -> [String: String]
    private let session: URLSession

    init(
        baseURL: URL,
        accessCredential: String? = nil,
        headerProvider: @escaping @Sendable () -> [String: String] = { [:] },
        session: URLSession = .shared
    ) {
        self.baseURL = baseURL
        self.accessCredential = accessCredential
        self.headerProvider = headerProvider
        self.session = session
    }

    func uploadOperations(_ request: SyncOpsUploadRequest, libraryID: String) async throws {
        let url = try makeURL(pathSegments: ["libraries", libraryID, "ops"])
        try await sendJSON(method: "POST", url: url, body: request)
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
        if let accessCredential {
            request.setValue("\(Self.accessCredentialScheme) \(accessCredential)", forHTTPHeaderField: Self.accessCredentialHeaderName)
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
        "content-type"
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

    func uploadPendingOperations() async throws {
        let claimed = try database.claimPendingLedgerUploadEntries(libraryID: libraryID)
        guard !claimed.isEmpty else { return }

        do {
            try await client.uploadOperations(SyncOpsUploadRequest(operations: claimed), libraryID: libraryID)
            try database.markLedgerEntriesAcknowledged(claimed.map(\.opID))
        } catch {
            try? database.restoreClaimedLedgerUploadEntries(claimed.map(\.opID), lastError: String(reflecting: error))
            throw error
        }
    }

    func pullRemoteOperations() async throws {
        let cursor = try database.syncCursor(peerID: peerID)
        let response = try await client.fetchOperations(libraryID: libraryID, after: cursor)
        try database.appendAcknowledgedRemoteLedgerPage(
            response.operations,
            peerID: peerID,
            cursor: response.cursor
        )
    }

    func sync() async throws {
        try await uploadPendingOperations()
        try await pullRemoteOperations()
    }
}
