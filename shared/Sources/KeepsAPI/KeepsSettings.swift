import Foundation
import Security

public enum KeepsSettings {
    public static let baseURLKey = "ios.sync.base_url"
    public static let libraryIDKey = "ios.sync.library_id"
    private static let legacyCredentialKey = "ios.sync.access_credential"
    private static let service = "local.keeps.nas"
    private static let account = "access-credential"

    public static func load(defaults: UserDefaults = .standard) throws -> KeepsConfiguration? {
        let base = (defaults.string(forKey: baseURLKey) ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        guard !base.isEmpty else { return nil }
        let library = defaults.string(forKey: libraryIDKey) ?? "local-library"
        let url = try validate(base, library: library)
        var credential = try readCredential()
        if credential == nil, let legacy = defaults.string(forKey: legacyCredentialKey), !legacy.isEmpty {
            try storeCredential(legacy)
            credential = legacy
        }
        if credential != nil { defaults.removeObject(forKey: legacyCredentialKey) }
        return KeepsConfiguration(baseURL: url, libraryID: library, accessCredential: credential)
    }

    @discardableResult public static func save(baseURLString: String, libraryID: String, accessCredential: String, defaults: UserDefaults = .standard) throws -> KeepsConfiguration {
        let base = baseURLString.trimmingCharacters(in: .whitespacesAndNewlines)
        let library = libraryID.trimmingCharacters(in: .whitespacesAndNewlines)
        let url = try validate(base, library: library)
        let credential = accessCredential.trimmingCharacters(in: .whitespacesAndNewlines)
        try storeCredential(credential)
        defaults.set(base, forKey: baseURLKey)
        defaults.set(library, forKey: libraryIDKey)
        defaults.removeObject(forKey: legacyCredentialKey)
        return KeepsConfiguration(baseURL: url, libraryID: library, accessCredential: credential.isEmpty ? nil : credential)
    }

    private static func validate(_ base: String, library: String) throws -> URL {
        guard let url = URL(string: base), ["http", "https"].contains(url.scheme?.lowercased() ?? ""), url.host != nil,
              url.user == nil, url.password == nil, url.query == nil, url.fragment == nil, !library.isEmpty else { throw KeepsAPIError.invalidConfiguration }
        return url
    }
    private static var query: [String: Any] {
        [kSecClass as String: kSecClassGenericPassword, kSecAttrService as String: service, kSecAttrAccount as String: account]
    }
    private static func readCredential() throws -> String? {
        var request = query
        request[kSecReturnData as String] = true
        request[kSecMatchLimit as String] = kSecMatchLimitOne
        var item: CFTypeRef?
        let status = SecItemCopyMatching(request as CFDictionary, &item)
        if status == errSecItemNotFound { return nil }
        guard status == errSecSuccess else { throw keychainError(status) }
        guard let data = item as? Data, let result = String(data: data, encoding: .utf8) else { throw KeepsAPIError.invalidResponse }
        return result
    }
    private static func storeCredential(_ credential: String) throws {
        if credential.isEmpty {
            let status = SecItemDelete(query as CFDictionary)
            guard status == errSecSuccess || status == errSecItemNotFound else { throw keychainError(status) }
            return
        }
        let changes = [kSecValueData as String: Data(credential.utf8)]
        let status = SecItemUpdate(query as CFDictionary, changes as CFDictionary)
        if status == errSecItemNotFound {
            var item = query.merging(changes) { _, new in new }
            item[kSecAttrAccessible as String] = kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
            let added = SecItemAdd(item as CFDictionary, nil)
            guard added == errSecSuccess else { throw keychainError(added) }
        } else if status != errSecSuccess { throw keychainError(status) }
    }
    private static func keychainError(_ status: OSStatus) -> NSError {
        NSError(domain: NSOSStatusErrorDomain, code: Int(status), userInfo: [NSLocalizedDescriptionKey: SecCopyErrorMessageString(status, nil) as String? ?? "Keychain error \(status)"])
    }
}
