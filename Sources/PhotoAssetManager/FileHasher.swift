import CryptoKit
import Foundation

enum FileHashError: LocalizedError {
    case unreadable(URL)

    var errorDescription: String? {
        switch self {
        case .unreadable(let url): "无法读取文件用于 hash：\(url.path)"
        }
    }
}

enum FileHasher {
    static func sha256(url: URL) throws -> String {
        guard let handle = try? FileHandle(forReadingFrom: url) else {
            throw FileHashError.unreadable(url)
        }
        defer { try? handle.close() }

        var hasher = SHA256()
        while true {
            let data = try handle.read(upToCount: 1024 * 1024) ?? Data()
            if data.isEmpty { break }
            hasher.update(data: data)
        }
        return hasher.finalize().map { String(format: "%02x", $0) }.joined()
    }
}
