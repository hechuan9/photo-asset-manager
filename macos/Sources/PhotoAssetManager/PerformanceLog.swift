import Foundation
import OSLog

enum PerformanceLog {
    private static let logger = Logger(subsystem: "PhotoAssetManager", category: "Performance")

    @discardableResult
    static func measure<T>(_ label: String, _ work: () throws -> T) rethrows -> T {
        let start = Date()
        defer {
            let milliseconds = Date().timeIntervalSince(start) * 1000
            logger.notice("\(label, privacy: .public) \(milliseconds, format: .fixed(precision: 1)) ms")
        }
        return try work()
    }

    static func event(_ label: String, detail: String = "") {
        if detail.isEmpty {
            logger.notice("\(label, privacy: .public)")
        } else {
            logger.notice("\(label, privacy: .public) \(detail, privacy: .public)")
        }
    }
}
