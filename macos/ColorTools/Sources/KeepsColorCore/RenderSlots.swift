import Foundation
import Darwin

/// Separate file descriptions let flock coordinate all photo workers, including after a restart.
public final class RenderSlots {
    private let handles: [FileHandle]

    public init(directory: URL, limit: Int) throws {
        guard (1...20).contains(limit) else { throw NSError(domain: "KeepsRenderSlots", code: 1, userInfo: [NSLocalizedDescriptionKey: "KEEPS_RENDER_LIMIT must be between 1 and 20"]) }
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        handles = try (0..<limit).map { index in
            let path = directory.appendingPathComponent("render-\(index).lock").path
            let descriptor = open(path, O_RDWR | O_CREAT | O_CLOEXEC, 0o600)
            guard descriptor >= 0 else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
            return FileHandle(fileDescriptor: descriptor, closeOnDealloc: true)
        }
    }

    public static func configured(environment: [String: String] = ProcessInfo.processInfo.environment) throws -> RenderSlots? {
        guard environment["KEEPS_RENDER_LIMIT"] != nil || environment["KEEPS_RENDER_SLOTS"] != nil else { return nil }
        guard let rawLimit = environment["KEEPS_RENDER_LIMIT"], let limit = Int(rawLimit),
              let path = environment["KEEPS_RENDER_SLOTS"], path.hasPrefix("/") else {
            throw NSError(domain: "KeepsRenderSlots", code: 2, userInfo: [NSLocalizedDescriptionKey: "Set KEEPS_RENDER_LIMIT and an absolute KEEPS_RENDER_SLOTS directory together"])
        }
        return try RenderSlots(directory: URL(fileURLWithPath: path), limit: limit)
    }

    public func acquire(cancelled: () -> Bool = { false }) throws -> FileHandle {
        while true {
            if cancelled() { throw CancellationError() }
            if let handle = try acquireAvailable() { return handle }
            Thread.sleep(forTimeInterval: 0.1)
        }
    }

    private func acquireAvailable() throws -> FileHandle? {
        for handle in handles {
            if flock(handle.fileDescriptor, LOCK_EX | LOCK_NB) == 0 { return handle }
            guard errno == EWOULDBLOCK || errno == EINTR else {
                throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
            }
        }
        return nil
    }

    public static func withSlot<T: Sendable>(directory: URL, limit: Int,
                                             operation: @Sendable () async throws -> T) async throws -> T {
        let slots = try RenderSlots(directory: directory, limit: limit)
        while true {
            try Task.checkCancellation()
            if let handle = try slots.acquireAvailable() {
                defer { release(handle) }
                return try await operation()
            }
            try await Task.sleep(for: .milliseconds(100))
        }
    }

    public static func release(_ handle: FileHandle) { flock(handle.fileDescriptor, LOCK_UN) }
}
