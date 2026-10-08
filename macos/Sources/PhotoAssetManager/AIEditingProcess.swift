import Foundation
import Darwin

struct AIEditingFailure: LocalizedError {
    let message: String
    var errorDescription: String? { message }
    init(_ message: String) { self.message = message }
}

private final class AIProcessOutput: @unchecked Sendable {
    private let lock = NSLock()
    private var bytes = Data()
    private var ended = false
    func finish() { lock.lock(); ended = true; lock.unlock() }
    func isFinished() -> Bool { lock.lock(); defer { lock.unlock() }; return ended }
    func append(_ data: Data) {
        lock.lock(); defer { lock.unlock() }
        bytes.append(data)
        if bytes.count > 32 * 1024 * 1024 { bytes.removeFirst(bytes.count - 32 * 1024 * 1024) }
    }
    func text() -> String {
        lock.lock(); defer { lock.unlock() }
        return String(decoding: bytes, as: UTF8.self)
    }
}

@MainActor final class AIEditingProcess {
    struct Result { let exitCode: Int32; let output: String; let errors: String }
    private var process: Process?
    private var cancelled = false
    private(set) var lastOutput = ""
    private(set) var lastErrors = ""

    static func environment(home: URL, ambient: [String: String]) -> [String: String] {
        // Authentication and configuration are supplied exclusively by Keeps.
        var environment: [String: String] = ["CODEX_HOME": home.path, "PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "LANG": "en_US.UTF-8"]
        for key in ["HOME", "TMPDIR", "USER", "LOGNAME", "SSL_CERT_FILE", "SSL_CERT_DIR"] {
            if let value = ambient[key] { environment[key] = value }
        }
        return environment
    }

    func run(executable: URL, arguments: [String], home: URL, directory: URL, input: String? = nil,
             timeout: TimeInterval, progress: ((String) -> Void)? = nil) async throws -> Result {
        guard process == nil else { throw AIEditingFailure("已有 AI 进程正在运行。") }
        cancelled = false
        lastOutput = ""; lastErrors = ""
        let child = Process(), outputPipe = Pipe(), errorPipe = Pipe(), inputPipe = Pipe()
        let output = AIProcessOutput(), errors = AIProcessOutput()
        child.executableURL = executable
        child.arguments = arguments
        child.currentDirectoryURL = directory
        child.environment = Self.environment(home: home, ambient: ProcessInfo.processInfo.environment)
        child.standardOutput = outputPipe; child.standardError = errorPipe; child.standardInput = inputPipe
        defer {
            if child.isRunning {
                for pid in processTree(child.processIdentifier).reversed() { Darwin.kill(pid, SIGKILL) }
            }
            lastOutput = output.text(); lastErrors = errors.text()
            try? inputPipe.fileHandleForWriting.close()
            process = nil
        }
        try child.run()
        process = child
        for (handle, collector) in [(outputPipe.fileHandleForReading, output), (errorPipe.fileHandleForReading, errors)] {
            DispatchQueue.global(qos: .utility).async {
                while true {
                    let data = handle.availableData
                    if data.isEmpty { break }
                    collector.append(data)
                }
                collector.finish()
            }
        }
        if let input { try inputPipe.fileHandleForWriting.write(contentsOf: Data(input.utf8)) }
        try inputPipe.fileHandleForWriting.close()
        let deadline = Date().addingTimeInterval(timeout)
        while child.isRunning {
            progress?(output.text() + "\n" + errors.text())
            if cancelled || Task.isCancelled || Date() >= deadline {
                let wasCancelled = cancelled || Task.isCancelled
                let descendants = processTree(child.processIdentifier)
                for pid in descendants.reversed() { Darwin.kill(pid, SIGTERM) }
                let stopDeadline = Date().addingTimeInterval(3)
                while child.isRunning && Date() < stopDeadline { try? await Task.sleep(for: .milliseconds(100)) }
                for pid in descendants.reversed() { Darwin.kill(pid, SIGKILL) }
                throw AIEditingFailure(wasCancelled ? "操作已取消。" : "操作超时，请检查网络后重试。")
            }
            try? await Task.sleep(for: .milliseconds(100))
        }
        let drainDeadline = Date().addingTimeInterval(5)
        while !output.isFinished() || !errors.isFinished() {
            guard Date() < drainDeadline else { throw AIEditingFailure("子进程输出未能结束。") }
            try? await Task.sleep(for: .milliseconds(20))
        }
        progress?(output.text() + "\n" + errors.text())
        return Result(exitCode: child.terminationStatus, output: output.text(), errors: errors.text())
    }

    func cancel() { cancelled = true }

    private func processTree(_ root: Int32) -> [Int32] {
        // Stop the renderer and MCP descendants too, so cancellation releases source files.
        let listing = Process(), pipe = Pipe()
        listing.executableURL = URL(fileURLWithPath: "/bin/ps")
        listing.arguments = ["-axo", "pid=,ppid="]
        listing.standardOutput = pipe
        do { try listing.run() } catch { return [root] }
        let data = pipe.fileHandleForReading.readDataToEndOfFile()
        listing.waitUntilExit()
        let pairs = String(decoding: data, as: UTF8.self).split(separator: "\n").compactMap { line -> (Int32, Int32)? in
            let fields = line.split(whereSeparator: \.isWhitespace).compactMap { Int32($0) }
            return fields.count == 2 ? (fields[0], fields[1]) : nil
        }
        var descendants: [Int32] = [root]
        var offset = 0
        while offset < descendants.count {
            descendants.append(contentsOf: pairs.filter { $0.1 == descendants[offset] }.map(\.0))
            offset += 1
        }
        return descendants
    }
}
