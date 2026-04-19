import Foundation

struct NASMountReport: Sendable {
    var mountedRoots: [String] = []
    var alreadyAvailableRoots: [String] = []
    var failedRoots: [String] = []

    var hasFailures: Bool {
        !failedRoots.isEmpty
    }

    var checkedRootCount: Int {
        mountedRoots.count + alreadyAvailableRoots.count + failedRoots.count
    }
}

struct NASMountManager: Sendable {
    private let timeoutSeconds: TimeInterval

    init(timeoutSeconds: TimeInterval = 18) {
        self.timeoutSeconds = timeoutSeconds
    }

    func mountNASRootsIfNeeded(for sources: [SourceDirectory]) async -> NASMountReport {
        let roots = uniqueVolumeRoots(from: sources)
        guard !roots.isEmpty else { return NASMountReport() }

        let timeoutSeconds = timeoutSeconds
        return await Task.detached(priority: .utility) {
            var report = NASMountReport()
            for root in roots {
                if FileManager.default.fileExists(atPath: root.path) {
                    report.alreadyAvailableRoots.append(root.path)
                    continue
                }

                let mounted = Self.mount(root: root, timeoutSeconds: timeoutSeconds)
                if mounted {
                    report.mountedRoots.append(root.path)
                } else {
                    report.failedRoots.append(root.path)
                }
            }
            return report
        }.value
    }

    private func uniqueVolumeRoots(from sources: [SourceDirectory]) -> [NASVolumeRoot] {
        var seen: Set<String> = []
        var roots: [NASVolumeRoot] = []
        for source in sources where source.storageKind == .nas {
            guard let root = NASVolumeRoot(path: source.path, host: nasHost()) else { continue }
            guard seen.insert(root.path).inserted else { continue }
            roots.append(root)
        }
        return roots.sorted { $0.path < $1.path }
    }

    private func nasHost() -> String {
        UserDefaults.standard.string(forKey: "nasSMBHost") ?? "chuan_nas.local"
    }

    private static func mount(root: NASVolumeRoot, timeoutSeconds: TimeInterval) -> Bool {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/osascript")
        process.arguments = ["-e", "mount volume \"\(appleScriptEscaped(root.smbURL))\""]

        do {
            try process.run()
        } catch {
            return false
        }

        let semaphore = DispatchSemaphore(value: 0)
        DispatchQueue.global(qos: .utility).async {
            process.waitUntilExit()
            semaphore.signal()
        }

        if semaphore.wait(timeout: .now() + timeoutSeconds) == .timedOut {
            process.terminate()
            return false
        }

        return process.terminationStatus == 0 && FileManager.default.fileExists(atPath: root.path)
    }

    private static func appleScriptEscaped(_ value: String) -> String {
        value
            .replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "\"", with: "\\\"")
    }
}

private struct NASVolumeRoot: Sendable {
    let path: String
    let smbURL: String

    init?(path sourcePath: String, host: String) {
        let components = URL(fileURLWithPath: sourcePath).pathComponents
        guard components.count >= 3, components[1] == "Volumes" else { return nil }
        let shareName = components[2]
        path = "/Volumes/\(shareName)"
        smbURL = "smb://\(host)/\(shareName.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? shareName)"
    }
}
