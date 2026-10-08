import Foundation
import Testing
import KeepsAPI
import CryptoKit
@testable import PhotoAssetManager

@MainActor struct AIEditingSettingsTests {
    @Test func bundledSampleIsLocalAndMissingResourceFails() throws {
        let runtime = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: runtime) }
        try FileManager.default.createDirectory(at: runtime, withIntermediateDirectories: true)
        #expect(throws: (any Error).self) { try AIEditingSettingsStore.bundledSample(in: runtime) }
        let sample = runtime.appendingPathComponent("sample.jpg")
        try Data([0xff, 0xd8, 0xff, 0xd9]).write(to: sample)
        #expect(try AIEditingSettingsStore.bundledSample(in: runtime) == sample)
    }

    @Test func environmentDoesNotInheritHostAuthentication() {
        let home = URL(fileURLWithPath: "/tmp/keeps-only")
        let env = AIEditingProcess.environment(home: home, ambient: [
            "HOME": "/Users/test", "CODEX_HOME": "/host/codex", "OPENAI_API_KEY": "secret",
            "CODEX_ACCESS_TOKEN": "secret", "OPENAI_BASE_URL": "https://unexpected.invalid", "PATH": "/host/bin"])
        #expect(env["CODEX_HOME"] == home.path)
        #expect(env["HOME"] == "/Users/test")
        #expect(env["OPENAI_API_KEY"] == nil)
        #expect(env["CODEX_ACCESS_TOKEN"] == nil)
        #expect(env["OPENAI_BASE_URL"] == nil)
        #expect(env["PATH"] == "/usr/bin:/bin:/usr/sbin:/sbin")
    }

    @Test func requiresMatchingChatGPTAccount() throws {
        let payload = try JSONSerialization.data(withJSONObject: ["email": "hechuan@climamind.ai"])
        let encoded = payload.base64EncodedString().replacingOccurrences(of: "=", with: "").replacingOccurrences(of: "+", with: "-").replacingOccurrences(of: "/", with: "_")
        let auth: [String: Any] = ["tokens": ["id_token": "header.\(encoded).signature"]]
        #expect(try AIEditingSettingsStore.emailFromAuth(JSONSerialization.data(withJSONObject: auth)) == "hechuan@climamind.ai")
        try AIEditingSettingsStore.validateAccount(actual: "hechuan@climamind.ai", expected: " HECHUAN@climamind.ai ")
        #expect(throws: (any Error).self) { try AIEditingSettingsStore.validateAccount(actual: "other@example.com", expected: "hechuan@climamind.ai") }
        var mixed = auth; mixed["OPENAI_API_KEY"] = "not-allowed"
        #expect(throws: (any Error).self) { try AIEditingSettingsStore.emailFromAuth(JSONSerialization.data(withJSONObject: mixed)) }
    }

    @Test func changingExpectedAccountInvalidatesConnection() {
        let suite = "KeepsAI-\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suite)!
        defer { defaults.removePersistentDomain(forName: suite) }
        let store = AIEditingSettingsStore(defaults: defaults)
        store.expectedEmail = "different@example.com"
        #expect(!store.connectionVerified)
        #expect(defaults.string(forKey: "aiEditing.expectedEmail") == "different@example.com")
    }

    @Test func resultMustMatchRenderedCandidateAndStayInJob() throws {
        let job = FileManager.default.temporaryDirectory.appendingPathComponent("keeps-result-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: job) }
        let render = job.appendingPathComponent("render")
        try FileManager.default.createDirectory(at: render, withIntermediateDirectories: true)
        func write(_ value: [String: Any], _ file: String) throws { try JSONSerialization.data(withJSONObject: value).write(to: job.appendingPathComponent(file)) }
        try write(["status": "selected", "candidateID": "one", "reason": "测试"], "result.json")
        try write(["candidates": [["id": "one"]]], "render/candidates.json")
        let preview = render.appendingPathComponent("one-preview.jpg"), full = render.appendingPathComponent("one-full.jpg")
        try Data([1]).write(to: preview); try Data([1]).write(to: full)
        try write(["candidateID": "one", "preview": preview.path, "fullSize": full.path], "render/selection.json")
        #expect(try AIEditingSettingsStore.validateResult(in: job).selected)
        try write(["candidateID": "other", "preview": preview.path, "fullSize": full.path], "render/selection.json")
        #expect(throws: (any Error).self) { try AIEditingSettingsStore.validateResult(in: job) }
        try write(["candidateID": "one", "preview": preview.path, "fullSize": "/etc/hosts"], "render/selection.json")
        #expect(throws: (any Error).self) { try AIEditingSettingsStore.validateResult(in: job) }
    }

    @Test func missingOrDifferentAccountNeverStartsCodex() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent("keeps-auth-test-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: root) }
        let runtime = root.appendingPathComponent("runtime")
        let started = root.appendingPathComponent("started")
        for file in ["codex", "keeps-color-mcp", "darktable.app/Contents/MacOS/darktable-cli"] {
            let url = runtime.appendingPathComponent(file)
            try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
            try Data("#!/bin/sh\necho started > '\(started.path)'\n".utf8).write(to: url)
            try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: url.path)
        }
        try Data("test skill".utf8).write(to: runtime.appendingPathComponent("SKILL.md"))
        let suite = "KeepsAI-\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suite)!
        defer { defaults.removePersistentDomain(forName: suite) }
        let store = AIEditingSettingsStore(root: root, runtime: runtime, defaults: defaults)
        await store.prepare()
        #expect(store.runtimeReady)
        await store.testConnection()
        #expect(store.errorMessage != nil)
        #expect(!FileManager.default.fileExists(atPath: started.path))
        let payload = try JSONSerialization.data(withJSONObject: ["email": "wrong@example.com"]).base64EncodedString()
        let auth: [String: Any] = ["tokens": ["id_token": "header.\(payload).signature"]]
        try JSONSerialization.data(withJSONObject: auth).write(to: root.appendingPathComponent("codex/auth.json"))
        await store.refresh()
        #expect(store.errorMessage?.contains("不一致") == true)
        await store.testConnection()
        #expect(!FileManager.default.fileExists(atPath: started.path))
        #expect(!store.connectionVerified)
    }

    @Test func authorizationURLStaysOutOfLogs() {
        let text = "Open https://auth.openai.com/oauth/authorize?state=secret&code_challenge=private\n"
        #expect(AIEditingSettingsStore.authorizationURL(in: text)?.host == "auth.openai.com")
        let redacted = AIEditingSettingsStore.redacted(text + " eyJabc.defghi.jklmnop")
        #expect(!redacted.contains("state=secret"))
        #expect(!redacted.contains("eyJabc"))
    }

    @Test func processTimeoutAndCancellationFinish() async throws {
        let directory = FileManager.default.temporaryDirectory
        let process = AIEditingProcess()
        await #expect(throws: (any Error).self) {
            _ = try await process.run(executable: URL(fileURLWithPath: "/bin/sleep"), arguments: ["30"], home: directory, directory: directory, timeout: 0.1)
        }
        let task = Task { try await process.run(executable: URL(fileURLWithPath: "/bin/sleep"), arguments: ["30"], home: directory, directory: directory, timeout: 60) }
        try await Task.sleep(for: .milliseconds(100))
        task.cancel()
        await #expect(throws: (any Error).self) { _ = try await task.value }
        let result = try await process.run(executable: URL(fileURLWithPath: "/bin/echo"), arguments: ["finished"], home: directory, directory: directory, timeout: 5)
        #expect(result.output == "finished\n")
    }
}
