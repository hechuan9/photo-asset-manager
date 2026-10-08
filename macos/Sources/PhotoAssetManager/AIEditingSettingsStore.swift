import Foundation
import Combine
import CryptoKit

@MainActor final class AIEditingSettingsStore: ObservableObject {
    @Published var expectedEmail: String {
        didSet {
            defaults.set(expectedEmail, forKey: "aiEditing.expectedEmail")
            connectionVerified = false
        }
    }
    @Published private(set) var accountEmail: String?
    @Published private(set) var status = "尚未检查"
    @Published private(set) var errorMessage: String?
    @Published private(set) var isBusy = false
    @Published private(set) var loginURL: URL?
    @Published private(set) var runtimeReady = false
    @Published private(set) var connectionVerified = false
    @Published private(set) var resultPreview: URL?
    @Published private(set) var resultSummary: String?
    let logDirectory: URL
    let root: URL
    let runtime: URL
    let codeRuntime: URL
    private let defaults: UserDefaults
    private let runner = AIEditingProcess()
    private var home: URL { root.appendingPathComponent("codex", isDirectory: true) }
    private var executable: URL { codeRuntime.appendingPathComponent("codex") }
    private let model = "gpt-6-luna"

    init(root: URL? = nil, runtime: URL? = nil, defaults: UserDefaults = .standard) {
        self.defaults = defaults
        self.expectedEmail = defaults.string(forKey: "aiEditing.expectedEmail") ?? "hechuan@climamind.ai"
        self.root = root ?? FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("Keeps/AIEditing", isDirectory: true)
        self.runtime = runtime ?? (Bundle.main.resourceURL ?? Bundle.main.bundleURL).appendingPathComponent("AIEditing", isDirectory: true)
        self.codeRuntime = runtime ?? Bundle.main.bundleURL.appendingPathComponent("Contents/Helpers", isDirectory: true)
        self.logDirectory = self.root.appendingPathComponent("logs", isDirectory: true)
    }

    func prepare() async { await refresh() }

    func refresh() async {
        guard !isBusy else { return }
        errorMessage = nil
        do {
            try prepareDirectories()
            try checkRuntime()
            try readAccount()
            if accountEmail != nil { try requireAccount() }
            status = accountEmail == nil ? "请登录 Keeps 专属 Codex 账户" : "已登录 \(accountEmail!)"
        } catch { fail(error) }
    }

    func login() async {
        await perform("等待浏览器登录", includeProcessLog: false) {
            try self.prepareDirectories()
            try self.checkRuntime()
            self.connectionVerified = false
            self.loginURL = nil
            defer { self.loginURL = nil }
            let result = try await self.runner.run(executable: self.executable,
                arguments: ["login", "-c", "cli_auth_credentials_store=\"file\"", "-c", "forced_login_method=\"chatgpt\""],
                home: self.home, directory: self.root, timeout: 300) { text in
                    if let url = Self.authorizationURL(in: text) { self.loginURL = url }
                }
            // Login output contains OAuth state and is never written to disk.
            guard result.exitCode == 0 else { throw AIEditingFailure("登录未完成（退出码 \(result.exitCode)），请重试浏览器授权。") }
            try self.readAccount()
            try self.requireAccount()
            self.status = "已登录 \(self.accountEmail!)；请运行连接测试"
        }
    }

    func logout() async {
        await perform("正在退出 Keeps 专属账户", includeProcessLog: false) {
            self.connectionVerified = false
            try self.prepareDirectories()
            guard FileManager.default.isExecutableFile(atPath: self.executable.path) else {
                throw AIEditingFailure("缺少内置 Codex，无法执行退出登录。")
            }
            let result = try await self.runner.run(executable: self.executable,
                arguments: ["logout", "-c", "cli_auth_credentials_store=\"file\""],
                home: self.home, directory: self.root, timeout: 15)
            guard result.exitCode == 0 else { throw AIEditingFailure("退出登录失败（退出码 \(result.exitCode)）。") }
            self.accountEmail = nil
            self.status = "已退出 Keeps 专属账户"
        }
    }

    func testConnection() async {
        await perform("正在验证账户与模型连接") {
            self.connectionVerified = false
            try self.prepareDirectories(); try self.checkRuntime(); try self.readAccount(); try self.requireAccount()
            let job = try self.newJob("connection")
            let result = try await self.runner.run(executable: self.executable,
                arguments: self.execArguments(job: job) + ["-"], home: self.home, directory: job,
                input: "Reply with exactly KEEPS_CONNECTION_OK. Do not use any tools.", timeout: 90)
            try self.writeLog(result, job: job)
            guard result.exitCode == 0 else { throw AIEditingFailure.process(result, job: job) }
            let reply = try String(contentsOf: job.appendingPathComponent("result.json"), encoding: .utf8)
            guard reply.trimmingCharacters(in: .whitespacesAndNewlines) == "KEEPS_CONNECTION_OK" else {
                throw AIEditingFailure("模型未返回预期连接确认。诊断日志：\(job.path)")
            }
            try self.readAccount(); try self.requireAccount()
            self.connectionVerified = true
            self.status = "连接验证通过：\(self.accountEmail!) · \(self.model)"
        }
    }

    func testFixedPhoto() async {
        await perform("正在使用内置公开样片验证 AI 调色") {
            try await self.runPhoto(Self.bundledSample(in: self.runtime))
        }
    }

    static func bundledSample(in runtime: URL) throws -> URL {
        let source = runtime.appendingPathComponent("sample.jpg")
        guard FileManager.default.isReadableFile(atPath: source.path) else {
            throw AIEditingFailure("安装包缺少内置测试图，请重新安装 Keeps。")
        }
        return source
    }

    func testPhoto(_ source: URL) async {
        await perform("正在执行单张 AI 调色测试") { try await self.runPhoto(source) }
    }

    func validateEditingAccount() throws {
        try prepareDirectories(); try checkRuntime(); try readAccount(); try requireAccount()
    }

    func gradePhoto(_ source: URL, job: URL, progress: ((String, Double, Double) -> Void)? = nil) async throws -> AIGradeResult {
        guard !isBusy else { throw AIEditingFailure("已有 AI 操作正在执行。") }
        isBusy = true
        defer { isBusy = false }
        try validateEditingAccount()
        let result = job.appendingPathComponent("result.json")
        if FileManager.default.fileExists(atPath: result.path) {
            do { return try Self.gradeResult(in: job) }
            catch {
                // Preserve rejected model output for diagnosis, then allow an explicit retry.
                let rejected = job.appendingPathComponent("rejected-result-" + UUID().uuidString + ".json")
                try FileManager.default.moveItem(at: result, to: rejected)
            }
        }
        try await runPhoto(source, job: job, progress: progress)
        return try Self.gradeResult(in: job)
    }

    func stopForExit() { runner.stopForExit() }

    private func runPhoto(_ source: URL, job requestedJob: URL? = nil, progress: ((String, Double, Double) -> Void)? = nil) async throws {
        self.resultPreview = nil; self.resultSummary = nil
        try self.prepareDirectories(); try self.checkRuntime(); try self.readAccount(); try self.requireAccount()
        guard requestedJob != nil || self.connectionVerified else { throw AIEditingFailure("请先完成模型连接测试。") }
        let allowed = ["arw", "3fr", "dng", "cr2", "cr3", "nef", "raf", "orf", "rw2", "jpg", "jpeg", "heic", "heif"]
        guard allowed.contains(source.pathExtension.lowercased()) else { throw AIEditingFailure("当前支持 RAW、JPEG 和 HEIF 照片。") }
        let accessed = source.startAccessingSecurityScopedResource()
        defer { if accessed { source.stopAccessingSecurityScopedResource() } }
        guard FileManager.default.isReadableFile(atPath: source.path) else { throw AIEditingFailure("无法读取选中的照片。") }
        let job = try requestedJob ?? self.newJob("photo")
        try FileManager.default.createDirectory(at: job, withIntermediateDirectories: true)
        let skill = try Data(contentsOf: self.runtime.appendingPathComponent("SKILL.md"))
        let schema: [String: Any] = ["type": "object", "properties": [
            "status": ["type": "string", "enum": ["selected", "unchanged", "needs_review"]],
            "candidateID": ["type": "string"], "reason": ["type": "string"]],
            "required": ["status", "candidateID", "reason"], "additionalProperties": false]
        let schemaURL = job.appendingPathComponent("result.schema.json")
        try JSONSerialization.data(withJSONObject: schema).write(to: schemaURL, options: .atomic)
        var arguments = self.execArguments(job: job) + ["--output-schema", schemaURL.path]
        let settings: [String: Any] = [
            "mcp_servers.keeps_color.required": true,
            "mcp_servers.keeps_color.default_tools_approval_mode": "auto",
            "mcp_servers.keeps_color.command": self.codeRuntime.appendingPathComponent("keeps-color-mcp").path,
            "mcp_servers.keeps_color.args": ["--darktable", self.codeRuntime.appendingPathComponent("darktable.app/Contents/MacOS/darktable-cli").path,
                "--source", source.path, "--job", job.appendingPathComponent("render").path],
            "mcp_servers.keeps_color.startup_timeout_sec": 180,
            "mcp_servers.keeps_color.tool_timeout_sec": 240]
        for key in settings.keys.sorted() { arguments += ["-c", key + "=" + (try Self.json(settings[key]!))] }
        let version = try await self.runner.run(executable: self.executable, arguments: ["--version"], home: self.home, directory: job, timeout: 15)
        guard version.exitCode == 0 else { throw AIEditingFailure("无法检查内置 Codex 版本。") }
        let started = Date()
        var runExitCode: Int32 = -1
        defer {
            let metadata: [String: Any] = ["model": self.model, "codexVersion": version.output.trimmingCharacters(in: .whitespacesAndNewlines),
                "engineVersion": "5.6.2", "elapsedSeconds": Date().timeIntervalSince(started), "exitCode": runExitCode,
                "skillSHA256": SHA256.hash(data: skill).map { String(format: "%02x", $0) }.joined()]
            if let data = try? JSONSerialization.data(withJSONObject: metadata, options: [.prettyPrinted, .sortedKeys]) {
                try? data.write(to: job.appendingPathComponent("run.json"), options: .atomic)
            }
        }
        let prompt = String(decoding: skill, as: UTF8.self) + "\nComplete this photo independently using only Keeps tools. Return the specified result, with reason in Chinese."
        var steps = AIEditingSteps()
        let result = try await self.runner.run(executable: self.executable, arguments: arguments + ["-"],
            home: self.home, directory: job, input: prompt, timeout: 600, onEvent: { line in
                if let step = steps.consume(line) { progress?(step.title, step.floor, step.ceiling) }
            })
        try self.writeLog(result, job: job)
        runExitCode = result.exitCode
        guard result.exitCode == 0 else { throw AIEditingFailure.process(result, job: job) }
        try self.readAccount(); try self.requireAccount()
        let validated = try Self.validateResult(in: job)
        self.resultPreview = validated.preview
        self.resultSummary = validated.reason
        self.status = validated.selected ? "单张调色测试成功；结果保存在本机" : "测试完成；结果需要人工检查"
    }

    func cancel() { runner.cancel() }

    private func perform(_ message: String, includeProcessLog: Bool = true, operation: () async throws -> Void) async {
        guard !isBusy else { return }
        isBusy = true; status = message; errorMessage = nil
        defer { isBusy = false }
        do { try await operation() } catch {
            let trace = String(reflecting: error) + (includeProcessLog ? "\n" + runner.lastErrors : "")
            if let data = Self.redacted(trace).data(using: .utf8) {
                try? data.write(to: logDirectory.appendingPathComponent("failure-\(UUID().uuidString).log"), options: .atomic)
            }
            fail(error)
        }
    }
    private func fail(_ error: Error) { errorMessage = AIEditingFailure.userMessage(error); status = "需要处理" }
    private func prepareDirectories() throws {
        for directory in [root, home, logDirectory] {
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        }
        try Data("cli_auth_credentials_store = \"file\"\nforced_login_method = \"chatgpt\"\n".utf8)
            .write(to: home.appendingPathComponent("config.toml"), options: .atomic)
    }
    private func checkRuntime() throws {
        runtimeReady = false
        for relative in ["codex", "codex-code-mode-host", "keeps-color-mcp", "darktable.app/Contents/MacOS/darktable-cli"] {
            guard FileManager.default.isExecutableFile(atPath: codeRuntime.appendingPathComponent(relative).path) else {
                throw AIEditingFailure("当前应用缺少内置 AI 修图组件：\(relative)。请安装包含 AI 修图运行时的 Keeps。")
            }
        }
        guard FileManager.default.isReadableFile(atPath: runtime.appendingPathComponent("SKILL.md").path) else {
            throw AIEditingFailure("当前应用缺少内置 AI 调色 Skill。")
        }
        runtimeReady = true
    }
    private func readAccount() throws {
        let auth = home.appendingPathComponent("auth.json")
        guard FileManager.default.fileExists(atPath: auth.path) else { accountEmail = nil; connectionVerified = false; return }
        let email = try Self.emailFromAuth(Data(contentsOf: auth))
        if accountEmail != email { connectionVerified = false }
        accountEmail = email
    }
    private func requireAccount() throws {
        guard let accountEmail else { throw AIEditingFailure("请先登录 Keeps 专属 Codex 账户。") }
        try Self.validateAccount(actual: accountEmail, expected: expectedEmail)
    }
    private func newJob(_ kind: String) throws -> URL {
        let job = logDirectory.appendingPathComponent("\(kind)-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: job, withIntermediateDirectories: false, attributes: [.posixPermissions: 0o700])
        return job
    }
    private func execArguments(job: URL) -> [String] {
        var arguments = ["exec", "--ignore-user-config", "--ignore-rules", "--skip-git-repo-check", "--json", "-m", model,
            "-s", "read-only", "-C", job.path, "-o", job.appendingPathComponent("result.json").path]
        for feature in ["shell_tool", "unified_exec", "apps", "hooks", "plugins", "remote_plugin", "skill_search"] {
            arguments += ["--disable", feature]
        }
        arguments += ["--enable", "skip_host_skill_discovery"]
        for setting in ["cli_auth_credentials_store=\"file\"", "forced_login_method=\"chatgpt\"", "model_reasoning_effort=\"high\"", "web_search=\"disabled\"", "project_doc_max_bytes=0"] {
            arguments += ["-c", setting]
        }
        return arguments
    }
    private func writeLog(_ result: AIEditingProcess.Result, job: URL) throws {
        try Data(Self.redacted(result.errors).utf8).write(to: job.appendingPathComponent("stderr.log"), options: .atomic)
        // Keep protocol events for diagnosis, without embedded image data or credentials.
        let lines = result.output.split(separator: "\n").map { Self.redacted(String($0)) }
        try Data(lines.joined(separator: "\n").utf8).write(to: job.appendingPathComponent("events.jsonl"), options: .atomic)
    }
    // Codex parses overrides as TOML, which does not accept JSON escaped slashes.
    static func json(_ value: Any) throws -> String {
        String(decoding: try JSONSerialization.data(withJSONObject: value, options: [.fragmentsAllowed, .sortedKeys, .withoutEscapingSlashes]), as: UTF8.self)
    }

    static func validateAccount(actual: String, expected: String) throws {
        let target = expected.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !target.isEmpty, target.contains("@"), actual.caseInsensitiveCompare(target) == .orderedSame else {
            throw AIEditingFailure("当前登录为 \(actual)，与指定账户 \(target) 不一致。请退出并使用指定账户登录。")
        }
    }
    static func emailFromAuth(_ data: Data) throws -> String {
        guard let auth = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              (auth["OPENAI_API_KEY"] as? String ?? "").isEmpty,
              let tokens = auth["tokens"] as? [String: Any], let token = tokens["id_token"] as? String else {
            throw AIEditingFailure("专属账户凭证无效，请重新登录。")
        }
        let pieces = token.split(separator: ".")
        guard pieces.count == 3 else { throw AIEditingFailure("账户身份信息无效，请重新登录。") }
        var encoded = String(pieces[1]).replacingOccurrences(of: "-", with: "+").replacingOccurrences(of: "_", with: "/")
        encoded += String(repeating: "=", count: (4 - encoded.count % 4) % 4)
        guard let payload = Data(base64Encoded: encoded), let claims = try JSONSerialization.jsonObject(with: payload) as? [String: Any],
              let email = claims["email"] as? String, email.contains("@") else {
            throw AIEditingFailure("无法识别登录账户邮箱，请重新登录。")
        }
        return email
    }
    static func authorizationURL(in text: String) -> URL? {
        let pattern = #"https://auth\.openai\.com/[^\s<>\"]+"#
        guard let range = text.range(of: pattern, options: .regularExpression), let url = URL(string: String(text[range])) else { return nil }
        return url
    }
    static func redacted(_ text: String) -> String {
        var result = text.replacingOccurrences(of: #"https://auth\.openai\.com/[^\s\"]+"#, with: "[authorization URL omitted]", options: .regularExpression)
        result = result.replacingOccurrences(of: #"eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+"#, with: "[token omitted]", options: .regularExpression)
        result = result.replacingOccurrences(of: #"\"data\"\s*:\s*\"[A-Za-z0-9+/=]{256,}\""#, with: "\"data\":\"[image omitted]\"", options: .regularExpression)
        return result
    }
    static func gradeResult(in job: URL) throws -> AIGradeResult {
        let checked = try validateResult(in: job)
        let result = try JSONSerialization.jsonObject(with: Data(contentsOf: job.appendingPathComponent("result.json"))) as! [String: Any]
        guard checked.selected else {
            return AIGradeResult(status: result["status"] as! String, reason: checked.reason)
        }
        let selection = try JSONSerialization.jsonObject(with: Data(contentsOf: job.appendingPathComponent("render/selection.json"))) as! [String: Any]
        let state = try JSONSerialization.jsonObject(with: Data(contentsOf: job.appendingPathComponent("render/candidates.json"))) as! [String: Any]
        guard let full = selection["fullSize"] as? String,
              let id = result["candidateID"] as? String,
              let candidates = state["candidates"] as? [[String: Any]],
              let encoded = candidates.first(where: { $0["id"] as? String == id })?["recipe"] as? String,
              let recipe = Data(base64Encoded: encoded),
              (try JSONSerialization.jsonObject(with: recipe)) is [String: Any] else {
            throw AIEditingFailure("调色配方缺失，无法无损保存。")
        }
        let fullURL = URL(fileURLWithPath: full)
        let xmpURL = fullURL.deletingPathExtension().appendingPathExtension("xmp")
        return AIGradeResult(status: "selected", reason: checked.reason, fullSize: fullURL,
            recipeJSON: String(decoding: recipe, as: UTF8.self), xmp: try String(contentsOf: xmpURL, encoding: .utf8))
    }

    struct ValidatedResult { let selected: Bool; let preview: URL?; let reason: String }
    static func validateResult(in job: URL) throws -> ValidatedResult {
        func object(_ path: String) throws -> [String: Any] {
            guard let value = try JSONSerialization.jsonObject(with: Data(contentsOf: job.appendingPathComponent(path))) as? [String: Any] else {
                throw AIEditingFailure("调色结果格式无效。")
            }
            return value
        }
        let result = try object("result.json"), store = try object("render/candidates.json")
        guard let status = result["status"] as? String, ["selected", "unchanged", "needs_review"].contains(status),
              let id = result["candidateID"] as? String, let reason = result["reason"] as? String,
              let candidates = store["candidates"] as? [[String: Any]], candidates.contains(where: { $0["id"] as? String == id }) else {
            throw AIEditingFailure("AI 返回了未知候选或无效状态，结果未被接受。")
        }
        guard status == "selected" else { return ValidatedResult(selected: false, preview: nil, reason: reason) }
        let selection = try object("render/selection.json")
        guard selection["candidateID"] as? String == id, let preview = selection["preview"] as? String, let full = selection["fullSize"] as? String else {
            throw AIEditingFailure("AI 选择与已渲染结果不一致。")
        }
        let directory = job.appendingPathComponent("render").resolvingSymlinksInPath().path + "/"
        for path in [preview, full] {
            let url = URL(fileURLWithPath: path).resolvingSymlinksInPath()
            guard url.path.hasPrefix(directory), FileManager.default.isReadableFile(atPath: url.path) else {
                throw AIEditingFailure("候选图像缺失或位于任务目录之外。")
            }
        }
        return ValidatedResult(selected: true, preview: URL(fileURLWithPath: preview), reason: reason)
    }
}

struct AIGradeResult: Codable, Sendable {
    var status: String
    var reason: String
    var fullSize: URL? = nil
    var recipeJSON: String? = nil
    var xmp: String? = nil
}


struct AIEditingSteps {
    private var adjustments = 0
    mutating func consume(_ line: String) -> (title: String, floor: Double, ceiling: Double)? {
        guard let data = line.data(using: .utf8),
              let event = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let kind = event["type"] as? String else { return nil }
        if kind == "turn.started" { return ("AI 正在观察照片", 0.17, 0.28) }
        guard kind == "item.started", let item = event["item"] as? [String: Any],
              item["type"] as? String == "mcp_tool_call", item["server"] as? String == "keeps_color",
              let tool = item["tool"] as? String else { return nil }
        switch tool {
        case "inspect_photo": return ("生成初始预览，分析曝光与色彩", 0.20, 0.32)
        case "set_adjustments":
            adjustments += 1
            let floor = 0.34 + Double(min(adjustments - 1, 3)) * 0.09
            return ("第 \(adjustments) 轮：调整曝光与色彩", floor, min(0.77, floor + 0.09))
        case "render_preview": return ("第 \(max(1, adjustments)) 轮：渲染调色预览", 0.42, 0.74)
        case "compare_candidates", "preview_region": return ("AI 正在比较效果、检查细节", 0.51, 0.78)
        case "select_candidate": return ("已选定效果，生成全尺寸结果", 0.78, 0.85)
        default: return nil
        }
    }
}
