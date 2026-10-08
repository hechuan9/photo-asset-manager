import AppKit
import SwiftUI

struct AIEditingSettingsView: View {
    @StateObject private var store = AIEditingSettingsStore()

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 20) {
                VStack(alignment: .leading, spacing: 8) {
                    Label("AI 修图", systemImage: "slider.horizontal.3").font(.title2)
                    Text("AI 在线分析照片，调色由这台 Mac 执行。Keeps 使用独立登录，不使用本机其他 Codex 会话的账户。")
                        .font(.callout).foregroundStyle(.secondary)
                }
                runtimeSection
                accountSection
                verificationSection
                progressSection
                resultSection
            }
            .padding(24)
        }
        .frame(width: 620, height: 660)
        .task { await store.prepare() }
    }

    private var runtimeSection: some View {
        GroupBox {
            HStack {
                Label(store.runtimeReady ? "运行环境已就绪" : "准备修图运行环境", systemImage: store.runtimeReady ? "checkmark.circle" : "shippingbox")
                Spacer()
                Button("检查运行环境") { Task { await store.prepare() } }
                    .disabled(store.isBusy)
            }.padding(8)
        } label: { Text("1. 运行环境") }
    }

    private var accountSection: some View {
        GroupBox {
            VStack(alignment: .leading, spacing: 12) {
                TextField("修图账户邮箱", text: $store.expectedEmail)
                    .textFieldStyle(.roundedBorder)
                    .disabled(store.isBusy)
                    .accessibilityIdentifier("ai-editing-email")
                Text("请在浏览器中使用此邮箱登录。账户核对通过后才能测试修图。")
                    .font(.caption).foregroundStyle(.secondary)
                if let email = store.accountEmail {
                    LabeledContent("当前登录", value: email).textSelection(.enabled)
                } else {
                    Text("尚未登录 Keeps 修图账户").foregroundStyle(.secondary)
                }
                HStack {
                    Button("登录修图账户") { Task { await store.login() } }
                        .disabled(store.isBusy || !store.runtimeReady || store.expectedEmail.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                        .accessibilityIdentifier("ai-editing-login")
                    Button("刷新状态") { Task { await store.refresh() } }.disabled(store.isBusy)
                    Spacer()
                    Button("退出登录") { Task { await store.logout() } }
                        .disabled(store.isBusy || store.accountEmail == nil)
                }
                if let url = store.loginURL {
                    Button("在浏览器中继续登录") { NSWorkspace.shared.open(url) }
                        .accessibilityIdentifier("ai-editing-open-login")
                }
            }.padding(8)
        } label: { Text("2. 独立账户") }
    }

    private var verificationSection: some View {
        GroupBox {
            VStack(alignment: .leading, spacing: 12) {
                Text("先确认 AI 连接可用，再使用固定样片完成试调色。原片保持不变，预览会发送给 AI 服务进行分析。")
                    .font(.callout).foregroundStyle(.secondary)
                HStack {
                    Button("测试 AI 连接") { Task { await store.testConnection() } }
                        .disabled(store.isBusy || !store.runtimeReady || store.accountEmail == nil)
                        .accessibilityIdentifier("ai-editing-test-connection")
                    if store.connectionVerified {
                        Label("连接已验证", systemImage: "checkmark.circle").foregroundStyle(.green)
                    }
                    Spacer()
                }
                Text("内置公开风景样片（CC0），无需连接照片库。").font(.caption).foregroundStyle(.secondary)
                Button("使用内置样片验证") { Task { await store.testFixedPhoto() } }
                    .disabled(store.isBusy || !store.connectionVerified)
                    .accessibilityIdentifier("ai-editing-test-photo")
            }.padding(8)
        } label: { Text("3. 验证与试修图") }
    }

    private var progressSection: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack(alignment: .top) {
                if store.isBusy { ProgressView().controlSize(.small) }
                Text(store.status).font(.callout).textSelection(.enabled)
                Spacer()
                if store.isBusy { Button("取消", action: store.cancel) }
            }
            if let error = store.errorMessage {
                Text(error).font(.callout).foregroundStyle(.red).textSelection(.enabled)
                    .accessibilityIdentifier("ai-editing-error")
            }
            Button("打开运行日志") { NSWorkspace.shared.open(store.logDirectory) }
                .font(.callout)
        }
    }

    @ViewBuilder private var resultSection: some View {
        if store.resultPreview != nil || store.resultSummary != nil {
            VStack(alignment: .leading, spacing: 10) {
                Text("试修图结果").font(.headline)
                if let preview = store.resultPreview, let image = NSImage(contentsOf: preview) {
                    Image(nsImage: image).resizable().scaledToFit().frame(maxHeight: 320)
                        .accessibilityLabel("AI 试修图预览")
                }
                if let summary = store.resultSummary { Text(summary).font(.callout).textSelection(.enabled) }
                if let preview = store.resultPreview {
                    Button("查看结果文件") { NSWorkspace.shared.activateFileViewerSelecting([preview]) }
                }
                Text("此结果仅保存在本机，不会发布到资料库。")
                    .font(.caption).foregroundStyle(.secondary)
            }
        }
    }

}
