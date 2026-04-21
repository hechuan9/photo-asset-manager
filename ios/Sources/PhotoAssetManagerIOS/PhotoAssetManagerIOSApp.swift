import SwiftUI

@main
struct PhotoAssetManagerIOSApp: App {
    @StateObject private var library = IOSLibraryStore()

    var body: some Scene {
        WindowGroup {
            IOSRootView()
                .environmentObject(library)
        }
    }
}

struct IOSRootView: View {
    @EnvironmentObject private var library: IOSLibraryStore
    @Environment(\.scenePhase) private var scenePhase
    @State private var showingSettings = false

    var body: some View {
        NavigationStack {
            ZStack {
                LinearGradient(
                    colors: [
                        Color(red: 0.02, green: 0.02, blue: 0.03),
                        Color(red: 0.07, green: 0.08, blue: 0.10)
                    ],
                    startPoint: .topLeading,
                    endPoint: .bottomTrailing
                )
                .ignoresSafeArea()

                VStack(spacing: 0) {
                    IOSSyncStatusBar(
                        count: library.assets.count,
                        summary: library.lastSyncSummary,
                        isConfigured: library.hasRemoteSyncConfiguration,
                        isSyncing: library.isSyncing,
                        lastError: library.lastError
                    )

                    if library.assets.isEmpty {
                        IOSGalleryEmptyState(
                            isConfigured: library.hasRemoteSyncConfiguration,
                            openSettings: {
                                showingSettings = true
                            },
                            syncNow: {
                                Task {
                                    await library.syncNow(statusPrefix: "立即刷新")
                                }
                            }
                        )
                    } else {
                        IOSWaterfallGallery()
                            .environmentObject(library)
                    }
                }
            }
            .navigationTitle("图库")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    Text("\(library.assets.count) 张")
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(.secondary)
                }

                ToolbarItemGroup(placement: .topBarTrailing) {
                    Button {
                        if library.hasRemoteSyncConfiguration {
                            Task {
                                await library.syncNow(statusPrefix: "立即刷新")
                            }
                        } else {
                            showingSettings = true
                        }
                    } label: {
                        if library.isSyncing {
                            ProgressView()
                                .progressViewStyle(.circular)
                        } else {
                            Image(systemName: "arrow.clockwise")
                        }
                    }
                    .disabled(library.isSyncing)

                    Button {
                        showingSettings = true
                    } label: {
                        Image(systemName: "slider.horizontal.3")
                    }
                }
            }
            .toolbarColorScheme(.dark, for: .navigationBar)
            .sheet(isPresented: $showingSettings, onDismiss: {
                library.reloadConfiguration()
            }) {
                IOSSyncSettingsView {
                    library.reloadConfiguration()
                }
            }
            .task {
                library.loadIfNeeded()
            }
            .onChange(of: scenePhase, initial: true) { _, newPhase in
                library.loadIfNeeded()
                library.setAutomaticSyncActive(newPhase == .active)
            }
        }
    }
}

struct IOSSyncStatusBar: View {
    var count: Int
    var summary: String
    var isConfigured: Bool
    var isSyncing: Bool
    var lastError: String?

    var body: some View {
        VStack(spacing: 10) {
            HStack(alignment: .firstTextBaseline, spacing: 12) {
                VStack(alignment: .leading, spacing: 4) {
                    Text(isConfigured ? "iPhone 本地投影" : "本地缓存模式")
                        .font(.system(size: 13, weight: .semibold))
                        .foregroundStyle(.secondary)
                    Text(summary)
                        .font(.system(size: 22, weight: .semibold))
                        .foregroundStyle(.white)
                }

                Spacer()

                VStack(alignment: .trailing, spacing: 4) {
                    Text(isSyncing ? "同步中" : (isConfigured ? "自动拉取 macOS ledger" : "先配置 control plane"))
                        .font(.system(size: 13, weight: .medium))
                        .foregroundStyle(isConfigured ? Color(red: 0.79, green: 0.90, blue: 1.0) : Color(red: 1.0, green: 0.84, blue: 0.60))
                    Text("\(count)")
                        .font(.system(size: 32, weight: .bold, design: .rounded))
                        .foregroundStyle(.white)
                }
            }

            if let lastError, !lastError.isEmpty {
                Text(lastError)
                    .font(.footnote)
                    .foregroundStyle(Color(red: 1.0, green: 0.74, blue: 0.74))
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .lineLimit(3)
            }
        }
        .padding(.horizontal, 18)
        .padding(.top, 14)
        .padding(.bottom, 16)
        .background(
            RoundedRectangle(cornerRadius: 22, style: .continuous)
                .fill(Color.white.opacity(0.08))
                .overlay(
                    RoundedRectangle(cornerRadius: 22, style: .continuous)
                        .stroke(Color.white.opacity(0.08), lineWidth: 1)
                )
        )
        .padding(.horizontal, 12)
        .padding(.top, 10)
        .padding(.bottom, 8)
    }
}

struct IOSGalleryEmptyState: View {
    var isConfigured: Bool
    var openSettings: () -> Void
    var syncNow: () -> Void

    var body: some View {
        VStack(spacing: 18) {
            Spacer()

            Image(systemName: "square.grid.2x2.fill")
                .font(.system(size: 48))
                .foregroundStyle(.white.opacity(0.9))

            Text("还没有同步到图库")
                .font(.system(size: 28, weight: .semibold))
                .foregroundStyle(.white)

            Text(isConfigured
                 ? "应用会在前台自动拉取 macOS 已上传的 ledger，并优先显示缩略图。"
                 : "先填 control plane 地址，随后 iPhone 会自动拉取这个只读图库。")
                .font(.system(size: 15, weight: .medium))
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)
                .frame(maxWidth: 320)

            HStack(spacing: 12) {
                Button("立即刷新") {
                    syncNow()
                }
                .buttonStyle(.borderedProminent)

                Button("设置") {
                    openSettings()
                }
                .buttonStyle(.bordered)
            }

            Spacer()
        }
        .padding(.horizontal, 24)
    }
}

struct IOSSyncSettingsView: View {
    @Environment(\.dismiss) private var dismiss
    @AppStorage(SyncPreferenceKey.baseURL) private var baseURL = ""
    @AppStorage(SyncPreferenceKey.libraryID) private var libraryID = "local-library"
    @AppStorage(SyncPreferenceKey.peerID) private var peerID = "control-plane"
    @AppStorage(SyncPreferenceKey.authMode) private var authModeRawValue = SyncAuthenticationMode.bearer.rawValue
    @AppStorage(SyncPreferenceKey.accessCredential) private var accessCredential = ""
    @AppStorage(SyncPreferenceKey.awsRegion) private var awsRegion = "us-east-1"
    @AppStorage(SyncPreferenceKey.awsAccessKeyID) private var awsAccessKeyID = ""
    @AppStorage(SyncPreferenceKey.awsSecretAccessKey) private var awsSecretAccessKey = ""
    @AppStorage(SyncPreferenceKey.awsSessionToken) private var awsSessionToken = ""

    var didSave: () -> Void

    private var authMode: SyncAuthenticationMode {
        get { SyncAuthenticationMode(rawValue: authModeRawValue) ?? .bearer }
        nonmutating set { authModeRawValue = newValue.rawValue }
    }

    var body: some View {
        NavigationStack {
            Form {
                Section("Control Plane") {
                    TextField("https://control-plane.example.com", text: $baseURL)
                        .textInputAutocapitalization(.never)
                        .keyboardType(.URL)
                        .autocorrectionDisabled()
                    TextField("libraryID", text: $libraryID)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()
                    TextField("peerID", text: $peerID)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()
                    Picker("认证方式", selection: Binding(
                        get: { authMode },
                        set: { authMode = $0 }
                    )) {
                        ForEach(SyncAuthenticationMode.allCases) { mode in
                            Text(mode.displayName).tag(mode)
                        }
                    }
                    if authMode == .bearer {
                        SecureField("Bearer token（可留空）", text: $accessCredential)
                            .textInputAutocapitalization(.never)
                            .autocorrectionDisabled()
                    } else {
                        TextField("AWS region", text: $awsRegion)
                            .textInputAutocapitalization(.never)
                            .autocorrectionDisabled()
                        TextField("AWS access key ID", text: $awsAccessKeyID)
                            .textInputAutocapitalization(.never)
                            .autocorrectionDisabled()
                        SecureField("AWS secret access key", text: $awsSecretAccessKey)
                            .textInputAutocapitalization(.never)
                            .autocorrectionDisabled()
                        SecureField("AWS session token（可留空）", text: $awsSessionToken)
                            .textInputAutocapitalization(.never)
                            .autocorrectionDisabled()
                    }
                }

                Section("说明") {
                    Text("当前 iOS 端只负责同步验证和瀑布流浏览，不会扫描、移动、删除或覆盖任何原片。")
                    Text("前台会自动拉取 ledger。缩略图优先读本地缓存，没有本地缓存时，再通过 derivative metadata 取远端下载链接。")
                    Text("若 control plane 使用 API Gateway AWS_IAM，请切到 AWS IAM 并填写 region 与临时或长期凭证。")
                }
            }
            .navigationTitle("同步设置")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("关闭") {
                        dismiss()
                    }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("保存") {
                        didSave()
                        dismiss()
                    }
                }
            }
        }
    }
}
