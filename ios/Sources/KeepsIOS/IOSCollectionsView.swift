import SwiftUI
import KeepsAPI

struct IOSCollectionRoute: Hashable {
    let directory: String
    let title: String
    let hasChildren: Bool
}

struct IOSCollectionsView: View {
    let configuration: KeepsConfiguration?
    @StateObject private var navigation = IOSDirectoryStore()

    var body: some View {
        List {
            Section {
                IOSDirectoryRows(navigation: navigation, connected: configuration != nil, path: nil) {
                    Task { await navigation.load(configuration: configuration, path: nil) }
                }
            } header: {
                Text("服务器").font(.title2.bold()).foregroundStyle(.primary).textCase(nil)
            }
            Section {
                Label("暂未开放", systemImage: "iphone")
                    .foregroundStyle(.secondary).frame(minHeight: 44)
            } header: {
                Text("本地").font(.title2.bold()).foregroundStyle(.primary).textCase(nil)
            }
        }
        .listStyle(.insetGrouped)
        .scrollContentBackground(.hidden)
        .background(.black)
        .contentMargins(.bottom, 100, for: .scrollContent)
        .task(id: configuration) { await navigation.load(configuration: configuration, path: nil) }
        .refreshable { await navigation.load(configuration: configuration, path: nil) }
    }
}

struct IOSDirectoryBrowser: View {
    let configuration: KeepsConfiguration?
    let path: String
    @StateObject private var navigation = IOSDirectoryStore()

    var body: some View {
        List {
            IOSDirectoryRows(navigation: navigation, connected: configuration != nil, path: path) {
                Task { await navigation.load(configuration: configuration, path: path) }
            }.listRowBackground(Color.clear)
        }
            .listStyle(.plain).scrollContentBackground(.hidden)
            .frame(height: 220)
            .background(.ultraThinMaterial)
            .clipped()
            .task(id: configuration) { await navigation.load(configuration: configuration, path: path) }
    }
}

private struct IOSDirectoryRows: View {
    @ObservedObject var navigation: IOSDirectoryStore
    let connected: Bool
    let path: String?
    let retry: () -> Void

    var body: some View {
        Group {
            if !connected {
                Label("连接服务器后显示目录", systemImage: "server.rack")
                    .foregroundStyle(.secondary).frame(minHeight: 60)
            } else if navigation.loading {
                ProgressView("正在读取目录…").frame(minHeight: 60)
            } else if let error = navigation.error {
                VStack(alignment: .leading, spacing: 8) {
                    Label("无法读取目录", systemImage: "wifi.exclamationmark")
                    Text(error).font(.caption).foregroundStyle(.secondary).textSelection(.enabled)
                    Button("重试目录", action: retry)
                }.padding(.vertical, 8)
            } else if navigation.directories.isEmpty {
                Text(path == nil ? "暂无服务器目录" : "没有子目录")
                    .foregroundStyle(.secondary).frame(minHeight: 44)
            } else {
                ForEach(navigation.directories) { directory in
                    NavigationLink(value: IOSCollectionRoute(directory: directory.path, title: directory.name, hasChildren: directory.hasChildren)) {
                        HStack(spacing: 12) {
                            Image(systemName: "folder").foregroundStyle(.secondary)
                            Text(directory.name).lineLimit(2)
                            Spacer(minLength: 8)
                            Text(directory.photoCount.formatted()).foregroundStyle(.secondary).monospacedDigit()
                        }.frame(minHeight: 44)
                    }
                }
            }
        }
    }
}

@MainActor
private final class IOSDirectoryStore: ObservableObject {
    @Published private(set) var directories: [KeepsNavigationDirectory] = []
    @Published private(set) var loading = true
    @Published private(set) var error: String?
    private var generation = 0

    func load(configuration: KeepsConfiguration?, path: String?) async {
        generation += 1
        let requestGeneration = generation
        loading = true
        error = nil
        directories = []
        guard let configuration else { loading = false; return }
        do {
            let result = try await KeepsClient(configuration: configuration).navigation(path: path)
            try Task.checkCancellation()
            guard generation == requestGeneration else { return }
            directories = result.directories
        } catch {
            guard generation == requestGeneration, !Task.isCancelled else { return }
            self.error = String(reflecting: error)
        }
        loading = false
    }
}
