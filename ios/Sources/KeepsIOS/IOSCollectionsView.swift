import SwiftUI
import KeepsAPI

struct IOSCollectionRoute: Hashable {
    let directory: String
    let title: String
    let hasChildren: Bool
}

struct IOSCollectionsView: View {
    let configuration: KeepsConfiguration?
    @ObservedObject var navigation: IOSDirectoryStore

    var body: some View {
        List {
            Section {
                IOSDirectoryRows(state: navigation.state(for: nil, configuration: configuration), connected: configuration != nil, path: nil)
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
        .bottomPullRefresh(bottomInset: 90) { await navigation.load(configuration: configuration, path: nil, force: true) }
    }
}

struct IOSDirectoryBrowser: View {
    let configuration: KeepsConfiguration?
    let path: String
    @ObservedObject var navigation: IOSDirectoryStore

    @State private var contentHeight: CGFloat = 0

    var body: some View {
        GeometryReader { geometry in
            ScrollView {
                VStack(spacing: 0) {
                    IOSDirectoryRows(state: navigation.state(for: path, configuration: configuration),
                                     connected: configuration != nil, path: path, menu: true)
                    .padding(.horizontal, 16).padding(.vertical, 8)
                }
                .frame(maxWidth: .infinity)
                .onGeometryChange(for: CGFloat.self) { $0.size.height } action: { contentHeight = $0 }
            }
            .contentMargins(.bottom, contentHeight > geometry.size.height - 100 ? 100 : 0, for: .scrollContent)
            .bottomPullRefresh { await navigation.load(configuration: configuration, path: path, force: true) }
            .frame(height: min(contentHeight, geometry.size.height))
            .background(.ultraThinMaterial)
            .clipped()
        }
        .task(id: configuration) { await navigation.load(configuration: configuration, path: path) }
    }
}

private struct IOSDirectoryRows: View {
    let state: IOSDirectoryStore.Entry
    let connected: Bool
    let path: String?
    var menu = false

    var body: some View {
        Group {
            if let error = state.error, state.directories != nil {
                VStack(alignment: .leading, spacing: 4) {
                    Text("目录刷新失败，显示已缓存内容").font(.caption)
                    Text(error).font(.caption2).foregroundStyle(.secondary).lineLimit(2)
                    Text("从底部上拉刷新").font(.caption)
                }
            }
            if !connected {
                Label("连接服务器后显示目录", systemImage: "server.rack")
                    .foregroundStyle(.secondary).frame(minHeight: 60)
            } else if state.loading && state.directories == nil {
                ProgressView("正在读取目录…").frame(minHeight: 60)
            } else if let error = state.error, state.directories == nil {
                VStack(alignment: .leading, spacing: 8) {
                    Label("无法读取目录", systemImage: "wifi.exclamationmark")
                    Text(error).font(.caption).foregroundStyle(.secondary).textSelection(.enabled)
                    Text("从底部上拉刷新").font(.caption)
                }.padding(.vertical, 8)
            } else if state.directories?.isEmpty != false {
                Text(path == nil ? "暂无服务器目录" : "没有子目录")
                    .foregroundStyle(.secondary).frame(minHeight: 44)
            } else {
                ForEach(state.directories ?? []) { directory in
                    NavigationLink(value: IOSCollectionRoute(directory: directory.path, title: directory.name, hasChildren: directory.hasChildren)) {
                        HStack(spacing: 12) {
                            Image(systemName: "folder").foregroundStyle(.secondary)
                            Text(directory.name).lineLimit(2)
                            Spacer(minLength: 8)
                            Text(directory.photoCount.formatted()).foregroundStyle(.secondary).monospacedDigit()
                            if menu { Image(systemName: "chevron.right").font(.caption).foregroundStyle(.secondary) }
                        }.frame(minHeight: 44).contentShape(Rectangle())
                    }
                    .buttonStyle(.plain)
                }
            }
        }
    }
}

/// Only an upward finger drag beyond the bottom edge can request a refresh.
private struct BottomPullRefresh: ViewModifier {
    let action: () async -> Void
    var bottomInset: CGFloat = 0
    @State private var overscroll: CGFloat = 0
    @State private var translation: CGFloat = 0
    @State private var dragging = false
    @State private var armed = false
    @State private var refreshing = false

    func body(content: Content) -> some View {
        content
            .scrollBounceBehavior(.always)
            .onScrollGeometryChange(for: CGFloat.self) { geometry in
                let bottom = max(-geometry.contentInsets.top,
                    geometry.contentSize.height + geometry.contentInsets.bottom - geometry.containerSize.height)
                return max(0, geometry.contentOffset.y - bottom)
            } action: { _, distance in
                overscroll = distance
                if dragging && translation < -60 && distance > 60 { armed = true }
            }
            .simultaneousGesture(DragGesture().onChanged { value in
                dragging = true
                translation = value.translation.height
                if translation < -60 && overscroll > 60 { armed = true }
            }.onEnded { _ in
                let shouldRefresh = armed && !refreshing
                dragging = false; armed = false; translation = 0
                guard shouldRefresh else { return }
                refreshing = true
                Task {
                    await action()
                    refreshing = false
                }
            })
            .overlay(alignment: .bottom) {
                if refreshing { ProgressView().padding(12).background(.regularMaterial, in: Capsule()).padding(.bottom, bottomInset) }
                else if dragging && overscroll > 15 {
                    Text(armed ? "松开刷新" : "继续上拉刷新")
                        .font(.caption).padding(10).background(.regularMaterial, in: Capsule()).padding(.bottom, bottomInset)
                }
            }
    }
}

extension View {
    func bottomPullRefresh(bottomInset: CGFloat = 0, _ action: @escaping () async -> Void) -> some View {
        modifier(BottomPullRefresh(action: action, bottomInset: bottomInset))
    }
}
