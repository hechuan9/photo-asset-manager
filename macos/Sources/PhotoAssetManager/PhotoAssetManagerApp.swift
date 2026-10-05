import SwiftUI
import KeepsAPI

@main
struct PhotoAssetManagerApp: App {
    @Environment(\.scenePhase) private var scenePhase
    @StateObject private var library = LibraryStore()
    var body: some Scene {
        WindowGroup {
            ContentView().environmentObject(library).frame(minWidth: 1080, minHeight: 720)
        }
        .onChange(of: scenePhase, initial: true) { _, phase in library.setActive(phase == .active) }
        .windowStyle(.hiddenTitleBar)
        .defaultSize(width: 1440, height: 900)
        .commands {
            CommandGroup(after: .sidebar) {
                Toggle("过滤隐藏目录内容", isOn: $library.hiddenDirectoryFilterEnabled)
            }
            CommandMenu("照片") {
                Button("上一张") { library.selectAdjacent(-1) }
                Button("下一张") { library.selectAdjacent(1) }
                Divider()
                ForEach(0...5, id: \.self) { rating in
                    Button(rating == 0 ? "清除评分" : "\(rating) 星") { library.updateSelected(KeepsAssetPatch(rating: rating)) }
                        .keyboardShortcut(KeyEquivalent(Character(String(rating))), modifiers: [])
                }
                .disabled(library.selectedIDs.isEmpty || library.isMutating)
                Divider()
                Button("留用") { library.updateSelected(KeepsAssetPatch(flagState: "picked")) }.keyboardShortcut("p", modifiers: [])
                Button("排除") { library.updateSelected(KeepsAssetPatch(flagState: "rejected")) }.keyboardShortcut("x", modifiers: [])
                Button("清除标记") { library.updateSelected(KeepsAssetPatch(flagState: "unflagged")) }.keyboardShortcut("u", modifiers: [])
            }
        }
        Settings {
            TabView {
                ServerSettingsView()
                    .tabItem { Label("服务器", systemImage: "server.rack") }
                Group {
                    if let client = library.client {
                        NASSourceSettingsView(client: client).id(ObjectIdentifier(client))
                    } else {
                        Text("请先在服务器设置中验证并保存连接。")
                            .padding(24)
                    }
                }
                .tabItem { Label("来源", systemImage: "folder") }
            }
            .environmentObject(library)
        }
    }
}
