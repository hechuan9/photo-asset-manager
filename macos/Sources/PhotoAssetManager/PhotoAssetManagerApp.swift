import SwiftUI
import KeepsAPI

@main
struct PhotoAssetManagerApp: App {
    @Environment(\.scenePhase) private var scenePhase
    @StateObject private var library = LibraryStore()
    @StateObject private var batch = AIEditingBatchStore()
    @FocusedValue(\.gallerySelection) private var gallerySelection
    var body: some Scene {
        WindowGroup {
            ContentView().environmentObject(library).environmentObject(batch).frame(minWidth: 1080, minHeight: 720)
                .onReceive(batch.editor.$isBusy, perform: synchronizeEditorActivity)
                .onReceive(NotificationCenter.default.publisher(for: NSApplication.willTerminateNotification)) { _ in batch.stopForExit() }
        }
        .onChange(of: scenePhase, initial: true) { _, phase in library.setActive(phase == .active) }
        .windowStyle(.hiddenTitleBar)
        .defaultSize(width: 1440, height: 900)
        .commands {
            CommandGroup(after: .pasteboard) {
                if let gallerySelection {
                    Button("全选照片", action: gallerySelection.selectAll)
                        .keyboardShortcut("a", modifiers: .command)
                        .disabled(library.isOperationBlocking)
                    Button("取消选择照片", action: gallerySelection.deselectAll)
                        .keyboardShortcut("a", modifiers: [.command, .shift])
                        .disabled(library.isOperationBlocking)
                }
            }
            CommandGroup(after: .sidebar) {
                Toggle("过滤隐藏目录内容", isOn: $library.hiddenDirectoryFilterEnabled).disabled(library.isOperationBlocking)
            }
            CommandMenu("照片") {
                Group {
                Button("上一张") { library.selectAdjacent(-1) }
                Button("下一张") { library.selectAdjacent(1) }
                Divider()
                ForEach(0...5, id: \.self) { rating in
                    Button(rating == 0 ? "清除评分" : "\(rating) 星") { library.updateSelected(KeepsAssetPatch(rating: rating)) }
                        .keyboardShortcut(KeyEquivalent(Character(String(rating))), modifiers: [])
                }
                .disabled(library.selectedIDs.isEmpty || library.isMutating || library.isSelectingAll)
                Divider()
                Button("留用") { library.updateSelected(KeepsAssetPatch(flagState: "picked")) }.keyboardShortcut("p", modifiers: [])
                Button("排除") { library.updateSelected(KeepsAssetPatch(flagState: "rejected")) }.keyboardShortcut("x", modifiers: [])
                Button("清除标记") { library.updateSelected(KeepsAssetPatch(flagState: "unflagged")) }.keyboardShortcut("u", modifiers: [])
                }.disabled(library.isOperationBlocking || library.isSelectingAll)
            }
        }
        Window("任务追踪", id: "nas-tasks") {
            Group {
                if let client = library.client {
                    NASTasksView(client: client).id(ObjectIdentifier(client))
                } else {
                    Text("请先在设置中连接服务器。").padding(24)
                }
            }.disabled(library.isOperationBlocking)
            .overlay { if library.isOperationBlocking { Text(library.isAIEditingBlocking ? "正在进行 AI 调色，请在主窗口查看进度。" : library.isAISettingsBusy ? "AI 修图设置正在运行，请在设置中查看进度。" : "正在整理照片或文件夹，请在主窗口查看进度。").padding().background(.regularMaterial) } }
        }
        .windowResizability(.contentSize)
        Settings {
            TabView {
                ServerSettingsView()
                    .disabled(library.isOperationBlocking)
                    .tabItem { Label("服务器", systemImage: "server.rack") }
                Group {
                    if let client = library.client {
                        NASSourceSettingsView(client: client).id(ObjectIdentifier(client))
                    } else {
                        Text("请先在服务器设置中验证并保存连接。")
                            .padding(24)
                    }
                }
                .disabled(library.isOperationBlocking)
                .tabItem { Label("来源", systemImage: "folder") }
                AIEditingSettingsView(store: batch.editor)
                    .disabled(library.isImportingPhotos || library.isMutating || library.isCheckingConnection || library.isUpdatingHiddenDirectory || library.directoryToRename != nil || library.directoryToTrash != nil)
                    .tabItem { Label("AI 修图", systemImage: "slider.horizontal.3") }
            }
            .environmentObject(library)
            .onReceive(batch.editor.$isBusy, perform: synchronizeEditorActivity)
            .disabled(library.isDirectoryOperationBlocking || batch.isRunning)
            .overlay {
                if library.isDirectoryOperationBlocking || batch.isRunning {
                    Text(batch.isBlocking ? "正在进行 AI 调色，请在主窗口查看进度。" : "正在整理照片或文件夹，请在主窗口查看进度。")
                        .padding().background(.regularMaterial)
                }
            }
        }
    }

    private func synchronizeEditorActivity(_ busy: Bool) {
        guard library.isAISettingsBusy != busy else { return }
        library.isAISettingsBusy = busy
        if busy { library.pauseLibraryForDirectoryOperation() }
        else if !library.isOperationBlocking { library.refresh(force: true) }
    }
}
