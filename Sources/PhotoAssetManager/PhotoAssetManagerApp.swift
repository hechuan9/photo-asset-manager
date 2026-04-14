import SwiftUI

@main
struct PhotoAssetManagerApp: App {
    @StateObject private var library = LibraryStore()

    var body: some Scene {
        WindowGroup {
            ContentView()
                .environmentObject(library)
                .frame(minWidth: 1180, minHeight: 760)
        }
        .commands {
            CommandGroup(after: .newItem) {
                Button("扫描本地目录") {
                    library.chooseAndScan(storageKind: .local)
                }
                .keyboardShortcut("i", modifiers: [.command, .shift])

                Button("扫描 NAS 目录") {
                    library.chooseAndScan(storageKind: .nas)
                }
                .keyboardShortcut("n", modifiers: [.command, .shift])
            }
        }
    }
}
