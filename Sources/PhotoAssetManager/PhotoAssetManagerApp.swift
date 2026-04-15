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
            FolderScopeCommands(library: library)
        }
    }
}

struct FolderScopeCommands: Commands {
    @ObservedObject var library: LibraryStore

    var body: some Commands {
        CommandMenu("文件夹") {
            Button("仅当前文件夹") {
                library.setBrowseScope(.direct)
            }
            .disabled(library.filter.browseSelection == nil)

            Button("包含子文件夹") {
                library.setBrowseScope(.recursive)
            }
            .disabled(library.filter.browseSelection == nil)
        }
    }
}
