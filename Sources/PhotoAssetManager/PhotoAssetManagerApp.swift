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
    }
}
