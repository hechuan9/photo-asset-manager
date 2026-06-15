import SwiftUI

@main
struct PhotoAssetManagerApp: App {
    @StateObject private var library = LibraryStore()
    @State private var didProcessLaunchRepairArgument = false

    var body: some Scene {
        WindowGroup {
            ContentView()
                .environmentObject(library)
                .frame(minWidth: 1180, minHeight: 760)
                .onAppear {
                    guard !didProcessLaunchRepairArgument else { return }
                    didProcessLaunchRepairArgument = true
                    guard ProcessInfo.processInfo.arguments.contains("--repair-preview-sync-state") else { return }
                    library.repairPreviewSyncStateFromLaunchArgument()
                }
        }
        .commands {
            ImportCommands(library: library)
            AssetSelectionCommands(library: library)
            FolderScopeCommands(library: library)
            ToolCommands(library: library)
        }
    }
}

struct AssetSelectionCommands: Commands {
    @ObservedObject var library: LibraryStore

    var body: some Commands {
        CommandMenu("照片") {
            Button("上一张") {
                library.selectAdjacentAsset(.previous)
            }
            .keyboardShortcut(.leftArrow, modifiers: [])
            .disabled(library.selectedAsset == nil || library.isBusy)

            Button("下一张") {
                library.selectAdjacentAsset(.next)
            }
            .keyboardShortcut(.rightArrow, modifiers: [])
            .disabled(library.selectedAsset == nil || library.isBusy)

            Divider()

            Button("1 星") {
                library.setSelectedAssetRating(1)
            }
            .keyboardShortcut("1", modifiers: [])
            .disabled(library.selectedAsset == nil || library.isBusy)

            Button("2 星") {
                library.setSelectedAssetRating(2)
            }
            .keyboardShortcut("2", modifiers: [])
            .disabled(library.selectedAsset == nil || library.isBusy)

            Button("3 星") {
                library.setSelectedAssetRating(3)
            }
            .keyboardShortcut("3", modifiers: [])
            .disabled(library.selectedAsset == nil || library.isBusy)

            Button("4 星") {
                library.setSelectedAssetRating(4)
            }
            .keyboardShortcut("4", modifiers: [])
            .disabled(library.selectedAsset == nil || library.isBusy)

            Button("5 星") {
                library.setSelectedAssetRating(5)
            }
            .keyboardShortcut("5", modifiers: [])
            .disabled(library.selectedAsset == nil || library.isBusy)

            Button("清除评分") {
                library.setSelectedAssetRating(0)
            }
            .keyboardShortcut("0", modifiers: [])
            .disabled(library.selectedAsset == nil || library.isBusy)

            Divider()

            Button("留用") {
                library.setSelectedAssetFlagState(.picked)
            }
            .keyboardShortcut("p", modifiers: [])
            .disabled(library.selectedAsset == nil || library.isBusy)

            Button("排除") {
                library.setSelectedAssetFlagState(.rejected)
            }
            .keyboardShortcut("x", modifiers: [])
            .disabled(library.selectedAsset == nil || library.isBusy)

            Button("清除标记") {
                library.setSelectedAssetFlagState(.unflagged)
            }
            .keyboardShortcut("u", modifiers: [])
            .disabled(library.selectedAsset == nil || library.isBusy)

            Divider()

            Button("从回收站恢复") {
                library.restoreAssetsFromTrash(Array(library.selectedAssetIDs))
            }
            .disabled(library.selectedAssetIDs.isEmpty || library.isBusy)
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

struct ImportCommands: Commands {
    @ObservedObject var library: LibraryStore

    var body: some Commands {
        CommandGroup(after: .newItem) {
            Button("导入照片...") {
                library.beginPhotoImport()
            }
            .keyboardShortcut("i", modifiers: [.command, .shift])
            .disabled(library.isBusy)
        }
    }
}

struct ToolCommands: Commands {
    @ObservedObject var library: LibraryStore

    var body: some Commands {
        CommandMenu("工具") {
            Button("补齐同步 Ledger") {
                library.backfillSyncLedger()
            }
            .disabled(library.isBusy)

            Divider()

            Button("补齐拍摄时间") {
                library.fillMissingCaptureTimes()
            }
            .disabled(library.isBusy)

            Divider()

            Button("重建全部预览 (1200px，一次性)") {
                library.rebuildAllPreviews()
            }
            .disabled(library.isBusy || library.derivativeStorageURL == nil)

            Button("修复预览同步状态") {
                library.repairPreviewSyncState()
            }
            .disabled(library.isBusy || library.isSyncing || !library.hasRemoteSyncConfiguration)
        }
    }
}
