// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "KeepsIOSState",
    platforms: [.macOS(.v14)],
    dependencies: [.package(path: "../shared")],
    targets: [
        .target(name: "KeepsIOSState", dependencies: [.product(name: "KeepsAPI", package: "shared")],
                path: "Sources/KeepsIOS",
                exclude: ["IOSCollectionsView.swift", "IOSZoomablePhoto.swift",
                          "KeepsIOSApp.swift", "WaterfallGalleryView.swift", "IOSPhotoCollectionView.swift", "IOSThumbnailDownload.swift"],
                sources: ["IOSDirectoryStore.swift", "IOSLibraryStore.swift"]),
        .testTarget(name: "KeepsIOSStateTests", dependencies: ["KeepsIOSState"], path: "Tests")
    ]
)
