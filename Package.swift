// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "PhotoAssetManager",
    platforms: [
        .macOS(.v14)
    ],
    products: [
        .executable(name: "PhotoAssetManager", targets: ["PhotoAssetManager"])
    ],
    targets: [
        .executableTarget(
            name: "PhotoAssetManager",
            exclude: ["Resources"],
            linkerSettings: [
                .linkedLibrary("sqlite3")
            ]
        ),
        .testTarget(
            name: "PhotoAssetManagerTests",
            dependencies: ["PhotoAssetManager"]
        )
    ]
)
