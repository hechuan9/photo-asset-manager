// swift-tools-version: 6.0
import PackageDescription
let package = Package(
    name: "KeepsAPI",
    platforms: [.macOS(.v14), .iOS(.v17)],
    products: [.library(name: "KeepsAPI", targets: ["KeepsAPI"])],
    targets: [.target(name: "KeepsAPI"), .testTarget(name: "KeepsAPITests", dependencies: ["KeepsAPI"])]
)
