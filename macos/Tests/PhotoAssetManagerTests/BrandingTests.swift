import Foundation
import Testing

struct BrandingTests {
    @Test func macOSBundleUsesKeepsBranding() throws {
        let plist = try propertyList(at: repositoryRoot().appendingPathComponent("macos/Sources/PhotoAssetManager/Resources/Info.plist"))

        #expect(plist["CFBundleName"] as? String == "Keeps")
        #expect(plist["CFBundleIdentifier"] as? String == "local.keeps")
        #expect(plist["CFBundleExecutable"] as? String == "Keeps")
    }

    @Test func packageScriptsUseKeepsAppOutput() throws {
        let packageScript = try String(contentsOf: repositoryRoot().appendingPathComponent("macos/scripts/package_app.sh"), encoding: .utf8)
        let iosPackageScript = try String(contentsOf: repositoryRoot().appendingPathComponent("ios/scripts/package_app.sh"), encoding: .utf8)

        #expect(packageScript.contains(".build/app/Keeps.app"))
        #expect(packageScript.contains("\"$MACOS_DIR/Keeps\""))
        #expect(iosPackageScript.contains("KeepsIOS.xcarchive"))
        #expect(iosPackageScript.contains("KeepsIOS.ipa"))
    }

    @Test func iOSProjectUsesKeepsDisplayNameAndBundleID() throws {
        let project = try String(contentsOf: repositoryRoot().appendingPathComponent("ios/KeepsIOS.xcodeproj/project.pbxproj"), encoding: .utf8)

        #expect(project.contains("INFOPLIST_KEY_CFBundleDisplayName = Keeps;"))
        #expect(project.contains("PRODUCT_BUNDLE_IDENTIFIER = com.hechuan.Keeps;"))
        #expect(project.contains("PRODUCT_NAME = Keeps;"))
    }

    @Test func appIconSourceIsKeepsSizedPng() throws {
        let iconURL = repositoryRoot().appendingPathComponent("ios/Assets.xcassets/AppIcon.appiconset/AppIcon.png")
        let dimensions = try pngDimensions(at: iconURL)

        #expect(dimensions.width == 1024)
        #expect(dimensions.height == 1024)
    }

    private func repositoryRoot() -> URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }

    private func propertyList(at url: URL) throws -> [String: Any] {
        let data = try Data(contentsOf: url)
        let plist = try PropertyListSerialization.propertyList(from: data, options: [], format: nil)
        return try #require(plist as? [String: Any])
    }

    private func pngDimensions(at url: URL) throws -> (width: UInt32, height: UInt32) {
        let data = try Data(contentsOf: url)
        #expect(data.count > 24)
        #expect(data[0..<8].elementsEqual([137, 80, 78, 71, 13, 10, 26, 10]))

        let width = data[16..<20].reduce(UInt32(0)) { ($0 << 8) | UInt32($1) }
        let height = data[20..<24].reduce(UInt32(0)) { ($0 << 8) | UInt32($1) }
        return (width, height)
    }
}
