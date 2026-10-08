import CoreImage
import Foundation
import ImageIO
import Testing
@testable import PhotoAssetManager

struct AIEditingImagesTests {
    @Test(arguments: [(7032, 4688), (1001, 667), (667, 1001), (511, 341), (1, 1)])
    func heifDimensionsMatchEncodedCanvas(dimensions: (Int, Int)) async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        let source = root.appendingPathComponent("synthetic.png")
        let image = CIImage(color: CIColor(red: 0.2, green: 0.5, blue: 0.7))
            .cropped(to: CGRect(x: 0, y: 0, width: dimensions.0, height: dimensions.1))
        try CIContext().writePNGRepresentation(of: image, to: source, format: .RGBA8,
                                             colorSpace: CGColorSpace(name: CGColorSpace.sRGB)!)
        let original = try await AIEditingImages.inspect(source, image: true)
        let outputs = try await AIEditingImages.derivatives(from: source, directory: root)
        for (role, maximum) in [("standard", 65536), ("thumbnail", 512), ("browse", 64)] {
            let url = try #require(outputs[role])
            let info = try await AIEditingImages.inspect(url, image: true)
            #expect(info.width > 0 && info.height > 0)
            #expect(info.width % 2 == 0 && info.height % 2 == 0)
            #expect(max(info.width, info.height) <= maximum)
            let decoded = try #require(CGImageSourceCreateWithURL(url as CFURL, nil))
            let pixels = try #require(CGImageSourceCreateImageAtIndex(decoded, 0, nil))
            #expect(pixels.width == info.width && pixels.height == info.height)
            if let exiftool = ProcessInfo.processInfo.environment["KEEPS_TEST_EXIFTOOL"] {
                let process = Process(), output = Pipe()
                process.executableURL = URL(fileURLWithPath: exiftool)
                process.arguments = ["-j", "-G0", "-n", "-ImageWidth", "-ImageHeight", url.path]
                process.standardOutput = output
                try process.run()
                let data = output.fileHandleForReading.readDataToEndOfFile()
                process.waitUntilExit()
                #expect(process.terminationStatus == 0)
                let records = try #require(JSONSerialization.jsonObject(with: data) as? [[String: Any]])
                let metadata = try #require(records.first)
                #expect(metadata["File:ImageWidth"] as? Int == info.width)
                #expect(metadata["File:ImageHeight"] as? Int == info.height)
            }
        }
        #expect(try await AIEditingImages.inspect(source, image: false).hash == original.hash)
    }
}
