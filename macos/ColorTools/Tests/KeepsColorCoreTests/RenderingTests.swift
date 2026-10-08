import Foundation
import ImageIO
import CoreGraphics
import XCTest
@testable import KeepsColorCore

final class RenderingTests: XCTestCase {
    func testRealRAWRecipeReplayAndMasks() throws {
        let environment = ProcessInfo.processInfo.environment
        guard let executable = environment["KEEPS_TEST_DARKTABLE"], let source = environment["KEEPS_TEST_RAW"] else {
            throw XCTSkip("Set KEEPS_TEST_DARKTABLE and KEEPS_TEST_RAW for real renderer validation")
        }
        let sourceURL = URL(fileURLWithPath: source)
        let before = try contentHash(sourceURL)
        let directory = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().appendingPathComponent(".build/real-render-\(UUID().uuidString)")
        let encoder = JSONEncoder(); encoder.outputFormatting = [.sortedKeys]
        var engine: DarktableProcess? = try DarktableProcess(executable: URL(fileURLWithPath: executable), source: sourceURL, directory: directory)
        func render(_ engine: DarktableProcess, _ recipe: ColorRecipe, _ operation: String, full: Bool = false) throws -> URL {
            let candidate = try engine.store.add(operationID: operation, parentID: nil, recipe: encoder.encode(recipe))
            return try engine.render(xmp: DarktableRecipe.apply(recipe, to: engine.baseline), candidateID: candidate.id, full: full)
        }
        var recipe = ColorRecipe(exposureEV: 1.5, whiteBalanceRGB: [1.01, 1, 0.98], contrast: 1.3, skew: -0.2, saturation: 1.05)
        let plain = try pixels(render(engine!, recipe, "plain"))
        recipe.localAdjustments = [LocalExposure(id: "subject", exposureEV: 1, mask: .ellipse(centerX: 0.42, centerY: 0.4, radiusX: 0.15, radiusY: 0.2, rotation: 0, feather: 0.06))]
        let ellipse = try pixels(render(engine!, recipe, "ellipse"))
        let inside = difference(plain, ellipse, x: 0.36...0.48, y: 0.3...0.5)
        let outside = difference(plain, ellipse, x: 0.75...0.9, y: 0.7...0.9)
        XCTAssertGreaterThan(inside, 3, "Ellipse must affect the intended region")
        XCTAssertLessThan(outside, 1, "Ellipse must not become a global adjustment")
        recipe.localAdjustments.append(LocalExposure(id: "window", exposureEV: -0.5, mask: .gradient(anchorX: 0.5, anchorY: 0.2, rotation: 0, compression: 0.3)))
        let first = try pixels(render(engine!, recipe, "combined"))
        XCTAssertGreaterThan(difference(first, ellipse, x: 0...1, y: 0...1), 0.1, "Gradient must affect pixels")
        engine = nil
        let restarted = try DarktableProcess(executable: URL(fileURLWithPath: executable), source: sourceURL, directory: directory)
        let replay = try pixels(render(restarted, recipe, "replay"))
        XCTAssertEqual(first.bytes, replay.bytes, "Rebuilt recipe in a fresh process must reproduce pixels")
        let full = try pixels(render(restarted, recipe, "full", full: true), width: first.width, height: first.height)
        XCTAssertLessThan(difference(first, full, x: 0...1, y: 0...1), 3, "Preview and full-size must use consistent mask geometry/color")
        recipe.exposureEV = 0.5
        let changed = try pixels(render(restarted, recipe, "earlier-exposure"))
        XCTAssertGreaterThan(difference(first, changed, x: 0...1, y: 0...1), 3)
        XCTAssertEqual(try contentHash(sourceURL), before)
        print("Real renderer artifacts: \(directory.path); ellipse inside=\(inside), outside=\(outside)")
    }

    private struct Pixels { let bytes: [UInt8]; let width: Int; let height: Int }
    private func pixels(_ url: URL, width: Int = 400, height: Int = 267) throws -> Pixels {
        let source = try XCTUnwrap(CGImageSourceCreateWithURL(url as CFURL, nil))
        let image = try XCTUnwrap(CGImageSourceCreateImageAtIndex(source, 0, nil))
        var bytes = [UInt8](repeating: 0, count: width * height * 4)
        try bytes.withUnsafeMutableBytes { buffer in
            let context = try XCTUnwrap(CGContext(data: buffer.baseAddress, width: width, height: height, bitsPerComponent: 8, bytesPerRow: width*4, space: CGColorSpace(name: CGColorSpace.sRGB)!, bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue))
            context.interpolationQuality = .high
            context.draw(image, in: CGRect(x: 0, y: 0, width: width, height: height))
        }
        return Pixels(bytes: bytes, width: width, height: height)
    }
    private func difference(_ a: Pixels, _ b: Pixels, x: ClosedRange<Double>, y: ClosedRange<Double>) -> Double {
        var sum = 0.0; var count = 0
        for row in Int(y.lowerBound*Double(a.height))..<min(a.height, Int(y.upperBound*Double(a.height))) {
            for col in Int(x.lowerBound*Double(a.width))..<min(a.width, Int(x.upperBound*Double(a.width))) {
                for channel in 0..<3 {
                    let index = (row*a.width+col)*4+channel
                    sum += abs(Double(a.bytes[index])-Double(b.bytes[index])); count += 1
                }
            }
        }
        return sum / Double(count)
    }
}
