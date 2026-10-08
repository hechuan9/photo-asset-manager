import Foundation
import XCTest
@testable import KeepsColorCore
@testable import KeepsColorMCP

final class IterationTests: XCTestCase {
    @MainActor func testIterationStartsFromExactRecipeAndRetainsUntouchedParameters() throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent("keeps-iteration-" + UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        let job = root.appendingPathComponent("job")
        try FileManager.default.createDirectory(at: job, withIntermediateDirectories: true)
        let executable = root.appendingPathComponent("darktable")
        try Data("#!/bin/sh\nprintf 'darktable 5.6.2\\n'\n".utf8).write(to: executable)
        try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: executable.path)
        let source = root.appendingPathComponent("sample.jpg")
        try Data([1, 2, 3]).write(to: source)
        try Data(DarktableRecipeTests.baseline.utf8).write(to: job.appendingPathComponent("baseline.xmp"))
        let engine = try DarktableProcess(executable: executable, source: source, directory: job)
        let base = ColorRecipe(exposureEV: 1.25, whiteBalanceRGB: [1.1, 1, 0.9], contrast: 1.4, saturation: 0.8)
        let tools = try ColorTools(engine: engine, baseRecipe: JSONEncoder().encode(base))
        let initial = try XCTUnwrap(engine.store.candidates.first { $0.operationID == "iteration-base" })
        XCTAssertEqual(try DarktableRecipe.decode(initial.recipe), base)
        _ = try tools.call("set_adjustments", ["parentID": initial.id, "operationID": "reduce-exposure", "adjustments": ["exposureEV": 0.5]])
        let next = try XCTUnwrap(engine.store.candidates.last)
        var expected = base
        expected.exposureEV = 0.5
        XCTAssertEqual(try DarktableRecipe.decode(next.recipe), expected)
        XCTAssertEqual(try Data(contentsOf: source), Data([1, 2, 3]))
    }
}
