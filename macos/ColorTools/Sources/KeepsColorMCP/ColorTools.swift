import Foundation
import KeepsColorCore

@MainActor final class ColorTools {
    private let engine: DarktableProcess
    private let encoder: JSONEncoder
    private let initial: ColorCandidate
    private var reviewed: Set<String> = []

    init(engine: DarktableProcess) throws {
        self.engine = engine
        encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        initial = try engine.store.add(operationID: "baseline", parentID: nil, recipe: encoder.encode(ColorRecipe()))
    }

    private func preview(_ id: String, full: Bool = false) throws -> URL {
        let candidate = try engine.store.candidate(id)
        let recipe = try DarktableRecipe.decode(candidate.recipe)
        return try engine.render(xmp: DarktableRecipe.apply(recipe, to: engine.baseline), candidateID: id, full: full)
    }
    func call(_ name: String, _ arguments: [String: Any]) throws -> [[String: Any]] {
        guard let definition = tools.first(where: { $0["name"] as? String == name }),
              let schema = definition["inputSchema"] as? [String: Any],
              let fields = schema["properties"] as? [String: Any],
              Set(arguments.keys).isSubset(of: Set(fields.keys)) else { throw ColorToolError("Unknown tool or argument") }
        func string(_ key: String) throws -> String {
            guard let value = arguments[key] as? String else { throw ColorToolError("Missing string: \(key)") }
            return value
        }
        switch name {
        case "inspect_photo":
            let image = try imageBlock(preview(initial.id))
            reviewed.insert(initial.id)
            return [try textBlock(["initialCandidateID": initial.id, "candidates": engine.store.candidates.map(\.id), "engine": "darktable 5.6.2", "supportsLocalMasks": engine.supportsLocalMasks, "supportedAdjustments": engine.isJPEG ? ["exposureEV", "saturation"] : ["exposureEV", "whiteBalanceRGB", "contrast", "skew", "saturation", "localAdjustments"], "remainingAdjustments": max(0, 5 - engine.store.candidates.count)]), image]
        case "get_recipe":
            let candidate = try engine.store.candidate(string("candidateID"))
            return [try textBlock(["candidateID": candidate.id, "recipe": JSONSerialization.jsonObject(with: candidate.recipe)])]
        case "set_adjustments":
            let parent = try engine.store.candidate(string("parentID"))
            let operation = try string("operationID")
            guard let patch = arguments["adjustments"] as? [String: Any], !patch.isEmpty else { throw ColorToolError("Empty adjustments") }
            var merged = try JSONSerialization.jsonObject(with: parent.recipe) as! [String: Any]
            for (key, value) in patch { merged[key] = value }
            let recipe = try DarktableRecipe.decode(JSONSerialization.data(withJSONObject: merged))
            guard recipe.localAdjustments.isEmpty || engine.supportsLocalMasks else { throw ColorToolError("Local masks currently require source orientation 1") }
            _ = try DarktableRecipe.apply(recipe, to: engine.baseline)
            guard engine.store.candidates.count < 5 || engine.store.candidates.contains(where: { $0.operationID == operation }) else { throw ColorToolError("Adjustment budget exhausted") }
            let candidate = try engine.store.add(operationID: operation, parentID: parent.id, recipe: encoder.encode(recipe))
            return [try textBlock(["candidateID": candidate.id])]
        case "preview_region":
            func number(_ key: String) throws -> Double {
                guard let value = arguments[key] as? Double else { throw ColorToolError("Missing number: \(key)") }
                return value
            }
            let (data, stats) = try PreviewAnalysis.crop(preview(string("candidateID")), x: number("x"), y: number("y"), width: number("width"), height: number("height"))
            return [try textBlock(stats), ["type": "image", "mimeType": "image/jpeg", "data": data.base64EncodedString()]]
        case "render_preview":
            let id = try string("candidateID")
            let image = try imageBlock(preview(id))
            reviewed.insert(id)
            return [image]
        case "compare_candidates":
            let first = try string("firstID"), second = try string("secondID")
            let images = [try imageBlock(preview(first)), try imageBlock(preview(second))]
            reviewed.formUnion([first, second])
            return images
        case "select_candidate":
            let id = try string("candidateID")
            guard reviewed.contains(id) else { throw ColorToolError("Review this candidate with render_preview or compare_candidates before selecting it") }
            let rendered = try preview(id)
            let full = try preview(id, full: true)
            let selection: [String: Any] = ["candidateID": id, "preview": rendered.path, "fullSize": full.path, "published": false]
            try JSONSerialization.data(withJSONObject: selection, options: [.sortedKeys]).write(to: engine.directory.appendingPathComponent("selection.json"), options: .atomic)
            return [try textBlock(selection)]
        default: throw ColorToolError("Unknown tool")
        }
    }
}
