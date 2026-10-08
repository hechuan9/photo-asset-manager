import Foundation
import KeepsColorCore

@MainActor func textBlock(_ object: Any) throws -> [String: Any] {
    let data = try JSONSerialization.data(withJSONObject: object, options: [.sortedKeys])
    return ["type": "text", "text": String(decoding: data, as: UTF8.self)]
}
@MainActor func imageBlock(_ url: URL) throws -> [String: Any] {
    ["type": "image", "mimeType": "image/jpeg", "data": try Data(contentsOf: url).base64EncodedString()]
}
@MainActor func objectSchema(_ fields: [String: Any], required: [String] = []) -> [String: Any] {
    ["type": "object", "properties": fields, "required": required, "additionalProperties": false]
}
@MainActor let stringField: [String: Any] = ["type": "string"]
@MainActor let candidateSchema = objectSchema(["candidateID": stringField], required: ["candidateID"])
@MainActor let recipeSchema: [String: Any] = objectSchema([
    "exposureEV": ["type": "number", "minimum": -5, "maximum": 5],
    "whiteBalanceRGB": ["type": "array", "items": ["type": "number", "minimum": 0.5, "maximum": 2], "minItems": 3, "maxItems": 3],
    "contrast": ["type": "number", "minimum": 0.5, "maximum": 3],
    "skew": ["type": "number", "minimum": -1, "maximum": 1],
    "saturation": ["type": "number", "minimum": 0, "maximum": 2],
    "localAdjustments": ["type": "array", "maxItems": 8, "items": objectSchema([
        "id": stringField, "exposureEV": ["type": "number", "minimum": -3, "maximum": 3],
        "mask": ["oneOf": [
            objectSchema(["ellipse": objectSchema(Dictionary(uniqueKeysWithValues: ["centerX", "centerY", "radiusX", "radiusY", "rotation", "feather"].map { ($0, ["type": "number"]) }), required: ["centerX", "centerY", "radiusX", "radiusY", "rotation", "feather"])], required: ["ellipse"]),
            objectSchema(["gradient": objectSchema(Dictionary(uniqueKeysWithValues: ["anchorX", "anchorY", "rotation", "compression"].map { ($0, ["type": "number"]) }), required: ["anchorX", "anchorY", "rotation", "compression"])], required: ["gradient"])
        ]]
    ], required: ["id", "exposureEV", "mask"])]
])
@MainActor let tools: [[String: Any]] = [
    ["name": "preview_region", "description": "Inspect a subject/face region from the rendered preview. x/y are normalized from TOP LEFT; width/height normalized by image size. Returns an enlarged crop and display-luma statistics, not a quality score.", "inputSchema": objectSchema(["candidateID": stringField, "x": ["type": "number"], "y": ["type": "number"], "width": ["type": "number"], "height": ["type": "number"]], required: ["candidateID", "x", "y", "width", "height"])],
    ["name": "inspect_photo", "description": "Read the initial engine-rendered photo and persisted candidate IDs. No camera JPEG target.", "inputSchema": objectSchema([:])],
    ["name": "get_recipe", "description": "Read a complete immutable recipe by candidateID. WB is source-relative RGB multipliers; exposure in EV; mask centers measured from TOP LEFT and normalized to image width/height, radii/feather to shorter image edge, rotations in degrees. Gradient compression normalized to image diagonal.", "inputSchema": candidateSchema],
    ["name": "set_adjustments", "description": "Create immutable candidate from parentID with absolute overrides. operationID makes identical retries idempotent. localAdjustments replaces entire local list. Maximum four candidates after baseline. Parent remains unchanged.", "inputSchema": objectSchema(["operationID": stringField, "parentID": stringField, "adjustments": recipeSchema], required: ["operationID", "parentID", "adjustments"])],
    ["name": "render_preview", "description": "Render and return an sRGB preview image for visual review.", "inputSchema": candidateSchema],
    ["name": "compare_candidates", "description": "Return both candidate previews in order for direct comparison.", "inputSchema": objectSchema(["firstID": stringField, "secondID": stringField], required: ["firstID", "secondID"])],
    ["name": "select_candidate", "description": "Select a reviewed candidate, render full size locally, and persist selection. This does not publish to NAS.", "inputSchema": candidateSchema]
]

