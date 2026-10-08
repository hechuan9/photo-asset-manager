import Foundation
import zlib

public enum RecipeError: Error, CustomStringConvertible {
    case invalid(String)
    public var description: String { switch self { case .invalid(let message): return message } }
}

public enum ColorMask: Codable, Equatable, Sendable {
    case ellipse(centerX: Double, centerY: Double, radiusX: Double, radiusY: Double, rotation: Double, feather: Double)
    case gradient(anchorX: Double, anchorY: Double, rotation: Double, compression: Double)
}
public struct LocalExposure: Codable, Equatable, Sendable {
    public var id: String
    public var exposureEV: Double
    public var mask: ColorMask
    public init(id: String, exposureEV: Double, mask: ColorMask) { self.id = id; self.exposureEV = exposureEV; self.mask = mask }
}
public struct ColorRecipe: Codable, Equatable, Sendable {
    public var exposureEV: Double
    public var whiteBalanceRGB: [Double]
    public var contrast: Double
    public var skew: Double
    public var saturation: Double
    public var localAdjustments: [LocalExposure]
    public init(exposureEV: Double = 0, whiteBalanceRGB: [Double] = [1,1,1], contrast: Double = 1.5, skew: Double = 0, saturation: Double = 1, localAdjustments: [LocalExposure] = []) {
        self.exposureEV = exposureEV; self.whiteBalanceRGB = whiteBalanceRGB; self.contrast = contrast; self.skew = skew; self.saturation = saturation; self.localAdjustments = localAdjustments
    }
    public func validate() throws {
        func check(_ value: Double, _ range: ClosedRange<Double>, _ name: String) throws {
            guard value.isFinite, range.contains(value) else { throw RecipeError.invalid("\(name) outside \(range)") }
        }
        try check(exposureEV, -5...5, "exposureEV")
        guard whiteBalanceRGB.count == 3 else { throw RecipeError.invalid("whiteBalanceRGB requires three source-relative multipliers") }
        for v in whiteBalanceRGB { try check(v, 0.5...2, "whiteBalanceRGB") }
        try check(contrast, 0.5...3, "contrast"); try check(skew, -1...1, "skew"); try check(saturation, 0...2, "saturation")
        guard localAdjustments.count <= 8, Set(localAdjustments.map(\.id)).count == localAdjustments.count else { throw RecipeError.invalid("At most eight uniquely named local adjustments") }
        for local in localAdjustments {
            guard !local.id.isEmpty else { throw RecipeError.invalid("Empty adjustment id") }
            try check(local.exposureEV, -3...3, "local exposure")
            switch local.mask {
            case let .ellipse(x,y,rx,ry,rotation,feather):
                try check(x,0...1,"centerX"); try check(y,0...1,"centerY"); try check(rx,0.002...1,"radiusX"); try check(ry,0.002...1,"radiusY"); try check(rotation,-180...180,"rotation"); try check(feather,0.001...1,"feather")
            case let .gradient(x,y,rotation,compression):
                try check(x,0...1,"anchorX"); try check(y,0...1,"anchorY"); try check(rotation,-180...180,"rotation"); try check(compression,0.001...1,"compression")
            }
        }
    }
}

// The binary layouts are pinned to darktable release-5.6.2: src/iop/{exposure,
// temperature,sigmoid,colorbalancergb}.c, src/develop/{blend,masks}.h and
// src/common/{exif.cc,iop_order.c}. Reject upgrades until replay is verified.
public enum DarktableRecipe {
    public static let engineVersion = "5.6.2"
    public static func decode(_ data: Data) throws -> ColorRecipe {
        guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any] else { throw RecipeError.invalid("Recipe must be an object") }
        let allowed: Set<String> = ["exposureEV","whiteBalanceRGB","contrast","skew","saturation","localAdjustments"]
        guard Set(object.keys).isSubset(of: allowed) else { throw RecipeError.invalid("Unknown recipe parameter") }
        if let locals = object["localAdjustments"] as? [[String: Any]] {
            for local in locals {
                guard Set(local.keys) == Set(["id","exposureEV","mask"]), let mask = local["mask"] as? [String:Any], mask.count == 1, let kind = mask.keys.first, let fields = mask[kind] as? [String:Any] else { throw RecipeError.invalid("Invalid local adjustment") }
                let keys: Set<String>
                switch kind {
                case "ellipse": keys = ["centerX","centerY","radiusX","radiusY","rotation","feather"]
                case "gradient": keys = ["anchorX","anchorY","rotation","compression"]
                default: throw RecipeError.invalid("Unknown mask type")
                }
                guard Set(fields.keys) == keys else { throw RecipeError.invalid("Unknown or missing mask parameter") }
            }
        }
        let defaults = try JSONSerialization.jsonObject(with: JSONEncoder().encode(ColorRecipe())) as! [String:Any]
        var merged = defaults; for (key,value) in object { merged[key] = value }
        let recipe = try JSONDecoder().decode(ColorRecipe.self, from: JSONSerialization.data(withJSONObject: merged))
        try recipe.validate(); return recipe
    }
    public static func apply(_ recipe: ColorRecipe, to baselineXMP: String) throws -> String {
        try recipe.validate()
        let doc = try XMLDocument(xmlString: baselineXMP, options: [.nodePreserveAll])
        guard let description = try doc.nodes(forXPath: "//*[local-name()='Description']").first as? XMLElement,
              description.attribute(forName: "darktable:xmp_version")?.stringValue == "5",
              description.attribute(forName: "darktable:iop_order_version")?.stringValue == "4",
              description.attribute(forName: "darktable:iop_order_list") == nil,
              let history = try doc.nodes(forXPath: "//*[local-name()='history']/*[local-name()='Seq']").first as? XMLElement,
              let masks = try doc.nodes(forXPath: "//*[local-name()='masks_history']/*[local-name()='Seq']").first as? XMLElement,
              masks.childCount == 0 else { throw RecipeError.invalid("Requires pristine darktable 5.6.2 RAW baseline (XMP 5, order 4, no masks/custom order)") }
        let items = history.children?.compactMap { $0 as? XMLElement } ?? []
        func module(_ operation: String, version: Int, size: Int) throws -> (XMLElement, [UInt8]) {
            let matches = items.filter { $0.attribute(forName: "darktable:operation")?.stringValue == operation }
            guard matches.count == 1, let node = matches.first,
                  node.attribute(forName: "darktable:modversion")?.stringValue == String(version),
                  node.attribute(forName: "darktable:multi_priority")?.stringValue == "0",
                  node.attribute(forName: "darktable:blendop_version")?.stringValue == "14",
                  let encoded = node.attribute(forName: "darktable:params")?.stringValue else { throw RecipeError.invalid("Unsupported \(operation) module version/instance") }
            let bytes = try unpack(encoded)
            guard bytes.count == size else { throw RecipeError.invalid("Invalid \(operation) parameter size") }
            return (node,bytes)
        }
        let (exposure, originalExposure) = try module("exposure", version: 7, size: 28)
        var exposureBytes = originalExposure
        put(0, into: &exposureBytes, at: 0); putFloat(recipe.exposureEV, into: &exposureBytes, at: 8)
        put(0, into: &exposureBytes, at: 20)
        set(exposure,"params",hex(exposureBytes))
        let (wb, initialWB) = try module("temperature", version: 4, size: 20)
        var wbBytes = initialWB
        for index in 0..<3 { putFloat(Double(readFloat(wbBytes,index*4))*recipe.whiteBalanceRGB[index], into: &wbBytes, at: index*4) }
        set(wb,"params",hex(wbBytes))
        let (sigmoid, initialSigmoid) = try module("sigmoid", version: 3, size: 56)
        var sigmoidBytes = initialSigmoid
        putFloat(recipe.contrast,into:&sigmoidBytes,at:0); putFloat(recipe.skew,into:&sigmoidBytes,at:4)
        set(sigmoid,"params",hex(sigmoidBytes))
        guard let blendText = exposure.attribute(forName:"darktable:blendop_params")?.stringValue else { throw RecipeError.invalid("Missing blend parameters") }
        let initialBlend = try unpack(blendText)
        guard initialBlend.count == 420 else { throw RecipeError.invalid("Unsupported blend v14 size") }
        var num = items.count
        if recipe.saturation != 1 {
            guard !items.contains(where: { $0.attribute(forName:"darktable:operation")?.stringValue == "colorbalancergb" }) else { throw RecipeError.invalid("Baseline already has color balance") }
            var params = [UInt8](repeating:0,count:132)
            for index in [12,14] { putFloat(1,into:&params,at:index*4) }
            for index in [28,30] { putFloat(0.1845,into:&params,at:index*4) }
            putFloat(recipe.saturation-1,into:&params,at:19*4); put(1,into:&params,at:128)
            let node = exposure.copy() as! XMLElement
            set(node,"num",String(num)); set(node,"operation","colorbalancergb"); set(node,"modversion","5"); set(node,"params",hex(params)); set(node,"multi_name","Keeps saturation")
            history.addChild(node); num += 1
        }
        for (index,local) in recipe.localAdjustments.enumerated() {
            let shapeID = 1000 + index*2, groupID = shapeID+1
            var shape = [UInt8](repeating:0,count:28)
            let type: Int
            switch local.mask {
            case let .ellipse(x,y,rx,ry,rotation,feather):
                type = 32
                for (i,v) in [x,y,rx,ry,rotation,feather].enumerated() { putFloat(v,into:&shape,at:i*4) }
                put(0,into:&shape,at:24)
            case let .gradient(x,y,rotation,compression):
                type = 16
                for (i,v) in [x,y,rotation,compression,0,0].enumerated() { putFloat(v,into:&shape,at:i*4) }
                put(1,into:&shape,at:24)
            }
            var group = [UInt8](repeating:0,count:16)
            put(UInt32(shapeID),into:&group,at:0); put(UInt32(groupID),into:&group,at:4); put(3,into:&group,at:8); putFloat(1,into:&group,at:12)
            for (id,t,bytes,name) in [(shapeID,type,shape,local.id),(groupID,4,group,local.id+" group")] {
                let node = XMLElement(name:"rdf:li")
                for (key,value) in ["mask_num":String(num),"mask_id":String(id),"mask_type":String(t),"mask_name":name,"mask_version":"6","mask_points":hex(bytes),"mask_nb":"1","mask_src":"0000000000000000"] { set(node,key,value) }
                masks.addChild(node)
            }
            var params = originalExposure
            put(0,into:&params,at:0); putFloat(0,into:&params,at:4); putFloat(local.exposureEV,into:&params,at:8); put(0,into:&params,at:20); put(0,into:&params,at:24)
            var blend = initialBlend; put(3,into:&blend,at:0); put(UInt32(groupID),into:&blend,at:24)
            let node = exposure.copy() as! XMLElement
            set(node,"num",String(num)); set(node,"params",hex(params)); set(node,"blendop_params",hex(blend)); set(node,"multi_priority",String(index+1)); set(node,"multi_name",local.id)
            history.addChild(node); num += 1
        }
        set(description,"history_end",String(num))
        if !recipe.localAdjustments.isEmpty {
            var entries = rawOrder.map { "\($0),0" }
            let position = entries.firstIndex(of:"exposure,0")! + 1
            entries.insert(contentsOf: recipe.localAdjustments.indices.map { "exposure,\($0+1)" }, at: position)
            set(description,"iop_order_list",entries.joined(separator:","))
        }
        return doc.xmlString(options: [])
    }
    private static func set(_ node: XMLElement, _ key: String, _ value: String) {
        let name = "darktable:"+key
        if let attribute = node.attribute(forName:name) { attribute.stringValue = value }
        else { node.addAttribute(XMLNode.attribute(withName:name,stringValue:value) as! XMLNode) }
    }
    private static func put(_ value: UInt32, into bytes: inout [UInt8], at offset: Int) { for i in 0..<4 { bytes[offset+i] = UInt8(truncatingIfNeeded:value >> (i*8)) } }
    private static func putFloat(_ value: Double, into bytes: inout [UInt8], at offset: Int) { put(Float(value).bitPattern,into:&bytes,at:offset) }
    private static func readFloat(_ bytes: [UInt8], _ offset: Int) -> Float { Float(bitPattern:(0..<4).reduce(UInt32(0)) { $0 | UInt32(bytes[offset+$1]) << ($1*8) }) }
    private static func hex(_ bytes: [UInt8]) -> String { bytes.map { String(format:"%02x",$0) }.joined() }
    private static func unpack(_ text: String) throws -> [UInt8] {
        if text.hasPrefix("gz") {
            guard text.count >= 4, let compressed = Data(base64Encoded:String(text.dropFirst(4))) else { throw RecipeError.invalid("Invalid compressed blob") }
            var destination = [UInt8](repeating:0,count:65536); var length = uLongf(destination.count)
            let result = compressed.withUnsafeBytes { source in uncompress(&destination,&length,source.bindMemory(to:UInt8.self).baseAddress,uLong(compressed.count)) }
            guard result == Z_OK else { throw RecipeError.invalid("Invalid zlib blob") }; return Array(destination.prefix(Int(length)))
        }
        guard text.count % 2 == 0 else { throw RecipeError.invalid("Invalid hex blob") }
        let chars = Array(text); var result: [UInt8] = []
        for i in stride(from:0,to:chars.count,by:2) { guard let byte = UInt8(String(chars[i...i+1]),radix:16) else { throw RecipeError.invalid("Invalid hex blob") }; result.append(byte) }
        return result
    }
    // Include inactive modules because darktable validates the complete pipeline order.
    private static let rawOrder: [String] = ["rawprepare","invert","temperature","rasterfile","highlights","cacorrect","hotpixels","rawdenoise","demosaic","denoiseprofile","bilateral","rotatepixels","scalepixels","lens","cacorrectrgb","hazeremoval","ashift","flip","enlargecanvas","overlay","clipping","liquify","spots","retouch","exposure","mask_manager","tonemap","toneequal","crop","graduatednd","profile_gamma","equalizer","colorin","channelmixerrgb","diffuse","censorize","negadoctor","blurs","primaries","nlmeans","colorchecker","neutral","defringe","atrous","lowpass","highpass","sharpen","colortransfer","colormapping","channelmixer","basicadj","colorharmonizer","colorbalance","colorequal","colorbalancergb","rgbcurve","rgblevels","basecurve","filmic","sigmoid","agx","filmicrgb","lut3d","colisa","tonecurve","levels","shadhi","zonesystem","globaltonemap","relight","bilat","colorcorrection","colorcontrast","velvia","vibrance","colorzones","bloom","colorize","lowlight","monochrome","grain","soften","splittoning","vignette","colorreconstruct","finalscale","colorout","clahe","overexposed","rawoverexposed","dither","borders","watermark","gamma"]
}
