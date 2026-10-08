import Foundation

struct AIEditingLimits: Codable, Equatable, Sendable {
    var photos = 20
    var ai = 20
    var renders = 2
    var downloads = 2
    var uploads = 2

    var bounded: Self {
        var result = self
        result.photos = min(20, max(1, photos))
        result.ai = min(20, max(1, ai))
        result.renders = min(20, max(1, renders))
        result.downloads = min(20, max(1, downloads))
        result.uploads = min(20, max(1, uploads))
        return result
    }

    static func load(defaults: UserDefaults = .standard) -> Self {
        guard let data = defaults.data(forKey: "aiEditing.limits"),
              let value = try? JSONDecoder().decode(Self.self, from: data) else { return Self() }
        return value.bounded
    }

    func save(defaults: UserDefaults = .standard) {
        defaults.set(try? JSONEncoder().encode(bounded), forKey: "aiEditing.limits")
    }
}
