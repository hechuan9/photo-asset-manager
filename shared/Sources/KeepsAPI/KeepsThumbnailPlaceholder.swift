import Foundation
import ImageIO

/// One bundled image keeps loading, unavailable and failed thumbnails visually stable.
public enum KeepsThumbnailPlaceholder {
    public static let image: CGImage = {
        guard let url = Bundle.module.url(forResource: "ThumbnailPlaceholder", withExtension: "png"),
              let source = CGImageSourceCreateWithURL(url as CFURL, nil),
              let image = CGImageSourceCreateImageAtIndex(source, 0, [kCGImageSourceShouldCacheImmediately: true] as CFDictionary) else {
            preconditionFailure("Bundled ThumbnailPlaceholder.png is missing or invalid")
        }
        return image
    }()
}
