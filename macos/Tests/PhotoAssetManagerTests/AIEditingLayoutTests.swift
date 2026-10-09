import SwiftUI
import Testing
@testable import PhotoAssetManager

struct AIEditingLayoutTests {
    @Test func landscapePortraitAndSquareHaveEqualPreviewArea() {
        let sizes = [CGFloat(1.5), 2.0 / 3.0, 1.0].map {
            AIEditingComparisonLayout.previewSize(availableWidth: 1000, aspectRatio: $0, maximumHeight: 540)
        }
        for size in sizes {
            #expect(abs(size.width * size.height - 135_000) < 0.01)
        }
        #expect(sizes[0].width > sizes[0].height)
        #expect(sizes[1].height > sizes[1].width)
    }

    @Test func narrowWindowsAndExtremeRatiosFitWithoutCropping() {
        for ratio in [CGFloat(0.1), 2.0 / 3.0, 1.5, 10.0] {
            let size = AIEditingComparisonLayout.previewSize(availableWidth: 500, aspectRatio: ratio, maximumHeight: 300)
            #expect(size.width * 2 + 16 <= 500.01)
            #expect(size.height <= 300.01)
            #expect(abs(size.width / size.height - ratio) < 0.001)
        }
    }
}
