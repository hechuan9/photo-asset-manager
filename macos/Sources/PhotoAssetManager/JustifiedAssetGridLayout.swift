import Foundation
import KeepsAPI

enum JustifiedAssetGridLayout {
    struct Row {
        let indices: Range<Int>
        let height: CGFloat
    }

    static func aspectRatio(_ preview: KeepsPreview?) -> CGFloat {
        guard let preview, preview.width > 0, preview.height > 0 else { return 1.5 }
        return CGFloat(preview.width) / CGFloat(preview.height)
    }

    static func rows(aspectRatios: [CGFloat], availableWidth: CGFloat, targetHeight: CGFloat) -> [Row] {
        let width = max(1, availableWidth)
        var rows: [Row] = []
        var start = 0
        var ratioSum: CGFloat = 0
        for (index, ratio) in aspectRatios.enumerated() {
            ratioSum += ratio
            let gaps = CGFloat(index - start)
            if ratioSum * targetHeight + gaps >= width {
                rows.append(Row(indices: start..<(index + 1), height: max(1, width - gaps) / ratioSum))
                start = index + 1
                ratioSum = 0
            }
        }
        // 不把末行少量照片放大到超过用户选择的缩略图尺寸。
        if start < aspectRatios.count {
            let gaps = CGFloat(aspectRatios.count - start - 1)
            rows.append(Row(indices: start..<aspectRatios.count, height: min(targetHeight, max(1, width - gaps) / ratioSum)))
        }
        return rows
    }
}
