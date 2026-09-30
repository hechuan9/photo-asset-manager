import Foundation

public enum KeepsGalleryDensity: String, CaseIterable, Sendable {
    case large, medium, compact

    public var columns: Int {
        switch self { case .large: 3; case .medium: 5; case .compact: 9 }
    }

    public func pinched(magnification: CGFloat) -> Self {
        if magnification < 0.8 {
            switch self { case .large: .medium; case .medium, .compact: .compact }
        } else if magnification > 1.25 {
            switch self { case .compact: .medium; case .medium, .large: .large }
        } else { self }
    }
}

public enum KeepsPhotoGrid {
    public struct Row {
        public let indices: Range<Int>
        public let sizes: [CGSize]
        public var height: CGFloat { sizes.map(\.height).max() ?? 0 }
    }

    public static func rows(aspectRatios: [CGFloat], width: CGFloat, density: KeepsGalleryDensity) -> [Row] {
        guard width > 0, width.isFinite else { return [] }
        let ratios = aspectRatios.map { $0.isFinite && $0 > 0 ? $0 : 1 }
        let columns = density.columns
        let gap: CGFloat = 1
        let squareSide = max(1, (width - CGFloat(columns - 1) * gap) / CGFloat(columns))
        // 以一行三张（中档五张）常见 2:3 竖图为面积基准，横图减少列数来保持面积接近。
        let targetRootArea = squareSide / sqrt(CGFloat(2.0 / 3.0))
        var result: [Row] = []
        var start = 0
        while start < ratios.count {
            let available = min(columns + 1, ratios.count - start)
            var count = min(columns, available)
            if density != .compact {
                count = (1...available).min { lhs, rhs in
                    score(start: start, count: lhs) < score(start: start, count: rhs)
                }!
            }
            let indices = start..<(start + count)
            if density == .compact {
                result.append(Row(indices: indices, sizes: Array(repeating: CGSize(width: squareSide, height: squareSide), count: count)))
            } else {
                let roots = indices.map { sqrt(ratios[$0]) }
                let fitted = max(1, width - CGFloat(count - 1) * gap) / roots.reduce(0, +)
                let sparse = start + count == ratios.count && fitted > targetRootArea
                let rootArea = min(sparse ? targetRootArea : fitted, width * 0.9 * (roots.min() ?? 1))
                result.append(Row(indices: indices, sizes: roots.map { CGSize(width: rootArea * $0, height: rootArea / $0) }))
            }
            start += count
        }
        return result

        func score(start: Int, count: Int) -> CGFloat {
            let rootSum = ratios[start..<(start + count)].reduce(CGFloat.zero) { $0 + sqrt($1) }
            let fitted = max(1, width - CGFloat(count - 1) * gap) / rootSum
            return abs(log(fitted / targetRootArea))
        }
    }
}
