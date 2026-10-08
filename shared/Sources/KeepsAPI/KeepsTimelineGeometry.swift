import Foundation

/// Full-library positions with viewport lookup independent of the number of photos passed while scrolling.
public struct KeepsTimelineGeometry {
    public let count: Int
    public let contentSize: CGSize
    private let columns: Int
    private let squareSide: CGFloat
    private let rows: [Row]
    private let rowCount: Int

    private struct Row {
        let y: CGFloat
        let height: CGFloat
        let widths: [CGFloat]
    }

    public init(aspectRatios: [CGFloat], width: CGFloat, density: KeepsGalleryDensity, style: KeepsGalleryStyle) {
        columns = density.columns
        guard width.isFinite, width > 0 else {
            count = 0
            contentSize = .zero
            squareSide = 0
            rows = []
            rowCount = 0
            return
        }
        count = aspectRatios.count
        rowCount = (count + columns - 1) / columns
        squareSide = max(1, (width - CGFloat(columns - 1)) / CGFloat(columns))
        if style == .square {
            rows = []
            contentSize = CGSize(width: width, height: rowCount == 0 ? 0 : CGFloat(rowCount) * (squareSide + 1) - 1)
        } else {
            var built: [Row] = []
            built.reserveCapacity(rowCount)
            var y: CGFloat = 0
            for start in stride(from: 0, to: count, by: columns) {
                let ratios = aspectRatios[start..<min(start + columns, count)].map { $0.isFinite && $0 > 0 ? $0 : 1 }
                let height = max(1, width - CGFloat(ratios.count - 1)) / ratios.reduce(0, +)
                built.append(Row(y: y, height: height, widths: ratios.map { height * $0 }))
                y += height + 1
            }
            rows = built
            contentSize = CGSize(width: width, height: built.isEmpty ? 0 : y - 1)
        }
    }

    public func frame(at index: Int) -> CGRect? {
        guard (0..<count).contains(index) else { return nil }
        let rowIndex = index / columns
        let column = index % columns
        guard !rows.isEmpty else {
            return CGRect(x: CGFloat(column) * (squareSide + 1), y: CGFloat(rowIndex) * (squareSide + 1), width: squareSide, height: squareSide)
        }
        let row = rows[rowIndex]
        let x = row.widths.prefix(column).reduce(CGFloat(column), +)
        return CGRect(x: x, y: row.y, width: row.widths[column], height: row.height)
    }

    /// Candidate indices for intersecting rows; callers can filter frames for horizontal clipping.
    public func indices(in rect: CGRect) -> Range<Int> {
        guard count > 0, rect.width > 0, rect.height > 0,
              rect.minX < contentSize.width, rect.maxX > 0,
              rect.minY < contentSize.height, rect.maxY > 0 else { return 0..<0 }
        let lower: Int
        let upper: Int
        if rows.isEmpty {
            let stride = squareSide + 1
            lower = min(rowCount, max(0, Int(floor((max(0, rect.minY) + 1) / stride))))
            upper = min(rowCount, max(0, Int(ceil(min(contentSize.height, rect.maxY) / stride))))
        } else {
            lower = firstRow { $0.y + $0.height > rect.minY }
            upper = firstRow { $0.y >= rect.maxY }
        }
        return min(count, lower * columns)..<min(count, max(lower, upper) * columns)
    }

    private func firstRow(where predicate: (Row) -> Bool) -> Int {
        var lower = 0
        var upper = rows.count
        while lower < upper {
            let middle = lower + (upper - lower) / 2
            if predicate(rows[middle]) { upper = middle } else { lower = middle + 1 }
        }
        return lower
    }
}
