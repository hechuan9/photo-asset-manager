import Foundation

public enum KeepsGalleryDensity: String, CaseIterable, Sendable {
    case single, large, medium, compact

    public var columns: Int {
        switch self { case .single: 1; case .large: 3; case .medium: 5; case .compact: 9 }
    }

    public func pinched(magnification: CGFloat) -> Self {
        if magnification < 0.8 {
            switch self { case .single: .large; case .large: .medium; case .medium, .compact: .compact }
        } else if magnification > 1.25 {
            switch self { case .compact: .medium; case .medium: .large; case .large, .single: .single }
        } else { self }
    }
}

public enum KeepsGalleryStyle: String, CaseIterable, Sendable {
    case square, aspectRatio
}

public enum KeepsPhotoGrid {
    public struct Row {
        public let indices: Range<Int>
        public let sizes: [CGSize]
        public var height: CGFloat { sizes.map(\.height).max() ?? 0 }
    }

    public static func rows(aspectRatios: [CGFloat], width: CGFloat, density: KeepsGalleryDensity,
                            style: KeepsGalleryStyle = .square) -> [Row] {
        guard width > 0, width.isFinite else { return [] }
        let ratios = aspectRatios.map { $0.isFinite && $0 > 0 ? $0 : 1 }
        let columns = density.columns
        let gap: CGFloat = 1
        let squareSide = max(1, (width - CGFloat(columns - 1) * gap) / CGFloat(columns))
        return stride(from: 0, to: ratios.count, by: columns).map { start in
            let indices = start..<min(start + columns, ratios.count)
            let sizes: [CGSize]
            switch style {
            case .square:
                sizes = Array(repeating: CGSize(width: squareSide, height: squareSide), count: indices.count)
            case .aspectRatio:
                let rowRatios = ratios[indices]
                let height = max(1, width - CGFloat(indices.count - 1) * gap) / rowRatios.reduce(0, +)
                sizes = rowRatios.map { CGSize(width: height * $0, height: height) }
            }
            return Row(indices: indices, sizes: sizes)
        }
    }
}

extension KeepsPhotoGrid {
    /// A browsing session owns its slots; content updates never repack surviving rows.
    public struct Snapshot {
        public struct Slot: Identifiable, Equatable {
            public let id: UUID
            public let size: CGSize
        }
        public struct StableRow: Identifiable, Equatable {
            public let id: UUID
            public let slots: [Slot]
            public var height: CGFloat { slots.map(\.size.height).max() ?? 0 }
        }
        public private(set) var rows: [StableRow] = []
        private var knownIDs: Set<UUID> = []
        public init() {}

        public mutating func update(ids: [UUID], aspectRatios: [CGFloat], width: CGFloat,
                                    density: KeepsGalleryDensity, style: KeepsGalleryStyle = .square, reset: Bool = false) {
            guard width > 0, ids.count == aspectRatios.count else { return }
            if reset || knownIDs.isEmpty {
                rows = makeRows(Array(ids.indices))
                knownIDs = Set(ids)
                return
            }
            let present = Set(ids)
            let anchors = ids.indices.filter { knownIDs.contains(ids[$0]) }
            rows.removeAll { row in row.slots.allSatisfy { !present.contains($0.id) } }
            guard let first = anchors.first, let last = anchors.last else {
                rows = makeRows(Array(ids.indices))
                knownIDs = present
                return
            }
            let newest = ids.indices.filter { $0 < first && !knownIDs.contains(ids[$0]) }
            let oldest = ids.indices.filter { $0 > last && !knownIDs.contains(ids[$0]) }
            rows.insert(contentsOf: makeRows(newest), at: 0)
            rows.append(contentsOf: makeRows(oldest))
            // Historical inserts within the visible window wait for an explicit reflow.
            knownIDs = present.union(rows.flatMap { $0.slots.map(\.id) })

            func makeRows(_ indices: [Int]) -> [StableRow] {
                KeepsPhotoGrid.rows(aspectRatios: indices.map { aspectRatios[$0] }, width: width, density: density, style: style).map { row in
                    let slots = zip(row.indices, row.sizes).map { index, size in Slot(id: ids[indices[index]], size: size) }
                    return StableRow(id: slots[0].id, slots: slots)
                }
            }
        }
    }
}
