import Foundation
import Testing
@testable import KeepsAPI

struct PhotoGridTests {
    @Test func squareRowsHaveFixedColumnsRegardlessOfAspectRatio() {
        for density in KeepsGalleryDensity.allCases {
            let ratios: [CGFloat] = Array(repeating: [1.5, 2.0 / 3, 1], count: 9).flatMap { $0 }
            let rows = KeepsPhotoGrid.rows(aspectRatios: ratios, width: 402, density: density)
            #expect(rows.dropLast().allSatisfy { $0.indices.count == density.columns })
            #expect(rows.flatMap(\.sizes).allSatisfy { $0 == rows[0].sizes[0] && $0.width == $0.height })
            #expect(rows.flatMap { Array($0.indices) } == Array(ratios.indices))
        }
    }

    @Test func aspectRatioRowsPreserveProportionsWithAlignedEdges() {
        let ratios: [CGFloat] = [1.5, 2.0 / 3, 2.0 / 3, 0.5, 2]
        for density in KeepsGalleryDensity.allCases {
            let rows = KeepsPhotoGrid.rows(aspectRatios: ratios, width: 402, density: density, style: .aspectRatio)
            for row in rows {
                for (index, size) in zip(row.indices, row.sizes) {
                    #expect(abs(size.width / size.height - ratios[index]) < 0.0001)
                    #expect(size.height == row.height)
                }
                #expect(abs(row.sizes.reduce(CGFloat.zero) { $0 + $1.width } + CGFloat(row.sizes.count - 1) - 402) < 0.001)
            }
            #expect(rows.flatMap { Array($0.indices) } == Array(ratios.indices))
        }
    }

    @Test func singlePhotoUsesFullWidthAndSelectedStyle() {
        let square = KeepsPhotoGrid.rows(aspectRatios: [0.5, 2], width: 402, density: .single)
        #expect(square.map(\.sizes) == [[CGSize(width: 402, height: 402)], [CGSize(width: 402, height: 402)]])
        let original = KeepsPhotoGrid.rows(aspectRatios: [0.5, 2], width: 402, density: .single, style: .aspectRatio)
        #expect(original.map(\.sizes) == [[CGSize(width: 402, height: 804)], [CGSize(width: 402, height: 201)]])
    }

    @Test func emptyAndInvalidDimensionsAreHandled() {
        for style in KeepsGalleryStyle.allCases {
            #expect(KeepsPhotoGrid.rows(aspectRatios: [], width: 402, density: .large, style: style).isEmpty)
            for width: CGFloat in [0, -1, .infinity, .nan] {
                #expect(KeepsPhotoGrid.rows(aspectRatios: [1], width: width, density: .large, style: style).isEmpty)
            }
            let rows = KeepsPhotoGrid.rows(aspectRatios: [0, -1, .infinity, .nan], width: 402, density: .single, style: style)
            #expect(rows.flatMap(\.sizes).allSatisfy { $0 == CGSize(width: 402, height: 402) })
        }
    }

    @Test func pinchStepsHaveThresholdsAndStopAtBounds() {
        #expect(KeepsGalleryDensity.single.pinched(magnification: 0.7) == .large)
        #expect(KeepsGalleryDensity.large.pinched(magnification: 0.7) == .medium)
        #expect(KeepsGalleryDensity.medium.pinched(magnification: 0.7) == .compact)
        #expect(KeepsGalleryDensity.compact.pinched(magnification: 0.7) == .compact)
        #expect(KeepsGalleryDensity.compact.pinched(magnification: 1.4) == .medium)
        #expect(KeepsGalleryDensity.medium.pinched(magnification: 1.4) == .large)
        #expect(KeepsGalleryDensity.large.pinched(magnification: 1.4) == .single)
        #expect(KeepsGalleryDensity.single.pinched(magnification: 1.4) == .single)
        #expect(KeepsGalleryDensity.medium.pinched(magnification: 1.1) == .medium)
    }
}
