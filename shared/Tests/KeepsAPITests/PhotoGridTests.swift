import Foundation
import Testing
@testable import KeepsAPI

struct PhotoGridTests {
    @Test func largeUsesThreePortraitsOrTwoLandscapesWithComparableArea() {
        let portrait = KeepsPhotoGrid.rows(aspectRatios: Array(repeating: 2.0 / 3, count: 12), width: 402, density: .large)
        let landscape = KeepsPhotoGrid.rows(aspectRatios: Array(repeating: 1.5, count: 12), width: 402, density: .large)
        #expect(portrait.allSatisfy { $0.indices.count == 3 })
        #expect(landscape.allSatisfy { $0.indices.count == 2 })
        let p = portrait[0].sizes[0], l = landscape[0].sizes[0]
        #expect(abs(p.width * p.height / (l.width * l.height) - 1) < 0.02)
    }

    @Test func mixedRowPreservesProportionsAndEqualAreas() {
        let ratios: [CGFloat] = [1.5, 2.0 / 3, 2.0 / 3]
        let rows = KeepsPhotoGrid.rows(aspectRatios: ratios, width: 402, density: .large)
        #expect(rows.count == 1)
        #expect(rows[0].indices.count == 3)
        let sizes = rows[0].sizes
        for index in ratios.indices {
            #expect(abs(sizes[index].width / sizes[index].height - ratios[index]) < 0.0001)
            #expect(abs(sizes[index].width * sizes[index].height - sizes[0].width * sizes[0].height) < 0.001)
        }
        #expect(abs(sizes.reduce(CGFloat.zero) { $0 + $1.width } + 2 - 402) < 0.001)
    }

    @Test func mediumAndCompactIncreaseDensityWithoutLosingAssets() {
        let ratios = Array(repeating: CGFloat(2.0 / 3), count: 45)
        let medium = KeepsPhotoGrid.rows(aspectRatios: ratios, width: 402, density: .medium)
        let compact = KeepsPhotoGrid.rows(aspectRatios: ratios, width: 402, density: .compact)
        #expect(medium.allSatisfy { $0.indices.count == 5 })
        #expect(compact.allSatisfy { $0.indices.count == 9 })
        for density in KeepsGalleryDensity.allCases {
            let rows = KeepsPhotoGrid.rows(aspectRatios: ratios, width: 402, density: density)
            #expect(rows.flatMap { Array($0.indices) } == Array(ratios.indices))
        }
        #expect(compact.flatMap(\.sizes).allSatisfy { $0.width == $0.height })
    }

    @Test func sparseAndExtremePhotosRemainBounded() {
        for density in KeepsGalleryDensity.allCases {
            #expect(KeepsPhotoGrid.rows(aspectRatios: [], width: 402, density: density).isEmpty)
            let rows = KeepsPhotoGrid.rows(aspectRatios: [0.03, 20, 1, 0.5, 2], width: 402, density: density)
            for row in rows {
                #expect(row.height <= 402 * 0.9)
                #expect(row.sizes.reduce(CGFloat.zero) { $0 + $1.width } + CGFloat(row.sizes.count - 1) <= 402.001)
                #expect(row.sizes.allSatisfy { $0.width > 0 && $0.height > 0 })
            }
        }
    }

    @Test func pinchStepsHaveThresholdsAndStopAtBounds() {
        #expect(KeepsGalleryDensity.large.pinched(magnification: 0.7) == .medium)
        #expect(KeepsGalleryDensity.medium.pinched(magnification: 0.7) == .compact)
        #expect(KeepsGalleryDensity.compact.pinched(magnification: 0.7) == .compact)
        #expect(KeepsGalleryDensity.compact.pinched(magnification: 1.4) == .medium)
        #expect(KeepsGalleryDensity.medium.pinched(magnification: 1.4) == .large)
        #expect(KeepsGalleryDensity.large.pinched(magnification: 1.4) == .large)
        #expect(KeepsGalleryDensity.medium.pinched(magnification: 1.1) == .medium)
    }
}
