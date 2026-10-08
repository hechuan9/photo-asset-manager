import Foundation
import Testing
@testable import KeepsAPI

struct KeepsTimelineGeometryTests {
    @Test func hundredThousandItemsSupportDeepViewportAndLastPartialRow() throws {
        for style in KeepsGalleryStyle.allCases {
            let geometry = KeepsTimelineGeometry(aspectRatios: Array(repeating: 1, count: 100_000), width: 399, density: .compact, style: style)
            #expect(geometry.count == 100_000)
            #expect(geometry.frame(at: -1) == nil)
            #expect(geometry.frame(at: 100_000) == nil)
            #expect(geometry.frame(at: 0)?.origin == .zero)
            let deep = try #require(geometry.frame(at: 75_003))
            let indices = geometry.indices(in: CGRect(x: 0, y: deep.minY, width: 399, height: deep.height))
            #expect(indices == 74_997..<75_006)
            let last = try #require(geometry.frame(at: 99_999))
            #expect(abs(last.maxY - geometry.contentSize.height) < 0.001)
            #expect(geometry.indices(in: last) == 99_999..<100_000)
        }
    }

    @Test func rectanglesAtRowEdgesAndGapsDoNotIncludeAdjacentRows() throws {
        for style in KeepsGalleryStyle.allCases {
            let geometry = KeepsTimelineGeometry(aspectRatios: Array(repeating: 1, count: 9), width: 302, density: .large, style: style)
            #expect(geometry.indices(in: CGRect(x: 0, y: 0, width: 302, height: 100)) == 0..<3)
            #expect(geometry.indices(in: CGRect(x: 0, y: 100, width: 302, height: 1)).isEmpty)
            #expect(geometry.indices(in: CGRect(x: 0, y: 101, width: 302, height: 100)) == 3..<6)
            #expect(geometry.indices(in: CGRect(x: 0, y: -50, width: 302, height: 51)) == 0..<3)
            #expect(geometry.indices(in: CGRect(x: 0, y: 302, width: 302, height: 20)).isEmpty)
            #expect(geometry.indices(in: CGRect(x: 302, y: 0, width: 1, height: 100)).isEmpty)
            #expect(geometry.indices(in: .zero).isEmpty)
        }
    }

    @Test func aspectRatioFramesMatchExistingGridAcrossDensities() throws {
        let ratios: [CGFloat] = [0.5, 2, 1, 3, 0.25, .nan, -1, 1.5, 4, 0.7, 1]
        for density in KeepsGalleryDensity.allCases {
            for style in KeepsGalleryStyle.allCases {
                let geometry = KeepsTimelineGeometry(aspectRatios: ratios, width: 390, density: density, style: style)
                let expected = KeepsPhotoGrid.rows(aspectRatios: ratios, width: 390, density: density, style: style)
                var y: CGFloat = 0
                for row in expected {
                    var x: CGFloat = 0
                    for (index, size) in zip(row.indices, row.sizes) {
                        let frame = try #require(geometry.frame(at: index))
                        #expect(abs(frame.minX - x) < 0.00001)
                        #expect(abs(frame.minY - y) < 0.00001)
                        #expect(frame.size == size)
                        x += size.width + 1
                    }
                    #expect(geometry.indices(in: CGRect(x: 0, y: y + row.height / 2, width: 390, height: 0.01)) == row.indices)
                    y += row.height + 1
                }
                #expect(geometry.contentSize.height == y - 1)
            }
        }
    }

    @Test func emptyAndInvalidWidthsHaveNoFrames() {
        for width: CGFloat in [0, -1, .nan, .infinity] {
            let geometry = KeepsTimelineGeometry(aspectRatios: [1], width: width, density: .medium, style: .square)
            #expect(geometry.count == 0)
            #expect(geometry.contentSize == .zero)
        }
        let geometry = KeepsTimelineGeometry(aspectRatios: [], width: 390, density: .medium, style: .aspectRatio)
        #expect(geometry.contentSize.height == 0)
        #expect(geometry.frame(at: 0) == nil)
        #expect(geometry.indices(in: CGRect(x: 0, y: 0, width: 390, height: 800)).isEmpty)
    }
}
