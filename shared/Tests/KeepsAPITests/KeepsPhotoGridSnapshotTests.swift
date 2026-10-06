import Foundation
import Testing
@testable import KeepsAPI

struct KeepsPhotoGridSnapshotTests {
    private func id(_ number: Int) -> UUID {
        UUID(uuidString: String(format: "00000000-0000-0000-0000-%012d", number))!
    }
    private func update(_ snapshot: inout KeepsPhotoGrid.Snapshot, _ numbers: [Int],
                        ratio: CGFloat = 1, reset: Bool = false) {
        snapshot.update(ids: numbers.map(id), aspectRatios: numbers.map { _ in ratio },
                        width: 390, density: .compact, reset: reset)
    }

    @Test func deletedPhotoKeepsSlotAndRowIdentity() {
        var snapshot = KeepsPhotoGrid.Snapshot()
        update(&snapshot, Array(1...18))
        let original = snapshot.rows
        update(&snapshot, Array(1...18).filter { $0 != 3 })
        #expect(snapshot.rows == original)
        #expect(snapshot.rows[0].slots[2].id == id(3))
    }

    @Test func onlyFullyEmptyRowIsRemoved() {
        var snapshot = KeepsPhotoGrid.Snapshot()
        update(&snapshot, Array(1...27))
        let original = snapshot.rows
        update(&snapshot, Array(1...9) + Array(19...27))
        #expect(snapshot.rows == [original[0], original[2]])
    }

    @Test func newAndOlderPhotosGetSeparateRowsWithoutFillingExistingRow() {
        var snapshot = KeepsPhotoGrid.Snapshot()
        update(&snapshot, [10, 20, 30])
        let original = snapshot.rows[0]
        update(&snapshot, [1, 2, 10, 20, 30, 40, 50])
        #expect(snapshot.rows.count == 3)
        #expect(snapshot.rows[1] == original)
        #expect(snapshot.rows[0].slots.map(\.id) == [id(1), id(2)])
        #expect(snapshot.rows[2].slots.map(\.id) == [id(40), id(50)])
    }

    @Test func historicalInsertWaitsForExplicitReflow() {
        var snapshot = KeepsPhotoGrid.Snapshot()
        update(&snapshot, [10, 20, 30])
        let original = snapshot.rows
        update(&snapshot, [10, 15, 20, 30])
        update(&snapshot, [10, 15, 20, 30])
        #expect(snapshot.rows == original)
        update(&snapshot, [10, 15, 20, 30], reset: true)
        #expect(snapshot.rows.flatMap(\.slots).map(\.id) == [10, 15, 20, 30].map(id))
    }

    @Test func previewDimensionsDoNotRepackRowsAndRefreshRemovesHoles() {
        var snapshot = KeepsPhotoGrid.Snapshot()
        let ids = Array(1...9).map(id)
        snapshot.update(ids: ids, aspectRatios: ids.map { _ in 1 }, width: 390, density: .large)
        let original = snapshot.rows
        snapshot.update(ids: ids, aspectRatios: ids.map { _ in 2 }, width: 390, density: .large)
        #expect(snapshot.rows == original)
        snapshot.update(ids: Array(ids.dropFirst()), aspectRatios: Array(repeating: 2, count: 8),
                        width: 390, density: .large, reset: true)
        #expect(!snapshot.rows.flatMap(\.slots).contains { $0.id == ids[0] })
        #expect(snapshot.rows != original)
    }
}
