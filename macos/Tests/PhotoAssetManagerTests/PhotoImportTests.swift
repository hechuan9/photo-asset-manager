import Foundation
import Testing
@testable import PhotoAssetManager

struct PhotoImportTests {
    @Test func importPlanFlattensMatchedExternalRawIntoTargetFolders() throws {
        let root = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: root) }

        let importSource = root.appendingPathComponent("import", isDirectory: true)
        let rawSource = root.appendingPathComponent("raw-library", isDirectory: true)
        let mainTarget = root.appendingPathComponent("library", isDirectory: true)
        let targetRaw = root.appendingPathComponent("Hasselblad RAW", isDirectory: true)
        let nested = importSource.appendingPathComponent("2024/06", isDirectory: true)
        try FileManager.default.createDirectory(at: nested, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: rawSource, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: mainTarget, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: targetRaw, withIntermediateDirectories: true)

        let jpeg = nested.appendingPathComponent("IMG_0001.jpg")
        let sidecar = nested.appendingPathComponent("IMG_0001.xmp")
        let raw = rawSource.appendingPathComponent("deep/IMG_0001.3fr")
        let rawSidecar = rawSource.appendingPathComponent("deep/IMG_0001.xmp")
        try FileManager.default.createDirectory(at: raw.deletingLastPathComponent(), withIntermediateDirectories: true)
        try Data("jpeg".utf8).write(to: jpeg)
        try Data("xmp".utf8).write(to: sidecar)
        try Data("raw".utf8).write(to: raw)
        try Data("raw-xmp".utf8).write(to: rawSidecar)

        let configuration = PhotoImportConfiguration(
            importSource: importSource,
            rawSource: rawSource,
            target: PhotoImportTarget(path: mainTarget.path, displayName: "library", storageKind: .local),
            targetRawRoot: targetRaw
        )
        let plan = try FileOperations().buildPhotoImportPlan(configuration: configuration)

        #expect(plan.mainDestination == mainTarget)
        #expect(plan.hasselbladRawDestination == targetRaw)
        #expect(plan.stats.photoCount == 1)
        #expect(plan.stats.matchedRawCount == 1)
        #expect(plan.items.count == 4)

        let jpegItem = try #require(plan.items.first { $0.sourcePath.hasSuffix("2024/06/IMG_0001.jpg") })
        let importSidecarItem = try #require(plan.items.first { $0.sourcePath.hasSuffix("2024/06/IMG_0001.xmp") })
        let rawItem = try #require(plan.items.first { $0.sourcePath.hasSuffix("deep/IMG_0001.3fr") })
        let rawSidecarItem = try #require(plan.items.first { $0.sourcePath.hasSuffix("deep/IMG_0001.xmp") })

        #expect(!jpegItem.routesToHasselbladRaw)
        #expect(jpegItem.destinationPath == mainTarget.appendingPathComponent("IMG_0001.jpg").path)
        #expect(!importSidecarItem.routesToHasselbladRaw)
        #expect(importSidecarItem.destinationPath == mainTarget.appendingPathComponent("IMG_0001.xmp").path)
        #expect(rawItem.routesToHasselbladRaw)
        #expect(rawItem.destinationPath == targetRaw.appendingPathComponent("IMG_0001.3fr").path)
        #expect(rawSidecarItem.routesToHasselbladRaw)
        #expect(rawSidecarItem.destinationPath == targetRaw.appendingPathComponent("IMG_0001.xmp").path)
    }

    @Test func importPlanImportsOnlyNonRawPhotosWhenRawSourceIsMissing() throws {
        let root = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: root) }

        let importSource = root.appendingPathComponent("import", isDirectory: true)
        let mainTarget = root.appendingPathComponent("library", isDirectory: true)
        try FileManager.default.createDirectory(at: importSource, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: mainTarget, withIntermediateDirectories: true)

        let raw = importSource.appendingPathComponent("IMG_0002.3fr")
        let jpeg = importSource.appendingPathComponent("IMG_0002.jpg")
        try Data("raw".utf8).write(to: raw)
        try Data("jpeg".utf8).write(to: jpeg)

        let configuration = PhotoImportConfiguration(
            importSource: importSource,
            rawSource: nil,
            target: PhotoImportTarget(path: mainTarget.path, displayName: "library", storageKind: .local),
            targetRawRoot: nil
        )
        let plan = try FileOperations().buildPhotoImportPlan(configuration: configuration)

        #expect(plan.hasselbladRawDestination == nil)
        #expect(plan.items.count == 1)
        #expect(plan.items.allSatisfy { !$0.routesToHasselbladRaw })
        #expect(plan.items[0].destinationPath == mainTarget.appendingPathComponent("IMG_0002.jpg").path)
    }

    @Test func importPlanRejectsFlattenedFilenameCollisions() throws {
        let root = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: root) }

        let importSource = root.appendingPathComponent("import", isDirectory: true)
        let mainTarget = root.appendingPathComponent("library", isDirectory: true)
        let first = importSource.appendingPathComponent("a/IMG_0005.jpg", isDirectory: false)
        let second = importSource.appendingPathComponent("b/IMG_0005.jpg", isDirectory: false)
        try FileManager.default.createDirectory(at: first.deletingLastPathComponent(), withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: second.deletingLastPathComponent(), withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: mainTarget, withIntermediateDirectories: true)
        try Data("one".utf8).write(to: first)
        try Data("two".utf8).write(to: second)

        let configuration = PhotoImportConfiguration(
            importSource: importSource,
            rawSource: nil,
            target: PhotoImportTarget(path: mainTarget.path, displayName: "library", storageKind: .local),
            targetRawRoot: nil
        )

        #expect(throws: FileOperationError.self) {
            try FileOperations().buildPhotoImportPlan(configuration: configuration)
        }
    }

    @Test func importPlanRejectsAmbiguousRawMatches() throws {
        let root = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: root) }

        let importSource = root.appendingPathComponent("import", isDirectory: true)
        let rawSource = root.appendingPathComponent("raw-library", isDirectory: true)
        let mainTarget = root.appendingPathComponent("library", isDirectory: true)
        let targetRaw = root.appendingPathComponent("Hasselblad RAW", isDirectory: true)
        try FileManager.default.createDirectory(at: importSource, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: rawSource, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: mainTarget, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: targetRaw, withIntermediateDirectories: true)

        try Data("jpeg".utf8).write(to: importSource.appendingPathComponent("IMG_0003.jpg"))
        try Data("raw-a".utf8).write(to: rawSource.appendingPathComponent("IMG_0003.3fr"))
        let nestedRaw = rawSource.appendingPathComponent("nested/IMG_0003.fff")
        try FileManager.default.createDirectory(at: nestedRaw.deletingLastPathComponent(), withIntermediateDirectories: true)
        try Data("raw-b".utf8).write(to: nestedRaw)

        let configuration = PhotoImportConfiguration(
            importSource: importSource,
            rawSource: rawSource,
            target: PhotoImportTarget(path: mainTarget.path, displayName: "library", storageKind: .local),
            targetRawRoot: targetRaw
        )

        #expect(throws: FileOperationError.self) {
            try FileOperations().buildPhotoImportPlan(configuration: configuration)
        }
    }

    @Test func copyImportedFolderPlacesFilesIntoSplitDestinations() async throws {
        let root = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: root) }

        let importSource = root.appendingPathComponent("import", isDirectory: true)
        let rawSource = root.appendingPathComponent("raw-library", isDirectory: true)
        let mainTarget = root.appendingPathComponent("library", isDirectory: true)
        let targetRaw = root.appendingPathComponent("Hasselblad RAW", isDirectory: true)
        try FileManager.default.createDirectory(at: importSource, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: rawSource, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: mainTarget, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: targetRaw, withIntermediateDirectories: true)

        try Data("jpeg".utf8).write(to: importSource.appendingPathComponent("IMG_0004.jpg"))
        try Data("raw".utf8).write(to: rawSource.appendingPathComponent("IMG_0004.3fr"))

        let configuration = PhotoImportConfiguration(
            importSource: importSource,
            rawSource: rawSource,
            target: PhotoImportTarget(path: mainTarget.path, displayName: "library", storageKind: .local),
            targetRawRoot: targetRaw
        )
        let plan = try FileOperations().buildPhotoImportPlan(configuration: configuration)

        try await FileOperations().copyImportedFolder(destination: plan.mainDestination, items: plan.items) { _, _ in }

        #expect(FileManager.default.fileExists(atPath: mainTarget.appendingPathComponent("IMG_0004.jpg").path))
        #expect(FileManager.default.fileExists(atPath: targetRaw.appendingPathComponent("IMG_0004.3fr").path))
        #expect(!FileManager.default.fileExists(atPath: mainTarget.appendingPathComponent("IMG_0004.3fr").path))
    }

    private func makeTemporaryDirectory() throws -> URL {
        let root = FileManager.default.temporaryDirectory
            .resolvingSymlinksInPath()
            .appendingPathComponent("PhotoAssetManagerTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        return root
    }
}