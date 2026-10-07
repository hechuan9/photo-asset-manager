import Foundation
import Testing
@testable import KeepsAPI

struct AssetVersionTests {
    @Test func olderResponsesHaveNoDeprecatedFiles() throws {
        let version = try decodeDetails(extraFields: "")
        #expect(version.deprecatedFiles == nil)
        #expect(version.items.isEmpty)
    }

    @Test func deprecatedFilesPreserveReasonAndRetainedLocationWithoutBecomingVersions() throws {
        let version = try decodeDetails(extraFields: """
        ,"deprecatedFiles":[{"path":"/photos/copy.HEIC","retainedPath":"/photos/B0018262.HEIC","basis":"content_hash","reason":"完整文件内容相同，保留 B0018262.HEIC"}]
        """)
        let duplicate = try #require(version.deprecatedFiles?.first)
        #expect(duplicate.path == "/photos/copy.HEIC")
        #expect(duplicate.retainedPath == "/photos/B0018262.HEIC")
        #expect(duplicate.basis == "content_hash")
        #expect(duplicate.reason == "完整文件内容相同，保留 B0018262.HEIC")
        #expect(version.items.isEmpty)
    }

    private func decodeDetails(extraFields: String) throws -> KeepsAssetVersions {
        try JSONDecoder().decode(KeepsAssetVersions.self, from: Data("""
        {"items":[]\(extraFields)}
        """.utf8))
    }
}
