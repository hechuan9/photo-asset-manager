import Foundation
import Testing

struct StartupPerformanceTests {
    @Test func libraryStartupSchedulesAvailabilityCheckWithoutBlockingRefresh() throws {
        let source = try sourceFile("Sources/PhotoAssetManager/LibraryStore.swift")

        #expect(source.contains("startAvailabilityRefreshInBackground()"))
        #expect(!functionBody(named: "refresh", in: source).contains("markMissingFiles"))
        #expect(source.contains("backgroundTask"))
    }

    @Test func assetQueriesDeriveStatusWithoutPerAssetFileLookups() throws {
        let source = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")

        #expect(source.contains("func countsByStatus() throws -> [AssetStatus: Int]"))
        #expect(!functionBody(named: "countsByStatus", in: source).contains("queryAssets"))
        #expect(!source.contains("return asset.withStatus(derivedStatus(for: asset.id))"))
        #expect(!source.contains("private func derivedStatus(for assetID: UUID)"))
    }

    @Test func contentViewHasBottomBackgroundTaskBar() throws {
        let source = try sourceFile("Sources/PhotoAssetManager/ContentView.swift")

        #expect(source.contains("BackgroundTaskBar()"))
        #expect(source.contains(".safeAreaInset(edge: .bottom)"))
    }

    @Test func startupRefreshUsesPagedAssetLoading() throws {
        let store = try sourceFile("Sources/PhotoAssetManager/LibraryStore.swift")
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")
        let content = try sourceFile("Sources/PhotoAssetManager/ContentView.swift")

        #expect(database.contains("func queryAssets(filter: LibraryFilter, limit: Int, offset: Int = 0) throws -> [Asset]"))
        #expect(database.contains("LIMIT ? OFFSET ?"))
        #expect(database.contains("WITH page AS"))
        #expect(database.contains("idx_assets_sort_time"))
        #expect(functionBody(named: "refresh", in: store).contains("assetPageSize + 1"))
        #expect(!functionBody(named: "refresh", in: store).contains("countsByStatus"))
        #expect(store.contains("func refreshCounts()"))
        #expect(store.contains("func loadMoreAssets()"))
        #expect(content.contains("Button(\"加载更多\")"))
    }

    @Test func browseGraphPersistsFolderMembershipAndFiltersAssetQueries() throws {
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")

        #expect(database.contains("CREATE TABLE IF NOT EXISTS browse_nodes"))
        #expect(database.contains("CREATE TABLE IF NOT EXISTS browse_edges"))
        #expect(database.contains("CREATE TABLE IF NOT EXISTS browse_file_instances"))
        #expect(database.contains("UNIQUE(kind, canonical_key)"))
        #expect(database.contains("WITH RECURSIVE selected_browse_nodes"))
        #expect(database.contains("browse_file_instances bfi"))
        #expect(database.contains("func upsertBrowseFolderMembership(filePath: String, fileInstanceID: UUID, storageKind: StorageKind)"))
        #expect(database.contains("func backfillBrowseGraphFromFileInstances()"))
        #expect(database.contains("try backfillBrowseGraphFromFileInstances()"))
        #expect(database.contains("func browseNodeIDs(selection: BrowseSelection) throws -> [UUID]"))
        #expect(database.contains("upsertBrowseFolderMembership(filePath: scanned.url.path, fileInstanceID: fileID, storageKind: scanned.storageKind)"))
        #expect(database.contains("BrowseScope.direct"))
        #expect(database.contains("BrowseScope.recursive"))
    }

    @Test func databaseMigrationDoesNotRestoreInterruptedScanRootsAsSources() throws {
        let database = try sourceFile("Sources/PhotoAssetManager/SQLiteDatabase.swift")

        #expect(database.contains("WHERE status IN ('finished', 'finished_with_errors', 'resumed')"))
        #expect(!database.contains("CASE WHEN status IN ('finished', 'finished_with_errors', 'resumed') THEN imported_at ELSE NULL END"))
    }

    @Test func scannerSkipsRecycleDirectoriesByPathComponent() throws {
        let scanner = try sourceFile("Sources/PhotoAssetManager/PhotoScanner.swift")

        #expect(scanner.contains("private let skippedDirectoryNames"))
        #expect(scanner.contains("url.pathComponents.contains"))
        #expect(scanner.contains("\"#recycle\""))
        #expect(functionBody(named: "shouldSkipDirectory", in: scanner).contains("skippedDirectoryNames.contains"))
    }

    private func sourceFile(_ path: String) throws -> String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repositoryRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let file = repositoryRoot.appendingPathComponent(path)
        return try String(contentsOf: file, encoding: .utf8)
    }

    private func functionBody(named name: String, in source: String) -> String {
        guard let range = source.range(of: "func \(name)") else { return "" }
        let suffix = source[range.lowerBound...]
        guard let openBrace = suffix.firstIndex(of: "{") else { return "" }

        var depth = 0
        var body = ""
        for character in suffix[openBrace...] {
            if character == "{" {
                depth += 1
            } else if character == "}" {
                depth -= 1
            }
            body.append(character)
            if depth == 0 {
                return body
            }
        }
        return body
    }
}
