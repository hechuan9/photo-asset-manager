import Foundation
import AppKit
import Testing
import KeepsAPI
@testable import PhotoAssetManager

@MainActor struct RemoteLibraryTests {
    @Test func unconfiguredLaunchHasNoLocalLibraryOrBackgroundWork() {
        let store = LibraryStore(loadSavedSettings: false)
        #expect(store.client == nil)
        store.refresh()
        #expect(!store.isLoading)
        #expect(store.assets.isEmpty)
        #expect(store.lastError == nil)
    }

    @Test func browserReadsRemoteAssetsAndSelectionRemainsPresentationState() async throws {
        let sessionConfiguration = URLSessionConfiguration.ephemeral
        sessionConfiguration.protocolClasses = [LibraryStubProtocol.self]
        let store = LibraryStore(configuration: KeepsConfiguration(baseURL: URL(string: "https://nas.invalid")!, libraryID: "test-library"), session: URLSession(configuration: sessionConfiguration), loadSavedSettings: false)
        store.refresh()
        store.refreshNavigation()
        try await waitUntil { !store.isLoading && !store.isLoadingNavigation }
        #expect(store.lastError == nil)
        #expect(store.assets.count == 2)
        #expect(store.total == 2)
        #expect(store.directories.first?.path == "2026")
        let first = store.assets[0].id
        let second = store.assets[1].id
        store.select(first, extending: false)
        store.select(second, extending: true)
        #expect(store.selectedIDs == [first, second])
        store.select(second, extending: true)
        #expect(store.selectedIDs == [first])
        store.selectAdjacent(1)
        #expect(store.selectedIDs == [second])
        store.updateSelected(KeepsAssetPatch(rating: 5))
        try await waitUntil { !store.isMutating && !store.isLoading }
        #expect(store.lastError == nil)
    }

    @Test func connectionTestValidatesAPIWithoutSavingOrSwitching() async {
        var persisted = false
        let old = KeepsConfiguration(baseURL: URL(string: "https://old.invalid")!, libraryID: "old")
        let store = LibraryStore(configuration: old, session: stubSession(), loadSavedSettings: false, persistConfiguration: { _ in persisted = true })
        let success = await store.checkConnection(baseURL: "https://working.invalid", libraryID: "new", accessCredential: "test-value", save: false)
        #expect(success)
        #expect(!persisted)
        #expect(store.configuration == old)
        #expect(store.connectionMessage != nil)
        #expect(store.connectionError == nil)
    }

    @Test func validatedConnectionPersistsThenSwitches() async throws {
        var persisted: KeepsConfiguration?
        let store = LibraryStore(session: stubSession(), loadSavedSettings: false, persistConfiguration: { persisted = $0 })
        let success = await store.checkConnection(baseURL: " https://working.invalid ", libraryID: " new ", accessCredential: "test-value", save: true)
        #expect(success)
        #expect(persisted?.libraryID == "new")
        #expect(store.configuration == persisted)
        try await waitUntil { !store.isLoading }
        #expect(store.assets.count == 2)
    }

    @Test func rejectedConnectionPreservesSavedServer() async {
        var persisted = false
        let old = KeepsConfiguration(baseURL: URL(string: "https://old.invalid")!, libraryID: "old")
        let store = LibraryStore(configuration: old, session: stubSession(), loadSavedSettings: false, persistConfiguration: { _ in persisted = true })
        for host in ["unauthorized.invalid", "invalid-api.invalid"] {
            let success = await store.checkConnection(baseURL: "https://\(host)", libraryID: "new", accessCredential: "test-value", save: true)
            #expect(!success)
            #expect(!persisted)
            #expect(store.configuration == old)
            #expect(store.client?.configuration == old)
            #expect(store.connectionError != nil)
            #expect(!store.isCheckingConnection)
        }
    }

    @Test func persistenceFailureDoesNotActivateCandidateServer() async {
        let old = KeepsConfiguration(baseURL: URL(string: "https://old.invalid")!, libraryID: "old")
        let store = LibraryStore(configuration: old, session: stubSession(), loadSavedSettings: false, persistConfiguration: { _ in
            throw NSError(domain: "SettingsTest", code: 1, userInfo: [NSLocalizedDescriptionKey: "storage unavailable"])
        })
        let success = await store.checkConnection(baseURL: "https://working.invalid", libraryID: "new", accessCredential: "test-value", save: true)
        #expect(!success)
        #expect(store.configuration == old)
        #expect(store.connectionError?.contains("storage unavailable") == true)
    }

    @Test func scopeSwitchClearsSelectionAndReturnsToUnifiedLibrary() async throws {
        let store = LibraryStore(configuration: KeepsConfiguration(baseURL: URL(string: "https://working.invalid")!, libraryID: "test"), session: stubSession(), loadSavedSettings: false)
        store.showLibrary(directory: "2026", picked: true)
        try await waitUntil { !store.isLoading }
        store.select(store.assets[0].id, extending: false)
        store.query.q = "old search"
        store.showLibrary()
        #expect(store.selectedIDs.isEmpty)
        #expect(store.query.directory == nil)
        #expect(store.query.flagState == nil)
        #expect(store.query.q.isEmpty)
        #expect(store.locationTitle == "全部照片")
        try await waitUntil { !store.isLoading }
        #expect(store.assets.count == 2)
        #expect(store.lastError == nil)
    }

    @Test func sourcesFailureDoesNotBlockHTTPAssetBrowsing() async throws {
        let store = LibraryStore(configuration: KeepsConfiguration(baseURL: URL(string: "https://sources-unavailable.invalid")!, libraryID: "test"), session: stubSession(), loadSavedSettings: false)
        store.refresh()
        store.refreshNavigation()
        try await waitUntil { !store.isLoading && !store.isLoadingNavigation }
        #expect(store.navigationError != nil)
        #expect(store.directories.isEmpty)
        #expect(store.lastError == nil)
        #expect(store.assets.count == 2)
        store.select(store.assets[0].id, extending: false)
        #expect(store.selectedAsset != nil)
    }

    @Test func unavailableServerReportsFailureWithoutLocalFallback() async throws {
        let store = LibraryStore(configuration: KeepsConfiguration(baseURL: URL(string: "https://unauthorized.invalid")!, libraryID: "test"), session: stubSession(), loadSavedSettings: false)
        store.showLibrary()
        try await waitUntil { !store.isLoading }
        #expect(store.lastError != nil)
        #expect(store.assets.isEmpty)
        #expect(store.configuration?.baseURL.host == "unauthorized.invalid")
    }

    @Test func directorySwitchClearsOldScopeAndLoadsChildrenFromServer() async throws {
        let store = LibraryStore(configuration: KeepsConfiguration(baseURL: URL(string: "https://working.invalid")!, libraryID: "test"), session: stubSession(), loadSavedSettings: false)
        store.query.flagState = "picked"
        store.query.trashed = true
        store.query.recursive = false
        store.showLibrary(directory: "2026")
        store.refreshNavigation()
        try await waitUntil { !store.isLoadingNavigation }
        store.loadChildren(of: "2026")
        try await waitUntil { !store.isLoading && store.loadingDirectories.isEmpty }
        #expect(store.query.flagState == nil)
        #expect(!store.query.trashed)
        #expect(store.query.recursive)
        #expect(store.directoryChildren["2026"]?.first?.name == "2026")
        let generation = store.navigationGeneration
        store.refreshNavigation()
        #expect(store.navigationGeneration == generation)
        #expect(store.directoryChildren["2026"] != nil)
        try await waitUntil { !store.isLoadingNavigation }
    }

    @Test func nativeOutlineRetainsExpansionSelectionAndCachedChildrenOnRefresh() async throws {
        _ = NSApplication.shared
        let store = LibraryStore(configuration: KeepsConfiguration(baseURL: URL(string: "https://tree.invalid")!, libraryID: "test"), session: stubSession(), loadSavedSettings: false)
        store.refreshNavigation()
        try await waitUntil { !store.isLoadingNavigation }
        let outline = NSOutlineView()
        let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier("directory"))
        outline.addTableColumn(column); outline.outlineTableColumn = column
        let coordinator = DirectoryOutlineView.Coordinator(library: store)
        coordinator.outline = outline
        outline.dataSource = coordinator; outline.delegate = coordinator
        coordinator.update()
        #expect(outline.numberOfRows == 2)
        let root = try #require(outline.item(atRow: 0) as? DirectoryOutlineView.Coordinator.Node)
        let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 300, height: 400), styleMask: .borderless, backing: .buffered, defer: false)
        window.contentView = outline
        window.makeFirstResponder(outline)
        outline.selectRowIndexes(IndexSet(integer: 0), byExtendingSelection: false)
        let rightArrow = try #require(NSEvent.keyEvent(with: .keyDown, location: .zero, modifierFlags: .function, timestamp: 0, windowNumber: window.windowNumber, context: nil, characters: String(UnicodeScalar(NSRightArrowFunctionKey)!), charactersIgnoringModifiers: String(UnicodeScalar(NSRightArrowFunctionKey)!), isARepeat: false, keyCode: 124))
        outline.keyDown(with: rightArrow)
        #expect(outline.isItemExpanded(root))
        #expect(store.selectedIDs.isEmpty)
        store.loadChildren(of: "root")
        try await waitUntil { store.loadingDirectories.isEmpty }
        coordinator.update()
        #expect(outline.numberOfRows == 3)
        let child = try #require(outline.item(atRow: 1) as? DirectoryOutlineView.Coordinator.Node)
        outline.expandItem(child)
        try await waitUntil { store.loadingDirectories.isEmpty }
        coordinator.update()
        #expect(outline.numberOfRows == 4)
        outline.selectRowIndexes(IndexSet(integer: 1), byExtendingSelection: false)
        #expect(store.query.directory == "root/child")
        outline.collapseItem(root)
        #expect(outline.numberOfRows == 2)
        outline.expandItem(root)
        coordinator.update()
        #expect(outline.numberOfRows == 4)
        #expect(LibraryStubProtocol.treeRequests.count(for: "root") == 1)
        #expect(LibraryStubProtocol.treeRequests.count(for: "root/child") == 1)
        store.refreshNavigation()
        #expect(store.expandedPaths.contains("root"))
        #expect(store.directoryChildren["root"] != nil)
        try await waitUntil { !store.isLoadingNavigation && store.loadingDirectories.isEmpty }
        coordinator.update()
        #expect(outline.isItemExpanded(root))
        #expect(outline.isItemExpanded(child))
        #expect(outline.item(atRow: 0) as? DirectoryOutlineView.Coordinator.Node === root)
        #expect(outline.selectedRow == 1)
        #expect(store.query.directory == "root/child")
    }

    @Test func switchingServerRejectsLateNavigationFromOldConnection() async throws {
        let store = LibraryStore(configuration: KeepsConfiguration(baseURL: URL(string: "https://slow-old.invalid")!, libraryID: "test"), session: stubSession(), loadSavedSettings: false, persistConfiguration: { _ in })
        store.refreshNavigation()
        try await waitUntil { LibraryStubProtocol.treeRequests.count(for: "slow-start") > 0 }
        let saved = await store.checkConnection(baseURL: "https://working.invalid", libraryID: "test", accessCredential: "", save: true)
        #expect(saved)
        try await waitUntil { !store.isLoadingNavigation }
        try await Task.sleep(for: .milliseconds(300))
        #expect(store.directories.map(\.path) == ["2026"])
        #expect(store.expandedPaths.isEmpty)
        #expect(store.directoryChildren.isEmpty)
    }

    @Test func directoryCellShowsLoadingUntilResponseAndClearsOnReuse() async throws {
        _ = NSApplication.shared
        let store = LibraryStore(configuration: KeepsConfiguration(baseURL: URL(string: "https://loading.invalid")!, libraryID: "test"), session: stubSession(), loadSavedSettings: false)
        store.refreshNavigation()
        try await waitUntil { !store.isLoadingNavigation }
        let outline = NSOutlineView()
        let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier("directory"))
        outline.addTableColumn(column); outline.outlineTableColumn = column
        let coordinator = DirectoryOutlineView.Coordinator(library: store)
        coordinator.outline = outline; outline.dataSource = coordinator; outline.delegate = coordinator
        coordinator.update()
        let item = try #require(outline.item(atRow: 0))
        store.loadChildren(of: "2026")
        coordinator.update()
        let cell = try #require(coordinator.outlineView(outline, viewFor: nil, item: item) as? DirectoryOutlineView.DirectoryCell)
        #expect(!cell.spinner.isHidden)
        #expect(cell.countLabel.stringValue == "0")
        #expect(store.directoryChildren["2026"] == nil)
        try await waitUntil { store.loadingDirectories.isEmpty }
        coordinator.update()
        #expect(store.directoryChildren["2026"]?.isEmpty == true)
        let directory = try #require(store.directories.first)
        cell.configure(directory, loading: false)
        #expect(cell.spinner.isHidden)
        #expect(cell.imageView?.isHidden == false)
        #expect(cell.countLabel.stringValue == "0")
        cell.configure(directory, loading: true)
        #expect(!cell.spinner.isHidden)
        cell.configure(directory, loading: false)
        #expect(cell.spinner.isHidden)
    }

    @Test func visiblePaginationContinuesAfterAnInFlightPageFinishes() async throws {
        let store = LibraryStore(configuration: KeepsConfiguration(baseURL: URL(string: "https://pages.invalid")!, libraryID: "test"), session: stubSession(), loadSavedSettings: false)
        store.refresh()
        store.setPaginationVisible(true)
        try await waitUntil { !store.isLoading }
        #expect(store.assets.count == 2)
        #expect(store.nextCursor == nil)
        #expect(store.lastError == nil)
    }

    @Test func paginationWaitsUntilFooterBecomesVisible() async throws {
        let store = LibraryStore(configuration: KeepsConfiguration(baseURL: URL(string: "https://pages.invalid")!, libraryID: "test"), session: stubSession(), loadSavedSettings: false)
        store.refresh()
        try await waitUntil { !store.isLoading }
        #expect(store.assets.count == 1)
        #expect(store.nextCursor == "next")
        store.setPaginationVisible(true)
        try await waitUntil { !store.isLoading }
        #expect(store.assets.count == 2)
        #expect(store.nextCursor == nil)
    }

    @Test func hiddenDirectoryMarkInheritsWithoutMatchingSiblingPrefixes() async throws {
        let store = LibraryStore(configuration: KeepsConfiguration(baseURL: URL(string: "https://working.invalid")!, libraryID: "test"), session: stubSession(), loadSavedSettings: false)
        store.refreshNavigation()
        try await waitUntil { !store.isLoadingNavigation }
        #expect(store.isDirectoryHidden("root/private"))
        #expect(store.isDirectoryHidden("root/private/child"))
        #expect(!store.isDirectoryHidden("root/private-other"))
        #expect(!store.isDirectoryHidden("root"))
        store.showLibrary()
        try await waitUntil { !store.isLoading }
        store.select(store.assets[0].id, extending: false)
        store.setDirectoryHidden("root/private", hidden: true)
        #expect(store.assets.isEmpty)
        #expect(store.selectedIDs.isEmpty)
        try await waitUntil { !store.isUpdatingHiddenDirectory && !store.isLoading }
        #expect(store.hiddenDirectoryPaths == ["root/private"])
        #expect(store.lastError == nil)
    }

    @Test func hiddenFilterSwitchClearsSelectionPersistsAndReloads() async throws {
        let name = "KeepsTests-" + UUID().uuidString
        let preferences = try #require(UserDefaults(suiteName: name))
        defer { preferences.removePersistentDomain(forName: name) }
        let store = LibraryStore(configuration: KeepsConfiguration(baseURL: URL(string: "https://working.invalid")!, libraryID: "test"), session: stubSession(), loadSavedSettings: false, preferences: preferences)
        #expect(store.hiddenDirectoryFilterEnabled)
        store.refresh()
        try await waitUntil { !store.isLoading }
        store.select(store.assets[0].id, extending: false)
        store.hiddenDirectoryFilterEnabled = false
        #expect(store.assets.isEmpty)
        #expect(store.selectedIDs.isEmpty)
        try await waitUntil { !store.isLoading }
        #expect(store.assets.count == 2)
        let reloaded = LibraryStore(loadSavedSettings: false, preferences: preferences)
        #expect(!reloaded.hiddenDirectoryFilterEnabled)
    }

    private func stubSession() -> URLSession {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [LibraryStubProtocol.self]
        return URLSession(configuration: configuration)
    }

    private func waitUntil(_ predicate: () -> Bool) async throws {
        for _ in 0..<200 {
            if predicate() { return }
            try await Task.sleep(for: .milliseconds(5))
        }
        Issue.record("remote request did not complete")
    }
}

private final class RequestCounts: @unchecked Sendable {
    private let lock = NSLock()
    private var values: [String: Int] = [:]
    func record(_ path: String) { lock.lock(); defer { lock.unlock() }; values[path, default: 0] += 1 }
    func count(for path: String) -> Int { lock.lock(); defer { lock.unlock() }; return values[path, default: 0] }
}

private final class LibraryStubProtocol: URLProtocol, @unchecked Sendable {
    static let treeRequests = RequestCounts()
    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func startLoading() {
        if request.url?.host == "slow-old.invalid", request.url?.path.hasSuffix("/navigation") == true {
            Self.treeRequests.record("slow-start")
            DispatchQueue.global().asyncAfter(deadline: .now() + 0.2) { [self] in
                deliver(body: "{\"directories\":[{\"path\":\"old-root\",\"name\":\"old-root\",\"photoCount\":0,\"hasChildren\":false}]}", status: 200)
            }
            return
        }
        if request.url?.host == "loading.invalid", request.url?.query != nil, request.url?.path.hasSuffix("/navigation") == true {
            DispatchQueue.global().asyncAfter(deadline: .now() + 0.1) { [self] in
                deliver(body: "{\"directories\":[]}", status: 200)
            }
            return
        }
        let first = Self.asset("00000000-0000-0000-0000-000000000001")
        let second = Self.asset("00000000-0000-0000-0000-000000000002")
        let path = request.url!.path
        let body: String
        let status = request.url?.host == "unauthorized.invalid" || (request.url?.host == "sources-unavailable.invalid" && path.hasSuffix("/navigation")) ? 401 : 200
        if status == 401 { body = "access denied" }
        else if request.url?.host == "invalid-api.invalid" && path.hasSuffix("/assets") { body = "{}" }
        else if request.httpMethod == "PATCH" { body = second }
        else if path.hasSuffix("/assets"), request.url?.host == "pages.invalid" {
            let hasCursor = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?.queryItems?.contains { $0.name == "cursor" } == true
            body = hasCursor ? "{\"items\":[\(second)],\"total\":2}" : "{\"items\":[\(first)],\"total\":2,\"nextCursor\":\"next\"}"
        }
        else if path.hasSuffix("/assets") { body = "{\"items\":[\(first),\(second)],\"total\":2}" }
        else if path.hasSuffix("/hidden-directories") { body = "{\"paths\":[\"root/private\"]}" }
        else if path.hasSuffix("/counts") { body = "{\"all\":2,\"picked\":0,\"trashed\":0}" }
        else if path.hasSuffix("/navigation"), request.url?.host == "tree.invalid" {
            let requestedPath = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?.queryItems?.first { $0.name == "path" }?.value ?? ""
            Self.treeRequests.record(requestedPath)
            let folders: [[String: Any]]
            switch requestedPath {
            case "root": folders = [["path": "root/child", "name": "child", "photoCount": 0, "hasChildren": true]]
            case "root/child": folders = [["path": "root/child/leaf", "name": "leaf", "photoCount": 0, "hasChildren": false]]
            default: folders = [["path": "root", "name": "root", "photoCount": 0, "hasChildren": true], ["path": "other", "name": "other", "photoCount": 0, "hasChildren": false]]
            }
            body = String(data: try! JSONSerialization.data(withJSONObject: ["directories": folders]), encoding: .utf8)!
        }
        else if path.hasSuffix("/navigation") { body = "{\"path\":null,\"directories\":[{\"path\":\"2026\",\"name\":\"2026\",\"photoCount\":0,\"hasChildren\":false}]}" }
        else if path.hasSuffix("/directories") { body = "{\"directories\":[{\"path\":\"2026\",\"count\":2}]}" }
        else { Issue.record("unexpected client request: \(path)"); body = "{}" }
        deliver(body: body, status: status)
    }
    private func deliver(body: String, status: Int) {
        client?.urlProtocol(self, didReceive: HTTPURLResponse(url: request.url!, statusCode: status, httpVersion: nil, headerFields: nil)!, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: Data(body.utf8))
        client?.urlProtocolDidFinishLoading(self)
    }
    override func stopLoading() {}
    private static func asset(_ id: String) -> String {
        "{\"id\":\"\(id)\",\"cameraMake\":\"\",\"cameraModel\":\"\",\"lensModel\":\"\",\"originalFilename\":\"photo.jpg\",\"contentFingerprint\":\"hash\",\"metadataFingerprint\":\"meta\",\"rating\":0,\"flagState\":\"unflagged\",\"tags\":[],\"createdAt\":\"2026-01-01T00:00:00Z\",\"updatedAt\":\"2026-01-01T00:00:00Z\",\"trashed\":false}"
    }
}
