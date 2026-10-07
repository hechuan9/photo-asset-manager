import SwiftUI
import UIKit
import KeepsAPI
import OSLog

struct IOSPhotoCollectionView: UIViewControllerRepresentable {
    let library: IOSLibraryStore
    @Binding var selecting: Bool
    @Binding var selectedIDs: Set<UUID>
    @Binding var density: KeepsGalleryDensity
    @Binding var visibleDates: String
    var style: KeepsGalleryStyle
    var preparationFinished: (String?) -> Void
    var open: (KeepsAsset) -> Void
    var information: (KeepsAsset) -> Void

    func makeUIViewController(context: Context) -> PhotoCollectionController { PhotoCollectionController() }

    func updateUIViewController(_ controller: PhotoCollectionController, context: Context) {
        controller.preparationFinished = preparationFinished
        controller.open = open
        controller.information = information
        controller.select = { id in selecting = true; selectedIDs.insert(id) }
        controller.toggleSelection = { id in
            if !selectedIDs.insert(id).inserted { selectedIDs.remove(id) }
        }
        controller.changeDensity = { density = $0 }
        controller.changeDates = { if visibleDates != $0 { visibleDates = $0 } }
        controller.loadOlder = { await library.loadMore() }
        controller.loadNewer = { await library.loadNewer() }
        controller.restore = { [weak controller] asset in
            let found = await library.restoreWindow(around: asset)
            if let controller { applyLibrary(to: controller) }
            return found
        }
        applyLibrary(to: controller)
    }

    static func dismantleUIViewController(_ controller: PhotoCollectionController, coordinator: ()) {
        controller.cancelPreparation()
    }

    private func applyLibrary(to controller: PhotoCollectionController) {
        controller.update(assets: library.assets, configuration: library.configuration, density: density, style: style,
                          selecting: selecting, selected: selectedIDs,
                          older: library.canLoadMore, newer: library.canLoadNewer)
    }
}

final class PhotoCollectionController: UIViewController, UICollectionViewDelegate, UICollectionViewDataSourcePrefetching {
    var open: (KeepsAsset) -> Void = { _ in }
    var information: (KeepsAsset) -> Void = { _ in }
    var select: (UUID) -> Void = { _ in }
    var toggleSelection: (UUID) -> Void = { _ in }
    var changeDensity: (KeepsGalleryDensity) -> Void = { _ in }
    var changeDates: (String) -> Void = { _ in }
    var loadOlder: () async -> Void = {}
    var loadNewer: () async -> Void = {}
    var preparationFinished: (String?) -> Void = { _ in }
    var restore: (KeepsAsset) async -> Bool = { _ in false }

    private var collection: UICollectionView!
    private var source: UICollectionViewDiffableDataSource<UUID, UUID>!
    private var grid = KeepsPhotoGrid.Snapshot()
    private var rows: [KeepsPhotoGrid.Snapshot.StableRow] = []
    private var assets: [UUID: KeepsAsset] = [:]
    private var displayed: [KeepsAsset] = []
    private var pending: [KeepsAsset] = []
    private var configuration: KeepsConfiguration?
    private var density = KeepsGalleryDensity.large
    private var appliedDensity: KeepsGalleryDensity?
    private var style = KeepsGalleryStyle.square
    private var appliedStyle: KeepsGalleryStyle?
    private var appliedWidth: CGFloat = 0
    private var selecting = false
    private var selected: Set<UUID> = []
    private var older = false
    private var newer = false
    private var applying = false
    private var recoveringAnchor = false
    private var unavailableAnchor: UUID?
    private var paging = false
    private var pageReadFinished = false
    private var preparedImages: [UUID: (key: String, image: CGImage)] = [:]
    private var preparationTask: Task<Void, Never>?
    private var prepared = false
    private var paginationDirection: CGFloat = 0
    private var viewportHeight: CGFloat = 0
    private var dates = ""
    private var prefetches: [IndexPath: Task<Void, Never>] = [:]

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .black
        let layout = UICollectionViewCompositionalLayout { [weak self] section, _ in
            guard let self, self.rows.indices.contains(section) else { return nil }
            let row = self.rows[section]
            let items = row.slots.reversed().map { slot in
                NSCollectionLayoutItem(layoutSize: .init(widthDimension: .absolute(slot.size.width),
                                                         heightDimension: .absolute(slot.size.height)))
            }
            let group = NSCollectionLayoutGroup.horizontal(
                layoutSize: .init(widthDimension: .fractionalWidth(1), heightDimension: .absolute(row.height)),
                subitems: items)
            group.interItemSpacing = .fixed(1)
            let result = NSCollectionLayoutSection(group: group)
            result.contentInsets.bottom = 1
            return result
        }
        collection = UICollectionView(frame: .zero, collectionViewLayout: layout)
        collection.backgroundColor = .black
        collection.alwaysBounceVertical = true
        collection.contentInsetAdjustmentBehavior = .never
        collection.contentInset.bottom = 90
        collection.delegate = self
        collection.prefetchDataSource = self
        collection.accessibilityIdentifier = "photo-grid"
        collection.translatesAutoresizingMaskIntoConstraints = false
        view.addSubview(collection)
        NSLayoutConstraint.activate([
            collection.topAnchor.constraint(equalTo: view.topAnchor),
            collection.bottomAnchor.constraint(equalTo: view.bottomAnchor),
            collection.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            collection.trailingAnchor.constraint(equalTo: view.trailingAnchor)
        ])
        let registration = UICollectionView.CellRegistration<PhotoCollectionCell, UUID> { [weak self] cell, _, id in
            self?.configure(cell, id: id)
        }
        source = UICollectionViewDiffableDataSource<UUID, UUID>(collectionView: collection) { collection, path, id in
            collection.dequeueConfiguredReusableCell(using: registration, for: path, item: id)
        }
        collection.addGestureRecognizer(UIPinchGestureRecognizer(target: self, action: #selector(pinched(_:))))

    }

    func update(assets: [KeepsAsset], configuration: KeepsConfiguration?, density: KeepsGalleryDensity, style: KeepsGalleryStyle = .square,
                selecting: Bool, selected: Set<UUID>, older: Bool, newer: Bool) {
        loadViewIfNeeded()
        let appearanceChanged = self.selecting != selecting || self.selected != selected || self.configuration != configuration
        self.configuration = configuration
        self.density = density
        self.style = style
        self.selecting = selecting
        self.selected = selected
        self.older = older
        self.newer = newer
        pending = assets
        if appearanceChanged { updateVisibleCells() }
        applyPending()
    }

    override func viewDidLayoutSubviews() {
        super.viewDidLayoutSubviews()
        let previousHeight = viewportHeight
        viewportHeight = collection.bounds.height
        // The date header appears after the first visible assets are known.
        if previousHeight > 0, previousHeight != viewportHeight,
           !collection.isTracking, !collection.isDecelerating,
           abs(collection.contentOffset.y - max(0, collection.contentSize.height + collection.contentInset.bottom - previousHeight)) < 1 {
            collection.contentOffset.y = bottomOffset
        }
        applyPending()
    }

    private struct Anchor { let id: UUID; let distance: CGFloat }

    private func anchor(among candidates: Set<UUID>? = nil) -> Anchor? {
        let visible = collection.indexPathsForVisibleItems.compactMap { path -> (UUID, CGRect)? in
            guard let id = source.itemIdentifier(for: path), assets[id] != nil,
                  candidates == nil || candidates!.contains(id),
                  let frame = collection.layoutAttributesForItem(at: path)?.frame else { return nil }
            return (id, frame)
        }.sorted { $0.1.minY < $1.1.minY }
        guard let item = visible.first(where: { $0.1.midY >= collection.contentOffset.y }) ?? visible.first else { return nil }
        return Anchor(id: item.0, distance: item.1.minY - collection.contentOffset.y)
    }

    private func applyPending() {
        guard isViewLoaded, !applying, !recoveringAnchor,
              !collection.isTracking, !collection.isDecelerating,
              collection.bounds.width > 0, collection.bounds.height > 0 else { return }
        let width = collection.bounds.width
        let reflow = appliedWidth != width || appliedDensity != density || appliedStyle != style
        guard reflow || displayed != pending else {
            prepareInitialViewport()
            return
        }
        if !prepared { cancelPreparation() }
        let pendingIDs = Set(pending.map(\.id))
        let oldAnchor = anchor(among: pendingIDs) ?? anchor()
        if let oldAnchor, !pendingIDs.contains(oldAnchor.id), unavailableAnchor != oldAnchor.id,
           let asset = assets[oldAnchor.id] {
            // A fast reversal can reach the side evicted by a page still waiting for scrolling to end.
            recoveringAnchor = true
            Task { @MainActor [weak self] in
                guard let self else { return }
                let found = await restore(asset)
                recoveringAnchor = false
                unavailableAnchor = found ? nil : asset.id
                applyPending()
            }
            return
        }
        #if DEBUG
        let previousOffset = collection.contentOffset.y
        let previousAnchorDistance = oldAnchor?.distance ?? 0
        #endif
        let initial = displayed.isEmpty
        let oldAssets = assets
        displayed = pending
        assets = Dictionary(uniqueKeysWithValues: displayed.map { ($0.id, $0) })
        grid.update(ids: displayed.map(\.id),
                    aspectRatios: displayed.map { JustifiedAssetGridLayout.aspectRatio($0.gridPreview) },
                    width: width, density: density, style: style, reset: reflow)
        rows = grid.rows.reversed()
        appliedWidth = width
        appliedDensity = density
        appliedStyle = style
        prefetches.values.forEach { $0.cancel() }
        prefetches.removeAll()
        var snapshot = NSDiffableDataSourceSnapshot<UUID, UUID>()
        for row in rows {
            snapshot.appendSections([row.id])
            snapshot.appendItems(row.slots.reversed().map(\.id), toSection: row.id)
        }
        let existing = Set(source.snapshot().itemIdentifiers)
        snapshot.reconfigureItems(snapshot.itemIdentifiers.filter { existing.contains($0) && oldAssets[$0] != assets[$0] })
        applying = true
        UIView.performWithoutAnimation {
            source.apply(snapshot, animatingDifferences: false)
            collection.collectionViewLayout.invalidateLayout()
            collection.layoutIfNeeded()
            if let oldAnchor, let path = source.indexPath(for: oldAnchor.id),
               let frame = collection.layoutAttributesForItem(at: path)?.frame {
                let anchoredOffset = frame.minY - oldAnchor.distance
                let target = max(0, min(bottomOffset, anchoredOffset))
                if abs(collection.contentOffset.y - target) > 0.5 { collection.contentOffset.y = target }
            } else if initial {
                collection.contentOffset.y = bottomOffset
            }
        }
        collection.layoutIfNeeded()
        applying = false
        prepareInitialViewport()
        updateVisibleCells()
        updateDates()
        #if DEBUG
        Logger(subsystem: "com.hechuan.Keeps", category: "photo-grid")
            .debug("Grid window=\(self.displayed.count) rows=\(self.rows.count) visibleCells=\(self.collection.visibleCells.count) offset=\(previousOffset)->\(self.collection.contentOffset.y) anchorDistance=\(previousAnchorDistance)")
        #endif
    }

    private var bottomOffset: CGFloat { max(0, collection.contentSize.height + collection.contentInset.bottom - collection.bounds.height) }

    private func configure(_ cell: PhotoCollectionCell, id: UUID) {
        cell.configure(asset: assets[id], configuration: configuration, compact: style == .square,
                       selecting: selecting, selected: selected.contains(id), scale: view.window?.screen.scale ?? 3,
                       preparedImage: preparedImages[id])
    }

    private func updateVisibleCells() {
        for path in collection.indexPathsForVisibleItems {
            if let id = source.itemIdentifier(for: path), let cell = collection.cellForItem(at: path) as? PhotoCollectionCell {
                configure(cell, id: id)
            }
        }
    }

    private func updateDates() {
        let values = collection.indexPathsForVisibleItems.compactMap { path -> String? in
            guard let id = source.itemIdentifier(for: path), let asset = assets[id] else { return nil }
            return String((asset.captureTime ?? asset.createdAt).prefix(10))
        }.sorted()
        guard let first = values.first, let last = values.last else { return }
        let next = first == last ? first : "\(first) – \(last)"
        guard dates != next else { return }
        dates = next
        // UIKit layout callbacks can run inside SwiftUI's update pass.
        Task { @MainActor [weak self] in
            guard let self, self.dates == next else { return }
            self.changeDates(next)
        }
    }

    func scrollViewDidScroll(_ scrollView: UIScrollView) {
        guard !applying else { return }
        updateDates()
        if scrollView.isDragging {
            paginationDirection = -scrollView.panGestureRecognizer.velocity(in: scrollView).y
        }
        guard scrollView.isDragging || scrollView.isDecelerating, !paging else { return }
        if paginationDirection < 0, scrollView.contentOffset.y < scrollView.bounds.height, older { page(newer: false) }
        else if paginationDirection > 0, bottomOffset - scrollView.contentOffset.y < scrollView.bounds.height, newer { page(newer: true) }
    }

    private func page(newer: Bool) {
        guard !paging else { return }
        paging = true
        pageReadFinished = false
        Task { @MainActor [weak self] in
            guard let self else { return }
            if newer { await loadNewer() } else { await loadOlder() }
            pageReadFinished = true
            if !collection.isTracking && !collection.isDecelerating { paging = false }
            applyPending()
        }
    }

    func scrollViewDidEndDragging(_ scrollView: UIScrollView, willDecelerate decelerate: Bool) {
        if !decelerate { finishScrolling() }
    }

    func cancelPreparation() {
        preparationTask?.cancel()
        preparationTask = nil
    }

    private func prepareInitialViewport() {
        guard !prepared, preparationTask == nil, !displayed.isEmpty,
              let configuration else { return }
        let visible = collection.indexPathsForVisibleItems.compactMap { path -> (KeepsAsset, Int)? in
            guard let id = source.itemIdentifier(for: path), let asset = assets[id],
                  let frame = collection.layoutAttributesForItem(at: path)?.frame else { return nil }
            return (asset, Int(ceil(max(frame.width, frame.height) * (view.window?.screen.scale ?? 3))))
        }
        guard !visible.isEmpty else { return }
        preparationTask = Task { @MainActor [weak self] in
            guard let self else { return }
            do {
                for (asset, pixels) in visible {
                    guard let preview = asset.gridPreview else { continue }
                    let role: KeepsMediaRole = asset.thumbnail == nil ? .preview : .thumbnail
                    let image = try await PreviewCache.cache(for: role).image(
                        assetID: asset.id, preview: preview, configuration: configuration, maxPixelSize: pixels)
                    try Task.checkCancellation()
                    let key = PreviewCache.key(assetID: asset.id, preview: preview, configuration: configuration, role: role)
                    preparedImages[asset.id] = ("\(key):\(pixels)", image)
                }
                try Task.checkCancellation()
                prepared = true
                updateVisibleCells()
                collection.layoutIfNeeded()
                preparationFinished(nil)
            } catch {
                guard !Task.isCancelled else { return }
                Logger(subsystem: "com.hechuan.Keeps", category: "photo-grid")
                    .error("Initial thumbnails failed: \(String(reflecting: error), privacy: .public)")
                preparationFinished(String(reflecting: error))
            }
        }
    }

    func scrollViewDidEndDecelerating(_ scrollView: UIScrollView) { finishScrolling() }

    private func finishScrolling() {
        if pageReadFinished { paging = false }
        applyPending()
    }

    @objc private func pinched(_ gesture: UIPinchGestureRecognizer) {
        guard gesture.state == .ended else { return }
        let next = density.pinched(magnification: gesture.scale)
        if next != density { changeDensity(next) }
    }

    func collectionView(_ collectionView: UICollectionView, didSelectItemAt indexPath: IndexPath) {
        guard let id = source.itemIdentifier(for: indexPath), let asset = assets[id] else { return }
        if selecting { toggleSelection(id) } else { open(asset) }
    }

    func collectionView(_ collectionView: UICollectionView, contextMenuConfigurationForItemAt indexPath: IndexPath,
                        point: CGPoint) -> UIContextMenuConfiguration? {
        guard let id = source.itemIdentifier(for: indexPath), let asset = assets[id] else { return nil }
        return UIContextMenuConfiguration(identifier: nil, previewProvider: nil) { [weak self] _ in
            UIMenu(children: [
                UIAction(title: "查看照片", image: UIImage(systemName: "photo")) { _ in self?.open(asset) },
                UIAction(title: "选择", image: UIImage(systemName: "checkmark.circle")) { _ in self?.select(id) },
                UIAction(title: "信息与整理", image: UIImage(systemName: "info.circle")) { _ in self?.information(asset) }
            ])
        }
    }

    func collectionView(_ collectionView: UICollectionView, prefetchItemsAt indexPaths: [IndexPath]) {
        guard let configuration else { return }
        for path in indexPaths where prefetches[path] == nil {
            guard let id = source.itemIdentifier(for: path), let asset = assets[id], let preview = asset.gridPreview,
                  let frame = collectionView.layoutAttributesForItem(at: path)?.frame else { continue }
            let pixels = Int(ceil(max(frame.width, frame.height) * (view.window?.screen.scale ?? 3)))
            prefetches[path] = Task(priority: .utility) {
                do {
                    _ = try await PreviewCache.cache(for: asset.thumbnail == nil ? .preview : .thumbnail)
                        .image(assetID: id, preview: preview, configuration: configuration, maxPixelSize: pixels)
                } catch {
                    guard !Task.isCancelled else { return }
                    Logger(subsystem: "com.hechuan.Keeps", category: "photo-grid")
                        .error("Thumbnail prefetch failed: \(String(reflecting: error), privacy: .public)")
                }
            }
        }
    }

    func collectionView(_ collectionView: UICollectionView, cancelPrefetchingForItemsAt indexPaths: [IndexPath]) {
        for path in indexPaths { prefetches.removeValue(forKey: path)?.cancel() }
    }
}

private final class PhotoCollectionCell: UICollectionViewCell {
    private let photo = UIImageView()
    private let badge = UIImageView()
    private var task: Task<Void, Never>?
    private var key: String?
    private var preparedImage: (key: String, image: CGImage)?
    private var asset: KeepsAsset?
    private var configuration: KeepsConfiguration?
    private var scale: CGFloat = 3

    override init(frame: CGRect) {
        super.init(frame: frame)
        contentView.backgroundColor = .secondarySystemBackground
        contentView.clipsToBounds = true
        photo.clipsToBounds = true
        photo.tintColor = .secondaryLabel
        contentView.addSubview(photo)
        badge.tintColor = .white
        badge.layer.shadowOpacity = 0.7
        badge.layer.shadowRadius = 2
        contentView.addSubview(badge)
        isAccessibilityElement = true
    }

    required init?(coder: NSCoder) { fatalError("init(coder:) has not been implemented") }

    override func prepareForReuse() {
        super.prepareForReuse()
        task?.cancel()
        task = nil
        key = nil
        preparedImage = nil
        asset = nil
        photo.image = nil
    }

    func configure(asset: KeepsAsset?, configuration: KeepsConfiguration?, compact: Bool,
                   selecting: Bool, selected: Bool, scale: CGFloat,
                   preparedImage: (key: String, image: CGImage)? = nil) {
        self.preparedImage = preparedImage
        self.asset = asset
        self.configuration = configuration
        self.scale = scale
        contentView.backgroundColor = asset == nil ? .clear : .secondarySystemBackground
        accessibilityCustomActions = [UIAccessibilityCustomAction(name: "重新载入缩略图", target: self, selector: #selector(retryImage))]
        accessibilityValue = nil
        accessibilityLabel = asset?.originalFilename
        accessibilityTraits = selected ? [.button, .selected] : .button
        isAccessibilityElement = asset != nil
        photo.contentMode = compact ? .scaleAspectFill : .scaleAspectFit
        let symbol = selecting ? (selected ? "checkmark.circle.fill" : "circle") : (asset?.flagState == "picked" ? "heart.fill" : nil)
        badge.image = symbol.flatMap { UIImage(systemName: $0) }
        badge.tintColor = selected ? .systemCyan : .white
        badge.isHidden = symbol == nil
        loadImage()
    }

    override func layoutSubviews() {
        super.layoutSubviews()
        photo.frame = contentView.bounds
        badge.frame = CGRect(x: bounds.width - 25, y: bounds.height - 25, width: 21, height: 21)
        loadImage()
    }

    @objc private func retryImage() -> Bool {
        key = nil
        loadImage()
        return true
    }

    private func loadImage() {
        guard let asset, let configuration, let preview = asset.gridPreview, bounds.width > 0 else {
            task?.cancel(); key = nil
            photo.image = self.asset == nil ? nil : UIImage(systemName: "photo")
            accessibilityValue = self.asset == nil ? nil : "预览尚未生成"
            return
        }
        let role: KeepsMediaRole = asset.thumbnail == nil ? .preview : .thumbnail
        let pixels = Int(ceil(max(bounds.width, bounds.height) * scale))
        let next = PreviewCache.key(assetID: asset.id, preview: preview, configuration: configuration, role: role) + ":\(pixels)"
        if let preparedImage, preparedImage.key == next {
            task?.cancel()
            key = next
            photo.image = UIImage(cgImage: preparedImage.image)
            self.preparedImage = nil
            return
        }
        guard key != next else { return }
        task?.cancel()
        key = next
        task = Task { @MainActor [weak self] in
            do {
                let image = try await PreviewCache.cache(for: role).image(
                    assetID: asset.id, preview: preview, configuration: configuration, maxPixelSize: pixels)
                try Task.checkCancellation()
                guard let self, key == next else { return }
                photo.image = UIImage(cgImage: image)
            } catch {
                guard !Task.isCancelled, let self, key == next else { return }
                photo.image = UIImage(systemName: "photo.badge.exclamationmark")
                accessibilityValue = String(reflecting: error)
                Logger(subsystem: "com.hechuan.Keeps", category: "photo-grid")
                    .error("Thumbnail failed: \(String(reflecting: error), privacy: .public)")
            }
        }
    }
}
