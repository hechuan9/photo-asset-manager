import SwiftUI
import UIKit
import KeepsAPI

struct IOSZoomablePhoto: UIViewControllerRepresentable {
    let asset: KeepsAsset
    let configuration: KeepsConfiguration?
    var toggleControls: () -> Void
    var close: () -> Void

    func makeUIViewController(context: Context) -> PhotoZoomController {
        PhotoZoomController(asset: asset, configuration: configuration, toggleControls: toggleControls, close: close)
    }

    func updateUIViewController(_ controller: PhotoZoomController, context: Context) {
        controller.update(asset: asset, configuration: configuration)
        controller.toggleControls = toggleControls
        controller.close = close
    }
}

final class PhotoZoomController: UIViewController, UIScrollViewDelegate, UIGestureRecognizerDelegate {
    private let scroll = UIScrollView()
    private let host: UIHostingController<IOSPreviewImage>
    private var asset: KeepsAsset
    private var configuration: KeepsConfiguration?
    private var viewport = CGSize.zero
    var toggleControls: () -> Void
    var close: () -> Void

    init(asset: KeepsAsset, configuration: KeepsConfiguration?, toggleControls: @escaping () -> Void, close: @escaping () -> Void) {
        self.asset = asset
        self.configuration = configuration
        self.toggleControls = toggleControls
        self.close = close
        host = UIHostingController(rootView: IOSPreviewImage(asset: asset, configuration: configuration))
        super.init(nibName: nil, bundle: nil)
    }

    required init?(coder: NSCoder) { fatalError("init(coder:) is unavailable") }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .black
        scroll.delegate = self
        scroll.minimumZoomScale = 1
        scroll.maximumZoomScale = 5
        scroll.showsHorizontalScrollIndicator = false
        scroll.showsVerticalScrollIndicator = false
        scroll.contentInsetAdjustmentBehavior = .never
        view.addSubview(scroll)
        addChild(host)
        host.view.backgroundColor = .clear
        scroll.addSubview(host.view)
        host.didMove(toParent: self)
        let doubleTap = UITapGestureRecognizer(target: self, action: #selector(zoom(_:)))
        doubleTap.numberOfTapsRequired = 2
        let singleTap = UITapGestureRecognizer(target: self, action: #selector(toggle))
        singleTap.require(toFail: doubleTap)
        singleTap.delegate = self
        scroll.addGestureRecognizer(doubleTap)
        scroll.addGestureRecognizer(singleTap)
        let down = UISwipeGestureRecognizer(target: self, action: #selector(swipeDown))
        down.direction = .down
        down.delegate = self
        scroll.addGestureRecognizer(down)
    }

    override func viewDidLayoutSubviews() {
        super.viewDidLayoutSubviews()
        scroll.frame = view.bounds
        guard viewport != view.bounds.size else { return }
        viewport = view.bounds.size
        scroll.setZoomScale(1, animated: false)
        host.view.frame = view.bounds
        scroll.contentSize = viewport
    }

    func update(asset: KeepsAsset, configuration: KeepsConfiguration?) {
        guard self.asset != asset || self.configuration != configuration else { return }
        self.asset = asset
        self.configuration = configuration
        host.rootView = IOSPreviewImage(asset: asset, configuration: configuration)
    }

    func viewForZooming(in scrollView: UIScrollView) -> UIView? { host.view }

    @objc private func toggle() { toggleControls() }
    @objc private func swipeDown() { if scroll.zoomScale <= 1.01 { close() } }
    @objc private func zoom(_ gesture: UITapGestureRecognizer) {
        if scroll.zoomScale > 1.01 {
            scroll.setZoomScale(1, animated: true)
        } else {
            let point = gesture.location(in: host.view)
            let size = CGSize(width: scroll.bounds.width / 2.5, height: scroll.bounds.height / 2.5)
            scroll.zoom(to: CGRect(x: point.x - size.width / 2, y: point.y - size.height / 2,
                                   width: size.width, height: size.height), animated: true)
        }
    }

    func gestureRecognizer(_ gestureRecognizer: UIGestureRecognizer, shouldReceive touch: UITouch) -> Bool {
        // 预览加载失败时，保留 SwiftUI 重试按钮的点击。
        !(touch.view is UIControl)
    }
}
