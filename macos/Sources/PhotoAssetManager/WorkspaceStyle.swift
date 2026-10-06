import SwiftUI

// Neutral surfaces keep the surrounding interface from competing with the photos.
enum WorkspaceStyle {
    static let canvas = Color(white: 0.115)
    static let panel = Color(white: 0.17)
    static let field = Color(white: 0.15)
    static let tile = Color(white: 0.12)
    static let selection = Color(white: 0.115)
    static let text = Color(white: 0.88)
    static let accent = Color(red: 0.40, green: 0.68, blue: 1.0)
}

struct WorkspaceErrorView: View {
    var title: String
    var details: String
    @State private var showsDetails = false

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(title).foregroundStyle(.secondary)
            Button("查看错误详情") { showsDetails = true }.buttonStyle(.link)
        }
        .font(.caption)
        .sheet(isPresented: $showsDetails) {
            VStack(alignment: .leading, spacing: 16) {
                Text(title).font(.headline)
                ScrollView {
                    Text(details).font(.system(size: 12, design: .monospaced))
                        .textSelection(.enabled).frame(maxWidth: .infinity, alignment: .leading)
                }
                HStack { Spacer(); Button("关闭") { showsDetails = false }.keyboardShortcut(.cancelAction) }
            }.padding(24).frame(width: 560, height: 380)
        }
    }
}

struct OverlayScrollerConfiguration: NSViewRepresentable {
    func makeNSView(context: Context) -> ConfigurationView { ConfigurationView() }
    func updateNSView(_ view: ConfigurationView, context: Context) { view.configure() }

    final class ConfigurationView: NSView {
        override func viewDidMoveToWindow() {
            super.viewDidMoveToWindow()
            configure()
        }

        func configure() {
            guard let scroll = enclosingScrollView else { return }
            scroll.scrollerStyle = .overlay
            scroll.autohidesScrollers = true
        }
    }
}
