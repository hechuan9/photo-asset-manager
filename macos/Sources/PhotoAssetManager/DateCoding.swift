import Foundation

enum DateCoding {
    private static func makeFormatter() -> ISO8601DateFormatter {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }

    static func encode(_ date: Date) -> String {
        makeFormatter().string(from: date)
    }

    static func decode(_ value: String?) -> Date? {
        guard let value, !value.isEmpty else { return nil }
        return makeFormatter().date(from: value)
    }
}
