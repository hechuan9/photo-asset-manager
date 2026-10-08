import Foundation
import KeepsColorCore

@MainActor func run() throws {
    let args = CommandLine.arguments
    guard args.count == 7, args[1] == "--darktable", args[3] == "--source", args[5] == "--job" else {
        throw ColorToolError("Usage: keeps-color-mcp --darktable /path/darktable-cli --source /path/input.ARW --job /private/job")
    }
    let engine = try DarktableProcess(executable: URL(fileURLWithPath: args[2]), source: URL(fileURLWithPath: args[4]), directory: URL(fileURLWithPath: args[6]))
    try JSONRPCServer(colorTools: ColorTools(engine: engine)).run()
}
do { try run() }
catch {
    FileHandle.standardError.write(Data("\(String(reflecting: error))\n".utf8))
    exit(1)
}
