#!/usr/bin/env python3
"""Check actual ImportSource memory use against a large sparse test file on macOS."""
import json
from pathlib import Path
import re
import subprocess
import tempfile

root = Path(__file__).resolve().parents[1]
with tempfile.TemporaryDirectory(prefix='keeps-import-memory-') as directory:
    work = Path(directory)
    fixture = work / 'source'
    fixture.mkdir()
    size = 2 * 1024**3
    with (fixture / 'sample.nef').open('wb') as stream:
        stream.truncate(size)
    main = work / 'main.swift'
    main.write_text('''import Foundation
let hashed = CommandLine.arguments.contains("hash")
let files = try ImportSource.scan(URL(fileURLWithPath: CommandLine.arguments[1]), calculateHashes: hashed)
precondition(files.count == 1 && files[0].size == 2147483648)
precondition((files[0].sha256 != nil) == hashed)
print("validated")
''')
    binary = work / 'probe'
    subprocess.run(['swiftc', str(root / 'Sources/PhotoAssetManager/ImportSource.swift'), str(main), '-o', str(binary)], check=True)
    measurements = {}
    for mode in ['default', 'hash']:
        args = ['/usr/bin/time', '-l', str(binary), str(fixture)]
        if mode == 'hash':
            args.append('hash')
        result = subprocess.run(args, capture_output=True, text=True, check=True)
        match = re.search(r'(\d+)\s+maximum resident set size', result.stderr)
        if not match or 'validated' not in result.stdout:
            raise RuntimeError(result.stdout + result.stderr)
        rss = int(match[1])
        if rss >= 128 * 1024**2:
            raise AssertionError(f'{mode}: peak RSS {rss} exceeds 128 MiB for a 2 GiB file')
        measurements[mode] = {'inputBytes': size, 'peakRSSBytes': rss}
    print(json.dumps(measurements, indent=2))
