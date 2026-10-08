#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESOURCES_DIR="${1:?Usage: bundle_ai_runtime.sh app-resources-directory}"
CODEX_BINARY="${KEEPS_CODEX_BINARY:-}"
DARKTABLE_APP="${KEEPS_DARKTABLE_APP:-}"

if [[ -z "$CODEX_BINARY" && -z "$DARKTABLE_APP" ]]; then
    echo "AI editing runtime not supplied; packaging without AI runtime." >&2
    exit 0
fi
if [[ -z "$CODEX_BINARY" || -z "$DARKTABLE_APP" ]]; then
    echo "KEEPS_CODEX_BINARY and KEEPS_DARKTABLE_APP must both be supplied." >&2
    exit 1
fi
if [[ "$CODEX_BINARY" != /* || "$DARKTABLE_APP" != /* ]]; then
    echo "AI runtime inputs must be absolute paths." >&2
    exit 1
fi
if [[ ! -f "$CODEX_BINARY" || ! -x "$CODEX_BINARY" || ! -x "$DARKTABLE_APP/Contents/MacOS/darktable-cli" || ! -f "$DARKTABLE_APP/Contents/Info.plist" ]]; then
    echo "AI runtime inputs must contain executable Codex and a complete darktable.app." >&2
    exit 1
fi

swift build --package-path "$ROOT_DIR/ColorTools" -c release >&2
RUNTIME_DIR="$RESOURCES_DIR/AIEditing"
if [[ -e "$RUNTIME_DIR" ]]; then
    echo "AIEditing destination already exists; rebuild the app into a clean destination." >&2
    exit 1
fi
mkdir -p "$RUNTIME_DIR"
cp "$CODEX_BINARY" "$RUNTIME_DIR/codex"
cp "$ROOT_DIR/ColorTools/.build/release/keeps-color-mcp" "$RUNTIME_DIR/keeps-color-mcp"
cp "$ROOT_DIR/ColorTools/Skills/keeps-color/SKILL.md" "$RUNTIME_DIR/SKILL.md"
/usr/bin/ditto "$DARKTABLE_APP" "$RUNTIME_DIR/darktable.app"
chmod +x "$RUNTIME_DIR/codex" "$RUNTIME_DIR/keeps-color-mcp"

# Preserve the full dependency layout, then sign nested code before its containing bundles.
python3 - "$RUNTIME_DIR" "$CODEX_BINARY" "$DARKTABLE_APP" <<'PY'
import hashlib
import json
from pathlib import Path
import plistlib
import subprocess
import sys

runtime, codex_source, darktable_source = map(Path, sys.argv[1:])
macho_magic = {bytes.fromhex(value) for value in (
    "feedface", "cefaedfe", "feedfacf", "cffaedfe", "cafebabe", "bebafeca", "cafebabf", "bfbafeca"
)}

def digest(path):
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

source_hashes = {
    "codex": digest(codex_source),
    "darktable-cli": digest(darktable_source / "Contents/MacOS/darktable-cli"),
}
for path in sorted(runtime.rglob("*"), key=lambda item: len(item.parts), reverse=True):
    if not path.is_file() or path.is_symlink():
        continue
    with path.open("rb") as stream:
        is_code = stream.read(4) in macho_magic
    if is_code:
        subprocess.run(["codesign", "--force", "--sign", "-", str(path)], check=True)

bundles = [path for path in runtime.rglob("*") if path.is_dir() and not path.is_symlink()
           and path.suffix in (".app", ".framework", ".xpc", ".bundle")]
for path in sorted(bundles, key=lambda item: len(item.parts), reverse=True):
    subprocess.run(["codesign", "--force", "--sign", "-", str(path)], check=True)
subprocess.run(["codesign", "--verify", "--deep", "--strict", str(runtime / "darktable.app")], check=True)
for name in ("codex", "keeps-color-mcp"):
    subprocess.run(["codesign", "--verify", "--strict", str(runtime / name)], check=True)

with (runtime / "darktable.app/Contents/Info.plist").open("rb") as stream:
    darktable_info = plistlib.load(stream)
codex_version = subprocess.run([str(runtime / "codex"), "--version"], check=True,
                               capture_output=True, text=True, timeout=30).stdout.strip()
manifest = {
    "schemaVersion": 1,
    "distribution": "local-debug",
    "codex": {"version": codex_version, "source": str(codex_source), "sourceSHA256": source_hashes["codex"]},
    "darktable": {"version": darktable_info.get("CFBundleShortVersionString"),
                  "source": str(darktable_source), "sourceCLISHA256": source_hashes["darktable-cli"]},
    "files": {str(path.relative_to(runtime)): digest(path) for path in sorted(runtime.rglob("*"))
              if path.is_file() and not path.is_symlink()},
    "distributionReview": "Local debug assembly only; redistribution licensing and notarization remain separate release requirements.",
}
(runtime / "runtime-manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
PY
