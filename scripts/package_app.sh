#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_DIR="$ROOT_DIR/.build/app/PhotoAssetManager.app"
CONTENTS_DIR="$APP_DIR/Contents"
MACOS_DIR="$CONTENTS_DIR/MacOS"

cd "$ROOT_DIR"
swift build

rm -rf "$APP_DIR"
mkdir -p "$MACOS_DIR"
cp "$ROOT_DIR/.build/debug/PhotoAssetManager" "$MACOS_DIR/PhotoAssetManager"
chmod +x "$MACOS_DIR/PhotoAssetManager"
cp "$ROOT_DIR/Sources/PhotoAssetManager/Resources/Info.plist" "$CONTENTS_DIR/Info.plist"

echo "$APP_DIR"
