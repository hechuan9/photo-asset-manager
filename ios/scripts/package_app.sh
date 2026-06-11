#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

DIST_DIR="ios/.build/enterprise"
mkdir -p "$DIST_DIR"

ARCHIVE_PATH="$DIST_DIR/PhotoAssetManagerIOS.xcarchive"
IPA_PATH="$DIST_DIR/PhotoAssetManagerIOS.ipa"

echo ">>> Archiving PhotoAssetManagerIOS for enterprise (generic iOS, Release)..."
xcodebuild \
  -project ios/PhotoAssetManagerIOS.xcodeproj \
  -scheme PhotoAssetManagerIOS \
  -destination 'generic/platform=iOS' \
  -configuration Release \
  -archivePath "$ARCHIVE_PATH" \
  archive

echo ">>> Exporting enterprise IPA..."
xcodebuild \
  -exportArchive \
  -archivePath "$ARCHIVE_PATH" \
  -exportPath "$DIST_DIR" \
  -exportOptionsPlist ios/scripts/exportOptions-enterprise.plist

echo ">>> Enterprise IPA ready: $IPA_PATH"
ls -lh "$IPA_PATH"
