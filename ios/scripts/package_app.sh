#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

DIST_DIR="ios/.build/enterprise"
mkdir -p "$DIST_DIR"

ARCHIVE_PATH="$DIST_DIR/KeepsIOS.xcarchive"
IPA_PATH="$DIST_DIR/KeepsIOS.ipa"

echo ">>> Archiving Keeps for enterprise (generic iOS, Release)..."
xcodebuild \
  -project ios/KeepsIOS.xcodeproj \
  -scheme KeepsIOS \
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

echo ">>> Keeps enterprise IPA ready: $IPA_PATH"
ls -lh "$IPA_PATH"
