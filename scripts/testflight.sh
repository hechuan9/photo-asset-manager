#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ $# -ne 2 ]]; then
  echo "用法: $0 ios|macos archive|upload" >&2
  exit 2
fi

case "$1" in
  ios)
    PROJECT="ios/KeepsIOS.xcodeproj"
    SCHEME="KeepsIOS"
    DESTINATION="generic/platform=iOS"
    ARCHIVE="ios/.build/testflight/KeepsIOS.xcarchive"
    ;;
  macos)
    PROJECT="macos/Keeps.xcodeproj"
    SCHEME="Keeps"
    DESTINATION="generic/platform=macOS"
    ARCHIVE="macos/.build/testflight/Keeps.xcarchive"
    ;;
  *) echo "不支持的平台: $1" >&2; exit 2 ;;
esac

case "$2" in
  archive)
    mkdir -p "$(dirname "$ARCHIVE")"
    xcodebuild -project "$PROJECT" -scheme "$SCHEME" \
      -configuration Release -destination "$DESTINATION" \
      -archivePath "$ARCHIVE" -allowProvisioningUpdates archive
    ;;
  upload)
    test -f "$ARCHIVE/Info.plist" || { echo "请先归档: $0 $1 archive" >&2; exit 1; }
    xcodebuild -exportArchive -archivePath "$ARCHIVE" \
      -exportOptionsPlist scripts/ExportOptions.testflight.plist \
      -exportPath "$(dirname "$ARCHIVE")/upload" -allowProvisioningUpdates
    ;;
  *) echo "不支持的操作: $2" >&2; exit 2 ;;
esac
