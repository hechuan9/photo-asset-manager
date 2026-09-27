#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

command -v gitleaks >/dev/null 2>&1 || {
  echo "缺少 gitleaks，请先执行 brew install gitleaks。" >&2
  exit 1
}

swift build

SCAN_DIR="$(mktemp -d)"
trap 'rm -rf "$SCAN_DIR"' EXIT
# 扫描当前工作区中的受版本管理及未忽略文件，避免构建产物与已删除文件。
git ls-files -z --cached --others --exclude-standard \
  | while IFS= read -r -d '' file; do
      [[ -f "$file" ]] || continue
      mkdir -p "$SCAN_DIR/$(dirname "$file")"
      cp "$file" "$SCAN_DIR/$file"
    done

gitleaks dir "$SCAN_DIR" --redact --no-banner
