#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

swift build

FILES_LIST="$(mktemp)"
trap 'rm -f "$FILES_LIST"' EXIT
git ls-files -z --cached --others --exclude-standard \
  | while IFS= read -r -d '' file; do
      case "$file" in
        .gitignore|scripts/pre_merge_gate.sh)
          continue
          ;;
      esac
      [[ -f "$file" ]] || continue
      printf '%s\0' "$file"
    done > "$FILES_LIST"

if [[ -s "$FILES_LIST" ]]; then
  MATCHES="$(
    xargs -0 rg -n -i \
    "(api[_-]?key|secret|token|password|passwd|private[_-]?key|aws_access_key|aws_secret|authorization|bearer|client_secret|OPENAI_API_KEY|GITHUB_TOKEN|AIza|sk-[A-Za-z0-9]|-----BEGIN)" \
    -- < "$FILES_LIST" || true
  )"
  FILTERED_MATCHES="$(
    printf '%s\n' "$MATCHES" | rg -v \
      "Sources/PhotoAssetManager/ContentView.swift:.*Bearer token（可留空）" || true
  )"
  if [[ -n "$FILTERED_MATCHES" ]]; then
    printf '%s\n' "$FILTERED_MATCHES"
    echo "疑似敏感信息匹配，停止发布。" >&2
    exit 1
  fi
fi
