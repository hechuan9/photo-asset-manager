#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

"$ROOT_DIR/macos/scripts/pre_merge_gate.sh"

FILES_LIST="$(mktemp)"
trap 'rm -f "$FILES_LIST"' EXIT

cd "$ROOT_DIR"
git ls-files -z --cached --others --exclude-standard \
  | while IFS= read -r -d '' file; do
      case "$file" in
        .gitignore|scripts/pre_merge_gate.sh|macos/scripts/pre_merge_gate.sh)
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
    printf '%s\n' "$MATCHES" | rg -v -i \
      "(README.md:.*(Secrets Manager|secret version|tfstate|Aurora secret|connection secret|VPC endpoints|interface endpoint|gateway endpoint)|docs/ARCHITECTURE.md:.*(Secrets Manager|secret ARN|connection secret|VPC endpoints|interface endpoint|gateway endpoint)|control_plane/README.md:.*(DATABASE_CONNECTION_SECRET_ARN|connection secret)|control_plane/control_plane/app.py:.*(DATABASE_CONNECTION_SECRET_ARN|_database_url_from_connection_secret|boto3\\.client\\(\"secretsmanager\"\\)|SecretString|secret_string|payload\\['password'\\]|\"password\"|Aurora connection secret missing fields)|control_plane/tests/test_api.py:.*(_database_url_from_connection_secret|test_database_url_can_be_built_from_aurora_connection_secret|FakeSecretsManager|get_secret_value|SecretId|SecretString|p@ss word|p@ss/word|secretsmanager:secret|service_name == \"secretsmanager\")|infra/terraform/README.md:.*(Secrets Manager|secret version|secret recovery|tfstate|connection secret|VPC endpoints|interface endpoint|gateway endpoint)|infra/terraform/.*\\.tf:.*(Secrets Manager secret|Secrets Manager ARN|aws_secretsmanager_|aurora_connection_secret|AuroraConnectionSecret|secretsmanager:DescribeSecret|secretsmanager:GetSecretValue|service_name.*secretsmanager|resource \"aws_vpc_endpoint\" \"secretsmanager\"|secret_string = jsonencode|resource \"random_password\" \"aurora_master_password\"|random_password\\.aurora_master_password|master_password.*random_password|DATABASE_CONNECTION_SECRET_ARN|authorization_type = \"AWS_IAM\"|allowed_headers = .*Authorization))" || true
  )"
  if [[ -n "$FILTERED_MATCHES" ]]; then
    printf '%s\n' "$FILTERED_MATCHES"
    echo "疑似敏感信息匹配，停止发布。" >&2
    exit 1
  fi
fi

if command -v terraform >/dev/null 2>&1; then
  terraform -chdir="$ROOT_DIR/infra/terraform" fmt -check
fi
