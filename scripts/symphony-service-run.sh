#!/bin/zsh
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: symphony-service-run.sh <workflow> <service-name>" >&2
  exit 64
fi

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
project_root="$repo_root/elixir"

workflow="$1"
service_name="$2"
token_file="$HOME/.config/symphony/linear_api_key"
logs_root="$HOME/.local/state/symphony/$service_name"

export PATH="$HOME/.local/share/mise/shims:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin:$HOME/.local/bin:$PATH"

if [[ ! -f "$token_file" ]]; then
  echo "missing Linear API key file: $token_file" >&2
  exit 1
fi

if [[ ! -f "$workflow" ]]; then
  echo "missing workflow file: $workflow" >&2
  exit 1
fi

if ! command -v mise >/dev/null 2>&1; then
  echo "missing mise in PATH" >&2
  exit 1
fi

export LINEAR_API_KEY="$(<"$token_file")"

mkdir -p "$logs_root"
cd "$project_root"

exec mise exec -- \
  ./bin/symphony \
  --i-understand-that-this-will-be-running-without-the-usual-guardrails \
  --logs-root "$logs_root" \
  "$workflow"
