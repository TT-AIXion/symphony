#!/bin/zsh
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
project_root="$repo_root/elixir"
token_file="$HOME/.config/symphony/linear_api_key"

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "This setup script is for macOS only." >&2
  exit 1
fi

if ! command -v mise >/dev/null 2>&1; then
  echo "mise is required. Install it first." >&2
  exit 1
fi

if ! command -v gh >/dev/null 2>&1; then
  echo "gh is required. Install it first." >&2
  exit 1
fi

if ! command -v codex >/dev/null 2>&1; then
  echo "codex is required. Install it first." >&2
  exit 1
fi

if [[ ! -f "$HOME/.codex/auth.json" ]]; then
  echo "missing Codex auth file: $HOME/.codex/auth.json" >&2
  exit 1
fi

if ! gh auth status >/dev/null 2>&1; then
  echo "gh is installed but not authenticated." >&2
  exit 1
fi

if ! gh auth status 2>/dev/null | grep -q 'Logged in to github.com account TT-AIXion'; then
  echo "gh account TT-AIXion is not configured on this machine." >&2
  exit 1
fi

mkdir -p "$HOME/.config/symphony"

if [[ -n "${LINEAR_API_KEY:-}" ]]; then
  umask 077
  printf '%s\n' "$LINEAR_API_KEY" > "$token_file"
fi

if [[ ! -f "$token_file" ]]; then
  if [[ -t 0 && -t 1 ]]; then
    echo "LINEAR_API_KEY is required to run Symphony against Linear."
    printf 'Enter LINEAR_API_KEY: ' > /dev/tty
    read -r -s linear_api_key < /dev/tty
    printf '\n' > /dev/tty

    if [[ -z "${linear_api_key:-}" ]]; then
      echo "LINEAR_API_KEY was not provided." >&2
      exit 1
    fi

    umask 077
    printf '%s\n' "$linear_api_key" > "$token_file"
  else
    echo "missing $token_file" >&2
    echo "Set LINEAR_API_KEY in the environment before running this script, or run it interactively so it can prompt you." >&2
    exit 1
  fi
fi

cd "$project_root"
mise trust
mise install
mise exec -- mix setup
mise exec -- mix build

cd "$repo_root"
./scripts/install-launch-agent.sh

echo
echo "Symphony service setup complete for repo: $repo_root"
echo "LaunchAgent: ~/Library/LaunchAgents/com.aixion.symphony.symphony.plist"
echo "Updater: ~/Library/LaunchAgents/com.aixion.symphony.symphony-updater.plist"
