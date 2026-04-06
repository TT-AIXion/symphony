#!/bin/zsh
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
project_root="$repo_root/elixir"
service_label="com.aixion.symphony.symphony"
gui_domain="gui/$(id -u)"
logs_root="$HOME/.local/state/symphony/symphony-updater"
log_file="$logs_root/update.log"

mkdir -p "$logs_root"
exec >>"$log_file" 2>&1
export PATH="$HOME/.local/share/mise/shims:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin:$HOME/.local/bin:$PATH"
export SYMPHONY_HOME="$repo_root"

timestamp() {
  date '+%Y-%m-%d %H:%M:%S'
}

echo "[$(timestamp)] updater start"

if ! command -v mise >/dev/null 2>&1; then
  echo "[$(timestamp)] missing mise in PATH"
  exit 1
fi

if ! command -v codex >/dev/null 2>&1; then
  echo "[$(timestamp)] missing codex in PATH"
  exit 1
fi

cd "$repo_root"
current_branch="$(git branch --show-current)"

if [[ -z "$current_branch" ]]; then
  echo "[$(timestamp)] unable to determine current branch"
  exit 1
fi

echo "[$(timestamp)] pulling origin/$current_branch"

if ./scripts/with-gh-account.sh git pull --ff-only origin "$current_branch"; then
  echo "[$(timestamp)] fast-forward pull succeeded"
else
  echo "[$(timestamp)] fast-forward pull failed; invoking Codex pull skill"

  codex exec \
    -C "$repo_root" \
    -a never \
    -s danger-full-access \
    --no-alt-screen \
    "Use the pull skill to update the current branch from origin/$current_branch. Resolve conflicts if needed, preserve local repo policy, and stop only after the branch is up to date."

  echo "[$(timestamp)] Codex pull flow completed"
fi

echo "[$(timestamp)] rebuilding escript"
cd "$project_root"
mise trust
mise install
mise exec -- mix build

echo "[$(timestamp)] restarting $service_label"
launchctl kickstart -k "$gui_domain/$service_label"
echo "[$(timestamp)] updater complete"
