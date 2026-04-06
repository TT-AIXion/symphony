#!/bin/zsh
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
project_root="$repo_root/elixir"
service_name="symphony"
label="com.aixion.symphony.${service_name}"
launch_agents_dir="$HOME/Library/LaunchAgents"
workflow_dir="$HOME/.config/symphony/workflows"
workflow_link="$workflow_dir/${service_name}.WORKFLOW.md"
logs_root="$HOME/.local/state/symphony/${service_name}"
plist_path="$launch_agents_dir/${label}.plist"
runner="$HOME/.local/bin/symphony-service-run"
gui_domain="gui/$(id -u)"

mkdir -p "$launch_agents_dir" "$workflow_dir" "$logs_root"

if [[ ! -x "$runner" ]]; then
  echo "missing runner: $runner" >&2
  exit 1
fi

rm -f "$workflow_link"
ln -s "$project_root/WORKFLOW.md" "$workflow_link"

cat > "$plist_path" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>${label}</string>
    <key>ProgramArguments</key>
    <array>
      <string>${runner}</string>
      <string>${workflow_link}</string>
      <string>${service_name}</string>
    </array>
    <key>WorkingDirectory</key>
    <string>${project_root}</string>
    <key>KeepAlive</key>
    <true/>
    <key>RunAtLoad</key>
    <true/>
    <key>ProcessType</key>
    <string>Background</string>
    <key>StandardOutPath</key>
    <string>${logs_root}/launchd.out.log</string>
    <key>StandardErrorPath</key>
    <string>${logs_root}/launchd.err.log</string>
  </dict>
</plist>
PLIST

plutil -lint "$plist_path" >/dev/null
launchctl bootout "$gui_domain" "$plist_path" >/dev/null 2>&1 || true
launchctl bootstrap "$gui_domain" "$plist_path"
launchctl enable "$gui_domain/$label"
launchctl kickstart -k "$gui_domain/$label"

echo "Installed $label"
echo "Plist: $plist_path"
echo "Workflow: $workflow_link"
