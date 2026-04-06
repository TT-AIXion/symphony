#!/usr/bin/env bash
set -euo pipefail

required_user="${SYMPHONY_GH_REQUIRED_USER:-TT-AIXion}"

if ! command -v gh >/dev/null 2>&1; then
  echo "gh CLI is required" >&2
  exit 127
fi

if [ "$#" -lt 1 ]; then
  echo "usage: $0 <command> [args...]" >&2
  exit 64
fi

current_user="$(
  gh auth status 2>/dev/null |
    awk '/Active account:/ { print $NF; exit }'
)"

restore_user="$current_user"
switched=0

restore() {
  if [ "$switched" -eq 1 ] && [ -n "${restore_user:-}" ] && [ "$restore_user" != "$required_user" ]; then
    gh auth switch -u "$restore_user" >/dev/null 2>&1 || true
  fi
}

trap restore EXIT

if [ -z "${current_user:-}" ]; then
  echo "unable to determine active gh account" >&2
  exit 1
fi

if [ "$current_user" != "$required_user" ]; then
  gh auth switch -u "$required_user" >/dev/null
  switched=1
fi

exec "$@"
