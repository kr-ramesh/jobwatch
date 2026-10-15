#!/usr/bin/env bash
# Install or update jobwatch with one command:
#   curl -fsSL https://raw.githubusercontent.com/kr-ramesh/jobwatch/main/get.sh | bash
# Mint/Ubuntu/Debian: installs the latest .deb with apt (asks for your password).
# Other Linux: installs for your user only, into ~/.local. macOS: installs to ~/.local/bin.
set -euo pipefail

REPO="kr-ramesh/jobwatch"

say() { printf '\033[1m%s\033[0m\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }
command -v curl >/dev/null || die "curl is needed (sudo apt install curl)"
command -v python3 >/dev/null || die "python3 is needed"

say "Finding the latest jobwatch release…"
RELEASE="$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest")" || die "could not reach GitHub"
read -r TAG DEB_URL < <(python3 -c '
import json, sys
r = json.load(sys.stdin)
debs = [a["browser_download_url"] for a in r.get("assets", []) if a["name"].endswith("_all.deb")]
print(r["tag_name"], debs[0] if debs else "-")' <<<"$RELEASE")

# Readable by apt's unprivileged downloader, unlike a mktemp -d folder.
TMP="/tmp/jobwatch-install-$$"
mkdir -p "$TMP" && chmod 755 "$TMP"
trap 'rm -rf "$TMP"' EXIT

was_running=false
if command -v jobwatch >/dev/null && jobwatch --quit 2>/dev/null | grep -q stopped; then
  was_running=true
fi

if [[ "$(uname)" == "Linux" ]] && command -v apt-get >/dev/null && [[ "$DEB_URL" != "-" ]]; then
  say "Downloading jobwatch $TAG…"
  curl -fsSL -o "$TMP/jobwatch.deb" "$DEB_URL"
  chmod 644 "$TMP/jobwatch.deb"
  say "Installing (you may be asked for your password)…"
  sudo apt-get install -y --reinstall "$TMP/jobwatch.deb"
  BIN=jobwatch
elif [[ "$(uname)" == "Linux" ]]; then
  say "Downloading jobwatch $TAG…"
  curl -fsSL "https://github.com/$REPO/archive/refs/tags/$TAG.tar.gz" | tar xz -C "$TMP" --strip-components=1
  bash "$TMP/install.sh"
  BIN="$HOME/.local/bin/jobwatch"
else
  say "Downloading jobwatch $TAG…"
  mkdir -p "$HOME/.local/bin"
  curl -fsSL -o "$HOME/.local/bin/jobwatch" "https://raw.githubusercontent.com/$REPO/$TAG/jobwatch.py"
  chmod 755 "$HOME/.local/bin/jobwatch"
  BIN="$HOME/.local/bin/jobwatch"
fi

if $was_running; then
  "$BIN" --no-window >/dev/null && say "Restarted the background checker on the new version."
fi
echo
say "jobwatch $TAG is installed."
if [[ "$(uname)" == "Linux" ]]; then
  echo "Open it from your app menu (search \"jobwatch\"), or run: $BIN"
else
  echo "Run it with: $BIN"
fi
echo "To update later, run the same install command again."
