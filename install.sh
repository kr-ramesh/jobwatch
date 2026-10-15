#!/usr/bin/env bash
# Install jobwatch for the current user only (no sudo), on any Linux desktop.
#   ./install.sh             install into ~/.local
#   ./install.sh --uninstall remove it again
# On Mint/Ubuntu/Debian the .deb (see build-deb.sh) is the easier route.
set -euo pipefail
cd "$(dirname "$0")"

BIN="$HOME/.local/bin/jobwatch"
APP="$HOME/.local/share/applications/jobwatch.desktop"
ICON="$HOME/.local/share/icons/hicolor/scalable/apps/jobwatch.svg"

if [[ "${1:-}" == "--uninstall" ]]; then
  "$BIN" --quit 2>/dev/null || true
  rm -f "$BIN" "$APP" "$ICON" "${XDG_CONFIG_HOME:-$HOME/.config}/autostart/jobwatch.desktop"
  echo "jobwatch removed (your settings in ~/.config/jobwatch were kept)"
  exit 0
fi

install -Dm755 jobwatch.py                "$BIN"
install -Dm644 packaging/jobwatch.desktop "$APP"
install -Dm644 packaging/jobwatch.svg     "$ICON"
sed -i "s|^Exec=jobwatch|Exec=$BIN|" "$APP"
command -v update-desktop-database >/dev/null && update-desktop-database -q "$HOME/.local/share/applications" || true

echo "installed: find 'jobwatch' in your app menu, or run $BIN"
python3 -c 'import gi; gi.require_version("WebKit2", "4.1")' 2>/dev/null \
  || python3 -c 'import gi; gi.require_version("WebKit2", "4.0")' 2>/dev/null \
  || echo "note: for a native window, install: sudo apt install python3-gi gir1.2-webkit2-4.1  (otherwise it opens in Chrome/your browser)"
command -v notify-send >/dev/null || echo "note: for desktop notifications, install: sudo apt install libnotify-bin"
