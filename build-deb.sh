#!/usr/bin/env bash
# Build dist/jobwatch_<version>_all.deb for Linux Mint / Ubuntu / Debian.
# Users install it by double-clicking it, or: sudo apt install ./jobwatch_<version>_all.deb
set -euo pipefail

# ---- settings
VERSION="0.1.0"
MAINTAINER="kr-ramesh <kramesh.tlw@gmail.com>"   # shown in the package details
# ----

cd "$(dirname "$0")"
PKG="jobwatch_${VERSION}_all"
BUILD="$(mktemp -d)"
trap 'rm -rf "$BUILD"' EXIT
ROOT="$BUILD/$PKG"

install -Dm755 jobwatch.py                   "$ROOT/usr/bin/jobwatch"
sed -i '1s|.*|#!/usr/bin/python3|'           "$ROOT/usr/bin/jobwatch"
install -Dm644 packaging/jobwatch.desktop    "$ROOT/usr/share/applications/jobwatch.desktop"
install -Dm644 packaging/jobwatch.svg        "$ROOT/usr/share/icons/hicolor/scalable/apps/jobwatch.svg"
install -Dm644 README.md                     "$ROOT/usr/share/doc/jobwatch/README.md"

mkdir -p "$ROOT/DEBIAN"
cat > "$ROOT/DEBIAN/control" <<EOF
Package: jobwatch
Version: $VERSION
Architecture: all
Maintainer: $MAINTAINER
Depends: python3 (>= 3.8), openssh-client, libnotify-bin, python3-gi, gir1.2-gtk-3.0, gir1.2-webkit2-4.1 | gir1.2-webkit2-4.0
Section: utils
Priority: optional
Installed-Size: $(du -sk "$ROOT/usr" | cut -f1)
Description: Watch jobs on your SSH servers and get notified when they finish
 Desktop app that checks Slurm jobs (or matching processes on servers without
 Slurm) on hosts from ~/.ssh/config, shows them in one window, and sends a
 desktop notification when a job, or a set of jobs you chose, finishes.
EOF
cat > "$ROOT/DEBIAN/postinst" <<'EOF'
#!/bin/sh
set -e
command -v gtk-update-icon-cache >/dev/null && gtk-update-icon-cache -q /usr/share/icons/hicolor || true
command -v update-desktop-database >/dev/null && update-desktop-database -q /usr/share/applications || true
EOF
chmod 755 "$ROOT/DEBIAN/postinst"

mkdir -p dist
if command -v dpkg-deb >/dev/null; then
  dpkg-deb --root-owner-group --build "$ROOT" "dist/$PKG.deb" >/dev/null
else
  # No dpkg-deb (e.g. building on a non-Debian machine): assemble the ar archive by hand.
  (cd "$ROOT/DEBIAN" && tar --owner=0 --group=0 -czf "$BUILD/control.tar.gz" ./*)
  (cd "$ROOT" && tar --owner=0 --group=0 --exclude=./DEBIAN -czf "$BUILD/data.tar.gz" .)
  echo "2.0" > "$BUILD/debian-binary"
  rm -f "dist/$PKG.deb"
  (cd "$BUILD" && ar rc "$OLDPWD/dist/$PKG.deb" debian-binary control.tar.gz data.tar.gz)
fi
echo "built dist/$PKG.deb"
