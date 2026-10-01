#!/bin/sh
# One-line installer for the LightChain worker CLI:
#
#   curl -fsSL https://github.com/lightchain-protocol/lightchain-worker/releases/latest/download/install.sh | sudo sh
#
# It downloads the lightchain-worker binary for this OS and CPU, checks its
# SHA-256 against the release's checksums.txt, and installs it. Nothing else:
# no service, no config, no keys.
#
# Environment:
#   LIGHTCHAIN_WORKER_VERSION   release tag to install (default: latest)
#   LIGHTCHAIN_WORKER_RELEASES  releases base URL (default: the GitHub releases;
#                               a mirror works, and so does a file:// directory
#                               when curl is installed)
#   INSTALL_DIR                 where the binary goes (default: /usr/local/bin)
#
# sudo drops the caller's environment, so pass these after it:
#   curl -fsSL …/install.sh | sudo LIGHTCHAIN_WORKER_VERSION=v1.2.3 sh
set -eu

releases=${LIGHTCHAIN_WORKER_RELEASES:-https://github.com/lightchain-protocol/lightchain-worker/releases}
version=${LIGHTCHAIN_WORKER_VERSION:-latest}
install_dir=${INSTALL_DIR:-/usr/local/bin}

die() {
	echo "install: $*" >&2
	exit 1
}

case $(uname -s) in
Linux) os=linux ;;
Darwin) os=darwin ;;
*) die "no release build for $(uname -s); build from source instead" ;;
esac
case $(uname -m) in
x86_64 | amd64) arch=amd64 ;;
aarch64 | arm64) arch=arm64 ;;
*) die "no release build for $(uname -m); build from source instead" ;;
esac
asset=lightchain-worker-$os-$arch

if [ "$version" = latest ]; then
	base=$releases/latest/download
else
	base=$releases/download/$version
fi

fetch() {
	if command -v curl >/dev/null 2>&1; then
		curl -fsSL -o "$2" "$1"
	elif command -v wget >/dev/null 2>&1; then
		wget -qO "$2" "$1"
	else
		die "needs curl or wget"
	fi || die "download failed: $1"
}

# Runs in $(…): die exits the subshell, and set -e then stops the script.
sha256() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$1" | cut -d ' ' -f 1
	elif command -v shasum >/dev/null 2>&1; then
		shasum -a 256 "$1" | cut -d ' ' -f 1
	else
		die "needs sha256sum or shasum to verify the download"
	fi
}

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
trap 'exit 1' INT TERM
fetch "$base/$asset" "$tmp/$asset"
fetch "$base/checksums.txt" "$tmp/checksums.txt"

want=$(awk -v f="$asset" '$2 == f { print $1 }' "$tmp/checksums.txt")
[ -n "$want" ] || die "checksums.txt has no entry for $asset"
got=$(sha256 "$tmp/$asset")
[ "$got" = "$want" ] || die "checksum mismatch for $asset (got $got, expected $want); nothing installed"

# Copy next to the target, then rename: replacing a running binary in place fails.
chmod 0755 "$tmp/$asset"
mkdir -p "$install_dir" 2>/dev/null || true
cp "$tmp/$asset" "$install_dir/.lightchain-worker.new" 2>/dev/null ||
	die "cannot write to $install_dir; run with sudo or set INSTALL_DIR"
mv -f "$install_dir/.lightchain-worker.new" "$install_dir/lightchain-worker"
echo "installed $install_dir/lightchain-worker ($version, $os/$arch, sha256 $got)"
