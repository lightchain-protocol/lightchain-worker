#!/bin/sh
# Runs scripts/install.sh against a release directory served over file://:
# a matching checksum installs, a tampered binary installs nothing.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
fail() {
	echo "install_test: FAIL: $*" >&2
	exit 1
}

case $(uname -s) in Linux) os=linux ;; Darwin) os=darwin ;; esac
case $(uname -m) in x86_64 | amd64) arch=amd64 ;; aarch64 | arm64) arch=arm64 ;; esac
asset=lightchain-worker-$os-$arch
dl=$work/releases/download/v0.0.1
mkdir -p "$dl" "$work/releases/latest"
ln -s "$dl" "$work/releases/latest/download"
printf '#!/bin/sh\necho fake worker\n' >"$dl/$asset"
(cd "$dl" && { sha256sum "$asset" 2>/dev/null || shasum -a 256 "$asset"; } >checksums.txt)

run_installer() {
	LIGHTCHAIN_WORKER_RELEASES=file://$work/releases INSTALL_DIR=$work/bin sh "$here/install.sh" "$@"
}

run_installer >/dev/null || fail "latest did not install"
[ "$("$work/bin/lightchain-worker")" = "fake worker" ] || fail "installed binary does not run"

rm -rf "$work/bin"
LIGHTCHAIN_WORKER_VERSION=v0.0.1 run_installer >/dev/null || fail "pinned version did not install"

rm -rf "$work/bin"
echo tampered >>"$dl/$asset"
if run_installer 2>"$work/err"; then fail "tampered binary installed"; fi
grep -q "checksum mismatch" "$work/err" || fail "unexpected error: $(cat "$work/err")"
[ ! -e "$work/bin/lightchain-worker" ] || fail "tampered binary left behind"

echo "install_test: ok"
