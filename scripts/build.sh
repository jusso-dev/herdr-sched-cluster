#!/bin/sh
# Herdr runs this once, at `herdr plugin install` and `herdr plugin update`,
# in the fresh checkout. It leaves bin/hsched behind and nothing else.
#
# Two ways to get the binary:
#   source    `go build`, when a Go toolchain is on PATH. Go 1.21 or later
#             fetches the exact toolchain go.mod asks for by itself.
#   prebuilt  the matching release asset from GitHub, checked against the
#             release's checksums.txt, when there is no Go. This is what lets
#             a Mac with no Go install the plugin.
#
# HSCHED_BUILD=source or HSCHED_BUILD=prebuilt forces one; the default tries
# source first because it is always exactly the commit Herdr checked out.
set -eu
cd "$(dirname "$0")/.."

repo="jusso-dev/herdr-sched-cluster"
version=$(sed -n 's/^version = "\(.*\)"$/\1/p' herdr-plugin.toml | head -n 1)
mode="${HSCHED_BUILD:-auto}"

from_source() {
  echo "sched: building hsched $version from source with $(go version)"
  CGO_ENABLED=0 go build -trimpath -o ./bin/hsched ./cmd/hsched
}

from_release() {
  os=$(uname -s | tr '[:upper:]' '[:lower:]')
  case "$(uname -m)" in
    x86_64 | amd64) arch=amd64 ;;
    arm64 | aarch64) arch=arm64 ;;
    *) echo "sched: no prebuilt hsched for $(uname -m); install Go and reinstall" >&2; return 1 ;;
  esac
  case "$os" in
    darwin | linux) ;;
    *) echo "sched: no prebuilt hsched for $os" >&2; return 1 ;;
  esac
  asset="hsched_${version}_${os}_${arch}.tar.gz"
  base="https://github.com/$repo/releases/download/v$version"
  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' EXIT
  echo "sched: downloading $asset from the v$version release"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL -o "$tmp/$asset" "$base/$asset"
    curl -fsSL -o "$tmp/checksums.txt" "$base/checksums.txt"
  elif command -v wget >/dev/null 2>&1; then
    wget -q -O "$tmp/$asset" "$base/$asset"
    wget -q -O "$tmp/checksums.txt" "$base/checksums.txt"
  else
    echo "sched: neither curl nor wget is installed" >&2
    return 1
  fi
  want=$(awk -v f="$asset" '$2 == f { print $1 }' "$tmp/checksums.txt")
  if [ -z "$want" ]; then
    echo "sched: checksums.txt has no entry for $asset" >&2
    return 1
  fi
  if command -v sha256sum >/dev/null 2>&1; then
    got=$(sha256sum "$tmp/$asset" | awk '{ print $1 }')
  else
    got=$(shasum -a 256 "$tmp/$asset" | awk '{ print $1 }')
  fi
  if [ "$got" != "$want" ]; then
    echo "sched: checksum mismatch for $asset; refusing it" >&2
    return 1
  fi
  mkdir -p bin
  tar -xzf "$tmp/$asset" -C "$tmp" hsched
  mv -f "$tmp/hsched" ./bin/hsched
  chmod 0755 ./bin/hsched
  echo "sched: installed prebuilt hsched $version for $os/$arch"
}

case "$mode" in
  source) from_source ;;
  prebuilt) from_release ;;
  auto)
    if command -v go >/dev/null 2>&1 && from_source; then
      :
    else
      command -v go >/dev/null 2>&1 && echo "sched: go build failed; trying the prebuilt release instead" >&2
      from_release || {
        echo "sched: could not build or download hsched. Install Go (https://go.dev/dl/) or check that release v$version exists." >&2
        exit 1
      }
    fi
    ;;
  *) echo "sched: HSCHED_BUILD must be source or prebuilt, not $mode" >&2; exit 2 ;;
esac
./bin/hsched version
