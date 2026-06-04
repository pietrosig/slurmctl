#!/bin/sh
set -eu

repo="${SLURMCTL_REPO:-pietrosig/slurmctl}"
asset="${SLURMCTL_ASSET:-slurmctl}"
url="${SLURMCTL_INSTALL_URL:-https://github.com/$repo/releases/latest/download/$asset}"

if [ "${BIN_DIR:-}" ]; then
    bin_dir="$BIN_DIR"
elif [ "${PREFIX:-}" ]; then
    bin_dir="$PREFIX/bin"
else
    bin_dir="$HOME/.local/bin"
fi
target="$bin_dir/slurmctl"

download() {
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$1" -o "$2"
        return
    fi
    if command -v wget >/dev/null 2>&1; then
        wget -qO "$2" "$1"
        return
    fi
    echo "slurmctl install: curl or wget is required" >&2
    exit 127
}

python_ok=false
for py in "${SLURMCTL_PYTHON:-}" python3.13 python3.12 python3.11 python3; do
    [ -n "$py" ] || continue
    if "$py" -c "import sys; raise SystemExit(sys.version_info < (3, 11))" >/dev/null 2>&1; then
        python_ok=true
        break
    fi
done

if [ "$python_ok" != true ]; then
    echo "slurmctl install: Python 3.11+ is required" >&2
    exit 127
fi

tmp="${TMPDIR:-/tmp}/slurmctl-install.$$"
trap 'rm -f "$tmp"' EXIT INT HUP TERM

download "$url" "$tmp"
chmod +x "$tmp"

if ! "$py" "$tmp" --help >/dev/null 2>&1; then
    echo "slurmctl install: downloaded file did not pass validation" >&2
    exit 1
fi

mkdir -p "$bin_dir"
mv "$tmp" "$target"
chmod +x "$target"
trap - EXIT INT HUP TERM

echo "installed slurmctl to $target"
case ":$PATH:" in
    *:"$bin_dir":*) ;;
    *)
        echo "warning: $bin_dir is not on PATH" >&2
        echo "add this to your shell profile:" >&2
        echo "  export PATH=\"$bin_dir:\$PATH\"" >&2
        ;;
esac
