#!/usr/bin/env bash
# Build the icinga2-splunk .deb inside a Debian/Ubuntu environment (run as
# root, e.g. in a container of the target release).
#
# Usage: build-deb.sh [OUTDIR]       (default OUTDIR: ./dist/deb)
#
# The package version gets a release suffix (e.g. 2.0.0~deb12, 2.0.0~ubuntu22.04)
# so the builds for different releases can live side by side in one APT repo.

set -euo pipefail

SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTDIR="$(mkdir -p "${1:-${SRC_DIR}/dist/deb}" && cd "${1:-${SRC_DIR}/dist/deb}" && pwd)"

# shellcheck source=/dev/null
. /etc/os-release
case "${ID}" in
  debian) SUFFIX="~deb${VERSION_ID%%.*}" ;;
  ubuntu) SUFFIX="~ubuntu${VERSION_ID}" ;;
  *) echo "Unsupported distribution: ${ID}" >&2; exit 1 ;;
esac

export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -qq -y --no-install-recommends \
  build-essential debhelper dh-python python3 python3-pytest python3-requests lintian \
  >/dev/null

# Build from a copy so the release-specific changelog edit doesn't touch SRC_DIR.
BUILD_DIR="$(mktemp -d)"
trap 'rm -rf "${BUILD_DIR}"' EXIT
cp -a "${SRC_DIR}/." "${BUILD_DIR}/icinga2-splunk"
cd "${BUILD_DIR}/icinga2-splunk"
rm -rf .venv dist build

UPSTREAM="$(dpkg-parsechangelog -S Version)"
PY_VERSION="$(sed -n 's/^__version__ = "\(.*\)"/\1/p' src/icinga2_splunk/__init__.py)"
if [[ "${UPSTREAM}" != "${PY_VERSION}" ]]; then
  echo "debian/changelog (${UPSTREAM}) and __version__ (${PY_VERSION}) differ" >&2
  exit 1
fi
sed -i "1s/^icinga2-splunk (${UPSTREAM}) [a-z-]*;/icinga2-splunk (${UPSTREAM}${SUFFIX}) ${VERSION_CODENAME};/" \
  debian/changelog

dpkg-buildpackage -us -uc -b

DEB="$(ls "${BUILD_DIR}"/icinga2-splunk_*_all.deb)"
lintian --fail-on error "${DEB}" || exit 1
cp "${DEB}" "${OUTDIR}/"
echo "Built ${OUTDIR}/$(basename "${DEB}")"
