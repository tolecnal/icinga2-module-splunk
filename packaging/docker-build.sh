#!/usr/bin/env bash
# Build and test the .deb for one or more releases using Docker.
#
# Usage: packaging/docker-build.sh [IMAGE...]
#   default images: debian:12 debian:13 ubuntu:22.04 ubuntu:24.04 ubuntu:26.04
# Packages are written to dist/deb/.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
IMAGES=("$@")
[[ ${#IMAGES[@]} -gt 0 ]] || IMAGES=(debian:12 debian:13 ubuntu:22.04 ubuntu:24.04 ubuntu:26.04)
mkdir -p dist/deb

# Only the files git knows about (tracked or new, not ignored) go into the build.
SRC_TAR="$(mktemp)"
trap 'rm -f "${SRC_TAR}"' EXIT
git ls-files -z --cached --others --exclude-standard | tar --null -T - -cf "${SRC_TAR}"

for image in "${IMAGES[@]}"; do
  echo "=== ${image}: build"
  docker run --rm -i -v "${PWD}/dist/deb:/out" "${image}" bash -c \
    'mkdir /src && tar -xf - -C /src && /src/packaging/build-deb.sh /out && chown -R '"$(id -u):$(id -g)"' /out' \
    < "${SRC_TAR}"

  # shellcheck disable=SC2016
  release="$(docker run --rm "${image}" bash -c '. /etc/os-release; [ "$ID" = debian ] && echo "deb${VERSION_ID%%.*}" || echo "ubuntu${VERSION_ID}"')"
  deb="$(find dist/deb -name "icinga2-splunk_*~${release}_all.deb" | sort | tail -1)"

  echo "=== ${image}: install test"
  docker run --rm -v "${PWD}/dist/deb:/out:ro" -v "${PWD}/packaging:/packaging:ro" "${image}" \
    /packaging/test-deb.sh "/out/$(basename "${deb}")"
done
