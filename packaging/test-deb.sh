#!/usr/bin/env bash
# Install a built .deb on a clean Debian/Ubuntu system (run as root, e.g. in a
# fresh container) and check the result.
#
# Usage: test-deb.sh path/to/icinga2-splunk_*.deb

set -euo pipefail

DEB="$(realpath "$1")"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -qq -y --no-install-recommends "${DEB}" >/dev/null

fail() { echo "FAIL: $*" >&2; exit 1; }

icinga2-splunk --version
icinga2-splunk-check --help >/dev/null || fail "icinga2-splunk-check --help"
python3 -c 'import icinga2_splunk.event_stream_client' || fail "import"

getent passwd icinga2-splunk >/dev/null || fail "service user missing"
perms="$(stat -c '%a %U:%G' /etc/icinga2-splunk/splunk_config.json)"
[[ "${perms}" == "640 root:icinga2-splunk" ]] || fail "config permissions are ${perms}"
[[ -f /lib/systemd/system/icinga2-splunk.service || -f /usr/lib/systemd/system/icinga2-splunk.service ]] \
  || fail "systemd unit missing"

# The service user can read the packaged config (connections are expected to
# fail: the example points at placeholder hosts).
out="$(su -s /bin/sh -c icinga2-splunk-check icinga2-splunk 2>&1 || true)"
grep -q "Configuration loaded" <<<"${out}" || fail "config not readable by service user: ${out}"

apt-get purge -qq -y icinga2-splunk >/dev/null
[[ ! -e /etc/icinga2-splunk/splunk_config.json ]] || fail "config left after purge"
echo "OK: $(basename "${DEB}")"
