#!/usr/bin/env bash
# Install a built .deb on a clean Debian/Ubuntu system (run as root, e.g. in a
# fresh container) and check the result.
#
# Usage: test-deb.sh path/to/icinga2-splunk_*.deb

set -euo pipefail

DEB="$(realpath "$1")"
export DEBIAN_FRONTEND=noninteractive

# Simulate an Icinga2 host: a CA in a directory only root can enter.
install -d -m 0700 /var/lib/icinga2/certs
printf '%s\n' '-----BEGIN CERTIFICATE-----' 'dGVzdA==' '-----END CERTIFICATE-----' \
  > /var/lib/icinga2/certs/ca.crt

apt-get update -qq

# Leftover unit from a script install must be reported.
install -d /etc/systemd/system
printf '[Service]\nExecStart=/usr/bin/python3 -m icinga2_splunk.event_stream_client\n' \
  > /etc/systemd/system/icinga2-splunk.service
install_log="$(apt-get install -qq -y --no-install-recommends "${DEB}" 2>&1)"
grep -q "overrides the packaged service" <<<"${install_log}" \
  || { echo "FAIL: no warning about overriding unit: ${install_log}" >&2; exit 1; }
rm /etc/systemd/system/icinga2-splunk.service

fail() { echo "FAIL: $*" >&2; exit 1; }

icinga2-splunk --version
icinga2-splunk-check --help >/dev/null || fail "icinga2-splunk-check --help"
python3 -c 'import icinga2_splunk.event_stream_client' || fail "import"

getent passwd icinga2-splunk >/dev/null || fail "service user missing"
perms="$(stat -c '%a %U:%G' /etc/icinga2-splunk/splunk_config.json)"
[[ "${perms}" == "640 root:icinga2-splunk" ]] || fail "config permissions are ${perms}"
[[ -f /lib/systemd/system/icinga2-splunk.service || -f /usr/lib/systemd/system/icinga2-splunk.service ]] \
  || fail "systemd unit missing"

cmp -s /var/lib/icinga2/certs/ca.crt /etc/icinga2-splunk/icinga2-ca.crt || fail "CA not copied"
grep -q '"/etc/icinga2-splunk/icinga2-ca.crt"' /etc/icinga2-splunk/splunk_config.json \
  || fail "packaged config does not use the copied CA"

# The service user can read the packaged config (connections are expected to
# fail: the example points at placeholder hosts).
out="$(su -s /bin/sh -c icinga2-splunk-check icinga2-splunk 2>&1 || true)"
grep -q "Configuration loaded" <<<"${out}" || fail "config not readable by service user: ${out}"

# Pointing at Icinga2's private CA gives a clear error instead of a traceback.
sed -i 's|/etc/icinga2-splunk/icinga2-ca.crt|/var/lib/icinga2/certs/ca.crt|' \
  /etc/icinga2-splunk/splunk_config.json
set +e
out="$(su -s /bin/sh -c icinga2-splunk icinga2-splunk 2>&1)"
rc=$?
set -e
[[ ${rc} -eq 78 ]] || fail "expected exit 78 for unreadable CA, got ${rc}: ${out}"
grep -q "not a file readable by user icinga2-splunk" <<<"${out}" || fail "unclear CA error: ${out}"
grep -q "Traceback" <<<"${out}" && fail "traceback for unreadable CA: ${out}"

apt-get purge -qq -y icinga2-splunk >/dev/null
[[ ! -e /etc/icinga2-splunk/splunk_config.json ]] || fail "config left after purge"
[[ ! -e /etc/icinga2-splunk/icinga2-ca.crt ]] || fail "CA copy left after purge"
echo "OK: $(basename "${DEB}")"
