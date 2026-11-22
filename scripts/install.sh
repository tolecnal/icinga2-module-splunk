#!/usr/bin/env bash
# Install the Icinga2 -> Splunk integration into a virtualenv and set up the
# systemd service. Re-running the script upgrades an existing installation.
#
# Environment overrides:
#   PREFIX        virtualenv location        (default: /opt/icinga2-splunk)
#   PYTHON        Python interpreter (>=3.10) (default: python3)
#   SERVICE_USER  user running the service    (default: nagios; RHEL uses icinga)
#   CONFIG_FILE   configuration path          (default: /etc/icinga2/splunk_config.json)

set -euo pipefail

PREFIX="${PREFIX:-/opt/icinga2-splunk}"
PYTHON="${PYTHON:-python3}"
SERVICE_USER="${SERVICE_USER:-nagios}"
CONFIG_FILE="${CONFIG_FILE:-/etc/icinga2/splunk_config.json}"
UNIT_FILE=/etc/systemd/system/icinga2-splunk.service

SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "=== Icinga2 to Splunk Integration Installer ==="

if [[ "${EUID}" -ne 0 ]]; then
  echo "Please run as root or with sudo" >&2
  exit 1
fi

if ! id "${SERVICE_USER}" >/dev/null 2>&1; then
  echo "Service user '${SERVICE_USER}' does not exist; set SERVICE_USER" >&2
  exit 1
fi

if ! "${PYTHON}" -c 'import sys; sys.exit(sys.version_info < (3, 10))'; then
  echo "${PYTHON} is older than Python 3.10; set PYTHON to a newer interpreter" >&2
  exit 1
fi

echo "Installing into virtualenv ${PREFIX}..."
"${PYTHON}" -m venv "${PREFIX}"
"${PREFIX}/bin/pip" install --quiet --upgrade pip
"${PREFIX}/bin/pip" install --quiet --upgrade "${SRC_DIR}"

if [[ ! -f "${CONFIG_FILE}" ]]; then
  echo "Creating ${CONFIG_FILE}..."
  install -d -m 0755 "$(dirname "${CONFIG_FILE}")"
  # The file holds credentials: readable by root and the service group only.
  install -m 0640 -o root -g "${SERVICE_USER}" \
    "${SRC_DIR}/config.json.example" "${CONFIG_FILE}"
  echo "Please edit ${CONFIG_FILE} with your settings"
else
  echo "${CONFIG_FILE} already exists, leaving it untouched"
  if [[ -n "$(find "${CONFIG_FILE}" -perm -o=r)" ]]; then
    echo "WARNING: ${CONFIG_FILE} is world-readable but contains secrets." >&2
    echo "         Fix with: chown root:${SERVICE_USER} ${CONFIG_FILE} && chmod 0640 ${CONFIG_FILE}" >&2
  fi
fi

echo "Installing systemd unit ${UNIT_FILE}..."
sed -e "s|^User=.*|User=${SERVICE_USER}|" \
    -e "s|^Group=.*|Group=${SERVICE_USER}|" \
    -e "s|/opt/icinga2-splunk|${PREFIX}|g" \
    -e "s|/etc/icinga2/splunk_config.json|${CONFIG_FILE}|g" \
    "${SRC_DIR}/scripts/icinga2-splunk.service" > "${UNIT_FILE}"
chmod 0644 "${UNIT_FILE}"
systemctl daemon-reload

cat <<EOF

=== Installation complete ===

Next steps:
1. Edit ${CONFIG_FILE} with your Icinga2 and Splunk settings
2. Test the connection: sudo -u ${SERVICE_USER} ${PREFIX}/bin/icinga2-splunk-check -c ${CONFIG_FILE}
3. Enable and start:    systemctl enable --now icinga2-splunk
4. Follow the logs:     journalctl -u icinga2-splunk -f

EOF
