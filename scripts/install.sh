#!/bin/bash
# Installation script for Icinga2 to Splunk integration

set -e

echo "=== Icinga2 to Splunk Integration Installer ==="

# Check if running as root
if [ "$EUID" -ne 0 ]; then
  echo "Please run as root or with sudo"
  exit 1
fi

# Install Python dependencies
echo "Installing Python dependencies..."
pip3 install requests

# Create directories
echo "Creating directories..."
mkdir -p /usr/local/bin/icinga2_splunk
mkdir -p /etc/icinga2
mkdir -p /var/log/icinga2

# Copy Python module files
echo "Installing Python modules..."
cp -r icinga2_splunk/* /usr/local/bin/icinga2_splunk/

# Make scripts executable
chmod +x /usr/local/bin/icinga2_splunk/event_stream_client.py
chmod +x /usr/local/bin/icinga2_splunk/test_connection.py

# Copy configuration example
if [ ! -f /etc/icinga2/splunk_config.json ]; then
  echo "Creating configuration file..."
  cp icinga2_splunk/config.json.example /etc/icinga2/splunk_config.json
  echo "Please edit /etc/icinga2/splunk_config.json with your settings"
else
  echo "Configuration file already exists, skipping..."
fi

# Install systemd service
echo "Installing systemd service..."
cp scripts/icinga2-splunk.service /etc/systemd/system/
systemctl daemon-reload

echo ""
echo "=== Installation Complete ==="
echo ""
echo "Next steps:"
echo "1. Edit /etc/icinga2/splunk_config.json with your Icinga2 and Splunk settings"
echo "2. Test the connection: /usr/local/bin/icinga2_splunk/test_connection.py"
echo "3. Enable the service: systemctl enable icinga2-splunk"
echo "4. Start the service: systemctl start icinga2-splunk"
echo "5. Check status: systemctl status icinga2-splunk"
echo "6. View logs: journalctl -u icinga2-splunk -f"
echo ""
