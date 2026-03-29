#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

chmod +x "$SCRIPT_DIR/data-source-option-run.sh"

echo "Copying service files..."
sudo cp "$SCRIPT_DIR/data-source-option.service" /etc/systemd/system/
sudo cp "$SCRIPT_DIR/data-source-option.timer" /etc/systemd/system/
sudo cp "$SCRIPT_DIR/data-source-option-boot.service" /etc/systemd/system/

echo "Reloading systemd..."
sudo systemctl daemon-reload

echo "Enabling timer (daily 15:00)..."
sudo systemctl enable --now data-source-option.timer

echo "Enabling boot check service..."
sudo systemctl enable data-source-option-boot.service

echo "Done."
echo ""
echo "Status:  systemctl status data-source-option.timer"
echo "Next:    systemctl list-timers data-source-option.timer"
echo "Logs:    journalctl -u data-source-option.service"
echo "Run now: sudo systemctl start data-source-option.service"
