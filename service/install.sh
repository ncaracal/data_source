#!/bin/bash
set -e

# Installs the 4 data-source services + timers + boot units:
#   data-source-btceth        daily 07:30 UTC  (BTC/ETH spot aggTrades + kline + verify)
#   data-source-altcoin-spot  daily 08:00 UTC  (altcoin spot download/verify + kline)
#   data-source-altcoin-um    daily 08:20 UTC  (altcoin um download/verify + kline)
#   data-source-option        daily 09:00 UTC  (BVOLIndex)

SERVICE_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVICES="btceth altcoin-spot altcoin-um option"

# Retire the old combined altcoin unit (replaced by altcoin-spot + altcoin-um).
if systemctl list-unit-files 'data-source-altcoin.*' 2>/dev/null | grep -q data-source-altcoin; then
    echo "Removing old data-source-altcoin units..."
    sudo systemctl disable --now data-source-altcoin.timer 2>/dev/null || true
    sudo systemctl disable data-source-altcoin-boot.service 2>/dev/null || true
    # Note: does NOT stop an in-progress data-source-altcoin.service run.
    sudo rm -f /etc/systemd/system/data-source-altcoin.service \
               /etc/systemd/system/data-source-altcoin.timer \
               /etc/systemd/system/data-source-altcoin-boot.service
fi

echo "Making run scripts executable..."
for s in $SERVICES; do
    chmod +x "$SERVICE_DIR/$s/data-source-$s-run.sh"
done

echo "Copying unit files..."
for s in $SERVICES; do
    sudo cp "$SERVICE_DIR/$s/data-source-$s.service"      /etc/systemd/system/
    sudo cp "$SERVICE_DIR/$s/data-source-$s.timer"         /etc/systemd/system/
    sudo cp "$SERVICE_DIR/$s/data-source-$s-boot.service"  /etc/systemd/system/
done

echo "Reloading systemd..."
sudo systemctl daemon-reload

echo "Enabling timers + boot units..."
for s in $SERVICES; do
    sudo systemctl enable --now "data-source-$s.timer"
    sudo systemctl enable "data-source-$s-boot.service"
done

echo "Done."
echo ""
echo "Timers:  systemctl list-timers 'data-source-*.timer'"
echo "Logs:    journalctl -u data-source-altcoin-spot.service -u data-source-altcoin-um.service"
echo "Run now: sudo systemctl start data-source-altcoin-spot.service"
