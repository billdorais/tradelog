#!/bin/bash
set -e

echo "=== ib-gateway-start.sh: patching TrustedIPs ==="

IBC_CONFIG=/home/ibgateway/ibc/config.ini

# Patch IBC's own config so it writes TrustedIPs=* into jts.ini
if grep -q "^TrustedTwsApiClientIPs=" "$IBC_CONFIG" 2>/dev/null; then
    sed -i 's/^TrustedTwsApiClientIPs=.*/TrustedTwsApiClientIPs=*/' "$IBC_CONFIG"
    echo "Patched TrustedTwsApiClientIPs=* in $IBC_CONFIG"
else
    echo "TrustedTwsApiClientIPs line not found — appending to $IBC_CONFIG"
    echo "TrustedTwsApiClientIPs=*" >> "$IBC_CONFIG"
fi

# Also pre-write jts.ini as belt-and-suspenders
mkdir -p /home/ibgateway/Jts
cat > /home/ibgateway/Jts/jts.ini << 'EOF'
[Logon]
Locale=en
UseSSL=true

[IBGateway]
ApiOnly=true
TrustedIPs=*
EOF
echo "Written jts.ini with TrustedIPs=*"

echo "=== Starting watchdog (will exit container if Gateway stops responding) ==="

# Watchdog: runs in background, checks port 4002 every 60s.
# After Gateway has had 3 minutes to start up, if it stops responding
# for 3 consecutive checks (3 min) the container exits so Railway restarts it.
watchdog() {
    # Give Gateway time to start before we begin watching
    echo "Watchdog: waiting 3 minutes for Gateway to start..."
    sleep 180

    consecutive_failures=0
    while true; do
        sleep 60
        if nc -z 127.0.0.1 4002 2>/dev/null; then
            if [ "$consecutive_failures" -gt 0 ]; then
                echo "Watchdog: port 4002 recovered after $consecutive_failures failure(s)"
            fi
            consecutive_failures=0
        else
            consecutive_failures=$((consecutive_failures + 1))
            echo "Watchdog: port 4002 not responding (failure $consecutive_failures/3)"
            if [ "$consecutive_failures" -ge 3 ]; then
                echo "Watchdog: Gateway unresponsive for 3 minutes — exiting container to trigger Railway restart"
                kill -TERM 1 2>/dev/null || exit 1
            fi
        fi
    done
}

watchdog &

echo "=== Handing off to run.sh ==="
exec /home/ibgateway/scripts/run.sh
