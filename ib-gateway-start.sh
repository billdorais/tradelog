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

echo "=== Handing off to run.sh ==="
exec /home/ibgateway/scripts/run.sh
