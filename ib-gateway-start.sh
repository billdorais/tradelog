#!/bin/bash
set -e

# Pre-configure jts.ini with TrustedIPs=* so IBC doesn't lock it to 127.0.0.1
mkdir -p /home/ibgateway/Jts
cat > /home/ibgateway/Jts/jts.ini << 'EOF'
[Logon]
Locale=en
UseSSL=true

[IBGateway]
ApiOnly=true
TrustedIPs=*
EOF

echo "Pre-configured /home/ibgateway/Jts/jts.ini with TrustedIPs=*"

exec /home/ibgateway/scripts/run.sh
