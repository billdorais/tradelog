#!/bin/bash
set -e

echo "=== ib-gateway-start.sh: patching TrustedIPs ==="

# Show all .ini files BEFORE run.sh touches them (helps debug source of 127.0.0.1)
echo "--- ini files found at startup ---"
find /home/ibgateway -name "*.ini" 2>/dev/null | while read f; do
    echo "=== $f ==="
    cat "$f"
done

# Patch TrustedIPs=127.0.0.1 → TrustedIPs=* in ALL files (ini, sh, conf)
PATCHED=$(find /home/ibgateway -type f \( -name "*.ini" -o -name "*.sh" -o -name "*.conf" \) \
    -exec grep -l "TrustedIPs=127\.0\.0\.1" {} \; 2>/dev/null)

if [ -n "$PATCHED" ]; then
    echo "$PATCHED" | xargs sed -i 's/TrustedIPs=127\.0\.0\.1/TrustedIPs=*/g'
    echo "Patched TrustedIPs in: $PATCHED"
else
    echo "TrustedIPs=127.0.0.1 not found in any file — may be generated at runtime"
fi

# Also pre-write jts.ini as a fallback
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
