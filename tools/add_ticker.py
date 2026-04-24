"""Add a ticker's 12 routing rules and print the TV alert checklist.

Usage:
    python tools/add_ticker.py QQQ
    python tools/add_ticker.py QQQ --base-url https://kairos.up.railway.app
    python tools/add_ticker.py --fix-strategy-mismatch

Defaults to local: http://localhost:5000
"""
import argparse
import json
import os
import sys
import urllib.error
import urllib.request


def _post(url, body):
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        try:
            payload = json.loads(e.read())
        except Exception:
            payload = {"error": str(e)}
        return e.code, payload


def add_ticker(base_url, ticker):
    url = base_url.rstrip("/") + "/api/progress/add_ticker"
    code, data = _post(url, {"ticker": ticker})
    if code != 200:
        print(f"Error {code}: {data.get('error') or data}", file=sys.stderr)
        return 1

    created = data.get("created", [])
    skipped = data.get("skipped", [])
    specs   = data.get("specs", [])

    print(f"\n{ticker}: created {len(created)} rule(s), skipped {len(skipped)} existing.")
    if skipped:
        print(f"  Skipped (already exist): {', '.join(skipped)}")
    print(f"\nTradingView alert checklist ({len(specs)} alerts) — ordered to minimize switching:\n")

    last_pine, last_level = None, None
    for i, s in enumerate(specs, 1):
        tv = s["tv_settings"]
        markers = []
        if tv["pine_script"] != last_pine:
            markers.append("[load Pine]")
        if tv["level"] != last_level:
            markers.append("[change Level dropdown]")
        last_pine, last_level = tv["pine_script"], tv["level"]
        marker = " " + " ".join(markers) if markers else ""

        print(f"  {i:>2}. {s['name']}{marker}")
        print(f"      Pine: {tv['pine_script']}   Level: {tv['level']}   Timeframe: {tv['interval']}m")
        payload_oneline = json.dumps(s["payload"], separators=(",", ":"))
        print(f"      Payload: {payload_oneline}")
        print()
    return 0


def fix_mismatch(base_url):
    url = base_url.rstrip("/") + "/api/progress/fix_strategy_mismatch"
    code, data = _post(url, {})
    if code != 200:
        print(f"Error {code}: {data.get('error') or data}", file=sys.stderr)
        return 1
    fixed = data.get("fixed", [])
    if not fixed:
        print("No mismatched rules — nothing to fix.")
    else:
        print(f"Fixed {len(fixed)} rule(s):")
        for n in fixed:
            print(f"  - {n}")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("ticker", nargs="?", help="ticker symbol, e.g. QQQ")
    ap.add_argument("--base-url", default=os.environ.get("KAIROS_BASE_URL", "http://localhost:5000"),
                    help="Kairos base URL (default: http://localhost:5000 or $KAIROS_BASE_URL)")
    ap.add_argument("--fix-strategy-mismatch", action="store_true",
                    help="run the one-shot cleanup that fixes rules whose strategy node value differs from their name")
    args = ap.parse_args()

    if args.fix_strategy_mismatch:
        return fix_mismatch(args.base_url)
    if not args.ticker:
        ap.print_help()
        return 2
    return add_ticker(args.base_url, args.ticker.upper())


if __name__ == "__main__":
    sys.exit(main())
