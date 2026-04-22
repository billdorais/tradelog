"""Provision strategy variants from a config file.

For each (ticker × timeframe × strategy × params) variant, run a backtest
with an in-sample / out-of-sample split, gate on two-stage Sharpe stats,
and emit routing rules for the survivors.

Defaults to dry-run — pass --apply to actually create routing rules via
the /api/routing/rules endpoint.

Usage:
    python -m tools.provision_variants --config variants.json
    python -m tools.provision_variants --config variants.json --apply \
        --api-base http://localhost:5000
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

import pandas as pd
from backtesting import Backtest

from strategies.bt_strategies import STRATEGIES


def backtest_window(df, strategy_cls, params, cash=10_000):
    bt = Backtest(
        df, strategy_cls, cash=cash, commission=0.0,
        exclusive_orders=True,
        trade_on_close=getattr(strategy_cls, "_trade_on_close", False),
    )
    typed = {k: type(getattr(strategy_cls, k, v))(v) for k, v in params.items()}
    stats = bt.run(**typed)
    def _num(k):
        v = stats.get(k)
        try:
            f = float(v)
            return 0.0 if (f != f or f in (float("inf"), float("-inf"))) else f
        except (TypeError, ValueError):
            return 0.0
    return {
        "sharpe":   _num("Sharpe Ratio"),
        "return":   _num("Return [%]"),
        "n_trades": int(stats.get("# Trades") or 0),
        "win_rate": _num("Win Rate [%]"),
        "max_dd":   _num("Max. Drawdown [%]"),
    }


def split_train_test(df, train_frac=0.7):
    split = int(len(df) * train_frac)
    return df.iloc[:split], df.iloc[split:]


def two_stage_gate(is_stats, oos_stats, min_train_sharpe=1.0, oos_ratio=0.7, min_trades=10):
    """Return (passed, reason). IS must clear min_train_sharpe with enough
    trades; OOS must retain at least `oos_ratio` of the IS Sharpe. The OOS
    check is the one that kills overfits in a multi-variant grid."""
    if is_stats["n_trades"] < min_trades:
        return False, f"IS n_trades={is_stats['n_trades']} < {min_trades}"
    if is_stats["sharpe"] < min_train_sharpe:
        return False, f"IS Sharpe={is_stats['sharpe']:.2f} < {min_train_sharpe}"
    threshold = oos_ratio * is_stats["sharpe"]
    if oos_stats["sharpe"] < threshold:
        return False, f"OOS Sharpe={oos_stats['sharpe']:.2f} < {oos_ratio}×IS={threshold:.2f}"
    return True, "OK"


def expand_variants(config):
    """Cross-product ticker × timeframe × strategy for each entry."""
    defaults = config.get("defaults", {})
    for entry in config.get("variants", []):
        tickers    = entry.get("tickers") or [entry["ticker"]]
        tfs        = entry.get("tfs")     or [entry["tf"]]
        strategies = entry.get("strategies") or [entry["strategy"]]
        for ticker in tickers:
            for tf in tfs:
                for strat in strategies:
                    params = {**defaults.get(strat, {}), **entry.get("params", {})}
                    yield {"ticker": ticker, "tf": tf, "strategy": strat, "params": params}


def variant_name(v):
    """Self-describing rule name: CAM_{TICKER}_{STRATEGY}_{TF}."""
    stem = v["strategy"].replace("cam_", "").upper()
    return f"CAM_{v['ticker'].upper()}_{stem}_{v['tf'].upper()}"


def load_bars(ticker, tf, start, end):
    from strategies.data import fetch_bars
    raw = fetch_bars(ticker, start, end, tf)
    if len(raw) < 60:
        raise RuntimeError(f"only {len(raw)} bars — need ≥60 to split train/test")
    df = pd.DataFrame(raw).set_index("time")
    df.index = pd.to_datetime(df.index)
    df.columns = [c.title() for c in df.columns]
    df = df[["Open", "High", "Low", "Close"]].dropna()
    df["Volume"] = 0
    return df


def evaluate_variant(v, start, end, gate_cfg):
    strategy_cls = STRATEGIES[v["strategy"]][0]
    try:
        df = load_bars(v["ticker"], v["tf"], start, end)
    except Exception as e:
        return {**v, "status": "data-error", "reason": str(e)}
    train_df, test_df = split_train_test(df)
    try:
        is_stats  = backtest_window(train_df, strategy_cls, v["params"])
        oos_stats = backtest_window(test_df,  strategy_cls, v["params"])
    except Exception as e:
        return {**v, "status": "run-error", "reason": str(e)}
    passed, reason = two_stage_gate(is_stats, oos_stats, **gate_cfg)
    return {
        **v, "status": "pass" if passed else "fail",
        "reason": reason, "is": is_stats, "oos": oos_stats,
    }


def build_routing_nodes(v):
    """Construct the `nodes` array for a routing rule. Matches the shape
    consumed by routes/webhook.py — a list of dicts each with a `type`."""
    return [
        {"type": "strategy", "value": variant_name(v)},
        {"type": "broker",   "value": v.get("broker",  "alpaca-paper")},
        {"type": "quantity", "amount": v.get("quantity", 1)},
        {"type": "trading_hours", "start": "09:30", "end": "15:55", "tz": "America/New_York"},
    ]


def post_rule(api_base, name, nodes):
    body = json.dumps({"name": name, "nodes": nodes}).encode("utf-8")
    req  = urllib.request.Request(
        f"{api_base.rstrip('/')}/api/routing/rules",
        data=body, headers={"Content-Type": "application/json"}, method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())


def print_summary(results, apply_mode):
    print(f"\n{'Rule name':<36}{'Status':<8}{'IS Shp':>8}{'OOS Shp':>10}  Reason")
    print("-" * 100)
    for r in results:
        name = variant_name(r)
        if r["status"] in ("data-error", "run-error"):
            print(f"{name:<36}{'ERROR':<8}{'—':>8}{'—':>10}  {r['reason'][:50]}")
            continue
        is_s, oos_s = r["is"]["sharpe"], r["oos"]["sharpe"]
        status = "PASS" if r["status"] == "pass" else "fail"
        print(f"{name:<36}{status:<8}{is_s:>8.2f}{oos_s:>10.2f}  {r['reason']}")

    passed = [r for r in results if r["status"] == "pass"]
    print(f"\n{len(passed)}/{len(results)} passed")
    if not passed:
        return
    verb = "Created" if apply_mode else "Would create"
    print(f"\n{verb} routing rules:")
    for r in passed:
        print(f"  • {variant_name(r)}  → {r.get('broker', 'alpaca-paper')}  qty={r['params'].get('quantity', 1)}")
    print("\nTradingView alerts to create manually:")
    for r in passed:
        print(f"  • alert name '{variant_name(r)}' on {r['ticker']} {r['tf']} chart")


def run(config_path, apply=False, api_base="http://localhost:5000"):
    config   = json.loads(Path(config_path).read_text())
    gate_cfg = config.get("gate", {})
    start    = config.get("start_date", "2024-01-01")
    end      = config.get("end_date",   "2024-12-31")

    results = [evaluate_variant(v, start, end, gate_cfg) for v in expand_variants(config)]
    print_summary(results, apply_mode=apply)

    if apply:
        for r in (r for r in results if r["status"] == "pass"):
            name = variant_name(r)
            try:
                resp = post_rule(api_base, name, build_routing_nodes(r))
                print(f"  ✓ {name}  id={resp.get('id')}")
            except urllib.error.URLError as e:
                print(f"  ✗ {name}  {e}", file=sys.stderr)
    return results


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", default="variants.json")
    ap.add_argument("--apply", action="store_true",
                    help="Create routing rules via /api/routing/rules (default: dry-run)")
    ap.add_argument("--api-base", default="http://localhost:5000",
                    help="Base URL of the tradelog API (used with --apply)")
    args = ap.parse_args()
    run(args.config, apply=args.apply, api_base=args.api_base)


if __name__ == "__main__":
    main()
