"""Unit tests for the provision_variants driver — focused on the pure
functions (gate logic, variant expansion, naming) that don't need live
data or an HTTP endpoint."""
from __future__ import annotations

import pandas as pd

from tools.provision_variants import (
    build_routing_nodes,
    expand_variants,
    gate_walk_forward,
    split_train_test,
    two_stage_gate,
    variant_name,
    walk_forward_folds,
)


def _stats(sharpe, n_trades=20):
    return {"sharpe": sharpe, "n_trades": n_trades,
            "return": 10.0, "win_rate": 55.0, "max_dd": -5.0}


def test_gate_fails_on_insufficient_trades():
    ok, reason = two_stage_gate(_stats(2.0, n_trades=3), _stats(1.5), min_trades=10)
    assert not ok and "n_trades" in reason


def test_gate_fails_on_low_in_sample_sharpe():
    ok, reason = two_stage_gate(_stats(0.5), _stats(0.4), min_train_sharpe=1.0)
    assert not ok and "IS Sharpe" in reason


def test_gate_fails_when_oos_degrades_too_much():
    # IS=1.5, OOS=0.5 → ratio=0.33 < default 0.7 → fail
    ok, reason = two_stage_gate(_stats(1.5), _stats(0.5))
    assert not ok and "OOS Sharpe" in reason


def test_gate_passes_when_oos_stays_close_to_is():
    # IS=1.5, OOS=1.2 → ratio=0.8 ≥ 0.7 → pass
    ok, reason = two_stage_gate(_stats(1.5), _stats(1.2))
    assert ok and reason == "OK"


def test_split_train_test_default_70_30():
    df = pd.DataFrame({"x": range(100)})
    train, test = split_train_test(df)
    assert len(train) == 70 and len(test) == 30


def test_expand_variants_cross_product():
    cfg = {
        "defaults": {"cam_breakout": {"stop_loss": 0.50}},
        "variants": [{
            "tickers": ["AAPL", "PLTR"],
            "tfs":     ["5m", "10m"],
            "strategies": ["cam_breakout"],
            "params": {"ema_period": 13},
        }],
    }
    out = list(expand_variants(cfg))
    assert len(out) == 4   # 2 tickers × 2 tfs × 1 strategy
    # Defaults merged, entry params override
    assert all(v["params"]["stop_loss"] == 0.50 for v in out)
    assert all(v["params"]["ema_period"] == 13 for v in out)


def test_variant_name_is_self_describing():
    v = {"ticker": "aapl", "tf": "5m", "strategy": "cam_h3l3_reversal", "params": {}}
    assert variant_name(v) == "CAM_AAPL_H3L3_REVERSAL_5M"


def test_walk_forward_yields_expected_fold_boundaries():
    df = pd.DataFrame({"x": range(400)})
    folds = list(walk_forward_folds(df, n_folds=3, min_fold_bars=30))
    assert len(folds) == 3
    # chunk = 400 // 4 = 100 → train_end=100/200/300, test_end=200/300/400
    assert len(folds[0][0]) == 100 and len(folds[0][1]) == 100
    assert len(folds[1][0]) == 200 and len(folds[1][1]) == 100
    assert len(folds[2][0]) == 300 and len(folds[2][1]) == 100


def test_walk_forward_falls_back_to_single_split_when_data_thin():
    """n_folds=3 on only 90 bars → chunk=22 < min_fold_bars=30, fallback."""
    df = pd.DataFrame({"x": range(90)})
    folds = list(walk_forward_folds(df, n_folds=3, min_fold_bars=30))
    assert len(folds) == 1  # fallback to single train/test split


def test_gate_walk_forward_fails_if_any_fold_fails():
    good = {"is": {"sharpe": 1.5, "n_trades": 20, "return": 0, "win_rate": 0, "max_dd": 0},
            "oos":{"sharpe": 1.2, "n_trades": 15, "return": 0, "win_rate": 0, "max_dd": 0}}
    bad  = {"is": {"sharpe": 1.5, "n_trades": 20, "return": 0, "win_rate": 0, "max_dd": 0},
            "oos":{"sharpe": 0.3, "n_trades": 15, "return": 0, "win_rate": 0, "max_dd": 0}}
    ok, reason = gate_walk_forward([good, bad, good])
    assert not ok and "fold 1" in reason


def test_gate_walk_forward_passes_when_all_folds_pass():
    fold = {"is": {"sharpe": 1.5, "n_trades": 20, "return": 0, "win_rate": 0, "max_dd": 0},
            "oos":{"sharpe": 1.2, "n_trades": 15, "return": 0, "win_rate": 0, "max_dd": 0}}
    ok, _ = gate_walk_forward([fold, fold, fold])
    assert ok


def test_build_routing_nodes_has_all_required_types():
    v = {"ticker": "AAPL", "tf": "5m", "strategy": "cam_breakout",
         "broker": "alpaca-paper", "quantity": 2, "params": {}}
    nodes = build_routing_nodes(v)
    types = [n["type"] for n in nodes]
    assert set(types) == {"strategy", "broker", "quantity", "trading_hours"}
    by_type = {n["type"]: n for n in nodes}
    assert by_type["strategy"]["value"] == "CAM_AAPL_BREAKOUT_5M"
    assert by_type["broker"]["value"]   == "alpaca-paper"
    assert by_type["quantity"]["amount"] == 2
