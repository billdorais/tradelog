"""Deterministic 'top 9 from each snapshot' Crew Paper wiring.

_snapshot_top_picks mirrors the two Refined leaderboards verbatim (no LLM): TV
snapshot top-9 as [TV], Kairos snapshot top-9 as [Kairos], with per-pick side
gates. A name in BOTH top-9s is assigned to its higher-ranked book and the other
book backfills, so the result stays 18 unique picks.
"""
from __future__ import annotations

import os

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import app as A
import routes.crew as crew


def _snap(names):
    return {"top_strategies": names, "top_scored": [{"name": n} for n in names]}


class _FakeResp:
    def __init__(self, payload): self._p = payload
    def get_json(self): return self._p


class _FakeClient:
    def __init__(self, sides): self._sides = sides
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def get(self, url):
        # acct=1 -> TV Farm sides, acct=5 -> Kairos Farm sides, else empty
        acct = "1" if "account=1" in url else "5" if "account=5" in url else "0"
        return _FakeResp({"side_gated_candidates": self._sides.get(acct, [])})


class _FakeApp:
    def __init__(self, sides=None): self._sides = sides or {}
    def test_client(self): return _FakeClient(self._sides)


def _set_snaps(tv, kr):
    A._refined_last_result        = _snap(tv)
    A._kairos_refined_last_result = _snap(kr)


def test_top9_each_18_unique_with_overlap_resolved():
    tv = ["NVDA_CAM_BREAKOUT_R3S3_V02_5MIN"] + [f"T{i}_CAM_BREAKOUT_R3S3_V02_5MIN" for i in range(2, 10)]
    kr = (["K1_CAM_REVERSAL_R3S3_V02_5MIN", "K2_CAM_REVERSAL_R3S3_V02_5MIN",
           "NVDA_CAM_BREAKOUT_R3S3_V02_5MIN"]
          + [f"K{i}_CAM_REVERSAL_R3S3_V02_5MIN" for i in range(4, 12)])
    _set_snaps(tv, kr)
    picks, warns = crew._snapshot_top_picks(_FakeApp(), n=9)
    slugs = [p["strategy"] for p in picks]
    assert len(picks) == 18 and len(set(slugs)) == 18          # 18 unique
    tvp = [p["strategy"] for p in picks if p["entry"] == "tv"]
    krp = [p["strategy"] for p in picks if p["entry"] == "kairos"]
    assert len(tvp) == 9 and len(krp) == 9
    # Overlap (TV rank 0 < Kairos rank 2) → stays in TV, excluded from Kairos.
    assert "NVDA_CAM_BREAKOUT_R3S3_V02_5MIN" in tvp
    assert "NVDA_CAM_BREAKOUT_R3S3_V02_5MIN" not in krp
    # Kairos still reaches 9 by pulling one deeper.
    assert "K10_CAM_REVERSAL_R3S3_V02_5MIN" in krp


def test_side_gate_applied_from_farm():
    tv = [f"T{i}_CAM_BREAKOUT_R3S3_V02_5MIN" for i in range(1, 10)]
    kr = [f"K{i}_CAM_REVERSAL_R3S3_V02_5MIN" for i in range(1, 10)]
    _set_snaps(tv, kr)
    sides = {
        "1": [{"strategy": "T1_CAM_BREAKOUT_R3S3_V02_5MIN", "best_side": "LONG"}],
        "5": [{"strategy": "K1_CAM_REVERSAL_R3S3_V02_5MIN", "best_side": "SHORT"}],
    }
    picks = {p["strategy"]: p for p in crew._snapshot_top_picks(_FakeApp(sides), n=9)[0]}
    assert picks["T1_CAM_BREAKOUT_R3S3_V02_5MIN"]["side"] == "long"
    assert picks["K1_CAM_REVERSAL_R3S3_V02_5MIN"]["side"] == "short"
    # An un-flagged name stays two-sided.
    assert picks["T2_CAM_BREAKOUT_R3S3_V02_5MIN"]["side"] == "both"


def test_empty_snapshot_warns():
    _set_snaps([], [])
    picks, warns = crew._snapshot_top_picks(_FakeApp(), n=9)
    assert picks == []
    assert any("empty" in w.lower() for w in warns)
