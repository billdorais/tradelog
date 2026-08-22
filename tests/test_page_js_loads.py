"""Every page's inline JS must survive LOAD, not merely parse.

Added after a shipped regression: `let feedTab = activeTab;` sat 29 lines above
`let activeTab = ...`, a temporal dead zone that threw ReferenceError the moment
the script ran. The whole dashboard froze at "Loading…" with every tile showing an
em-dash — no tab switched, no data arrived.

`node --check` passed it, because the code is syntactically perfect. Nothing here
executed the script, so nothing caught it. These tests run each page's JS in a
sandbox under a stub DOM and fail if it throws before it finishes loading.

Scope, deliberately: this proves the script REACHES the end. It does not click
anything or assert behaviour. That narrow guarantee is exactly the class of bug
that shipped, and it is cheap enough to run on every page every time.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess

os.environ.setdefault("WEBHOOK_TOKEN", "test-token")
for _k in ("ALPACA_KEY", "COINBASE_KEY", "IB_HOST", "IB_HOST_LIVE", "DATABASE_URL"):
    os.environ.pop(_k, None)

import pytest

import app as a

_HARNESS = os.path.join(os.path.dirname(__file__), "js_load_harness.js")

# Pages carrying enough JS to break. A new page belongs here.
_PAGES = ["/", "/analysis", "/diagnostics", "/review", "/routing", "/recap",
          "/simulate", "/strategy-explorer", "/crew"]

_ACCOUNTS = [{"num": n, "tag": t, "label": l, "color": "#888", "paper": p}
             for n, t, l, p in [("1", "alpaca", "TV Farm", True),
                                ("2", "alpaca2", "TV Refined", True),
                                ("3", "alpaca3", "Kairos Refined", True),
                                ("4", "alpaca4", "Crew Paper", True),
                                ("5", "alpaca5", "Kairos Farm", True),
                                ("6", "alpaca6", "Crew Live", False)]]

pytestmark = pytest.mark.skipif(shutil.which("node") is None,
                                reason="node not on PATH")


def _run(js_path):
    return subprocess.run(["node", _HARNESS, str(js_path)],
                          capture_output=True, text=True, timeout=60)


def _extract(client, path, tmp_path, name):
    r = client.get(path)
    if r.status_code == 404:
        pytest.skip(f"{path} not routed in this build")
    assert r.status_code == 200, f"{path} returned {r.status_code}"
    js = "\n".join(re.findall(r"<script>(.*?)</script>",
                              r.get_data(as_text=True), re.S))
    if not js.strip():
        pytest.skip(f"{path} has no inline script")
    out = tmp_path / f"{name}.js"
    out.write_text(js, encoding="utf-8")
    return out


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(a, "ALPACA_ACCOUNTS", _ACCOUNTS)
    a.app.config["TESTING"] = True
    return a.app.test_client()


@pytest.mark.parametrize("path", _PAGES)
def test_page_javascript_runs_to_completion(client, tmp_path, path):
    js = _extract(client, path, tmp_path, path.strip("/") or "index")
    r  = _run(js)
    assert r.returncode == 0, f"{path} threw during load:\n  {r.stderr.strip()}"


@pytest.mark.parametrize("path", ["/", "/analysis", "/review", "/diagnostics"])
def test_pages_also_load_without_the_live_account(monkeypatch, tmp_path, path):
    """The account-dependent pages take a different branch when acct6 is absent —
    tabs are pruned and the default falls back — so that path needs running too."""
    monkeypatch.setattr(a, "ALPACA_ACCOUNTS", [x for x in _ACCOUNTS if x["num"] != "6"])
    a.app.config["TESTING"] = True
    js = _extract(a.app.test_client(), path, tmp_path, (path.strip("/") or "index") + "_nolive")
    r  = _run(js)
    assert r.returncode == 0, f"{path} threw without acct6:\n  {r.stderr.strip()}"


def test_the_harness_actually_catches_a_dead_zone(tmp_path):
    """A guard that cannot fail is worse than none — it reads as proof while
    proving nothing. This reproduces the shipped bug in miniature."""
    good = tmp_path / "good.js"
    good.write_text("let a = 1; let b = a; function f() { return b; }", encoding="utf-8")
    assert _run(good).returncode == 0

    bad = tmp_path / "bad.js"
    bad.write_text("let b = a; let a = 1;", encoding="utf-8")     # the exact shape
    r = _run(bad)
    assert r.returncode == 1
    assert "before initialization" in r.stderr


def test_the_harness_does_not_need_the_network(tmp_path):
    """fetch never resolves in the harness. A page that cannot finish loading
    without a live response would hang in a browser too."""
    js = tmp_path / "f.js"
    js.write_text("fetch('/api/x').then(r => r.json()); const done = 1;", encoding="utf-8")
    assert _run(js).returncode == 0
