# Adding a paper account

Paper accounts (Paper All, Refined, Kairos engine, Crew Paper, …) are driven by a
single **account registry** built at startup in [app.py](../app.py). Adding another
account is mostly configuration — you do **not** edit the ~30 risk-monitor / EOD /
fills / endpoint sites individually any more; they all loop the registry.

## TL;DR — add account N

1. **Set env vars** (the only required step for a working broker):
   ```
   ALPACA_KEY{N}=...
   ALPACA_SECRET{N}=...
   ALPACA_PAPER{N}=true        # "false" for a live account
   ```
   Slot 1 is special: it uses the bare `ALPACA_KEY` / `ALPACA_SECRET` / `ALPACA_PAPER`
   (no number) and the default `AlpacaBroker()` constructor.

2. **Optionally add an `ACCOUNT_META` row** in [app.py](../app.py) to customise the
   label, colour, and feature flags. Without a row, the slot still works with
   defaults (`label="Paper N"`, grey colour, all gates on, `auto_source=True`):
   ```python
   ACCOUNT_META = {
       ...
       "5": {"tag": "alpaca5", "label": "My Account", "color": "#abcdef",
             "daytype_gate": True, "reversal_gate": True, "retest": True,
             "auto_source": True},
   }
   ```
   - `daytype_gate` / `reversal_gate` — include this account's tag in the day-type
     entry gates (`DAYTYPE_GATE_ACCOUNTS` / `DAYTYPE_REVERSAL_GATE_ACCOUNTS`).
   - `retest` — honour reversal-retest windows (`ENGINE_RETEST_ACCOUNTS`; env var
     overrides if set).
   - `auto_source` — informational flag: whether the account is fed by auto-entry
     sources (Refined snapshot / engine pilot). Note these auto-sources name their
     targets explicitly, so a new account only receives auto entries if you also add
     it to the snapshot target / `ENGINE_PILOT_ALL` / `ENGINE_PILOT_EXTRA`. Crew
     Paper (acct 4) is left out of all of them, so it trades only crew-wired rules.

   If `MAX_ALPACA_ACCOUNTS` (default 8) is smaller than N, raise it.

3. **Routing target string** is `alpaca-paper-{N}` (and `alpaca-live-{N}`). The
   registry derives these automatically; `_routing_broker_to_tag()` and the webhook
   resolution ([routes/webhook.py](../routes/webhook.py)) pick them up with no edits.

## Front-end (per-account tabs/cards)

**This is now registry-driven too** (changed when Crew Live / acct 6 was added).
Routes pass `_ui_accounts()` — configured accounts in `UI_ACCOUNT_ORDER`, each with
`num`, `tag`, `label`, `color`, `paper`, and a `tab` key — and the templates loop it.
Only accounts the server actually has keys for render a control, so a deploy missing
`ALPACA_KEY{N}` shows no tab rather than one that fetches nothing.

To add account N to the UI:

1. Add its tab key to `_TAB_KEY_BY_NUM` in [app.py](../app.py) (`"6": "live"`). A
   slot with no key renders a button `switchTab` cannot handle — a test enforces
   that every `ACCOUNT_META` slot has one.
2. Add a row to `_ACCT_TABS` in [templates/index.html](../templates/index.html) for
   its chart label / note text, keyed by that tab key.
3. Add its `allAlpacaN{Execs,Positions}` pair and the `account=N` fetches in the
   dashboard refresh `Promise.all`, plus its entry in `_execsByAcct` /
   `_positionsByAcct`.
4. Give it colours: `.active-<tabkey>` in index.html, `_SRC_BG` / `_SRC_COL` in
   [templates/analysis.html](../templates/analysis.html), and
   `.node-broker-alpaca-{paper,live}-N` in [templates/routing.html](../templates/routing.html).

Tabs, feed buttons, source buttons, the diagnostics account filter, the chart-review
picker and the Replay dropdown all render from the registry with no further edits.

**Routing is the exception, on purpose.** Its broker `<option>` list is static and
lists every *possible* target, because a rule may reference a broker this deploy has
no keys for and the router still has to display and edit it. So a new account needs
its two `<option>`s and its entry in the broker-label map added by hand.

[tests/test_ui_accounts.py](../tests/test_ui_accounts.py) pins all of the above,
including that an unconfigured account renders nowhere.

## Live (real-money) accounts

`ALPACA_PAPER{N}=false` makes a slot real money, and that changes the rules:

- The account is **inert until armed**. `LIVE_TRADING_ARMED=1` plus an explicit
  `LIVE_SIZE_DOLLARS` are both required; `_live_entry_allowed()` refuses every entry
  otherwise, and it FAILS CLOSED (an unreadable balance refuses the trade). See
  `LIVE_MAX_POSITION_PCT` and `LIVE_MAX_ENTRIES_PER_DAY`.
- Check `/api/accounts/preflight?account=<tag>` before arming. It asks Alpaca rather
  than inferring: equity, buying power, blocks, whether leverage is extended.
- The UI must mark it. Live books render a `●` and `title="REAL MONEY"`, keep a tint
  even when inactive, and get the only red broker chip in the router. A real-money
  book that looks like a paper book is the failure mode to avoid.

## What the registry already handles for you

Built once in [app.py](../app.py) (`ALPACA_ACCOUNTS`, `ACCOUNTS_BY_NUM`,
`ACCOUNTS_BY_TAG`) and looped everywhere:

- Risk monitor position polling + stop-close, exit-params recovery, max-hold recovery
- EOD close-all, leaderboard losers, fills/analysis caches + invalidation
- `/api/alpaca/account` / `/api/alpaca/analysis` (`?account=N`), `_alpaca_account_ctx`
- Webhook broker resolution (`alpaca-paper-N` → broker instance + lock tag)
