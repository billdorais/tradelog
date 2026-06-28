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

The backend is fully registry-driven; the templates still list accounts explicitly.
To surface account N in the UI, follow the acct-4 ("crew") pattern:

- **Dashboard** [templates/index.html](../templates/index.html): a P&L tab + feed
  tab, the `allAlpacaN*` arrays, `_acctForTab`, `switchTab`/`switchFeedTab` branches,
  the `account=N` fetch pair in the refresh `Promise.all`, the open-positions
  row-class, and the `.active-*` / `.*-row` CSS.
- **Analysis** [templates/analysis.html](../templates/analysis.html): a `srcAlpacaN`
  source button, the `tabs` map + colours in `switchSource`, and the `isAlpacaN` /
  `account=N` branches in `load()` and the label/breakdown helpers.
- **Routing** [templates/routing.html](../templates/routing.html): broker `<option>`s,
  the broker-label map, and a `.node-broker-alpaca-paper-N` colour.

The Refined-vs-Kairos A/B tools ([templates/journal.html](../templates/journal.html),
[templates/entry_engine.html](../templates/entry_engine.html)) are pilot-specific
comparisons, not per-account views, and are intentionally left at two columns.

## What the registry already handles for you

Built once in [app.py](../app.py) (`ALPACA_ACCOUNTS`, `ACCOUNTS_BY_NUM`,
`ACCOUNTS_BY_TAG`) and looped everywhere:

- Risk monitor position polling + stop-close, exit-params recovery, max-hold recovery
- EOD close-all, leaderboard losers, fills/analysis caches + invalidation
- `/api/alpaca/account` / `/api/alpaca/analysis` (`?account=N`), `_alpaca_account_ctx`
- Webhook broker resolution (`alpaca-paper-N` → broker instance + lock tag)
