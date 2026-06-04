# Kairos Trading Knowledge Base

This file is loaded into the AI trading advisor on every crew run.
Add your own validated observations below. Be specific — cite weeks, strategies, and numbers.

---

## Your Validated Observations

*(Add your own observations here as you discover them. Examples below.)*

- [Add your own observations here]

---

## Camarilla Pivot System — Core Theory

### What They Are
Camarilla pivots were developed by Nick Scott in the 1980s for bond futures. Unlike standard
pivots (which use a simple midpoint), Camarilla levels use a 1.1 multiplier applied to the
previous day's range, producing tighter intraday support/resistance zones that price respects
with high frequency in liquid instruments.

### Level Formulas (based on prior day OHLC)
```
H4 = Close + 1.1 × (High - Low) / 2     ← breakout level (long trigger)
H3 = Close + 1.1 × (High - Low) / 4     ← reversal sell zone
H2 = Close + 1.1 × (High - Low) / 6
H1 = Close + 1.1 × (High - Low) / 12

L1 = Close - 1.1 × (High - Low) / 12
L2 = Close - 1.1 × (High - Low) / 6
L3 = Close - 1.1 × (High - Low) / 4     ← reversal buy zone
L4 = Close - 1.1 × (High - Low) / 2     ← breakout level (short trigger)
```

### The Two Trading Rules

**R3S3 — Reversal at H3/L3:**
- Price reaching H3 from below → fade it (short), target L3
- Price reaching L3 from above → fade it (long), target H3
- Logic: H3/L3 represent the statistical edge of the day's expected range.
  Most days, price does NOT break through H3/L3, so reversals have a high hit rate.
- Stop: Just beyond H4 (for shorts) or L4 (for longs)
- Works best: Range-bound days, low-VIX regimes, stable large-cap stocks
- Fails: Trending days with gap-and-go, high-VIX news-driven days

**R4S4 — Breakout at H4/L4:**
- Price breaking above H4 → go long (trend continuation expected)
- Price breaking below L4 → go short (trend continuation expected)
- Logic: H4/L4 breach is statistically rare — when it happens, the day has unusual
  directional conviction and the move tends to extend.
- Stop: Trailing stop to ride the trend (wider trail needed — breakouts need room)
- Works best: Trending days, post-catalyst moves, momentum regimes
- Fails: False breakouts on low volume, pre-market gap that fills intraday

### Regime Dependency — The Most Important Concept

| Regime      | Reversal (R3S3) | Breakout (R4S4) |
|-------------|-----------------|-----------------|
| Ranging     | ✓ Strong        | ✗ False signals |
| Trending    | ✗ Gets stopped  | ✓ Strong        |
| High VIX    | ✗ Unreliable    | ⚠ Wide ranges   |
| Low VIX     | ✓ Consistent    | ⚠ Small ranges  |

**Key insight:** The biggest mistake is running both strategy types equally in all regimes.
In a trending week (SPY +1.5%, VIX dropping), BREAKOUT strategies should dominate sizing.
In a choppy/ranging week (SPY flat, VIX stable), REVERSAL strategies should dominate.

### Stop Placement Philosophy

For **reversals (R3S3)**:
- Initial stop beyond H4/L4 (the next level) — this is the invalidation point
- Trail should be relatively wide (0.30–0.50%) to avoid being stopped on noise
- The edge is in the entry, not the stop — tight stops destroy the win rate
- Target: The opposite H/L level (H3 → L3 or L3 → H3)

For **breakouts (R4S4)**:
- Wider initial room needed — breakouts often test the level before running
- Trail of 0.13–0.20% is appropriate once in profit
- A trigger (0.1%) before activating the trail prevents premature stop placement
- Partial profits at the H5/L5 extension level

### Level Sensitivity by Instrument

Not all instruments respect Camarilla levels equally:
- **Best:** SPY, QQQ, SPX, large-cap liquid stocks (AAPL, MSFT, GOOG, AMZN)
- **Good:** Mid-cap liquid names (PLTR, NFLX, IWM, GLD, SMH)
- **Weaker:** Low-float or thin stocks (ASTS, HOOD) — pivot levels less reliable
- **Avoid for reversals:** High-beta momentum names during earnings season

### Intraday Time Considerations

- **9:30–10:00:** Levels most reliable after the open settles; avoid first 5 min
- **10:00–11:30:** Prime reversal window if the daily range is being established
- **11:30–13:00:** Lunch hour — thin volume, avoid new entries
- **13:00–15:00:** Breakout window if the afternoon trend develops
- **15:00–15:55:** Caution — EOD volatility, positions should be flat

### V02 Strategy Implementation (Your System)
- 5-minute bars: Signal fires when price reaches the level on a 5-min close
- TradingView alert → Kairos webhook → Alpaca order (market)
- R3S3: Entry at H3/L3 touch, trail 0.38% (reversals need wider trail)
- R4S4: Entry at H4/L4 break, trail 0.13–0.15% (tighter — trend already confirmed)
- Max hold 15 minutes: Positions not working within 15m are closed flat
- Trigger on reversals (0.1%): Waits for 0.1% move in your favour before trail activates

### Known Edge Cases and Failure Modes

1. **Gap days:** If the open gaps past H4/L4, the level loses predictive value for that day
2. **News catalyst:** Fed/FOMC/earnings override pivot logic entirely — stay flat
3. **Level clustering:** When H3 ≈ yesterday's H4, levels stack and the zone is stronger
4. **Weekly pivots:** W-levels (computed from weekly OHLC) provide longer-horizon context
5. **Failed reversal signal:** If R3S3 triggers but price immediately breaks H4/L4, exit immediately — the regime has shifted to breakout

---

## Performance Notes to Reference

*(This section is auto-populated from your trading history — add manual observations here)*

- The scoring system uses 20-day lookback with 10-day recency blend (60/40)
- Composite score: Sharpe 35% + PF 30% + Win Rate 20% + Trades 15%
- Min 5 trades required for Refined eligibility
- 3 consecutive losses = auto-demotion from Refined
