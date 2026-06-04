# Kairos Trading Knowledge Base

This file is loaded into the AI trading advisor on every crew run.
Add your own validated observations below. Be specific — cite weeks, strategies, and numbers.

---

## Your Validated Observations

*(Add your own observations here as you discover them. Examples below.)*

- [Add your own observations here]

---

## Thor Young — "A Complete Day Trading System" Key Principles
*(Source: "Spotting Optimal Trade Entry Opportunities" webinar, TradingTerminal.com)*

### The Where, When, How Framework
Every trading system needs three things:
1. **Where** — where to look for the trade (Camarilla pivot levels)
2. **When** — when to take the trade (volume confirmation, order book, value accepted)
3. **How** — how to manage the trade (stops, targets, partials)

Taking a trade with anything less than all three is, at best, gambling. Kairos currently
has strong "Where" (pivot level entries) and "How" (trailing stops, max hold). The "When"
is partly handled by the 5-min bar signal but volume confirmation is not assessed.

### The Core Principle — Acceptance and Rejection of Value
The market is made of participants (people or algos programmed by people) who inherently
overreact to value. The Camarilla system quantifies this behavioural pattern.

- **Value is accepted** when the price consolidates and volume drops — the market agrees
  on a price. During acceptance, volume drops and the market becomes "untradable" (chop).
- **Value is rejected** when price moves away — up or down. This rejection is where trades begin.
- **The edge**: Wait for value to be accepted at a Camarilla level, then trade the rejection.
  Do NOT trade during the acceptance phase (grey area).

### The Grey Area — AVOID
Between R1/R2/S1/S2 is where you get chopped. Playing near value means:
- Lots of stops while waiting for direction
- Coin-flip at best, worse in practice (price oscillates as it builds momentum)
- Use Camarilla pivots to FIND the grey area, then wait for the extremes (R3/R4, S3/S4)

**Application to Kairos:** Kairos correctly fires at R3/S3 and R4/S4 levels. Avoid adding
strategies that fire at R1/R2/S1/S2 — these are grey zone entries.

### Inside Day vs Outside Day — CRITICAL PRE-MARKET CLASSIFICATION
This is the most important pre-session decision. Determines which strategy type to run.

**How to identify:**
- Compute today's Central Pivot Range (CPR) = R2 − S2 = (prior day H−L) × 1.1/3
- Compare to yesterday's CPR width = (day-before-yesterday H−L) × 1.1/3

| Day Type    | CPR Width vs Yesterday | Expected Behaviour | Strategy to favour |
|-------------|------------------------|--------------------|--------------------|
| Inside Day  | TODAY wider than yest. | Price stays in range, traverses between levels | REVERSAL R3S3 |
| Outside Day | TODAY narrower than yest.| Price breaks out, new value search | BREAKOUT R4S4 |
| Neutral     | Similar width, overlapping | Chop then big move | Wait for confirmation |

**Inside Day rules:**
- Do NOT enter breakout longs near R4 — already extended, probability is to the downside
- Play traverses: short at R3, target S4; long at S3, target R4
- The range gives plenty of room — be patient, these take time

**Outside Day rules:**
- Avoid the grey area entirely
- Wait for extremes to be rejected, then play breakouts or extreme reversals at the 4th levels
- On R4 break, expect a large move; ride with trailing stop, exit on trend average failure
- On tight pivot range with gap open: look for R5/R6 or S5/S6 run before trend reversal

**Neutral (no bias) day:**
- Today's CPR sits between yesterday's CPR — chop likely, then big move
- R4/S4 act as directional bias indicators
- Open above CPR → long bias, buy R4 targeting R5/R6
- Open below CPR → short bias, sell S4 targeting S5/S6

**Application to Kairos gap:** The system currently fires BOTH breakout and reversal signals
regardless of day type. On an Inside Day, suppressing BREAKOUT R4S4 signals and widening
reversal trails would likely improve the win rate significantly. This is a known missing filter.

### Forming a Daily Bias — 3-Step Pre-Market Process
Before the open, complete this sequence:

**Step 1 — Develop Bias from Pivot Ranges:**
- **Bullish Bias**: Today's CPR is HIGHER than yesterday's CPR (not just wider — higher in price)
  - Even overlapping CPRs are bullish, just weaker
- **Bearish Bias**: Today's CPR is LOWER than yesterday's CPR
- **No Bias**: Today's CPR is sandwiched inside yesterday's CPR

**Step 2 — Confirm the Bias at the Open:**
Where price opens relative to the levels is critical.
- Opens ABOVE R4: Look for R4 retest long, or short R3 if rejected
- Opens BELOW S4: Look for S4 retest short, or long S3 if rejected
- Opens WITHIN the range: Look for traverses and outside day breakouts
- Prior session closed ABOVE its CPR + today opens above pivots → Trend long bias

**Step 3 — Judge Inside or Outside Day potential:**
See the table above. Decide which strategy type dominates the session.

**Application to Kairos:** The AI advisor can assess this with available data but the automated
system has no pre-market bias filter. Adding a daily bias gate to the signal router would
require daily OHLC data (available via Alpaca) and a new routing node.

### Order Book Confirmation — "The When" (Manual Only)
Thor combines pivots with BookMap/Level 2 order book to confirm entries:
- Wait for large institutional orders to appear at the Camarilla level
- "Price moves to Size" — large orders near the level confirm it as valid support/resistance
- Iceberg orders (recycling size) at levels = strong institutional commitment
- A bullish book (more bids below price) at S3 gives high-confidence long

**Application to Kairos:** Order book data is not accessible via TradingView webhooks or
Alpaca's REST API in real-time. This "When" confirmation is a manual filter that automated
systems cannot replicate. However, using volume confirmation (relative volume at signal time)
as a proxy is possible and would filter low-quality entries.

### R5/R6 and S5/S6 — Extension Levels
On outside breakout days, price frequently runs to the extension levels:
- R5 = R4 + 1.168 × (R4 − R3)
- R6 = (High/Low) × Close
- S5 = S4 − 1.168 × (S3 − S4)
- S6 = Close − (R6 − Close)

These are meaningful profit targets on strong breakout days when R4/S4 is breached cleanly.

### Key Quotes
- "Every trading system needs three things. Where to look for a trade. When to take the
  trade. How to manage the trade. Taking a trade with anything less is, at best, gambling."
- "The market will inherently overreact to value. People by design overreact to almost everything."
- "Like surfing, to make money in the market position is extremely important. You got to
  know where to wait first."
- "Once value is accepted the stock becomes untradable." (Enter after the rejection, not during acceptance.)

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
