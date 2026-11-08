// Daily + Regimes views of the journal. Data: routes/journal_daily.py.
(() => {
  'use strict';

  const $ = (sel, root = document) => root.querySelector(sel);
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  const sign = (v) => (v > 0 ? '+' : v < 0 ? '−' : '');
  const money = (v) => v == null ? '—' : `${sign(v)}$${Math.abs(v).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  const pct = (v, d = 2) => v == null ? '—' : `${sign(v)}${Math.abs(v).toFixed(d)}%`;
  const share = (v) => v == null ? '—' : `${Math.round(v * 100)}%`;
  const cls = (v) => (v > 0 ? 'pos' : v < 0 ? 'neg' : '');
  const isoDay = (d) => d.toISOString().slice(0, 10);
  const daysAgo = (n) => { const d = new Date(); d.setDate(d.getDate() - n); return isoDay(d); };
  const niceDate = (iso) => new Date(iso + 'T12:00:00Z').toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric', timeZone: 'UTC' });
  const BACKFILL_FROM = '2026-08-10';

  async function getJSON(url, opts) {
    const r = await fetch(url, opts);
    const d = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(d.error || `${r.status} ${r.statusText}`);
    return d;
  }

  // ── View switching ──────────────────────────────────────────────────────
  const VIEWS = { weekly: 'viewWeekly', daily: 'viewDaily', regimes: 'viewRegimes' };
  const loaded = {};
  function showView(name) {
    if (!VIEWS[name]) name = 'weekly';
    document.querySelectorAll('.view-tab').forEach((b) => {
      const on = b.dataset.view === name;
      b.classList.toggle('active', on);
      b.setAttribute('aria-selected', on ? 'true' : 'false');
    });
    Object.entries(VIEWS).forEach(([k, id]) => { const el = document.getElementById(id); if (el) el.hidden = k !== name; });
    if (location.hash.slice(1) !== name) history.replaceState(null, '', name === 'weekly' ? location.pathname : `#${name}`);
    if (!loaded[name]) {
      loaded[name] = true;
      if (name === 'daily') initDaily();
      if (name === 'regimes') initRegimes();
    }
  }

  // ── Shared meta (accounts, note tags, rules) ────────────────────────────
  let META = null;
  async function meta() {
    if (META) return META;
    const d = await getJSON(`/api/journal/days?from=${daysAgo(3)}&to=${isoDay(new Date())}`);
    META = { accounts: d.accounts || [], note_tags: d.note_tags || [], rules: d.rules || {}, dimensions: d.dimensions || {} };
    return META;
  }
  const acct = (num) => (META?.accounts || []).find((a) => a.num === String(num)) || { label: `Account ${num}`, color: '#888' };

  // ── Daily ───────────────────────────────────────────────────────────────
  function initDaily() {
    const root = document.getElementById('viewDaily');
    root.innerHTML = `
      <div class="page-title">Daily Journal</div>
      <div class="page-sub">Market conditions and every strategy's result, one row per trading day. Saved automatically after each close.</div>
      <div class="jd-toolbar">
        <label>From <input type="date" id="jdFrom" class="jd-input"></label>
        <label>To <input type="date" id="jdTo" class="jd-input"></label>
        <button class="jd-btn" id="jdReload">Show</button>
        <button class="jd-btn" id="jdBackfill" title="Rebuild every day from the From date to today">Rebuild from From date</button>
        <span class="jd-status" id="jdStatus" role="status"></span>
      </div>
      <div class="jd-help" id="jdRules"></div>
      <div class="jd-toolbar">
        <label>Add event <input type="date" id="jdEvDate" class="jd-input"></label>
        <input type="text" id="jdEvLabel" class="jd-input" placeholder="e.g. CPI release" maxlength="80" aria-label="Event name">
        <button class="jd-btn" id="jdEvAdd">Add</button>
      </div>
      <div id="jdDays"><div class="jd-empty">Loading…</div></div>`;
    $('#jdFrom').value = daysAgo(30);
    $('#jdTo').value = isoDay(new Date());
    $('#jdEvDate').value = isoDay(new Date());
    $('#jdReload').addEventListener('click', loadDaily);
    $('#jdBackfill').addEventListener('click', backfill);
    $('#jdEvAdd').addEventListener('click', addEvent);
    loadDaily();
  }

  function labelChip(dim, v) {
    const tone = v === 'trend-up' || v === 'gap-up' ? 'up' : v === 'trend-down' || v === 'gap-down' ? 'down'
      : v === 'vix-high' || v === 'wide-range' ? 'warn' : '';
    return `<span class="chip ${tone}" title="${esc(META?.dimensions?.[dim] || dim)}">${esc(v)}</span>`;
  }

  function dayCard(d) {
    const L = d.labels || {};
    const chips = ['trend', 'gap', 'range', 'vix'].filter((k) => L[k]).map((k) => labelChip(k, L[k]))
      .concat((d.events || []).map((e) => `<span class="chip event">${esc(e.label)}</span>`)).join('');
    const m = d.market || {};
    const mk = (sym, f) => f ? `<span>${sym} <b class="${cls(f.ret_pct)}">${pct(f.ret_pct)}</b></span>` : '';
    const spy = m.SPY;
    const dt = d.daytype_counts || {};
    const mkt = [
      mk('SPY', spy), mk('QQQ', m.QQQ), mk('IWM', m.IWM),
      spy ? `<span>SPY gap <b>${pct(spy.gap_pct)}</b> · range <b>${pct(spy.range_pct).replace('+', '')}</b></span>` : '',
      m.VIX ? `<span>VIX <b>${m.VIX.close.toFixed(2)}</b> (prior ${m.VIX.prev_close.toFixed(2)})</span>` : '',
      Object.keys(dt).length ? `<span>Day types: Outside <b>${dt.Outside || 0}</b> · Neutral <b>${dt.Neutral || 0}</b> · Inside <b>${dt.Inside || 0}</b></span>` : '',
    ].filter(Boolean).join('');
    const accts = Object.entries(d.accounts || {}).sort((x, y) => x[0].localeCompare(y[0])).map(([n, t]) => {
      const a = acct(n);
      return `<span class="jd-acct"><span class="dot" style="background:${esc(a.color)}"></span>${esc(a.label)} <b class="${cls(t.pnl)}">${money(t.pnl)}</b> <span>(${t.trades})</span></span>`;
    }).join('') || '<span class="jd-acct">No closed trades recorded.</span>';
    const tags = (META?.note_tags || []).map((t) => `<button type="button" class="jd-tag" data-tag="${esc(t)}" aria-pressed="${(d.note_tags || []).includes(t)}">${esc(t)}</button>`).join('');
    return `
      <section class="jd-day" data-date="${esc(d.date)}">
        <div class="jd-day-head"><span class="jd-date">${esc(niceDate(d.date))}</span><span class="jd-chips">${chips}</span></div>
        ${mkt ? `<div class="jd-mkt">${mkt}</div>` : ''}
        <div class="jd-accts">${accts}</div>
        <div class="jd-notes">
          <div class="jd-tags" aria-label="Quick tags">${tags}</div>
          <div class="jd-note-row">
            <input type="text" class="jd-input jd-note" id="note-${esc(d.date)}" maxlength="2000" placeholder="What happened today? (optional)" value="${esc(d.notes || '')}" aria-label="Notes for ${esc(d.date)}">
            <button type="button" class="jd-btn jd-save">Save</button><span class="jd-saved">Saved</span>
          </div>
        </div>
      </section>`;
  }

  async function loadDaily() {
    const box = $('#jdDays');
    try {
      await meta();
      const d = await getJSON(`/api/journal/days?from=${$('#jdFrom').value}&to=${$('#jdTo').value}`);
      const r = d.rules || {};
      $('#jdRules').textContent = `Labels follow fixed rules: trend day when SPY's open-to-close move is at least ${Math.round(r.trend_body_min * 100)}% of its range, chop at or under ${Math.round(r.chop_body_max * 100)}%; gap day at ±${r.gap_min_pct}%; VIX from the prior close (low under ${r.vix_low}, high from ${r.vix_high}); wide or narrow range against SPY's 20-day median.`;
      paintStatus(d.snapshot);
      const days = (d.days || []).slice().reverse();
      box.innerHTML = days.length ? days.map(dayCard).join('')
        : `<div class="jd-empty">No days recorded in this range yet.<br>Press <b>Rebuild from From date</b> to fill them in from the trade history.</div>`;
      box.querySelectorAll('.jd-day').forEach(wireDay);
    } catch (e) {
      box.innerHTML = `<div class="jd-empty">Couldn't load the daily journal: ${esc(e.message)}</div>`;
    }
  }

  function wireDay(card) {
    const day = card.dataset.date;
    const save = async () => {
      const tags = [...card.querySelectorAll('.jd-tag[aria-pressed="true"]')].map((b) => b.dataset.tag);
      const notes = card.querySelector('.jd-note').value;
      try {
        await getJSON(`/api/journal/days/${day}/notes`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ notes, tags }) });
        const ok = card.querySelector('.jd-saved');
        ok.textContent = 'Saved'; ok.style.opacity = '1'; setTimeout(() => { ok.style.opacity = '0'; }, 1800);
      } catch (e) {
        const ok = card.querySelector('.jd-saved');
        ok.textContent = `Not saved: ${e.message}`; ok.style.opacity = '1';
      }
    };
    card.querySelectorAll('.jd-tag').forEach((b) => b.addEventListener('click', () => {
      b.setAttribute('aria-pressed', b.getAttribute('aria-pressed') === 'true' ? 'false' : 'true');
      save();
    }));
    card.querySelector('.jd-save').addEventListener('click', save);
    card.querySelector('.jd-note').addEventListener('keydown', (e) => { if (e.key === 'Enter') save(); });
  }

  function paintStatus(s) {
    const el = $('#jdStatus');
    if (!el || !s) return;
    if (s.running) el.textContent = `Rebuilding ${s.from} to ${s.to}…`;
    else if (s.error) el.textContent = `Last rebuild failed: ${s.error}`;
    else if (s.result) el.textContent = `Last rebuild: ${s.result.days} days, ${s.result.strategy_rows} strategy rows` + (s.result.errors?.length ? ` (${s.result.errors.length} warnings)` : '');
    else el.textContent = '';
  }

  async function backfill() {
    const btn = $('#jdBackfill');
    const from = $('#jdFrom').value || BACKFILL_FROM;
    btn.disabled = true;
    try {
      await getJSON('/api/journal/snapshot', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ from, to: isoDay(new Date()) }) });
      $('#jdStatus').textContent = `Rebuilding ${from} to today… this can take a few minutes the first time.`;
      const poll = async () => {
        const s = await getJSON('/api/journal/snapshot').catch(() => null);
        if (s && s.running) { setTimeout(poll, 5000); return; }
        btn.disabled = false;
        paintStatus(s);
        loadDaily();
      };
      setTimeout(poll, 4000);
    } catch (e) {
      btn.disabled = false;
      $('#jdStatus').textContent = `Couldn't start the rebuild: ${e.message}`;
    }
  }

  async function addEvent() {
    const date = $('#jdEvDate').value, label = $('#jdEvLabel').value.trim();
    if (!date || !label) { $('#jdStatus').textContent = 'Pick a date and name the event first.'; return; }
    try {
      await getJSON('/api/journal/events', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ date, label, kind: 'event' }) });
      $('#jdEvLabel').value = '';
      $('#jdStatus').textContent = `Added "${label}" on ${date}. It appears on that day after its next rebuild.`;
    } catch (e) {
      $('#jdStatus').textContent = `Couldn't add the event: ${e.message}`;
    }
  }

  // ── Regimes ─────────────────────────────────────────────────────────────
  let RG = { account: '5', strategy: '' };
  function initRegimes() {
    const root = document.getElementById('viewRegimes');
    root.innerHTML = `
      <div class="page-title">Regimes</div>
      <div class="page-sub">How each strategy does under different market conditions, from the daily journal.</div>
      <div class="jd-toolbar">
        <label>Account <select id="rgAcct" class="jd-select"></select></label>
        <label>From <input type="date" id="rgFrom" class="jd-input"></label>
        <label>To <input type="date" id="rgTo" class="jd-input"></label>
        <label>Min trades <input type="number" id="rgMin" class="jd-input" value="10" min="1" max="500" style="width:5rem"></label>
        <button class="jd-btn" id="rgReload">Show</button>
      </div>
      <div class="jd-help">Small samples mislead. A condition with fewer than 10 trades is greyed out, and nothing here is proof on its own: use it to pick conditions worth watching, then check them on new days.</div>
      <div id="rgDetail"></div>
      <div id="rgList"><div class="jd-empty">Loading…</div></div>`;
    $('#rgFrom').value = BACKFILL_FROM;
    $('#rgTo').value = isoDay(new Date());
    $('#rgReload').addEventListener('click', loadRegimeList);
    $('#rgAcct').addEventListener('change', () => { RG.account = $('#rgAcct').value; RG.strategy = ''; $('#rgDetail').innerHTML = ''; loadRegimeList(); });
    meta().then(() => {
      $('#rgAcct').innerHTML = META.accounts.map((a) => `<option value="${esc(a.num)}"${a.num === RG.account ? ' selected' : ''}>${esc(a.label)}</option>`).join('');
      if (!META.accounts.some((a) => a.num === RG.account) && META.accounts.length) RG.account = META.accounts[0].num;
      loadRegimeList();
    }).catch((e) => { $('#rgList').innerHTML = `<div class="jd-empty">Couldn't load accounts: ${esc(e.message)}</div>`; });
  }

  const rgQuery = () => `account=${encodeURIComponent(RG.account)}&from=${$('#rgFrom').value}&to=${$('#rgTo').value}`;

  async function loadRegimeList() {
    const box = $('#rgList');
    try {
      const d = await getJSON(`/api/journal/regimes?${rgQuery()}&min_trades=${encodeURIComponent($('#rgMin').value || 10)}`);
      if (!d.strategies.length) {
        box.innerHTML = `<div class="jd-empty">No strategy on ${esc(acct(RG.account).label)} has enough trades in this range.<br>If the Daily tab is empty, rebuild it there first.</div>`;
        return;
      }
      const flips = d.strategies.filter((s) => s.flipped).length;
      box.innerHTML = `
        <div class="jd-card">
          <h3>Strategies on ${esc(acct(RG.account).label)}</h3>
          <div class="jd-sub">${d.days} trading days. A strategy is flagged when its last ${d.flip_days} trading days and everything before have opposite signs, with at least ${d.flip_min_trades} trades on each side and a change with a t-stat of at least ${d.flip_min_t}. ${flips} flagged. Click a row for its regime breakdown.</div>
          <div class="jd-scroll tall"><table class="jd-table">
            <thead><tr><th>Strategy</th><th>Trades</th><th>Win %</th><th>% per trade</th><th>t-stat</th><th>Earlier</th><th>Last ${d.flip_days} days</th><th>Net</th></tr></thead>
            <tbody>${d.strategies.map((s) => `
              <tr class="jd-click" data-strategy="${esc(s.strategy)}" tabindex="0">
                <td>${esc(s.strategy)}${s.flipped ? `<span class="jd-flag ${esc(s.direction)}">${esc(s.direction)}</span>` : ''}</td>
                <td>${s.trades}</td><td>${share(s.win_rate)}</td>
                <td class="${cls(s.mean_pct)}">${pct(s.mean_pct, 3)}</td><td>${s.t == null ? '—' : s.t.toFixed(2)}</td>
                <td class="${cls(s.earlier_mean_pct)}">${pct(s.earlier_mean_pct, 3)}</td>
                <td class="${cls(s.recent_mean_pct)}">${pct(s.recent_mean_pct, 3)}</td>
                <td class="${cls(s.pnl)}">${money(s.pnl)}</td>
              </tr>`).join('')}</tbody>
          </table></div>
        </div>`;
      box.querySelectorAll('tr.jd-click').forEach((tr) => {
        const open = () => { RG.strategy = tr.dataset.strategy; loadRegimeDetail(); };
        tr.addEventListener('click', open);
        tr.addEventListener('keydown', (e) => { if (e.key === 'Enter') open(); });
      });
      if (RG.strategy) loadRegimeDetail();
    } catch (e) {
      box.innerHTML = `<div class="jd-empty">Couldn't load regimes: ${esc(e.message)}</div>`;
    }
  }

  function breakdownTable(rows, scale) {
    return `<table class="jd-table">
      <thead><tr><th>Condition</th><th>Trades</th><th>Win %</th><th>% per trade</th><th></th><th>t-stat</th><th>Net</th></tr></thead>
      <tbody>${rows.map((r) => {
        const w = r.mean_pct == null || !scale ? 0 : Math.min(60, Math.abs(r.mean_pct) / scale * 60);
        const bar = `<span class="jd-bar" style="width:${w.toFixed(1)}px;background:${r.mean_pct >= 0 ? '#7FE098' : '#ef8a80'}"></span>`;
        return `<tr class="${r.trades < 10 ? 'thin' : ''}"><td>${esc(r.value)}</td><td>${r.trades}</td><td>${share(r.win_rate)}</td>
          <td class="${r.trades < 10 ? '' : cls(r.mean_pct)}">${pct(r.mean_pct, 3)}</td><td style="text-align:left;width:64px">${bar}</td>
          <td>${r.t == null ? '—' : r.t.toFixed(2)}</td><td class="${r.trades < 10 ? '' : cls(r.pnl)}">${money(r.pnl)}</td></tr>`;
      }).join('')}</tbody></table>`;
  }

  async function loadRegimeDetail() {
    const box = $('#rgDetail');
    box.innerHTML = `<div class="jd-card"><div class="jd-empty">Loading ${esc(RG.strategy)}…</div></div>`;
    try {
      const d = await getJSON(`/api/journal/regimes?${rgQuery()}&strategy=${encodeURIComponent(RG.strategy)}`);
      const o = d.overall, f = d.flip, g = d.good_vs_bad;
      const allRows = Object.values(d.breakdown).flat().filter((r) => r.trades >= 10 && r.mean_pct != null);
      const scale = Math.max(0.0001, ...allRows.map((r) => Math.abs(r.mean_pct)));
      const flipLine = f.cutoff
        ? `Before ${esc(f.cutoff)}: ${pct(f.earlier.mean_pct, 3)} per trade over ${f.earlier.trades} trades. Since: ${pct(f.recent.mean_pct, 3)} over ${f.recent.trades}.` + (f.flipped ? ` <span class="jd-flag ${esc(f.direction)}">${esc(f.direction)}</span>` : '')
        : 'Not enough trading days yet to compare earlier and recent results.';
      const gvb = g.rows.slice(0, 8);
      box.innerHTML = `
        <div class="jd-card">
          <h3>${esc(d.strategy)} on ${esc(acct(d.account).label)}</h3>
          <div class="jd-sub">${o.trades} trades on ${o.days} days · win ${share(o.win_rate)} · ${pct(o.mean_pct, 3)} per trade · t-stat ${o.t == null ? '—' : o.t.toFixed(2)} · net <span class="${cls(o.pnl)}">${money(o.pnl)}</span><br>${flipLine}</div>
          <button class="jd-btn" id="rgClose">Close</button>
        </div>
        <div class="jd-card">
          <h3>What its winning days had in common</h3>
          <div class="jd-sub">Share of its ${g.win_days} winning days and ${g.loss_days} losing days that carried each label, biggest difference first.</div>
          ${gvb.length ? `<div class="jd-scroll"><table class="jd-table">
            <thead><tr><th>Label</th><th>Winning days</th><th>Losing days</th><th>Difference</th></tr></thead>
            <tbody>${gvb.map((r) => `<tr><td>${esc(d.dimensions[r.dimension] || r.dimension)}: ${esc(r.value)}</td><td>${share(r.win_share)}</td><td>${share(r.loss_share)}</td><td class="${cls(r.diff)}">${r.diff > 0 ? '+' : r.diff < 0 ? '−' : ''}${Math.round(Math.abs(r.diff) * 100)} pts</td></tr>`).join('')}</tbody>
          </table></div>` : '<div class="jd-empty">Needs both winning and losing days.</div>'}
        </div>
        <div class="jd-grid2">${Object.entries(d.breakdown).map(([dim, rows]) => `
          <div class="jd-card"><h3>${esc(d.dimensions[dim] || dim)}</h3><div class="jd-scroll">${breakdownTable(rows, scale)}</div></div>`).join('')}
        </div>`;
      $('#rgClose').addEventListener('click', () => { RG.strategy = ''; box.innerHTML = ''; });
      box.scrollIntoView({ behavior: 'smooth', block: 'start' });
    } catch (e) {
      box.innerHTML = `<div class="jd-card"><div class="jd-empty">Couldn't load ${esc(RG.strategy)}: ${esc(e.message)}</div></div>`;
    }
  }

  // ── Boot ────────────────────────────────────────────────────────────────
  document.querySelectorAll('.view-tab').forEach((b) => b.addEventListener('click', () => showView(b.dataset.view)));
  window.addEventListener('hashchange', () => showView(location.hash.slice(1) || 'weekly'));
  showView(location.hash.slice(1) || 'weekly');
})();
