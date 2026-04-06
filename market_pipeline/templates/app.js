'use strict';

const API = '';

/* ══ STATE ══ */
const SYMS = ['AAPL', 'MSFT', 'GOOGL'];
const COLORS = {
  AAPL: '#3b82f6',
  MSFT: '#22d3ee',
  GOOGL: '#22c55e'
};

let active = 'market';
let busy = false;
let chartMode = 'real';
let chartRange = '24h';
let visSyms = new Set(SYMS);
let mainChart = null;
let ringChart = null;
let succRate = 98.5;
let callCount = 6479;
let hoverIdx = -1;

const TV = {
  table: null,
  cols: [],
  page: 1,
  pageSize: 50,
  totalRows: 0,
  totalPages: 1,
  sortCol: null,
  sortDir: 'desc',
  search: '',
  filters: {}
};

/* ══ UTILS ══ */
let toastT = null;

function toast(msg) {
  clearTimeout(toastT);
  document.getElementById('toast-msg').textContent = msg;
  const el = document.getElementById('toast');
  el.classList.add('on');
  toastT = setTimeout(() => el.classList.remove('on'), 2600);
}

function setLoad(show, lbl = 'Lädt…') {
  document.getElementById('chip-load').style.display = show ? '' : 'none';
  document.getElementById('chip-load-lbl').textContent = lbl;
}

function pctCls(v) {
  const n = Number(v);
  return !Number.isFinite(n) || n === 0 ? 'c-neu' : n > 0 ? 'c-up' : 'c-dn';
}

function dtCls(d = '') {
  d = d.toLowerCase();
  if (d.includes('int') || d.includes('serial') || d.includes('bigint')) return 'c-num';
  if (d.includes('numeric') || d.includes('float') || d.includes('decimal')) return 'c-num';
  if (d.includes('timestamp') || d.includes('time zone') || d.includes('date')) return 'c-ts';
  if (d.includes('json')) return 'c-json';
  return '';
}

async function apiFetch(path) {
  try {
    const r = await fetch(API + path);
    if (!r.ok) throw new Error('HTTP ' + r.status);
    return await r.json();
  } catch (e) {
    console.error('API:', path, e);
    return null;
  }
}

/* ══ NAVIGATION ══ */
function goTo(id) {
  if (id === active || busy) return;

  busy = true;
  const prev = document.getElementById('scr-' + active);
  const next = document.getElementById('scr-' + id);

  if (!prev || !next) {
    busy = false;
    return;
  }

  prev.classList.remove('active');
  prev.classList.add('s-out-l');

  next.style.transition = 'none';
  next.style.opacity = '0';
  next.style.transform = 'translateX(36px)';
  next.style.visibility = 'hidden';
  next.classList.add('active');

  requestAnimationFrame(() =>
    requestAnimationFrame(() => {
      next.style.transition = '';
      next.style.opacity = '';
      next.style.transform = '';
      next.style.visibility = '';
    })
  );

  setTimeout(() => {
    prev.classList.remove('s-out-l', 's-out-r');
    busy = false;
  }, 240);

  active = id;
  document.querySelectorAll('.nb').forEach(b => {
    b.classList.toggle('on', b.dataset.nav === id);
  });
}

document.querySelectorAll('.nb').forEach(b => {
  b.addEventListener('click', () => goTo(b.dataset.nav));
});

/* ══ CHART CONTROLS ══ */
document.getElementById('mode-pg').querySelectorAll('[data-mode]').forEach(b => {
  b.addEventListener('click', () => {
    document.querySelectorAll('[data-mode]').forEach(x => x.classList.remove('on'));
    b.classList.add('on');
    chartMode = b.dataset.mode;
    renderChart();
  });
});

document.getElementById('range-pg').querySelectorAll('[data-range]').forEach(b => {
  b.addEventListener('click', () => {
    document.querySelectorAll('[data-range]').forEach(x => x.classList.remove('on'));
    b.classList.add('on');
    chartRange = b.dataset.range;
    renderChart();
  });
});

document.getElementById('resetZoomBtn').addEventListener('click', () => renderChart());
document.getElementById('fitViewBtn').addEventListener('click', () => renderChart());

document.querySelectorAll('#sym-row .sym-pill').forEach(b => {
  b.addEventListener('click', () => {
    if (b.dataset.sym === 'AAPL') return;

    if (visSyms.has(b.dataset.sym)) {
      visSyms.delete(b.dataset.sym);
    } else {
      visSyms.add(b.dataset.sym);
    }

    b.classList.toggle('on', visSyms.has(b.dataset.sym));
    renderChart();
  });
});

/* ══ SYNTHETIC DATA ══ */
function makeSeries(sym, tfH, nPts) {
  const now = Date.now();
  const step = (tfH * 3600000) / nPts;
  const prices = { AAPL: 187.32, MSFT: 421.55, GOOGL: 142.14 };
  const base = prices[sym] || 180;
  const seed = sym.split('').reduce((a, c) => a + c.charCodeAt(0), 0);
  const amp = base * (0.011 + (seed % 7) * 0.0017);
  const drift = ((seed % 5) - 2) * base * 0.00045;
  const pts = [];

  for (let i = nPts; i >= 0; i--) {
    const p = 1 - (i / nPts);
    const t = now - (i * step);
    const raw =
      base +
      Math.sin(p * Math.PI * 2.8 + seed * 0.03) * amp +
      Math.cos(p * Math.PI * 5.2 + seed * 0.011) * amp * 0.35 +
      Math.sin(p * Math.PI * 11.6 + seed * 0.017) * amp * 0.09 +
      drift * p * nPts * 0.28;

    pts.push({
      x: t,
      y: +raw.toFixed(2),
      price: +raw.toFixed(2)
    });
  }

  if (chartMode === 'norm' && pts.length) {
    const f = pts[0].price;
    return pts.map(p => ({
      x: p.x,
      y: +(((p.price - f) / f) * 100).toFixed(2),
      price: p.price
    }));
  }

  return pts;
}

const nPts = h => ({ 24: 56, 12: 40, 6: 28, 3: 20, 1: 14 }[h] || 40);

/* ══ CROSSHAIR PLUGIN ══ */
const xhair = {
  id: 'xhair',
  afterDatasetsDraw(chart) {
    if (hoverIdx < 0) return;

    const { ctx, chartArea, data } = chart;
    if (!chartArea) return;

    const meta0 = chart.getDatasetMeta(0);
    const pt0 = meta0?.data[hoverIdx];
    if (!pt0) return;

    const x = pt0.x;
    const y = pt0.y;

    ctx.save();

    ctx.beginPath();
    ctx.moveTo(x, chartArea.top);
    ctx.lineTo(x, chartArea.bottom);
    ctx.lineWidth = 1;
    ctx.strokeStyle = 'rgba(255,255,255,.25)';
    ctx.setLineDash([4, 5]);
    ctx.stroke();

    ctx.beginPath();
    ctx.moveTo(chartArea.left, y);
    ctx.lineTo(chartArea.right, y);
    ctx.lineWidth = 1;
    ctx.strokeStyle = 'rgba(59,130,246,.25)';
    ctx.setLineDash([3, 4]);
    ctx.stroke();

    ctx.setLineDash([]);

    data.datasets.forEach((ds, di) => {
      const meta = chart.getDatasetMeta(di);
      const pt = meta.data[hoverIdx];
      if (!pt) return;

      ctx.beginPath();
      ctx.arc(pt.x, pt.y, 8, 0, Math.PI * 2);
      ctx.strokeStyle = 'rgba(255,255,255,.2)';
      ctx.lineWidth = 1.5;
      ctx.stroke();

      ctx.beginPath();
      ctx.arc(pt.x, pt.y, 4.5, 0, Math.PI * 2);
      ctx.fillStyle = ds.borderColor;
      ctx.fill();

      ctx.beginPath();
      ctx.arc(pt.x, pt.y, 1.8, 0, Math.PI * 2);
      ctx.fillStyle = 'rgba(255,255,255,.9)';
      ctx.fill();
    });

    ctx.restore();
  }
};

Chart.register(xhair);

/* ══ MAIN CHART ══ */
function renderChart() {
  const canvas = document.getElementById('mainChart');
  if (!canvas) return;

  hoverIdx = -1;
  hideFpb();

  const h = Number(chartRange) || 24;
  const n = nPts(chartRange);
  const syms = [...visSyms].filter(s => SYMS.includes(s));

  const datasets = syms.map(sym => ({
    label: sym,
    data: makeSeries(sym, h, n),
    parsing: false,
    borderColor: COLORS[sym],
    backgroundColor: COLORS[sym] + '14',
    borderWidth: 2.6,
    pointRadius: 0,
    pointHoverRadius: 0,
    pointHitRadius: 22,
    tension: 0.22,
    fill: false
  }));

  if (mainChart) {
    mainChart.destroy();
    mainChart = null;
  }

  mainChart = new Chart(canvas, {
    type: 'line',
    data: { datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 260 },
      events: ['mousemove', 'mouseout', 'touchstart', 'touchmove', 'touchend'],
      interaction: { mode: 'index', intersect: false, axis: 'x' },
      plugins: {
        legend: { display: false },
        tooltip: { enabled: false }
      },
      scales: {
        x: {
          type: 'time',
          time: { tooltipFormat: 'dd.MM HH:mm' },
          ticks: {
            maxRotation: 0,
            autoSkip: true,
            color: '#4e6886',
            font: { family: 'IBM Plex Mono', size: 10 }
          },
          grid: { color: 'rgba(255,255,255,.04)' }
        },
        y: {
          ticks: {
            color: '#8aa4c8',
            callback: v => chartMode === 'real' ? '$' + Number(v).toFixed(0) : Number(v).toFixed(0) + '%',
            font: { family: 'IBM Plex Mono', size: 10 }
          },
          grid: { color: 'rgba(255,255,255,.04)' }
        }
      }
    }
  });

  canvas.addEventListener('mousemove', e => handleHover(e.clientX, e.clientY));
  canvas.addEventListener('mouseleave', () => {
    hoverIdx = -1;
    hideFpb();
    updateLegDefault();
    mainChart?.update('none');
  });

  canvas.addEventListener('touchstart', e => {
    e.preventDefault();
    const t = e.touches[0];
    handleHover(t.clientX, t.clientY);
  }, { passive: false });

  canvas.addEventListener('touchmove', e => {
    e.preventDefault();
    const t = e.touches[0];
    handleHover(t.clientX, t.clientY);
  }, { passive: false });

  canvas.addEventListener('touchend', () => {
    setTimeout(() => {
      hoverIdx = -1;
      hideFpb();
      updateLegDefault();
      mainChart?.update('none');
    }, 1200);
  });

  updateLegDefault();
}

function handleHover(cx, cy) {
  if (!mainChart) return;

  const pts = mainChart.getElementsAtEventForMode(
    { clientX: cx, clientY: cy, type: 'mousemove' },
    'index',
    { intersect: false },
    false
  );

  if (pts && pts.length) {
    hoverIdx = pts[0].index;
    showFpb(pts);
    updateLegHover(pts);
    mainChart.update('none');
  } else {
    hoverIdx = -1;
    hideFpb();
    updateLegDefault();
    mainChart.update('none');
  }
}

function showFpb(pts) {
  const box = document.getElementById('fpb');
  const rawTime = mainChart.data.datasets[0].data[pts[0].index]?.x;
  const time = rawTime
    ? new Date(rawTime).toLocaleString('de-DE', {
        day: '2-digit',
        month: '2-digit',
        hour: '2-digit',
        minute: '2-digit'
      })
    : ' ';

  box.innerHTML =
    `<div class="fp-time">${time}</div>` +
    pts.map(p => {
      const ds = mainChart.data.datasets[p.datasetIndex];
      const raw = ds.data[p.index] || {};
      const first = ds.data[0];

      let pctStr = '--';
      if (first && raw.price != null) {
        const pv = ((raw.price - first.price) / first.price) * 100;
        pctStr = (pv >= 0 ? '+' : '') + pv.toFixed(2) + '%';
      }

      const mv = chartMode === 'real'
        ? '$' + Number(raw.y ?? raw.price ?? 0).toFixed(2)
        : (raw.y >= 0 ? '+' : '') + Number(raw.y ?? 0).toFixed(2) + '%';

      const pc = pctStr.startsWith('+') ? '#22c55e' : '#ef4444';

      return `
        <div class="fp-row">
          <span style="width:8px;height:8px;border-radius:50%;background:${ds.borderColor};display:inline-block"></span>
          <span class="fp-sym">${ds.label}</span>
          <span class="fp-val">${mv}</span>
        </div>
        <div class="fp-sub">
          <span></span>
          <span class="fp-rp">real $${Number(raw.price ?? 0).toFixed(2)}</span>
          <span class="fp-pct" style="color:${pc}">${pctStr}</span>
        </div>
      `;
    }).join('');

  box.classList.add('show');
}

function hideFpb() {
  document.getElementById('fpb').classList.remove('show');
}

function perf(ds) {
  const f = ds?.data?.[0];
  const l = ds?.data?.[ds.data.length - 1];
  if (!f || !l) return '--';

  const x = ((l.price - f.price) / f.price) * 100;
  return (x >= 0 ? '+' : '') + x.toFixed(2) + '%';
}

function updateLegDefault() {
  const leg = document.getElementById('chart-legend');
  const dss = mainChart?.data.datasets || [];

  leg.innerHTML = dss.map(ds => {
    const last = ds.data?.[ds.data.length - 1];
    const mv = chartMode === 'real'
      ? (last ? '$' + Number(last.y).toFixed(2) : '--')
      : (last ? (last.y >= 0 ? '+' : '') + Number(last.y).toFixed(2) + '%' : '--');

    const p = perf(ds);
    const pc = p.startsWith('+') ? 'c-up' : 'c-dn';

    return `
      <div class="leg-row">
        <span style="width:9px;height:9px;border-radius:50%;background:${ds.borderColor};flex-shrink:0;display:inline-block"></span>
        <span class="leg-sym">${ds.label}</span>
        <span class="leg-price">${mv}</span>
        <span class="leg-chg ${pc}">${p}</span>
      </div>
    `;
  }).join('');
}

function updateLegHover(pts) {
  const leg = document.getElementById('chart-legend');

  leg.innerHTML = pts.map(p => {
    const ds = mainChart.data.datasets[p.datasetIndex];
    const raw = ds.data[p.index] || {};
    const first = ds.data[0];

    let pctStr = '--';
    if (first && raw.price != null) {
      const pv = ((raw.price - first.price) / first.price) * 100;
      pctStr = (pv >= 0 ? '+' : '') + pv.toFixed(2) + '%';
    }

    const mv = chartMode === 'real'
      ? '$' + Number(raw.y ?? 0).toFixed(2)
      : (raw.y >= 0 ? '+' : '') + Number(raw.y ?? 0).toFixed(2) + '%';

    const pc = pctStr.startsWith('+') ? 'c-up' : 'c-dn';

    return `
      <div class="leg-row">
        <span style="width:9px;height:9px;border-radius:50%;background:${ds.borderColor};flex-shrink:0;display:inline-block"></span>
        <span class="leg-sym">${ds.label}</span>
        <span class="leg-price">${mv}</span>
        <span class="leg-chg ${pc}">${pctStr}</span>
      </div>
    `;
  }).join('');
}

/* ══ RING CHART ══ */
function initRing() {
  const ctx = document.getElementById('ringChart').getContext('2d');

  ringChart = new Chart(ctx, {
    type: 'doughnut',
    data: {
      datasets: [{
        data: [succRate, 100 - succRate],
        backgroundColor: ['#22c55e', '#162130'],
        borderWidth: 0
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      cutout: '82%',
      plugins: {
        legend: { display: false },
        tooltip: { enabled: false }
      }
    }
  });
}

function updateRing() {
  if (!ringChart) return;

  ringChart.data.datasets[0].data = [succRate, 100 - succRate];
  ringChart.update('none');
  document.getElementById('ring-pct').textContent = succRate.toFixed(0) + '%';
}

function updatePipeLog() {
  const lat = Math.floor(Math.random() * 80 + 25);
  document.getElementById('pipe-log').innerHTML = [
    `⚡ Avg round-trip: ${lat}ms`,
    `📡 Refresh: ${new Date().toLocaleTimeString('de-DE')}`,
    `🔄 Pipeline: ${Math.floor(Math.random() * 60 + 10)} req/s`,
    `📊 Active streams: ${visSyms.size}`,
    `✓&nbsp; Connection: Stable`,
    `🎯 Success rate: ${succRate.toFixed(1)}%`
  ].join('<br>');
}

function tickPipeline() {
  callCount += Math.floor(Math.random() * 8 + 1);
  succRate = Math.min(99.9, Math.max(95, succRate + (Math.random() * 0.4 - 0.1)));
  updateRing();
  updatePipeLog();
}

/* ══ TABLES ══ */
let _allTables = [];
let _autoRefreshT = null;

async function loadTables(silent = false) {
  const tree = document.getElementById('db-tree');
  const q = document.getElementById('db-search').value.toLowerCase().trim();

  if (!silent && !q) {
    tree.innerHTML = '<div class="spin-wrap"><div class="spinner"></div>Lädt Tabellen…</div>';
  }

  const data = await apiFetch('/api/tables');

  if (!data || !data.tables) {
    tree.innerHTML = `<div class="err-box"><i class="fa-solid fa-triangle-exclamation"></i>Verbindung zur Datenbank fehlgeschlagen</div>`;
    document.getElementById('s-dot').style.background = 'var(--red)';
    document.getElementById('s-lbl').textContent = 'OFFLINE';
    return;
  }

  document.getElementById('s-dot').style.background = 'var(--green)';
  document.getElementById('s-lbl').textContent = 'ONLINE';

  _allTables = data.tables || [];

  document.getElementById('cnt-dim').textContent = _allTables.filter(t => t.type === 'dim').length;
  document.getElementById('cnt-fact').textContent = _allTables.filter(t => t.type === 'fact').length;
  document.getElementById('cnt-log').textContent = _allTables.filter(t => t.type === 'log').length;

  const list = q ? _allTables.filter(t => t.name.toLowerCase().includes(q)) : _allTables;

  if (!list.length) {
    tree.innerHTML = `<div style="padding:20px;text-align:center;color:var(--faint);font-family:'IBM Plex Mono',monospace;font-size:11px">Keine Treffer</div>`;
    return;
  }

  tree.innerHTML = list.map(t => {
    const tl = t.type === 'dim' ? 'DIM' : t.type === 'fact' ? 'FACT' : 'LOG';
    const rc = typeof t.row_count === 'number' ? t.row_count.toLocaleString('de-DE') : (t.row_count || '?');

    return `
      <div class="dbl-item" data-t="${t.name}">
        <i class="fa-solid fa-table-cells dbl-icon"></i>
        <div style="min-width:0">
          <div class="dbl-name">${t.name}</div>
          <div class="dbl-desc">${t.column_count || '?'} Spalten · ${rc} Zeilen</div>
        </div>
        <span class="dbt-type ${t.type}">${tl}</span>
        <span class="dbl-count">${rc}</span>
        <i class="fa-solid fa-chevron-right dbl-arrow"></i>
      </div>
    `;
  }).join('');

  tree.querySelectorAll('.dbl-item').forEach(el => {
    el.addEventListener('click', () => openViewer(el.dataset.t));
  });
}

document.getElementById('db-search').addEventListener('input', () => loadTables());

function showList() {
  document.getElementById('tbl-list-card').style.display = '';
  document.getElementById('tbl-viewer-card').style.display = 'none';
  _stopAutoRefresh();
}

function showViewer() {
  document.getElementById('tbl-list-card').style.display = 'none';
  document.getElementById('tbl-viewer-card').style.display = '';
}

document.getElementById('tv-back').addEventListener('click', showList);

async function openViewer(tableName) {
  goTo('tables');
  showViewer();

  TV.table = tableName;
  TV.page = 1;
  TV.search = '';
  TV.sortCol = null;
  TV.sortDir = 'desc';
  TV.filters = {};
  TV.cols = [];

  const tl = tableName.startsWith('dim_') ? 'DIM' : tableName.startsWith('log_') ? 'LOG' : 'FACT';
  const tc = tableName.startsWith('dim_') ? 'dim' : tableName.startsWith('log_') ? 'log' : 'fact';

  document.getElementById('tv-tname').textContent = tableName;
  document.getElementById('tv-tmeta').textContent = 'Lädt…';

  const badge = document.getElementById('tv-badge');
  badge.textContent = tl;
  badge.className = 'dbt-type ' + tc;

  document.getElementById('tv-search').value = '';
  document.getElementById('tv-af-tags').innerHTML = '';

  await loadData();
  buildFilterPanel();

  _stopAutoRefresh();
  _autoRefreshT = setInterval(() => {
    if (active === 'tables' && TV.table) loadData(true);
  }, 30000);
}

function buildFilterPanel() {
  const fields = document.getElementById('tv-flt-fields');
  const colNames = TV.cols.map(c => c.name);
  let html = '';

  const dateCols = ['called_at_utc', 'fetched_at_utc', 'candle_time_utc', 'quote_time_utc', 'report_date'];
  const hasDate = dateCols.some(d => colNames.includes(d));

  if (hasDate) {
    html += `
      <div class="sf-f" style="flex:1;min-width:130px">
        <div class="sf-lbl">Von Datum</div>
        <input class="sf-inp" type="date" id="fv-from"/>
      </div>
      <div class="sf-f" style="flex:1;min-width:130px">
        <div class="sf-lbl">Bis Datum</div>
        <input class="sf-inp" type="date" id="fv-to"/>
      </div>
    `;
  }

  if (colNames.includes('symbol_id')) {
    html += `
      <div class="sf-f">
        <div class="sf-lbl">Symbol ID</div>
        <input class="sf-inp" type="number" id="fv-sym" placeholder="z. B. 1 = AAPL"/>
      </div>
    `;
  }

  if (colNames.includes('indicator_id')) {
    html += `
      <div class="sf-f">
        <div class="sf-lbl">Indicator ID</div>
        <input class="sf-inp" type="number" id="fv-ind" placeholder="1=RSI 2=MACD"/>
      </div>
    `;
  }

  if (colNames.includes('interval_id')) {
    html += `
      <div class="sf-f">
        <div class="sf-lbl">Interval ID</div>
        <input class="sf-inp" type="number" id="fv-itv" placeholder="z. B. 3 = 1day"/>
      </div>
    `;
  }

  if (colNames.includes('endpoint')) {
    html += `
      <div class="sf-f">
        <div class="sf-lbl">Endpoint</div>
        <input class="sf-inp" id="fv-ep" placeholder="/quote …"/>
      </div>
    `;
  }

  if (colNames.includes('http_status')) {
    html += `
      <div class="sf-f">
        <div class="sf-lbl">HTTP Status</div>
        <input class="sf-inp" type="number" id="fv-st" placeholder="200"/>
      </div>
    `;
  }

  if (!html) {
    html = `
      <div style="font-size:10px;color:var(--faint);font-family:'IBM Plex Mono',monospace;padding:4px 0">
        Keine spezifischen Filter für diese Tabelle — Schnellsuche oben verwenden.
      </div>
    `;
  }

  fields.innerHTML = html;
}

async function loadData(silent = false) {
  const wrap = document.getElementById('tv-tbl-wrap');

  if (!silent) {
    wrap.innerHTML = '<div class="spin-wrap"><div class="spinner"></div>Lädt Daten…</div>';
    setLoad(true, TV.table + ' lädt…');
  }

  const p = new URLSearchParams({
    page: TV.page,
    page_size: TV.pageSize
  });

  if (TV.search) p.set('search', TV.search);
  if (TV.sortCol) p.set('sort_col', TV.sortCol);
  if (TV.sortDir) p.set('sort_dir', TV.sortDir);
  if (TV.filters.date_from) p.set('date_from', TV.filters.date_from);
  if (TV.filters.date_to) p.set('date_to', TV.filters.date_to);
  if (TV.filters.symbol_id) p.set('symbol_id', TV.filters.symbol_id);
  if (TV.filters.indicator_id) p.set('indicator_id', TV.filters.indicator_id);
  if (TV.filters.interval_id) p.set('interval_id', TV.filters.interval_id);
  if (TV.filters.endpoint) p.set('endpoint', TV.filters.endpoint);
  if (TV.filters.http_status) p.set('http_status', TV.filters.http_status);

  const data = await apiFetch(`/api/table/${TV.table}?${p}`);
  setLoad(false);

  if (!data) {
    if (!silent) {
      wrap.innerHTML = `<div class="err-box"><i class="fa-solid fa-triangle-exclamation"></i>Fehler beim Laden der Daten</div>`;
    }
    return;
  }

  const colsChanged =
    JSON.stringify(TV.cols.map(c => c.name)) !== JSON.stringify((data.columns || []).map(c => c.name));

  TV.cols = data.columns || [];
  TV.totalRows = data.total || 0;
  TV.totalPages = data.total_pages || Math.max(1, Math.ceil(TV.totalRows / TV.pageSize));
  TV.page = Math.min(TV.page, TV.totalPages);

  if (colsChanged) buildFilterPanel();

  const st = TV.totalRows ? ((TV.page - 1) * TV.pageSize + 1) : 0;
  const en = Math.min(TV.page * TV.pageSize, TV.totalRows);

  document.getElementById('tv-tmeta').textContent = `${TV.totalRows.toLocaleString('de-DE')} Zeilen · ${TV.cols.length} Spalten`;
  document.getElementById('qs-rows').textContent = TV.totalRows.toLocaleString('de-DE');
  document.getElementById('qs-cols').textContent = TV.cols.length;
  document.getElementById('qs-page').textContent = `${TV.page}/${TV.totalPages}`;
  document.getElementById('qs-show').textContent = data.rows ? data.rows.length : 0;
  document.getElementById('tv-info').textContent = `${TV.totalRows.toLocaleString('de-DE')} Zeilen · Seite ${TV.page}/${TV.totalPages} · ${st}–${en}`;
  document.getElementById('pg-lbl').textContent = `${TV.page} / ${TV.totalPages}`;
  document.getElementById('pg-prev').disabled = TV.page <= 1;
  document.getElementById('pg-next').disabled = TV.page >= TV.totalPages;

  renderTable(data);
  renderPages();
}

function renderTable(data) {
  const wrap = document.getElementById('tv-tbl-wrap');

  if (!data.rows || !data.columns) {
    wrap.innerHTML = `<div class="err-box">Keine Daten vorhanden</div>`;
    return;
  }

  const thead = data.columns.map(c => {
    const pk = c.is_pk ? 'h-pk' : c.is_fk ? 'h-fk' : '';
    const sc = TV.sortCol === c.name ? (TV.sortDir === 'asc' ? 's-asc' : 's-desc') : '';
    const ic = c.is_pk ? '🔑 ' : c.is_fk ? '🔗 ' : '';

    return `<th class="${pk} ${sc}" data-col="${c.name}" title="${c.dtype || ''}">${ic}${c.name}</th>`;
  }).join('');

  const tbody = data.rows.map((row, ri) => {
    const cells = data.columns.map(c => {
      const val = row[c.name];
      let txt;
      let cls = '';

      if (val === null || val === undefined) {
        txt = 'NULL';
        cls = 'c-null';
      } else if (typeof val === 'boolean') {
        txt = val ? 'true' : 'false';
        cls = val ? 'c-true' : 'c-false';
      } else if (typeof val === 'object') {
        try {
          txt = JSON.stringify(val);
        } catch {
          txt = String(val);
        }
        cls = 'c-json';
      } else {
        txt = String(val);
        cls = c.is_pk ? 'c-pk' : c.is_fk ? 'c-fk' : dtCls(c.dtype || '');
      }

      const disp = txt.length > 100 ? txt.slice(0, 97) + '…' : txt;

      return `<td class="${cls}" title="${txt.replace(/"/g, '&quot;').replace(/</g, '&lt;')}">${disp}</td>`;
    }).join('');

    return `<tr class="${ri % 2 === 0 ? '' : 'even'}">${cells}</tr>`;
  }).join('') || `
    <tr>
      <td colspan="${data.columns.length}" style="text-align:center;color:var(--faint);padding:20px;font-family:'IBM Plex Mono',monospace">
        Keine Ergebnisse für diesen Filter
      </td>
    </tr>
  `;

  wrap.innerHTML = `<table class="tv-t"><thead><tr>${thead}</tr></thead><tbody>${tbody}</tbody></table>`;

  wrap.querySelectorAll('th[data-col]').forEach(th => {
    th.addEventListener('click', () => {
      TV.sortDir = TV.sortCol === th.dataset.col && TV.sortDir === 'desc' ? 'asc' : 'desc';
      TV.sortCol = th.dataset.col;
      TV.page = 1;
      loadData();
    });
  });
}

function renderPages() {
  const el = document.getElementById('tv-pages');
  const p = TV.page;
  const tp = TV.totalPages;

  if (tp <= 1) {
    el.innerHTML = '';
    return;
  }

  let pills = [];
  const add = n => {
    if (n < 1 || n > tp) return;
    pills.push(`<button class="pg-pill${n === p ? ' on' : ''}" data-p="${n}" type="button">${n}</button>`);
  };
  const dots = () => pills.push(`<span class="pg-dots">…</span>`);

  add(1);
  if (p > 4) dots();

  for (let i = Math.max(2, p - 2); i <= Math.min(tp - 1, p + 2); i++) add(i);

  if (p < tp - 3) dots();
  add(tp);

  el.innerHTML = pills.join('');

  el.querySelectorAll('.pg-pill[data-p]').forEach(btn => {
    btn.addEventListener('click', () => {
      TV.page = +btn.dataset.p;
      loadData();
    });
  });
}

function _stopAutoRefresh() {
  if (_autoRefreshT) {
    clearInterval(_autoRefreshT);
    _autoRefreshT = null;
  }
}

/* ══ FILTER EVENTS ══ */
document.getElementById('tv-flt-toggle').addEventListener('click', () => {
  const body = document.getElementById('tv-flt-body');
  const arrow = document.getElementById('tv-flt-arrow');
  const open = body.style.display === 'flex';

  body.style.display = open ? 'none' : 'flex';
  arrow.classList.toggle('open', !open);
});

document.getElementById('tv-flt-apply').addEventListener('click', () => {
  TV.filters = {};
  TV.page = 1;

  const g = id => {
    const e = document.getElementById(id);
    return (e && e.value.trim()) || '';
  };

  const from = g('fv-from');
  const to = g('fv-to');
  if (from) TV.filters.date_from = from;
  if (to) TV.filters.date_to = to;

  const sym = g('fv-sym');
  if (sym) TV.filters.symbol_id = sym;

  const ind = g('fv-ind');
  if (ind) TV.filters.indicator_id = ind;

  const itv = g('fv-itv');
  if (itv) TV.filters.interval_id = itv;

  const ep = g('fv-ep');
  if (ep) TV.filters.endpoint = ep;

  const st = g('fv-st');
  if (st) TV.filters.http_status = st;

  document.getElementById('tv-af-tags').innerHTML =
    Object.entries(TV.filters).map(([k, v]) => `<span class="af-tag">${k}: ${v}</span>`).join('');

  loadData();
});

document.getElementById('tv-flt-reset').addEventListener('click', () => {
  document.querySelectorAll('#tv-flt-fields .sf-inp').forEach(el => el.value = '');
  TV.filters = {};
  TV.page = 1;
  document.getElementById('tv-af-tags').innerHTML = '';
  loadData();
});

let searchT = null;
document.getElementById('tv-search').addEventListener('input', e => {
  clearTimeout(searchT);
  searchT = setTimeout(() => {
    TV.search = e.target.value.trim();
    TV.page = 1;
    loadData();
  }, 400);
});

document.getElementById('pg-prev').addEventListener('click', () => {
  if (TV.page > 1) {
    TV.page--;
    loadData();
  }
});

document.getElementById('pg-next').addEventListener('click', () => {
  if (TV.page < TV.totalPages) {
    TV.page++;
    loadData();
  }
});

/* ══ REFRESH BUTTON ══ */
let spinning = false;

document.getElementById('refreshBtn').addEventListener('click', async () => {
  if (spinning) return;

  spinning = true;
  const ic = document.getElementById('ref-ic');

  ic.style.transition = 'transform .6s';
  ic.style.transform = 'rotate(360deg)';

  if (active === 'market') {
    renderChart();
  } else if (active === 'pipe') {
    tickPipeline();
  } else if (active === 'tables') {
    if (TV.table) {
      await loadData();
    } else {
      await loadTables();
    }
  }

  setTimeout(() => {
    ic.style.transform = '';
    ic.style.transition = '';
    spinning = false;
  }, 700);

  toast('Aktualisiert');
});

/* ══ CLOCK ══ */
setInterval(() => {
  document.getElementById('s-time').textContent = new Date().toLocaleTimeString('de-DE', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  });
}, 1000);

/* ══ BOOT ══ */
initRing();
updatePipeLog();
renderChart();
loadTables();
setInterval(tickPipeline, 24000);
