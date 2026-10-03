// ===== Tab switching (Examples section) =====
document.querySelectorAll('.tab-header').forEach((btn) => {
  btn.addEventListener('click', () => {
    const tab = btn.dataset.tab;
    document.querySelectorAll('.tab-header').forEach((b) => b.classList.toggle('active', b === btn));
    document.querySelectorAll('.tab-panel').forEach((p) => {
      p.classList.toggle('active', p.dataset.panel === tab);
    });
  });
});

// ===== Install command variants =====
const installCmd = document.getElementById('install-cmd');
document.querySelectorAll('.install-variants button').forEach((b) => {
  b.addEventListener('click', () => {
    document.querySelectorAll('.install-variants button').forEach((x) => x.classList.remove('active'));
    b.classList.add('active');
    installCmd.textContent = b.dataset.cmd;
  });
});

// ===== Copy buttons =====
document.querySelectorAll('.copy-btn').forEach((b) => {
  b.addEventListener('click', async () => {
    const target = document.getElementById(b.dataset.copy);
    if (!target) return;
    try {
      await navigator.clipboard.writeText(target.textContent.trim());
      b.textContent = '✓';
      b.classList.add('copied');
      setTimeout(() => {
        b.textContent = '⧉';
        b.classList.remove('copied');
      }, 1500);
    } catch (e) {
      b.textContent = '✗';
      setTimeout(() => (b.textContent = '⧉'), 1500);
    }
  });
});

// ===== Back to top =====
const btt = document.getElementById('back-to-top');
window.addEventListener('scroll', () => {
  btt.hidden = window.scrollY < 400;
});
btt.addEventListener('click', () => window.scrollTo({ top: 0, behavior: 'smooth' }));

// ===== Smooth-scroll for anchor links =====
document.querySelectorAll('a[href^="#"]').forEach((a) => {
  a.addEventListener('click', (e) => {
    const id = a.getAttribute('href').slice(1);
    if (!id) return;
    const el = document.getElementById(id);
    if (!el) return;
    e.preventDefault();
    el.scrollIntoView({ behavior: 'smooth', block: 'start' });
  });
});

// ===== Playground =====

const $ = (id) => document.getElementById(id);

const pg = {
  worksheetKey: 'customers',
  output: 'dataframe',
  pageSize: 200,
  filterActive: true,
  filterRecent: false,
  filterRegion: false,
  running: false,
  cancelRequested: false,
  cancelled: false,
  rowsReceived: 0,
  pagesReceived: 0,
  pagesPlanned: 0,
};

const logEl = $('pg-log');
const statusEl = $('pg-status');
const progressFill = $('pg-progress-fill');
const progressText = $('pg-progress-text');
const resultEl = $('pg-result');

function resetLog() {
  logEl.innerHTML = '';
}
function appendLog(msg, level) {
  level = level || 'info';
  if (logEl.children.length === 1 && logEl.firstElementChild.classList.contains('pg-log-empty')) {
    logEl.innerHTML = '';
  }
  const t = new Date().toLocaleTimeString('en-GB', { hour12: false });
  const line = document.createElement('div');
  line.className = 'pg-log-line';
  const lvl = level.toUpperCase();
  line.innerHTML =
    '<span class="pg-log-time">' + t + '</span>' +
    '<span class="pg-log-msg">' +
      '<span class="lvl-' + level + '">[' + lvl + ']</span> ' +
      msg +
    '</span>';
  logEl.appendChild(line);
  logEl.scrollTop = logEl.scrollHeight;
}

function setStatus(text, cls) {
  statusEl.textContent = text;
  statusEl.className = 'pg-status' + (cls ? ' ' + cls : '');
}

function setProgress(pages, totalPages, rows) {
  const pct = totalPages > 0 ? Math.min(100, (pages / totalPages) * 100) : 0;
  progressFill.style.width = pct + '%';
  progressText.textContent = pages + ' / ' + totalPages + ' pages · ' + rows.toLocaleString() + ' rows';
}

function computePlannedPages(ws, pageSize) {
  return Math.max(1, Math.ceil(ws.totalRows / pageSize));
}

function getWorksheet() {
  return PG_DATA[pg.worksheetKey];
}

function buildFilterDescription() {
  const parts = [];
  if (pg.filterActive) parts.push('Status = "Active"');
  if (pg.filterRecent) parts.push('_updatedAt > "2025-01-01"');
  if (pg.filterRegion) parts.push('Region ∈ {"HK", "SZ"}');
  if (parts.length === 0) return 'NocolyFilter.quick() — no constraints';
  if (parts.length === 1) return 'NocolyFilter.quick(' + parts[0] + ')';
  return 'NocolyFilter.and_group([' + parts.join(', ') + '])';
}

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function renderResult(ws) {
  resultEl.hidden = false;
  let title = '';
  let body = '';

  if (pg.output === 'dataframe') {
    title = 'pandas.DataFrame.head() — first 5 rows';
    const cols = ws.columns.map(function (c) { return c.name; });
    body = '<table class="pg-table"><thead><tr>' +
      cols.map(function (c) { return '<th>' + c + '</th>'; }).join('') +
      '</tr></thead><tbody>';
    ws.rows.forEach(function (r) {
      body += '<tr>' + cols.map(function (c) {
        const v = r[c];
        if (v === null || v === undefined) return '<td class="null">null</td>';
        return '<td>' + escapeHtml(String(v)) + '</td>';
      }).join('') + '</tr>';
    });
    body += '</tbody></table>';
  } else if (pg.output === 'json') {
    title = 'WorksheetExporter.export(output_type="json") — ' + ws.rows.length + ' of ' + ws.totalRows.toLocaleString() + ' rows';
    body = '<pre style="margin:0">' + escapeHtml(JSON.stringify(ws.rows, null, 2)) + '</pre>';
  } else if (pg.output === 'csv') {
    title = 'WorksheetExporter.export(output_type="csv")';
    const cols = ws.columns.map(function (c) { return c.name; });
    let csv = cols.join(',') + '\n';
    ws.rows.forEach(function (r) {
      csv += cols.map(function (c) {
        const v = r[c];
        if (v === null || v === undefined) return '';
        return String(v).includes(',') ? '"' + v + '"' : v;
      }).join(',') + '\n';
    });
    body = '<pre style="margin:0;white-space:pre">' + escapeHtml(csv) + '</pre>';
  } else if (pg.output === 'parquet') {
    title = 'StreamingExporter.stream_async() → partitioned Parquet at /tmp/nocoly-export/' + ws.id + '/';
    let tree = '<div class="pg-tree">';
    tree += '<div class="tree-row tree-folder">/tmp/nocoly-export/' + ws.id + '/</div>';
    Object.keys(ws.partitions).forEach(function (part) {
      const count = ws.partitions[part];
      tree += '<div class="tree-row tree-folder">└── ' + part + '/</div>';
      tree += '<div class="tree-row tree-file">    └── data_0.parquet <span class="tree-meta">(' +
        count.toLocaleString() + ' rows · ' + (count * 12 / 1024).toFixed(1) + ' KiB)</span></div>';
    });
    tree += '</div>';
    body = tree;
  }

  resultEl.innerHTML = '<div class="pg-result-title">' + title + '</div>' + body;
}

function sleep(ms) {
  return new Promise(function (r) { setTimeout(r, ms); });
}

async function runExport() {
  if (pg.running) return;
  pg.running = true;
  pg.cancelRequested = false;
  pg.cancelled = false;
  pg.rowsReceived = 0;
  pg.pagesReceived = 0;
  $('pg-run').disabled = true;
  $('pg-cancel').disabled = false;
  $('pg-reset').disabled = true;
  resetLog();
  resultEl.hidden = true;

  const ws = getWorksheet();
  pg.pagesPlanned = computePlannedPages(ws, pg.pageSize);
  setStatus('Starting…', 'running');
  setProgress(0, pg.pagesPlanned, 0);

  appendLog('WorksheetExporter().export(host="https://your-nocoly-host", worksheet_id="' + ws.id + '", output_type="' + pg.output + '")', 'info');
  await sleep(180);

  appendLog('Filter: ' + buildFilterDescription(), 'info');
  await sleep(120);

  appendLog('Resolving credentials from env (NOCOLY_APP_KEY:***, NOCOLY_APP_SIGN:***)', 'ok');
  await sleep(100);

  appendLog('POST /api/v3/app/worksheets/' + ws.id + '/rows/list  →  200 OK  (page_size=' + pg.pageSize + ')', 'info');
  await sleep(150);

  for (let p = 1; p <= pg.pagesPlanned; p++) {
    if (pg.cancelRequested) {
      pg.cancelled = true;
      break;
    }
    const rowsInPage = Math.min(pg.pageSize, ws.totalRows - pg.rowsReceived);
    appendLog('page ' + p + '/' + pg.pagesPlanned + ' received  ·  has_more=' + (p < pg.pagesPlanned) + '  ·  ' + rowsInPage + ' rows', 'page');
    pg.pagesReceived = p;
    pg.rowsReceived = Math.min(ws.totalRows, p * pg.pageSize);
    setProgress(pg.pagesReceived, pg.pagesPlanned, pg.rowsReceived);

    if (p % 5 === 0 && pg.output === 'parquet') {
      appendLog('RowGroupBuffer flushed @ ' + (128 * 0.4).toFixed(1) + ' MiB → writing partition ' + ws.partitionBy + '=…', 'ok');
    }

    await sleep(220 + Math.random() * 180);
  }

  if (pg.cancelled) {
    setStatus('Cancelled', 'cancelled');
    appendLog('Cooperative cancel received between pages; raising JobCancelled (code="job_cancelled")', 'warn');
    pg.running = false;
    $('pg-run').disabled = false;
    $('pg-cancel').disabled = true;
    $('pg-reset').disabled = false;
    return;
  }

  appendLog('Server returned has_more=False — pagination loop complete', 'ok');
  appendLog('Schema inferred from page 1: ' + ws.columns.map(function (c) { return c.name + ':' + c.type; }).join(', '), 'ok');
  appendLog('ExportResult(rows_written=' + pg.rowsReceived.toLocaleString() + ', output_dir="/tmp/nocoly-export/' + ws.id + '")', 'ok');
  setStatus('Succeeded', 'success');
  setProgress(pg.pagesPlanned, pg.pagesPlanned, pg.rowsReceived);
  renderResult(ws);
  pg.running = false;
  $('pg-run').disabled = false;
  $('pg-cancel').disabled = true;
  $('pg-reset').disabled = false;
}

function resetPlayground() {
  if (pg.running) return;
  resetLog();
  resultEl.hidden = true;
  pg.rowsReceived = 0;
  pg.pagesReceived = 0;
  setProgress(0, pg.pagesPlanned, 0);
  setStatus('Idle');
  appendLog('Reset. Configure and click Run export.', 'info');
}

// Wire up playground controls
$('pg-worksheet').addEventListener('change', function (e) {
  pg.worksheetKey = e.target.value;
  if (!pg.running) resetPlayground();
});
$('pg-output').addEventListener('change', function (e) {
  pg.output = e.target.value;
});
$('pg-pagesize').addEventListener('input', function (e) {
  pg.pageSize = +e.target.value;
  $('pg-pagesize-label').textContent = pg.pageSize;
  if (!pg.running) {
    const ws = getWorksheet();
    pg.pagesPlanned = computePlannedPages(ws, pg.pageSize);
    setProgress(0, pg.pagesPlanned, 0);
  }
});
$('pg-f-active').addEventListener('change', function (e) { pg.filterActive = e.target.checked; });
$('pg-f-recent').addEventListener('change', function (e) { pg.filterRecent = e.target.checked; });
$('pg-f-region').addEventListener('change', function (e) { pg.filterRegion = e.target.checked; });
$('pg-run').addEventListener('click', runExport);
$('pg-cancel').addEventListener('click', function () {
  if (pg.running) {
    pg.cancelRequested = true;
    appendLog('POST /jobs/.../cancel received — flipping cancel_check flag', 'warn');
    $('pg-cancel').disabled = true;
  }
});
$('pg-reset').addEventListener('click', resetPlayground);

// Init
(function init() {
  const ws = getWorksheet();
  pg.pagesPlanned = computePlannedPages(ws, pg.pageSize);
  setProgress(0, pg.pagesPlanned, 0);
})();
