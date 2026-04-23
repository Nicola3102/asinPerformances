"""
将前端 New Listing 页面导出为单文件 HTML 快照。

默认页面：
  http://localhost:5173/trend/New%20Listing

默认输出（在 backend 目录运行时）：
  python3.11 -m app.services.report_pst --out ./charts/report_pst1.html
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

logger = logging.getLogger(__name__)

DEFAULT_URL = "http://localhost:5173/trend/New%20Listing"
DEFAULT_WAIT_MS = 180_000
DEFAULT_SETTLE_MS = 1_500
TREND_NEW_LISTING_CACHE_KEY = "asinPerformances.v4.trendNewListingJson"
TOOLTIP_LOOKBACK_DAYS = 35

READY_CHECK_JS = """
() => {
  const hasError = Boolean(
    document.querySelector('.trend-embed-error-title, .trend-embed-error-body')
  );
  if (hasError) return 'error';

  const pageReady = Boolean(document.querySelector('.trend-new-listing-page'));
  const loading = Boolean(document.querySelector('.trend-embed-loading'));
  const hasKpi = document.querySelectorAll('.trend-new-listing-kpi-card').length >= 2;
  const hasChart =
    Boolean(document.querySelector('.trend-new-listing-chart-wrap canvas')) ||
    Boolean(document.querySelector('.trend-new-listing-empty'));
  const hasTable =
    Boolean(document.querySelector('.trend-new-listing-table')) ||
    Boolean(document.querySelector('.trend-new-listing-table-empty'));

  return pageReady && !loading && hasKpi && hasChart && hasTable;
}
"""

EXTRACT_PAYLOAD_JS = """
async ({ cacheKey }) => {
  const tryParse = (raw) => {
    try {
      return raw ? JSON.parse(raw) : null;
    } catch {
      return null;
    }
  };

  const fetchJsonText = async (url) => {
    const resp = await fetch(url, { credentials: 'same-origin' });
    if (!resp.ok) {
      throw new Error(`拉取 new-listing JSON 失败: ${resp.status} @ ${url}`);
    }
    return await resp.text();
  };

  const fetchJson = async (url) => tryParse(await fetchJsonText(url));

  let base = tryParse(localStorage.getItem(cacheKey));
  if (!base || !base.views || !base.views.all) {
    base = await fetchJson('/api/trend/new-listing?format=json&json_views=all');
  }
  if (!base || !base.views || !base.views.all) {
    throw new Error('未获取到 all 视图数据');
  }

  const merged = {
    ...base,
    views: { ...(base.views || {}) },
  };
  const storeIds = Array.isArray(merged.storeIds) ? merged.storeIds : [];
  const missingIds = storeIds.filter((id) => !merged.views[String(id)]);
  for (const sid of missingIds) {
    const part = await fetchJson(`/api/trend/new-listing?format=json&json_views=store&store_id=${encodeURIComponent(String(sid))}`);
    if (part && part.views && typeof part.views === 'object') {
      merged.views = { ...merged.views, ...part.views };
    }
  }

  return JSON.stringify(merged);
}
"""


def _load_chart_js_embed_html() -> str:
    repo_root = Path(__file__).resolve().parents[3]
    candidates = [
        repo_root / "frontend" / "node_modules" / "chart.js" / "dist" / "chart.umd.min.js",
        repo_root / "frontend" / "node_modules" / "chart.js" / "dist" / "chart.umd.js",
    ]
    for path in candidates:
        if path.exists():
            logger.info("内联 Chart.js: %s", path)
            source = path.read_text(encoding="utf-8").replace("</", "<\\/")
            return f"<script>{source}</script>"
    logger.warning("未找到本地 Chart.js，回退到 CDN")
    return '<script src="https://cdn.jsdelivr.net/npm/chart.js@4.5.1/dist/chart.umd.min.js"></script>'


def _build_report_html(payload: dict, source_url: str, chart_js_tag: str) -> str:
    payload_json = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")
    source_url_json = json.dumps(source_url, ensure_ascii=False)
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="generator" content="app.services.report_pst">
  <title>New Listing Report PST</title>
  <style>
    :root {{
      color-scheme: dark;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      line-height: 1.5;
      font-weight: 400;
      color: #e8ecf1;
      background: #0f1419;
    }}
    * {{ box-sizing: border-box; }}
    html, body {{ margin: 0; padding: 0; background: #0f1419; }}
    body {{ padding: 1rem 1.25rem 2rem; }}
    .trend-new-listing-page {{
      color: #e8ecf1;
      max-width: 100%;
    }}
    .trend-new-listing-toolbar {{
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 0.75rem 1rem;
      margin-bottom: 1rem;
    }}
    .trend-new-listing-label {{
      font-size: 0.8rem;
      color: #94a3b8;
    }}
    .trend-new-listing-select {{
      padding: 0.45rem 0.65rem;
      border-radius: 8px;
      border: 1px solid rgba(61, 157, 255, 0.35);
      background: #1a2332;
      color: #e8ecf1;
      font-size: 0.9rem;
      min-width: 180px;
    }}
    .trend-new-listing-meta {{
      font-size: 0.78rem;
      color: #8b9cb3;
      line-height: 1.45;
      flex: 1 1 320px;
    }}
    .trend-new-listing-kpi {{
      display: flex;
      flex-wrap: wrap;
      gap: 0.75rem;
      margin-bottom: 0.75rem;
    }}
    .trend-new-listing-kpi-card {{
      background: #1a2332;
      border: 1px solid rgba(125, 211, 192, 0.12);
      border-radius: 10px;
      padding: 0.65rem 1rem;
      min-width: 140px;
    }}
    .trend-new-listing-kpi-title {{
      display: block;
      margin-bottom: 0.2rem;
      color: #8b9cb3;
      font-size: 0.78rem;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }}
    .trend-new-listing-kpi-card strong {{
      font-size: 1.35rem;
      font-weight: 600;
      color: #7dd3c0;
    }}
    .trend-new-listing-hint {{
      margin: 0 0 1rem;
      font-size: 0.85rem;
      color: #94a3b8;
      line-height: 1.5;
    }}
    .trend-new-listing-code {{
      font-size: 0.8em;
      padding: 0.1em 0.35em;
      border-radius: 4px;
      background: rgba(15, 23, 42, 0.6);
      color: #a5d4ff;
    }}
    .trend-new-listing-chart-wrap {{
      position: relative;
      height: min(560px, 68vh);
      min-height: 360px;
      width: 100%;
      background: #1a2332;
      border-radius: 12px;
      padding: 0.75rem;
      border: 1px solid rgba(61, 157, 255, 0.1);
      box-sizing: border-box;
    }}
    .trend-new-listing-empty {{
      margin: 0;
      padding: 2rem;
      color: #94a3b8;
      font-size: 0.9rem;
    }}
    .trend-new-listing-table-wrap {{
      margin-top: 1rem;
      background: #121a27;
      border: 1px solid rgba(61, 157, 255, 0.12);
      border-radius: 12px;
      padding: 0.85rem;
    }}
    .trend-new-listing-table-title {{
      margin: 0 0 0.6rem;
      font-size: 0.95rem;
      font-weight: 650;
      color: #e8ecf1;
    }}
    .trend-new-listing-table-caption {{
      margin: 0 0 0.75rem;
      font-size: 0.78rem;
      color: #8b9cb3;
      line-height: 1.45;
    }}
    .trend-new-listing-table-empty {{
      margin: 0;
      padding: 0.75rem 0.25rem;
      color: #94a3b8;
      font-size: 0.9rem;
    }}
    .trend-new-listing-table-scroll {{
      overflow: auto;
      max-width: 100%;
      min-height: 360px;
      max-height: min(78vh, 860px);
      border-radius: 10px;
      border: 1px solid rgba(125, 211, 192, 0.12);
    }}
    .trend-new-listing-table {{
      width: max(980px, 100%);
      border-collapse: collapse;
      background: #0e1520;
    }}
    .trend-new-listing-table th,
    .trend-new-listing-table td {{
      padding: 0.55rem 0.6rem;
      border-bottom: 1px solid rgba(148, 163, 184, 0.14);
      font-size: 0.84rem;
      color: #e8ecf1;
      text-align: right;
      white-space: nowrap;
    }}
    .trend-new-listing-table thead th {{
      position: sticky;
      top: 0;
      z-index: 2;
      background: #101b2a;
      color: #b8c6db;
      font-weight: 650;
      text-align: right;
    }}
    .trend-new-listing-table td.is-sticky-col,
    .trend-new-listing-table th.is-sticky-col {{
      position: sticky;
      left: 0;
      z-index: 3;
      background: #0f1726;
      text-align: left;
    }}
    .trend-new-listing-table th.is-sticky-col--2,
    .trend-new-listing-table td.is-sticky-col--2 {{
      left: 150px;
      z-index: 3;
      text-align: right;
    }}
    .trend-new-listing-table th.is-sticky-col--1,
    .trend-new-listing-table td.is-sticky-col--1 {{
      min-width: 150px;
    }}
    .trend-new-listing-table tbody tr:hover td {{
      background: rgba(61, 157, 255, 0.08);
    }}
    .trend-report-warning {{
      margin: 0.75rem 0 0;
      font-size: 0.78rem;
      color: #fbbf24;
    }}
  </style>
</head>
<body>
  <div class="trend-new-listing-page">
    <div class="trend-new-listing-toolbar">
      <label class="trend-new-listing-label" for="trend-nl-store">店铺</label>
      <select id="trend-nl-store" class="trend-new-listing-select"></select>
      <span id="trend-meta" class="trend-new-listing-meta"></span>
    </div>

    <div class="trend-new-listing-kpi">
      <div class="trend-new-listing-kpi-card">
        <span class="trend-new-listing-kpi-title">Total Asins</span>
        <strong id="kpi-total">0</strong>
      </div>
      <div class="trend-new-listing-kpi-card">
        <span class="trend-new-listing-kpi-title">Active Asins</span>
        <strong id="kpi-active">0</strong>
      </div>
    </div>

    <p class="trend-new-listing-hint">
      柱形为各上新批次（open_date）贡献的 sessions 堆叠；黑色折线为每日合计。悬停柱形可查看批次明细与当日合计。
      下方表格为完整数据，不再使用虚拟滚动，因此滚动到底部可以查看所有上新日。
      导出来源：
      <code class="trend-new-listing-code" id="source-url"></code>
    </p>

    <div class="trend-new-listing-chart-wrap">
      <canvas id="trend-chart" aria-label="New Listing chart"></canvas>
      <p id="chart-empty" class="trend-new-listing-empty" hidden>暂无图表数据。</p>
    </div>

    <div class="trend-new-listing-table-wrap">
      <h3 class="trend-new-listing-table-title">批次明细（上新数 &amp; 上新后每日 sessions）</h3>
      <p class="trend-new-listing-table-caption">
        前两列「上新日 / 上新 ASIN 数」来自 amazon_listing 的上新批次统计；后续列为导出时的 sessions 明细。
      </p>
      <div class="trend-new-listing-table-scroll" id="table-scroll">
        <table class="trend-new-listing-table" id="cohort-table"></table>
      </div>
      <p class="trend-report-warning" id="store-warning" hidden></p>
    </div>
  </div>

  <script>
    window.__REPORT_DATA__ = {payload_json};
    window.__REPORT_SOURCE_URL__ = {source_url_json};
  </script>
  {chart_js_tag}
  <script>
    (() => {{
      const payload = window.__REPORT_DATA__ || {{}};
      const sourceUrl = window.__REPORT_SOURCE_URL__ || '';
      const LOOKBACK_DAYS = {TOOLTIP_LOOKBACK_DAYS};
      const storeSelect = document.getElementById('trend-nl-store');
      const metaEl = document.getElementById('trend-meta');
      const totalEl = document.getElementById('kpi-total');
      const activeEl = document.getElementById('kpi-active');
      const sourceEl = document.getElementById('source-url');
      const tableEl = document.getElementById('cohort-table');
      const chartEmptyEl = document.getElementById('chart-empty');
      const storeWarningEl = document.getElementById('store-warning');
      const chartCanvas = document.getElementById('trend-chart');

      if (sourceEl) sourceEl.textContent = sourceUrl;

      const formatNum = (value) => Number(value ?? 0).toLocaleString('zh-CN');
      const formatSessionSharePercent = (numerator, denominator) => {{
        const n = Number(numerator ?? 0);
        const d = Number(denominator ?? 0);
        if (!Number.isFinite(n) || !Number.isFinite(d) || d <= 0) return '—';
        const pct = n / d * 100;
        const digits = pct !== 0 && Math.abs(pct) < 0.1 ? 4 : 2;
        return `${{pct.toFixed(digits)}}%`;
      }};
      const escapeHtml = (value) =>
        String(value ?? '')
          .replace(/&/g, '&amp;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;')
          .replace(/"/g, '&quot;')
          .replace(/'/g, '&#39;');

      const ymdAddDays = (ymd, deltaDays) => {{
        const head = String(ymd || '').slice(0, 10);
        if (!/^\\d{{4}}-\\d{{2}}-\\d{{2}}$/.test(head)) return null;
        const [y, m, d] = head.split('-').map(Number);
        const t = Date.UTC(y, m - 1, d) + deltaDays * 86400000;
        const u = new Date(t);
        const yy = u.getUTCFullYear();
        const mm = String(u.getUTCMonth() + 1).padStart(2, '0');
        const dd = String(u.getUTCDate()).padStart(2, '0');
        return `${{yy}}-${{mm}}-${{dd}}`;
      }};

      const parseBatchYmd = (label) => {{
        const m = /批次\\s+(\\d{{4}}-\\d{{2}}-\\d{{2}})/.exec(String(label || ''));
        return m ? m[1] : null;
      }};

      const cohortInWindow = (sessionYmd, cohortYmd) => {{
        const s = String(sessionYmd || '').slice(0, 10);
        const c = String(cohortYmd || '').slice(0, 10);
        const minY = ymdAddDays(s, -LOOKBACK_DAYS);
        if (!/^\\d{{4}}-\\d{{2}}-\\d{{2}}$/.test(s) || !/^\\d{{4}}-\\d{{2}}-\\d{{2}}$/.test(c) || !minY) return true;
        return c >= minY && c <= s;
      }};

      const views = payload && typeof payload.views === 'object' ? payload.views : {{}};
      const viewKeys = Object.keys(views).sort((a, b) => {{
        if (a === 'all') return -1;
        if (b === 'all') return 1;
        return Number(a) - Number(b);
      }});

      if (!viewKeys.length) {{
        metaEl.textContent = '未找到可导出的视图数据。';
        chartEmptyEl.hidden = false;
        chartCanvas.hidden = true;
        tableEl.innerHTML = '';
        return;
      }}

      storeSelect.innerHTML = viewKeys
        .map((key) => {{
          const label = key === 'all' ? '全部店铺' : `店铺 ${{key}}`;
          return `<option value="${{escapeHtml(key)}}">${{escapeHtml(label)}}</option>`;
        }})
        .join('');

      const missingStoreCount =
        Array.isArray(payload.storeIds) && payload.storeIds.length
          ? payload.storeIds.filter((id) => !(String(id) in views)).length
          : 0;
      if (missingStoreCount > 0) {{
        storeWarningEl.hidden = false;
        storeWarningEl.textContent =
          `当前导出文件仅包含已加载的视图；仍有 ${{missingStoreCount}} 个店铺未写入此导出文件。`;
      }}

      let chart = null;

      const renderTable = (view, cohortTrackDays) => {{
        const rows = Array.isArray(view.cohortTable) ? view.cohortTable : [];
        if (!rows.length) {{
          tableEl.innerHTML = '<tbody><tr><td class="trend-new-listing-table-empty">暂无表格数据。</td></tr></tbody>';
          return;
        }}
        const headerDays = Array.from({{ length: cohortTrackDays }}, (_, i) => `<th title="sessions 合计；括号内为 sessions / 上新 ASIN 数">第${{i + 1}}天</th>`).join('');
        const body = rows
          .map((row) => {{
            const cd = String(row?.cohortDate ?? '');
            const newAsin = Number(row?.newAsin ?? 0);
            const daySessions = Array.isArray(row?.daySessions) ? row.daySessions : [];
            const cells = Array.from(
              {{ length: cohortTrackDays }},
              (_, i) => `<td>${{formatNum(daySessions[i] ?? 0)}} (${{formatSessionSharePercent(daySessions[i] ?? 0, newAsin)}})</td>`
            ).join('');
            return `
              <tr>
                <td class="is-sticky-col is-sticky-col--1">${{escapeHtml(cd || '–')}}</td>
                <td class="is-sticky-col is-sticky-col--2">${{formatNum(newAsin)}}</td>
                ${{cells}}
              </tr>
            `;
          }})
          .join('');
        tableEl.innerHTML = `
          <thead>
            <tr>
              <th class="is-sticky-col is-sticky-col--1">上新日（PST）</th>
              <th class="is-sticky-col is-sticky-col--2">上新 ASIN 数</th>
              ${{headerDays}}
            </tr>
          </thead>
          <tbody>${{body}}</tbody>
        `;
      }};

      const renderChart = (view) => {{
        const labels = Array.isArray(view.labels) ? view.labels : [];
        const datasets = Array.isArray(view.datasets) ? view.datasets : [];
        const barDatasets = datasets.map((ds) => ({{
          type: 'bar',
          label: ds.label ?? '',
          data: Array.isArray(ds.data) ? ds.data.map((x) => Number(x ?? 0)) : [],
          backgroundColor: ds.backgroundColor,
          borderWidth: ds.borderWidth ?? 0,
          stack: ds.stack ?? 'sess',
          yAxisID: ds.yAxisID ?? 'y',
        }}));

        if (!labels.length || !barDatasets.length) {{
          chartCanvas.hidden = true;
          chartEmptyEl.hidden = false;
          if (chart) {{
            chart.destroy();
            chart = null;
          }}
          return;
        }}

        chartCanvas.hidden = false;
        chartEmptyEl.hidden = true;
        const lineTotal =
          Array.isArray(view.lineTotal) && view.lineTotal.length === labels.length
            ? view.lineTotal.map((n) => Number(n ?? 0))
            : labels.map((_, idx) => barDatasets.reduce((acc, ds) => acc + Number(ds.data[idx] ?? 0), 0));
        const maxY = Math.max(1, ...lineTotal) * 1.12;
        const chartData = {{
          labels,
          datasets: [
            ...barDatasets,
            {{
              type: 'line',
              label: '当日 sessions 合计',
              data: lineTotal,
              borderColor: '#111827',
              backgroundColor: 'transparent',
              borderWidth: 2.5,
              pointRadius: 4,
              pointBackgroundColor: '#111827',
              tension: 0.2,
              order: 100,
              yAxisID: 'y1',
            }},
          ],
        }};

        if (chart) chart.destroy();
        chart = new Chart(chartCanvas, {{
          type: 'bar',
          data: chartData,
          options: {{
            responsive: true,
            maintainAspectRatio: false,
            interaction: {{ mode: 'index', intersect: false }},
            plugins: {{
              legend: {{ position: 'top', labels: {{ color: '#d7e3f4' }} }},
              tooltip: {{
                filter: (tooltipItem) => {{
                  const ds = tooltipItem.chart.data.datasets[tooltipItem.datasetIndex] || {{}};
                  if (ds.type === 'line' || String(ds.label ?? '') === '当日 sessions 合计') {{
                    return true;
                  }}
                  const sessionYmd = labels[tooltipItem.dataIndex];
                  const cohortYmd = parseBatchYmd(String(ds.label ?? ''));
                  if (!cohortYmd) return true;
                  return cohortInWindow(sessionYmd, cohortYmd);
                }},
                callbacks: {{
                  footer: (items) => {{
                    if (!items.length) return '';
                    const idx = items[0].dataIndex;
                    const total = lineTotal[idx];
                    const day = Array.isArray(view.byDay) ? view.byDay[idx] : null;
                    const newAsinCount = Number(day?.newAsinCount ?? 0);
                    if (total == null) return '';
                    return `合计 ${{formatNum(total)}} sessions / 上新 ASIN 数 ${{formatNum(newAsinCount)}} / 占比 ${{formatSessionSharePercent(total, newAsinCount)}}`;
                  }},
                }},
              }},
            }},
            scales: {{
              x: {{
                stacked: true,
                ticks: {{ maxRotation: 45, minRotation: 0, color: '#b8c6db' }},
                grid: {{ color: 'rgba(148, 163, 184, 0.08)' }},
              }},
              y: {{
                stacked: true,
                beginAtZero: true,
                max: maxY,
                title: {{ display: true, text: 'Sessions（堆叠）', color: '#b8c6db' }},
                ticks: {{ color: '#b8c6db' }},
                grid: {{ color: 'rgba(148, 163, 184, 0.08)' }},
              }},
              y1: {{
                stacked: false,
                position: 'right',
                beginAtZero: true,
                max: maxY,
                grid: {{ drawOnChartArea: false }},
                title: {{ display: true, text: '合计（折线）', color: '#b8c6db' }},
                ticks: {{ color: '#b8c6db' }},
              }},
            }},
          }},
        }});
      }};

      const renderView = (key) => {{
        const view = views[key] || views.all;
        const kpi = view && typeof view.kpi === 'object' ? view.kpi : {{}};
        totalEl.textContent = formatNum(kpi.totalAsin ?? 0);
        activeEl.textContent = formatNum(kpi.activeAsin ?? 0);
        metaEl.textContent =
          `KPI：open_date > ${{payload.listingSince || '—'}} · 每批 ${{payload.cohortTrackDays ?? 30}} 日 · 横轴 ${{payload.sessionChartStart || '—'}}～${{payload.sessionChartEnd || '—'}}` +
          (payload.generatedAt ? ` · 生成 ${{payload.generatedAt}}` : '');
        renderChart(view || {{}});
        renderTable(view || {{}}, Math.max(1, Number(payload.cohortTrackDays ?? 30)));
      }};

      storeSelect.addEventListener('change', () => renderView(storeSelect.value));
      storeSelect.value = viewKeys.includes('all') ? 'all' : viewKeys[0];
      renderView(storeSelect.value);
    }})();
  </script>
</body>
</html>
"""


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="导出 New Listing 页面为静态 HTML 快照")
    parser.add_argument("--url", default=DEFAULT_URL, help="要导出的页面 URL")
    parser.add_argument(
        "--out",
        default="./charts/report_pst1.html",
        help="输出 HTML 文件路径",
    )
    parser.add_argument(
        "--wait-ms",
        type=int,
        default=DEFAULT_WAIT_MS,
        help="等待页面内容就绪的最长时间（毫秒）",
    )
    parser.add_argument(
        "--settle-ms",
        type=int,
        default=DEFAULT_SETTLE_MS,
        help="页面就绪后额外等待渲染稳定的时间（毫秒）",
    )
    parser.add_argument(
        "--viewport-width",
        type=int,
        default=1600,
        help="浏览器视口宽度",
    )
    parser.add_argument(
        "--viewport-height",
        type=int,
        default=2200,
        help="浏览器视口高度",
    )
    parser.add_argument(
        "--headful",
        action="store_true",
        help="使用可见浏览器运行，便于本地排查",
    )
    return parser.parse_args()


def _launch_browser(pw, *, headful: bool):
    launch_errors: list[str] = []
    launch_plan = [
        ("bundled chromium", {"headless": not headful}),
        ("local chrome", {"headless": not headful, "channel": "chrome"}),
    ]
    for label, kwargs in launch_plan:
        try:
            logger.info("启动浏览器: %s", label)
            return pw.chromium.launch(**kwargs)
        except Exception as exc:
            launch_errors.append(f"{label}: {exc}")
    raise RuntimeError(
        "无法启动浏览器。请先安装 Playwright 浏览器(`playwright install chromium`)或确认本机已安装 Google Chrome。\n"
        + "\n".join(launch_errors)
    )


def export_report(
    *,
    url: str,
    out_path: Path,
    wait_ms: int,
    settle_ms: int,
    viewport_width: int,
    viewport_height: int,
    headful: bool,
) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as pw:
        browser = _launch_browser(pw, headful=headful)
        try:
            page = browser.new_page(
                viewport={"width": viewport_width, "height": viewport_height},
                device_scale_factor=1,
            )
            logger.info("打开页面: %s", url)
            page.goto(url, wait_until="domcontentloaded", timeout=wait_ms)
            try:
                page.wait_for_load_state("networkidle", timeout=min(wait_ms, 10_000))
            except PlaywrightTimeoutError:
                logger.info("networkidle 未达成，改用页面内容就绪条件继续")
            page.wait_for_function(READY_CHECK_JS, timeout=wait_ms)
            page.wait_for_timeout(max(0, settle_ms))

            error_text = page.locator(".trend-embed-error-body").all_inner_texts()
            if error_text:
                raise RuntimeError("\n".join(t.strip() for t in error_text if t.strip()))

            payload_raw = page.evaluate(
                EXTRACT_PAYLOAD_JS,
                {
                    "cacheKey": TREND_NEW_LISTING_CACHE_KEY,
                },
            )
            payload = json.loads(payload_raw)
            if not isinstance(payload, dict) or not isinstance(payload.get("views"), dict):
                raise RuntimeError("导出的 New Listing 数据格式不正确")
            html = _build_report_html(payload, page.url, _load_chart_js_embed_html())
        finally:
            browser.close()

    out_path.write_text(html, encoding="utf-8")
    return out_path.resolve()


def main() -> int:
    setup_logging()
    args = parse_args()
    out_path = Path(args.out).expanduser()

    try:
        written = export_report(
            url=args.url,
            out_path=out_path,
            wait_ms=max(1_000, int(args.wait_ms)),
            settle_ms=max(0, int(args.settle_ms)),
            viewport_width=max(800, int(args.viewport_width)),
            viewport_height=max(600, int(args.viewport_height)),
            headful=bool(args.headful),
        )
    except PlaywrightTimeoutError as exc:
        logger.error("页面加载超时，请确认前端服务已启动且接口可访问: %s", exc)
        return 1
    except Exception as exc:
        logger.error("导出失败: %s", exc)
        return 1

    logger.info("已写入 %s", written)
    return 0


if __name__ == "__main__":
    sys.exit(main())
