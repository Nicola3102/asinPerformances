"""
线上 amazon_sales_and_traffic_daily 按日 sessions + amazon_search_data 按周 impressions，双 Y 轴折线图 HTML。

- Sessions：按 store_id、DATE(current_date) 汇总（与 weekly_upload_asin_date 一致）。
- Impressions：当同时传入 --start-date 与 --end-date 时查询：
    SELECT asd.store_id, asd.week_no, SUM(asd.impression_count),
           MIN(DATE(asd.start_date)), MAX(DATE(asd.start_date))
    FROM amazon_search_data asd
    WHERE DATE(asd.start_date) BETWEEN :start AND :end
    GROUP BY asd.store_id, asd.week_no
  每个 week_no 的数据点画在该周 MIN~MAX(start_date) 的日历中点（如 2/22–2/28 → 2/25），
  点上标注 week_no；点击点弹出该周详情。

用法（backend 目录）：
  python3.11 -m app.services.weekly_upload_asin_date_add_impression \\
    --out ./charts/traffic_and_impression.html \\
    --start-date 2026-02-22 --end-date 2026-04-01
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from sqlalchemy import text

from app.config import settings
from app.logging_config import setup_logging
from app.online_engine import get_online_engine

logger = logging.getLogger(__name__)


def _parse_ymd(s: str) -> date:
    return datetime.strptime(str(s).strip(), "%Y-%m-%d").date()


def _cell_date(v) -> date:
    if v is None:
        raise ValueError("null date")
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return _parse_ymd(str(v)[:10])


def _iter_dates(start: date, end: date) -> list[date]:
    if start > end:
        return []
    out, cur = [], start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def _week_mid_date(d_min: date, d_max: date) -> date:
    """当周标注日：日历区间 [d_min, d_max] 的中点（含端点，与 2/22–2/28 → 2/25 一致）。"""
    span = (d_max - d_min).days
    return d_min + timedelta(days=span // 2)

def _week_no_to_week_start(week_no: str) -> date:
    """
    与 groupA_impression.py 的 _date_to_week_no 口径一致：周日为一周开始。
    week_no 格式：YYYYWW（WW 为 2 位周序号）。
    """
    wn = str(week_no).strip()
    if not wn.isdigit() or len(wn) < 6:
        raise ValueError(f"Invalid week_no: {week_no!r}")
    year = int(wn[:4])
    week_num = int(wn[4:])  # 允许不是严格两位，但需可转 int
    # 注意：amazon_search_data.week_no 与 groupA_impression 的 week_no 存在 1 的偏移。
    # 你给出的例子：你期望 week_no=202609 对应 2026-02-22~02-28；
    # 但 groupA 口径下 2026-02-22 的 week_no 是 202608，因此这里按 groupA_week_no = week_no - 1 修正。
    groupa_week_num = week_num - 1

    def _groupa_first_sunday(y: int) -> date:
        jan1 = date(y, 1, 1)
        return jan1 + timedelta(days=(6 - jan1.weekday()) % 7)

    def _groupa_last_week_num(y: int) -> int:
        first = _groupa_first_sunday(y)
        first_next = _groupa_first_sunday(y + 1)
        last_week_start = first_next - timedelta(days=7)
        return (last_week_start - first).days // 7 + 1

    if groupa_week_num <= 0:
        # 跨年兜底：落到上一年的最后一周
        prev_year = year - 1
        groupa_week_num = _groupa_last_week_num(prev_year)
        year = prev_year

    first_sunday = _groupa_first_sunday(year)
    return first_sunday + timedelta(weeks=groupa_week_num - 1)

def _week_no_to_week_range(week_no: str) -> tuple[date, date, date]:
    """
    返回 (d_min, d_max, mid)：
    - d_min：周开始（周日）
    - d_max：周结束（周六）
    - mid：标注点（周中点 = +3）
    """
    ws = _week_no_to_week_start(week_no)
    we = ws + timedelta(days=6)
    mid = ws + timedelta(days=3)
    return ws, we, mid


def _label_index_of_date(date_to_idx: dict[date, int], labels: list[date], target: date) -> int:
    """
    首选精确匹配；若标签中不存在该日期，则退化到最近点。
    这样可避免同一横坐标聚合到多个 week_no（你遇到的 202610/202611 点在一起）。
    """
    if target in date_to_idx:
        return date_to_idx[target]
    if not labels:
        return 0
    best_i = 0
    best_d = abs((labels[0] - target).days)
    for i, d in enumerate(labels):
        dist = abs((d - target).days)
        if dist < best_d:
            best_d = dist
            best_i = i
    return best_i


def fetch_traffic_daily_by_store(
    start_date: date | None,
    end_date: date | None,
) -> tuple[list[date], dict[int | None, dict[date, int]], list[int]]:
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        raise ValueError("Online DB 未配置：需设置 online_db_host, online_db_user 等")

    where_parts = []
    params: dict = {}
    if start_date is not None:
        where_parts.append("DATE(asatd.`current_date`) >= :d0")
        params["d0"] = start_date.strftime("%Y-%m-%d")
    if end_date is not None:
        where_parts.append("DATE(asatd.`current_date`) <= :d1")
        params["d1"] = end_date.strftime("%Y-%m-%d")
    where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""

    sql = text(
        f"""
        SELECT asatd.store_id,
               SUM(COALESCE(asatd.sessions, 0)) AS total_sessions,
               DATE(asatd.`current_date`) AS d
        FROM amazon_sales_and_traffic_daily AS asatd
        {where_sql}
        GROUP BY asatd.store_id, DATE(asatd.`current_date`)
        ORDER BY d ASC, asatd.store_id ASC
        """
    )

    per_store: dict[int, dict[date, int]] = {}
    all_dates: set[date] = set()

    with get_online_engine().connect() as conn:
        rows = conn.execute(sql, params).fetchall()

    for r in rows:
        sid = int(r[0]) if r[0] is not None else None
        if sid is None:
            continue
        total = int(r[1] or 0)
        d = _cell_date(r[2])
        all_dates.add(d)
        per_store.setdefault(sid, {})[d] = total

    sorted_dates = sorted(all_dates)
    store_ids = sorted(per_store.keys())

    totals: dict[date, int] = {d: 0 for d in sorted_dates}
    for sid in store_ids:
        for d, v in per_store[sid].items():
            totals[d] = totals.get(d, 0) + v

    series_map: dict[int | None, dict[date, int]] = {None: totals}
    for sid in store_ids:
        series_map[sid] = per_store[sid]

    return sorted_dates, series_map, store_ids


def fetch_impression_weekly(
    start_date: date,
    end_date: date,
) -> tuple[dict[int, list[dict]], list[dict]]:
    """
    返回 (per_store_weeks, all_stores_weeks)。
    每项: week_no, impressions, d_min, d_max, mid (iso), store_id(单店时)。
    """
    if not settings.ONLINE_DB_HOST or not settings.ONLINE_DB_USER:
        raise ValueError("Online DB 未配置")

    sql_by_store = text(
        """
        SELECT asd.store_id,
               asd.week_no,
               SUM(COALESCE(asd.impression_count, 0)) AS total_impression
        FROM amazon_search_data AS asd
        WHERE asd.start_date BETWEEN :d0 AND :d1
        GROUP BY asd.store_id, asd.week_no
        ORDER BY asd.week_no ASC, asd.store_id ASC
        """
    )
    sql_all_weeks = text(
        """
        SELECT asd.week_no,
               SUM(COALESCE(asd.impression_count, 0)) AS total_impression
        FROM amazon_search_data AS asd
        WHERE asd.start_date BETWEEN :d0 AND :d1
        GROUP BY asd.week_no
        ORDER BY asd.week_no ASC
        """
    )
    params = {
        "d0": start_date.strftime("%Y-%m-%d"),
        "d1": end_date.strftime("%Y-%m-%d"),
    }

    per_store: dict[int, list[dict]] = {}
    all_by_wn: dict[str, int] = {}

    with get_online_engine().connect() as conn:
        rows = conn.execute(sql_by_store, params).fetchall()
        rows_all = conn.execute(sql_all_weeks, params).fetchall()

    for r in rows_all:
        wn = str(r[0]).strip() if r[0] is not None else ""
        if not wn:
            continue
        all_by_wn[wn] = int(r[1] or 0)

    for r in rows:
        sid = int(r[0]) if r[0] is not None else None
        if sid is None:
            continue
        wn = str(r[1]).strip() if r[1] is not None else ""
        if not wn:
            continue
        imp = int(r[2] or 0)
        item = {
            "week_no": wn,
            "impressions": imp,
            "store_id": sid,
        }
        d_min, d_max, mid = _week_no_to_week_range(wn)
        item["d_min"] = d_min.isoformat()
        item["d_max"] = d_max.isoformat()
        item["mid"] = mid.isoformat()
        per_store.setdefault(sid, []).append(item)

    all_weeks: list[dict] = []
    for wn in sorted(all_by_wn.keys()):
        imp_total = int(all_by_wn.get(wn, 0) or 0)
        d0, d1, mid = _week_no_to_week_range(wn)
        all_weeks.append(
            {
                "week_no": wn,
                "impressions": imp_total,
                "d_min": d0.isoformat(),
                "d_max": d1.isoformat(),
                "mid": mid.isoformat(),
            }
        )

    logger.info(
        "[ImpressionWeekly] range=%s..%s store_groups=%s aggregate_weeks=%s",
        params["d0"],
        params["d1"],
        len(per_store),
        len(all_weeks),
    )
    return per_store, all_weeks


def _merge_label_dates(
    traffic_dates: list[date],
    imp_weeks_all: list[dict],
    range_start: date | None,
    range_end: date | None,
) -> list[date]:
    s: set[date] = set(traffic_dates)
    for w in imp_weeks_all:
        s.add(_parse_ymd(w["mid"]))
        s.add(_parse_ymd(w["d_min"]))
        s.add(_parse_ymd(w["d_max"]))
    if range_start is not None and range_end is not None:
        s.update(_iter_dates(range_start, range_end))
    return sorted(s)


def _series_for_labels(m: dict[date, int], labels: list[date]) -> list[int]:
    return [int(m.get(d, 0) or 0) for d in labels]


def _impression_line_for_labels(
    weeks: list[dict],
    labels: list[date],
) -> tuple[list[int | None], list[dict | None]]:
    """与 labels 对齐的 impression 值与点击用 meta（同 index 可合并多周）。"""
    vals: list[int | None] = [None] * len(labels)
    metas: list[dict | None] = [None] * len(labels)
    date_to_idx = {d: i for i, d in enumerate(labels)}
    for w in weeks:
        mid_d = _parse_ymd(w["mid"])
        idx = _label_index_of_date(date_to_idx, labels, mid_d)
        imp = int(w["impressions"])
        vals[idx] = (vals[idx] or 0) + imp
        entry = {
            "week_no": w["week_no"],
            "impressions": imp,
            "d_min": w["d_min"],
            "d_max": w["d_max"],
            "mid": w["mid"],
        }
        if w.get("store_id") is not None:
            entry["store_id"] = w["store_id"]
        if metas[idx] is None:
            metas[idx] = {"weeks": [entry]}
        else:
            metas[idx]["weeks"].append(entry)
    return vals, metas


def build_chart_payload(
    labels: list[date],
    series_map: dict[int | None, dict[date, int]],
    store_ids: list[int],
    impression_per_store: dict[int, list[dict]],
    impression_all: list[dict],
    impression_enabled: bool,
) -> dict:
    lab_iso = [d.isoformat() for d in labels]

    def sessions_series(key: int | None) -> list[int]:
        return _series_for_labels(series_map.get(key, {}), labels)

    imp_all_vals, imp_all_meta = _impression_line_for_labels(impression_all, labels)
    by_store_imp: dict[str, list[int | None]] = {}
    by_store_meta: dict[str, list[dict | None]] = {}
    for sid in store_ids:
        wks = impression_per_store.get(sid, [])
        v, m = _impression_line_for_labels(wks, labels)
        by_store_imp[str(sid)] = v
        by_store_meta[str(sid)] = m

    return {
        "labels": lab_iso,
        "store_ids": store_ids,
        "all_data": sessions_series(None),
        "by_store": {str(sid): sessions_series(sid) for sid in store_ids},
        "impressionEnabled": impression_enabled,
        "impression_all": imp_all_vals,
        "impression_all_meta": imp_all_meta,
        "impression_by_store": by_store_imp,
        "impression_meta_by_store": by_store_meta,
    }


def render_html(payload: dict) -> str:
    data_json = json.dumps(payload, ensure_ascii=False)
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Sessions + 周 Impression</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2.2.0/dist/chartjs-plugin-datalabels.min.js"></script>
  <style>
    :root {{
      --bg: #0f1419;
      --panel: #1a2332;
      --text: #e8ecf1;
      --muted: #8b9cb3;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      font-family: "IBM Plex Sans", "Segoe UI", system-ui, sans-serif;
      background: var(--bg);
      color: var(--text);
    }}
    .wrap {{
      max-width: 1100px;
      margin: 0 auto;
      padding: 1.75rem 1.25rem 2.5rem;
    }}
    h1 {{
      font-weight: 600;
      font-size: 1.35rem;
      margin: 0 0 0.35rem;
    }}
    .sub {{ color: var(--muted); font-size: 0.875rem; margin-bottom: 1rem; line-height: 1.5; }}
    .toolbar {{
      display: flex;
      flex-wrap: wrap;
      gap: 0.75rem 1.25rem;
      margin-bottom: 1rem;
      padding: 0.85rem 1rem;
      background: var(--panel);
      border-radius: 10px;
      border: 1px solid rgba(125, 211, 192, 0.12);
    }}
    label {{ font-size: 0.8rem; color: var(--muted); }}
    select {{
      background: #243044;
      color: var(--text);
      border: 1px solid rgba(61, 157, 255, 0.35);
      border-radius: 6px;
      padding: 0.45rem 0.65rem;
      font-size: 0.9rem;
      min-width: 200px;
    }}
    .chart-box {{
      background: var(--panel);
      border-radius: 12px;
      padding: 1rem 0.75rem 1.25rem;
      border: 1px solid rgba(61, 157, 255, 0.1);
    }}
    canvas {{ max-height: 440px; }}
    #detailPanel {{
      display: none;
      margin-top: 1rem;
      padding: 1rem 1.1rem;
      background: #243044;
      border-radius: 10px;
      border: 1px solid rgba(255, 179, 71, 0.35);
      font-size: 0.9rem;
      line-height: 1.55;
    }}
    #detailPanel h3 {{ margin: 0 0 0.5rem; font-size: 1rem; color: #ffb347; }}
    #detailPanel .row {{ margin: 0.35rem 0; }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Traffic daily + 周 Impression</h1>
    <p class="sub">
      绿线/填色：amazon_sales_and_traffic_daily 按日 <code>SUM(sessions)</code>（左轴）。<br />
      橙点折线：amazon_search_data 在 <code>start_date</code> 区间内按 <code>store_id + week_no</code> 汇总
      <code>SUM(impression_count)</code>（右轴）；点取当周 <code>start_date</code> 最小～最大日期的<strong>中间日历日</strong>，点上为 <code>week_no</code>。<br />
      <strong>点击橙色周点</strong>查看该周区间与 impression 明细。
    </p>
    <div class="toolbar">
      <div>
        <label for="storeSel">店铺</label><br />
        <select id="storeSel">
          <option value="">全部店铺（合计）</option>
        </select>
      </div>
    </div>
    <div class="chart-box">
      <canvas id="c" height="120"></canvas>
    </div>
    <div id="detailPanel"><h3>周 impression 详情</h3><div id="detailBody"></div></div>
  </div>
  <script>
    if (typeof ChartDataLabels !== 'undefined') {{
      Chart.register(ChartDataLabels);
    }}
    var payload = {data_json};
    var labels = payload.labels;
    var allData = payload.all_data;
    var byStore = payload.by_store;
    var storeIds = payload.store_ids || [];
    var impOn = payload.impressionEnabled;
    var impAll = payload.impression_all || [];
    var impAllMeta = payload.impression_all_meta || [];
    var impByStore = payload.impression_by_store || {{}};
    var impMetaByStore = payload.impression_meta_by_store || {{}};

    var sel = document.getElementById('storeSel');
    storeIds.forEach(function (sid) {{
      var o = document.createElement('option');
      o.value = String(sid);
      o.textContent = 'store_id = ' + sid;
      sel.appendChild(o);
    }});

    var panel = document.getElementById('detailPanel');
    var detailBody = document.getElementById('detailBody');

    function currentImpressionSeries() {{
      var v = sel.value;
      if (!impOn) return {{ data: [], meta: [] }};
      if (!v) return {{ data: impAll, meta: impAllMeta }};
      return {{
        data: impByStore[v] || labels.map(function () {{ return null; }}),
        meta: impMetaByStore[v] || labels.map(function () {{ return null; }})
      }};
    }}

    function hideDetail() {{
      panel.style.display = 'none';
      detailBody.innerHTML = '';
    }}

    function showDetail(meta) {{
      if (!meta || !meta.weeks || !meta.weeks.length) return;
      var html = '';
      meta.weeks.forEach(function (w) {{
        html += '<div class="row"><strong>week_no</strong> ' + escapeHtml(String(w.week_no));
        if (w.store_id != null) html += ' · store_id ' + escapeHtml(String(w.store_id));
        html += '</div>';
        html += '<div class="row">impressions: <strong>' + Number(w.impressions).toLocaleString() + '</strong></div>';
        html += '<hr style="border:none;border-top:1px solid rgba(255,255,255,0.12);margin:0.6rem 0"/>';
      }});
      detailBody.innerHTML = html;
      panel.style.display = 'block';
    }}

    function escapeHtml(s) {{
      if (s == null) return '';
      return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    }}

    var impSeries = currentImpressionSeries();
    var ctx = document.getElementById('c').getContext('2d');
    var chart = new Chart(ctx, {{
      type: 'line',
      data: {{
        labels: labels,
        datasets: [
          {{
            label: 'Sessions（左轴）',
            data: allData,
            yAxisID: 'y',
            borderColor: '#7dd3c0',
            backgroundColor: 'rgba(125, 211, 192, 0.12)',
            fill: true,
            tension: 0.22,
            pointRadius: 2,
            pointHoverRadius: 5,
            borderWidth: 2,
            order: 2,
          }},
        ],
      }},
      options: {{
        responsive: true,
        maintainAspectRatio: true,
        interaction: {{ mode: 'nearest', intersect: true }},
        onClick: function (ev, els, ch) {{
          if (!els.length) return;
          var el = els[0];
          var ix = el.index;
          // 以“当前店铺选择”对应的 meta 为准，避免 dataset._impressionMeta 在更新时出现错位
          // （尤其是切换 store 后，确保数值和 week_no 明细一致）
          var meta = null;
          try {{
            var is = currentImpressionSeries();
            meta = (is && is.meta && is.meta[ix]) ? is.meta[ix] : null;
          }} catch (e) {{
            meta = null;
          }}
          if (meta) showDetail(meta);
        }},
        plugins: {{
          legend: {{ labels: {{ color: '#c5d0de' }} }},
          tooltip: {{
            callbacks: {{
              label: function (ctx) {{
                var v = ctx.parsed.y;
                if (v == null) return '';
                if (ctx.dataset.yAxisID === 'y1')
                  return ' impressions: ' + Number(v).toLocaleString();
                return ' sessions: ' + Number(v).toLocaleString();
              }},
            }},
          }},
          datalabels: {{
            display: function (ctx) {{
              if (ctx.dataset.yAxisID !== 'y1') return false;
              var v = ctx.dataset.data[ctx.dataIndex];
              return v != null && v !== '' && !isNaN(Number(v));
            }},
            color: '#ffb347',
            backgroundColor: 'rgba(0,0,0,0.45)',
            borderRadius: 4,
            padding: {{ top: 2, bottom: 2, left: 4, right: 4 }},
            font: {{ size: 10, weight: '600' }},
            align: 'top',
            offset: 6,
            formatter: function (value, ctx) {{
              var m = ctx.dataset._impressionMeta && ctx.dataset._impressionMeta[ctx.dataIndex];
              if (!m || !m.weeks || !m.weeks.length) return '';
              return m.weeks.map(function (w) {{ return w.week_no; }}).join(',');
            }},
          }},
        }},
        scales: {{
          x: {{
            ticks: {{ color: '#8b9cb3', maxRotation: 45 }},
            grid: {{ color: 'rgba(139, 156, 179, 0.08)' }},
          }},
          y: {{
            id: 'y',
            position: 'left',
            beginAtZero: true,
            title: {{ display: true, text: 'Sessions', color: '#8b9cb3' }},
            ticks: {{
              color: '#8b9cb3',
              callback: function (v) {{ return Number(v).toLocaleString(); }},
            }},
            grid: {{ color: 'rgba(139, 156, 179, 0.1)' }},
          }},
          y1: {{
            id: 'y1',
            position: 'right',
            beginAtZero: true,
            display: impOn,
            title: {{ display: true, text: '周 Impressions', color: '#ffb347' }},
            ticks: {{
              color: '#ffb347',
              callback: function (v) {{ return Number(v).toLocaleString(); }},
            }},
            grid: {{ drawOnChartArea: false }},
          }},
        }},
      }},
    }});

    if (impOn) {{
      var dsImp = {{
        label: '周 impressions（右轴，点即 week 中点）',
        data: impSeries.data,
        yAxisID: 'y1',
        borderColor: '#ffb347',
        backgroundColor: 'rgba(255, 179, 71, 0.15)',
        fill: false,
        tension: 0.2,
        spanGaps: false,
        pointRadius: 6,
        pointHoverRadius: 9,
        pointBackgroundColor: '#ffb347',
        pointBorderColor: '#1a2332',
        pointBorderWidth: 2,
        borderWidth: 2,
        order: 1,
      }};
      dsImp._impressionMeta = impSeries.meta;
      chart.data.datasets.push(dsImp);
      chart.update();
    }}

    function applyStore() {{
      hideDetail();
      var v = sel.value;
      var ds0 = chart.data.datasets[0];
      if (!v) {{
        ds0.label = 'Sessions（全部店铺合计）';
        ds0.data = allData;
        ds0.borderColor = '#7dd3c0';
        ds0.backgroundColor = 'rgba(125, 211, 192, 0.12)';
      }} else {{
        ds0.label = 'store_id ' + v + ' sessions';
        ds0.data = byStore[v] || labels.map(function () {{ return 0; }});
        ds0.borderColor = '#3d9dff';
        ds0.backgroundColor = 'rgba(61, 157, 255, 0.12)';
      }}
      if (impOn && chart.data.datasets.length > 1) {{
        var is = currentImpressionSeries();
        chart.data.datasets[1].data = is.data;
        chart.data.datasets[1]._impressionMeta = is.meta;
      }}
      chart.update();
    }}
    sel.addEventListener('change', applyStore);
  </script>
</body>
</html>
"""


def write_report(out: str | Path, start_date: date | None, end_date: date | None) -> Path:
    sorted_dates, series_map, store_ids = fetch_traffic_daily_by_store(start_date, end_date)

    impression_enabled = start_date is not None and end_date is not None
    impression_per_store: dict[int, list[dict]] = {}
    impression_all: list[dict] = []
    if impression_enabled:
        impression_per_store, impression_all = fetch_impression_weekly(start_date, end_date)

    labels = _merge_label_dates(sorted_dates, impression_all, start_date, end_date)
    if not labels:
        labels = sorted_dates

    payload = build_chart_payload(
        labels,
        series_map,
        store_ids,
        impression_per_store,
        impression_all,
        impression_enabled,
    )
    out_path = Path(out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(render_html(payload), encoding="utf-8")
    logger.info(
        "[Traffic+Impression] wrote labels=%s traffic_days=%s stores=%s impression_weeks=%s path=%s",
        len(labels),
        len(sorted_dates),
        len(store_ids),
        len(impression_all),
        out_path,
    )
    return out_path


def main(argv: list[str]) -> int:
    setup_logging(level=logging.INFO)
    p = argparse.ArgumentParser(
        description="线上 daily sessions +（可选）amazon_search_data 周 impression 双轴图 HTML"
    )
    p.add_argument("--out", type=str, required=True, help="输出 .html 路径")
    p.add_argument(
        "--start-date",
        type=str,
        default="",
        help="traffic 的 current_date 下限（含）；与 --end-date 同时传时另查 amazon_search_data 周 impression",
    )
    p.add_argument(
        "--end-date",
        type=str,
        default="",
        help="traffic 的 current_date 上限（含）；与 --start-date 同时传时另查周 impression（WHERE DATE(start_date) BETWEEN 起止）",
    )
    args = p.parse_args(argv)
    start_d = _parse_ymd(args.start_date) if args.start_date.strip() else None
    end_d = _parse_ymd(args.end_date) if args.end_date.strip() else None
    if start_d and end_d and start_d > end_d:
        p.error("start-date 不能晚于 end-date")
    try:
        write_report(args.out, start_d, end_d)
        return 0
    except Exception as e:
        logger.exception("[Traffic+Impression] failed: %s", e)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
