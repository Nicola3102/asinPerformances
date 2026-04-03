"""
线上 amazon_sales_and_traffic_daily：按店铺、按日汇总 sessions，生成交互折线图 HTML。

SQL 口径（与需求一致，current_date 列加反引号）：
  SELECT store_id, SUM(sessions), DATE(`current_date`) AS d
  FROM amazon_sales_and_traffic_daily
  GROUP BY store_id, DATE(`current_date`);

- 横轴：日历日；纵轴：sessions 合计。
- 默认「全部店铺」折线；下拉可选 store_id 单店。
- 悬停 tooltip 展示当日 session 总数。

用法（backend 目录）：
  python3.11 -m app.services.weekly_upload_asin_date --out ./charts/traffic_daily_by_store.html
  python3.11 -m app.services.weekly_upload_asin_date --out ./charts/out.html --start-date 2026-03-01 --end-date 2026-04-03
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


def fetch_traffic_daily_by_store(
    start_date: date | None,
    end_date: date | None,
) -> tuple[list[date], dict[int | None, dict[date, int]], list[int]]:
    """
    返回 (sorted_dates, series_map, store_ids)。
    series_map[None] 为全店按日合计；series_map[sid] 为单店。
    """
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


def build_chart_payload(
    sorted_dates: list[date],
    series_map: dict[int | None, dict[date, int]],
    store_ids: list[int],
) -> dict:
    labels = [d.isoformat() for d in sorted_dates]

    def series_for(key: int | None) -> list[int | None]:
        m = series_map.get(key, {})
        return [int(m.get(d, 0) or 0) for d in sorted_dates]

    return {
        "labels": labels,
        "store_ids": store_ids,
        "all_data": series_for(None),
        "by_store": {str(sid): series_for(sid) for sid in store_ids},
    }


def render_html(payload: dict) -> str:
    data_json = json.dumps(payload, ensure_ascii=False)
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>amazon_sales_and_traffic_daily — 按日 sessions</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <style>
    :root {{
      --bg: #0f1419;
      --panel: #1a2332;
      --text: #e8ecf1;
      --muted: #8b9cb3;
      --accent: #3d9dff;
      --line-all: #7dd3c0;
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
      letter-spacing: -0.02em;
      margin: 0 0 0.35rem;
    }}
    .sub {{
      color: var(--muted);
      font-size: 0.875rem;
      margin-bottom: 1.25rem;
      line-height: 1.5;
    }}
    .toolbar {{
      display: flex;
      flex-wrap: wrap;
      align-items: center;
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
    canvas {{ max-height: 420px; }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Traffic daily（线上库）</h1>
    <p class="sub">
      数据来源：<code>amazon_sales_and_traffic_daily</code>，按 <code>store_id</code> 与 <code>DATE(current_date)</code> 汇总 <code>SUM(sessions)</code>。
      默认折线为全店合计；选择店铺后仅显示该店。鼠标悬停数据点可查看当日 sessions。
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
  </div>
  <script>
    var payload = {data_json};
    var labels = payload.labels;
    var allData = payload.all_data;
    var byStore = payload.by_store;
    var storeIds = payload.store_ids || [];

    var sel = document.getElementById('storeSel');
    storeIds.forEach(function (sid) {{
      var o = document.createElement('option');
      o.value = String(sid);
      o.textContent = 'store_id = ' + sid;
      sel.appendChild(o);
    }});

    var ctx = document.getElementById('c').getContext('2d');
    var chart = new Chart(ctx, {{
      type: 'line',
      data: {{
        labels: labels,
        datasets: [{{
          label: 'Sessions（合计）',
          data: allData,
          borderColor: '#7dd3c0',
          backgroundColor: 'rgba(125, 211, 192, 0.12)',
          fill: true,
          tension: 0.22,
          pointRadius: 2,
          pointHoverRadius: 6,
          borderWidth: 2,
        }}],
      }},
      options: {{
        responsive: true,
        maintainAspectRatio: true,
        interaction: {{ mode: 'index', intersect: false }},
        plugins: {{
          legend: {{ labels: {{ color: '#c5d0de' }} }},
          tooltip: {{
            callbacks: {{
              label: function (ctx) {{
                var v = ctx.parsed.y;
                if (v == null) return '';
                return ' sessions: ' + v.toLocaleString();
              }},
            }},
          }},
        }},
        scales: {{
          x: {{
            ticks: {{ color: '#8b9cb3', maxRotation: 45, minRotation: 0 }},
            grid: {{ color: 'rgba(139, 156, 179, 0.08)' }},
          }},
          y: {{
            beginAtZero: true,
            ticks: {{
              color: '#8b9cb3',
              callback: function (v) {{ return Number(v).toLocaleString(); }},
            }},
            grid: {{ color: 'rgba(139, 156, 179, 0.1)' }},
          }},
        }},
      }},
    }});

    function applyStore() {{
      var v = sel.value;
      var ds = chart.data.datasets[0];
      if (!v) {{
        ds.label = 'Sessions（全部店铺合计）';
        ds.data = allData;
        ds.borderColor = '#7dd3c0';
        ds.backgroundColor = 'rgba(125, 211, 192, 0.12)';
      }} else {{
        ds.label = 'store_id ' + v;
        ds.data = byStore[v] || labels.map(function () {{ return 0; }});
        ds.borderColor = '#3d9dff';
        ds.backgroundColor = 'rgba(61, 157, 255, 0.12)';
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
    payload = build_chart_payload(sorted_dates, series_map, store_ids)
    out_path = Path(out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(render_html(payload), encoding="utf-8")
    logger.info(
        "[TrafficDaily] wrote %s dates=%s stores=%s path=%s",
        len(sorted_dates),
        (sorted_dates[0], sorted_dates[-1]) if sorted_dates else None,
        len(store_ids),
        out_path,
    )
    return out_path


def main(argv: list[str]) -> int:
    setup_logging(level=logging.INFO)
    p = argparse.ArgumentParser(description="线上 daily traffic 按店折线图 HTML")
    p.add_argument("--out", type=str, required=True, help="输出 .html 路径")
    p.add_argument(
        "--start-date",
        type=str,
        default="",
        help="可选：过滤 current_date 起始 YYYY-MM-DD（含）",
    )
    p.add_argument(
        "--end-date",
        type=str,
        default="",
        help="可选：过滤 current_date 结束 YYYY-MM-DD（含）",
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
        logger.exception("[TrafficDaily] failed: %s", e)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
