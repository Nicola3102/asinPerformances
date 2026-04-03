"""
基于本地库表 daily_upload_asin_dates（业务上亦称 daily_upload_asin_data）绘制 session 变化图。

本地连接使用 app.database.SessionLocal，即 settings.database_url（MYSQL_HOST / MYSQL_*）。

图 1 — 按 (store_id, created_at) 批次：
  - 折线：该批次 ASIN 在上新日起第 1～30 天（按 session_date）的每日 sessions 合计
  - 文内标注：该批次 distinct ASIN 数量

图 2 — 按店铺 + 日历 session_date 区间：
  - 堆叠柱：每个 session_date 上，各 created_at 批次贡献的 sessions（可看出每批 ASIN 的 session 构成）
  - 折线：当日 sessions 总计（与堆叠柱总和一致，便于读趋势）
  - 右轴柱状：当日 created_at 的上新 ASIN 数（distinct asin）

用法（在 backend 目录）：
  python3.11 -m app.services.draw_daily_session_change --out-dir ./charts
  python3.11 -m app.services.draw_daily_session_change --store-id 1 --session-start 2026-02-01 --session-end 2026-03-31

中文显示：自动选用系统内建字体（macOS 如苹方 PingFang SC）。若仍为方框，可指定：
  MPL_CHINESE_FONT="PingFang SC,Heiti SC" python3.11 -m app.services.draw_daily_session_change ...
"""

from __future__ import annotations

import argparse
import logging
import os
import platform
import sys
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path

from sqlalchemy import text

from app.database import SessionLocal, init_db
from app.logging_config import setup_logging

logger = logging.getLogger(__name__)

# 与 DailyUploadAsinData.__tablename__ 一致
TABLE = "daily_upload_asin_dates"
COHORT_DAYS = 30


@lru_cache(maxsize=1)
def _configure_matplotlib_for_cjk() -> None:
    """
    配置可渲染中文的 sans-serif，避免标题/图例/坐标轴出现「豆腐块」。
    优先读环境变量 MPL_CHINESE_FONT（字体族名，逗号分隔）。
    """
    import matplotlib.pyplot as plt
    from matplotlib import font_manager

    plt.rcParams["axes.unicode_minus"] = False

    raw = (os.environ.get("MPL_CHINESE_FONT") or "").strip()
    if raw:
        env_families = [x.strip() for x in raw.split(",") if x.strip()]
        if env_families:
            tail = [x for x in plt.rcParams["font.sans-serif"] if x not in env_families]
            plt.rcParams["font.sans-serif"] = env_families + tail
            logger.info("Matplotlib 字体（MPL_CHINESE_FONT）: %s", env_families)
            return

    registered = {f.name for f in font_manager.fontManager.ttflist}

    system = platform.system()
    if system == "Darwin":
        candidates = [
            "PingFang SC",
            "PingFang TC",
            "Hiragino Sans GB",
            "Heiti SC",
            "STHeiti",
            "Songti SC",
            "Kaiti SC",
            "Arial Unicode MS",
        ]
    elif system == "Windows":
        candidates = [
            "Microsoft YaHei",
            "Microsoft JhengHei",
            "SimHei",
            "KaiTi",
            "FangSong",
        ]
    else:
        candidates = [
            "Noto Sans CJK SC",
            "Noto Serif CJK SC",
            "Source Han Sans SC",
            "Source Han Serif SC",
            "WenQuanYi Micro Hei",
            "WenQuanYi Zen Hei",
            "Droid Sans Fallback",
        ]

    ordered: list[str] = []
    for fam in candidates:
        if fam in registered and fam not in ordered:
            ordered.append(fam)

    # 名称在部分系统上与候选不完全一致时，按关键字扫一遍已注册字体
    if not ordered:
        markers = (
            "PingFang",
            "Hiragino Sans GB",
            "Heiti SC",
            "STHeiti",
            "Songti SC",
            "Noto Sans CJK",
            "Noto Serif CJK",
            "Source Han Sans",
            "Source Han Serif",
            "Microsoft YaHei",
            "SimHei",
            "WenQuanYi",
            "Droid Sans Fallback",
        )
        seen: set[str] = set()
        for f in font_manager.fontManager.ttflist:
            name = f.name or ""
            if any(m in name for m in markers) and name not in seen:
                seen.add(name)
                ordered.append(name)

    if ordered:
        tail = [x for x in plt.rcParams["font.sans-serif"] if x not in ordered]
        plt.rcParams["font.sans-serif"] = ordered + tail
        logger.info("Matplotlib 中文字体: %s", ordered[:5])
    else:
        logger.warning(
            "未检测到常见中文字体，中文可能显示为方框；"
            "macOS 一般自带苹方；Linux 可安装 fonts-noto-cjk；"
            "或设置环境变量 MPL_CHINESE_FONT=字体族名"
        )


def _parse_ymd(s: str) -> date:
    return datetime.strptime(str(s).strip(), "%Y-%m-%d").date()


def _fetch_distinct_store_ids_in_session_range(
    db, session_start: date, session_end: date
) -> list[int]:
    q = text(
        f"SELECT DISTINCT store_id FROM {TABLE} WHERE session_date >= :d0 AND session_date <= :d1 ORDER BY store_id"
    )
    rows = db.execute(q, {"d0": session_start, "d1": session_end}).fetchall()
    return [int(r[0]) for r in rows if r[0] is not None]


def _fetch_distinct_cohorts(
    db,
    store_id: int | None,
    cohort_start: date | None,
    cohort_end: date | None,
) -> list[tuple[int, date]]:
    cond = ["created_at IS NOT NULL"]
    params: dict = {}
    if store_id is not None:
        cond.append("store_id = :sid")
        params["sid"] = store_id
    if cohort_start is not None:
        cond.append("created_at >= :c0")
        params["c0"] = cohort_start
    if cohort_end is not None:
        cond.append("created_at <= :c1")
        params["c1"] = cohort_end
    where = " AND ".join(cond)
    q = text(
        f"SELECT DISTINCT store_id, created_at FROM {TABLE} WHERE {where} ORDER BY store_id, created_at"
    )
    rows = db.execute(q, params).fetchall()
    out: list[tuple[int, date]] = []
    for r in rows:
        sid = int(r[0])
        c = r[1]
        if c is None:
            continue
        if isinstance(c, datetime):
            c = c.date()
        out.append((sid, c))
    return out


def _fetch_cohort_session_series(db, store_id: int, cohort_date: date) -> tuple[list[int], list[int]]:
    """
    返回 (day_index 1..30, sessions_per_day)。
    第 n 天对应 session_date == cohort_date + (n-1) 天。
    """
    end_sd = cohort_date + timedelta(days=COHORT_DAYS - 1)
    q = text(
        f"""
        SELECT session_date, SUM(COALESCE(sessions, 0)) AS s
        FROM {TABLE}
        WHERE store_id = :sid AND created_at = :cd
          AND session_date >= :sd0 AND session_date <= :sd1
        GROUP BY session_date
        """
    )
    rows = db.execute(
        q,
        {"sid": store_id, "cd": cohort_date, "sd0": cohort_date, "sd1": end_sd},
    ).fetchall()
    by_day: dict[date, int] = {}
    for r in rows:
        sd = r[0]
        if isinstance(sd, datetime):
            sd = sd.date()
        by_day[sd] = int(r[1] or 0)
    xs = list(range(1, COHORT_DAYS + 1))
    ys = []
    for n in xs:
        d = cohort_date + timedelta(days=n - 1)
        ys.append(int(by_day.get(d, 0)))
    return xs, ys


def _fetch_cohort_asin_count(db, store_id: int, cohort_date: date) -> int:
    q = text(
        f"""
        SELECT COUNT(DISTINCT asin) FROM {TABLE}
        WHERE store_id = :sid AND created_at = :cd
        """
    )
    r = db.execute(q, {"sid": store_id, "cd": cohort_date}).scalar()
    return int(r or 0)


def _fetch_session_matrix(
    db,
    store_id: int,
    session_start: date,
    session_end: date,
) -> tuple[list[date], list[date], dict[tuple[date, date], int]]:
    """
    返回 (sorted session_dates, sorted cohort_dates created_at,
          (session_date, created_at) -> sum sessions)
    """
    q = text(
        f"""
        SELECT session_date, created_at, SUM(COALESCE(sessions, 0)) AS s
        FROM {TABLE}
        WHERE store_id = :sid
          AND created_at IS NOT NULL
          AND session_date >= :d0 AND session_date <= :d1
        GROUP BY session_date, created_at
        """
    )
    rows = db.execute(
        q, {"sid": store_id, "d0": session_start, "d1": session_end}
    ).fetchall()
    mat: dict[tuple[date, date], int] = {}
    session_days: set[date] = set()
    cohort_days: set[date] = set()
    for r in rows:
        sd, cd = r[0], r[1]
        if isinstance(sd, datetime):
            sd = sd.date()
        if isinstance(cd, datetime):
            cd = cd.date()
        if cd is None:
            continue
        session_days.add(sd)
        cohort_days.add(cd)
        mat[(sd, cd)] = int(r[2] or 0)
    s_list = sorted(session_days)
    c_list = sorted(cohort_days)
    return s_list, c_list, mat


def _fetch_daily_new_asin_counts(
    db,
    store_id: int,
    day_start: date,
    day_end: date,
) -> dict[date, int]:
    q = text(
        f"""
        SELECT created_at, COUNT(DISTINCT asin) AS n
        FROM {TABLE}
        WHERE store_id = :sid AND created_at IS NOT NULL
          AND created_at >= :d0 AND created_at <= :d1
        GROUP BY created_at
        """
    )
    rows = db.execute(q, {"sid": store_id, "d0": day_start, "d1": day_end}).fetchall()
    out: dict[date, int] = {}
    for r in rows:
        cd = r[0]
        if isinstance(cd, datetime):
            cd = cd.date()
        out[cd] = int(r[1] or 0)
    return out


def _plot_cohort_charts(
    db,
    out_dir: Path,
    store_id: int | None,
    cohort_start: date | None,
    cohort_end: date | None,
) -> int:
    _configure_matplotlib_for_cjk()
    import matplotlib.pyplot as plt

    cohorts = _fetch_distinct_cohorts(db, store_id, cohort_start, cohort_end)
    if not cohorts:
        logger.warning("未找到任何 (store_id, created_at) 批次，跳过图 1")
        return 0
    out_dir.mkdir(parents=True, exist_ok=True)
    n = 0
    for sid, cd in cohorts:
        xs, ys = _fetch_cohort_session_series(db, sid, cd)
        n_asin = _fetch_cohort_asin_count(db, sid, cd)
        fig, ax = plt.subplots(figsize=(11, 5))
        ax.plot(xs, ys, marker="o", linewidth=2, markersize=4, color="#2563eb", label="Sessions")
        ax.set_xlabel("上新后第 n 天（session_date）")
        ax.set_ylabel("Sessions（合计）")
        ax.set_title(f"店铺 {sid} · 上新日 {cd} · 30 日 session 走势")
        ax.set_xticks(range(1, COHORT_DAYS + 1, 2))
        ax.grid(True, alpha=0.3)
        ax.legend(loc="upper right")
        ax.text(
            0.02,
            0.98,
            f"本批次上新 ASIN 数（distinct）：{n_asin}",
            transform=ax.transAxes,
            va="top",
            fontsize=11,
            bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.5),
        )
        fn = out_dir / f"cohort_store{sid}_created{cd}.png"
        fig.tight_layout()
        fig.savefig(fn, dpi=150)
        plt.close(fig)
        n += 1
        logger.info("已写入 %s", fn)
    return n


def _plot_stacked_overview(
    db,
    out_dir: Path,
    store_id: int,
    session_start: date,
    session_end: date,
) -> bool:
    _configure_matplotlib_for_cjk()
    import matplotlib.pyplot as plt

    s_list, c_list, mat = _fetch_session_matrix(db, store_id, session_start, session_end)
    if not s_list:
        logger.warning("店铺 %s 在 %s～%s 无 session 数据，跳过图 2", store_id, session_start, session_end)
        return False
    new_asin = _fetch_daily_new_asin_counts(db, store_id, session_start, session_end)

    n_x = len(s_list)
    x = list(range(n_x))
    width = 0.72
    n_c = len(c_list)
    cmap = plt.cm.tab20.colors
    bottom = [0.0] * n_x
    totals = [0.0] * n_x

    fig, ax1 = plt.subplots(figsize=(14, 6))

    for i, cd in enumerate(c_list):
        heights = [float(mat.get((sd, cd), 0)) for sd in s_list]
        for j in range(n_x):
            totals[j] += heights[j]
        ax1.bar(
            x,
            heights,
            width,
            bottom=bottom,
            color=cmap[i % len(cmap)],
            label=f"批次 {cd}",
            edgecolor="white",
            linewidth=0.3,
        )
        for j in range(n_x):
            bottom[j] += heights[j]

    ax1.plot(x, totals, color="#111827", linewidth=2.2, marker="D", markersize=4, label="当日 sessions 合计")
    ax1.set_xlabel("session_date（日历日）")
    ax1.set_ylabel("Sessions")
    ax1.set_title(
        f"店铺 {store_id} · 堆叠 sessions（按上新批次）+ 合计折线 · {session_start} ~ {session_end}"
    )
    ax1.set_xticks(x)
    ax1.set_xticklabels([d.strftime("%m-%d") for d in s_list], rotation=45, ha="right")
    ax1.grid(True, axis="y", alpha=0.3)

    ax2 = ax1.twinx()
    asin_heights = [float(new_asin.get(sd, 0)) for sd in s_list]
    ax2.bar(
        x,
        asin_heights,
        width=0.35,
        alpha=0.35,
        color="#f97316",
        label="当日上新 ASIN 数（created_at）",
    )
    ax2.set_ylabel("上新 ASIN 数")
    ax2.tick_params(axis="y")

    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax1.legend(h1 + h2, l1 + l2, loc="upper left", fontsize=7, ncol=2)

    out_dir.mkdir(parents=True, exist_ok=True)
    fn = out_dir / f"stacked_store{store_id}_{session_start}_{session_end}.png"
    fig.tight_layout()
    fig.savefig(fn, dpi=150)
    plt.close(fig)
    logger.info("已写入 %s", fn)
    return True


def main(argv: list[str]) -> int:
    setup_logging(level=logging.INFO)
    p = argparse.ArgumentParser(description="绘制 daily_upload 批次 session 变化图")
    p.add_argument("--out-dir", type=str, default="./charts_daily_session", help="输出 PNG 目录")
    p.add_argument("--store-id", type=int, default=None, help="仅该店铺；不传则图 1 含所有店铺")
    p.add_argument(
        "--cohort-start",
        type=str,
        default="",
        help="筛选 created_at 下限 YYYY-MM-DD（图 1）",
    )
    p.add_argument("--cohort-end", type=str, default="", help="筛选 created_at 上限 YYYY-MM-DD（图 1）")
    p.add_argument(
        "--session-start",
        type=str,
        default="",
        help="图 2 的 session_date 起始（与 --session-end 联用；不传 --store-id 时为每个店铺各出一张）",
    )
    p.add_argument("--session-end", type=str, default="", help="图 2 的 session_date 结束")
    p.add_argument(
        "--skip-cohort-plots",
        action="store_true",
        help="不生成图 1（仅图 2）",
    )
    p.add_argument(
        "--skip-stacked",
        action="store_true",
        help="不生成图 2（仅图 1）",
    )
    args = p.parse_args(argv)
    out_dir = Path(args.out_dir).resolve()

    cohort_start = _parse_ymd(args.cohort_start) if args.cohort_start.strip() else None
    cohort_end = _parse_ymd(args.cohort_end) if args.cohort_end.strip() else None

    init_db()
    db = SessionLocal()
    try:
        if not args.skip_cohort_plots:
            _plot_cohort_charts(db, out_dir, args.store_id, cohort_start, cohort_end)
        if not args.skip_stacked:
            if not args.session_start.strip() or not args.session_end.strip():
                logger.warning("图 2 需要 --session-start 与 --session-end，已跳过")
            else:
                ss = _parse_ymd(args.session_start)
                se = _parse_ymd(args.session_end)
                if ss > se:
                    p.error("session-start 不能晚于 session-end")
                store_ids = (
                    [args.store_id]
                    if args.store_id is not None
                    else _fetch_distinct_store_ids_in_session_range(db, ss, se)
                )
                if not store_ids:
                    logger.warning("图 2：区间内无 store_id，已跳过")
                for sid in store_ids:
                    _plot_stacked_overview(db, out_dir, sid, ss, se)
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
