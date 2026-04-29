"""
将前端 New Listing 页面导出为单文件 HTML 快照（布局/样式与 /trend/New Listing 一致）。

默认页面：
  http://localhost:5173/trend/New%20Listing

指定横轴区间（与页面/API 的 ``start_date``、``session_end`` 一致），例如 2025-05-11～2026-04-26。
``--api-only`` 的 ``--api-base`` 必须是 **后端**（如 ``http://127.0.0.1:9090``），不要填 Vite 的 ``5173``。

  # 推荐大区间：直连后端 API 合并各店 JSON，不依赖 Playwright 等首屏
  python3.11 -m app.services.report_pst \\
    --api-only --api-base http://127.0.0.1:9090 \\
    --start-date 2025-05-11 --session-end 2026-04-26 \\
    --out ./charts/new_listing_20250511_20260426.html

  # 或带参打开前端（需 Vite 已起），由页面拉同一区间后再抽取 JSON
  python3.11 -m app.services.report_pst \\
    --url 'http://localhost:5173/trend/New%20Listing?start_date=2025-05-11&session_end=2026-04-26&refresh=1' \\
    --start-date 2025-05-11 --session-end 2026-04-26 \\
    --wait-ms 900000 --out ./charts/new_listing_20250511_20260426.html

默认输出（在 backend 目录运行时）：
  python3.11 -m app.services.report_pst --out ./charts/report_pst1.html

本脚本与仓库其余后端代码一致，需要 **Python 3.10+**（Docker 镜像为 3.11）。若本机 ``python3`` 指向 3.9（macOS CLT 常见），请改用 ``python3.11``，或在容器内执行：

  docker compose exec backend python -m app.services.report_pst --api-only --api-base http://127.0.0.1:9090 ...
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

logger = logging.getLogger(__name__)

DEFAULT_URL = "http://localhost:5173/trend/New%20Listing"
DEFAULT_WAIT_MS = 180_000
DEFAULT_SETTLE_MS = 1_500
TREND_NEW_LISTING_CACHE_KEY = "asinPerformances.v4.trendNewListingJson"
TOOLTIP_LOOKBACK_DAYS = 35


class _CliProgressBar:
    """简单终端进度条（仅在 TTY 输出，避免污染重定向日志）。"""

    def __init__(self, *, width: int = 26) -> None:
        self.width = max(10, int(width))
        self.enabled = bool(getattr(sys.stderr, "isatty", lambda: False)())
        self._last_line_len = 0

    def update(self, done: int, total: int, label: str = "") -> None:
        total_i = max(1, int(total))
        done_i = max(0, min(int(done), total_i))
        if not self.enabled:
            return
        ratio = done_i / total_i
        filled = int(self.width * ratio)
        bar = "#" * filled + "-" * (self.width - filled)
        pct = int(ratio * 100)
        line = f"[{bar}] {pct:3d}% ({done_i}/{total_i}) {label}".rstrip()
        sys.stderr.write("\r" + line)
        if self._last_line_len > len(line):
            sys.stderr.write(" " * (self._last_line_len - len(line)))
        sys.stderr.flush()
        self._last_line_len = len(line)

    def finish(self, label: str = "") -> None:
        if not self.enabled:
            return
        tail = f" {label}" if label else ""
        sys.stderr.write("\r" + " " * self._last_line_len + "\r")
        sys.stderr.write(f"[{'#' * self.width}] 100%{tail}\n")
        sys.stderr.flush()
        self._last_line_len = 0


class _LongWaitHeartbeat:
    """在阻塞 HTTP 期间周期性打日志，避免用户误以为进程卡死（进度条在响应返回前无法前进）。"""

    def __init__(self, message: str, *, interval_sec: float = 45.0) -> None:
        self.message = message
        self.interval_sec = max(10.0, float(interval_sec))
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> _LongWaitHeartbeat:
        def _run() -> None:
            n = 0
            while not self._stop.wait(timeout=self.interval_sec):
                n += 1
                logger.info(
                    "%s（已等待约 %.0fs；大区间单次请求可达数分钟，进度条在收到响应前会停在当前百分比）",
                    self.message,
                    n * self.interval_sec,
                )

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *args: object) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)


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
async ({ cacheKey, startDate, sessionEnd }) => {
  const tryParse = (raw) => {
    try {
      return raw ? JSON.parse(raw) : null;
    } catch {
      return null;
    }
  };

  const rangeQs =
    startDate && sessionEnd
      ? `&start_date=${encodeURIComponent(String(startDate))}&session_end=${encodeURIComponent(String(sessionEnd))}`
      : '';
  const forceNetwork = Boolean(rangeQs);

  const fetchJsonText = async (url) => {
    const resp = await fetch(url, { credentials: 'same-origin' });
    if (!resp.ok) {
      throw new Error(`拉取 new-listing JSON 失败: ${resp.status} @ ${url}`);
    }
    return await resp.text();
  };

  const fetchJson = async (url) => tryParse(await fetchJsonText(url));

  let base = tryParse(localStorage.getItem(cacheKey));
  if (forceNetwork || !base || !base.views || !base.views.all) {
    base = await fetchJson('/api/trend/new-listing?format=json&json_views=all' + rangeQs);
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
    const part = await fetchJson(
      `/api/trend/new-listing?format=json&json_views=store&store_id=${encodeURIComponent(String(sid))}` + rangeQs,
    );
    if (part && part.views && typeof part.views === 'object') {
      merged.views = { ...merged.views, ...part.views };
    }
  }

  return JSON.stringify(merged);
}
"""


def _new_listing_range_query(start_date: str | None, session_end: str | None) -> str:
    if not start_date or not session_end:
        return ""
    return "&" + urlencode({"start_date": start_date, "session_end": session_end})


def _profile_query(profile: bool) -> str:
    return "&profile=1" if profile else ""


def _log_profile_timings(label: str, payload: dict | None, *, top_n: int = 15) -> None:
    """打印 ``profileTimingsSec`` 中耗时最大的若干 key（需请求带 ``profile=1``）。"""
    if not isinstance(payload, dict):
        return
    raw = payload.get("profileTimingsSec")
    if not isinstance(raw, dict) or not raw:
        logger.info("[%s] 无 profileTimingsSec（未加 --profile 或后端未返回）", label)
        return
    rows: list[tuple[str, float]] = []
    for k, v in raw.items():
        if k == "total":
            continue
        try:
            rows.append((str(k), float(v)))
        except (TypeError, ValueError):
            continue
    rows.sort(key=lambda x: -x[1])
    top = rows[: max(1, int(top_n))]
    logger.info("[%s] profileTimingsSec Top-%s（秒，降序）: %s", label, len(top), top)
    tv = raw.get("total")
    if tv is not None:
        logger.info("[%s] profile total: %s", label, tv)


def build_new_listing_page_url(base_url: str, start_date: str | None, session_end: str | None) -> str:
    """为 Playwright 打开与导出区间一致的前端页（需与 App.tsx 中 query 约定一致）。"""
    if not start_date or not session_end:
        return base_url
    parts = urlparse(base_url)
    q_pairs = dict(parse_qsl(parts.query, keep_blank_values=True))
    q_pairs["start_date"] = start_date
    q_pairs["session_end"] = session_end
    q_pairs["refresh"] = "1"
    new_query = urlencode(q_pairs)
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, new_query, parts.fragment))


def _http_get_json_optional(url: str, *, timeout_sec: int = 20) -> dict | None:
    """单次 GET JSON，失败返回 None（用于 heavy-status 等可选探测）。"""
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None


def wait_until_new_listing_heavy_idle(
    api_base: str,
    *,
    max_wait_sec: float,
    poll_sec: float = 3.0,
) -> None:
    """
    在拉取 ``json_views=all`` 前等待 heavy 槽位空闲，减少与浏览器里 New Listing 页抢同一把锁导致的 429。

    若后端无 ``/heavy-status``（旧版本），则立即返回。
    """
    if max_wait_sec <= 0:
        return
    url = f"{api_base.rstrip('/')}/api/trend/new-listing/heavy-status"
    deadline = time.monotonic() + max_wait_sec
    first = True
    while time.monotonic() < deadline:
        data = _http_get_json_optional(url, timeout_sec=15)
        if data is None:
            if first:
                logger.info("未读到 heavy-status（旧后端可忽略），直接请求主接口")
            return
        first = False
        if not data.get("heavy_build_busy"):
            logger.info("new-listing heavy 槽位空闲，开始拉取主 JSON")
            return
        busy_for = data.get("heavy_build_busy_for_sec")
        remain = max(0.0, deadline - time.monotonic())
        logger.info(
            "new-listing heavy 正忙（已约 %ss），%.0fs 后再检测（剩余约 %.0fs）",
            busy_for if busy_for is not None else -1,
            poll_sec,
            remain,
        )
        time.sleep(min(poll_sec, max(0.5, remain)))
    logger.warning(
        "等待 heavy 空闲已达 %.0fs，仍将请求主 JSON；若仍 429，请关闭浏览器 New Listing 标签或执行 docker compose build backend 更新排队逻辑",
        max_wait_sec,
    )


def _effective_wait_ms(wait_ms: int, start_date: str | None, session_end: str | None) -> int:
    if not start_date or not session_end:
        return wait_ms
    try:
        d0 = datetime.strptime(start_date, "%Y-%m-%d").date()
        d1 = datetime.strptime(session_end, "%Y-%m-%d").date()
        span = max(0, (d1 - d0).days)
    except ValueError:
        return wait_ms
    if span > 120:
        return max(wait_ms, 900_000)
    if span > 60:
        return max(wait_ms, 600_000)
    return max(wait_ms, 300_000)


def _http_get_json_with_retry(url: str, *, timeout_sec: int, max_rounds: int = 80) -> dict:
    last_exc: BaseException | None = None
    for attempt in range(max_rounds):
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
                raw = resp.read().decode("utf-8")
            return json.loads(raw)
        except urllib.error.HTTPError as exc:
            last_exc = exc
            if exc.code == 429:
                ra = exc.headers.get("Retry-After", "20") if exc.headers else "20"
                try:
                    delay = max(1.0, float(ra))
                except ValueError:
                    delay = 20.0
                logger.info("HTTP 429，%ss 后重试 (%s/%s): %s", int(delay), attempt + 1, max_rounds, url)
                if attempt == 0 and delay <= 12:
                    logger.info(
                        "提示: 若连续快速 429，多为其它客户端正占用 heavy 槽位，或后端仍为短排队版本；"
                        "可先关掉浏览器 New Listing 页，并执行 docker compose build backend 后重启容器"
                    )
                time.sleep(delay)
                continue
            body = ""
            try:
                body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                pass
            snippet = body[:1200] + ("…" if len(body) > 1200 else "")
            logger.error("HTTP %s %s body=%s", exc.code, url, snippet or "(empty)")
            raise
        except urllib.error.URLError as exc:
            last_exc = exc
            raise
    raise RuntimeError(f"多次 429 后仍失败: {url}") from last_exc


def _fetch_store_json(
    base: str,
    sid_i: int,
    start_date: str,
    session_end: str,
    timeout_sec: int,
    *,
    profile: bool = False,
) -> tuple[int, dict | None]:
    """单店 ``json_views=store`` 请求；与浏览器预取 URL 一致。"""
    sq = _new_listing_range_query(start_date, session_end) or ""
    store_url = (
        f"{base}/api/trend/new-listing?format=json&json_views=store&store_id={sid_i}{sq}{_profile_query(profile)}"
    )
    part = _http_get_json_with_retry(store_url, timeout_sec=timeout_sec)
    if isinstance(part, dict) and isinstance(part.get("views"), dict):
        return sid_i, part
    return sid_i, None


def fetch_new_listing_merged_payload(
    *,
    api_base: str,
    start_date: str,
    session_end: str,
    timeout_sec: int = 900,
    wait_heavy_sec: int = 900,
    store_concurrency: int = 1,
    profile: bool = False,
) -> dict:
    """
    与浏览器内 EXTRACT_PAYLOAD_JS 等价：先 all，再逐个 store 合并进 ``views``。
    适用于大区间导出，避免 Playwright 长时间占用。

    进度条“走得慢”的常见原因（非 bug）：

    1. **区间极宽**：``start_date``～``session_end`` 跨越多百日时，后端 ``build_report_payload`` 每次都要扫矩阵/cohort，单次 HTTP 可达数分钟。
    2. **店铺数 × 串行**：``json_views=all`` 只带 ``views.all``；缺失的每家店要再打一次 ``json_views=store``；默认 **串行**（总耗时 ≈ 各店耗时之和）。
    3. **429 退避**：与其它客户端抢 heavy 槽位或池满时，会按 ``Retry-After`` **睡眠**再试，日志里会看到多次 “HTTP 429”。
    4. **缓存未命中**：单店若未命中服务端短 TTL 缓存，每次都会 **冷算**（比命中 ``X-New-Listing-Server-Cache: hit`` 慢很多）。
    """
    try:
        span_days = (
            datetime.strptime(session_end, "%Y-%m-%d").date()
            - datetime.strptime(start_date, "%Y-%m-%d").date()
        ).days
    except ValueError:
        span_days = -1
    logger.info(
        "拉取区间 start_date=%s session_end=%s（约 %s 天）；缺失店铺将各打一次 store 接口，慢主要来自「区间宽度 × 店铺数」及 429 退避",
        start_date,
        session_end,
        span_days,
    )
    if span_days > 90 and store_concurrency > 1:
        logger.warning(
            "区间约 %s 天且 store_concurrency=%s：大区间下多店并行易压满 online reporting / DB 池导致 500；"
            "建议 --store-concurrency 1，或缩短区间；导出时可去掉 --profile 以减轻服务端",
            span_days,
            store_concurrency,
        )

    t_wait0 = time.perf_counter()
    wait_until_new_listing_heavy_idle(api_base, max_wait_sec=float(wait_heavy_sec))
    logger.info("heavy 等待阶段耗时 %.1fs", time.perf_counter() - t_wait0)

    progress = _CliProgressBar()
    progress.update(0, 1, "拉取 all 视图")
    logger.info(
        "已开始请求 json_views=all；约 %s 天区间时服务端可能单次计算数分钟，进度条在 0%% 不动属正常现象",
        span_days,
    )
    base = api_base.rstrip("/")
    q = _new_listing_range_query(start_date, session_end).lstrip("&")
    all_url = f"{base}/api/trend/new-listing?format=json&json_views=all"
    if q:
        all_url += "&" + q
    all_url += _profile_query(profile)
    t_all0 = time.perf_counter()
    with _LongWaitHeartbeat("仍等待 json_views=all 的 HTTP 响应"):
        base_payload = _http_get_json_with_retry(all_url, timeout_sec=timeout_sec)
    logger.info("all 视图 JSON 拉取耗时 %.1fs", time.perf_counter() - t_all0)
    _log_profile_timings("json_views=all", base_payload)
    if not isinstance(base_payload, dict):
        raise RuntimeError("new-listing all 响应不是 JSON 对象")
    views = dict(base_payload.get("views") or {})
    if "all" not in views:
        raise RuntimeError("new-listing 缺少 views.all")
    merged: dict = {**base_payload, "views": views}
    store_ids = merged.get("storeIds") or []
    if not isinstance(store_ids, list):
        store_ids = []
    missing_store_ids: list[int] = []
    for sid in store_ids:
        try:
            sid_i = int(sid)
        except (TypeError, ValueError):
            continue
        if str(sid_i) in merged["views"]:
            continue
        missing_store_ids.append(sid_i)

    total_steps = 1 + len(missing_store_ids)
    progress.update(1, total_steps, f"all 视图完成，待拉店铺 {len(missing_store_ids)}")

    done_steps = 1
    t_stores0 = time.perf_counter()
    conc = max(1, min(12, int(store_concurrency)))
    if not missing_store_ids:
        logger.info("payload.views 已含全部店铺，无需额外 json_views=store 请求")
    elif conc == 1:
        for sid_i in missing_store_ids:
            t_one = time.perf_counter()
            with _LongWaitHeartbeat(f"仍等待店铺 {sid_i}（json_views=store）的 HTTP 响应"):
                _sid, part = _fetch_store_json(
                    base, sid_i, start_date, session_end, timeout_sec, profile=profile
                )
            if part is not None:
                merged["views"] = {**merged["views"], **part["views"]}
                _log_profile_timings(f"json_views=store store_id={sid_i}", part)
            done_steps += 1
            progress.update(done_steps, total_steps, f"店铺 {sid_i}")
            logger.info("店铺 %s JSON 单次耗时 %.1fs", sid_i, time.perf_counter() - t_one)
    else:
        logger.info(
            "并行拉取 %s 个店铺（并发=%s）；进度条在首批响应返回前也可能长时间不变，属正常现象",
            len(missing_store_ids),
            min(conc, len(missing_store_ids)),
        )
        logger.info(
            "过大并发可能触发 online reporting 连接池排队，遇异常请改回 --store-concurrency 1",
        )
        lock = threading.Lock()

        def _job(sid_i: int) -> tuple[int, dict | None]:
            return _fetch_store_json(
                base, sid_i, start_date, session_end, timeout_sec, profile=profile
            )

        with ThreadPoolExecutor(max_workers=min(conc, len(missing_store_ids))) as ex:
            futures = {ex.submit(_job, sid_i): sid_i for sid_i in missing_store_ids}
            for fut in as_completed(futures):
                sid_i, part = fut.result()
                if part is not None:
                    with lock:
                        merged["views"] = {**merged["views"], **part["views"]}
                    _log_profile_timings(f"json_views=store store_id={sid_i}", part)
                done_steps += 1
                progress.update(done_steps, total_steps, f"店铺 {sid_i}")

    if missing_store_ids:
        logger.info(
            "店铺阶段总耗时 %.1fs（%s 店，并发=%s）",
            time.perf_counter() - t_stores0,
            len(missing_store_ids),
            conc,
        )

    progress.finish("JSON 合并完成")
    merged.pop("profileTimingsSec", None)
    return merged


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
    cohort_track_days = int(payload.get("cohortTrackDays") or 30) if isinstance(payload, dict) else 30
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
    .nl-data-parse-error {{
      margin: 0 0 0.75rem;
      padding: 0.55rem 0.75rem;
      border-radius: 8px;
      background: rgba(127, 29, 29, 0.35);
      border: 1px solid rgba(248, 113, 113, 0.45);
      color: #fecaca;
      font-size: 0.82rem;
      line-height: 1.45;
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
    .nl-batch-toolbar {{
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 0.75rem 1rem;
      margin: 0 0 0.75rem;
    }}
    .nl-batch-toolbar label {{
      font-size: 0.82rem;
      color: #cbd5e1;
      display: inline-flex;
      align-items: center;
      gap: 0.45rem;
      cursor: pointer;
      user-select: none;
    }}
    .nl-batch-toolbar input {{ accent-color: #3b9eff; }}
    .nl-batch-count {{
      font-size: 0.82rem;
      color: #94a3b8;
    }}
    .nl-heat-cell {{
      text-align: center !important;
      font-weight: 500;
      cursor: pointer;
    }}
    .nl-total-sessions {{
      cursor: pointer;
      text-decoration: underline dotted rgba(148, 163, 184, 0.55);
      text-underline-offset: 3px;
    }}
    .nl-sticky-0,
    .nl-sticky-1,
    .nl-sticky-2,
    .nl-sticky-3 {{
      position: sticky;
      z-index: 4;
      background: #0f1726;
    }}
    .nl-sticky-0 {{
      left: 0;
      min-width: 120px;
      text-align: left !important;
      box-shadow: 1px 0 0 rgba(148, 163, 184, 0.12);
    }}
    .nl-sticky-1 {{
      left: 120px;
      min-width: 132px;
      box-shadow: 1px 0 0 rgba(148, 163, 184, 0.12);
    }}
    .nl-sticky-2 {{
      left: 252px;
      min-width: 92px;
      box-shadow: 1px 0 0 rgba(148, 163, 184, 0.12);
    }}
    .nl-sticky-3 {{
      left: 344px;
      min-width: 96px;
      box-shadow: 1px 0 0 rgba(148, 163, 184, 0.12);
    }}
    thead .nl-sticky-0,
    thead .nl-sticky-1,
    thead .nl-sticky-2,
    thead .nl-sticky-3 {{
      background: #101b2a;
      z-index: 5;
    }}
    .nl-detail-modal {{
      position: fixed;
      inset: 0;
      z-index: 2000;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 1rem;
    }}
    .nl-detail-modal[hidden] {{
      display: none !important;
    }}
    .nl-detail-modal__backdrop {{
      position: absolute;
      inset: 0;
      background: rgba(2, 6, 23, 0.72);
      cursor: pointer;
    }}
    .nl-detail-modal__panel {{
      position: relative;
      z-index: 1;
      width: min(720px, 96vw);
      max-height: min(82vh, 640px);
      overflow: auto;
      background: #1a2332;
      border: 1px solid rgba(96, 165, 250, 0.35);
      border-radius: 12px;
      box-shadow: 0 20px 50px rgba(0, 0, 0, 0.55);
      padding: 1rem 1.1rem 1rem;
      color: #e8ecf1;
    }}
    .nl-detail-modal__close {{
      position: absolute;
      top: 0.55rem;
      right: 0.55rem;
      width: 2rem;
      height: 2rem;
      border: none;
      border-radius: 8px;
      background: rgba(148, 163, 184, 0.15);
      color: #e8ecf1;
      font-size: 1.25rem;
      line-height: 1;
      cursor: pointer;
    }}
    .nl-detail-modal__close:hover {{
      background: rgba(248, 113, 113, 0.25);
    }}
    .nl-detail-modal__title {{
      margin: 0 2.25rem 0.75rem 0;
      font-size: 0.95rem;
      font-weight: 650;
      color: #93c5fd;
    }}
    .nl-detail-table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.8rem;
    }}
    .nl-detail-table th,
    .nl-detail-table td {{
      padding: 0.45rem 0.55rem;
      border-bottom: 1px solid rgba(148, 163, 184, 0.18);
      text-align: left;
    }}
    .nl-detail-table th {{
      color: #94a3b8;
      font-weight: 600;
      position: sticky;
      top: 0;
      background: #1a2332;
    }}
    .nl-detail-table td:last-child {{
      text-align: right;
      font-variant-numeric: tabular-nums;
    }}
    .nl-detail-empty {{
      margin: 0;
      color: #94a3b8;
      font-size: 0.85rem;
    }}
    .trend-new-listing-table tbody tr:hover td.nl-sticky-0,
    .trend-new-listing-table tbody tr:hover td.nl-sticky-1,
    .trend-new-listing-table tbody tr:hover td.nl-sticky-2,
    .trend-new-listing-table tbody tr:hover td.nl-sticky-3 {{
      background: rgba(61, 157, 255, 0.12) !important;
    }}
    .trend-new-listing-table tbody tr:hover td.nl-heat-cell {{
      filter: brightness(1.14);
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
    <p id="nl-data-parse-error" class="nl-data-parse-error" hidden></p>
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
      <div class="nl-batch-toolbar">
        <label>
          <input type="checkbox" id="nl-collapse-small-batches" />
          折叠上新 ASIN 数小于 500 的批次
        </label>
        <span class="nl-batch-count" id="nl-batch-visible-count"></span>
      </div>
      <p class="trend-new-listing-table-caption">
        上新 / 补录：online 库中 <span class="trend-new-listing-code">amazon_listing.created_at</span> 与同条
        <span class="trend-new-listing-code">amazon_variation.created_at</span>（经 <span class="trend-new-listing-code">variation_id</span>）的日历日相差 <strong>小于 2 天</strong>（即 0 或 1 天）为上新，<strong>≥2 天</strong>或无法比较则为补录。
        「总 session 数」与每日格子均为<strong>仅上新 ASIN</strong> 的 sessions（数字）；<strong>仅「比值」列</strong>为（总 session ÷ 上新数）×10000，单位为 ‱。
        点击<strong>总 session 数</strong>查看该批次追踪期内合并后的 ASIN 明细；点击<strong>某日 session 格</strong>查看当日明细（列：asin、store_id、sessions）。
      </p>
      <div class="trend-new-listing-table-scroll" id="table-scroll">
        <table class="trend-new-listing-table" id="cohort-table"></table>
      </div>
      <p class="trend-report-warning" id="store-warning" hidden></p>
    </div>
    <div id="nl-detail-modal" class="nl-detail-modal" hidden>
      <div class="nl-detail-modal__backdrop" data-nl-modal-close="1" aria-hidden="true"></div>
      <div class="nl-detail-modal__panel" role="dialog" aria-modal="true" aria-labelledby="nl-detail-modal-title">
        <button type="button" class="nl-detail-modal__close" data-nl-modal-close="1" aria-label="关闭">×</button>
        <h4 id="nl-detail-modal-title" class="nl-detail-modal__title"></h4>
        <div id="nl-detail-modal-body"></div>
      </div>
    </div>
  </div>

  <script type="application/json" id="nl-report-payload">{payload_json}</script>
  <script>
    window.__REPORT_SOURCE_URL__ = {source_url_json};
  </script>
  {chart_js_tag}
  <script>
    (() => {{
      const parseEl = document.getElementById('nl-report-payload');
      const errEl = document.getElementById('nl-data-parse-error');
      let payload = {{}};
      try {{
        if (!parseEl || !parseEl.textContent || !parseEl.textContent.trim()) {{
          throw new Error('缺少 nl-report-payload 或内容为空');
        }}
        payload = JSON.parse(parseEl.textContent);
        window.__REPORT_DATA__ = payload;
        parseEl.remove();
      }} catch (e) {{
        if (errEl) {{
          errEl.hidden = false;
          errEl.textContent =
            '报表 JSON 解析失败（超大导出在部分浏览器用 file:// 打开时，旧写法会把整包数据当 JS 对象字面量解析而失败）。请用 Chrome 打开本页或重新导出。错误：' +
            String(e && e.message ? e.message : e);
        }}
      }}
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
      const collapseEl = document.getElementById('nl-collapse-small-batches');
      const batchCountEl = document.getElementById('nl-batch-visible-count');
      const nlModal = document.getElementById('nl-detail-modal');
      const nlModalTitle = document.getElementById('nl-detail-modal-title');
      const nlModalBody = document.getElementById('nl-detail-modal-body');

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
        if (!errEl || errEl.hidden) {{
          metaEl.textContent = '未找到可导出的视图数据。';
        }}
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
      let lastTableCtx = {{ rows: [], nDays: 30 }};
      const COLLAPSE_THRESHOLD = 500;

      const aggregatePartList = (flatParts) => {{
        const map = new Map();
        for (const p of flatParts || []) {{
          const asin = String(p?.asin ?? '').trim();
          if (!asin) continue;
          const sid = Number(p?.storeId ?? 0);
          const k = asin + '\\t' + sid;
          map.set(k, (map.get(k) || 0) + Number(p?.sessions ?? 0));
        }}
        return Array.from(map.entries())
          .map(([k, s]) => {{
            const tab = k.indexOf('\\t');
            return {{
              asin: tab >= 0 ? k.slice(0, tab) : k,
              storeId: tab >= 0 ? Number(k.slice(tab + 1)) : 0,
              sessions: s,
            }};
          }})
          .sort((a, b) => b.sessions - a.sessions || String(a.asin).localeCompare(String(b.asin)));
      }};

      const mergeRowAllDays = (row, nDays) => {{
        const asinDays = Array.isArray(row?.daySessionAsins) ? row.daySessionAsins : [];
        const flat = [];
        for (let i = 0; i < nDays; i++) {{
          const parts = Array.isArray(asinDays[i]) ? asinDays[i] : [];
          for (const p of parts) flat.push(p);
        }}
        return aggregatePartList(flat);
      }};

      const formatDetailTableHtml = (items) => {{
        if (!items.length)
          return '<p class="nl-detail-empty">暂无明细（可能该格无 ASIN 级数据）。</p>';
        const rowsHtml = items
          .map(
            (p) =>
              `<tr><td>${{escapeHtml(p.asin)}}</td><td>${{escapeHtml(String(p.storeId))}}</td><td>${{formatNum(
                p.sessions
              )}}</td></tr>`
          )
          .join('');
        return `<table class="nl-detail-table"><thead><tr><th>asin</th><th>store_id</th><th>sessions</th></tr></thead><tbody>${{rowsHtml}}</tbody></table>`;
      }};

      const openNlModal = (title, items) => {{
        if (!nlModal || !nlModalTitle || !nlModalBody) return;
        nlModalTitle.textContent = title;
        nlModalBody.innerHTML = formatDetailTableHtml(items);
        nlModal.hidden = false;
        document.body.style.overflow = 'hidden';
      }};

      const closeNlModal = () => {{
        if (!nlModal) return;
        nlModal.hidden = true;
        if (nlModalBody) nlModalBody.innerHTML = '';
        document.body.style.overflow = '';
      }};

      if (nlModal) {{
        nlModal.addEventListener('click', (ev) => {{
          if (ev.target && ev.target.getAttribute && ev.target.getAttribute('data-nl-modal-close'))
            closeNlModal();
        }});
        document.addEventListener('keydown', (ev) => {{
          if (ev.key === 'Escape' && nlModal && !nlModal.hidden) closeNlModal();
        }});
      }}

      tableEl.addEventListener('click', (ev) => {{
        const cell = ev.target.closest('td.nl-day-sessions, td.nl-total-sessions');
        if (!cell) return;
        const cohort = cell.getAttribute('data-cohort');
        if (!cohort) return;
        const row = lastTableCtx.rows.find((r) => String(r?.cohortDate ?? '') === cohort);
        if (!row) return;
        const nDays = lastTableCtx.nDays;
        const cdLabel = cohort || '—';
        if (cell.classList.contains('nl-total-sessions')) {{
          const items = mergeRowAllDays(row, nDays);
          openNlModal(`批次 ${{cdLabel}} · 总 session 明细（合并 ${{nDays}} 日）`, items);
          return;
        }}
        const dayIdx = Number(cell.getAttribute('data-day-index'));
        if (!Number.isFinite(dayIdx) || dayIdx < 0) return;
        const asinDays = Array.isArray(row?.daySessionAsins) ? row.daySessionAsins : [];
        const parts = Array.isArray(asinDays[dayIdx]) ? asinDays[dayIdx] : [];
        const items = aggregatePartList(parts);
        openNlModal(`批次 ${{cdLabel}} · 第 ${{dayIdx + 1}} 天 sessions 明细`, items);
      }});

      const heatBg = (v, rowMax) => {{
        const n = Number(v ?? 0);
        if (!Number.isFinite(n) || n <= 0) return 'rgba(122, 29, 29, 0.92)';
        const mx = Math.max(1, Number(rowMax || 1));
        const t = Math.min(1, n / mx);
        const r = Math.round(24 + (51 - 24) * t);
        const g = Math.round(32 + (132 - 32) * t);
        const b = Math.round(48 + (246 - 48) * t);
        return `rgb(${{r}},${{g}},${{b}})`;
      }};

      const formatPermyriad = (sessionsSum, newCount) => {{
        const s = Number(sessionsSum ?? 0);
        const d = Number(newCount ?? 0);
        if (!Number.isFinite(s) || !Number.isFinite(d)) return '—';
        if (d <= 0) {{
          if (s === 0) return '0.00‱';
          return '—';
        }}
        const v = (s / d) * 10000;
        const digits = v !== 0 && Math.abs(v) < 0.1 ? 4 : 2;
        return `${{v.toFixed(digits)}}‱`;
      }};

      const rowSessionsSum = (row, days) => {{
        const arr = Array.isArray(row?.daySessions) ? row.daySessions : [];
        let t = 0;
        for (let i = 0; i < days; i++) t += Number(arr[i] ?? 0);
        return t;
      }};

      const rowDayMax = (row, days) => {{
        const arr = Array.isArray(row?.daySessions) ? row.daySessions : [];
        let m = 0;
        for (let i = 0; i < days; i++) m = Math.max(m, Number(arr[i] ?? 0));
        return m;
      }};

      const renderTable = (view, cohortTrackDays) => {{
        const rows = Array.isArray(view.cohortTable) ? view.cohortTable : [];
        lastTableCtx = {{ rows: [], nDays: Math.max(1, Number(cohortTrackDays || 30)) }};
        if (batchCountEl) batchCountEl.textContent = '';
        if (!rows.length) {{
          tableEl.innerHTML =
            '<tbody><tr><td colspan="99" class="trend-new-listing-table-empty">暂无表格数据。</td></tr></tbody>';
          return;
        }}
        const nDays = Math.max(1, Number(cohortTrackDays || 30));
        const hideSmall = Boolean(collapseEl?.checked);
        const filtered = hideSmall
          ? rows.filter(
              (row) =>
                Number(row?.listingNewCount ?? row?.newAsin ?? 0) >= COLLAPSE_THRESHOLD
            )
          : rows;
        if (batchCountEl) {{
          batchCountEl.textContent = `当前显示 ${{filtered.length}} / ${{rows.length}} 个批次`;
        }}
        if (!filtered.length) {{
          tableEl.innerHTML =
            '<tbody><tr><td colspan="99" class="trend-new-listing-table-empty">在当前筛选下暂无批次。</td></tr></tbody>';
          return;
        }}
        const headerDays = Array.from(
          {{ length: nDays }},
          (_, i) => `<th title="仅上新 ASIN：第 ${{i + 1}} 天 sessions">第${{i + 1}}天</th>`
        ).join('');
        const body = filtered
          .map((row) => {{
            const cd = String(row?.cohortDate ?? '');
            const hasSplit =
              row?.listingNewCount != null || row?.listingRefurbishedCount != null;
            const newN = hasSplit
              ? Number(row?.listingNewCount ?? 0)
              : Number(row?.newAsin ?? 0);
            const refurbN = hasSplit ? Number(row?.listingRefurbishedCount ?? 0) : null;
            const mixText =
              refurbN !== null
                ? `${{formatNum(newN)}} / ${{formatNum(refurbN)}}`
                : `${{formatNum(Number(row?.newAsin ?? 0))}} / —`;
            const daySessions = Array.isArray(row?.daySessions) ? row.daySessions : [];
            const sumS = rowSessionsSum(row, nDays);
            const mx = rowDayMax(row, nDays);
            const ratio = formatPermyriad(sumS, newN);
            const asinDays = Array.isArray(row?.daySessionAsins) ? row.daySessionAsins : [];
            const cells = Array.from({{ length: nDays }}, (_, i) => {{
              const v = Number(daySessions[i] ?? 0);
              const bg = heatBg(v, mx);
              const cohortAttr = `data-cohort="${{escapeHtml(cd)}}"`;
              return `<td class="nl-heat-cell nl-day-sessions" ${{cohortAttr}} data-day-index="${{i}}" style="background-color:${{bg}}">${{formatNum(
                v
              )}}</td>`;
            }}).join('');
            const cohortAttrRow = `data-cohort="${{escapeHtml(cd)}}"`;
            return `
              <tr>
                <td class="nl-sticky-0">${{escapeHtml(cd || '–')}}</td>
                <td class="nl-sticky-1">${{mixText}}</td>
                <td class="nl-sticky-2 nl-total-sessions" ${{cohortAttrRow}}>${{formatNum(sumS)}}</td>
                <td class="nl-sticky-3">${{ratio}}</td>
                ${{cells}}
              </tr>
            `;
          }})
          .join('');
        tableEl.innerHTML = `
          <thead>
            <tr>
              <th class="nl-sticky-0">上新日（PST）</th>
              <th class="nl-sticky-1">上新 / 补录</th>
              <th class="nl-sticky-2">总 session 数</th>
              <th class="nl-sticky-3">比值</th>
              ${{headerDays}}
            </tr>
          </thead>
          <tbody>${{body}}</tbody>
        `;
        lastTableCtx = {{ rows: filtered, nDays }};
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
      if (collapseEl) {{
        collapseEl.addEventListener('change', () => renderView(storeSelect.value));
      }}
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
        "--start-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="与线上页面一致：start_date（须与 --session-end 同时使用）",
    )
    parser.add_argument(
        "--session-end",
        default=None,
        metavar="YYYY-MM-DD",
        help="与线上页面一致：session_end（须与 --start-date 同时使用）",
    )
    parser.add_argument(
        "--api-only",
        action="store_true",
        help="不经 Playwright，直接从 --api-base 请求 /api/trend/new-listing 合并 JSON（大区间推荐）",
    )
    parser.add_argument(
        "--api-base",
        default="http://127.0.0.1:9090",
        help="--api-only 时 **FastAPI 后端**根地址（无尾部斜杠），默认 9090；不要用 Vite 的 5173",
    )
    parser.add_argument(
        "--http-timeout",
        type=int,
        default=900,
        help="--api-only 时单次 HTTP 超时（秒，最大 3600）；大区间 + --profile 时建议 2400～3600",
    )
    parser.add_argument(
        "--wait-heavy-sec",
        type=int,
        default=900,
        help="--api-only 拉主 JSON 前，最多等待 /api/trend/new-listing/heavy-status 变为空闲的秒数（减轻与页面抢锁）",
    )
    parser.add_argument(
        "--store-concurrency",
        type=int,
        default=1,
        help="--api-only 拉各店 json_views=store 时的并发数（默认 1 串行最稳；2～4 可加速但可能打满 online 连接池）",
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        help="--api-only 时在请求 URL 加 profile=1，并在日志中输出 profileTimingsSec 耗时 Top（导出 HTML 前会移除该字段）",
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
    args = parser.parse_args()
    if (args.start_date is None) ^ (args.session_end is None):
        parser.error("--start-date 与 --session-end 必须同时指定或同时省略")
    if args.api_only and (not args.start_date or not args.session_end):
        parser.error("--api-only 需要同时指定 --start-date 与 --session-end")
    if args.api_only:
        try:
            netloc = urlparse(str(args.api_base)).netloc.lower()
        except Exception:
            netloc = ""
        if netloc.endswith(":5173"):
            parser.error(
                "--api-only 的 --api-base 必须指向后端 API（例如 http://127.0.0.1:9090），"
                "不要填 Vite 前端 http://localhost:5173；经前端代理仍会触发 new-listing 的 429 排队。"
            )
        if int(args.store_concurrency) < 1 or int(args.store_concurrency) > 12:
            parser.error("--store-concurrency 需在 1～12 之间")
    return args


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
    start_date: str | None,
    session_end: str | None,
) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    open_url = build_new_listing_page_url(url, start_date, session_end)
    eff_wait_ms = _effective_wait_ms(wait_ms, start_date, session_end)
    if open_url != url or eff_wait_ms != wait_ms:
        logger.info("区间导出: open_url=%s effective_wait_ms=%s", open_url, eff_wait_ms)

    with sync_playwright() as pw:
        browser = _launch_browser(pw, headful=headful)
        try:
            page = browser.new_page(
                viewport={"width": viewport_width, "height": viewport_height},
                device_scale_factor=1,
            )
            logger.info("打开页面: %s", open_url)
            page.goto(open_url, wait_until="domcontentloaded", timeout=eff_wait_ms)
            try:
                page.wait_for_load_state("networkidle", timeout=min(eff_wait_ms, 10_000))
            except PlaywrightTimeoutError:
                logger.info("networkidle 未达成，改用页面内容就绪条件继续")
            page.wait_for_function(READY_CHECK_JS, timeout=eff_wait_ms)
            page.wait_for_timeout(max(0, settle_ms))

            error_text = page.locator(".trend-embed-error-body").all_inner_texts()
            if error_text:
                raise RuntimeError("\n".join(t.strip() for t in error_text if t.strip()))

            payload_raw = page.evaluate(
                EXTRACT_PAYLOAD_JS,
                {
                    "cacheKey": TREND_NEW_LISTING_CACHE_KEY,
                    "startDate": start_date or "",
                    "sessionEnd": session_end or "",
                },
            )
            payload = json.loads(payload_raw)
            if not isinstance(payload, dict) or not isinstance(payload.get("views"), dict):
                raise RuntimeError("导出的 New Listing 数据格式不正确")
            html = _build_report_html(payload, open_url, _load_chart_js_embed_html())
        finally:
            browser.close()

    out_path.write_text(html, encoding="utf-8")
    return out_path.resolve()


def main() -> int:
    setup_logging()
    args = parse_args()
    out_path = Path(args.out).expanduser()

    if bool(getattr(args, "profile", False)) and not bool(getattr(args, "api_only", False)):
        logger.warning("--profile 当前仅对 --api-only 生效，已忽略")

    try:
        if args.api_only:
            http_timeout = max(60, min(int(args.http_timeout), 3600))
            if args.profile:
                logger.info(
                    "已开启 --profile：响应体略大且服务端多计时段；若 all 仍超时，请增大 --http-timeout（例如 3600）"
                )
            payload = fetch_new_listing_merged_payload(
                api_base=str(args.api_base),
                start_date=str(args.start_date),
                session_end=str(args.session_end),
                timeout_sec=http_timeout,
                wait_heavy_sec=max(0, int(args.wait_heavy_sec)),
                store_concurrency=int(args.store_concurrency),
                profile=bool(args.profile) and bool(args.api_only),
            )
            if not isinstance(payload, dict) or not isinstance(payload.get("views"), dict):
                raise RuntimeError("导出的 New Listing 数据格式不正确")
            source_ref = (
                f"{str(args.api_base).rstrip('/')}/trend/New%20Listing"
                f"?start_date={args.start_date}&session_end={args.session_end}"
            )
            html = _build_report_html(payload, source_ref, _load_chart_js_embed_html())
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(html, encoding="utf-8")
            written = out_path.resolve()
        else:
            written = export_report(
                url=args.url,
                out_path=out_path,
                wait_ms=max(1_000, int(args.wait_ms)),
                settle_ms=max(0, int(args.settle_ms)),
                viewport_width=max(800, int(args.viewport_width)),
                viewport_height=max(600, int(args.viewport_height)),
                headful=bool(args.headful),
                start_date=args.start_date,
                session_end=args.session_end,
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
