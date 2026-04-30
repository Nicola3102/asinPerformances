#!/usr/bin/env python3
"""
New Listing 接口压测：模拟多用户、不同日期区间并发请求 /api/trend/new-listing。

用法（在 backend 目录或项目根均可，需能访问后端）::

  cd backend
  python3.11 scripts/stress_new_listing.py --base-url http://127.0.0.1:9090 --workers 12 --iterations 8

多用户 + 同一页面大区间（与浏览器多人打开相同 start/end 接近）::

  python3.11 scripts/stress_new_listing.py --base-url http://127.0.0.1:9090 \\
    --workers 8 --iterations 3 --skip-probe --timeout 3600 \\
    --range-start 2026-01-01 --range-end 2026-04-29

多用户 + 各不相同的随机大区间（压 heavy 排队与 online 池）::

  python3.11 scripts/stress_new_listing.py --base-url http://127.0.0.1:9090 \\
    --workers 10 --iterations 5 --skip-probe --timeout 3600 \\
    --date-pool 12 --min-span-days 90 --max-span-days 120

说明：
  - 默认带 ``skip_sync=true``，主要压「报表构建 + JSON」，避免把 listing 全量同步打满（与页面首屏 JSON 行为一致）。
  - 浏览器带 ``?refresh=1`` 打开 New Listing 时，前端会为 ``json_views=all`` 附加 ``nocache=1``，等同压测里 ``--nocache``：绕过服务端进程内短缓存，**首包更慢**且与 heavy 单槽争用更明显。
  - 若需更接近「点同步并重载」，去掉 ``--skip-sync``（负载会明显变大）。
  - 若浏览器正打开大区间 New Listing，heavy 单槽被占时探测请求会 429；脚本会按 Retry-After 等待并重试。
  - 主循环里每次 ``json_views=all`` 遇 429 也会按 ``Retry-After`` 退避重试（见 ``--all-429-max-rounds``）；否则在单槽下易出现「单次请求约 240s 后 429、整轮 0 次 200」——多为排队超时而非接口损坏。
  - 若日志为 ``Remote end closed connection`` / ``Connection reset by peer`` 且统计里 **无 HTTP 状态码**：多为 **后端连接被掐断**（常见 OOM 杀进程、并发大区间拖死 worker）；脚本会对**瞬时断连**用同一轮次上限做 sleep 重试，仍全失败请降并发或查 ``docker compose logs``。
  - 仍失败可调大 ``--probe-timeout`` / ``--timeout``、服务端 ``new_listing_heavy_acquire_timeout_sec``，或 ``--skip-probe`` 只做 ``json_views=all`` 压测。

依赖：仅 Python 3.10+ 标准库。
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any
from urllib.parse import urlencode


def _transient_connection_error(exc: BaseException) -> bool:
    """
    在收到完整 HTTP 响应前连接被关闭：常见于后端 OOM/exit 137 被 kill、进程重启、
    或单 worker 上挂过多超长 new-listing 连接被对端 reset（与 429 不同，此处无 HTTP 状态码）。
    """
    if isinstance(exc, urllib.error.HTTPError):
        return False
    if isinstance(exc, urllib.error.URLError):
        return True
    if isinstance(exc, (BrokenPipeError, ConnectionResetError)):
        return True
    try:
        from http.client import RemoteDisconnected

        if isinstance(exc, RemoteDisconnected):
            return True
    except ImportError:
        pass
    if isinstance(exc, OSError):
        e = getattr(exc, "errno", None)
        if e in (104, 54, 32, 10054):  # ECONNRESET / pipe / win ECONNRESET 等
            return True
    s = str(exc).lower()
    return (
        "remote end closed connection" in s
        or "connection reset by peer" in s
        or "connection reset" in s
        or "errno 54" in s
    )


@dataclass
class OneShot:
    kind: str
    status: int
    elapsed_ms: float
    detail: str = ""


@dataclass
class RunStats:
    ok: int = 0
    err429: int = 0
    err5xx: int = 0
    err_other: int = 0
    net_err: int = 0
    latencies_ms: list[float] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def add(self, shot: OneShot) -> None:
        with self.lock:
            self.latencies_ms.append(shot.elapsed_ms)
            if shot.status == 200:
                self.ok += 1
            elif shot.status == 429:
                self.err429 += 1
            elif 500 <= shot.status < 600:
                self.err5xx += 1
            else:
                self.err_other += 1

    def add_net_error(self) -> None:
        with self.lock:
            self.net_err += 1


def _retry_after_sec_from_http_error(exc: urllib.error.HTTPError) -> float | None:
    if not exc.headers:
        return None
    raw = exc.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return max(1.0, float(raw))
    except ValueError:
        return None


def _http_get_json_meta(url: str, *, timeout_sec: float) -> tuple[int, str, float, float | None]:
    """
    返回 (status, body, elapsed_ms, retry_after_sec)。
    仅当 status==429 且响应头带 Retry-After 时 retry_after_sec 非空。
    """
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            return resp.getcode() or 200, raw, elapsed_ms, None
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        ra = _retry_after_sec_from_http_error(exc) if int(exc.code) == 429 else None
        return int(exc.code), raw, elapsed_ms, ra
    except Exception:
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        raise


def _http_get_json(url: str, *, timeout_sec: float) -> tuple[int, str, float]:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            return resp.getcode() or 200, raw, elapsed_ms
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        return int(exc.code), raw, elapsed_ms
    except Exception:
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        raise


def _parse_json_detail(body: str, max_len: int = 200) -> str:
    try:
        obj = json.loads(body)
        if isinstance(obj, dict):
            d = obj.get("detail")
            if isinstance(d, str) and d.strip():
                s = d.strip()
                return s if len(s) <= max_len else s[:max_len] + "…"
    except json.JSONDecodeError:
        pass
    b = body.strip().replace("\n", " ")
    return b if len(b) <= max_len else b[:max_len] + "…"


def _date_ranges(*, anchor: date, count: int, min_span: int, max_span: int) -> list[tuple[str, str]]:
    """生成若干互不相同的 [start,end]（YYYY-MM-DD），跨度在 [min_span, max_span] 天。"""
    rng = random.Random(42)
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for i in range(count * 3):
        if len(out) >= count:
            break
        span = rng.randint(min_span, max_span)
        end_off = rng.randint(0, 120)
        end_d = anchor - timedelta(days=end_off)
        start_d = end_d - timedelta(days=span)
        if start_d < date(2025, 1, 1):
            continue
        key = (start_d.isoformat(), end_d.isoformat())
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    while len(out) < count:
        end_d = anchor - timedelta(days=len(out) * 7)
        start_d = end_d - timedelta(days=min_span + len(out))
        out.append((start_d.isoformat(), end_d.isoformat()))
    return out[:count]


def _build_all_url(
    base: str,
    start: str,
    end: str,
    *,
    skip_sync: bool,
    nocache: bool,
) -> str:
    q: dict[str, Any] = {
        "format": "json",
        "json_views": "all",
        "start_date": start,
        "session_end": end,
    }
    if skip_sync:
        q["skip_sync"] = "true"
    if nocache:
        q["nocache"] = "1"
    return f"{base.rstrip('/')}/api/trend/new-listing?{urlencode(q)}"


def _build_store_url(
    base: str,
    store_id: int,
    start: str,
    end: str,
    *,
    skip_sync: bool,
    nocache: bool,
) -> str:
    q: dict[str, Any] = {
        "format": "json",
        "json_views": "store",
        "store_id": str(store_id),
        "start_date": start,
        "session_end": end,
    }
    if skip_sync:
        q["skip_sync"] = "true"
    if nocache:
        q["nocache"] = "1"
    return f"{base.rstrip('/')}/api/trend/new-listing?{urlencode(q)}"


def _worker(
    wid: int,
    *,
    base: str,
    ranges: list[tuple[str, str]],
    store_ids: list[int],
    iterations: int,
    timeout_sec: float,
    skip_sync: bool,
    nocache: bool,
    store_prob: float,
    stats: RunStats,
    all_429_max_rounds: int,
) -> None:
    rng = random.Random(1000 + wid)
    for _ in range(iterations):
        start, end = rng.choice(ranges)
        url = _build_all_url(base, start, end, skip_sync=skip_sync, nocache=nocache)
        status = 0
        body = ""
        elapsed_ms = 0.0
        detail = ""
        try:
            for round_i in range(max(1, all_429_max_rounds)):
                try:
                    status, body, elapsed_ms, ra_hint = _http_get_json_meta(url, timeout_sec=timeout_sec)
                except Exception as exc:
                    if _transient_connection_error(exc) and round_i + 1 < all_429_max_rounds:
                        wait = min(45.0, 4.0 + 5.0 * float(round_i))
                        sys.stderr.write(
                            f"[worker {wid}] GET all transient net ({round_i + 1}/{all_429_max_rounds}): {exc!s}; "
                            f"sleep {wait:.0f}s\n"
                        )
                        time.sleep(wait)
                        continue
                    stats.add_net_error()
                    sys.stderr.write(f"[worker {wid}] GET all failed: {exc!s}\n")
                    status = -1
                    break
                if status == 429:
                    wait = min(120.0, ra_hint if ra_hint is not None else 35.0)
                    if round_i + 1 >= all_429_max_rounds:
                        detail = _parse_json_detail(body)
                        stats.add(OneShot("all", status, elapsed_ms, detail))
                        break
                    time.sleep(wait)
                    continue
                detail = _parse_json_detail(body) if status != 200 else ""
                stats.add(OneShot("all", status, elapsed_ms, detail))
                break
        except Exception as exc:
            stats.add_net_error()
            sys.stderr.write(f"[worker {wid}] GET all failed: {exc!s}\n")
            continue

        if status != 200 or not store_ids or rng.random() > store_prob:
            continue
        sid = rng.choice(store_ids)
        surl = _build_store_url(base, sid, start, end, skip_sync=skip_sync, nocache=nocache)
        try:
            status2, body2, elapsed2 = _http_get_json(surl, timeout_sec=timeout_sec)
            detail2 = _parse_json_detail(body2) if status2 != 200 else ""
            stats.add(OneShot(f"store:{sid}", status2, elapsed2, detail2))
        except Exception as exc:
            stats.add_net_error()
            sys.stderr.write(f"[worker {wid}] GET store {sid} failed: {exc!s}\n")


def _probe_store_ids(
    base: str,
    *,
    timeout_sec: float,
    skip_sync: bool,
    max_rounds: int,
) -> list[int]:
    """
    拉一次默认区间 json_views=all 以拿到 storeIds。
    与页面大区间请求争用同一 heavy 槽位时会 429：按 Retry-After 睡眠后重试；单次请求最长 timeout_sec。
    """
    q: dict[str, Any] = {"format": "json", "json_views": "all"}
    if skip_sync:
        q["skip_sync"] = "true"
    url = f"{base.rstrip('/')}/api/trend/new-listing?{urlencode(q)}"
    last_detail = ""
    for round_i in range(max_rounds):
        try:
            status, body, _elapsed, ra_hint = _http_get_json_meta(url, timeout_sec=timeout_sec)
        except Exception as exc:
            last_detail = str(exc)
            wait = min(60.0, 5.0 * (round_i + 1))
            print(f"probe network/timeout ({round_i + 1}/{max_rounds}): {exc!s}; sleep {wait:.0f}s")
            if round_i + 1 >= max_rounds:
                raise RuntimeError(last_detail) from exc
            time.sleep(wait)
            continue
        if status == 429:
            wait = min(120.0, ra_hint if ra_hint is not None else 35.0)
            print(
                f"probe 429 heavy 排队 ({round_i + 1}/{max_rounds}), sleep {wait:.0f}s — "
                "可先关掉浏览器大区间 New Listing 页再跑压测"
            )
            if round_i + 1 >= max_rounds:
                raise RuntimeError(_parse_json_detail(body))
            time.sleep(wait)
            continue
        if status != 200:
            raise RuntimeError(f"probe all failed HTTP {status}: {_parse_json_detail(body)}")
        data = json.loads(body)
        raw = data.get("storeIds") if isinstance(data, dict) else None
        if not isinstance(raw, list) or not raw:
            return []
        out: list[int] = []
        for x in raw:
            try:
                out.append(int(x))
            except (TypeError, ValueError):
                continue
        return out[:50]
    raise RuntimeError("probe exhausted rounds")


def main() -> int:
    p = argparse.ArgumentParser(
        description="Stress test GET /api/trend/new-listing（多用户 / 大区间 / json_views=all）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--base-url", default="http://127.0.0.1:9090", help="后端根 URL（不要带末尾路径）")
    p.add_argument("--workers", type=int, default=8, help="并发虚拟用户数")
    p.add_argument("--iterations", type=int, default=6, help="每用户顺序请求轮数（每轮 1 次 all + 概率 store）")
    p.add_argument("--timeout", type=float, default=600.0, help="压测主循环单次 HTTP 超时（秒）")
    p.add_argument(
        "--probe-timeout",
        type=float,
        default=None,
        help="探测 storeIds 的单次请求超时（秒），默认 max(600, --timeout)，需覆盖 heavy 排队+一次全量构建",
    )
    p.add_argument(
        "--probe-max-rounds",
        type=int,
        default=36,
        help="探测阶段遇 429/网络错误时最多重试轮数（每轮会 sleep）",
    )
    p.add_argument(
        "--skip-probe",
        action="store_true",
        help="不探测 storeIds，仅压 json_views=all（避免与浏览器抢 heavy 时卡在探测）",
    )
    p.add_argument(
        "--all-429-max-rounds",
        type=int,
        default=36,
        help="每轮 all 遇 429 时最多重试次数（每轮会 sleep Retry-After，默认 36）；设为 1 则不做退避重试",
    )
    p.add_argument("--date-pool", type=int, default=10, help="不同起止区间池大小")
    p.add_argument("--min-span-days", type=int, default=28, help="随机区间最小跨度（天）")
    p.add_argument("--max-span-days", type=int, default=70, help="随机区间最大跨度（天）")
    p.add_argument("--store-follow-prob", type=float, default=0.35, help="all 成功后跟一次随机 store 的概率")
    p.add_argument(
        "--no-skip-sync",
        action="store_true",
        help="不传 skip_sync（压 listing 同步 + 报表，负载更大）",
    )
    p.add_argument("--nocache", action="store_true", help="每次带 nocache=1，绕过服务端短缓存（更狠）")
    p.add_argument("--seed-anchor", default="", help="区间锚点日期 YYYY-MM-DD，默认今天 UTC 日历日")
    p.add_argument(
        "--range-start",
        default="",
        help="与 --range-end 成对使用：所有 worker 只请求该固定起止日（模拟多用户同时查同一页面大区间）；指定后忽略 --date-pool/--min-span-days/--max-span-days",
    )
    p.add_argument(
        "--range-end",
        default="",
        help="与 --range-start 成对使用，YYYY-MM-DD",
    )
    args = p.parse_args()
    skip_sync = not bool(args.no_skip_sync)
    probe_timeout = float(args.probe_timeout) if args.probe_timeout is not None else max(600.0, float(args.timeout))

    if args.workers < 1 or args.iterations < 1:
        p.error("workers / iterations 必须 >= 1")

    try:
        anchor = date.fromisoformat(args.seed_anchor) if args.seed_anchor.strip() else date.today()
    except ValueError:
        p.error("--seed-anchor 须为 YYYY-MM-DD")

    rs = (args.range_start or "").strip()
    re = (args.range_end or "").strip()
    if (rs and not re) or (re and not rs):
        p.error("--range-start 与 --range-end 须同时指定或同时省略")
    if rs and re:
        try:
            d0 = date.fromisoformat(rs)
            d1 = date.fromisoformat(re)
        except ValueError:
            p.error("固定区间须为 YYYY-MM-DD")
        if d0 > d1:
            p.error("range-start 不能晚于 range-end")
        ranges = [(rs, re)]
        span_days = (d1 - d0).days
        print(
            f"固定区间模式: 共 1 组日期 {rs} .. {re}（跨度 {span_days} 天），"
            f"{args.workers} 个 worker 随机复用该 URL（模拟多用户同页大区间）",
        )
    else:
        ranges = _date_ranges(
            anchor=anchor,
            count=args.date_pool,
            min_span=args.min_span_days,
            max_span=args.max_span_days,
        )

    base = args.base_url.rstrip("/")
    all_429_max = max(1, int(args.all_429_max_rounds))
    print(f"base_url={base} workers={args.workers} iterations={args.iterations} skip_sync={skip_sync}")
    print(
        f"probe_timeout={probe_timeout}s probe_max_rounds={args.probe_max_rounds} "
        f"skip_probe={args.skip_probe} all_429_max_rounds={all_429_max}",
    )
    print(f"date_ranges ({len(ranges)}): {ranges[:3]}{' …' if len(ranges) > 3 else ''}")

    if args.skip_probe:
        store_ids: list[int] = []
        print("skip-probe: 不做 storeIds 探测，仅 all 请求")
    else:
        try:
            store_ids = _probe_store_ids(
                base,
                timeout_sec=probe_timeout,
                skip_sync=skip_sync,
                max_rounds=max(1, int(args.probe_max_rounds)),
            )
        except Exception as exc:
            print(f"WARN: 无法探测 storeIds（将只做 all 请求）: {exc}")
            store_ids = []
        else:
            print(f"probe store_ids count={len(store_ids)} sample={store_ids[:8]}")

    stats = RunStats()
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [
            ex.submit(
                _worker,
                i,
                base=base,
                ranges=ranges,
                store_ids=store_ids,
                iterations=args.iterations,
                timeout_sec=args.timeout,
                skip_sync=skip_sync,
                nocache=bool(args.nocache),
                store_prob=args.store_follow_prob,
                stats=stats,
                all_429_max_rounds=all_429_max,
            )
            for i in range(args.workers)
        ]
        for fut in as_completed(futs):
            fut.result()
    total_sec = time.perf_counter() - t0

    lat = sorted(stats.latencies_ms)
    def pct(p: float) -> float | None:
        if not lat:
            return None
        idx = min(len(lat) - 1, max(0, int(round((p / 100.0) * (len(lat) - 1)))))
        return lat[idx]

    total_req = stats.ok + stats.err429 + stats.err5xx + stats.err_other
    print("\n=== 结果 ===")
    print(f"总耗时: {total_sec:.1f}s  有 HTTP 状态请求数: {total_req}  网络/其它异常: {stats.net_err}")
    print(f"200: {stats.ok}  429: {stats.err429}  5xx: {stats.err5xx}  其它4xx等: {stats.err_other}")
    if lat:
        print(
            f"latency_ms  min={lat[0]:.0f}  p50={pct(50) or 0:.0f}  p95={pct(95) or 0:.0f}  max={lat[-1]:.0f}",
        )
    if stats.net_err > 0 and total_req == 0:
        print(
            "\n提示: 全部为「连接被关闭/reset」且无 HTTP 状态时，多为 **后端进程退出（常见 OOM exit 137）**、"
            "或 **并发大区间过长连接** 把 uvicorn 拖死。可：降低 --workers / --max-span-days、加大 Docker 内存、"
            "看 docker compose logs backend；脚本已对瞬时断连做有限次退避重试。"
        )
    print(
        "\n说明: json_views=all 与 html 共用进程内 heavy 单槽；并发多区间时未拿到槽位的请求会在 "
        "new_listing_heavy_acquire_timeout_sec（见服务端配置）后返回 429。"
        " latency 若集中在该秒级附近多为排队超时。"
    )
    print("若 Docker backend 出现 exit 137，多为 OOM；可调低 --workers/--max-span-days 或加大容器内存。")
    return 0 if stats.err5xx == 0 and stats.net_err == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
