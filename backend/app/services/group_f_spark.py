#!/usr/bin/env python3
"""
统计：在「传入的创建周」内创建的父 ASIN 中，在创建后连续 4 周内「无任何活动」的父 ASIN。
排除规则：该父 ASIN 下任意一个子 ASIN 若在创建后 4 周内出现在订单表、或有 impression_count、或有 sessions，则该父 ASIN 不符合（pass/不统计）。
从 backend/.env 读取 online_db_* 连接线上库。传入的周为创建周，仅输出符合上述条件的父 ASIN。
用法（在 backend 目录或项目根）:
  python scripts/run_parent_no_activity_4weeks.py              # 默认最近 4 个创建周
  python scripts/run_parent_no_activity_4weeks.py 6             # 最近 6 个创建周
  python scripts/run_parent_no_activity_4weeks.py 1            # 最近 1 个创建周
  python scripts/run_parent_no_activity_4weeks.py 202605        # 仅指定 1 个创建周
  python scripts/run_parent_no_activity_4weeks.py 202605 202606 202607   # 指定多个创建周（任意个数）
依赖: PySpark（需 MySQL JDBC 驱动 JAR，如 mysql-connector-java）或 pymysql（pip install pymysql 用于回退）
"""
import csv
import logging
import os
import sys
import time
from pathlib import Path

# 确保 backend 在 path 并加载 .env
ROOT = Path(__file__).resolve().parent.parent.parent
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

def _load_env():
    env_path = ROOT / ".env"
    env = {}
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip()
    try:
        from app.config import settings

        env.setdefault("online_db_host", getattr(settings, "ONLINE_DB_HOST", "") or "")
        env.setdefault("online_db_user", getattr(settings, "ONLINE_DB_USER", "") or "")
        env.setdefault("online_db_pwd", getattr(settings, "ONLINE_DB_PWD", "") or "")
        env.setdefault("online_db_port", str(getattr(settings, "ONLINE_DB_PORT", 3306) or 3306))
        env.setdefault("online_db_name", getattr(settings, "ONLINE_DB_NAME", "rug") or "rug")
    except Exception:
        pass
    return env

def _week_no_list(last_n=4):
    """最近 n 周的业务 week_no（与 Group F 专用 week_no 规则一致）。"""
    return _group_f_week_no_list(last_n)

def _week_no_to_sunday(week_no):
    """业务 week_no (YYYYWW) 对应周的周日。"""
    return _group_f_week_no_to_sunday(int(week_no))

def _sunday_to_week_no(d):
    """日期所在周的业务 week_no（与 Group F 专用 week_no 规则一致）。"""
    return _group_f_sunday_to_week_no(d)

def _current_week_no():
    """当前日期所在周的业务 week_no（与 Group F 专用 week_no 规则一致）。"""
    return _group_f_current_week_no()

def _week_no_minus_weeks(week_no, n):
    """业务 week_no 往前推 n 周后的 week_no。"""
    return _group_f_week_no_minus_weeks(int(week_no), n)

def _filter_creation_weeks_ended(creation_week_list):
    """只保留「创建周 + 4 周已全部结束」的创建周，即 creation_week <= 当前周 - 4。"""
    current = _current_week_no()
    latest_creation = _week_no_minus_weeks(current, 4)
    filtered = [w for w in creation_week_list if w <= latest_creation]
    return filtered, current, latest_creation

def _group_f_week_no_list(last_n=4):
    """最近 n 个 Group F 创建周（CLI 专用，不影响其他文件）。"""
    out = []
    current_week = _group_f_current_week_no()
    base_week = _group_f_week_no_minus_weeks(current_week, 4)
    for i in range(last_n):
        out.append(_group_f_week_no_minus_weeks(base_week, i))
    return out

def _group_f_filter_creation_weeks_ended(creation_week_list):
    """CLI 专用：只保留创建后 4 周已结束的 Group F 创建周。"""
    current = _group_f_current_week_no()
    latest_creation = _group_f_week_no_minus_weeks(current, 4)
    filtered = [w for w in creation_week_list if w <= latest_creation]
    return filtered, current, latest_creation

def _activity_weeks(creation_week_list):
    """根据创建周列表，计算「近四周」= 每个创建周之后的连续 4 周的并集（用于 2/3/4 步）。"""
    from datetime import timedelta
    out = set()
    for cw in creation_week_list:
        sun = _week_no_to_sunday(cw)
        for i in range(1, 5):
            next_sun = sun + timedelta(days=7 * i)
            out.add(_sunday_to_week_no(next_sun))
    return sorted(out)

def _creation_week_ranges(creation_week_list):
    """创建周转为可走索引的日期范围 [(week_no, start_dt, end_dt), ...]。"""
    from datetime import datetime, timedelta
    out = []
    for cw in creation_week_list:
        sunday = _week_no_to_sunday(cw)
        start_dt = datetime.combine(sunday, datetime.min.time())
        end_dt = start_dt + timedelta(days=7)
        out.append((int(cw), start_dt, end_dt))
    return out

def _candidate_activity_weeks(creation_week_no):
    """单个创建周对应后续连续 4 个活动周（业务 week_no）。"""
    from datetime import timedelta
    sun = _week_no_to_sunday(int(creation_week_no))
    return [_sunday_to_week_no(sun + timedelta(days=7 * i)) for i in range(1, 5)]

def _sql_step1_created_parents(creation_week_list):
    """Step 1: 查询指定创建周内创建的父 ASIN（parent_asin, parent_asin_create_at, store_id, creation_week_no）。"""
    ph = ",".join([f"'{w}'" for w in creation_week_list])
    return f"""
SELECT av.asin AS parent_asin, av.created_at AS parent_asin_create_at, al.store_id,
       (YEAR(av.created_at) * 100 + WEEK(av.created_at, 0)) AS creation_week_no
FROM amazon_variation av
INNER JOIN (SELECT DISTINCT variation_id, store_id FROM amazon_listing WHERE store_id IN (1, 7,12,25)) al ON al.variation_id = av.id
WHERE (YEAR(av.created_at) * 100 + WEEK(av.created_at, 0)) IN ({ph})
ORDER BY creation_week_no, parent_asin, store_id
"""

def _sql_step2_parents_with_orders(activity_week_list):
    """Step 2: 近四周内有订单的父 ASIN，每 (parent_asin, store_id) 取一条子 ASIN 及订单日期。"""
    ph = ",".join([f"'{w}'" for w in activity_week_list])
    return f"""
SELECT t.parent_asin, t.store_id, t.child_asin, t.example_date
FROM (
  SELECT av.asin AS parent_asin, al.store_id, oi.asin AS child_asin, oi.purchase_utc_date AS example_date,
         ROW_NUMBER() OVER (PARTITION BY av.asin, al.store_id ORDER BY oi.purchase_utc_date) AS rn
  FROM order_item oi
  INNER JOIN amazon_listing al ON oi.asin = al.asin AND oi.store_id = al.store_id AND al.store_id IN (1, 7, 12, 25)
  INNER JOIN amazon_variation av ON al.variation_id = av.id
  WHERE (YEAR(oi.purchase_utc_date) * 100 + WEEK(oi.purchase_utc_date, 0)) IN ({ph})
) t WHERE t.rn = 1
"""

def _sql_step3_parents_with_impression(activity_week_list):
    """Step 3: 近四周内有 impression 的父 ASIN，每 (parent_asin, store_id) 取一条子 ASIN 及 week_no、impression_count。"""
    ph = ",".join([f"'{w}'" for w in activity_week_list])
    return f"""
SELECT t.parent_asin, t.store_id, t.child_asin, t.week_no, t.impression_count
FROM (
  SELECT av.asin AS parent_asin, s.store_id, s.asin AS child_asin, s.week_no, COALESCE(s.impression_count, 0) AS impression_count,
         ROW_NUMBER() OVER (PARTITION BY av.asin, s.store_id ORDER BY s.week_no) AS rn
  FROM amazon_search s
  INNER JOIN amazon_listing al ON s.asin = al.asin AND s.store_id = al.store_id AND al.store_id IN (1, 7, 12, 25)
  INNER JOIN amazon_variation av ON al.variation_id = av.id
  WHERE s.week_no IN ({ph}) AND COALESCE(s.impression_count, 0) > 0
) t WHERE t.rn = 1
"""

def _sql_step4_parents_with_sessions(activity_week_list):
    """Step 4: 近四周内有 sessions 的父 ASIN，每 (parent_asin, store_id) 取一条子 ASIN 及 week_no、sessions。"""
    ph = ",".join([f"'{w}'" for w in activity_week_list])
    return f"""
SELECT t.parent_asin, t.store_id, t.child_asin, t.week_no, t.sessions
FROM (
  SELECT av.asin AS parent_asin, t0.store_id, t0.asin AS child_asin, t0.week_no, COALESCE(t0.sessions, 0) AS sessions,
         ROW_NUMBER() OVER (PARTITION BY av.asin, t0.store_id ORDER BY t0.week_no) AS rn
  FROM amazon_sales_traffic t0
  INNER JOIN amazon_listing al ON t0.asin = al.asin AND t0.store_id = al.store_id AND al.store_id IN (1, 7, 12, 25)
  INNER JOIN amazon_variation av ON al.variation_id = av.id
  WHERE t0.week_no IN ({ph}) AND COALESCE(t0.sessions, 0) > 0
) t WHERE t.rn = 1
"""

def _sql(creation_week_list):
    """单条完整 SQL（供 PySpark 使用）：1 的集合减 2/3/4 的并集，用子查询 + NOT IN 实现。"""
    activity_weeks = _activity_weeks(creation_week_list)
    ph_creation = ",".join([f"'{w}'" for w in creation_week_list])
    ph_activity = ",".join([f"'{w}'" for w in activity_weeks])
    return f"""
SELECT a.parent_asin AS asin, a.parent_asin_create_at AS created_at, a.store_id
FROM (
  SELECT av.asin AS parent_asin, av.created_at AS parent_asin_create_at, al.store_id,
         (YEAR(av.created_at) * 100 + WEEK(av.created_at, 0)) AS creation_week_no
  FROM amazon_variation av
  INNER JOIN (SELECT DISTINCT variation_id, store_id FROM amazon_listing WHERE store_id IN (1, 7,12,25)) al ON al.variation_id = av.id
  WHERE (YEAR(av.created_at) * 100 + WEEK(av.created_at, 0)) IN ({ph_creation})
) a
WHERE (a.parent_asin, a.store_id) NOT IN (
  SELECT DISTINCT av.asin, al.store_id FROM order_item oi
  INNER JOIN amazon_listing al ON oi.asin = al.asin AND oi.store_id = al.store_id AND al.store_id IN (1, 7,12,25)
  INNER JOIN amazon_variation av ON al.variation_id = av.id
  WHERE (YEAR(oi.purchase_utc_date) * 100 + WEEK(oi.purchase_utc_date, 0)) IN ({ph_activity})
  UNION
  SELECT DISTINCT av.asin, s.store_id FROM amazon_search s
  INNER JOIN amazon_listing al ON s.asin = al.asin AND s.store_id = al.store_id AND al.store_id IN (1, 7,12,25)
  INNER JOIN amazon_variation av ON al.variation_id = av.id
  WHERE s.week_no IN ({ph_activity}) AND COALESCE(s.impression_count, 0) > 0
  UNION
  SELECT DISTINCT av.asin, t.store_id FROM amazon_sales_traffic t
  INNER JOIN amazon_listing al ON t.asin = al.asin AND t.store_id = al.store_id AND al.store_id IN (1, 7,12,25)
  INNER JOIN amazon_variation av ON al.variation_id = av.id
  WHERE t.week_no IN ({ph_activity}) AND COALESCE(t.sessions, 0) > 0
)
ORDER BY a.creation_week_no, a.parent_asin, a.store_id
"""

def run_with_pyspark(env, creation_week_list):
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        log.info("PySpark 未安装，跳过 JDBC 方式")
        return None
    host = env.get("online_db_host") or env.get("online_db_host2")
    port = env.get("online_db_port", "3306")
    db = env.get("online_db_name") or env.get("online_db_database", "rug")
    user = env.get("online_db_user") or env.get("online_db_user2")
    pwd = env.get("online_db_pwd") or env.get("online_db_pwd2")
    if not host or not user:
        log.info("未配置 online_db_host / online_db_user，跳过 PySpark")
        return None
    try:
        url = f"jdbc:mysql://{host}:{port}/{db}"
        log.info("尝试使用 PySpark JDBC 连接 %s ...", host)
        t0 = time.perf_counter()
        spark = SparkSession.builder.appName("parent_no_activity_4weeks").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        log.info("Spark 会话已创建，耗时 %.1f 秒", time.perf_counter() - t0)
        sql = _sql(creation_week_list)
        log.info("正在通过 JDBC 执行 SQL（复杂查询可能在 MySQL 端耗时较长）...")
        t1 = time.perf_counter()
        df = spark.read.jdbc(
            url=url,
            table=f"({sql}) AS t",
            properties={"user": user, "password": pwd, "driver": "com.mysql.cj.jdbc.Driver"},
        )
        log.info("JDBC 查询已返回 DataFrame，耗时 %.1f 秒", time.perf_counter() - t1)
        return df
    except Exception as e:
        log.warning("PySpark JDBC 失败（将回退 pymysql）: %s", e)
        return None

def _execute_query(conn, sql, step_name):
    """执行一条 SQL，打日志并返回 (cols, rows)。"""
    with conn.cursor() as cur:
        t0 = time.perf_counter()
        log.info("[Group F] %s 执行中...", step_name)
        cur.execute(sql)
        elapsed = time.perf_counter() - t0
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    log.info("[Group F] %s 完成，返回 %d 行，耗时 %.1f 秒", step_name, len(rows), elapsed)
    return cols, rows

def _execute_nonquery(conn, sql, params, step_name):
    """执行 DDL/DML，打日志。"""
    with conn.cursor() as cur:
        t0 = time.perf_counter()
        log.info("[Group F] %s 执行中...", step_name)
        cur.execute(sql, params)
        affected = cur.rowcount
    elapsed = time.perf_counter() - t0
    log.info("[Group F] %s 完成，影响 %s 行，耗时 %.1f 秒", step_name, affected, elapsed)
    return affected

def _executemany_nonquery(conn, sql, params, step_name):
    """批量执行 DDL/DML，打日志。"""
    with conn.cursor() as cur:
        t0 = time.perf_counter()
        log.info("[Group F] %s 执行中（批量 %d 条）...", step_name, len(params))
        cur.executemany(sql, params)
        affected = cur.rowcount
    elapsed = time.perf_counter() - t0
    log.info("[Group F] %s 完成，影响 %s 行，耗时 %.1f 秒", step_name, affected, elapsed)
    return affected

def run_with_pymysql(env, creation_week_list):
    """分步执行 SQL，每步完成后立即打日志，便于观察进度。"""
    try:
        import pymysql
    except ImportError:
        log.info("pymysql 未安装")
        return None
    host = env.get("online_db_host") or env.get("online_db_host2")
    port = int(env.get("online_db_port", 3306))
    db = env.get("online_db_name") or env.get("online_db_database", "rug")
    user = env.get("online_db_user") or env.get("online_db_user2")
    pwd = env.get("online_db_pwd") or env.get("online_db_pwd2")
    if not host or not user:
        return None
    log.info("[Group F] 使用 pymysql 连接 %s:%s/%s ...", host, port, db)
    t0 = time.perf_counter()
    conn = pymysql.connect(
        host=host, port=port, user=user, password=pwd, database=db, charset="utf8mb4",
        read_timeout=1800, write_timeout=1800,
    )
    log.info("[Group F] 连接成功，耗时 %.1f 秒", time.perf_counter() - t0)
    run_start = time.perf_counter()
    activity_weeks = _activity_weeks(creation_week_list)
    log.info("[Group F] 活动周（近四周）week_no: %s，创建周: %s", activity_weeks, creation_week_list)
    try:
        # Step 0: 创建临时表并装载创建周范围，避免 YEAR/WEEK 导致索引失效
        _execute_nonquery(conn, "DROP TEMPORARY TABLE IF EXISTS tmp_group_f_creation_weeks", (), "Step 0/6 清理创建周临时表")
        _execute_nonquery(conn, "DROP TEMPORARY TABLE IF EXISTS tmp_group_f_candidates", (), "Step 0/6 清理候选父临时表")
        _execute_nonquery(conn, "DROP TEMPORARY TABLE IF EXISTS tmp_group_f_candidate_weeks", (), "Step 0/6 清理候选周临时表")
        _execute_nonquery(
            conn,
            """
            CREATE TEMPORARY TABLE tmp_group_f_creation_weeks (
              creation_week_no INT NOT NULL,
              start_dt DATETIME NOT NULL,
              end_dt DATETIME NOT NULL,
              PRIMARY KEY (creation_week_no)
            )
            """,
            (),
            "Step 0/6 创建创建周临时表",
        )
        creation_ranges = _creation_week_ranges(creation_week_list)
        _executemany_nonquery(
            conn,
            "INSERT INTO tmp_group_f_creation_weeks (creation_week_no, start_dt, end_dt) VALUES (%s, %s, %s)",
            creation_ranges,
            "Step 0/6 写入创建周范围",
        )

        # Step 1: 仅按创建时间范围拉取候选父 ASIN，并建立候选临时表
        log.info("[Group F] Step 1/4 开始（距查询开始 %.0f 秒）", time.perf_counter() - run_start)
        _execute_nonquery(
            conn,
            """
            CREATE TEMPORARY TABLE tmp_group_f_candidates AS
            SELECT
              av.id AS parent_id,
              av.asin AS parent_asin,
              av.created_at AS parent_asin_create_at,
              al.store_id AS store_id,
              cw.creation_week_no AS creation_week_no,
              cw.end_dt AS window_start,
              DATE_ADD(cw.end_dt, INTERVAL 28 DAY) AS window_end
            FROM tmp_group_f_creation_weeks cw
            INNER JOIN amazon_variation av
              ON av.created_at >= cw.start_dt AND av.created_at < cw.end_dt
            INNER JOIN amazon_listing al
              ON al.variation_id = av.id AND al.store_id IN (1, 7, 12, 25)
            GROUP BY av.id, av.asin, av.created_at, al.store_id, cw.creation_week_no, cw.end_dt
            """,
            (),
            "Step 1/4 构建候选父临时表",
        )
        _, candidate_rows = _execute_query(
            conn,
            """
            SELECT parent_id, parent_asin, parent_asin_create_at, store_id, creation_week_no
            FROM tmp_group_f_candidates
            ORDER BY creation_week_no, parent_asin, store_id
            """,
            "Step 1/4 读取候选父 ASIN",
        )
        if not candidate_rows:
            log.info("[Group F] 无候选，结果为空")
            return ["parent_asin", "created_at", "store_id", "impression_count_asin", "order_asin", "sessions_asin"], []
        base_rows = [(r[1], r[2], r[3], r[4], r[0]) for r in candidate_rows]

        candidate_week_rows = []
        for parent_id, parent_asin, _created_at, store_id, creation_week_no in candidate_rows:
            for activity_week_no in _candidate_activity_weeks(creation_week_no):
                candidate_week_rows.append((parent_id, parent_asin, store_id, int(activity_week_no)))
        _execute_nonquery(
            conn,
            """
            CREATE TEMPORARY TABLE tmp_group_f_candidate_weeks (
              parent_id BIGINT NOT NULL,
              parent_asin VARCHAR(64) NOT NULL,
              store_id INT NOT NULL,
              activity_week_no INT NOT NULL,
              PRIMARY KEY (parent_id, store_id, activity_week_no)
            )
            """,
            (),
            "Step 1/4 创建候选活动周临时表",
        )
        _executemany_nonquery(
            conn,
            "INSERT INTO tmp_group_f_candidate_weeks (parent_id, parent_asin, store_id, activity_week_no) VALUES (%s, %s, %s, %s)",
            candidate_week_rows,
            "Step 1/4 写入候选活动周",
        )

        # Step 2: 仅针对候选父 ASIN，检查近四周内是否有订单
        log.info("[Group F] Step 2/4 开始（距查询开始 %.0f 秒）", time.perf_counter() - run_start)
        _, rows2 = _execute_query(
            conn,
            """
            SELECT c.parent_asin, c.store_id, MIN(oi.asin) AS child_asin
            FROM tmp_group_f_candidates c
            INNER JOIN amazon_listing al
              ON al.variation_id = c.parent_id AND al.store_id = c.store_id
            INNER JOIN order_item oi
              ON oi.asin = al.asin
             AND oi.store_id = al.store_id
             AND oi.purchase_utc_date >= c.window_start
             AND oi.purchase_utc_date < c.window_end
            GROUP BY c.parent_asin, c.store_id
            """,
            "Step 2/4 候选父中有订单的父 ASIN",
        )
        order_asin_map = {}
        for r in rows2:
            if r[0] is not None and r[2] is not None:
                order_asin_map[(r[0], r[1])] = r[2]

        # Step 3: 仅针对候选父 ASIN，检查近四周内是否有 impression
        log.info("[Group F] Step 3/4 开始（距查询开始 %.0f 秒）", time.perf_counter() - run_start)
        _, rows3 = _execute_query(
            conn,
            """
            SELECT cw.parent_asin, cw.store_id, MIN(s.asin) AS child_asin
            FROM tmp_group_f_candidate_weeks cw
            INNER JOIN amazon_listing al
              ON al.variation_id = cw.parent_id AND al.store_id = cw.store_id
            INNER JOIN amazon_search s
              ON s.asin = al.asin
             AND s.store_id = al.store_id
             AND s.week_no = cw.activity_week_no
            WHERE COALESCE(s.impression_count, 0) > 0
            GROUP BY cw.parent_asin, cw.store_id
            """,
            "Step 3/4 候选父中有 impression 的父 ASIN",
        )
        impression_count_asin_map = {}
        for r in rows3:
            if r[0] is not None and r[2] is not None:
                impression_count_asin_map[(r[0], r[1])] = r[2]

        # Step 4: 仅针对候选父 ASIN，检查近四周内是否有 sessions
        log.info("[Group F] Step 4/4 开始（距查询开始 %.0f 秒）", time.perf_counter() - run_start)
        _, rows4 = _execute_query(
            conn,
            """
            SELECT cw.parent_asin, cw.store_id, MIN(t0.asin) AS child_asin
            FROM tmp_group_f_candidate_weeks cw
            INNER JOIN amazon_listing al
              ON al.variation_id = cw.parent_id AND al.store_id = cw.store_id
            INNER JOIN amazon_sales_traffic t0
              ON t0.asin = al.asin
             AND t0.store_id = al.store_id
             AND t0.week_no = cw.activity_week_no
            WHERE COALESCE(t0.sessions, 0) > 0
            GROUP BY cw.parent_asin, cw.store_id
            """,
            "Step 4/4 候选父中有 sessions 的父 ASIN",
        )
        sessions_asin_map = {}
        for r in rows4:
            if r[0] is not None and r[2] is not None:
                sessions_asin_map[(r[0], r[1])] = r[2]

        # 结果：列出指定周的所有父 ASIN，并填充 impression_count_asin / order_asin / sessions_asin（有则填一个子 ASIN，无则空）
        cols = ["parent_asin", "created_at", "store_id", "impression_count_asin", "order_asin", "sessions_asin"]
        result = []
        for r in base_rows:
            pa, created_at, store_id = r[0], r[1], r[2]
            key = (pa, store_id) if (pa is not None and store_id is not None) else None
            imp_asin = (impression_count_asin_map.get(key) or "") if key else ""
            ord_asin = (order_asin_map.get(key) or "") if key else ""
            sess_asin = (sessions_asin_map.get(key) or "") if key else ""
            result.append((pa, created_at, store_id, imp_asin, ord_asin, sess_asin))
        log.info("[Group F] 全部完成，共 %d 条，总耗时 %.1f 秒", len(result), time.perf_counter() - run_start)
        return cols, result
    finally:
        conn.close()
        log.info("[Group F] 连接已关闭")


def _group_f_first_sunday_of_year(year: int):
    """Group F 专用：包含该年 1 月 1 日的那周的周日。如 2026-01-01 为周四，则 2025-12-28。"""
    from datetime import date, timedelta
    jan1 = date(year, 1, 1)
    return jan1 - timedelta(days=(jan1.weekday() + 1) % 7)


def _group_f_sunday_to_week_no(d) -> int:
    """
    Group F 专用：日期所在周（周日为一周开始）的 week_no。
    规则：包含 1 月 1 日的那周为 week 1。如 2026-02-08 到 2026-02-14 = 202607，2026-03-08 到 2026-03-14 = 202611。
    """
    from datetime import date, timedelta
    if hasattr(d, "date"):
        d = d.date()
    sunday = d - timedelta(days=(d.weekday() + 1) % 7)
    thursday = sunday + timedelta(days=4)
    first = _group_f_first_sunday_of_year(thursday.year)
    week_num = (sunday - first).days // 7 + 1
    return thursday.year * 100 + week_num


def _group_f_current_week_no() -> int:
    """Group F 专用：当前日期所在周的 week_no。"""
    from datetime import date
    return _group_f_sunday_to_week_no(date.today())


def _group_f_week_no_to_sunday(week_no: int):
    """Group F 专用：week_no 对应周的周日。"""
    from datetime import timedelta
    y, w = week_no // 100, week_no % 100
    first = _group_f_first_sunday_of_year(y)
    return first + timedelta(days=(w - 1) * 7)


def _group_f_week_no_minus_weeks(week_no: int, n: int) -> int:
    """Group F 专用：week_no 往前推 n 周后的 week_no。"""
    from datetime import timedelta
    sun = _group_f_week_no_to_sunday(week_no)
    past = sun - timedelta(days=7 * n)
    return _group_f_sunday_to_week_no(past)


def _group_f_to_mysql_week_no(week_no: int) -> int:
    """Group F 创建周转为业务 week_no；当前业务定义与 Group F 周号规则一致。"""
    return int(week_no)


def compute_scan_weeks_list_for_api(current_week: int, scan_weeks: int) -> list:
    """
    供 /group-f 接口使用。根据当前周与扫描周数计算 week_no 列表。
    逻辑：当前周往前 4 周作为 base，再倒推 scan_weeks 周。
    使用 Group F 专用 week_no 规则：包含 1 月 1 日的那周为 week 1，如 2026-02-08~14=202607，2026-03-08~14=202611。
    如 current_week=202611, scan_weeks=2 → [202607, 202606]
    """
    base_week_no = _group_f_week_no_minus_weeks(current_week, 4)
    return [_group_f_week_no_minus_weeks(base_week_no, i) for i in range(scan_weeks)]


def get_group_f(scan_weeks_list: list) -> list:
    """
    供 /group-f 接口调用。返回指定周所有父 ASIN 的完整数据。
    每行: (parent_asin, created_at, store_id, impression_count_asin, order_asin, sessions_asin)
    """
    if not scan_weeks_list:
        return []
    try:
        from app.config import settings
    except ImportError:
        return []
    env = {
        "online_db_host": getattr(settings, "ONLINE_DB_HOST", "") or "",
        "online_db_user": getattr(settings, "ONLINE_DB_USER", "") or "",
        "online_db_pwd": getattr(settings, "ONLINE_DB_PWD", "") or "",
        "online_db_port": str(getattr(settings, "ONLINE_DB_PORT", 3306) or 3306),
        "online_db_name": getattr(settings, "ONLINE_DB_NAME", "rug") or "rug",
    }
    if not env["online_db_host"] or not env["online_db_user"]:
        return []
    out = run_with_pymysql(env, [int(w) for w in scan_weeks_list])
    if out is None:
        return []
    _, rows = out
    return rows


def _parse_weeks():
    """解析命令行（CLI 使用 Group F 专用 week_no）：无参=最近4个创建周；一个数字N(1..99)=最近N个创建周；一个6位数=单周；多个参数=显式创建周 week_no 列表。"""
    args = sys.argv[1:]
    if not args:
        return _group_f_week_no_list(4)
    if len(args) == 1:
        raw = args[0]
        try:
            n = int(raw)
            if 1 <= n <= 99:
                return _group_f_week_no_list(n)
            if 100000 <= n <= 999999:
                return [n]
        except ValueError:
            pass
        return [int(raw)]
    return [int(x) for x in args]

def main():
    log.info("加载 .env ...")
    env = _load_env()
    creation_week_list_group_f = _parse_weeks()
    creation_week_list_group_f, current_week_group_f, latest_creation_group_f = _group_f_filter_creation_weeks_ended(creation_week_list_group_f)
    if not creation_week_list_group_f:
        log.warning(
            "当前 Group F 周 %s，仅统计创建周 <= %s（创建后 4 周已结束）；传入的创建周均晚于该范围，无数据可统计",
            current_week_group_f,
            latest_creation_group_f,
        )
        print("无符合条件的创建周（创建周须 <= 当前周-4，即创建后 4 周均已结束）", flush=True)
        return
    creation_week_list_business = [_group_f_to_mysql_week_no(w) for w in creation_week_list_group_f]
    print(
        "当前 Group F 周 week_no: %s，最晚统计创建周: %s（仅统计创建周 <= %s，即创建后 4 周已全部结束）"
        % (current_week_group_f, latest_creation_group_f, latest_creation_group_f),
        flush=True,
    )
    print("Group F 创建周 week_no:", creation_week_list_group_f, f"（共 {len(creation_week_list_group_f)} 个创建周）", flush=True)
    print("对应业务 week_no:", creation_week_list_business, flush=True)
    print("结果: 列出指定周所有父 ASIN，并展示 created_at、store_id；impression_count_asin/order_asin/sessions_asin 为该父 ASIN 下在近四周内有对应数据的任意一个子 ASIN（无则空）\n", flush=True)

    # 优先使用优化后的 pymysql 路径；PySpark 作为兜底
    log.info("优先使用 pymysql 执行优化后的分步查询")
    out = run_with_pymysql(env, creation_week_list_business)
    if out is not None:
        cols, rows = out
        print("(使用 pymysql，若需 PySpark 请配置 spark.driver.extraClassPath 加入 mysql-connector-java JAR)\n")
        # 注释掉写入 CSV 部分
        # ts = time.strftime("%Y%m%d_%H%M%S", time.localtime())
        # weeks_str = "_".join(str(w) for w in creation_week_list)
        # csv_path = Path(__file__).resolve().parent / f"parent_no_activity_{weeks_str}_{ts}.csv"
        # with open(csv_path, "w", newline="", encoding="utf-8") as f:
        #     w = csv.writer(f)
        #     w.writerow(cols)
        #     for r in rows:
        #         w.writerow(["" if x is None else x for x in r])
        # log.info("完整结果已保存: %s", csv_path)
        print("\t".join(cols))
        for r in rows:
            print("\t".join(str(x) if x is not None else "" for x in r))
        print(f"\n共 {len(rows)} 条（指定周全部父 ASIN）")

        # 注释掉无活动 CSV 写入
        # no_activity = [r for r in rows if (not (r[3] or r[4] or r[5]))]
        # if no_activity:
        #     no_activity_path = Path(__file__).resolve().parent / f"parent_no_activity_无活动_{weeks_str}_{ts}.csv"
        #     with open(no_activity_path, "w", newline="", encoding="utf-8") as f:
        #         w = csv.writer(f)
        #         w.writerow(["parent_asin", "created_at", "store_id"])
        #         for r in no_activity:
        #             w.writerow([r[0], r[1], r[2]])
        #     log.info("三列均无值的父 ASIN 共 %d 条，已保存: %s", len(no_activity), no_activity_path)
        #     print(f"\nimpression_count_asin / order_asin / sessions_asin 均无值的父 ASIN 共 {len(no_activity)} 条:")
        #     print("parent_asin\tcreated_at\tstore_id")
        #     for r in no_activity:
        #         print("\t".join(str(x) if x is not None else "" for x in (r[0], r[1], r[2])))
        return

    df = run_with_pyspark(env, creation_week_list_business)
    if df is not None:
        log.info("计算行数并输出（show 可能触发额外拉取）...")
        t0 = time.perf_counter()
        n = df.count()
        log.info("行数: %d，耗时 %.1f 秒", n, time.perf_counter() - t0)
        print("(使用 PySpark JDBC)\n")
        df.show(100, truncate=False)
        print(f"\n共 {n} 个父 ASIN 符合条件")
        return

    print("ERROR: 未配置 .env 中的 online_db_host / online_db_user 等，或未安装 pyspark/pymysql", file=sys.stderr)
    sys.exit(1)

if __name__ == "__main__":
    main()
