import { useEffect, useMemo, useRef, useState } from 'react'
import {
  getGroupFData,
  getGroupFLockStatus,
  releaseGroupFLock,
  type GroupFResponse,
  type GroupFLockStatus,
  type GroupFRow,
} from '../../api/client'
import { PaginationControls } from '../../components/paginationControls'
import { formatCreatedAt } from '../../lib/formatters'
import {
  downloadGroupFCsv,
  formatCacheTime,
  loadGroupFCache,
  saveGroupFCache,
  type GroupFCacheEntry,
} from '../../lib/groupFExport'

const GROUP_F_PAGE_SIZE = 30

type AsinFilter = 'all' | 'has' | 'empty'

export default function GroupFPage() {
  const [scanWeeks, setScanWeeks] = useState(2)
  const [specificWeeks, setSpecificWeeks] = useState('')
  const [submittedSpecificWeeks, setSubmittedSpecificWeeks] = useState('')
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [data, setData] = useState<GroupFResponse | null>(null)
  const [dataRequestKey, setDataRequestKey] = useState<string | null>(null)
  const [cacheByKey, setCacheByKey] = useState<Record<string, GroupFCacheEntry>>(() => loadGroupFCache())
  const hasDataRef = useRef(false)
  const [lockStatus, setLockStatus] = useState<GroupFLockStatus | null>(null)
  const [waitingForLock, setWaitingForLock] = useState(false)
  const [reloadNonce, setReloadNonce] = useState(0)
  const lockPollTimerRef = useRef<number | null>(null)

  const [storeIdFilter, setStoreIdFilter] = useState('')
  const [impressionFilter, setImpressionFilter] = useState<AsinFilter>('all')
  const [orderFilter, setOrderFilter] = useState<AsinFilter>('all')
  const [sessionsFilter, setSessionsFilter] = useState<AsinFilter>('all')

  const weekNos = useMemo(() => {
    if (!submittedSpecificWeeks.trim()) return null
    const nums = submittedSpecificWeeks
      .split(/[,，\s]+/)
      .map((s) => parseInt(s.trim(), 10))
      .filter((n) => !Number.isNaN(n) && n >= 100000 && n <= 999999)
    return nums.length > 0 ? nums : null
  }, [submittedSpecificWeeks])
  const weekNosKey = useMemo(() => (weekNos == null ? null : JSON.stringify(weekNos)), [weekNos])

  const requestKey = weekNosKey == null ? `scan:${scanWeeks}` : `weeks:${weekNosKey}`
  const cachedEntry = cacheByKey[requestKey] ?? null
  const displayData = dataRequestKey === requestKey ? data : (cachedEntry?.data ?? null)
  const isShowingCachedData = dataRequestKey !== requestKey && cachedEntry != null

  useEffect(() => {
    setPage(1)
  }, [scanWeeks, submittedSpecificWeeks])

  useEffect(() => {
    setPage(1)
  }, [storeIdFilter, impressionFilter, orderFilter, sessionsFilter])

  useEffect(() => {
    hasDataRef.current = false
    const ctrl = new AbortController()
    setLoading(true)
    setError(null)
    setWaitingForLock(false)
    setLockStatus(null)
    getGroupFData(scanWeeks, weekNos ?? undefined, ctrl.signal)
      .then((res) => {
        hasDataRef.current = true
        setData(res)
        setDataRequestKey(requestKey)
        setCacheByKey((prev) => {
          const next = {
            ...prev,
            [requestKey]: {
              savedAt: new Date().toISOString(),
              data: res,
            },
          }
          saveGroupFCache(next)
          return next
        })
        setError(null)
        setWaitingForLock(false)
        setLockStatus(null)
      })
      .catch((e) => {
        if (e?.name === 'AbortError') return
        if ((e as { status?: number } | null)?.status === 429) {
          setWaitingForLock(true)
          setError(null)
          return
        }
        if (!hasDataRef.current) {
          setError(e instanceof Error ? e.message : 'Failed to load')
        }
      })
      .finally(() => setLoading(false))
    return () => ctrl.abort()
  }, [scanWeeks, weekNos, reloadNonce, requestKey])

  useEffect(() => {
    if (!waitingForLock) {
      if (lockPollTimerRef.current != null) {
        window.clearTimeout(lockPollTimerRef.current)
        lockPollTimerRef.current = null
      }
      return
    }

    const ctrl = new AbortController()
    let cancelled = false

    const poll = async () => {
      try {
        const status = await getGroupFLockStatus(ctrl.signal)
        if (cancelled) return
        setLockStatus(status)
        if (status.lock_held) {
          lockPollTimerRef.current = window.setTimeout(poll, status.is_stuck ? 5000 : 3000)
          return
        }
        setWaitingForLock(false)
        setReloadNonce((n) => n + 1)
      } catch (e) {
        if ((e as { name?: string } | null)?.name === 'AbortError' || cancelled) return
        setError(e instanceof Error ? e.message : '获取 Group F 查询状态失败')
        lockPollTimerRef.current = window.setTimeout(poll, 5000)
      }
    }

    poll()
    return () => {
      cancelled = true
      ctrl.abort()
      if (lockPollTimerRef.current != null) {
        window.clearTimeout(lockPollTimerRef.current)
        lockPollTimerRef.current = null
      }
    }
  }, [waitingForLock, requestKey])

  const handleSpecificWeeksKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault()
      setSubmittedSpecificWeeks(specificWeeks.trim())
    }
  }

  const handleReleaseLockAndRetry = async () => {
    try {
      setLoading(true)
      setError(null)
      await releaseGroupFLock()
      setWaitingForLock(false)
      setLockStatus(null)
      setReloadNonce((n) => n + 1)
    } catch (e) {
      setError(e instanceof Error ? e.message : '释放 Group F 锁失败')
    } finally {
      setLoading(false)
    }
  }

  const rawRows = useMemo(() => displayData?.rows ?? [], [displayData?.rows])
  const filteredRows = useMemo(() => {
    let filtered = rawRows
    if (storeIdFilter.trim()) {
      const ids = new Set(
        storeIdFilter.split(/[,，\s]+/).map((s) => parseInt(s.trim(), 10)).filter((n) => !Number.isNaN(n))
      )
      if (ids.size > 0) {
        filtered = filtered.filter((r: GroupFRow) => r.store_id != null && ids.has(r.store_id))
      }
    }
    const hasValue = (v: string | null | undefined) => v != null && String(v).trim() !== ''
    if (impressionFilter === 'has') filtered = filtered.filter((r: GroupFRow) => hasValue(r.impression_count_asin))
    else if (impressionFilter === 'empty') filtered = filtered.filter((r: GroupFRow) => !hasValue(r.impression_count_asin))
    if (orderFilter === 'has') filtered = filtered.filter((r: GroupFRow) => hasValue(r.order_asin))
    else if (orderFilter === 'empty') filtered = filtered.filter((r: GroupFRow) => !hasValue(r.order_asin))
    if (sessionsFilter === 'has') filtered = filtered.filter((r: GroupFRow) => hasValue(r.sessions_asin))
    else if (sessionsFilter === 'empty') filtered = filtered.filter((r: GroupFRow) => !hasValue(r.sessions_asin))
    return filtered
  }, [rawRows, storeIdFilter, impressionFilter, orderFilter, sessionsFilter])

  const rows = filteredRows
  const totalPages = Math.max(1, Math.ceil(rows.length / GROUP_F_PAGE_SIZE))
  const currentPage = Math.min(Math.max(1, page), totalPages)
  const startIdx = (currentPage - 1) * GROUP_F_PAGE_SIZE
  const pageRows = rows.slice(startIdx, startIdx + GROUP_F_PAGE_SIZE)
  const lockMessage = waitingForLock
    ? lockStatus?.lock_held
      ? `Group F 查询正在执行中，已运行 ${Math.round(lockStatus.duration_seconds ?? 0)} 秒，完成后将自动刷新结果。${lockStatus.is_stuck ? ' 若长时间不结束，可手动释放锁。' : ''}`
      : '检测到 Group F 查询已完成，正在自动刷新结果...'
    : null
  const cacheMessage = isShowingCachedData
    ? `当前展示的是该查询条件上次成功结果（缓存时间：${formatCacheTime(cachedEntry?.savedAt)}），最新查询完成后会自动刷新。`
    : null

  return (
    <div className="app">
      <h1>Group F</h1>
      <div className="group-f-controls">
        <label>
          指定 Group F 创建周：
          <input
            type="text"
            placeholder="202611,202610 回车查询"
            value={specificWeeks}
            onChange={(e) => setSpecificWeeks(e.target.value)}
            onKeyDown={handleSpecificWeeksKeyDown}
            disabled={loading}
            className="group-f-filter-input"
          />
          <span className="group-f-hint">（回车执行，留空则按扫描周数）</span>
        </label>
        <label>
          扫描周数：
          <select
            value={scanWeeks}
            onChange={(e) => setScanWeeks(Number(e.target.value))}
            disabled={loading || !!submittedSpecificWeeks.trim()}
          >
            {[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12].map((n) => (
              <option key={n} value={n}>{n}</option>
            ))}
          </select>
        </label>
        <span className="group-f-weeks">
          Group F 创建周：{loading && !displayData ? '计算中...' : displayData?.weeks?.length ? displayData.weeks.join(', ') : '–'}
        </span>
        <span className="group-f-weeks">
          对应业务周：{loading && !displayData ? '计算中...' : displayData?.business_weeks?.length ? displayData.business_weeks.join(', ') : '–'}
        </span>
      </div>
      {cacheMessage && <p className="empty-hint">{cacheMessage}</p>}
      {lockMessage && (
        <p className="error">
          {lockMessage}
          <button type="button" className="retry-btn" onClick={() => setReloadNonce((n) => n + 1)}>
            立即重试
          </button>
          <button type="button" className="retry-btn" onClick={handleReleaseLockAndRetry}>
            释放锁并重试
          </button>
        </p>
      )}
      {displayData && rawRows.length > 0 && (
        <div className="group-f-filters">
          <label>
            store_id：
            <input
              type="text"
              placeholder="1,7,12 留空全部"
              value={storeIdFilter}
              onChange={(e) => setStoreIdFilter(e.target.value)}
              className="group-f-filter-input"
            />
          </label>
          <label>
            impression_count_asin：
            <select value={impressionFilter} onChange={(e) => setImpressionFilter(e.target.value as AsinFilter)}>
              <option value="all">全部</option>
              <option value="has">有值</option>
              <option value="empty">无值</option>
            </select>
          </label>
          <label>
            order_asin：
            <select value={orderFilter} onChange={(e) => setOrderFilter(e.target.value as AsinFilter)}>
              <option value="all">全部</option>
              <option value="has">有值</option>
              <option value="empty">无值</option>
            </select>
          </label>
          <label>
            sessions_asin：
            <select value={sessionsFilter} onChange={(e) => setSessionsFilter(e.target.value as AsinFilter)}>
              <option value="all">全部</option>
              <option value="has">有值</option>
              <option value="empty">无值</option>
            </select>
          </label>
          {(storeIdFilter || impressionFilter !== 'all' || orderFilter !== 'all' || sessionsFilter !== 'all') && (
            <button
              type="button"
              className="group-f-clear-filters"
              onClick={() => {
                setStoreIdFilter('')
                setImpressionFilter('all')
                setOrderFilter('all')
                setSessionsFilter('all')
              }}
            >
              清除筛选
            </button>
          )}
          <button
            type="button"
            className="group-f-download-csv"
            onClick={() => downloadGroupFCsv(rows)}
            disabled={rows.length === 0}
          >
            下载 CSV
          </button>
        </div>
      )}
      {error && <p className="error">{error}</p>}
      <div className="group-f-table-wrap">
        <table className="data-table">
          <thead>
            <tr>
              <th>variation_id</th>
              <th>Parent ASIN</th>
              <th>Created At</th>
              <th>store_id</th>
              <th>impression_count_asin</th>
              <th>order_asin</th>
              <th>sessions_asin</th>
            </tr>
          </thead>
          <tbody>
            {rows.length > 0 ? (
              pageRows.map((r: GroupFRow, i: number) => (
                <tr key={`${r.parent_asin ?? ''}-${r.store_id ?? ''}-${startIdx + i}`}>
                  <td>{r.variation_id ?? '–'}</td>
                  <td>{r.parent_asin ?? '–'}</td>
                  <td>{formatCreatedAt(r.created_at)}</td>
                  <td>{r.store_id ?? '–'}</td>
                  <td>{r.impression_count_asin ?? '–'}</td>
                  <td>{r.order_asin ?? '–'}</td>
                  <td>{r.sessions_asin ?? '–'}</td>
                </tr>
              ))
            ) : loading ? (
              <tr>
                <td colSpan={7} className="empty-hint">
                  加载中... Group F 查询可能需要 5–6 分钟，请耐心等待。
                </td>
              </tr>
            ) : waitingForLock && rawRows.length === 0 ? (
              <tr>
                <td colSpan={7} className="empty-hint">
                  {lockMessage ?? 'Group F 查询进行中，完成后将自动刷新结果。'}
                </td>
              </tr>
            ) : error ? (
              <tr>
                <td colSpan={7} className="error">{error}</td>
              </tr>
            ) : rows.length === 0 ? (
              <tr>
                <td colSpan={7} className="empty-hint">
                  {!displayData ? '正在加载...' : rawRows.length === 0 ? '暂无符合条件的数据。' : '暂无符合筛选条件的数据，请调整筛选条件。'}
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
        {displayData && rows.length > 0 && (
            <div className="group-f-pagination">
              <p className="empty-hint">
                共 {rows.length} 个父 ASIN
                {rows.length !== rawRows.length ? `（已筛选，原始 ${rawRows.length} 条）` : '（指定周全部）'}
                ，每页 {GROUP_F_PAGE_SIZE} 条
              </p>
              {totalPages > 1 && (
                <PaginationControls
                  currentPage={currentPage}
                  totalPages={totalPages}
                  onChangePage={(pageNo: number) => setPage(pageNo)}
                />
              )}
            </div>
          )}
      </div>
    </div>
  )
}
