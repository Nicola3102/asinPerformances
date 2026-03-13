import { useEffect, useMemo, useRef, useState } from 'react'
import { NavLink, Navigate, Outlet, Route, Routes } from 'react-router-dom'
import {
  listSummaryByWeek,
  listWeeks,
  getDetail,
  getTableStats,
  downloadWeekData,
  operateSummary,
  refreshQueryStatus,
  syncFromOnline,
  getGroupFData,
  type SummaryRow,
  type DetailResponse,
  type DetailChildRow,
  type SearchQueryRow,
  type GroupFResponse,
  type GroupFRow,
} from './api/client'
import './App.css'

function formatParentOrderTotal(v: string | number | null | undefined): string {
  if (v == null || v === '') return '–'
  const n = Number(v)
  if (Number.isNaN(n)) return '–'
  return String(Math.round(n))
}

function formatCreatedAt(v: string | null | undefined): string {
  if (v == null || v === '') return '–'
  try {
    const d = new Date(v)
    if (Number.isNaN(d.getTime())) return v
    return d.toLocaleString('zh-CN', { dateStyle: 'short', timeStyle: 'short' })
  } catch {
    return v
  }
}

function formatNum(v: number | null | undefined): string {
  if (v == null) return '–'
  return String(v)
}

function escapeCsvCell(val: string | number): string {
  const s = String(val)
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`
  return s
}

function groupFRowsToCsv(rows: GroupFRow[]): string {
  const header = ['Parent ASIN', 'Created At', 'store_id', 'impression_count_asin', 'order_asin', 'sessions_asin']
  const lines = [header.map(escapeCsvCell).join(',')]
  for (const r of rows) {
    const created = formatCreatedAt(r.created_at)
    lines.push(
      [
        r.parent_asin ?? '',
        created,
        r.store_id ?? '',
        r.impression_count_asin ?? '',
        r.order_asin ?? '',
        r.sessions_asin ?? '',
      ].map(escapeCsvCell).join(',')
    )
  }
  return lines.join('\r\n')
}

function downloadGroupFCsv(rows: GroupFRow[], filename?: string): void {
  const csv = groupFRowsToCsv(rows)
  const blob = new Blob(['\uFEFF' + csv], { type: 'text/csv;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename ?? `group-f-${new Date().toISOString().slice(0, 10)}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

function SearchQueryTable({
  rows,
  compact = false,
  showHeader = true,
  className = '',
}: {
  rows: SearchQueryRow[]
  compact?: boolean
  /** 为 false 时每个子 ASIN 内不显示表头，仅主表「Search query (volume, impression, ...)」列有表头 */
  showHeader?: boolean
  className?: string
}) {
  if (!rows.length) return <span className="text-muted">–</span>
  return (
    <table className={`search-query-table ${compact ? 'search-query-table--compact' : ''} ${className}`}>
      {showHeader && (
        <thead>
          <tr>
            <th>search_query</th>
            <th>volume</th>
            <th>impression</th>
            <th>total_impression</th>
            <th>click</th>
            <th>total_click</th>
            <th>purchase_count</th>
          </tr>
        </thead>
      )}
      <tbody>
        {rows.map((r, i) => (
          <tr key={i}>
            <td>{r.search_query ?? '–'}</td>
            <td>{formatNum(r.search_query_volume)}</td>
            <td>{formatNum(r.search_query_impression_count)}</td>
            <td>{formatNum(r.search_query_total_impression)}</td>
            <td>{formatNum(r.search_query_click_count)}</td>
            <td>{formatNum(r.search_query_total_click)}</td>
            <td>{formatNum(r.search_query_purchase_count)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function ZoomModal({
  title,
  children,
  onClose,
}: {
  title: string
  children: React.ReactNode
  onClose: () => void
}) {
  return (
    <div className="zoom-overlay" onClick={onClose}>
      <div className="zoom-modal" onClick={(e) => e.stopPropagation()}>
        <div className="zoom-modal-header">
          <h3>{title}</h3>
          <button type="button" className="modal-close" onClick={onClose}>×</button>
        </div>
        <div className="zoom-modal-body">{children}</div>
      </div>
    </div>
  )
}

const SEARCH_QUERY_PREVIEW_LIMIT = 9

function DetailModal({
  data,
  onClose,
}: {
  data: DetailResponse
  onClose: () => void
}) {
  const [zoomedChildIndex, setZoomedChildIndex] = useState<number | null>(null)
  const [expandedChildren, setExpandedChildren] = useState<Set<number>>(new Set())

  const toggleExpand = (i: number) => {
    setExpandedChildren((prev) => {
      const next = new Set(prev)
      if (next.has(i)) next.delete(i)
      else next.add(i)
      return next
    })
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h2>Details</h2>
          <button type="button" className="modal-close" onClick={onClose}>
            ×
          </button>
        </div>
        <div className="modal-body">
          <div className="detail-summary">
            <span className="detail-summary-item">
              <strong>Parent ASIN</strong>: {data.parent_asin ?? '–'}
            </span>
            <span className="detail-summary-item">
              <strong>Parent order total</strong>: {formatParentOrderTotal(data.parent_order_total)}
            </span>
            <span className="detail-summary-item">
              <strong>Week No</strong>: {data.week_no != null ? String(data.week_no) : '–'}
            </span>
          </div>
        <table className="detail-table">
          <thead>
            <tr>
              <th>Child ASIN</th>
              <th>Child Impression</th>
              <th>Child Session</th>
              <th>Search query (volume, impression, total_impression, click, total_click, purchase_count)</th>
            </tr>
          </thead>
          <tbody>
            {data.children.map((row: DetailChildRow, i: number) => {
              const allRows = row.search_queries ?? []
              const overLimit = allRows.length > SEARCH_QUERY_PREVIEW_LIMIT
              const expanded = expandedChildren.has(i)
              const displayRows = overLimit && !expanded
                ? allRows.slice(0, SEARCH_QUERY_PREVIEW_LIMIT)
                : allRows
              const hasOrder = (row.order_num ?? 0) > 0
              const asinDisplay = row.child_asin != null && row.child_asin !== '' ? row.child_asin : '–'
              return (
                <tr key={`${row.child_asin ?? ''}-${i}`}>
                  <td>{hasOrder ? <span className="child-asin-with-order">{asinDisplay}</span> : asinDisplay}</td>
                  <td>{row.child_impression_count != null ? String(row.child_impression_count) : '–'}</td>
                  <td>{row.child_session_count != null ? String(row.child_session_count) : '–'}</td>
                  <td className="cell-search-query-wrap">
                    <SearchQueryTable rows={displayRows} compact showHeader={false} />
                    {overLimit && (
                      <button
                        type="button"
                        className="load-all-btn"
                        onClick={() => toggleExpand(i)}
                      >
                        {expanded ? '收起' : `加载所有 (共 ${allRows.length} 条)`}
                      </button>
                    )}
                    <button
                      type="button"
                      className="zoom-btn"
                      onClick={() => setZoomedChildIndex(i)}
                    >
                      放大
                    </button>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
        </div>
      </div>
      {zoomedChildIndex !== null && data.children[zoomedChildIndex] && (
        <ZoomModal
          title={`Search query · Child ASIN: ${data.children[zoomedChildIndex].child_asin ?? '–'}`}
          onClose={() => setZoomedChildIndex(null)}
        >
          <SearchQueryTable rows={data.children[zoomedChildIndex].search_queries ?? []} />
        </ZoomModal>
      )}
    </div>
  )
}

function AsinHomePage() {
  const [summary, setSummary] = useState<SummaryRow[]>([])
  const [availableWeeks, setAvailableWeeks] = useState<number[]>([])
  const [selectedWeek, setSelectedWeek] = useState<number | ''>('')
  const [selectedParentAsins, setSelectedParentAsins] = useState<Set<string>>(new Set())
  const [tableCount, setTableCount] = useState<number | null>(null)
  const [loading, setLoading] = useState(true)
  const [syncing, setSyncing] = useState(false)
  const [downloading, setDownloading] = useState(false)
  const [operatingKey, setOperatingKey] = useState<string | null>(null)
  const [refreshingQueryStatus, setRefreshingQueryStatus] = useState(false)
  const queryRefreshInFlightRef = useRef(false)
  const [error, setError] = useState<string | null>(null)
  const [detail, setDetail] = useState<DetailResponse | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [lastSyncCheck, setLastSyncCheck] = useState<{
    rows_fetched_from_online: number
    rows_inserted: number
    local_table_count_after: number
    table_name: string
    insert_ok: boolean
    step2_error?: string | null
    message?: string | null
  } | null>(null)
  const selectedWeekStats = useMemo(() => {
    const totalOrders = summary.reduce((acc, row) => {
      const n = Number(row.parent_order_total)
      return Number.isNaN(n) ? acc : acc + n
    }, 0)
    return {
      week_no: selectedWeek,
      parent_asin_count: summary.length,
      total_orders: totalOrders,
    }
  }, [summary, selectedWeek])

  const loadSummary = async (weekOverride?: number | '') => {
    setLoading(true)
    setError(null)
    const timeoutMs = 15000
    let timeoutId: ReturnType<typeof setTimeout>
    const timeoutPromise = new Promise<never>((_, reject) => {
      timeoutId = setTimeout(() => reject(new Error('请求超时，请确认后端已启动（如 docker compose up）')), timeoutMs)
    })
    try {
      const [weeksData, stats] = await Promise.race([
        Promise.all([listWeeks(), getTableStats()]),
        timeoutPromise,
      ]) as [Awaited<ReturnType<typeof listWeeks>>, Awaited<ReturnType<typeof getTableStats>>]
      clearTimeout(timeoutId!)
      const fallbackWeek = weeksData.length > 0 ? weeksData[0] : ''
      const effectiveWeek = weekOverride !== undefined ? weekOverride : (selectedWeek !== '' ? selectedWeek : fallbackWeek)
      const summaryData = typeof effectiveWeek === 'number' ? await listSummaryByWeek(effectiveWeek) : []
      const asinSet = new Set(summaryData.map((r) => (r.parent_asin || '').trim()).filter((x) => x !== ''))
      setSelectedParentAsins((prev) => {
        const next = new Set<string>()
        for (const asin of prev) {
          if (asinSet.has(asin)) next.add(asin)
        }
        return next
      })
      setAvailableWeeks(weeksData)
      setSelectedWeek(effectiveWeek)
      setSummary(summaryData)
      setTableCount(stats.count)
    } catch (e) {
      clearTimeout(timeoutId!)
      setError(e instanceof Error ? e.message : 'Failed to load')
      setSummary([])
      setSelectedParentAsins(new Set())
      setAvailableWeeks([])
      setSelectedWeek('')
      setTableCount(null)
    } finally {
      setLoading(false)
    }
  }

  const handleWeekChange = async (v: string) => {
    const nextWeek = v ? Number(v) : ''
    setSelectedWeek(nextWeek)
    await loadSummary(nextWeek)
  }

  const handleDownload = async () => {
    if (typeof selectedWeek !== 'number') return
    setDownloading(true)
    try {
      const selected: string[] = Array.from(selectedParentAsins)
      await downloadWeekData(selectedWeek, selected.length > 0 ? selected : undefined)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Download failed')
    } finally {
      setDownloading(false)
    }
  }

  const toggleParentAsin = (asin: string | null) => {
    const key = (asin || '').trim()
    if (!key) return
    setSelectedParentAsins((prev) => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  const handleSelectAll = () => {
    const all = summary.map((r) => (r.parent_asin || '').trim()).filter((x) => x !== '')
    setSelectedParentAsins(new Set(all))
  }

  const handleClearAll = () => {
    setSelectedParentAsins(new Set())
  }

  const triggerRefreshQueryStatus = async (week: number) => {
    if (queryRefreshInFlightRef.current) return
    queryRefreshInFlightRef.current = true
    setRefreshingQueryStatus(true)
    try {
      const out = await refreshQueryStatus(week)
      if ((out.checked_groups ?? 0) > 0 || (out.completed_groups ?? 0) > 0) {
        const fresh = await listSummaryByWeek(week)
        setSummary(fresh)
      }
    } catch (e) {
      // 不打断主流程，状态刷新失败仅轻量提示
      setError((prev) => prev ?? (e instanceof Error ? e.message : '状态刷新失败'))
    } finally {
      setRefreshingQueryStatus(false)
      queryRefreshInFlightRef.current = false
    }
  }

  const handleOperate = async (parent_asin: string | null, week_no: number | null) => {
    if (parent_asin == null || week_no == null) return
    const key = `${parent_asin}-${week_no}`
    setOperatingKey(key)
    setError(null)
    try {
      const res = await operateSummary(parent_asin, week_no)
      if (!res.updated || res.updated <= 0) {
        setError(`未匹配到可更新记录（parent_asin=${parent_asin}, week_no=${week_no}）`)
      }
      await loadSummary(selectedWeek)
    } catch (e) {
      setError(e instanceof Error ? e.message : '操作失败')
    } finally {
      setOperatingKey(null)
    }
  }

  useEffect(() => {
    loadSummary()
  }, [])

  useEffect(() => {
    if (typeof selectedWeek !== 'number') return
    void triggerRefreshQueryStatus(selectedWeek)
    const timer = setInterval(() => {
      void triggerRefreshQueryStatus(selectedWeek)
    }, 120000)
    return () => clearInterval(timer)
  }, [selectedWeek])

  const handleSync = async () => {
    setSyncing(true)
    setError(null)
    setLastSyncCheck(null)
    try {
      const result = await syncFromOnline()
      await loadSummary()
      setError(null)
      if (result.check?.local_table_count_after != null) {
        setTableCount(result.check.local_table_count_after)
      } else if (result.rows_synced >= 0) {
        setTableCount(result.rows_synced)
      }
      if (result.check) {
        setLastSyncCheck(result.check)
      }
      if (result.message && result.rows_synced === 0) {
        setError(result.message)
      } else if (result.check && !result.check.insert_ok && result.rows_synced > 0 && !result.check.step2_error) {
        setError(result.message ?? '插入条数与查询条数不一致，请检查')
      }
      // step2_error 时仅通过 result.message 在 sync-check 区域说明，不置为 error，表格照常展示 Step 1 数据
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Sync failed')
    } finally {
      setSyncing(false)
    }
  }

  const handleViewMore = async (parent_asin: string | null, week_no: number | string | null, store_id?: number | null) => {
    if (parent_asin == null || week_no == null || week_no === '') return
    setDetailLoading(true)
    setDetail(null)
    try {
      const data = await getDetail(parent_asin, Number(week_no), store_id)
      setDetail(data)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load detail')
    } finally {
      setDetailLoading(false)
    }
  }

  const closeModal = () => setDetail(null)

  return (
    <div className="app">
      <h1>ASIN Performances</h1>
      {error && (
        <p className="error">
          {error}
          <button type="button" className="retry-btn" onClick={() => { setError(null); loadSummary() }}>
            重试加载
          </button>
        </p>
      )}

      <div className="toolbar">
        <button
          type="button"
          className="sync-btn"
          onClick={handleSync}
          disabled={syncing}
        >
          {syncing ? 'Syncing...' : 'Sync from online DB'}
        </button>
        <label className="week-filter">
          week_no:
          <select
            value={selectedWeek === '' ? '' : String(selectedWeek)}
            onChange={(e) => { void handleWeekChange(e.target.value) }}
            disabled={loading || syncing}
          >
            {availableWeeks.length === 0 ? (
              <option value="">暂无 week_no</option>
            ) : (
              availableWeeks.map((w) => (
                <option key={w} value={String(w)}>
                  {w}
                </option>
              ))
            )}
          </select>
        </label>
        <button
          type="button"
          className="download-btn"
          onClick={() => { void handleDownload() }}
          disabled={downloading || typeof selectedWeek !== 'number'}
        >
          {downloading ? 'Downloading...' : 'Download'}
        </button>
        <button
          type="button"
          className="select-btn"
          onClick={handleSelectAll}
          disabled={summary.length === 0}
        >
          全选
        </button>
        <button
          type="button"
          className="select-btn"
          onClick={handleClearAll}
          disabled={selectedParentAsins.size === 0}
        >
          取消全选
        </button>
        <span className="table-stats">已选父 ASIN {selectedParentAsins.size} 个</span>
        
        {typeof selectedWeekStats.week_no === 'number' && (
          <span className="week-stats">
            week_no: {formatNum(selectedWeekStats.week_no)} | 父 ASIN 共 {formatNum(selectedWeekStats.parent_asin_count)} 个 | 总订单 {formatParentOrderTotal(selectedWeekStats.total_orders)} 笔
          </span>
        )}
        {refreshingQueryStatus && (
          <span className="week-stats">checked_status 刷新中...</span>
        )}
      </div>
      {lastSyncCheck && (
        <div className="sync-check">
          <span>检查：从 online 查询 {lastSyncCheck.rows_fetched_from_online} 条 → 插入表 {lastSyncCheck.table_name} {lastSyncCheck.rows_inserted} 条 → 插入后表内 {lastSyncCheck.local_table_count_after} 条</span>
          {lastSyncCheck.insert_ok && !lastSyncCheck.step2_error ? (
            <span className="sync-ok"> ✓ 一致</span>
          ) : lastSyncCheck.step2_error ? (
            <span className="sync-warn"> Step 2 未完成，仅展示 Step 1 数据</span>
          ) : (
            <span className="sync-warn"> ⚠ 不一致</span>
          )}
          {lastSyncCheck.message && (
            <div className="sync-check-msg">{lastSyncCheck.message}</div>
          )}
        </div>
      )}

      {loading ? (
        <p>Loading...</p>
      ) : (
        <div className="main-table-wrap">
          {summary.length === 0 && !error && (
            <p className="empty-hint">表内暂无数据，请点击上方「Sync from online DB」拉取（需在 backend/.env 中配置 online_db_host、online_db_user 等）。</p>
          )}
          <table>
            <thead>
              <tr>
                <th></th>
                <th>Parent ASIN</th>
                <th>Parent ASIN Create At</th>
                <th>Parent Order Total</th>
                <th>store_id</th>
                <th>operation_status</th>
                <th>checked_status</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {summary.map((row, i) => {
                const opDone = row.operation_status === true || (row.operated_at != null && row.operated_at !== '')
                const opKey = `${row.parent_asin}-${row.week_no}`
                const isOperating = operatingKey === opKey
                return (
                  <tr key={`${row.parent_asin}-${row.week_no}-${row.store_id ?? ''}-${i}`}>
                    <td>
                      <input
                        type="checkbox"
                        checked={selectedParentAsins.has((row.parent_asin || '').trim())}
                        onChange={() => toggleParentAsin(row.parent_asin)}
                      />
                    </td>
                    <td>{row.parent_asin ?? '-'}</td>
                    <td>{row.parent_asin_create_at != null ? String(row.parent_asin_create_at).slice(0, 19) : '–'}</td>
                    <td>{formatParentOrderTotal(row.parent_order_total)}</td>
                    <td>{formatNum(row.store_id)}</td>
                    <td>
                      <button
                        type="button"
                        className={opDone ? 'operate-btn operate-btn--done' : 'operate-btn'}
                        onClick={() => { void handleOperate(row.parent_asin, row.week_no) }}
                        disabled={isOperating}
                        title={opDone ? (row.operated_at ? `已操作于 ${String(row.operated_at).slice(0, 19)}` : '已操作') : '点击将该父 ASIN 在本周下的记录标记为已操作'}
                      >
                        {isOperating ? '处理中...' : (opDone ? '已操作' : '操作')}
                      </button>
                    </td>
                    <td>
                      <span className={row.checked_status === 'completed' ? 'query-status query-status--completed' : 'query-status query-status--pending'}>
                        {row.checked_status === 'completed' ? 'completed' : 'pending'}
                      </span>
                    </td>
                    <td>
                      <button
                        type="button"
                        className="view-more-btn"
                        onClick={() => handleViewMore(row.parent_asin, row.week_no, row.store_id)}
                      >
                        View more
                      </button>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

      {detailLoading && <p className="loading-detail">Loading detail...</p>}
      {detail && <DetailModal data={detail} onClose={closeModal} />}
    </div>
  )
}

const GROUP_F_PAGE_SIZE = 30

type AsinFilter = 'all' | 'has' | 'empty'

function GroupFPage() {
  const [scanWeeks, setScanWeeks] = useState(2)
  const [specificWeeks, setSpecificWeeks] = useState('')
  const [submittedSpecificWeeks, setSubmittedSpecificWeeks] = useState('')
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [data, setData] = useState<GroupFResponse | null>(null)
  const hasDataRef = useRef(false)

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
    getGroupFData(scanWeeks, weekNos ?? undefined, ctrl.signal)
      .then((res) => {
        hasDataRef.current = true
        setData(res)
        setError(null)
      })
      .catch((e) => {
        if (e?.name !== 'AbortError' && !hasDataRef.current) {
          setError(e instanceof Error ? e.message : 'Failed to load')
        }
      })
      .finally(() => setLoading(false))
    return () => ctrl.abort()
  }, [scanWeeks, weekNos === null ? null : JSON.stringify(weekNos)])

  const handleSpecificWeeksKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault()
      setSubmittedSpecificWeeks(specificWeeks.trim())
    }
  }

  const rawRows = data?.rows ?? []
  const filteredRows = useMemo(() => {
    let filtered = rawRows
    if (storeIdFilter.trim()) {
      const ids = new Set(
        storeIdFilter.split(/[,，\s]+/).map((s) => parseInt(s.trim(), 10)).filter((n) => !Number.isNaN(n))
      )
      if (ids.size > 0) {
        filtered = filtered.filter((r) => r.store_id != null && ids.has(r.store_id))
      }
    }
    const hasValue = (v: string | null | undefined) => v != null && String(v).trim() !== ''
    if (impressionFilter === 'has') filtered = filtered.filter((r) => hasValue(r.impression_count_asin))
    else if (impressionFilter === 'empty') filtered = filtered.filter((r) => !hasValue(r.impression_count_asin))
    if (orderFilter === 'has') filtered = filtered.filter((r) => hasValue(r.order_asin))
    else if (orderFilter === 'empty') filtered = filtered.filter((r) => !hasValue(r.order_asin))
    if (sessionsFilter === 'has') filtered = filtered.filter((r) => hasValue(r.sessions_asin))
    else if (sessionsFilter === 'empty') filtered = filtered.filter((r) => !hasValue(r.sessions_asin))
    return filtered
  }, [rawRows, storeIdFilter, impressionFilter, orderFilter, sessionsFilter])

  const rows = filteredRows
  const totalPages = Math.max(1, Math.ceil(rows.length / GROUP_F_PAGE_SIZE))
  const currentPage = Math.min(Math.max(1, page), totalPages)
  const startIdx = (currentPage - 1) * GROUP_F_PAGE_SIZE
  const pageRows = rows.slice(startIdx, startIdx + GROUP_F_PAGE_SIZE)

  return (
    <div className="app">
      <h1>Group F</h1>
      <div className="group-f-controls">
        <label>
          指定周：
          <input
            type="text"
            placeholder="如 202607 或 202607,202606，回车查询"
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
          活动周数：{loading ? '计算中...' : data?.weeks?.length ? data.weeks.join(', ') : '–'}
        </span>
      </div>
      {!loading && !error && data && rawRows.length > 0 && (
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
              <th>Parent ASIN</th>
              <th>Created At</th>
              <th>store_id</th>
              <th>impression_count_asin</th>
              <th>order_asin</th>
              <th>sessions_asin</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={6} className="empty-hint">
                  加载中... Group F 查询可能需要 5–6 分钟，请耐心等待。
                </td>
              </tr>
            ) : error ? (
              <tr>
                <td colSpan={6} className="error">{error}</td>
              </tr>
            ) : rows.length === 0 ? (
              <tr>
                <td colSpan={6} className="empty-hint">
                  {!data ? '正在加载...' : rawRows.length === 0 ? '暂无符合条件的数据。' : '暂无符合筛选条件的数据，请调整筛选条件。'}
                </td>
              </tr>
            ) : (
              pageRows.map((r: GroupFRow, i: number) => (
                <tr key={`${r.parent_asin ?? ''}-${r.store_id ?? ''}-${startIdx + i}`}>
                  <td>{r.parent_asin ?? '–'}</td>
                  <td>{formatCreatedAt(r.created_at)}</td>
                  <td>{r.store_id ?? '–'}</td>
                  <td>{r.impression_count_asin ?? '–'}</td>
                  <td>{r.order_asin ?? '–'}</td>
                  <td>{r.sessions_asin ?? '–'}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
        {!loading && !error && data && rows.length > 0 && (
            <div className="group-f-pagination">
              <p className="empty-hint">
                共 {rows.length} 个父 ASIN
                {rows.length !== rawRows.length ? `（已筛选，原始 ${rawRows.length} 条）` : '（指定周全部）'}
                ，每页 {GROUP_F_PAGE_SIZE} 条
              </p>
              {totalPages > 1 && (
                <div className="pagination-btns">
                  <button
                    type="button"
                    disabled={currentPage <= 1}
                    onClick={() => setPage((p) => Math.max(1, p - 1))}
                  >
                    上一页
                  </button>
                  <span>第 {currentPage} / {totalPages} 页</span>
                  <button
                    type="button"
                    disabled={currentPage >= totalPages}
                    onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                  >
                    下一页
                  </button>
                </div>
              )}
            </div>
          )}
      </div>
    </div>
  )
}

function PagePlaceholder({ title }: { title: string }) {
  return (
    <div className="app">
      <h1>{title}</h1>
      <p className="empty-hint">This page is ready.</p>
    </div>
  )
}

function AppLayout() {
  const [groupOpen, setGroupOpen] = useState(false)
  const groupRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    const onDocMouseDown = (e: MouseEvent) => {
      if (!groupRef.current) return
      if (!groupRef.current.contains(e.target as Node)) {
        setGroupOpen(false)
      }
    }
    document.addEventListener('mousedown', onDocMouseDown)
    return () => document.removeEventListener('mousedown', onDocMouseDown)
  }, [])

  return (
    <div className="app-shell">
      <nav className="top-nav">
        <div className="top-nav-group" ref={groupRef}>
          <button
            type="button"
            className={`top-nav-link top-nav-group-toggle ${groupOpen ? 'is-active' : ''}`}
            onClick={() => setGroupOpen((v) => !v)}
          >
            Group
          </button>
          <div className={`top-nav-menu ${groupOpen ? 'is-open' : ''}`}>
            <NavLink to="/" className="top-nav-menu-link" onClick={() => setGroupOpen(false)}>S</NavLink>
            <NavLink to="/group/A" className="top-nav-menu-link" onClick={() => setGroupOpen(false)}>A</NavLink>
            <NavLink to="/group/B" className="top-nav-menu-link" onClick={() => setGroupOpen(false)}>B</NavLink>
            <NavLink to="/group/F" className="top-nav-menu-link" onClick={() => setGroupOpen(false)}>F</NavLink>
          </div>
        </div>
        <NavLink to="/tasks" className={({ isActive }) => `top-nav-link ${isActive ? 'is-active' : ''}`}>
          Tasks
        </NavLink>
      </nav>
      <div className="app-shell-content">
        <Outlet />
      </div>
    </div>
  )
}

export default function App() {
  return (
    <Routes>
      <Route element={<AppLayout />}>
        <Route path="/" element={<AsinHomePage />} />
        <Route path="/group/A" element={<PagePlaceholder title="Group A" />} />
        <Route path="/group/B" element={<PagePlaceholder title="Group B" />} />
        <Route path="/group/F" element={<GroupFPage />} />
        <Route path="/grpup/A" element={<Navigate to="/group/A" replace />} />
        <Route path="/tasks" element={<PagePlaceholder title="Tasks" />} />
      </Route>
    </Routes>
  )
}
