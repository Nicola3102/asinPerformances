import { useEffect, useMemo, useRef, useState } from 'react'
import { NavLink, Navigate, Outlet, Route, Routes } from 'react-router-dom'
import {
  listSummaryConsolidatedByWeek,
  listWeeks,
  getDetail,
  getTableStats,
  downloadWeekData,
  operateSummary,
  refreshQueryStatus,
  syncFromOnline,
  getGroupFData,
  getGroupFLockStatus,
  releaseGroupFLock,
  listGroupAWeeks,
  getGroupASummary,
  getGroupADetail,
  operateGroupA,
  downloadGroupAData,
  getMonitorParents,
  getMonitorTrack,
  type SummaryRowConsolidated,
  type DetailResponse,
  type DetailChildRow,
  type SearchQueryRow,
  type GroupFResponse,
  type GroupFRow,
  type GroupFLockStatus,
  type GroupASummaryResponse,
  type GroupASummaryRow,
  type GroupADetailResponse,
  type GroupADetailChildRow,
  type MonitorParentItem,
  type MonitorTrackResponse,
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
  const header = ['variation_id', 'Parent ASIN', 'Created At', 'store_id', 'impression_count_asin', 'order_asin', 'sessions_asin']
  const lines = [header.map(escapeCsvCell).join(',')]
  for (const r of rows) {
    const created = formatCreatedAt(r.created_at)
    lines.push(
      [
        r.variation_id ?? '',
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

const GROUP_F_CACHE_STORAGE_KEY = 'group-f-cache-v1'

type GroupFCacheEntry = {
  savedAt: string
  data: GroupFResponse
}

function loadGroupFCache(): Record<string, GroupFCacheEntry> {
  if (typeof window === 'undefined') return {}
  try {
    const raw = window.localStorage.getItem(GROUP_F_CACHE_STORAGE_KEY)
    if (!raw) return {}
    const parsed = JSON.parse(raw)
    return parsed && typeof parsed === 'object' ? parsed as Record<string, GroupFCacheEntry> : {}
  } catch {
    return {}
  }
}

function saveGroupFCache(cache: Record<string, GroupFCacheEntry>): void {
  if (typeof window === 'undefined') return
  try {
    window.localStorage.setItem(GROUP_F_CACHE_STORAGE_KEY, JSON.stringify(cache))
  } catch {
    /* ignore storage failures */
  }
}

function formatCacheTime(v: string | null | undefined): string {
  if (!v) return ''
  try {
    const d = new Date(v)
    if (Number.isNaN(d.getTime())) return v
    return d.toLocaleString('zh-CN', { dateStyle: 'short', timeStyle: 'medium', hour12: false })
  } catch {
    return v
  }
}

function SearchQueryTable({
  rows,
  compact = false,
  showHeader = true,
  className = '',
  tailMetric = 'purchase',
}: {
  rows: SearchQueryRow[]
  compact?: boolean
  /** 为 false 时每个子 ASIN 内不显示表头，仅主表「Search query (volume, impression, ...)」列有表头 */
  showHeader?: boolean
  className?: string
  tailMetric?: 'purchase' | 'cart'
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
            <th>{tailMetric === 'cart' ? 'cart_count' : 'purchase_count'}</th>
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
            <td>{formatNum(tailMetric === 'cart' ? r.search_query_cart_count : r.search_query_purchase_count)}</td>
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

function PaginationControls({
  currentPage,
  totalPages,
  onChangePage,
}: {
  currentPage: number
  totalPages: number
  onChangePage: (page: number) => void | Promise<void>
}) {
  const [pageInput, setPageInput] = useState(String(currentPage))

  useEffect(() => {
    setPageInput(String(currentPage))
  }, [currentPage])

  const jumpToPage = () => {
    const next = Number(pageInput)
    if (Number.isNaN(next)) return
    const target = Math.min(Math.max(1, Math.trunc(next)), totalPages)
    void onChangePage(target)
  }

  return (
    <div className="pagination-btns">
      <button
        type="button"
        disabled={currentPage <= 1}
        onClick={() => { void onChangePage(Math.max(1, currentPage - 1)) }}
      >
        上一页
      </button>
      <span>第 {currentPage} / {totalPages} 页</span>
      <button
        type="button"
        disabled={currentPage >= totalPages}
        onClick={() => { void onChangePage(Math.min(totalPages, currentPage + 1)) }}
      >
        下一页
      </button>
      <label className="pagination-jump">
        <span>跳转到</span>
        <input
          type="number"
          min={1}
          max={totalPages}
          value={pageInput}
          onChange={(e) => setPageInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') jumpToPage()
          }}
        />
        <span>页</span>
      </label>
      <button type="button" onClick={jumpToPage}>
        确定
      </button>
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
  const [summary, setSummary] = useState<SummaryRowConsolidated[]>([])
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
      const summaryData = typeof effectiveWeek === 'number' ? await listSummaryConsolidatedByWeek(effectiveWeek) : []
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
        const fresh = await listSummaryConsolidatedByWeek(week)
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
        <span className="table-stats">
          已选父 ASIN {selectedParentAsins.size} 个
          {tableCount != null ? ` | 表内总行数 ${tableCount}` : ''}
        </span>
        
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
                const storeIdsStr = (row.store_ids ?? []).length > 0 ? (row.store_ids as number[]).join(', ') : '–'
                return (
                  <tr key={`${row.parent_asin}-${row.week_no}-${i}`}>
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
                    <td>{storeIdsStr}</td>
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
                        onClick={() => handleViewMore(row.parent_asin, row.week_no, undefined)}
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
const GROUP_A_PAGE_SIZE = 30
const MONITOR_VOLUME_SPIKE_THRESHOLD = 200

type AsinFilter = 'all' | 'has' | 'empty'

/** 按 child_asin 分组，每组内按 search_query 建表：行=search_query，列=week_no，单元格=volume/impression/click */
function buildChildTables(track: MonitorTrackResponse): Map<string, {
  queries: string[];
  cell: Map<string, { v: number | null; i: number | null; c: number | null }>;
  weekSummary: Map<number, { queryCount: number; volumeTotal: number; impressionTotal: number; clickTotal: number }>;
}> {
  const byChild = new Map<string, Map<string, Map<number, { v: number | null; i: number | null; c: number | null }>>>()
  for (const r of track.rows) {
    const c = r.child_asin ?? ''
    const q = r.search_query ?? ''
    const w = r.week_no ?? 0
    if (!byChild.has(c)) byChild.set(c, new Map())
    const byQuery = byChild.get(c)!
    if (!byQuery.has(q)) byQuery.set(q, new Map())
    const byWeek = byQuery.get(q)!
    byWeek.set(w, {
      v: r.search_query_volume ?? null,
      i: r.search_query_impression_count ?? null,
      c: r.search_query_click_count ?? null,
    })
  }
  const out = new Map<string, {
    queries: string[];
    cell: Map<string, { v: number | null; i: number | null; c: number | null }>;
    weekSummary: Map<number, { queryCount: number; volumeTotal: number; impressionTotal: number; clickTotal: number }>;
  }>()
  for (const [child, byQuery] of byChild) {
    const queries = Array.from(byQuery.entries())
      .filter(([q, byWeek]) => {
        if (q.trim() !== '') return true
        return Array.from(byWeek.values()).some((vals) => vals.v != null || vals.i != null || vals.c != null)
      })
      .map(([q]) => q)
      .sort()
    if (queries.length === 0) continue
    const cell = new Map<string, { v: number | null; i: number | null; c: number | null }>()
    const weekSummary = new Map<number, { queryCount: number; volumeTotal: number; impressionTotal: number; clickTotal: number }>()
    for (const q of queries) {
      const byWeek = byQuery.get(q)
      if (!byWeek) continue
      for (const [week, vals] of byWeek) {
        cell.set(`${q}\t${week}`, vals)
        const summary = weekSummary.get(week) ?? {
          queryCount: 0,
          volumeTotal: 0,
          impressionTotal: 0,
          clickTotal: 0,
        }
        summary.queryCount += 1
        summary.volumeTotal += vals.v ?? 0
        summary.impressionTotal += vals.i ?? 0
        summary.clickTotal += vals.c ?? 0
        weekSummary.set(week, summary)
      }
    }
    out.set(child, { queries, cell, weekSummary })
  }
  return out
}

function MonitorPage() {
  const [parents, setParents] = useState<MonitorParentItem[]>([])
  const [selectedParent, setSelectedParent] = useState('')
  const [parentSearch, setParentSearch] = useState('')
  const [track, setTrack] = useState<MonitorTrackResponse | null>(null)
  const [loadingParents, setLoadingParents] = useState(true)
  const [loadingTrack, setLoadingTrack] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    setLoadingParents(true)
    setError(null)
    getMonitorParents()
      .then((list) => {
        setParents(list)
        if (list.length > 0 && !selectedParent) setSelectedParent(list[0].parent_asin ?? '')
      })
      .catch((e) => setError(e instanceof Error ? e.message : 'Failed to load parents'))
      .finally(() => setLoadingParents(false))
  }, [])

  useEffect(() => {
    if (!selectedParent.trim()) {
      setTrack(null)
      return
    }
    setLoadingTrack(true)
    setError(null)
    getMonitorTrack(selectedParent)
      .then(setTrack)
      .catch((e) => setError(e instanceof Error ? e.message : 'Failed to load track'))
      .finally(() => setLoadingTrack(false))
  }, [selectedParent])

  const filteredParents = useMemo(() => {
    const keyword = parentSearch.trim().toUpperCase()
    if (!keyword) return parents
    return parents.filter((p) => (p.parent_asin ?? '').toUpperCase().includes(keyword))
  }, [parents, parentSearch])

  useEffect(() => {
    if (!filteredParents.length) return
    const matched = filteredParents.some((p) => (p.parent_asin ?? '') === selectedParent)
    if (!matched) setSelectedParent(filteredParents[0].parent_asin ?? '')
  }, [filteredParents, selectedParent])

  const childTables = track ? buildChildTables(track) : new Map()
  const weeks = track?.weeks ?? []
  const selectedParentMeta = parents.find((p) => (p.parent_asin ?? '') === selectedParent) ?? null
  const selectedParentVisible = filteredParents.some((p) => (p.parent_asin ?? '') === selectedParent)

  return (
    <div className="app">
      <h1>Monitor</h1>
      <p className="monitor-desc">追踪 operation_status=1 的父 ASIN 下各子 ASIN 的 search_query 按周数据（volume / impression / click）。</p>
      {loadingParents && <p className="loading-hint">加载父 ASIN 列表...</p>}
      {error && <p className="error">{error}</p>}
      {!loadingParents && parents.length === 0 && <p className="empty-hint">暂无已操作（operation_status=1）的父 ASIN。</p>}
      {!loadingParents && parents.length > 0 && (
        <div className="monitor-controls">
          <label>
            搜索父 ASIN：
            <input
              type="text"
              value={parentSearch}
              onChange={(e) => setParentSearch(e.target.value)}
              placeholder="输入父 ASIN 关键字"
              className="monitor-select"
            />
          </label>
          <label>
            父 ASIN：
            <select
              value={selectedParent}
              onChange={(e) => setSelectedParent(e.target.value)}
              disabled={loadingTrack}
              className="monitor-select"
            >
              {filteredParents.map((p) => (
                <option key={p.parent_asin ?? ''} value={p.parent_asin ?? ''}>{p.parent_asin ?? '–'}</option>
              ))}
            </select>
          </label>
          {selectedParentMeta && (
            <span className="monitor-operated-at">
              最早 operated_at：{formatCreatedAt(selectedParentMeta.operated_at)}
            </span>
          )}
        </div>
      )}
      {!loadingParents && parents.length > 0 && filteredParents.length === 0 && (
        <p className="empty-hint">未匹配到父 ASIN，请调整搜索关键字。</p>
      )}
      {loadingTrack && selectedParentVisible && <p className="loading-hint">加载追踪数据...</p>}
      {!loadingTrack && track && selectedParentVisible && childTables.size === 0 && <p className="empty-hint">该父 ASIN 暂无子 ASIN 或 search_query 数据。</p>}
      {!loadingTrack && track && selectedParentVisible && childTables.size > 0 && (
        <div className="monitor-tables">
          {Array.from(childTables.entries()).map(([childAsin, { queries, cell, weekSummary }]) => (
            <div key={childAsin} className="monitor-child-block">
              <h3>子 ASIN: {childAsin}</h3>
              <div className="monitor-table-wrap">
                <table className="data-table monitor-track-table">
                  <thead>
                    <tr>
                      <th rowSpan={2} className="monitor-col-query">search_query</th>
                      {weeks.map((w) => {
                        const summary = weekSummary.get(w)
                        return (
                          <th key={w} colSpan={3} className="monitor-week-col">
                            {`week ${w}(${summary?.queryCount ?? 0})`}
                          </th>
                        )
                      })}
                    </tr>
                    <tr>
                      {weeks.flatMap((w) => {
                        const summary = weekSummary.get(w)
                        return [
                          <th key={`${w}-v`}>{`volume(${summary?.volumeTotal ?? 0})`}</th>,
                          <th key={`${w}-i`}>{`impression(${summary?.impressionTotal ?? 0})`}</th>,
                          <th key={`${w}-c`}>{`click(${summary?.clickTotal ?? 0})`}</th>,
                        ]
                      })}
                    </tr>
                  </thead>
                  <tbody>
                    {queries.map((q: string) => (
                      <tr key={q}>
                        <td className="monitor-query-cell">{q || '–'}</td>
                        {weeks.flatMap((w, idx) => {
                          const val = cell.get(`${q}\t${w}`) ?? { v: null, i: null, c: null }
                          const prevWeek = idx > 0 ? weeks[idx - 1] : null
                          const prevVal = prevWeek != null ? (cell.get(`${q}\t${prevWeek}`) ?? { v: null, i: null, c: null }) : null
                          const highlightVolume =
                            idx > 0 &&
                            val.v != null &&
                            val.v >= MONITOR_VOLUME_SPIKE_THRESHOLD &&
                            (prevVal == null || prevVal.v == null || prevVal.v <= 0)
                          return [
                            <td key={`${q}-${w}-v`} className={highlightVolume ? 'monitor-volume-spike' : undefined}>
                              {formatNum(val.v)}
                            </td>,
                            <td key={`${q}-${w}-i`}>{formatNum(val.i)}</td>,
                            <td key={`${q}-${w}-c`}>{formatNum(val.c)}</td>,
                          ]
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

function GroupFPage() {
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

  const requestKey = weekNos == null ? `scan:${scanWeeks}` : `weeks:${JSON.stringify(weekNos)}`
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
  }, [scanWeeks, weekNos === null ? null : JSON.stringify(weekNos), reloadNonce, requestKey])

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

  const rawRows = displayData?.rows ?? []
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
                  onChangePage={(pageNo) => setPage(pageNo)}
                />
              )}
            </div>
          )}
      </div>
    </div>
  )
}

function GroupADetailModal({
  data,
  onClose,
}: {
  data: GroupADetailResponse
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
          <h2>Group A Details</h2>
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
              <strong>store_id</strong>: {data.store_id ?? '–'}
            </span>
            <span className="detail-summary-item">
              <strong>Week No</strong>: {data.week_no ?? '–'}
            </span>
            <span className="detail-summary-item">
              <strong>Total impression</strong>: {formatNum(data.total_impression_count)}
            </span>
            <span className="detail-summary-item">
              <strong>Total cart</strong>: {formatNum(data.total_cart_count)}
            </span>
            <span className="detail-summary-item">
              <strong>Total session</strong>: {formatNum(data.total_session_count)}
            </span>
          </div>
          <table className="detail-table">
            <thead>
              <tr>
                <th>Child ASIN</th>
                <th>Child Impression</th>
                <th>Child Session</th>
                <th>Search query (volume, impression, total_impression, click, total_click, cart_count)</th>
              </tr>
            </thead>
            <tbody>
              {data.children.map((row: GroupADetailChildRow, i: number) => {
                const allRows = row.search_queries ?? []
                const overLimit = allRows.length > SEARCH_QUERY_PREVIEW_LIMIT
                const expanded = expandedChildren.has(i)
                const displayRows = overLimit && !expanded
                  ? allRows.slice(0, SEARCH_QUERY_PREVIEW_LIMIT)
                  : allRows
                const asinDisplay = row.child_asin != null && row.child_asin !== '' ? row.child_asin : '–'
                return (
                  <tr key={`${row.child_asin ?? ''}-${i}`}>
                    <td>{asinDisplay}</td>
                    <td>{row.child_impression_count != null ? String(row.child_impression_count) : '–'}</td>
                    <td>{row.child_session_count != null ? String(row.child_session_count) : '–'}</td>
                    <td className="cell-search-query-wrap">
                      <SearchQueryTable rows={displayRows} compact showHeader={false} tailMetric="cart" />
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
          <SearchQueryTable rows={data.children[zoomedChildIndex].search_queries ?? []} tailMetric="cart" />
        </ZoomModal>
      )}
    </div>
  )
}

function GroupAPage() {
  const [weeks, setWeeks] = useState<number[]>([])
  const [selectedWeek, setSelectedWeek] = useState<number | ''>('')
  const [data, setData] = useState<GroupASummaryResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [detail, setDetail] = useState<GroupADetailResponse | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [operatingKey, setOperatingKey] = useState<string | null>(null)
  const [downloading, setDownloading] = useState(false)
  const [selectAllMode, setSelectAllMode] = useState(false)
  const [selectedRowKeys, setSelectedRowKeys] = useState<Set<string>>(new Set())

  const loadGroupA = async (opts?: { weekNo?: number | ''; page?: number }) => {
    setLoading(true)
    setError(null)
    try {
      const requestedWeek = opts?.weekNo !== undefined ? opts.weekNo : selectedWeek
      const requestedPage = opts?.page ?? data?.page ?? 1
      const [weekData, summary] = await Promise.all([
        listGroupAWeeks(),
        getGroupASummary(typeof requestedWeek === 'number' ? requestedWeek : null, requestedPage, GROUP_A_PAGE_SIZE),
      ])
      setWeeks(weekData)
      setData(summary)
      setSelectedWeek(summary.week_no ?? '')
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load Group A data')
      setWeeks([])
      setData(null)
      setSelectedWeek('')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void loadGroupA({ weekNo: '', page: 1 })
  }, [])

  const handleWeekChange = async (v: string) => {
    const nextWeek = v ? Number(v) : ''
    setSelectedWeek(nextWeek)
    setSelectAllMode(false)
    setSelectedRowKeys(new Set())
    await loadGroupA({ weekNo: nextWeek, page: 1 })
  }

  const handlePageChange = async (nextPage: number) => {
    if (!data) return
    await loadGroupA({ weekNo: selectedWeek, page: nextPage })
  }

  const handleViewMore = async (row: GroupASummaryRow) => {
    if (!row.parent_asin || row.week_no == null || row.store_id == null) return
    setDetailLoading(true)
    setDetail(null)
    try {
      const out = await getGroupADetail(row.parent_asin, row.week_no, row.store_id)
      setDetail(out)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load Group A detail')
    } finally {
      setDetailLoading(false)
    }
  }

  const handleOperate = async (row: GroupASummaryRow) => {
    if (!row.parent_asin || row.week_no == null || row.store_id == null) return
    const key = `${row.parent_asin}-${row.store_id}-${row.week_no}`
    setOperatingKey(key)
    setError(null)
    try {
      const out = await operateGroupA(row.parent_asin, row.store_id, row.week_no)
      if (!out.updated || out.updated <= 0) {
        setError(`未匹配到可更新记录（parent_asin=${row.parent_asin}, store_id=${row.store_id}, week_no=${row.week_no}）`)
      }
      await loadGroupA({ weekNo: selectedWeek, page: currentPage })
    } catch (e) {
      setError(e instanceof Error ? e.message : '操作失败')
    } finally {
      setOperatingKey(null)
    }
  }

  const rowKey = (row: GroupASummaryRow) => `${row.parent_asin ?? ''}||${row.store_id ?? ''}`

  const toggleRowSelection = (row: GroupASummaryRow) => {
    const key = rowKey(row)
    if (!key || key === '||') return
    setSelectAllMode(false)
    setSelectedRowKeys((prev) => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  const handleSelectAll = () => {
    setSelectAllMode(true)
    setSelectedRowKeys(new Set())
  }

  const handleClearAll = () => {
    setSelectAllMode(false)
    setSelectedRowKeys(new Set())
  }

  const handleDownload = async () => {
    const weekNo = data?.week_no
    if (weekNo == null) return
    setDownloading(true)
    setError(null)
    try {
      const selected = Array.from(selectedRowKeys)
      await downloadGroupAData(weekNo, selectAllMode ? undefined : (selected.length > 0 ? selected : undefined))
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Download failed')
    } finally {
      setDownloading(false)
    }
  }

  const currentPage = data?.page ?? 1
  const totalPages = data?.total_pages ?? 0
  const rows = data?.rows ?? []

  return (
    <div className="app">
      <h1>Group A</h1>
      {error && <p className="error">{error}</p>}
      <div className="group-a-controls">
        <label className="week-filter">
          week_no:
          <select
            value={selectedWeek === '' ? '' : String(selectedWeek)}
            onChange={(e) => { void handleWeekChange(e.target.value) }}
            disabled={loading}
          >
            {weeks.length === 0 ? (
              <option value="">暂无 week_no</option>
            ) : (
              weeks.map((w) => (
                <option key={w} value={String(w)}>
                  {w}
                </option>
              ))
            )}
          </select>
        </label>
        {!loading && data && (
          <span className="table-stats">
            共 {data.total} 条，默认每页 {GROUP_A_PAGE_SIZE} 条
            {selectAllMode ? ' | 已全选当前 week_no' : ` | 已选 ${selectedRowKeys.size} 条`}
          </span>
        )}
        <button
          type="button"
          className="select-btn"
          onClick={handleSelectAll}
          disabled={loading || !data || data.total === 0}
        >
          全选
        </button>
        <button
          type="button"
          className="select-btn"
          onClick={handleClearAll}
          disabled={!selectAllMode && selectedRowKeys.size === 0}
        >
          取消全选
        </button>
        <button
          type="button"
          className="download-btn"
          onClick={() => { void handleDownload() }}
          disabled={downloading || !data || data.total === 0}
        >
          {downloading ? 'Downloading...' : 'Download'}
        </button>
      </div>
      <div className="group-f-table-wrap">
        <table className="data-table">
          <thead>
            <tr>
              <th></th>
              <th>Parent ASIN</th>
              <th>store_id</th>
              <th>created_at</th>
              <th>total_impression_count</th>
              <th>total_cart_count</th>
              <th>total_session_count</th>
              <th>operation_status</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={9} className="empty-hint">加载中...</td>
              </tr>
            ) : error ? (
              <tr>
                <td colSpan={9} className="error">{error}</td>
              </tr>
            ) : rows.length === 0 ? (
              <tr>
                <td colSpan={9} className="empty-hint">暂无符合条件的数据。</td>
              </tr>
            ) : (
              rows.map((r: GroupASummaryRow, i: number) => (
                <tr key={`${r.parent_asin ?? ''}-${r.store_id ?? ''}-${r.week_no ?? ''}-${i}`}>
                  <td>
                    <input
                      type="checkbox"
                      checked={selectAllMode || selectedRowKeys.has(rowKey(r))}
                      onChange={() => toggleRowSelection(r)}
                    />
                  </td>
                  <td>{r.parent_asin ?? '–'}</td>
                  <td>{r.store_id ?? '–'}</td>
                  <td>{formatCreatedAt(r.created_at)}</td>
                  <td>{formatNum(r.total_impression_count)}</td>
                  <td>{formatNum(r.total_cart_count)}</td>
                  <td>{formatNum(r.total_session_count)}</td>
                  <td>
                    <button
                      type="button"
                      className={r.operation_status ? 'operate-btn operate-btn--done' : 'operate-btn'}
                      onClick={() => { void handleOperate(r) }}
                      disabled={operatingKey === `${r.parent_asin}-${r.store_id}-${r.week_no}`}
                      title={r.operation_status ? (r.operated_at ? `已操作于 ${formatCreatedAt(r.operated_at)}` : '已操作') : '点击标记为已操作（UTC+8）'}
                    >
                      {operatingKey === `${r.parent_asin}-${r.store_id}-${r.week_no}`
                        ? '处理中...'
                        : (r.operation_status ? '已操作' : '操作')}
                    </button>
                  </td>
                  <td>
                    <button
                      type="button"
                      className="view-more-btn"
                      onClick={() => { void handleViewMore(r) }}
                    >
                      View more
                    </button>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
        {!loading && !error && data && data.total > 0 && (
          <div className="group-f-pagination">
            <p className="empty-hint">
              week_no {data.week_no ?? '–'}，共 {data.total} 条，每页 {data.page_size} 条
            </p>
            {totalPages > 1 && (
              <PaginationControls
                currentPage={currentPage}
                totalPages={totalPages}
                onChangePage={(pageNo) => handlePageChange(pageNo)}
              />
            )}
          </div>
        )}
      </div>
      {detailLoading && <p className="loading-detail">Loading detail...</p>}
      {detail && <GroupADetailModal data={detail} onClose={() => setDetail(null)} />}
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
        <NavLink to="/monitor" className={({ isActive }) => `top-nav-link ${isActive ? 'is-active' : ''}`}>
          Monitor
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
        <Route path="/group/A" element={<GroupAPage />} />
        <Route path="/group/B" element={<PagePlaceholder title="Group B" />} />
        <Route path="/group/F" element={<GroupFPage />} />
        <Route path="/grpup/A" element={<Navigate to="/group/A" replace />} />
        <Route path="/tasks" element={<PagePlaceholder title="Tasks" />} />
        <Route path="/monitor" element={<MonitorPage />} />
      </Route>
    </Routes>
  )
}
