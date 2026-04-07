import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { NavLink, Navigate, Outlet, Route, Routes, useLocation } from 'react-router-dom'
import {
  listSummaryConsolidatedByWeek,
  listWeeks,
  getDetail,
  getTableStats,
  downloadWeekData,
  operateSummary,
  adCheckSummary,
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
  getTrendData,
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
  type TrendResponse,
  type TrendWeekPoint,
} from './api/client'
import './App.css'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Legend,
  Tooltip,
} from 'chart.js'
import { Chart } from 'react-chartjs-2'

ChartJS.register(CategoryScale, LinearScale, BarElement, LineElement, PointElement, Legend, Tooltip)

/** /api/trend/new-listing?format=json 的 views 中单店/全店视图 */
interface TrendNewListingViewPayload {
  labels: string[]
  datasets: Array<{
    type?: string
    label?: string
    data?: number[]
    backgroundColor?: string
    borderWidth?: number
    stack?: string
    yAxisID?: string
  }>
  lineTotal?: number[]
  kpi: { totalAsin: number; activeAsin: number; listingSince: string }
  /** 表格：每批上新(open_date)的上新数 + 上新后每天 sessions（第 1..N 天） */
  cohortTable?: Array<{ cohortDate: string; newAsin: number; daySessions: number[] }>
}

interface TrendNewListingJsonPayload {
  generatedAt?: string
  views: Record<string, TrendNewListingViewPayload>
  storeIds: number[]
  listingSince: string
  listingThrough: string
  sessionChartStart: string
  sessionChartEnd: string
  chartRangeAutoExpanded?: boolean
  cohortTrackDays?: number
  kpiSource?: string
}

/** v4：KPI 与线上 COUNT(*) open_date>since、status='Active' 对账 SQL 一致 */
const TREND_NEW_LISTING_CACHE_KEY = 'asinPerformances.v4.trendNewListingJson'

function readTrendNewListingCache(): TrendNewListingJsonPayload | null {
  try {
    const raw = localStorage.getItem(TREND_NEW_LISTING_CACHE_KEY)
    if (!raw) return null
    const data = JSON.parse(raw) as TrendNewListingJsonPayload
    if (!data || typeof data !== 'object' || !data.views || typeof data.views !== 'object') return null
    return data
  } catch {
    return null
  }
}

function writeTrendNewListingCache(data: TrendNewListingJsonPayload): void {
  try {
    localStorage.setItem(TREND_NEW_LISTING_CACHE_KEY, JSON.stringify(data))
  } catch {
    /* quota / 隐私模式等 */
  }
}

/** 默认用缓存、不自动打接口；?refresh=1 或 ?nocache=1 强制走网络 */
function getTrendNewListingBoot(): { payload: TrendNewListingJsonPayload | null; useCacheOnly: boolean } {
  if (typeof window === 'undefined') return { payload: null, useCacheOnly: false }
  try {
    const sp = new URLSearchParams(window.location.search)
    if (sp.get('refresh') === '1' || sp.get('nocache') === '1') {
      return { payload: null, useCacheOnly: false }
    }
    const cached = readTrendNewListingCache()
    if (cached) return { payload: cached, useCacheOnly: true }
  } catch {
    /* ignore */
  }
  return { payload: null, useCacheOnly: false }
}

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

function formatDecimal(v: number | null | undefined, fractionDigits = 2): string {
  if (v == null || Number.isNaN(v)) return '–'
  return v.toLocaleString('zh-CN', {
    minimumFractionDigits: fractionDigits,
    maximumFractionDigits: fractionDigits,
  })
}

function parseOptionalInt(v: string): number | null {
  const raw = v.trim()
  if (!raw) return null
  const parsed = Number.parseInt(raw, 10)
  return Number.isNaN(parsed) ? null : parsed
}

/** 与 backend `_listing_tracking_week_no` 一致：周日至周六为一周，week_no = 该周周六所在 ISO 年周（YYYYWW）。 */
function listingTrackingWeekNo(d: Date): number {
  const pyWd = d.getDay() === 0 ? 6 : d.getDay() - 1
  const daysSinceSunday = (pyWd + 1) % 7
  const ws = new Date(d.getFullYear(), d.getMonth(), d.getDate() - daysSinceSunday)
  const we = new Date(ws.getFullYear(), ws.getMonth(), ws.getDate() + 6)
  const { isoYear, isoWeek } = isoYearWeekForDate(we)
  return isoYear * 100 + isoWeek
}

/** Python `datetime.isocalendar()` 等价（本地日历日）。 */
function isoYearWeekForDate(d: Date): { isoYear: number; isoWeek: number } {
  const thursday = new Date(d.getFullYear(), d.getMonth(), d.getDate())
  const day = thursday.getDay() || 7
  thursday.setDate(thursday.getDate() + 4 - day)
  const isoYear = thursday.getFullYear()
  const jan4 = new Date(isoYear, 0, 4)
  const jd = jan4.getDay() || 7
  jan4.setDate(jan4.getDate() + 4 - jd)
  const isoWeek = 1 + Math.round((thursday.getTime() - jan4.getTime()) / 604800000)
  return { isoYear, isoWeek }
}

const TREND_WEEK_NO_MIN = 202515

/** 自 minWeekNo 起至 endDate 当周止的所有 week_no（与 listing_tracking 规则一致）。 */
function buildListingTrackingWeekRange(minWeekNo: number, endDate: Date): number[] {
  const endWn = listingTrackingWeekNo(endDate)
  const minYear = Math.floor(minWeekNo / 100)
  const d = new Date(minYear, 0, 1)
  while (listingTrackingWeekNo(d) < minWeekNo) {
    d.setDate(d.getDate() + 1)
  }
  const out: number[] = []
  let prev = -1
  while (true) {
    const wn = listingTrackingWeekNo(d)
    if (wn > endWn) break
    if (wn !== prev) {
      out.push(wn)
      prev = wn
    }
    d.setDate(d.getDate() + 7)
  }
  return out
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
  const [adCheckingKey, setAdCheckingKey] = useState<string | null>(null)
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

  const handleAdCheck = async (parent_asin: string | null, week_no: number | null) => {
    if (parent_asin == null || week_no == null) return
    const key = `${parent_asin}-${week_no}`
    setAdCheckingKey(key)
    setError(null)
    try {
      const res = await adCheckSummary(parent_asin, week_no)
      if (!res.updated || res.updated <= 0) {
        setError(`未匹配到可更新广告记录（parent_asin=${parent_asin}, week_no=${week_no}）`)
      }
      await loadSummary(selectedWeek)
    } catch (e) {
      setError(e instanceof Error ? e.message : '广告操作失败')
    } finally {
      setAdCheckingKey(null)
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
                <th>ad_check</th>
                <th>checked_status</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {summary.map((row, i) => {
                // 以 operation_status=1 为准；operated_at 可能因历史数据/回填残留而非空，但不应单独触发“已操作”显示
                const opDone = row.operation_status === true
                const adDone = row.ad_check === true || (row.ad_created_at != null && row.ad_created_at !== '')
                const hadOperationHistory = !opDone && !!row.last_operated_at
                const hadAdHistory = !adDone && !!row.last_ad_created_at
                const opKey = `${row.parent_asin}-${row.week_no}`
                const isOperating = operatingKey === opKey
                const isAdChecking = adCheckingKey === opKey
                const storeIdsStr = (row.store_ids ?? []).length > 0 ? (row.store_ids as number[]).join(', ') : '–'
                const opTooltip = opDone
                  ? (row.operated_at ? `已操作于 ${formatCreatedAt(row.operated_at)}` : '已操作')
                  : (row.last_operated_at ? `最近一次操作时间：${formatCreatedAt(row.last_operated_at)}` : '点击将该父 ASIN 在本周下的记录标记为已操作')
                const adTooltip = adDone
                  ? (row.ad_created_at ? `已开广告于 ${formatCreatedAt(row.ad_created_at)}` : '已开广告')
                  : (row.last_ad_created_at ? `最近一次开广告时间：${formatCreatedAt(row.last_ad_created_at)}` : '点击标记该父 ASIN 本周已开广告')
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
                        title={opTooltip}
                      >
                        {isOperating ? '处理中...' : (opDone ? '已操作' : (hadOperationHistory ? '操作*' : '操作'))}
                      </button>
                    </td>
                    <td>
                      <button
                        type="button"
                        className={adDone ? 'ad-check-btn ad-check-btn--done' : 'ad-check-btn'}
                        onClick={() => { void handleAdCheck(row.parent_asin, row.week_no) }}
                        disabled={isAdChecking}
                        title={adTooltip}
                      >
                        {isAdChecking ? '处理中...' : (adDone ? '已开广告' : (hadAdHistory ? '开广告*' : '开广告'))}
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
  const [selectedIncompleteWeek, setSelectedIncompleteWeek] = useState<number | null>(null)
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
      setSelectedIncompleteWeek(null)
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
  const weekStatuses = track?.week_statuses ?? []
  const incompleteWeekDetail = weekStatuses.find((item) => item.week_no === selectedIncompleteWeek) ?? null
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
          {selectedParentVisible && weekStatuses.length > 0 && (
            <span className="monitor-operated-at">
              周状态：
              {' '}
              {weekStatuses.map((item, idx) => (
                <span key={item.week_no ?? `wk-${idx}`} className="monitor-week-status-item">
                  <span>{`${item.week_no ?? '–'}${item.completed ? '✅' : ''}`}</span>
                  <button
                    type="button"
                    className="monitor-missing-btn"
                    disabled={(item.incomplete_count ?? 0) <= 0}
                    onClick={() => setSelectedIncompleteWeek(item.week_no ?? null)}
                    title={(item.incomplete_count ?? 0) > 0 ? '查看未完成子 ASIN' : '无未完成子 ASIN'}
                  >
                    {`未完成${item.incomplete_count ?? 0}`}
                  </button>
                </span>
              ))}
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
                            {`${w}(${summary?.queryCount ?? 0})`}
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
      {selectedIncompleteWeek != null && incompleteWeekDetail && (
        <div className="modal-overlay" onClick={() => setSelectedIncompleteWeek(null)}>
          <div className="modal monitor-missing-modal" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h2>{`${selectedParent || '父 ASIN'} - ${selectedIncompleteWeek} 未完成子 ASIN`}</h2>
              <button type="button" className="modal-close" onClick={() => setSelectedIncompleteWeek(null)}>×</button>
            </div>
            <div className="modal-body">
              {incompleteWeekDetail.incomplete_child_asins.length === 0 ? (
                <p className="empty-hint">该周没有未完成子 ASIN。</p>
              ) : (
                <div className="monitor-missing-list">
                  {incompleteWeekDetail.incomplete_child_asins.map((asin) => (
                    <span key={asin} className="monitor-missing-chip">{asin}</span>
                  ))}
                </div>
              )}
            </div>
          </div>
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

type TrendFilterState = {
  store_id: string
  used_model: string
  created_at_start: string
  created_at_end: string
  pid_min: string
  pid_max: string
  parent_asin: string
  selected_week_nos: number[]
  batch_id: string
}

const EMPTY_TREND_FILTERS: TrendFilterState = {
  store_id: '',
  used_model: '',
  created_at_start: '',
  created_at_end: '',
  pid_min: '',
  pid_max: '',
  parent_asin: '',
  selected_week_nos: [],
  batch_id: '',
}

type TrendLineDef = {
  key: keyof TrendWeekPoint
  label: string
  color: string
  formatter?: (value: number) => string
}

function TrendBarOverviewCard({ data }: { data: TrendWeekPoint[] }) {
  const [hoveredWeek, setHoveredWeek] = useState<null | {
    x: number
    y: number
    week_no: number
    new_asin_count: number
    active_asin_count: number
    total_impression: number
  }>(null)

  if (data.length === 0) {
    return (
      <div className="trend-chart-card trend-bar-card">
        <div className="trend-chart-header">
          <div>
            <h3>Weekly Batch Overview</h3>
          </div>
        </div>
        <p className="empty-hint">暂无数据</p>
      </div>
    )
  }

  const width = 1540
  const height = 380
  const padLeft = 72
  const padRight = 72
  const padTop = 26
  const padBottom = 74
  const chartWidth = width - padLeft - padRight
  const chartHeight = height - padTop - padBottom
  const groupWidth = chartWidth / Math.max(data.length, 1)
  const barWidth = Math.max(6, Math.min(18, groupWidth / 5))
  const newCountMax = Math.max(...data.map((item) => item.new_asin_count), 1)
  const activeCountMax = Math.max(...data.map((item) => item.active_asin_count), 1)
  const impressionMax = Math.max(...data.map((item) => item.total_impression), 1)
  const activeCountTicks = Array.from({ length: 5 }, (_, idx) => (activeCountMax * idx) / 4)
  const impressionTicks = Array.from({ length: 5 }, (_, idx) => (impressionMax * idx) / 4)
  const getXCenter = (index: number) => padLeft + groupWidth * index + groupWidth / 2
  const getNewCountY = (value: number) => padTop + chartHeight - (value / newCountMax) * chartHeight
  const getActiveCountY = (value: number) => padTop + chartHeight - (value / activeCountMax) * chartHeight
  const getImpressionY = (value: number) => padTop + chartHeight - (value / impressionMax) * chartHeight
  const getBarHeight = (y: number, value: number) => {
    const rawHeight = height - padBottom - y
    if (value <= 0) return 0
    return Math.max(4, rawHeight)
  }

  return (
    <div className="trend-chart-card trend-bar-card">
      <div className="trend-chart-header">
        <div>
          <h3>Weekly Batch Overview</h3>
          <p className="trend-chart-hint">默认展示所有 batch，筛选后自动联动</p>
        </div>
      </div>
      <div className="trend-chart-legend">
        <span className="trend-legend-item">
          <span className="trend-legend-swatch" style={{ backgroundColor: '#2563eb' }} />
          New ASIN Count
        </span>
        <span className="trend-legend-item">
          <span className="trend-legend-swatch" style={{ backgroundColor: '#16a34a' }} />
          Active ASIN Count
        </span>
        <span className="trend-legend-item">
          <span className="trend-legend-swatch" style={{ backgroundColor: '#f59e0b' }} />
          Total Impression
        </span>
      </div>
      <svg
        viewBox={`0 0 ${width} ${height}`}
        className="trend-chart-svg"
        role="img"
        aria-label="Weekly Batch Overview"
        onMouseLeave={() => setHoveredWeek(null)}
      >
        {activeCountTicks.map((tick, idx) => {
          const y = getActiveCountY(tick)
          return (
            <g key={`count-tick-${idx}`}>
              <line x1={padLeft} y1={y} x2={width - padRight} y2={y} className="trend-grid-line" />
              <text x={padLeft - 8} y={y + 4} textAnchor="end" className="trend-axis-text">
                {Math.round(tick).toLocaleString('zh-CN')}
              </text>
              <text x={width - padRight + 8} y={y + 4} textAnchor="start" className="trend-axis-text">
                {Math.round(impressionTicks[idx] ?? 0).toLocaleString('zh-CN')}
              </text>
            </g>
          )
        })}
        <line x1={padLeft} y1={padTop} x2={padLeft} y2={height - padBottom} className="trend-axis-line" />
        <line x1={width - padRight} y1={padTop} x2={width - padRight} y2={height - padBottom} className="trend-axis-line" />
        <line x1={padLeft} y1={height - padBottom} x2={width - padRight} y2={height - padBottom} className="trend-axis-line" />
        {data.map((item, index) => {
          const centerX = getXCenter(index)
          const bars = [
            { color: '#2563eb', value: item.new_asin_count, y: getNewCountY(item.new_asin_count), offset: -barWidth * 1.2 },
            { color: '#16a34a', value: item.active_asin_count, y: getActiveCountY(item.active_asin_count), offset: 0 },
            { color: '#f59e0b', value: item.total_impression, y: getImpressionY(item.total_impression), offset: barWidth * 1.2 },
          ]
          return (
            <g
              key={`bar-week-${item.week_no}`}
              onMouseEnter={() => setHoveredWeek({
                x: centerX,
                y: Math.min(...bars.map((bar) => bar.y)),
                week_no: item.week_no,
                new_asin_count: item.new_asin_count,
                active_asin_count: item.active_asin_count,
                total_impression: item.total_impression,
              })}
            >
              {bars.map((bar, barIdx) => (
                <rect
                  key={`bar-${item.week_no}-${barIdx}`}
                  x={centerX + bar.offset - barWidth / 2}
                  y={bar.value > 0 ? Math.min(bar.y, height - padBottom - 4) : bar.y}
                  width={barWidth}
                  height={getBarHeight(bar.y, bar.value)}
                  rx="3"
                  fill={bar.color}
                  className="trend-bar-rect"
                />
              ))}
              <g transform={`translate(${centerX - 10}, ${height - 16}) rotate(45)`}>
                <text x="0" y="0" textAnchor="start" className="trend-axis-text trend-axis-text--bold">
                  {item.week_no}
                </text>
              </g>
            </g>
          )
        })}
        {hoveredWeek && (
          <g transform={`translate(${Math.min(width - 220, hoveredWeek.x + 14)}, ${Math.max(18, hoveredWeek.y - 94)})`}>
            <rect width="200" height="82" rx="8" ry="8" className="trend-tooltip-box" />
            <text x="10" y="18" className="trend-tooltip-title">{`${hoveredWeek.week_no}`}</text>
            <text x="10" y="36" className="trend-tooltip-text">{`new asin: ${hoveredWeek.new_asin_count.toLocaleString('zh-CN')}`}</text>
            <text x="10" y="52" className="trend-tooltip-text">{`active asin: ${hoveredWeek.active_asin_count.toLocaleString('zh-CN')}`}</text>
            <text x="10" y="68" className="trend-tooltip-text">{`impression: ${hoveredWeek.total_impression.toLocaleString('zh-CN')}`}</text>
          </g>
        )}
      </svg>
    </div>
  )
}

function TrendChartFigure({
  title,
  data,
  lines,
  expanded = false,
}: {
  title: string
  data: TrendWeekPoint[]
  lines: TrendLineDef[]
  expanded?: boolean
}) {
  const showRelatedClickFormula =
    lines.some((l) => l.key === 'related_click') && lines.some((l) => l.key === 'total_clicks')

  const numericData = data
    .map((item) => ({
      week_no: item.week_no,
      total_asin_count: item.total_asin_count,
      active_asin_count: item.active_asin_count,
      values: lines.map((line) => {
        const raw = item[line.key]
        return typeof raw === 'number' ? Number(raw) : 0
      }),
    }))
    .filter((item) => item.values.every((value) => Number.isFinite(value)))

  if (numericData.length === 0) {
    return <p className="empty-hint">暂无数据</p>
  }
  const showImpressionAsinCount = lines.some((line) => line.key === 'total_impression')

  const [hoveredPoint, setHoveredPoint] = useState<null | {
    x: number
    y: number
    week_no: number
    label: string
    value: number
    color: string
    formatter: (value: number) => string
    total_asin_count: number
    active_asin_count: number
    impression_asin_count: number
  }>(null)

  const width = expanded ? 1040 : 760
  const height = expanded ? 520 : 360
  const padLeft = 64
  const padRight = 24
  const padTop = 24
  const padBottom = 64
  const chartWidth = width - padLeft - padRight
  const chartHeight = height - padTop - padBottom
  const allValues = numericData.flatMap((item) => item.values)
  let minValue = Math.min(...allValues, 0)
  let maxValue = Math.max(...allValues, 0)
  if (minValue === maxValue) {
    const delta = Math.max(1, Math.abs(maxValue || 1) * 0.1)
    minValue -= delta
    maxValue += delta
  }

  const getX = (index: number) => (
    padLeft + (numericData.length <= 1 ? chartWidth / 2 : (index * chartWidth) / (numericData.length - 1))
  )
  const getY = (value: number) => padTop + ((maxValue - value) / (maxValue - minValue)) * chartHeight
  const yTicks = Array.from({ length: 5 }, (_, idx) => minValue + ((maxValue - minValue) * idx) / 4)

  return (
    <>
      {showRelatedClickFormula && (
        <p className="trend-chart-formula">related click = sessions - total clicks</p>
      )}
      <div className="trend-chart-legend">
        {lines.map((line) => {
          const latest = numericData[numericData.length - 1]?.values[lines.indexOf(line)] ?? 0
          const formatter = line.formatter ?? ((value: number) => value.toLocaleString('zh-CN'))
          return (
            <span key={`${title}-${String(line.key)}`} className="trend-legend-item">
              <span className="trend-legend-swatch" style={{ backgroundColor: line.color }} />
              {`${line.label}: ${formatter(latest)}`}
            </span>
          )
        })}
      </div>
      <svg
        viewBox={`0 0 ${width} ${height}`}
        className="trend-chart-svg"
        role="img"
        aria-label={title}
        onMouseLeave={() => setHoveredPoint(null)}
      >
        {yTicks.map((tick, idx) => {
          const y = getY(tick)
          return (
            <g key={`${title}-tick-${idx}`}>
              <line x1={padLeft} y1={y} x2={width - padRight} y2={y} className="trend-grid-line" />
              <text x={padLeft - 8} y={y + 4} textAnchor="end" className="trend-axis-text">
                {(lines[0]?.formatter ?? ((value: number) => value.toLocaleString('zh-CN')))(tick)}
              </text>
            </g>
          )
        })}
        <line x1={padLeft} y1={padTop} x2={padLeft} y2={height - padBottom} className="trend-axis-line" />
        <line x1={padLeft} y1={height - padBottom} x2={width - padRight} y2={height - padBottom} className="trend-axis-line" />
        {lines.map((line, lineIdx) => {
          const points = numericData
            .map((item, index) => `${getX(index)},${getY(item.values[lineIdx])}`)
            .join(' ')
          return (
            <g key={`${title}-${String(line.key)}`}>
              <polyline fill="none" stroke={line.color} strokeWidth="3" points={points} />
              {numericData.map((item, index) => (
                <circle
                  key={`${title}-${String(line.key)}-${item.week_no}`}
                  cx={getX(index)}
                  cy={getY(item.values[lineIdx])}
                  r="5"
                  fill={line.color}
                  className="trend-point"
                  onMouseEnter={() => setHoveredPoint({
                    x: getX(index),
                    y: getY(item.values[lineIdx]),
                    week_no: item.week_no,
                    label: line.label,
                    value: item.values[lineIdx],
                    color: line.color,
                    formatter: line.formatter ?? ((value: number) => value.toLocaleString('zh-CN')),
                    total_asin_count: item.total_asin_count,
                    active_asin_count: item.active_asin_count,
                    impression_asin_count: data[index]?.impression_asin_count ?? 0,
                  })}
                />
              ))}
            </g>
          )
        })}
        {numericData.map((item, index) => (
          <g key={`${title}-meta-${item.week_no}`} transform={`translate(${getX(index)}, ${height - 18}) rotate(45)`}>
            <text x="0" y="0" textAnchor="start" className="trend-axis-text trend-axis-text--bold">
              {item.week_no}
            </text>
          </g>
        ))}
        {hoveredPoint && (
          <g transform={`translate(${Math.min(width - 220, hoveredPoint.x + 14)}, ${Math.max(18, hoveredPoint.y - (showImpressionAsinCount ? 94 : 78))})`}>
            <rect width="200" height={showImpressionAsinCount ? 82 : 66} rx="8" ry="8" className="trend-tooltip-box" />
            <text x="10" y="18" className="trend-tooltip-title">{`${hoveredPoint.week_no} | ${hoveredPoint.label}`}</text>
            <text x="10" y="36" className="trend-tooltip-text">{`value: ${hoveredPoint.formatter(hoveredPoint.value)}`}</text>
            <text x="10" y="52" className="trend-tooltip-text">{`asin: ${hoveredPoint.total_asin_count} | active: ${hoveredPoint.active_asin_count}`}</text>
            {showImpressionAsinCount && (
              <text x="10" y="68" className="trend-tooltip-text">
                {`impression asin: ${hoveredPoint.impression_asin_count}`}
              </text>
            )}
          </g>
        )}
      </svg>
    </>
  )
}

function TrendLineChartCard({
  title,
  data,
  lines,
  onExpand,
}: {
  title: string
  data: TrendWeekPoint[]
  lines: TrendLineDef[]
  onExpand: () => void
}) {
  return (
    <button type="button" className="trend-chart-card trend-chart-button" onClick={onExpand}>
      <div className="trend-chart-header">
        <div>
          <h3>{title}</h3>
          <p className="trend-chart-hint">点击放大查看</p>
        </div>
      </div>
      <TrendChartFigure title={title} data={data} lines={lines} />
    </button>
  )
}

/** SPA 内保留上次成功的 embed HTML，路由切回时先用内存展示，不必等 fetch */
let sessionImpressionCachedHtml: string | null = null

const SESSION_IMPRESSION_HTML_LS_KEY = 'asinPerformances.v1.sessionImpressionHtml'
/** localStorage 单 key 上限附近，避免配额爆掉 */
const SESSION_IMPRESSION_HTML_LS_MAX = 4_500_000

const SESSION_IMPRESSION_FIRST_BUILD_STUB = `<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/><title>生成中</title><style>body{margin:0;font-family:system-ui,"PingFang SC",sans-serif;padding:2rem;background:#0f1419;color:#94a3b8;line-height:1.6}</style></head><body><p>正在生成 session &amp; impression 报表（首次访问或后端内存缓存为空），请稍候…</p></body></html>`

function readSessionImpressionHtmlLs(): string | null {
  try {
    const s = localStorage.getItem(SESSION_IMPRESSION_HTML_LS_KEY)
    if (!s || s.length < 200) return null
    return s
  } catch {
    return null
  }
}

function writeSessionImpressionHtmlLs(html: string): void {
  try {
    if (!html || html.length < 200) return
    if (html.length > SESSION_IMPRESSION_HTML_LS_MAX) return
    localStorage.setItem(SESSION_IMPRESSION_HTML_LS_KEY, html)
  } catch {
    /* quota / 隐私模式 */
  }
}

/** 路由切换：仅 embed=1 读服务端/浏览器缓存；浏览器刷新（reload）时再 rebuild=1 拉最新（用 timeOrigin 避免 SPA 返回页误触发） */
function sessionImpressionRebuildStorageKey(): string {
  if (typeof performance === 'undefined') return 'si-rebuilt-unknown'
  const nav = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming | undefined
  const id =
    typeof performance.timeOrigin === 'number' && performance.timeOrigin > 0
      ? String(performance.timeOrigin)
      : nav
        ? `nav-${nav.startTime}-${nav.loadEventEnd}`
        : `fallback-${Date.now()}`
  return `si-rebuilt-${id}`
}

function shouldRunSessionImpressionRebuildAfterEmbed(): boolean {
  if (typeof performance === 'undefined') return false
  const nav = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming | undefined
  if (!nav || nav.type !== 'reload') return false
  try {
    return !sessionStorage.getItem(sessionImpressionRebuildStorageKey())
  } catch {
    return true
  }
}

function markSessionImpressionRebuildDone(): void {
  try {
    sessionStorage.setItem(sessionImpressionRebuildStorageKey(), '1')
  } catch {
    /* ignore */
  }
}

function getInitialSessionImpressionHtml(): string | null {
  if (sessionImpressionCachedHtml) return sessionImpressionCachedHtml
  return readSessionImpressionHtmlLs()
}

function TrendSessionImpressionEmbeddedPage() {
  const [html, setHtml] = useState<string | null>(() => getInitialSessionImpressionHtml())
  const [err, setErr] = useState<string | null>(null)
  const [bgRefreshing, setBgRefreshing] = useState(false)
  const [bgNotice, setBgNotice] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    setErr(null)
    setBgNotice(null)
    setBgRefreshing(false)

    const runRebuild = shouldRunSessionImpressionRebuildAfterEmbed()

    ;(async () => {
      try {
        const r1 = await fetch('/api/trend/session-impression?embed=1', {
          cache: 'default',
        })
        const embedCacheHdr = (r1.headers.get('X-Session-Impression-Cache') || '').toLowerCase()
        const t1 = await r1.text()
        if (!r1.ok) {
          throw new Error(`HTTP ${r1.status}: ${t1.slice(0, 200)}`)
        }

        const lsHtml = readSessionImpressionHtmlLs()
        let display = t1
        if (embedCacheHdr === 'miss') {
          if (lsHtml) {
            display = lsHtml
            if (!cancelled) {
              setBgNotice('展示浏览器本地缓存，正在向服务器同步最新报表…')
            }
          } else {
            display = SESSION_IMPRESSION_FIRST_BUILD_STUB
          }
        }

        if (!cancelled) {
          sessionImpressionCachedHtml = display
          setHtml(display)
        }

        if (embedCacheHdr === 'hit' && t1.length > 200) {
          writeSessionImpressionHtmlLs(t1)
        }

        // 服务端内存无缓存时必须 rebuild；浏览器刷新时亦 rebuild 拉最新（原逻辑）
        const needRebuild = runRebuild || embedCacheHdr === 'miss'
        if (!needRebuild || cancelled) return

        if (!cancelled) setBgRefreshing(true)
        const r2 = await fetch('/api/trend/session-impression?rebuild=1')
        const t2 = await r2.text()
        if (!cancelled) setBgRefreshing(false)

        if (!r2.ok) {
          if (!cancelled) {
            setBgNotice(
              embedCacheHdr === 'miss' && !lsHtml
                ? '报表生成失败，请稍后刷新页面重试，或新标签打开 /api/trend/session-impression?rebuild=1'
                : '刷新后后台同步失败，仍显示缓存内容。可再次刷新重试。',
            )
          }
          return
        }
        markSessionImpressionRebuildDone()
        writeSessionImpressionHtmlLs(t2)
        if (!cancelled) {
          sessionImpressionCachedHtml = t2
          setHtml(t2)
          setBgNotice(null)
          const cacheHdr2 = r2.headers.get('X-Session-Impression-Cache')
          if (cacheHdr2 === 'stale-fallback') {
            setBgNotice('后台刷新失败，已保留上次成功缓存。')
          }
        }
      } catch (e: unknown) {
        if (!cancelled) {
          setBgRefreshing(false)
          setErr(
            e instanceof Error
              ? e.message
              : '请求失败。请确认后端已启动（如 :9090），且 dev 时 Vite 已代理 /api 到后端；Network 面板勿用会过滤掉该请求的关键字（例如 022）。',
          )
        }
      }
    })()

    return () => {
      cancelled = true
    }
  }, [])

  if (err) {
    return (
      <div className="trend-embed-page trend-embed-page--message">
        <h2 className="trend-embed-error-title">报表加载失败</h2>
        <pre className="trend-embed-error-body">{err}</pre>
        <p className="trend-embed-hint">
          可直接访问{' '}
          <a href="/api/trend/session-impression?rebuild=1" target="_blank" rel="noreferrer">
            /api/trend/session-impression?rebuild=1
          </a>{' '}
          强制全量重算。进入本页会在服务端无缓存时自动触发重建；亦可<strong>刷新浏览器</strong>拉最新。
        </p>
      </div>
    )
  }
  if (html === null) {
    return (
      <div className="trend-embed-page trend-embed-page--message">
        <p className="trend-embed-loading">正在加载 session &amp; impression 报表…</p>
        <p className="trend-embed-hint">
          正在请求报表。服务端有内存缓存时较快；无缓存时会自动全量生成并写入浏览器本地缓存，下次进入可秒开。
        </p>
      </div>
    )
  }
  const showBar = bgRefreshing || Boolean(bgNotice)
  return (
    <div className={`trend-embed-page${showBar ? ' trend-embed-page--with-bar' : ''}`}>
      {showBar ? (
        <div className="trend-embed-bgbar" role="status">
          {bgRefreshing ? '正在后台与线上库同步最新报表…' : bgNotice}
        </div>
      ) : null}
      <iframe
        className="trend-embed-frame"
        title="session & impression 报表"
        srcDoc={html}
      />
    </div>
  )
}

function TrendNewListingEmbeddedPage() {
  const boot = useMemo(() => getTrendNewListingBoot(), [])
  const [payload, setPayload] = useState<TrendNewListingJsonPayload | null>(boot.payload)
  const [fromCache, setFromCache] = useState(boot.useCacheOnly)
  const [storeKey, setStoreKey] = useState<string>('all')
  const [err, setErr] = useState<string | null>(null)
  const [refreshing, setRefreshing] = useState(false)

  const fetchNewListingJson = useCallback(async (opts: { skipSync: boolean; writeCache: boolean }) => {
    const url = `/api/trend/new-listing?format=json${opts.skipSync ? '&skip_sync=false' : ''}`
    const res = await fetch(url)
    const text = await res.text()
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${text.slice(0, 200)}`)
    const data = JSON.parse(text) as TrendNewListingJsonPayload
    if (opts.writeCache) writeTrendNewListingCache(data)
    setPayload(data)
    setFromCache(false)
  }, [])

  useEffect(() => {
    let cancelled = false
    setErr(null)
    if (boot.useCacheOnly && boot.payload) {
      return () => {
        cancelled = true
      }
    }
    const nav =
      typeof performance !== 'undefined'
        ? (performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming | undefined)
        : undefined
    const syncOnReload = nav?.type === 'reload'
    fetchNewListingJson({ skipSync: syncOnReload, writeCache: true }).catch((e: unknown) => {
      if (!cancelled) setErr(e instanceof Error ? e.message : '请求失败')
    })
    return () => {
      cancelled = true
    }
  }, [boot.useCacheOnly, boot.payload, fetchNewListingJson])

  const view = payload?.views?.[storeKey] ?? payload?.views?.all

  if (err) {
    return (
      <div className="trend-embed-page trend-embed-page--message">
        <h2 className="trend-embed-error-title">页面加载失败</h2>
        <pre className="trend-embed-error-body">{err}</pre>
      </div>
    )
  }
  if (payload === null || !view) {
    return (
      <div className="trend-embed-page trend-embed-page--message">
        <p className="trend-embed-loading">正在加载 New Listing 报表…</p>
      </div>
    )
  }

  const barDatasets = (view.datasets || []).map((ds) => ({
    type: 'bar' as const,
    label: ds.label ?? '',
    data: (ds.data ?? []).map((x) => Number(x)),
    backgroundColor: ds.backgroundColor,
    borderWidth: ds.borderWidth ?? 0,
    stack: ds.stack ?? 'sess',
    yAxisID: ds.yAxisID ?? 'y',
  }))
  const lt =
    view.lineTotal && view.lineTotal.length === view.labels.length
      ? view.lineTotal.map((n) => Number(n))
      : view.labels.map((_, idx) =>
          barDatasets.reduce((acc, ds) => acc + Number(ds.data[idx] ?? 0), 0),
        )
  const maxY = Math.max(1, ...lt) * 1.12
  const chartData = {
    labels: view.labels,
    datasets: [
      ...barDatasets,
      {
        type: 'line' as const,
        label: '当日 sessions 合计',
        data: lt,
        borderColor: '#111827',
        backgroundColor: 'transparent',
        borderWidth: 2.5,
        pointRadius: 4,
        pointBackgroundColor: '#111827',
        tension: 0.2,
        order: 100,
        yAxisID: 'y1',
      },
    ],
  }

  const storeOptions = [
    { value: 'all', label: '全部店铺' },
    ...(payload.storeIds || []).map((id) => ({ value: String(id), label: `店铺 ${id}` })),
  ]

  const cohortTrackDays = Math.max(1, Number(payload.cohortTrackDays ?? 30))
  const cohortTable = Array.isArray((view as any)?.cohortTable) ? ((view as any).cohortTable as any[]) : []

  return (
    <div className="trend-embed-page trend-new-listing-page">
      <div className="trend-new-listing-toolbar">
        <label className="trend-new-listing-label" htmlFor="trend-nl-store">
          店铺
        </label>
        <select
          id="trend-nl-store"
          className="trend-new-listing-select"
          value={storeKey}
          onChange={(e) => setStoreKey(e.target.value)}
        >
          {storeOptions.map((o) => (
            <option key={o.value} value={o.value}>
              {o.label}
            </option>
          ))}
        </select>
        <button
          type="button"
          className="trend-new-listing-refresh-btn"
          disabled={refreshing}
          onClick={async () => {
            setRefreshing(true)
            setErr(null)
            try {
              await fetchNewListingJson({ skipSync: false, writeCache: true })
            } catch (e: unknown) {
              setErr(e instanceof Error ? e.message : '请求失败')
            } finally {
              setRefreshing(false)
            }
          }}
        >
          {refreshing ? '加载中…' : '重新从服务器加载'}
        </button>
        <button
          type="button"
          className="trend-new-listing-refresh-btn trend-new-listing-refresh-btn--secondary"
          disabled={refreshing}
          onClick={async () => {
            setRefreshing(true)
            setErr(null)
            try {
              await fetchNewListingJson({ skipSync: true, writeCache: true })
            } catch (e: unknown) {
              setErr(e instanceof Error ? e.message : '请求失败')
            } finally {
              setRefreshing(false)
            }
          }}
        >
          同步 listing 并重载
        </button>
        <span className="trend-new-listing-meta">
          KPI：open_date &gt; {payload.listingSince}（amazon_listing 全表行）· 每批 {payload.cohortTrackDays ?? 30} 日 · 横轴{' '}
          {payload.sessionChartStart}～{payload.sessionChartEnd}
          {payload.chartRangeAutoExpanded ? '（已按本地数据扩展区间）' : ''}
          {fromCache ? (
            <>
              {' '}
              · 默认展示本地缓存（生成 {payload.generatedAt ?? '—'}），不自动请求接口
            </>
          ) : null}
        </span>
      </div>
      <div className="trend-new-listing-kpi">
        <div className="trend-new-listing-kpi-card">
          <span className="trend-new-listing-kpi-title">Total Asins</span>
          <strong>{Number(view.kpi?.totalAsin ?? 0).toLocaleString()}</strong>
        </div>
        <div className="trend-new-listing-kpi-card">
          <span className="trend-new-listing-kpi-title">Active Asins</span>
          <strong>{Number(view.kpi?.activeAsin ?? 0).toLocaleString()}</strong>
        </div>
      </div>
      <p className="trend-new-listing-hint">
        柱形为各上新批次（open_date）贡献的 sessions 堆叠；黑色折线为每日合计。横坐标仅包含有 session 数据的日期。
        首次进入若无缓存会自动请求一次；之后默认展示浏览器本地缓存，不自动打接口。地址栏加{' '}
        <code className="trend-new-listing-code">?refresh=1</code> 可强制重新拉取。
        {payload.kpiSource === 'amazon_listing'
          ? ' 顶部 KPI：online amazon_listing，COUNT(*) 且 DATE(open_date) > listing_since；Active 另加 status = Active。'
          : payload.kpiSource === 'amazon_listing_new_asin_local_kpi'
            ? ' 上新日 / 各日上新 ASIN 数来自线上 amazon_listing；顶部 KPI 为本地表回退。'
            : ''}
      </p>
      <div className="trend-new-listing-chart-wrap">
        {view.labels.length === 0 || barDatasets.length === 0 ? (
          <p className="trend-new-listing-empty">暂无图表数据（请确认已同步 daily_upload_asin_dates 且 open_date 非空）。</p>
        ) : (
          <Chart
            type="bar"
            data={chartData}
            options={{
              responsive: true,
              maintainAspectRatio: false,
              interaction: { mode: 'index', intersect: false },
              plugins: {
                legend: { position: 'top' },
                tooltip: {
                  callbacks: {
                    footer: (items) => {
                      if (!items.length) return ''
                      const idx = items[0].dataIndex
                      const total = lt[idx]
                      return total != null ? `合计 ${Number(total).toLocaleString()} sessions` : ''
                    },
                  },
                },
              },
              scales: {
                x: {
                  stacked: true,
                  ticks: { maxRotation: 45, minRotation: 0 },
                },
                y: {
                  stacked: true,
                  beginAtZero: true,
                  max: maxY,
                  title: { display: true, text: 'Sessions（堆叠）' },
                },
                y1: {
                  stacked: false,
                  position: 'right',
                  beginAtZero: true,
                  max: maxY,
                  grid: { drawOnChartArea: false },
                  title: { display: true, text: '合计（折线）' },
                },
              },
            }}
          />
        )}
      </div>

      <div className="trend-new-listing-table-wrap">
        <h3 className="trend-new-listing-table-title">批次明细（上新数 &amp; 上新后每日 sessions）</h3>
        <p className="trend-new-listing-table-caption">
          前两列「上新日 / 上新 ASIN 数」来自线上 <code className="trend-new-listing-code">amazon_listing</code>
          （open_date 在 [{payload.listingSince}, listing_through] 内按日、asin 非空且 TRIM 非空；与顶部 KPI 全表 COUNT(*) 口径不同）。切换「店铺」按 store_id
          切分。后列为本地 sessions 明细。
        </p>
        {!cohortTable.length ? (
          <p className="trend-new-listing-table-empty">暂无表格数据（需要 open_date 批次与本地 sessions 明细）。</p>
        ) : (
          <div className="trend-new-listing-table-scroll">
            <table className="trend-new-listing-table">
              <thead>
                <tr>
                  <th className="is-sticky-col is-sticky-col--1">上新日（PST）</th>
                  <th className="is-sticky-col is-sticky-col--2">上新 ASIN 数</th>
                  {Array.from({ length: cohortTrackDays }, (_, i) => (
                    <th key={`d${i + 1}`}>{`第${i + 1}天`}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {cohortTable.map((row: any) => {
                  const cd = String(row?.cohortDate ?? '')
                  const newAsin = Number(row?.newAsin ?? 0)
                  const daySessions: number[] = Array.isArray(row?.daySessions)
                    ? row.daySessions.map((x: any) => Number(x ?? 0))
                    : []
                  return (
                    <tr key={cd || Math.random()}>
                      <td className="is-sticky-col is-sticky-col--1">{cd || '–'}</td>
                      <td className="is-sticky-col is-sticky-col--2">{newAsin.toLocaleString('zh-CN')}</td>
                      {Array.from({ length: cohortTrackDays }, (_, i) => (
                        <td key={`${cd}-s-${i}`}>{Number(daySessions[i] ?? 0).toLocaleString('zh-CN')}</td>
                      ))}
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}

function TrendPage() {
  const [filters, setFilters] = useState<TrendFilterState>(EMPTY_TREND_FILTERS)
  const [appliedFilters, setAppliedFilters] = useState<TrendFilterState>(EMPTY_TREND_FILTERS)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [data, setData] = useState<TrendResponse | null>(null)
  const [expandedChartKey, setExpandedChartKey] = useState<string | null>(null)
  const [filtersExpanded, setFiltersExpanded] = useState(false)
  const [weekNoSearch, setWeekNoSearch] = useState('')
  const [weekDropdownOpen, setWeekDropdownOpen] = useState(false)
  const weekMultiselectRef = useRef<HTMLDivElement | null>(null)

  const options = data?.filter_options
  /** 页面内即时生成 202515 → 本周，不等待接口，避免周次列表加载阻塞 */
  const syntheticWeekChoices = useMemo(
    () => buildListingTrackingWeekRange(TREND_WEEK_NO_MIN, new Date()),
    [],
  )
  const weekChoices = useMemo(() => {
    const set = new Set<number>(syntheticWeekChoices)
    for (const w of filters.selected_week_nos) {
      set.add(w)
    }
    return Array.from(set).sort((a, b) => a - b)
  }, [syntheticWeekChoices, filters.selected_week_nos])

  const filteredWeekChoices = useMemo(() => {
    const q = weekNoSearch.trim().toLowerCase()
    if (!q) return weekChoices
    return weekChoices.filter((wn) => String(wn).toLowerCase().includes(q))
  }, [weekChoices, weekNoSearch])

  const appliedSummaryFull = useMemo(() => {
    const parts: string[] = []
    const af = appliedFilters
    if (af.store_id.trim()) parts.push(`店铺 ${af.store_id}`)
    if (af.batch_id.trim()) {
      const bid = af.batch_id.trim()
      const bo = options?.batch_options?.find((b) => String(b.id) === bid)
      parts.push(bo?.label ? `批次 ${bo.label}` : `批次 id ${bid}`)
    }
    if (af.used_model.trim()) parts.push(`模型 ${af.used_model}`)
    if (af.created_at_start.trim() || af.created_at_end.trim()) {
      parts.push(`创建 ${af.created_at_start || '…'} ~ ${af.created_at_end || '…'}`)
    }
    if (af.pid_min.trim() || af.pid_max.trim()) {
      parts.push(`PID ${af.pid_min || '…'}–${af.pid_max || '…'}`)
    }
    const asinTokens = af.parent_asin.split(/[\s,;]+/).map((s) => s.trim()).filter(Boolean)
    if (asinTokens.length) parts.push(`父 ASIN ×${asinTokens.length}`)
    if (af.selected_week_nos.length) {
      const w = [...af.selected_week_nos].sort((a, b) => a - b)
      parts.push(w.length <= 3 ? `周次 ${w.join(', ')}` : `周次 ${w.length} 项`)
    }
    return parts.length > 0 ? `已应用 · ${parts.join(' · ')}` : '已应用 · 未限定（全部）'
  }, [appliedFilters, options?.batch_options])

  const appliedSummaryShort =
    appliedSummaryFull.length > 96 ? `${appliedSummaryFull.slice(0, 94)}…` : appliedSummaryFull

  useEffect(() => {
    const request = {
      store_id: parseOptionalInt(appliedFilters.store_id),
      used_model: appliedFilters.used_model.trim() || null,
      created_at_start: appliedFilters.created_at_start.trim() || null,
      created_at_end: appliedFilters.created_at_end.trim() || null,
      pid_min: parseOptionalInt(appliedFilters.pid_min),
      pid_max: parseOptionalInt(appliedFilters.pid_max),
      parent_asin: appliedFilters.parent_asin.trim() || null,
      week_nos:
        appliedFilters.selected_week_nos.length > 0
          ? [...appliedFilters.selected_week_nos].sort((a, b) => a - b)
          : null,
      batch_id: parseOptionalInt(appliedFilters.batch_id),
    }

    setLoading(true)
    setError(null)
    getTrendData(request)
      .then(setData)
      .catch((e) => setError(e instanceof Error ? e.message : 'Failed to load trend data'))
      .finally(() => setLoading(false))
  }, [appliedFilters])

  useEffect(() => {
    if (!weekDropdownOpen) return
    const onDoc = (e: MouseEvent) => {
      const el = weekMultiselectRef.current
      if (el && !el.contains(e.target as Node)) {
        setWeekDropdownOpen(false)
      }
    }
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setWeekDropdownOpen(false)
    }
    document.addEventListener('mousedown', onDoc)
    document.addEventListener('keydown', onKey)
    return () => {
      document.removeEventListener('mousedown', onDoc)
      document.removeEventListener('keydown', onKey)
    }
  }, [weekDropdownOpen])

  const handleApplyFilters = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    const intFields: Array<keyof Pick<TrendFilterState, 'store_id' | 'pid_min' | 'pid_max' | 'batch_id'>> = [
      'store_id',
      'pid_min',
      'pid_max',
      'batch_id',
    ]
    for (const key of intFields) {
      const raw = filters[key].trim()
      if (raw && parseOptionalInt(raw) == null) {
        setFiltersExpanded(true)
        setError(`${key} 需要填写整数`)
        return
      }
    }
    const pm = filters.pid_min.trim()
    const px = filters.pid_max.trim()
    if (pm && px) {
      const a = parseOptionalInt(pm)
      const b = parseOptionalInt(px)
      if (a != null && b != null && a > b) {
        setFiltersExpanded(true)
        setError('pid_min 不能大于 pid_max')
        return
      }
    }
    if (
      filters.created_at_start.trim() &&
      filters.created_at_end.trim() &&
      filters.created_at_start.trim() > filters.created_at_end.trim()
    ) {
      setFiltersExpanded(true)
      setError('created_at_start 不能晚于 created_at_end')
      return
    }
    setAppliedFilters({
      ...filters,
      selected_week_nos: [...filters.selected_week_nos],
    })
  }

  const handleResetFilters = () => {
    setFilters(EMPTY_TREND_FILTERS)
    setAppliedFilters(EMPTY_TREND_FILTERS)
    setError(null)
    setFiltersExpanded(false)
    setWeekNoSearch('')
    setWeekDropdownOpen(false)
  }

  const series = data?.series ?? []
  const chartConfigs = useMemo<Array<{ key: string; title: string; lines: TrendLineDef[] }>>(
    () => [
      {
        key: 'total_impression',
        title: 'Total Impression',
        lines: [{ key: 'total_impression', label: 'Impression', color: '#2563eb' }],
      },
      {
        key: 'total_sessions',
        title: 'Total Sessions',
        lines: [{ key: 'total_sessions', label: 'Sessions', color: '#16a34a' }],
      },
      {
        key: 'impression_asin_count',
        title: 'Impression ASIN Count',
        lines: [{ key: 'impression_asin_count', label: 'ASIN Count', color: '#7c3aed' }],
      },
      {
        key: 'related_click',
        title: 'Related Click vs Total Clicks',
        lines: [
          { key: 'related_click', label: 'Related Click', color: '#ea580c' },
          { key: 'total_clicks', label: 'Total Clicks', color: '#0f766e' },
        ],
      },
      {
        key: 'impression_asin_rate',
        title: 'Impression ASIN Rate',
        lines: [
          {
            key: 'impression_asin_rate',
            label: 'Impression / ASIN',
            color: '#dc2626',
            formatter: (value: number) => formatDecimal(value, 2),
          },
        ],
      },
    ],
    [],
  )
  const expandedChart = useMemo(
    () => chartConfigs.find((item) => item.key === expandedChartKey) ?? null,
    [chartConfigs, expandedChartKey],
  )

  return (
    <div className="app trend-page">
      <h1>Weekly trend</h1>
      <p className="monitor-desc">基于 `listing_tracking` 按筛选条件聚合展示周趋势。</p>
      <form className="trend-filters" onSubmit={handleApplyFilters}>
        <div className="trend-filter-bar">
          <div className="trend-filter-bar-top">
            <div className="trend-filter-quick">
              <label className="trend-filter-quick-field">
                <span className="trend-filter-quick-label">store_id</span>
                <select value={filters.store_id} onChange={(e) => setFilters((prev) => ({ ...prev, store_id: e.target.value }))}>
                  <option value="">全部</option>
                  {(options?.store_ids ?? []).map((item) => (
                    <option key={item} value={String(item)}>{item}</option>
                  ))}
                </select>
              </label>
              <label className="trend-filter-quick-field">
                <span className="trend-filter-quick-label">used_model</span>
                <select value={filters.used_model} onChange={(e) => setFilters((prev) => ({ ...prev, used_model: e.target.value }))}>
                  <option value="">全部</option>
                  {(options?.used_models ?? []).map((item) => (
                    <option key={item} value={item}>{item}</option>
                  ))}
                </select>
              </label>
              <div
                className={`trend-filter-quick-field trend-filter-quick-field--week${weekDropdownOpen ? ' is-week-dropdown-open' : ''}`}
              >
                <span className="trend-filter-quick-label">week_no（多选）</span>
                <div className="trend-week-multiselect trend-week-multiselect--in-bar" ref={weekMultiselectRef}>
                  <button
                    type="button"
                    className="trend-week-multiselect-trigger"
                    aria-expanded={weekDropdownOpen}
                    aria-haspopup="listbox"
                    onClick={() => setWeekDropdownOpen((o) => !o)}
                  >
                    <span className="trend-week-multiselect-trigger-text">
                      {filters.selected_week_nos.length === 0
                        ? `全部周次（${TREND_WEEK_NO_MIN}–${weekChoices[weekChoices.length - 1] ?? '…'}，${weekChoices.length} 个）`
                        : `已选 ${filters.selected_week_nos.length} / ${weekChoices.length} 周`}
                    </span>
                    <span
                      className={`trend-week-multiselect-chevron ${weekDropdownOpen ? 'is-open' : ''}`}
                      aria-hidden
                    >
                      ▼
                    </span>
                  </button>
                  {filters.selected_week_nos.length > 0 && weekChoices.length > 0 && (
                    <div className="trend-week-multiselect-chips">
                      {[...filters.selected_week_nos]
                        .sort((a, b) => a - b)
                        .slice(0, 8)
                        .map((wn) => (
                          <button
                            key={wn}
                            type="button"
                            className="trend-week-chip"
                            title="移除此周"
                            onClick={(e) => {
                              e.stopPropagation()
                              setFilters((prev) => ({
                                ...prev,
                                selected_week_nos: prev.selected_week_nos.filter((x) => x !== wn),
                              }))
                            }}
                          >
                            <span>{wn}</span>
                            <span className="trend-week-chip-x" aria-hidden>×</span>
                          </button>
                        ))}
                      {filters.selected_week_nos.length > 8 && (
                        <span className="trend-week-chip-more">
                          +{filters.selected_week_nos.length - 8}
                        </span>
                      )}
                    </div>
                  )}
                  {weekDropdownOpen && weekChoices.length > 0 && (
                    <div className="trend-week-multiselect-dropdown" role="listbox" aria-multiselectable>
                      <div className="trend-week-ms-dropdown-top">
                        <input
                          type="search"
                          className="trend-week-ms-search"
                          value={weekNoSearch}
                          onChange={(e) => setWeekNoSearch(e.target.value)}
                          placeholder="搜索周次…"
                          aria-label="在列表中筛选周次"
                          onMouseDown={(e) => e.stopPropagation()}
                        />
                        <div className="trend-week-ms-actions">
                          <button
                            type="button"
                            className="trend-week-ms-link"
                            onClick={() =>
                              setFilters((prev) => ({
                                ...prev,
                                selected_week_nos: [...syntheticWeekChoices],
                              }))}
                          >
                            全选
                          </button>
                          <button
                            type="button"
                            className="trend-week-ms-link"
                            onClick={() => setFilters((prev) => ({ ...prev, selected_week_nos: [] }))}
                          >
                            清空
                          </button>
                        </div>
                      </div>
                      <p className="trend-week-ms-count">
                        共 {weekChoices.length} 周（自 {TREND_WEEK_NO_MIN} 至当前周）
                        {weekNoSearch.trim() ? ` · 列表中 ${filteredWeekChoices.length} 个` : ''}
                      </p>
                      <div className="trend-week-ms-list">
                        {filteredWeekChoices.length === 0 ? (
                          <p className="trend-week-ms-empty">无匹配周次</p>
                        ) : (
                          filteredWeekChoices.map((wn) => {
                            const checked = filters.selected_week_nos.includes(wn)
                            return (
                              <label
                                key={wn}
                                className={`trend-week-ms-option${checked ? ' is-checked' : ''}`}
                                role="option"
                                aria-selected={checked}
                              >
                                <input
                                  type="checkbox"
                                  className="trend-week-ms-checkbox"
                                  checked={checked}
                                  onChange={() => {
                                    setFilters((prev) => ({
                                      ...prev,
                                      selected_week_nos: prev.selected_week_nos.includes(wn)
                                        ? prev.selected_week_nos.filter((x) => x !== wn)
                                        : [...prev.selected_week_nos, wn].sort((a, b) => a - b),
                                    }))
                                  }}
                                />
                                <span className="trend-week-ms-label">{wn}</span>
                              </label>
                            )
                          })
                        )}
                      </div>
                      <div className="trend-week-ms-footer">
                        <button
                          type="button"
                          className="trend-week-ms-done"
                          onClick={() => setWeekDropdownOpen(false)}
                        >
                          完成
                        </button>
                      </div>
                    </div>
                  )}
                </div>
               
              </div>
              <label className="trend-filter-quick-field trend-filter-quick-field--batch">
                <span className="trend-filter-quick-label">batch_id_title</span>
                <select value={filters.batch_id} onChange={(e) => setFilters((prev) => ({ ...prev, batch_id: e.target.value }))}>
                  <option value="">全部</option>
                  {(options?.batch_options ?? []).map((item) => (
                    <option key={item.id} value={String(item.id)}>{item.label}</option>
                  ))}
                </select>
              </label>
              <button
                type="button"
                className="trend-filter-expand-btn"
                id="trend-filter-toggle"
                aria-expanded={filtersExpanded}
                aria-controls="trend-filter-panel"
                aria-label={filtersExpanded ? '收起更多筛选条件' : '展开更多筛选条件'}
                title={filtersExpanded ? '收起更多筛选' : '展开更多筛选'}
                onClick={() => setFiltersExpanded((v) => !v)}
              >
                <span className={`trend-filter-chevron ${filtersExpanded ? 'is-open' : ''}`} aria-hidden>›</span>
              </button>
            </div>
            <div className="trend-filter-bar-actions">
              <button type="submit" className="trend-filter-bar-btn trend-filter-bar-btn--primary">查询</button>
              <button type="button" className="trend-filter-bar-btn" onClick={handleResetFilters}>重置</button>
            </div>
          </div>
          <p className="trend-filter-bar-summary" title={appliedSummaryFull}>
            {appliedSummaryShort}
          </p>
        </div>

        <div
          id="trend-filter-panel"
          className="trend-filter-details"
          role="region"
          aria-labelledby="trend-filter-toggle"
          hidden={!filtersExpanded}
        >
        <div className="trend-filter-grid">
          <p className="trend-filter-more-hint">更多条件：创建时间、PID 范围、父 ASIN</p>
          <div className="trend-filter-row">
            <div className="trend-filter-date-block">
              <span className="trend-filter-label-text trend-filter-label-text--block">created_at</span>
              <div className="trend-filter-date-pair">
                <label className="trend-filter-field trend-filter-field--inline">
                  <span className="trend-filter-sublabel">起始</span>
                  <input
                    type="date"
                    value={filters.created_at_start}
                    onChange={(e) => setFilters((prev) => ({ ...prev, created_at_start: e.target.value }))}
                  />
                </label>
                <label className="trend-filter-field trend-filter-field--inline">
                  <span className="trend-filter-sublabel">结束</span>
                  <input
                    type="date"
                    value={filters.created_at_end}
                    onChange={(e) => setFilters((prev) => ({ ...prev, created_at_end: e.target.value }))}
                  />
                </label>
              </div>
            </div>
          </div>

          <div className="trend-filter-row trend-filter-row--pid-asin">
            <div className="trend-filter-pid-range trend-filter-field">
              <span className="trend-filter-label-text trend-filter-pid-range-label">pid 范围</span>
              <div className="trend-filter-pid-inputs">
                <label>
                  <span className="trend-filter-sublabel">起</span>
                  <input
                    type="text"
                    inputMode="numeric"
                    value={filters.pid_min}
                    onChange={(e) => setFilters((prev) => ({ ...prev, pid_min: e.target.value }))}
                    placeholder="下限"
                  />
                </label>
                <label>
                  <span className="trend-filter-sublabel">止</span>
                  <input
                    type="text"
                    inputMode="numeric"
                    value={filters.pid_max}
                    onChange={(e) => setFilters((prev) => ({ ...prev, pid_max: e.target.value }))}
                    placeholder="上限"
                  />
                </label>
              </div>
            </div>
            <label className="trend-filter-field trend-filter-parent-asin">
              <span className="trend-filter-label-text">parent_asin</span>
              <textarea
                value={filters.parent_asin}
                onChange={(e) => setFilters((prev) => ({ ...prev, parent_asin: e.target.value }))}
                placeholder="多个父 ASIN：逗号、分号或换行分隔（精确匹配）"
                rows={3}
              />
            </label>
          </div>
        </div>
        </div>
      </form>
      {error && <p className="error">{error}</p>}
      {loading && <p className="loading-hint">加载趋势数据...</p>}
      {!loading && !error && data && (
        <>
          <p className="empty-hint">
            匹配记录数：{data.matched_row_count}，周数：{data.weeks.length}
          </p>
          {series.length === 0 ? (
            <p className="empty-hint">当前筛选条件下暂无可展示的趋势数据。</p>
          ) : (
            <>
              <TrendBarOverviewCard data={series} />
              <div className="trend-chart-grid">
                {chartConfigs.map((chart) => (
                  <TrendLineChartCard
                    key={chart.key}
                    title={chart.title}
                    data={series}
                    lines={chart.lines}
                    onExpand={() => setExpandedChartKey(chart.key)}
                  />
                ))}
              </div>
            </>
          )}
        </>
      )}
      {expandedChart && (
        <ZoomModal title={expandedChart.title} onClose={() => setExpandedChartKey(null)}>
          <div className="trend-chart-card trend-chart-card--expanded">
            <TrendChartFigure title={expandedChart.title} data={series} lines={expandedChart.lines} expanded />
          </div>
        </ZoomModal>
      )}
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
  const location = useLocation()
  const [groupOpen, setGroupOpen] = useState(false)
  const [trendOpen, setTrendOpen] = useState(false)
  const groupRef = useRef<HTMLDivElement | null>(null)
  const trendRef = useRef<HTMLDivElement | null>(null)

  const trendingSubPaths =
    location.pathname === '/trend' ||
    location.pathname === '/trend/session-impression' ||
    location.pathname === '/trend/session&impression' ||
    location.pathname === '/trend/New Listing'
  const trendingNavActive = trendingSubPaths || trendOpen

  useEffect(() => {
    const onDocMouseDown = (e: MouseEvent) => {
      const t = e.target as Node
      if (groupRef.current && !groupRef.current.contains(t)) {
        setGroupOpen(false)
      }
      if (trendRef.current && !trendRef.current.contains(t)) {
        setTrendOpen(false)
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
        <div className="top-nav-group" ref={trendRef}>
          <button
            type="button"
            className={`top-nav-link top-nav-group-toggle ${trendingNavActive ? 'is-active' : ''}`}
            onClick={() => setTrendOpen((v) => !v)}
            aria-expanded={trendOpen}
            aria-haspopup="menu"
          >
            Trending
          </button>
          <div className={`top-nav-menu top-nav-menu--wide ${trendOpen ? 'is-open' : ''}`}>
            <NavLink
              to="/trend"
              className="top-nav-menu-link"
              onClick={() => setTrendOpen(false)}
            >
              Weekly trend
            </NavLink>
            <NavLink
              to="/trend/session-impression"
              className="top-nav-menu-link"
              onClick={() => setTrendOpen(false)}
            >
              session & impression
            </NavLink>
            <NavLink
              to="/trend/New Listing"
              className="top-nav-menu-link"
              onClick={() => setTrendOpen(false)}
            >
              New Listing
            </NavLink>
          </div>
        </div>
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
        <Route path="/trend" element={<TrendPage />} />
        <Route path="/trend/session-impression" element={<TrendSessionImpressionEmbeddedPage />} />
        <Route path="/trend/session&impression" element={<Navigate to="/trend/session-impression" replace />} />
        <Route path="/trend/New Listing" element={<TrendNewListingEmbeddedPage />} />
      </Route>
    </Routes>
  )
}
