import {
  memo,
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from 'react'
import { createPortal } from 'react-dom'
import {
  type TrendWeekPoint,
} from '../../api/client'
import { formatPermyriadUntilVisible } from '../../lib/formatters'
import { Chart } from '../../lib/chartRegister'
import NlJsonWorker from '../../nlJsonParse.worker?worker'
import './trendRoutes.css'
import {
  collectNlOrderFlagCandidates,
  incrementNlOrderFlagsDailyCount,
  loadNlOrderPositiveKeySet,
  NL_ORDER_FLAGS_BATCH_MAX,
  NL_ORDER_FLAGS_MAX_POSTS_PER_DAY,
  persistNlOrderPositiveKeys,
  readNlOrderFlagsPostsToday,
  remainingNlOrderFlagPostsToday,
} from '../../lib/nlOrderFlagsStorage'

/** Dev：StrictMode 双挂载合并首屏 json_views=all；见 fetchNewListingJson dedupe */
let nlDevAllShellSingleFlight: Promise<void> | null = null

/** /api/trend/new-listing?format=json 的 views 中单店/全店视图 */
export interface TrendNewListingViewPayload {
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
  kpi: {
    totalAsin: number
    activeAsin: number
    listingSince: string
    listingNewCount?: number | null
    listingRefurbishedCount?: number | null
  }
  /** 表格：每批上新(open_date)的上新数 + 上新后每天 sessions（第 1..N 天） */
  cohortTable?: Array<{
    cohortDate: string
    newAsin: number
    listingNewCount?: number | null
    listingRefurbishedCount?: number | null
    daySessions: number[]
    /** 与 daySessions 同索引：该天 sessions>0 的 ASIN 明细 */
    daySessionAsins?: Array<Array<{ asin: string; storeId: number; sessions: number }>>
  }>
}

export interface TrendNewListingJsonPayload {
  generatedAt?: string
  views: Record<string, TrendNewListingViewPayload>
  storeIds: number[]
  listingSince: string
  listingThrough: string
  sessionRequestedStart?: string
  sessionRequestedEnd?: string
  sessionChartStart: string
  sessionChartEnd: string
  chartRangeAutoExpanded?: boolean
  cohortTrackDays?: number
  kpiSource?: string
  /** 后端 `profile=1` 时各阶段耗时（秒） */
  profileTimingsSec?: Record<string, number>
  /** 仅含 all 或单店片段时为 true */
  viewsPartial?: boolean
  jsonViewsMode?: string
  requestedStoreId?: number
}

/** v4：KPI 与线上 COUNT(*) open_date>since、status='Active' 对账 SQL 一致 */
export const TREND_NEW_LISTING_CACHE_KEY = 'asinPerformances.v9.trendNewListingJson'
/** 超过此大小的 JSON 在 Worker 中 parse，减轻主线程卡顿 */
export const TREND_NL_JSON_WORKER_MIN_LEN = 48_000

export async function parseTrendNewListingJsonText(text: string): Promise<TrendNewListingJsonPayload> {
  if (text.length < TREND_NL_JSON_WORKER_MIN_LEN) {
    return JSON.parse(text) as TrendNewListingJsonPayload
  }
  const Factory = NlJsonWorker as unknown as { new (): Worker }
  return new Promise((resolve, reject) => {
    const w = new Factory()
    w.onmessage = (ev: MessageEvent<{ ok: boolean; data?: unknown; error?: string }>) => {
      w.terminate()
      if (ev.data.ok && ev.data.data != null) resolve(ev.data.data as TrendNewListingJsonPayload)
      else reject(new Error(ev.data.error || 'JSON 解析失败'))
    }
    w.onerror = () => {
      w.terminate()
      reject(new Error('JSON Worker 错误'))
    }
    w.postMessage(text)
  })
}

/** 堆叠图 tooltip 仅展示相对悬停 session_date 往前 30 天内的批次 */
export const NL_STACK_TOOLTIP_COHORT_LOOKBACK_DAYS = 30
export const NL_ZERO_SESSION_HIGHLIGHT_EXCLUDE_RECENT_DAYS = 2
export const NL_COHORT_COLLAPSE_NEW_ASIN_THRESHOLD = 500

export function nlStackTooltipYmdAddDays(ymd: string, deltaDays: number): string | null {
  const head = String(ymd || '').slice(0, 10)
  if (!/^\d{4}-\d{2}-\d{2}$/.test(head)) return null
  const [y, m, d] = head.split('-').map(Number)
  const t = Date.UTC(y, m - 1, d) + deltaDays * 86400000
  const u = new Date(t)
  const yy = u.getUTCFullYear()
  const mm = String(u.getUTCMonth() + 1).padStart(2, '0')
  const dd = String(u.getUTCDate()).padStart(2, '0')
  return `${yy}-${mm}-${dd}`
}

export function nlStackTooltipParseBatchYmd(label: string): string | null {
  const m = /批次\s+(\d{4}-\d{2}-\d{2})/.exec(String(label || ''))
  return m ? m[1] : null
}

export function nlStackTooltipCohortInWindow(sessionYmd: string, cohortYmd: string): boolean {
  const s = String(sessionYmd || '').slice(0, 10)
  const minY = nlStackTooltipYmdAddDays(s, -NL_STACK_TOOLTIP_COHORT_LOOKBACK_DAYS)
  const c = String(cohortYmd || '').slice(0, 10)
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s) || !/^\d{4}-\d{2}-\d{2}$/.test(c) || !minY) return true
  return c >= minY && c <= s
}

export function nlGetOrCreateExternalTooltipEl(): HTMLDivElement | null {
  if (typeof document === 'undefined') return null
  let el = document.getElementById('trend-new-listing-chart-tooltip') as HTMLDivElement | null
  if (el) return el
  el = document.createElement('div')
  el.id = 'trend-new-listing-chart-tooltip'
  el.className = 'trend-new-listing-chart-tooltip'
  document.body.appendChild(el)
  return el
}

export function nlRenderExternalTooltip(
  context: any,
  labels: string[],
  lineTotals: number[],
) {
  const tooltip = context?.tooltip
  const tooltipEl = nlGetOrCreateExternalTooltipEl()
  if (!tooltip || !tooltipEl) return

  if (tooltip.opacity === 0) {
    tooltipEl.style.opacity = '0'
    tooltipEl.style.pointerEvents = 'none'
    return
  }

  const dataIndex = Number(tooltip.dataPoints?.[0]?.dataIndex ?? -1)
  const title = String(tooltip.title?.[0] ?? labels[dataIndex] ?? '')
  const total = dataIndex >= 0 ? Number(lineTotals[dataIndex] ?? 0) : 0
  const bodyItems = Array.isArray(tooltip.dataPoints)
    ? tooltip.dataPoints
        .map((point: any, idx: number) => ({ point, idx }))
        .filter((entry: { point: any; idx: number }) => String(entry.point?.dataset?.label ?? '') !== '当日 sessions 合计')
    : []

  const rowsHtml = bodyItems
    .map(({ point, idx }: any) => {
      const label = String(point?.dataset?.label ?? '')
      const value = Number(point?.raw ?? point?.parsed?.y ?? 0).toLocaleString('zh-CN')
      const color = String(tooltip.labelColors?.[idx]?.backgroundColor ?? point?.dataset?.backgroundColor ?? '#94a3b8')
      return `
        <div class="trend-new-listing-chart-tooltip-row">
          <span class="trend-new-listing-chart-tooltip-label">
            <span class="trend-new-listing-chart-tooltip-swatch" style="background:${color}"></span>
            ${label}
          </span>
          <span class="trend-new-listing-chart-tooltip-value">${value}</span>
        </div>
      `
    })
    .join('')

  tooltipEl.innerHTML = `
    <div class="trend-new-listing-chart-tooltip-title">${title}</div>
    <div class="trend-new-listing-chart-tooltip-total">当日 sessions 合计：${total.toLocaleString('zh-CN')}</div>
    <div class="trend-new-listing-chart-tooltip-list">${rowsHtml || '<div class="trend-new-listing-chart-tooltip-empty">无批次数据</div>'}</div>
  `

  const caretX = Number(tooltip.caretX ?? 0)
  const caretY = Number(tooltip.caretY ?? 0)
  const canvasRect = context.chart.canvas.getBoundingClientRect()
  const left = Math.min(window.innerWidth - 320, Math.max(12, canvasRect.left + caretX + 18))
  const top = Math.min(window.innerHeight - 24, Math.max(12, canvasRect.top + caretY - 12))

  tooltipEl.style.opacity = '1'
  tooltipEl.style.pointerEvents = 'none'
  tooltipEl.style.left = `${left}px`
  tooltipEl.style.top = `${top}px`
}

export function nlCurrentPstYmd(now: Date = new Date()): string {
  const fmt = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Los_Angeles',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  })
  const parts = fmt.formatToParts(now)
  const year = parts.find((p) => p.type === 'year')?.value ?? '1970'
  const month = parts.find((p) => p.type === 'month')?.value ?? '01'
  const day = parts.find((p) => p.type === 'day')?.value ?? '01'
  return `${year}-${month}-${day}`
}

/** 默认「最近 35 天」缓存新鲜度：期望批次最大日 = PST 当日−1（与后端默认 session_end 上界一致） */
export const NL_DEFAULT_CACHE_FRESH_MAX_LAG_DAYS = 1

/** 期望缓存中批次最大上新日（及默认 session 窗口上界）：PST 当日往前 NL_DEFAULT_CACHE_FRESH_MAX_LAG_DAYS 个日历日 */
export function nlExpectedDefaultCacheMaxDataYmd(now: Date = new Date()): string {
  const pstToday = nlCurrentPstYmd(now)
  return nlStackTooltipYmdAddDays(pstToday, -NL_DEFAULT_CACHE_FRESH_MAX_LAG_DAYS) ?? pstToday
}

/** 来自缓存 JSON：views.all.cohortTable 中最大 cohortDate；无行时用 session 区间元数据 */
export function nlMaxDataDateFromCachedPayload(data: TrendNewListingJsonPayload): string | null {
  const rows = data.views?.all?.cohortTable
  let max = ''
  if (Array.isArray(rows)) {
    for (const row of rows) {
      const cd = String(row?.cohortDate ?? '').slice(0, 10)
      if (/^\d{4}-\d{2}-\d{2}$/.test(cd) && cd > max) max = cd
    }
  }
  if (max) return max
  const meta = String(data.sessionRequestedEnd ?? data.sessionChartEnd ?? '').slice(0, 10)
  return /^\d{4}-\d{2}-\d{2}$/.test(meta) ? meta : null
}

/** 解析 generatedAt 为 YYYY-MM-DD（用于与 PST 今日比较；服务端多为 date 字符串） */
export function nlGeneratedAtPstYmd(data: TrendNewListingJsonPayload): string | null {
  const raw = data.generatedAt
  if (raw == null || raw === '') return null
  const s = String(raw).trim()
  const head = s.slice(0, 10)
  if (/^\d{4}-\d{2}-\d{2}$/.test(head)) return head
  const d = new Date(s)
  if (Number.isNaN(d.getTime())) return null
  return nlCurrentPstYmd(d)
}

/** 本地默认区间缓存：批次最大日已达 PST 昨日；若 DB 滞后则当天 PST 内仍接受缓存避免无限刷新 */
export function isTrendNewListingDefaultCacheFresh(data: TrendNewListingJsonPayload | null): boolean {
  if (!data) return false
  const expected = nlExpectedDefaultCacheMaxDataYmd()
  const maxData = nlMaxDataDateFromCachedPayload(data)
  if (!maxData) return false
  if (maxData === expected) return true
  const genPst = nlGeneratedAtPstYmd(data)
  const pstToday = nlCurrentPstYmd()
  if (maxData < expected && genPst === pstToday) return true
  return false
}

export function nlDiffCalendarDays(startYmd: string, endYmd: string): number | null {
  const s = String(startYmd || '').slice(0, 10)
  const e = String(endYmd || '').slice(0, 10)
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s) || !/^\d{4}-\d{2}-\d{2}$/.test(e)) return null
  const [sy, sm, sd] = s.split('-').map(Number)
  const [ey, em, ed] = e.split('-').map(Number)
  const start = Date.UTC(sy, sm - 1, sd)
  const end = Date.UTC(ey, em - 1, ed)
  return Math.floor((end - start) / 86400000)
}

export function nlShouldHighlightZeroSessionCell(
  cohortYmd: string,
  dayIndex: number,
  sessionValue: number,
  latestSessionYmd: string,
): boolean {
  if (Number(sessionValue ?? 0) !== 0) return false
  const cutoffYmd = nlStackTooltipYmdAddDays(latestSessionYmd, -NL_ZERO_SESSION_HIGHLIGHT_EXCLUDE_RECENT_DAYS)
  if (!cutoffYmd) return false
  const cohortHead = String(cohortYmd || '').slice(0, 10)
  if (!/^\d{4}-\d{2}-\d{2}$/.test(cohortHead) || cohortHead > cutoffYmd) return false
  const diffDays = nlDiffCalendarDays(cohortHead, cutoffYmd)
  if (diffDays == null || diffDays < 0) return false
  return dayIndex <= diffDays
}

export const NL_COHORT_ROW_HEIGHT_PX = 34

export type TrendNlCohortRow = NonNullable<TrendNewListingViewPayload['cohortTable']>[number]

export function nlComputeCohortSessionPerAsin(daySessions: number[], cohortTrackDays: number, newAsin: number): number {
  if (!Number.isFinite(newAsin) || newAsin <= 0 || !Number.isFinite(cohortTrackDays) || cohortTrackDays <= 0) return 0
  const totalSessions = Array.from({ length: cohortTrackDays }, (_, i) => Number(daySessions[i] ?? 0)).reduce(
    (acc, value) => acc + value,
    0,
  )
  return totalSessions / newAsin
}

export function nlSumCohortDaySessions(daySessions: number[], cohortTrackDays: number): number {
  let sum = 0
  for (let i = 0; i < cohortTrackDays; i++) sum += Number(daySessions[i] ?? 0)
  return sum
}

type TrendNlCohortVirtualRowHandlers = {
  nlPopoverOpenFromCell: (
    e: React.MouseEvent<HTMLTableCellElement>,
    cellKey: string,
    title: string,
    rawItems: Array<{ asin: string; storeId: number; sessions: number }> | undefined,
    cellTotal: number,
  ) => void
  nlPopoverPinFromCell: (
    e: React.MouseEvent<HTMLTableCellElement>,
    cellKey: string,
    title: string,
    rawItems: Array<{ asin: string; storeId: number; sessions: number }> | undefined,
    cellTotal: number,
  ) => void
  nlPopoverScheduleClose: () => void
  nlPopoverCancelClose: () => void
}

/** 单行 memo：避免虚拟滚动时重复聚合 daySessionAsins（行数×列数大时 CPU 高） */
const TrendNlCohortVirtualRow = memo(function TrendNlCohortVirtualRow({
  row,
  cohortTrackDays,
  latestSessionYmd,
  nlPopoverOpenFromCell,
  nlPopoverPinFromCell,
  nlPopoverScheduleClose,
  nlPopoverCancelClose,
}: {
  row: TrendNlCohortRow
  cohortTrackDays: number
  latestSessionYmd: string
} & TrendNlCohortVirtualRowHandlers) {
  const cd = String(row?.cohortDate ?? '')
  const listingCounts = useMemo(() => nlResolveListingCounts(row), [row])
  const daySessions = useMemo(
    () => (Array.isArray(row?.daySessions) ? row.daySessions.map((x) => Number(x ?? 0)) : []),
    [row?.daySessions],
  )
  const totalSessions = useMemo(
    () => nlSumCohortDaySessions(daySessions, cohortTrackDays),
    [cohortTrackDays, daySessions],
  )
  const sessionPerAsin = useMemo(
    () => nlComputeCohortSessionPerAsin(daySessions, cohortTrackDays, listingCounts.newCount),
    [cohortTrackDays, daySessions, listingCounts.newCount],
  )
  const totalItems = useMemo(
    () => nlAggregateTotalSessionItems(row?.daySessionAsins, cohortTrackDays),
    [cohortTrackDays, row?.daySessionAsins],
  )
  const dayAsins = row?.daySessionAsins

  return (
    <tr>
      <td className="is-sticky-col is-sticky-col--1">{cd || '–'}</td>
      <td className="is-sticky-col is-sticky-col--2">
        {formatListingMixCell(listingCounts.newCount, listingCounts.refurbishedCount)}
      </td>
      <td
        className="is-sticky-col is-sticky-col--3 trend-nl-hoverable-cell"
        onMouseEnter={(e) => {
          nlPopoverCancelClose()
          nlPopoverOpenFromCell(e, `${cd}-total`, 'ASIN · 店铺 ID · 累计 sessions(>0)', totalItems, totalSessions)
        }}
        onMouseLeave={nlPopoverScheduleClose}
        onClick={(e) =>
          nlPopoverPinFromCell(e, `${cd}-total`, 'ASIN · 店铺 ID · 累计 sessions(>0)', totalItems, totalSessions)
        }
      >
        {totalSessions.toLocaleString('zh-CN')}
      </td>
      <td>{formatPermyriadUntilVisible(sessionPerAsin, 2, 4)}</td>
      {Array.from({ length: cohortTrackDays }, (_, i) => {
        const value = Number(daySessions[i] ?? 0)
        const highlight = nlShouldHighlightZeroSessionCell(cd, i, value, latestSessionYmd)
        return (
          <td
            key={`${cd}-s-${i}`}
            className={`${highlight ? 'trend-new-listing-zero-alert ' : ''}trend-nl-hoverable-cell`}
            onMouseEnter={(e) => {
              nlPopoverCancelClose()
              nlPopoverOpenFromCell(
                e,
                `${cd}-day-${i}`,
                `ASIN · 店铺 ID · 第${i + 1}天 sessions`,
                Array.isArray(dayAsins) ? dayAsins[i] : undefined,
                value,
              )
            }}
            onMouseLeave={nlPopoverScheduleClose}
            onClick={(e) =>
              nlPopoverPinFromCell(
                e,
                `${cd}-day-${i}`,
                `ASIN · 店铺 ID · 第${i + 1}天 sessions`,
                Array.isArray(dayAsins) ? dayAsins[i] : undefined,
                value,
              )
            }
          >
            {value.toLocaleString('zh-CN')}
          </td>
        )
      })}
    </tr>
  )
})

export type NlDaySessionPopoverAnchor = {
  cellKey: string
  title: string
  left: number
  top: number
  items: Array<{ asin: string; storeId: number; sessions: number }>
  cellTotal: number
  pinned?: boolean
}

/** 固定定位浮动层（替代原生 title：可避免服务端旧缓存 JSON、多行 title 不显示等问题） */
export function NlDaySessionPopoverPortal({
  anchor,
  cancelClose,
  scheduleClose,
  onUnpin,
  hasOrder,
}: {
  anchor: NlDaySessionPopoverAnchor | null
  cancelClose: () => void
  scheduleClose: () => void
  onUnpin: () => void
  hasOrder: (asin: string, storeId: number) => boolean
}) {
  if (anchor == null || typeof document === 'undefined') return null
  const { left, top, items, cellTotal, pinned } = anchor
  const stale = cellTotal > 0 && items.length === 0
  const detailSum = items.reduce((acc, x) => acc + Number(x.sessions || 0), 0)
  const diff = Number(cellTotal || 0) - detailSum

  return createPortal(
    <div
      className="trend-nl-day-popover"
      style={{
        position: 'fixed',
        left,
        top,
        transform: 'translateX(-50%)',
        zIndex: 10050,
      }}
      role="tooltip"
      onMouseEnter={cancelClose}
      onMouseLeave={scheduleClose}
    >
      {stale ? (
        <div className="trend-nl-day-popover-stale">
          <div className="trend-nl-day-popover-stale-title">
            当日合计 {cellTotal.toLocaleString('zh-CN')} sessions
          </div>
          <p className="trend-nl-day-popover-stale-hint">
            未返回 ASIN 明细（常见于浏览器或服务端缓存了旧 JSON）。请点击「刷新数据」或 URL 加{' '}
            <code className="trend-new-listing-code">?refresh=1</code>
            ，请求加 <code className="trend-new-listing-code">nocache=1</code>。
          </p>
        </div>
      ) : (
        <>
          <div className="trend-nl-day-popover-head">
            <span>{anchor.title}</span>
            {pinned ? (
              <button
                type="button"
                className="trend-nl-day-popover-unpin-btn"
                onClick={onUnpin}
                title="取消固定"
              >
                取消固定
              </button>
            ) : null}
          </div>
          <div className="trend-nl-day-popover-meta">
            明细合计 {detailSum.toLocaleString('zh-CN')} / 单元格 {Number(cellTotal || 0).toLocaleString('zh-CN')}
            {diff !== 0 ? `（差值 ${diff.toLocaleString('zh-CN')}）` : ''}
          </div>
          <div className="trend-nl-day-popover-body">
            {items.map((x, idx) => (
              <div key={`${x.asin}-${x.storeId}-${idx}`} className="trend-nl-day-popover-row">
                <span className={`trend-nl-day-popover-asin${hasOrder(x.asin, x.storeId) ? ' is-has-order' : ''}`}>
                  {x.asin}
                </span>
                <span className="trend-nl-day-popover-store">{x.storeId}</span>
                <span className="trend-nl-day-popover-sessions">{x.sessions.toLocaleString('zh-CN')}</span>
              </div>
            ))}
          </div>
        </>
      )}
    </div>,
    document.body,
  )
}

export function TrendNewListingVirtualCohortTable({
  cohortTable,
  cohortTrackDays,
  latestSessionYmd,
  nlPopoverOpenFromCell,
  nlPopoverPinFromCell,
  nlPopoverScheduleClose,
  nlPopoverCancelClose,
  nlPopoverCloseNow,
}: {
  cohortTable: TrendNlCohortRow[]
  cohortTrackDays: number
  latestSessionYmd: string
  nlPopoverOpenFromCell: (
    e: React.MouseEvent<HTMLTableCellElement>,
    cellKey: string,
    title: string,
    rawItems: Array<{ asin: string; storeId: number; sessions: number }> | undefined,
    cellTotal: number,
  ) => void
  nlPopoverPinFromCell: (
    e: React.MouseEvent<HTMLTableCellElement>,
    cellKey: string,
    title: string,
    rawItems: Array<{ asin: string; storeId: number; sessions: number }> | undefined,
    cellTotal: number,
  ) => void
  nlPopoverScheduleClose: () => void
  nlPopoverCancelClose: () => void
  nlPopoverCloseNow: () => void
}) {
  const scrollRef = useRef<HTMLDivElement>(null)
  const [scrollTop, setScrollTop] = useState(0)
  const [viewH, setViewH] = useState(400)

  useLayoutEffect(() => {
    const el = scrollRef.current
    if (!el) return
    const upd = () => setViewH(Math.max(120, el.clientHeight))
    upd()
    const ro = new ResizeObserver(upd)
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  const overscan = 6
  const start = Math.max(0, Math.floor(scrollTop / NL_COHORT_ROW_HEIGHT_PX) - overscan)
  const visibleCount = Math.max(1, Math.ceil(viewH / NL_COHORT_ROW_HEIGHT_PX) + overscan * 2)
  const end = Math.min(cohortTable.length, start + visibleCount)
  const topPad = start * NL_COHORT_ROW_HEIGHT_PX
  const bottomPad = Math.max(0, cohortTable.length - end) * NL_COHORT_ROW_HEIGHT_PX

  return (
    <div
      ref={scrollRef}
      className="trend-new-listing-table-scroll"
      onScroll={(e) => {
        nlPopoverCloseNow()
        setScrollTop(e.currentTarget.scrollTop)
      }}
    >
      <table className="trend-new-listing-table">
        <thead>
          <tr>
            <th className="is-sticky-col is-sticky-col--1">上新日（PST）</th>
            <th className="is-sticky-col is-sticky-col--2">上新/补录</th>
            <th className="is-sticky-col is-sticky-col--3">总 session 数</th>
            <th>比值</th>
            {Array.from({ length: cohortTrackDays }, (_, i) => (
              <th key={`d${i + 1}`}>{`第${i + 1}天`}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {topPad > 0 ? (
            <tr className="trend-nl-vpad" aria-hidden>
              <td
                colSpan={4 + cohortTrackDays}
                style={{ height: topPad, padding: 0, border: 'none', background: 'transparent' }}
              />
            </tr>
          ) : null}
          {cohortTable.slice(start, end).map((row) => (
            <TrendNlCohortVirtualRow
              key={String(row?.cohortDate ?? '') || `r-${start}`}
              row={row}
              cohortTrackDays={cohortTrackDays}
              latestSessionYmd={latestSessionYmd}
              nlPopoverOpenFromCell={nlPopoverOpenFromCell}
              nlPopoverPinFromCell={nlPopoverPinFromCell}
              nlPopoverScheduleClose={nlPopoverScheduleClose}
              nlPopoverCancelClose={nlPopoverCancelClose}
            />
          ))}
          {bottomPad > 0 ? (
            <tr className="trend-nl-vpad" aria-hidden>
              <td
                colSpan={4 + cohortTrackDays}
                style={{ height: bottomPad, padding: 0, border: 'none', background: 'transparent' }}
              />
            </tr>
          ) : null}
        </tbody>
      </table>
    </div>
  )
}

export function readTrendNewListingCache(): TrendNewListingJsonPayload | null {
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

export function isTrendNewListingDefaultRange(data: TrendNewListingJsonPayload | null): boolean {
  if (!data) return false
  const endYmd = String(data.sessionRequestedEnd ?? data.sessionChartEnd ?? '').slice(0, 10)
  const expectedStart = nlStackTooltipYmdAddDays(endYmd, -34)
  const listingSince = String(data.listingSince ?? '').slice(0, 10)
  const requestedStart = String(data.sessionRequestedStart ?? data.listingSince ?? '').slice(0, 10)
  return Boolean(expectedStart && listingSince === expectedStart && requestedStart === expectedStart)
}

export function writeTrendNewListingCache(data: TrendNewListingJsonPayload): void {
  try {
    localStorage.setItem(TREND_NEW_LISTING_CACHE_KEY, JSON.stringify(data))
  } catch {
    /* quota / 隐私模式等 */
  }
}

/** 与后端 /api/trend/new-listing 一致：start_date + session_end 表示自定义区间（用于导出脚本 Playwright 带参打开）。 */
export function trendNewListingDateRangeFromSearch(): { start: string | null; end: string | null } {
  if (typeof window === 'undefined') return { start: null, end: null }
  try {
    const sp = new URLSearchParams(window.location.search)
    const s = (sp.get('start_date') || '').slice(0, 10)
    const e = (sp.get('session_end') || '').trim().slice(0, 10)
    if (s && e) return { start: s, end: e }
  } catch {
    /* ignore */
  }
  return { start: null, end: null }
}

/** 默认先用缓存首屏展示，同时后台刷新；?refresh=1 或 ?nocache=1 强制走网络且不读本地缓存，且会为 all 请求附加 nocache=1（后端全量重算，耗时常达数分钟）。 */
export function getTrendNewListingBoot(): { payload: TrendNewListingJsonPayload | null; useCacheOnly: boolean } {
  if (typeof window === 'undefined') return { payload: null, useCacheOnly: false }
  try {
    const sp = new URLSearchParams(window.location.search)
    if (sp.get('refresh') === '1' || sp.get('nocache') === '1') {
      return { payload: null, useCacheOnly: false }
    }
    // 自定义起止与「最近 35 天」缓存策略不同，不走本地默认区间缓存
    const urlRange = trendNewListingDateRangeFromSearch()
    if (urlRange.start && urlRange.end) {
      return { payload: null, useCacheOnly: false }
    }
    const cached = readTrendNewListingCache()
    if (
      cached &&
      isTrendNewListingDefaultRange(cached) &&
      isTrendNewListingDefaultCacheFresh(cached)
    ) {
      return { payload: cached, useCacheOnly: true }
    }
  } catch {
    /* ignore */
  }
  return { payload: null, useCacheOnly: false }
}

/** 地址栏 `?profile=1`：请求带 `profile=1` 并在控制台输出前后端分段耗时 */
export function trendNewListingProfileFromSearch(): boolean {
  if (typeof window === 'undefined') return false
  try {
    return new URLSearchParams(window.location.search).get('profile') === '1'
  } catch {
    return false
  }
}


export function nlAggregateTotalSessionItems(
  daySessionAsins: Array<Array<{ asin: string; storeId: number; sessions: number }>> | undefined,
  cohortTrackDays: number,
): Array<{ asin: string; storeId: number; sessions: number }> {
  if (!Array.isArray(daySessionAsins) || cohortTrackDays <= 0) return []
  const acc = new Map<string, { asin: string; storeId: number; sessions: number }>()
  for (let i = 0; i < cohortTrackDays; i += 1) {
    const items = Array.isArray(daySessionAsins[i]) ? daySessionAsins[i] : []
    for (const it of items) {
      const asin = String(it?.asin ?? '').trim()
      const storeId = Number(it?.storeId ?? 0)
      const sessions = Number(it?.sessions ?? 0)
      if (!asin || !Number.isFinite(storeId) || !Number.isFinite(sessions) || sessions <= 0) continue
      const key = `${asin}||${storeId}`
      const prev = acc.get(key)
      if (prev) prev.sessions += sessions
      else acc.set(key, { asin, storeId, sessions })
    }
  }
  return Array.from(acc.values()).sort((a, b) => (b.sessions - a.sessions) || a.asin.localeCompare(b.asin))
}

export function formatListingMixCell(newCount: number | null | undefined, refurbCount: number | null | undefined): string {
  const newValue = Number(newCount)
  const refurbValue = Number(refurbCount)
  if (!Number.isFinite(newValue) || !Number.isFinite(refurbValue)) return '–'
  return `${newValue.toLocaleString('zh-CN')} / ${refurbValue.toLocaleString('zh-CN')}`
}

export function nlResolveListingCounts(row: TrendNlCohortRow): {
  newCount: number
  refurbishedCount: number
} {
  const totalNewAsin = Math.max(0, Number(row?.newAsin ?? 0))
  const listingNewRaw = Number(row?.listingNewCount ?? NaN)
  const listingRefurbRaw = Number(row?.listingRefurbishedCount ?? NaN)
  const hasMix = Number.isFinite(listingNewRaw) && Number.isFinite(listingRefurbRaw)
  if (!hasMix) {
    return { newCount: totalNewAsin, refurbishedCount: 0 }
  }
  const newCount = Math.max(0, listingNewRaw)
  const refurbishedCount = Math.max(0, listingRefurbRaw)
  // 旧缓存/后端未回填 mix 时可能出现 0/0，但 totalNewAsin>0；回退到总上新口径避免“0/0”误导
  if (newCount + refurbishedCount <= 0 && totalNewAsin > 0) {
    return { newCount: totalNewAsin, refurbishedCount: 0 }
  }
  return { newCount, refurbishedCount }
}

export function parseOptionalInt(v: string): number | null {
  const raw = v.trim()
  if (!raw) return null
  const parsed = Number.parseInt(raw, 10)
  return Number.isNaN(parsed) ? null : parsed
}

/** 与 backend `_listing_tracking_week_no` 一致：周日至周六为一周，week_no = 该周周六所在 ISO 年周（YYYYWW）。 */
export function listingTrackingWeekNo(d: Date): number {
  const pyWd = d.getDay() === 0 ? 6 : d.getDay() - 1
  const daysSinceSunday = (pyWd + 1) % 7
  const ws = new Date(d.getFullYear(), d.getMonth(), d.getDate() - daysSinceSunday)
  const we = new Date(ws.getFullYear(), ws.getMonth(), ws.getDate() + 6)
  const { isoYear, isoWeek } = isoYearWeekForDate(we)
  return isoYear * 100 + isoWeek
}

/** Python `datetime.isocalendar()` 等价（本地日历日）。 */
export function isoYearWeekForDate(d: Date): { isoYear: number; isoWeek: number } {
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

export const TREND_WEEK_NO_MIN = 202515

/** 自 minWeekNo 起至 endDate 当周止的所有 week_no（与 listing_tracking 规则一致）。 */
export function buildListingTrackingWeekRange(minWeekNo: number, endDate: Date): number[] {
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





export type TrendFilterState = {
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

export const EMPTY_TREND_FILTERS: TrendFilterState = {
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

export type TrendLineDef = {
  key: keyof TrendWeekPoint
  label: string
  color: string
  formatter?: (value: number) => string
}

export function TrendBarOverviewCard({ data }: { data: TrendWeekPoint[] }) {
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

export function TrendChartFigure({
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

export function TrendLineChartCard({
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
export function TrendNewListingEmbeddedPage() {
  type TrendHttpError = Error & { status?: number; retryAfterSec?: number }
  const isTrendHttpStatus = (e: unknown, status: number): e is TrendHttpError =>
    typeof e === 'object' && e !== null && Number((e as { status?: number }).status) === status
  const waitWithAbort = (ms: number, signal?: AbortSignal): Promise<void> =>
    new Promise((resolve, reject) => {
      if (signal?.aborted) {
        reject(new DOMException('Aborted', 'AbortError'))
        return
      }
      const timer = window.setTimeout(() => {
        cleanup()
        resolve()
      }, Math.max(0, ms))
      const onAbort = () => {
        cleanup()
        reject(new DOMException('Aborted', 'AbortError'))
      }
      const cleanup = () => {
        window.clearTimeout(timer)
        if (signal) signal.removeEventListener('abort', onAbort)
      }
      if (signal) signal.addEventListener('abort', onAbort, { once: true })
    })

  const boot = useMemo(() => getTrendNewListingBoot(), [])
  const nlUrlRange = useMemo(() => trendNewListingDateRangeFromSearch(), [])
  /** 地址栏 ?refresh=1 / ?nocache=1：首屏不走本地缓存，且请求带 nocache=1 触发后端全量重算，耗时常明显变长 */
  const nlForcedNetwork = useMemo(() => {
    if (typeof window === 'undefined') return false
    try {
      const sp = new URLSearchParams(window.location.search)
      return sp.get('refresh') === '1' || sp.get('nocache') === '1'
    } catch {
      return false
    }
  }, [])
  const [payload, setPayload] = useState<TrendNewListingJsonPayload | null>(boot.payload)
  const [fromCache, setFromCache] = useState(boot.useCacheOnly)
  const [storeKey, setStoreKey] = useState<string>('all')
  const [displayStoreKey, setDisplayStoreKey] = useState<string>('all')
  const [startDateInput, setStartDateInput] = useState<string>(
    nlUrlRange.start ?? boot.payload?.listingSince ?? '',
  )
  const [endDateInput, setEndDateInput] = useState<string>(
    nlUrlRange.end ??
      boot.payload?.sessionRequestedEnd ??
      boot.payload?.sessionChartEnd ??
      '',
  )
  const [useDefaultDateRange, setUseDefaultDateRange] = useState<boolean>(
    nlUrlRange.start && nlUrlRange.end
      ? false
      : boot.payload
        ? isTrendNewListingDefaultRange(boot.payload)
        : true,
  )
  const [err, setErr] = useState<string | null>(null)
  const [waitingForBuildRetry, setWaitingForBuildRetry] = useState(false)
  const [refreshing, setRefreshing] = useState(false)
  const [storeViewLoading, setStoreViewLoading] = useState<string | null>(null)
  const [chartMountReady, setChartMountReady] = useState(false)
  const [hideSmallCohorts, setHideSmallCohorts] = useState(false)
  const nlProfilePendingRef = useRef<{
    t0: number
    afterParse: number
    fetchToHeadersMs: number
    readBodyMs: number
    parseMs: number
    server?: Record<string, number>
  } | null>(null)
  const nlShellAbortRef = useRef<AbortController | null>(null)
  const nlStoreAbortRef = useRef<AbortController | null>(null)
  /** 与 payload 同步，供空闲预取回调读取最新 views */
  const nlPayloadRef = useRef<TrendNewListingJsonPayload | null>(payload)
  /** 单店 JSON 正在请求中（用户切换或预取），避免重复打接口 */
  const nlStoreInFlightRef = useRef<Set<number>>(new Set())
  /** json_views=all 同 URL in-flight 合并：含后台 merge 刷新，减轻 StrictMode / 双 effect 对 heavy 的重复请求 */
  const nlJsonAllInflightRef = useRef<Map<string, Promise<void>>>(new Map())
  /** 取消上一轮预取链（idle 回调 + 当前预取请求） */
  const nlPrefetchGenRef = useRef(0)
  const nlPrefetchRicRef = useRef<number | null>(null)
  const nlPrefetchRicIsNativeRef = useRef(false)

  const [nlDayPopover, setNlDayPopover] = useState<NlDaySessionPopoverAnchor | null>(null)
  const nlPopTimerRef = useRef<number | null>(null)
  const nlDayPopoverRef = useRef<NlDaySessionPopoverAnchor | null>(null)
  const nlHasOrderRef = useRef<Map<string, boolean>>(new Map())
  const nlOrderFetchInFlightRef = useRef<AbortController | null>(null)
  const [nlOrderCacheEpoch, setNlOrderCacheEpoch] = useState(0)

  /** 持久化缓存：曾判定有订单的 asin||store_id 载入内存，减少 POST */
  useEffect(() => {
    const s = loadNlOrderPositiveKeySet()
    if (!s.size) return
    for (const k of s) nlHasOrderRef.current.set(k, true)
    setNlOrderCacheEpoch((x) => x + 1)
  }, [])

  useEffect(() => {
    nlDayPopoverRef.current = nlDayPopover
  }, [nlDayPopover])

  const nlHasOrder = useCallback(
    (asin: string, storeId: number) => {
      const key = `${String(asin || '').trim()}||${Number(storeId)}`
      return Boolean(nlHasOrderRef.current.get(key))
    },
    [nlOrderCacheEpoch],
  )

  useEffect(() => {
    const p = nlDayPopover
    if (!p || !Array.isArray(p.items) || p.items.length === 0) return
    // 仅在弹层打开后拉取一次缺失项；固定/非固定都需要高亮（复制时也希望看到）
    // 大区间查询时后端负载高；限制弹层触发的 order-flags 请求规模，避免与主报表请求抢 online pool。
    if (p.items.length > 450) return
    const uniq: Array<{ asin: string; store_id: number }> = []
    const seen = new Set<string>()
    for (const it of p.items) {
      const asin = String(it?.asin ?? '').trim()
      const sid = Number(it?.storeId ?? NaN)
      if (!asin || !Number.isFinite(sid)) continue
      const k = `${asin}||${sid}`
      if (seen.has(k)) continue
      seen.add(k)
      /** 仅跳过已确认有订单（内存 true）；无订单(false)下次仍审查 */
      if (nlHasOrderRef.current.get(k) === true) continue
      uniq.push({ asin, store_id: sid })
    }
    if (!uniq.length) return
    /** 与主报表联动：每日最多 2 次 order-flags POST；配额用尽时仅用缓存高亮 */
    if (remainingNlOrderFlagPostsToday() <= 0) return
    // 限制单次请求大小
    const batch = uniq.slice(0, 400)
    // 避免并发打多次（鼠标移动/反复开关弹层）；在上一请求完成前不再发起新请求
    if (nlOrderFetchInFlightRef.current) return
    const ac = new AbortController()
    nlOrderFetchInFlightRef.current = ac
    fetch('/api/trend/new-listing/order-flags', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ items: batch }),
      signal: ac.signal,
    })
      .then(async (res) => {
        const text = await res.text()
        if (!res.ok) throw new Error(text || `HTTP ${res.status}`)
        return JSON.parse(text) as { has_orders?: Array<{ asin: string; store_id: number }> }
      })
      .then((data) => {
        const rows = Array.isArray(data?.has_orders) ? data.has_orders : []
        // 与本批请求对齐：先视为无订单，再把服务端返回的有订单标 true（false 不持久化，下次仍会再审）
        for (const it of batch) {
          nlHasOrderRef.current.set(`${it.asin}||${it.store_id}`, false)
        }
        const positiveKeys: string[] = []
        for (const r of rows) {
          const asin = String(r?.asin ?? '').trim()
          const sid = Number((r as any)?.store_id ?? NaN)
          if (!asin || !Number.isFinite(sid)) continue
          const kk = `${asin}||${sid}`
          nlHasOrderRef.current.set(kk, true)
          positiveKeys.push(kk)
        }
        if (positiveKeys.length) persistNlOrderPositiveKeys(positiveKeys)
        incrementNlOrderFlagsDailyCount()
        setNlOrderCacheEpoch((x) => x + 1)
      })
      .catch((e) => {
        if (typeof e === 'object' && e !== null && (e as any).name === 'AbortError') return
        // 静默失败：不影响弹层展示
      })
      .finally(() => {
        // 允许下一次触发
        if (nlOrderFetchInFlightRef.current === ac) nlOrderFetchInFlightRef.current = null
      })
  }, [nlDayPopover])

  const nlPopoverCancelClose = useCallback(() => {
    if (nlPopTimerRef.current != null) {
      window.clearTimeout(nlPopTimerRef.current)
      nlPopTimerRef.current = null
    }
  }, [])

  const nlPopoverScheduleClose = useCallback(() => {
    if (nlDayPopoverRef.current?.pinned) return
    nlPopoverCancelClose()
    nlPopTimerRef.current = window.setTimeout(() => {
      // 定时器触发时再次读取最新状态，避免“先触发关闭计时、后点击固定”被旧计时器误关闭
      if (nlDayPopoverRef.current?.pinned) {
        nlPopTimerRef.current = null
        return
      }
      setNlDayPopover(null)
      nlPopTimerRef.current = null
    }, 180)
  }, [nlPopoverCancelClose])

  const nlPopoverCloseNow = useCallback(() => {
    nlPopoverCancelClose()
    setNlDayPopover(null)
  }, [nlPopoverCancelClose])

  const nlPopoverOpenFromCell = useCallback(
    (
      e: React.MouseEvent<HTMLTableCellElement>,
      cellKey: string,
      title: string,
      rawItems: Array<{ asin: string; storeId: number; sessions: number }> | undefined,
      cellTotal: number,
      pinned = false,
    ) => {
      nlPopoverCancelClose()
      // 固定态下，鼠标掠过其它格子不改写当前弹层；仅允许点击触发的 pinned 更新
      if (!pinned && nlDayPopoverRef.current?.pinned) return
      const arr = Array.isArray(rawItems) ? rawItems : []
      if (cellTotal <= 0 && arr.length === 0) {
        setNlDayPopover(null)
        return
      }
      const r = e.currentTarget.getBoundingClientRect()
      setNlDayPopover({
        cellKey,
        title,
        left: r.left + r.width / 2,
        top: r.bottom + 8,
        items: [...arr].sort((a, b) => (Number(b.sessions || 0) - Number(a.sessions || 0)) || String(a.asin).localeCompare(String(b.asin))),
        cellTotal,
        pinned,
      })
    },
    [nlPopoverCancelClose],
  )

  const nlPopoverPinFromCell = useCallback(
    (
      e: React.MouseEvent<HTMLTableCellElement>,
      cellKey: string,
      title: string,
      rawItems: Array<{ asin: string; storeId: number; sessions: number }> | undefined,
      cellTotal: number,
    ) => {
      if (nlDayPopover?.pinned && nlDayPopover.cellKey === cellKey) {
        setNlDayPopover(null)
        return
      }
      nlPopoverOpenFromCell(e, cellKey, title, rawItems, cellTotal, true)
    },
    [nlDayPopover?.cellKey, nlDayPopover?.pinned, nlPopoverOpenFromCell],
  )

  useEffect(() => {
    return () => nlPopoverCancelClose()
  }, [nlPopoverCancelClose])

  nlPayloadRef.current = payload

  const storeOptionsEarly = useMemo(
    () => [
      { value: 'all', label: '全部店铺' },
      ...((payload?.storeIds || []).map((id) => ({ value: String(id), label: `店铺 ${id}` })) as Array<{
        value: string
        label: string
      }>),
    ],
    [payload?.storeIds],
  )
  const view: TrendNewListingViewPayload | undefined = useMemo(() => {
    if (!payload?.views) return undefined
    if (storeKey === 'all') return payload.views.all
    if (payload.views[storeKey]) return payload.views[storeKey]
    if (displayStoreKey !== 'all' && payload.views[displayStoreKey]) return payload.views[displayStoreKey]
    return payload.views.all
  }, [displayStoreKey, payload?.views, storeKey])
  const showingFallbackStoreView = Boolean(payload?.views?.all) && storeKey !== 'all' && !payload?.views?.[storeKey]
  const storeOptions = storeOptionsEarly
  const cohortTrackDays = Math.max(1, Number(payload?.cohortTrackDays ?? 30))
  const cohortTable: TrendNlCohortRow[] = useMemo(
    () => (Array.isArray(view?.cohortTable) ? view.cohortTable : []),
    [view?.cohortTable],
  )
  const latestSessionYmd = useMemo(
    () => String(view?.labels?.[view.labels.length - 1] ?? payload?.sessionChartEnd ?? nlCurrentPstYmd()),
    [payload?.sessionChartEnd, view?.labels],
  )
  const filteredCohortTable = useMemo(
    () =>
      hideSmallCohorts
        ? cohortTable.filter((row) => Number(row?.newAsin ?? 0) >= NL_COHORT_COLLAPSE_NEW_ASIN_THRESHOLD)
        : cohortTable,
    [cohortTable, hideSmallCohorts],
  )

  /** 图表语义指纹：仅当真实数据变化才重置 chartMountReady，避免 store 预取合并 payload 时反复闪屏 */
  const nlChartIdentity = useMemo(() => {
    const va =
      storeKey === 'all'
        ? payload?.views?.all
        : payload?.views?.[storeKey]
    if (!va?.labels?.length) return ''
    const labels = va.labels
    const lineLen = va.lineTotal?.length ?? labels.length
    return `${storeKey}|${labels[0]}|${labels[labels.length - 1]}|${labels.length}|${lineLen}|${payload?.generatedAt ?? ''}|${payload?.sessionChartEnd ?? ''}`
  }, [payload?.generatedAt, payload?.sessionChartEnd, payload?.views, storeKey])

  /** 避免每次重渲染重建 massive chart data 对象，减轻 Chart.js 与主线程压力 */
  const nlStackChartPrep = useMemo(() => {
    if (!view?.labels?.length) return null
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
    return { chartData, maxY, lt, barCount: barDatasets.length }
  }, [view])

  const nlChartIdentityCommittedRef = useRef<string>('')

  /** trend/new-listing（json_views=all）成功后批量 order-flags：每日最多 2 次 POST；有订单键持久化 */
  const runNlOrderFlagsBulkAfterTrend = useCallback(
    async (snapshot: TrendNewListingJsonPayload, signal: AbortSignal | undefined) => {
      if (signal?.aborted || typeof window === 'undefined') return

      const candidates = collectNlOrderFlagCandidates(snapshot)
      if (!candidates.length) return

      const positiveLs = loadNlOrderPositiveKeySet()
      const pending: Array<{ asin: string; store_id: number }> = []
      const seen = new Set<string>()
      for (const it of candidates) {
        const k = `${it.asin}||${it.store_id}`
        if (seen.has(k)) continue
        seen.add(k)
        if (positiveLs.has(k)) {
          nlHasOrderRef.current.set(k, true)
          continue
        }
        if (nlHasOrderRef.current.get(k) === true) continue
        pending.push(it)
      }
      if (!pending.length) {
        setNlOrderCacheEpoch((x) => x + 1)
        return
      }

      while (
        pending.length > 0 &&
        readNlOrderFlagsPostsToday() < NL_ORDER_FLAGS_MAX_POSTS_PER_DAY &&
        !signal?.aborted
      ) {
        if (nlOrderFetchInFlightRef.current) return
        const batch = pending.splice(0, NL_ORDER_FLAGS_BATCH_MAX)
        if (!batch.length) break

        const ac = new AbortController()
        nlOrderFetchInFlightRef.current = ac
        try {
          const res = await fetch('/api/trend/new-listing/order-flags', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ items: batch }),
            signal: ac.signal,
          })
          const text = await res.text()
          if (!res.ok) throw new Error(text || `HTTP ${res.status}`)
          const parsed = JSON.parse(text) as {
            has_orders?: Array<{ asin: string; store_id: number }>
          }
          const rows = Array.isArray(parsed?.has_orders) ? parsed.has_orders : []
          const newPositiveKeys: string[] = []
          for (const it of batch) {
            nlHasOrderRef.current.set(`${it.asin}||${it.store_id}`, false)
          }
          for (const r of rows) {
            const asin = String(r?.asin ?? '').trim()
            const sid = Number((r as { store_id?: number }).store_id ?? NaN)
            if (!asin || !Number.isFinite(sid)) continue
            const kk = `${asin}||${sid}`
            nlHasOrderRef.current.set(kk, true)
            newPositiveKeys.push(kk)
          }
          if (newPositiveKeys.length) persistNlOrderPositiveKeys(newPositiveKeys)
          incrementNlOrderFlagsDailyCount()
          setNlOrderCacheEpoch((x) => x + 1)
        } catch (e) {
          if (typeof e === 'object' && e !== null && (e as { name?: string }).name === 'AbortError') return
        } finally {
          if (nlOrderFetchInFlightRef.current === ac) nlOrderFetchInFlightRef.current = null
        }
      }
    },
    [],
  )

  const fetchNewListingJson = useCallback(
    async (opts: {
      skipSync: boolean
      writeCache: boolean
      jsonViews: 'all' | 'full' | 'store'
      storeId?: number
      startDate?: string | null
      endDate?: string | null
      defaultDateRange?: boolean
      syncFormState?: boolean
      signal?: AbortSignal
      mergeIntoExisting?: boolean
      /** 仅首屏壳层在 Dev 下传 true：复用 nlDevAllShellSingleFlight，避免 StrictMode 连打两次 all */
      skipDuplicateDevShell?: boolean
    }) => {
      let serverNocache = false
      let wantProfile = false
      try {
        const sp = new URLSearchParams(window.location.search)
        // ?refresh=1 用于首屏强制重新拉取 all 视图；单店切换/预取若也强制 nocache，会导致每次都绕过服务端缓存而变慢
        serverNocache = (opts.jsonViews === 'all') && (sp.get('refresh') === '1' || sp.get('nocache') === '1')
        wantProfile = sp.get('profile') === '1'
      } catch {
        /* ignore */
      }
      let url = `/api/trend/new-listing?format=json&json_views=${opts.jsonViews}`
      if (opts.jsonViews === 'store' && opts.storeId != null) url += `&store_id=${opts.storeId}`
      if (opts.startDate) url += `&start_date=${encodeURIComponent(opts.startDate)}`
      if (opts.endDate) url += `&session_end=${encodeURIComponent(opts.endDate)}`
      if (opts.skipSync) url += '&skip_sync=false'
      if (serverNocache) url += '&nocache=1'
      if (wantProfile) url += '&profile=1'

      const runFetch = async () => {
        nlProfilePendingRef.current = null
        const t0 = typeof performance !== 'undefined' ? performance.now() : 0
        let res: Response
        try {
          res = await fetch(url, { signal: opts.signal })
        } catch (e: unknown) {
          const msg = e instanceof Error ? e.message : String(e)
          const low = msg.toLowerCase()
          if (
            low.includes('failed to fetch') ||
            low.includes('networkerror') ||
            low.includes('load failed')
          ) {
            throw new Error(
              `网络请求中断（${msg}）。大区间报表可能需数十分钟：若长时间 pending 后失败，请确认已用较新的 Vite 代理超时（vite.config.ts /api），或直连后端端口。`,
            )
          }
          throw e
        }
        const t1 = typeof performance !== 'undefined' ? performance.now() : 0
        const text = await res.text()
        const t2 = typeof performance !== 'undefined' ? performance.now() : 0
        if (!res.ok) {
          let message = `HTTP ${res.status}: ${text.slice(0, 200)}`
          try {
            const j = JSON.parse(text) as { detail?: unknown; error?: unknown }
            if (typeof j?.detail === 'string' && j.detail.trim()) {
              message = j.detail.trim()
            } else if (j?.detail != null) {
              message = `HTTP ${res.status}: ${JSON.stringify(j.detail)}`
            }
            if (typeof j?.error === 'string' && j.error.trim() && !message.includes(j.error.trim())) {
              message = `${j.error.trim()}: ${message}`
            }
          } catch {
            /* 非 JSON 或结构不符时保留原始摘要 */
          }
          const err = new Error(message) as TrendHttpError
          err.status = res.status
          const retryAfterRaw = res.headers.get('Retry-After')
          if (retryAfterRaw) {
            const retryAfter = Number.parseInt(retryAfterRaw, 10)
            if (Number.isFinite(retryAfter) && retryAfter > 0) err.retryAfterSec = retryAfter
          }
          throw err
        }
        const data = await parseTrendNewListingJsonText(text)
        const t3 = typeof performance !== 'undefined' ? performance.now() : 0
        if (wantProfile && typeof performance !== 'undefined') {
          nlProfilePendingRef.current = {
            t0,
            afterParse: t3,
            fetchToHeadersMs: t1 - t0,
            readBodyMs: t2 - t1,
            parseMs: t3 - t2,
            server: data.profileTimingsSec,
          }
        }
        let mergedSnapshot: TrendNewListingJsonPayload = data
        if (opts.mergeIntoExisting) {
          setPayload((prev) => {
            const next: TrendNewListingJsonPayload = {
              ...(prev ?? data),
              ...data,
              views: { ...(prev?.views ?? {}), ...data.views },
              viewsPartial: true,
            }
            mergedSnapshot = next
            nlPayloadRef.current = next
            if (opts.writeCache) writeTrendNewListingCache(next)
            return next
          })
        } else {
          nlStoreInFlightRef.current.clear()
          mergedSnapshot = data
          nlPayloadRef.current = data
          if (opts.writeCache) writeTrendNewListingCache(data)
          setPayload(data)
        }
        if (opts.syncFormState && opts.jsonViews === 'all') {
          setStartDateInput(String(data.listingSince ?? '').slice(0, 10))
          setEndDateInput(String(data.sessionRequestedEnd ?? data.sessionChartEnd ?? '').slice(0, 10))
          setUseDefaultDateRange(Boolean(opts.defaultDateRange) || isTrendNewListingDefaultRange(data))
        }
        setFromCache(false)
        if (opts.jsonViews === 'all' && mergedSnapshot.views?.all) {
          void runNlOrderFlagsBulkAfterTrend(mergedSnapshot, opts.signal)
        }
      }

      const dedupeAll = opts.jsonViews === 'all'
      if (
        dedupeAll &&
        import.meta.env.DEV &&
        opts.skipDuplicateDevShell &&
        nlDevAllShellSingleFlight
      ) {
        await nlDevAllShellSingleFlight
        return
      }
      if (dedupeAll) {
        const inflight = nlJsonAllInflightRef.current.get(url)
        if (inflight) {
          if (opts.signal) {
            await Promise.race([
              inflight,
              new Promise<never>((_, rej) => {
                const s = opts.signal!
                if (s.aborted) {
                  rej(s.reason ?? new DOMException('Aborted', 'AbortError'))
                  return
                }
                s.addEventListener(
                  'abort',
                  () => rej(s.reason ?? new DOMException('Aborted', 'AbortError')),
                  { once: true },
                )
              }),
            ])
          } else {
            await inflight
          }
          return
        }
        const p = (async () => {
          try {
            await runFetch()
          } finally {
            nlJsonAllInflightRef.current.delete(url)
            if (import.meta.env.DEV && opts.skipDuplicateDevShell) {
              nlDevAllShellSingleFlight = null
            }
          }
        })()
        nlJsonAllInflightRef.current.set(url, p)
        if (import.meta.env.DEV && opts.skipDuplicateDevShell) {
          nlDevAllShellSingleFlight = p
        }
        await p
        return
      }

      await runFetch()
    },
    [runNlOrderFlagsBulkAfterTrend],
  )

  const fetchNewListingAllWithRetry = useCallback(
    async (opts: {
      forceSync: boolean
      writeCache: boolean
      startDate?: string | null
      endDate?: string | null
      defaultDateRange?: boolean
      syncFormState?: boolean
      signal?: AbortSignal
      mergeIntoExisting?: boolean
      skipDuplicateDevShell?: boolean
    }) => {
      const maxAttempts = 24
      let attempt = 0
      while (true) {
        try {
          await fetchNewListingJson({
            skipSync: opts.forceSync,
            writeCache: opts.writeCache,
            jsonViews: 'all',
            startDate: opts.startDate,
            endDate: opts.endDate,
            defaultDateRange: opts.defaultDateRange,
            syncFormState: opts.syncFormState,
            signal: opts.signal,
            mergeIntoExisting: opts.mergeIntoExisting,
            skipDuplicateDevShell: opts.skipDuplicateDevShell,
          })
          setWaitingForBuildRetry(false)
          return
        } catch (e) {
          if (typeof e === 'object' && e !== null && (e as { name?: string }).name === 'AbortError') throw e
          if (!isTrendHttpStatus(e, 429) || attempt >= maxAttempts - 1) {
            setWaitingForBuildRetry(false)
            throw e
          }
          const retryAfterSec = Math.max(1, Number((e as TrendHttpError).retryAfterSec ?? 3))
          setWaitingForBuildRetry(true)
          setErr(`New Listing 报表正在生成中，约 ${retryAfterSec}s 后自动重试（${attempt + 1}/${maxAttempts}）`)
          await waitWithAbort(retryAfterSec * 1000, opts.signal)
          attempt += 1
        }
      }
    },
    [fetchNewListingJson],
  )

  const runNewListingAllViewFetch = useCallback(
    async (
      forceSync: boolean,
      next: { startDate?: string; endDate?: string; useDefaultDateRange?: boolean } = {},
    ) => {
      const nextUseDefaultDateRange = next.useDefaultDateRange ?? useDefaultDateRange
      const nextStartDate = (next.startDate ?? startDateInput).trim()
      const nextEndDate = (next.endDate ?? endDateInput).trim()
      if (nextStartDate && nextEndDate && nextStartDate > nextEndDate) {
        setErr('开始日期不能晚于结束日期')
        return
      }
      const requestStartDate = nextUseDefaultDateRange ? null : nextStartDate || null
      const requestEndDate = nextUseDefaultDateRange ? null : nextEndDate || null
      nlShellAbortRef.current?.abort()
      nlStoreAbortRef.current?.abort()
      nlPrefetchGenRef.current += 1
      const ac = new AbortController()
      nlShellAbortRef.current = ac
      setRefreshing(true)
      setErr(null)
      try {
        await fetchNewListingAllWithRetry({
          forceSync,
          writeCache: nextUseDefaultDateRange,
          startDate: requestStartDate,
          endDate: requestEndDate,
          defaultDateRange: nextUseDefaultDateRange,
          syncFormState: true,
          signal: ac.signal,
          mergeIntoExisting: false,
        })
        setWaitingForBuildRetry(false)
        setErr(null)
      } catch (e: unknown) {
        if (typeof e === 'object' && e !== null && (e as { name?: string }).name === 'AbortError') return
        setWaitingForBuildRetry(false)
        setErr(e instanceof Error ? e.message : '请求失败')
      } finally {
        setRefreshing(false)
      }
    },
    [endDateInput, fetchNewListingAllWithRetry, startDateInput, useDefaultDateRange],
  )

  useLayoutEffect(() => {
    const p = nlProfilePendingRef.current
    if (p == null || payload == null || typeof performance === 'undefined') return
    const t4 = performance.now()
    console.info('[New Listing] 加载分段耗时', {
      客户端毫秒: {
        到响应头: Math.round(p.fetchToHeadersMs),
        读响应体: Math.round(p.readBodyMs),
        JSON解析: Math.round(p.parseMs),
        解析到提交布局: Math.round(t4 - p.afterParse),
        自请求起合计: Math.round(t4 - p.t0),
      },
      服务端秒: p.server ?? '(未包含 profile 字段)',
    })
    nlProfilePendingRef.current = null
  }, [payload])

  useEffect(() => {
    let cancelled = false
    setErr(null)
    if (!import.meta.env.DEV) nlShellAbortRef.current?.abort()
    const ac = new AbortController()
    nlShellAbortRef.current = ac
    const custom = Boolean(nlUrlRange.start && nlUrlRange.end)
    const hasBootDefaultCache = Boolean(boot.payload?.views?.all && boot.useCacheOnly)
    fetchNewListingAllWithRetry({
      forceSync: false,
      writeCache: !custom,
      startDate: custom ? nlUrlRange.start : null,
      endDate: custom ? nlUrlRange.end : null,
      defaultDateRange: !custom,
      syncFormState: true,
      /** Dev：不传壳层 AbortSignal，避免 StrictMode cleanup/mount 打断首个 json_views=all */
      signal: import.meta.env.DEV ? undefined : ac.signal,
      // 有本地默认区间缓存时：后台静默合并刷新，首屏已可交互，避免整包替换闪烁；失败时仍保留图表
      mergeIntoExisting: hasBootDefaultCache,
      skipDuplicateDevShell: import.meta.env.DEV,
    })
      .then(() => {
        setWaitingForBuildRetry(false)
        setErr(null)
      })
      .catch((e: unknown) => {
        if (cancelled || (typeof e === 'object' && e !== null && (e as { name?: string }).name === 'AbortError'))
          return
        setWaitingForBuildRetry(false)
        if (nlPayloadRef.current?.views?.all) {
          console.warn('[New Listing] 后台刷新失败，仍显示已有数据', e)
          setErr(e instanceof Error ? `后台更新未成功：${e.message}` : '后台更新未成功')
        } else {
          setErr(e instanceof Error ? e.message : '请求失败')
        }
      })
    return () => {
      cancelled = true
      if (!import.meta.env.DEV) ac.abort()
    }
  }, [boot.useCacheOnly, boot.payload, fetchNewListingAllWithRetry, nlUrlRange.end, nlUrlRange.start])

  /** views.all 就绪后空闲预取各店 json_views=store，合并进 payload / 本地缓存；切换店铺时直接命中 views[id] */
  useEffect(() => {
    if (!payload?.views?.all) return
    // 自定义起止日期往往区间较大，预取各店会触发多次后端重算/online 查询，容易变慢或把 online pool 打满。
    // 默认 35 天窗口才做 idle 预取加速「切换店铺」。
    if (!useDefaultDateRange) return
    const ids = payload.storeIds || []
    if (!ids.length) return

    const gen = ++nlPrefetchGenRef.current
    let cancelled = false
    let prefetchAc: AbortController | null = null

    const cancelPrefetchSchedule = () => {
      const h = nlPrefetchRicRef.current
      nlPrefetchRicRef.current = null
      if (h == null) return
      if (nlPrefetchRicIsNativeRef.current && typeof cancelIdleCallback !== 'undefined') {
        cancelIdleCallback(h)
      } else {
        clearTimeout(h)
      }
      nlPrefetchRicIsNativeRef.current = false
    }

    const runOneStore = () => {
      if (cancelled || gen !== nlPrefetchGenRef.current) return
      const p = nlPayloadRef.current
      if (!p?.views?.all) return
      const sid = ids.find(
        (id) => !p.views[String(id)] && !nlStoreInFlightRef.current.has(id),
      )
      if (sid == null) return

      nlStoreInFlightRef.current.add(sid)
      prefetchAc?.abort()
      prefetchAc = new AbortController()

      fetchNewListingJson({
        skipSync: false,
        writeCache: useDefaultDateRange,
        jsonViews: 'store',
        storeId: sid,
        startDate: useDefaultDateRange ? null : startDateInput.trim() || null,
        endDate: useDefaultDateRange ? null : endDateInput.trim() || null,
        signal: prefetchAc.signal,
        mergeIntoExisting: true,
      })
        .catch((e: unknown) => {
          if (typeof e === 'object' && e !== null && (e as { name?: string }).name === 'AbortError') return
          /* 预取失败不弹窗，避免打扰；用户点开该店时会再请求 */
        })
        .finally(() => {
          nlStoreInFlightRef.current.delete(sid)
          if (gen !== nlPrefetchGenRef.current) return
          if (!cancelled) scheduleIdle()
        })
    }

    const scheduleIdle = () => {
      if (cancelled || gen !== nlPrefetchGenRef.current) return
      cancelPrefetchSchedule()
      const p = nlPayloadRef.current
      const hasMore = ids.some(
        (id) => !p?.views?.[String(id)] && !nlStoreInFlightRef.current.has(id),
      )
      if (!hasMore) return

      if (typeof requestIdleCallback !== 'undefined') {
        nlPrefetchRicIsNativeRef.current = true
        nlPrefetchRicRef.current = requestIdleCallback(
          () => {
            nlPrefetchRicRef.current = null
            nlPrefetchRicIsNativeRef.current = false
            runOneStore()
          },
          { timeout: 2500 },
        )
      } else {
        nlPrefetchRicIsNativeRef.current = false
        nlPrefetchRicRef.current = window.setTimeout(() => {
          nlPrefetchRicRef.current = null
          runOneStore()
        }, 48) as unknown as number
      }
    }

    scheduleIdle()
    return () => {
      cancelled = true
      cancelPrefetchSchedule()
      prefetchAc?.abort()
    }
  }, [
    boot.useCacheOnly,
    boot.payload,
    payload?.views?.all,
    payload?.sessionChartStart,
    payload?.sessionChartEnd,
    (payload?.storeIds || []).join(','),
    endDateInput,
    startDateInput,
    useDefaultDateRange,
    fetchNewListingJson,
  ])

  useEffect(() => {
    if (storeKey === 'all') {
      setDisplayStoreKey('all')
      setStoreViewLoading(null)
      return
    }
    const sid = Number(storeKey)
    if (!Number.isFinite(sid) || !payload?.views?.all) return
    if (payload.views[storeKey]) {
      setStoreViewLoading(null)
      return
    }
    if (nlStoreInFlightRef.current.has(sid)) {
      setStoreViewLoading(storeKey)
      return
    }
    nlStoreAbortRef.current?.abort()
    const ac = new AbortController()
    nlStoreAbortRef.current = ac
    nlStoreInFlightRef.current.add(sid)
    setStoreViewLoading(storeKey)
    fetchNewListingJson({
      skipSync: false,
      writeCache: useDefaultDateRange,
      jsonViews: 'store',
      storeId: sid,
      startDate: useDefaultDateRange ? null : startDateInput.trim() || null,
      endDate: useDefaultDateRange ? null : endDateInput.trim() || null,
      signal: ac.signal,
      mergeIntoExisting: true,
    })
      .catch((e: unknown) => {
        if (typeof e === 'object' && e !== null && (e as { name?: string }).name === 'AbortError') return
        setErr(e instanceof Error ? e.message : '店铺数据加载失败')
      })
      .finally(() => {
        nlStoreInFlightRef.current.delete(sid)
        setStoreViewLoading((k) => (k === storeKey ? null : k))
      })
    return () => ac.abort()
  }, [storeKey, payload?.views, startDateInput, endDateInput, useDefaultDateRange, fetchNewListingJson])

  useEffect(() => {
    if (storeKey === 'all') {
      setDisplayStoreKey('all')
      setStoreViewLoading(null)
      return
    }
    if (payload?.views?.[storeKey]) {
      setDisplayStoreKey(storeKey)
      setStoreViewLoading(null)
    }
  }, [storeKey, payload?.views])

  useEffect(() => {
    if (!view?.labels?.length) return
    if (nlChartIdentity && nlChartIdentityCommittedRef.current === nlChartIdentity) {
      return
    }
    nlChartIdentityCommittedRef.current = nlChartIdentity

    setChartMountReady(false)
    let raf0 = 0
    let raf1 = 0
    let tmo: ReturnType<typeof setTimeout> | null = null
    // 全店 JSON 大：不再用 requestIdleCallback（易排到数百毫秒后才挂载）；双 rAF 让出paint后再挂 Chart
    if (storeKey !== 'all') {
      tmo = setTimeout(() => setChartMountReady(true), 0)
    } else {
      raf0 = requestAnimationFrame(() => {
        raf1 = requestAnimationFrame(() => setChartMountReady(true))
      })
    }
    return () => {
      if (tmo != null) clearTimeout(tmo)
      cancelAnimationFrame(raf0)
      cancelAnimationFrame(raf1)
    }
  }, [nlChartIdentity, storeKey, view?.labels?.length])

  const nlFatalLoadError = Boolean(err && !waitingForBuildRetry && !(payload?.views?.all))

  if (nlFatalLoadError) {
    return (
      <div className="trend-embed-page trend-embed-page--message">
        <h2 className="trend-embed-error-title">页面加载失败</h2>
        <pre className="trend-embed-error-body">{err}</pre>
      </div>
    )
  }
  if (payload === null || !payload.views?.all) {
    return (
      <div className="trend-embed-page trend-embed-page--message">
        <p className="trend-embed-loading">正在加载 New Listing 报表…</p>
        {nlForcedNetwork ? (
          <p className="trend-embed-loading-hint">
            已开启强制刷新（跳过本地与服务端短缓存），需全量重算；默认约 35 天窗口 + 多店时单次请求可达数分钟，DevTools
            里会一直显示 pending，属正常排队/计算，并非页面卡死。
          </p>
        ) : null}
        {err ? <pre className="trend-embed-error-body">{err}</pre> : null}
      </div>
    )
  }

  if (view == null) {
    return (
      <div className="trend-embed-page trend-embed-page--message">
        <p className="trend-embed-loading">正在加载 New Listing 报表…</p>
        {nlForcedNetwork ? (
          <p className="trend-embed-loading-hint">
            已开启强制刷新：全量重算中，大区间可能需数分钟；网络面板 pending 为等待后端响应。
          </p>
        ) : null}
      </div>
    )
  }

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
          disabled={Boolean(storeViewLoading)}
          onChange={(e) => setStoreKey(e.target.value)}
        >
          {storeOptions.map((o) => (
            <option key={o.value} value={o.value}>
              {o.label}
            </option>
          ))}
        </select>
        <label className="trend-new-listing-label" htmlFor="trend-nl-start-date">
          起始日期
        </label>
        <input
          id="trend-nl-start-date"
          className="trend-new-listing-select trend-new-listing-date-input"
          type="date"
          value={startDateInput}
          disabled={refreshing}
          max={endDateInput || payload?.sessionChartEnd || undefined}
          onChange={(e) => {
            const next = e.target.value
            setStartDateInput(next)
            setUseDefaultDateRange(!next && !endDateInput)
          }}
        />
        <label className="trend-new-listing-label" htmlFor="trend-nl-end-date">
          结束日期
        </label>
        <input
          id="trend-nl-end-date"
          className="trend-new-listing-select trend-new-listing-date-input"
          type="date"
          value={endDateInput}
          disabled={refreshing}
          min={startDateInput || undefined}
          onChange={(e) => {
            const next = e.target.value
            setEndDateInput(next)
            setUseDefaultDateRange(!startDateInput && !next)
          }}
        />
        <button
          type="button"
          className="trend-new-listing-refresh-btn"
          disabled={refreshing}
          onClick={() => void runNewListingAllViewFetch(false)}
        >
          {refreshing ? '加载中…' : '查询'}
        </button>
        <button
          type="button"
          className="trend-new-listing-refresh-btn trend-new-listing-refresh-btn--secondary"
          disabled={refreshing}
          onClick={() => void runNewListingAllViewFetch(true)}
        >
          同步 listing 并重载
        </button>
        {waitingForBuildRetry ? (
          <span className="trend-new-listing-refresh-banner-warn" role="status">
            {err ?? '报表排队生成中，请稍候…'}
          </span>
        ) : err && payload?.views?.all ? (
          <span className="trend-new-listing-refresh-banner-warn" role="status">
            {err.length > 160 ? `${err.slice(0, 160)}…` : err}
          </span>
        ) : null}
        {showingFallbackStoreView ? (
          <span className="trend-new-listing-loading-note">
            {storeViewLoading ? `正在加载店铺 ${storeKey} 的图表与明细，当前先保留已加载内容…` : `准备加载店铺 ${storeKey}…`}
          </span>
        ) : null}
        <span className="trend-new-listing-meta">
          查询区间：{payload.sessionChartStart}～{payload.sessionChartEnd} {useDefaultDateRange ? '（默认最近35天）' : '（自定义）'} · KPI：open_date &gt; {payload.listingSince}
          （amazon_listing 全表行）· 每批 {payload.cohortTrackDays ?? 30} 日 · 横轴{' '}
          {payload.sessionChartStart}～{payload.sessionChartEnd}
          {payload.chartRangeAutoExpanded ? '（已按本地数据扩展区间）' : ''}
          {fromCache ? (
            <>
              {' '}
              · 默认展示本地缓存（生成 {payload.generatedAt ?? '—'}），不自动请求接口
            </>
          ) : null}
          {trendNewListingProfileFromSearch() ? (
            <> · 性能分析：打开开发者工具 Console 查看分段耗时</>
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
     
      <div className="trend-new-listing-chart-wrap">
        {!nlStackChartPrep || nlStackChartPrep.barCount === 0 ? (
          <p className="trend-new-listing-empty">暂无图表数据（请确认已同步 daily_upload_asin_dates 且 open_date 非空）。</p>
        ) : !chartMountReady ? (
          <p className="trend-new-listing-empty" style={{ minHeight: 320 }}>
            正在准备图表…
          </p>
        ) : (
          <Chart
            type="bar"
            data={nlStackChartPrep.chartData}
            options={{
              responsive: true,
              maintainAspectRatio: false,
              interaction: { mode: 'index', intersect: false },
              plugins: {
                legend: { position: 'top' },
                tooltip: {
                  enabled: false,
                  external: (context) => nlRenderExternalTooltip(context, view.labels, nlStackChartPrep.lt),
                  filter: (tooltipItem) => {
                    const chart = tooltipItem.chart
                    const ds = chart.data.datasets[tooltipItem.datasetIndex] as {
                      type?: string
                      label?: string
                    }
                    if (ds.type === 'line' || String(ds.label ?? '') === '当日 sessions 合计') {
                      return true
                    }
                    const sessionYmd = view.labels[tooltipItem.dataIndex]
                    const cohortYmd = nlStackTooltipParseBatchYmd(String(ds.label ?? ''))
                    if (!cohortYmd) return true
                    return nlStackTooltipCohortInWindow(sessionYmd, cohortYmd)
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
                  max: nlStackChartPrep.maxY,
                  title: { display: true, text: 'Sessions（堆叠）' },
                },
                y1: {
                  stacked: false,
                  position: 'right',
                  beginAtZero: true,
                  max: nlStackChartPrep.maxY,
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
        
        <div className="trend-new-listing-table-controls">
          <label className="trend-new-listing-table-toggle">
            <input
              type="checkbox"
              checked={hideSmallCohorts}
              onChange={(e) => setHideSmallCohorts(e.target.checked)}
            />
            <span>折叠上新 ASIN 数小于 {NL_COHORT_COLLAPSE_NEW_ASIN_THRESHOLD} 的批次</span>
          </label>
          <span className="trend-new-listing-table-stats">
            当前显示 {filteredCohortTable.length} / {cohortTable.length} 个批次
          </span>
        </div>
        {!cohortTable.length ? (
          <p className="trend-new-listing-table-empty">暂无表格数据（需要 open_date 批次与本地 sessions 明细）。</p>
        ) : !filteredCohortTable.length ? (
          <p className="trend-new-listing-table-empty">
            当前筛选后暂无批次数据。请取消折叠，或检查是否存在上新 ASIN 数不小于 {NL_COHORT_COLLAPSE_NEW_ASIN_THRESHOLD} 的批次。
          </p>
        ) : filteredCohortTable.length >= 10 ? (
          <TrendNewListingVirtualCohortTable
            cohortTable={filteredCohortTable}
            cohortTrackDays={cohortTrackDays}
            latestSessionYmd={latestSessionYmd}
            nlPopoverOpenFromCell={nlPopoverOpenFromCell}
            nlPopoverPinFromCell={nlPopoverPinFromCell}
            nlPopoverScheduleClose={nlPopoverScheduleClose}
            nlPopoverCancelClose={nlPopoverCancelClose}
            nlPopoverCloseNow={nlPopoverCloseNow}
          />
        ) : (
          <div className="trend-new-listing-table-scroll" onScroll={nlPopoverCloseNow}>
            <table className="trend-new-listing-table">
              <thead>
                <tr>
                  <th className="is-sticky-col is-sticky-col--1">上新日（PST）</th>
                  <th className="is-sticky-col is-sticky-col--2">上新/补录</th>
                  <th className="is-sticky-col is-sticky-col--3">总 session 数</th>
                  <th>比值</th>
                  {Array.from({ length: cohortTrackDays }, (_, i) => (
                    <th key={`d${i + 1}`}>{`第${i + 1}天`}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {filteredCohortTable.map((row: TrendNlCohortRow) => (
                  <TrendNlCohortVirtualRow
                    key={String(row?.cohortDate ?? '') || 'row'}
                    row={row}
                    cohortTrackDays={cohortTrackDays}
                    latestSessionYmd={latestSessionYmd}
                    nlPopoverOpenFromCell={nlPopoverOpenFromCell}
                    nlPopoverPinFromCell={nlPopoverPinFromCell}
                    nlPopoverScheduleClose={nlPopoverScheduleClose}
                    nlPopoverCancelClose={nlPopoverCancelClose}
                  />
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
      <NlDaySessionPopoverPortal
        anchor={nlDayPopover}
        cancelClose={nlPopoverCancelClose}
        scheduleClose={nlPopoverScheduleClose}
        onUnpin={nlPopoverCloseNow}
        hasOrder={nlHasOrder}
      />
    </div>
  )
}
