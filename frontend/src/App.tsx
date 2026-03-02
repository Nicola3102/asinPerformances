import { useEffect, useState } from 'react'
import {
  listSummary,
  getDetail,
  getTableStats,
  getSummaryStats,
  syncFromOnline,
  type SummaryRow,
  type DetailResponse,
  type DetailChildRow,
  type SearchQueryRow,
  type WeekStatsRow,
} from './api/client'
import './App.css'

function formatParentOrderTotal(v: string | number | null | undefined): string {
  if (v == null || v === '') return '–'
  const n = Number(v)
  if (Number.isNaN(n)) return '–'
  return String(Math.round(n))
}

function formatNum(v: number | null | undefined): string {
  if (v == null) return '–'
  return String(v)
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

function App() {
  const [summary, setSummary] = useState<SummaryRow[]>([])
  const [tableCount, setTableCount] = useState<number | null>(null)
  const [weekStats, setWeekStats] = useState<WeekStatsRow[]>([])
  const [loading, setLoading] = useState(true)
  const [syncing, setSyncing] = useState(false)
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

  const loadSummary = async () => {
    setLoading(true)
    setError(null)
    const timeoutMs = 15000
    let timeoutId: ReturnType<typeof setTimeout>
    const timeoutPromise = new Promise<never>((_, reject) => {
      timeoutId = setTimeout(() => reject(new Error('请求超时，请确认后端已启动（如 docker compose up）')), timeoutMs)
    })
    try {
      const [summaryData, stats, summaryStats] = await Promise.race([
        Promise.all([listSummary(), getTableStats(), getSummaryStats()]),
        timeoutPromise,
      ]) as [Awaited<ReturnType<typeof listSummary>>, Awaited<ReturnType<typeof getTableStats>>, Awaited<ReturnType<typeof getSummaryStats>>]
      clearTimeout(timeoutId!)
      setSummary(summaryData)
      setTableCount(stats.count)
      setWeekStats(summaryStats.by_week ?? [])
    } catch (e) {
      clearTimeout(timeoutId!)
      setError(e instanceof Error ? e.message : 'Failed to load')
      setSummary([])
      setTableCount(null)
      setWeekStats([])
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadSummary()
  }, [])

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
        {tableCount !== null && (
          <span className="table-stats">数据表 asin_performances 共 {tableCount} 条</span>
        )}
        {weekStats.length > 0 && (
          <span className="week-stats">
            {weekStats.map((w, i) => (
              <span key={w.week_no ?? i}>
                {i > 0 && ' · '}
                week_no: {formatNum(w.week_no)} | 父 ASIN 共 {formatNum(w.parent_asin_count)} 个 | 总订单 {formatParentOrderTotal(w.total_orders)} 笔
              </span>
            ))}
          </span>
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
                <th>Parent ASIN</th>
                <th>Parent ASIN Create At</th>
                <th>Parent Order Total</th>
                <th>store_id</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {summary.map((row, i) => (
                <tr key={`${row.parent_asin}-${row.week_no}-${row.store_id ?? ''}-${i}`}>
                  <td>{row.parent_asin ?? '-'}</td>
                  <td>{row.parent_asin_create_at != null ? String(row.parent_asin_create_at).slice(0, 19) : '–'}</td>
                  <td>{formatParentOrderTotal(row.parent_order_total)}</td>
                  <td>{formatNum(row.store_id)}</td>
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
              ))}
            </tbody>
          </table>
        </div>
      )}

      {detailLoading && <p className="loading-detail">Loading detail...</p>}
      {detail && <DetailModal data={detail} onClose={closeModal} />}
    </div>
  )
}

export default App
