import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
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
  type SummaryRowConsolidated,
  type DetailResponse,
} from '../../api/client'
import { devRequestSingleFlight } from '../../lib/devRequestSingleFlight'
import { formatCreatedAt, formatNum, formatParentOrderTotal } from '../../lib/formatters'
import { DetailModal } from './DetailModal'
import './homeRoutes.css'

export default function AsinHomePage() {
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

  const loadSummary = useCallback(async (weekOverride?: number | '') => {
    setLoading(true)
    setError(null)
    const timeoutMs = 15000
    let timeoutId: ReturnType<typeof setTimeout>
    const timeoutPromise = new Promise<never>((_, reject) => {
      timeoutId = setTimeout(() => reject(new Error('请求超时，请确认后端已启动（如 docker compose up）')), timeoutMs)
    })
    try {
      const [weeksData, stats] = await Promise.race([
        Promise.all([
          devRequestSingleFlight('home:list-weeks', () => listWeeks()),
          devRequestSingleFlight('home:table-stats', () => getTableStats()),
        ]),
        timeoutPromise,
      ]) as [Awaited<ReturnType<typeof listWeeks>>, Awaited<ReturnType<typeof getTableStats>>]
      clearTimeout(timeoutId!)
      const fallbackWeek = weeksData.length > 0 ? weeksData[0] : ''
      const effectiveWeek = weekOverride !== undefined ? weekOverride : (selectedWeek !== '' ? selectedWeek : fallbackWeek)
      const summaryData =
        typeof effectiveWeek === 'number'
          ? await devRequestSingleFlight(`home:summary:${effectiveWeek}`, () => listSummaryConsolidatedByWeek(effectiveWeek))
          : []
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
  }, [selectedWeek])

  const loadSummaryRef = useRef(loadSummary)

  useEffect(() => {
    loadSummaryRef.current = loadSummary
  }, [loadSummary])

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

  const triggerRefreshQueryStatus = useCallback(async (week: number) => {
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
  }, [])

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
    void loadSummaryRef.current()
  }, [])

  useEffect(() => {
    if (typeof selectedWeek !== 'number') return
    void triggerRefreshQueryStatus(selectedWeek)
    const timer = setInterval(() => {
      void triggerRefreshQueryStatus(selectedWeek)
    }, 30 * 60 * 1000) // 30 分钟
    return () => clearInterval(timer)
  }, [selectedWeek, triggerRefreshQueryStatus])

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
