import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import type { ChartOptions, TooltipItem } from "chart.js"
import {
  listAdSales,
  triggerAdSalesEnsureLatest,
  downloadAdSales,
  type AdSalesDailyPoint,
  type AdSalesRow,
  type AdSalesSummary,
} from "../../api/client"
import { Chart } from "../../lib/chartRegister"
import { devRequestSingleFlight } from "../../lib/devRequestSingleFlight"
import './adsRoutes.css'

const EMPTY_AD_SALES_SUMMARY: AdSalesSummary = {
  clicks: 0,
  impressions: 0,
  ad_cost: 0,
  sales_1d: 0,
  order_item_sales: 0,
  tacos: 0,
  ad_asin_count: 0,
  cpc: 0,
  acos: 0,
  cvr: 0,
  purchases: 0,
}

export function AdSalesPage() {
  const [storeId, setStoreId] = useState<string>('')
  const [startDate, setStartDate] = useState<string>('')
  const [endDate, setEndDate] = useState<string>('')
  const [page, setPage] = useState<number>(1)
  const pageSize = 30

  const [items, setItems] = useState<AdSalesRow[]>([])
  const [total, setTotal] = useState<number>(0)
  const [loading, setLoading] = useState<boolean>(false)
  const [error, setError] = useState<string | null>(null)
  const [summary, setSummary] = useState<AdSalesSummary>(EMPTY_AD_SALES_SUMMARY)
  const [dailySeries, setDailySeries] = useState<AdSalesDailyPoint[]>([])
  const [selected, setSelected] = useState<Set<number>>(new Set())
  const [sorts, setSorts] = useState<Array<{ field: string; dir: 'asc' | 'desc' }>>([])
  const [syncNotice, setSyncNotice] = useState<string | null>(null)
  const [syncRefreshing, setSyncRefreshing] = useState<boolean>(false)
  const adSalesSortEffectReadyRef = useRef(false)
  const loadRef = useRef<(nextPage: number, ensureLatest?: boolean) => Promise<void>>(async () => {})
  const triggerLatestRefreshRef = useRef<() => Promise<void>>(async () => {})

  const totalPages = Math.max(1, Math.ceil(total / pageSize))

  const sortQuery = useMemo(() => {
    if (!sorts.length) return null
    return sorts.map((s) => `${s.field}:${s.dir}`).join(',')
  }, [sorts])

  const load = useCallback(async (nextPage: number, ensureLatest = false) => {
    setLoading(true)
    setError(null)
    try {
      const sid = storeId.trim() ? Number(storeId.trim()) : null
      const requestStoreId = Number.isFinite(sid as number) ? (sid as number) : null
      const requestStartDate = startDate.trim() || null
      const requestEndDate = endDate.trim() || null
      const requestKey = [
        'ad-sales',
        requestStoreId ?? 'all',
        requestStartDate ?? '',
        requestEndDate ?? '',
        sortQuery ?? '',
        nextPage,
        ensureLatest ? '1' : '0',
      ].join(':')
      const res = await devRequestSingleFlight(requestKey, () =>
        listAdSales({
          store_id: requestStoreId,
          start_date: requestStartDate,
          end_date: requestEndDate,
          ensure_latest: ensureLatest,
          sort: sortQuery,
          page: nextPage,
          page_size: pageSize,
        }),
      )
      setItems(res.items || [])
      setTotal(res.total || 0)
      setPage(res.page || nextPage)
      setSummary(res.summary || EMPTY_AD_SALES_SUMMARY)
      setDailySeries(res.daily_series || [])
    } catch (e) {
      setError(e instanceof Error ? e.message : '加载失败')
    } finally {
      setLoading(false)
    }
  }, [storeId, startDate, endDate, sortQuery])

  const triggerLatestRefresh = useCallback(async () => {
    setSyncRefreshing(true)
    setError(null)
    try {
      const res = await triggerAdSalesEnsureLatest()
      setSyncNotice(res.message || '已在后台触发最新数据刷新，请稍后点击 Search 查看最新结果。')
    } catch (e) {
      setSyncNotice(null)
      setError(e instanceof Error ? e.message : '后台刷新触发失败')
    } finally {
      setSyncRefreshing(false)
    }
  }, [])

  useEffect(() => {
    loadRef.current = load
  }, [load])

  useEffect(() => {
    triggerLatestRefreshRef.current = triggerLatestRefresh
  }, [triggerLatestRefresh])

  useEffect(() => {
    let cancelled = false
    const boot = async () => {
      await loadRef.current(1)
      if (cancelled) return
      void triggerLatestRefreshRef.current()
    }
    void boot()
    return () => {
      cancelled = true
    }
  }, [])

  // 排序变化后立即重新拉取（回到第 1 页）
  useEffect(() => {
    if (!adSalesSortEffectReadyRef.current) {
      adSalesSortEffectReadyRef.current = true
      return
    }
    void loadRef.current(1)
  }, [sortQuery])

  const toggleSelected = (id: number) => {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  const toggleAllOnPage = () => {
    setSelected((prev) => {
      const ids = items.map((x) => x.id).filter((x) => typeof x === 'number')
      const allChecked = ids.length > 0 && ids.every((id) => prev.has(id))
      const next = new Set(prev)
      if (allChecked) ids.forEach((id) => next.delete(id))
      else ids.forEach((id) => next.add(id))
      return next
    })
  }

  const handleSearch = async () => {
    // 新查询条件：清空旧勾选，避免跨条件混淆
    setSelected(new Set())
    await load(1)
  }

  const toggleSort = (field: string, multi: boolean) => {
    setSorts((prev) => {
      const idx = prev.findIndex((x) => x.field === field)
      const next = multi ? [...prev] : []
      if (idx < 0) {
        next.push({ field, dir: 'desc' })
        return next
      }
      const cur = prev[idx]
      const flipped = cur.dir === 'desc' ? 'asc' : 'desc'
      const base = multi ? prev.filter((x) => x.field !== field) : []
      base.push({ field, dir: flipped })
      return base
    })
  }

  const sortMark = (field: string) => {
    const idx = sorts.findIndex((x) => x.field === field)
    if (idx < 0) return ''
    const s = sorts[idx]
    const arrow = s.dir === 'asc' ? '↑' : '↓'
    return ` ${arrow}${sorts.length > 1 ? String(idx + 1) : ''}`
  }

  const sortableThProps = (field: string) => ({
    role: 'button' as const,
    tabIndex: 0,
    style: { cursor: 'pointer', userSelect: 'none' as const },
    title: 'Click 排序；Shift+Click 多列排序',
    onClick: (e: React.MouseEvent) => toggleSort(field, e.shiftKey),
    onKeyDown: (e: React.KeyboardEvent) => {
      if (e.key === 'Enter' || e.key === ' ') toggleSort(field, (e as unknown as { shiftKey: boolean }).shiftKey)
    },
  })

  const handlePrev = async () => {
    const next = Math.max(1, page - 1)
    await load(next)
  }

  const handleNext = async () => {
    const next = Math.min(totalPages, page + 1)
    await load(next)
  }

  const handleDownloadSelected = async () => {
    const ids = Array.from(selected.values())
    if (!ids.length) return
    try {
      await downloadAdSales(ids)
    } catch (e) {
      setError(e instanceof Error ? e.message : '下载失败')
    }
  }

  const trendChartData = useMemo(() => {
    const labels = dailySeries.map((row) => row.date || '–')
    return {
      labels,
      datasets: [
        {
          type: 'line' as const,
          label: '广告花费',
          data: dailySeries.map((row) => row.ad_cost),
          borderColor: '#f59e0b',
          backgroundColor: 'rgba(245, 158, 11, 0.15)',
          tension: 0.3,
          pointRadius: 2,
          yAxisID: 'ySales',
        },
        {
          type: 'line' as const,
          label: '广告销售额',
          data: dailySeries.map((row) => row.sales_1d),
          borderColor: '#22c55e',
          backgroundColor: 'rgba(34, 197, 94, 0.15)',
          tension: 0.3,
          pointRadius: 2,
          yAxisID: 'ySales',
        },
        {
          type: 'line' as const,
          label: '点击',
          data: dailySeries.map((row) => row.clicks),
          borderColor: '#3b82f6',
          backgroundColor: 'rgba(59, 130, 246, 0.15)',
          tension: 0.3,
          pointRadius: 2,
          yAxisID: 'yMain',
        },
        {
          type: 'line' as const,
          label: 'impressions',
          data: dailySeries.map((row) => row.impressions),
          borderColor: '#a855f7',
          backgroundColor: 'rgba(168, 85, 247, 0.15)',
          tension: 0.3,
          pointRadius: 2,
          yAxisID: 'yImpressions',
        },
        {
          type: 'line' as const,
          label: 'CPC',
          data: dailySeries.map((row) => row.cpc),
          borderColor: '#0f766e',
          backgroundColor: 'rgba(15, 118, 110, 0.15)',
          tension: 0.3,
          pointRadius: 2,
          yAxisID: 'yRate',
        },
        {
          type: 'line' as const,
          label: 'ACoS (%)',
          data: dailySeries.map((row) => row.acos),
          borderColor: '#111827',
          backgroundColor: 'rgba(17, 24, 39, 0.12)',
          tension: 0.3,
          pointRadius: 2,
          yAxisID: 'yAcos',
        },
        {
          type: 'line' as const,
          label: 'CVR (%)',
          data: dailySeries.map((row) => row.cvr),
          borderColor: '#ef4444',
          backgroundColor: 'rgba(239, 68, 68, 0.15)',
          tension: 0.3,
          pointRadius: 2,
          yAxisID: 'yRate',
        },
        {
          type: 'line' as const,
          label: '广告订单',
          data: dailySeries.map((row) => row.purchases),
          borderColor: '#0ea5e9',
          backgroundColor: 'rgba(14, 165, 233, 0.15)',
          tension: 0.3,
          pointRadius: 2,
          yAxisID: 'yMain',
        },
        {
          type: 'line' as const,
          label: '广告ASIN销售额',
          data: dailySeries.map((row) => row.order_item_sales),
          borderColor: '#14b8a6',
          backgroundColor: 'rgba(20, 184, 166, 0.16)',
          tension: 0.28,
          pointRadius: 2,
          yAxisID: 'ySales',
        },
        {
          type: 'line' as const,
          label: 'TACoS (%)',
          data: dailySeries.map((row) => row.tacos),
          borderColor: '#111827',
          backgroundColor: 'rgba(17, 24, 39, 0.1)',
          tension: 0.28,
          pointRadius: 2,
          borderDash: [6, 3],
          yAxisID: 'yAcos',
        },
        {
          type: 'line' as const,
          label: '投广告ASIN数',
          data: dailySeries.map((row) => row.ad_asin_count),
          borderColor: '#8b5cf6',
          backgroundColor: 'rgba(139, 92, 246, 0.12)',
          tension: 0.28,
          pointRadius: 2,
          yAxisID: 'yMain',
        },
      ],
    }
  }, [dailySeries])

  const ADS_AXIS_TICK_SEGMENTS = 6

  const calcNiceStep = useCallback((rawStep: number, minStep: number) => {
    if (!Number.isFinite(rawStep) || rawStep <= 0) return minStep
    const exponent = Math.floor(Math.log10(rawStep))
    const pow = 10 ** exponent
    const base = rawStep / pow
    let niceBase = 10
    if (base <= 1) niceBase = 1
    else if (base <= 2) niceBase = 2
    else if (base <= 2.5) niceBase = 2.5
    else if (base <= 5) niceBase = 5
    return Math.max(minStep, niceBase * pow)
  }, [])

  const buildAxis = useCallback((
    values: number[],
    title: string,
    color: string,
    minStep: number,
    position: 'left' | 'right',
    offset = false,
    drawOnChartArea = false,
    fixedStepSize?: number,
  ) => {
    const nums = values.filter((v) => Number.isFinite(v))
    const maxVal = nums.length ? Math.max(...nums) : minStep
    // 固定步长：严格使用传入值，不再用 calcNiceStep 抬高（否则会 50→500）
    const stepSize =
      fixedStepSize && fixedStepSize > 0
        ? fixedStepSize
        : calcNiceStep(maxVal / ADS_AXIS_TICK_SEGMENTS, minStep)
    const minSpan = stepSize * ADS_AXIS_TICK_SEGMENTS
    const dataMax = Math.ceil(maxVal / stepSize) * stepSize
    const axisMax = Math.max(minSpan, dataMax, stepSize)
    return {
      type: 'linear' as const,
      position,
      offset,
      alignToPixels: false,
      beginAtZero: true,
      min: 0,
      max: axisMax,
      title: { display: true, text: title, color },
      ticks: {
        stepSize,
        autoSkip: false,
        color,
      },
      grid: { drawOnChartArea },
    }
  }, [calcNiceStep])

  const trendChartOptions = useMemo<ChartOptions<'line'>>(() => ({
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index' as const, intersect: false },
    plugins: {
      legend: { position: 'top' as const },
      tooltip: {
        callbacks: {
          label: (ctx: TooltipItem<'line'>) => {
            const label = ctx.dataset.label || ''
            const value = Number(ctx.parsed.y ?? 0)
            if (label === 'CPC' || label === '广告花费' || label === '广告销售额' || label === '广告ASIN销售额') {
              return `${label}: $${value.toFixed(2)}`
            }
            if (label === 'ACoS (%)' || label === 'CVR (%)' || label === 'TACoS (%)') return `${label}: ${value.toFixed(1)}%`
            if (label === '投广告ASIN数') return `${label}: ${value.toLocaleString()}`
            return `${label}: ${value.toLocaleString()}`
          },
        },
      },
    },
    scales: {
      yMain: buildAxis(
        [
          ...dailySeries.map((row) => row.clicks),
          ...dailySeries.map((row) => row.purchases),
          ...dailySeries.map((row) => row.ad_asin_count),
        ],
        '点击 / 广告订单 / 投广告ASIN数',
        '#334155',
        100,
        'left',
        false,
        true,
        100,
      ),
      yImpressions: buildAxis(
        dailySeries.map((row) => row.impressions),
        'impressions',
        '#a855f7',
        10000,
        'left',
        false,
      ),
      ySales: buildAxis(
        [
          ...dailySeries.map((row) => row.ad_cost),
          ...dailySeries.map((row) => row.sales_1d),
          ...dailySeries.map((row) => row.order_item_sales),
        ],
        '广告花费 / 广告销售额 / 广告ASIN销售额',
        '#22c55e',
        500,
        'left',
        false,
      ),
      yRate: buildAxis(
        [...dailySeries.map((row) => row.cpc), ...dailySeries.map((row) => row.cvr)],
        'CPC / CVR (%)',
        '#0f766e',
        0.1,
        'right',
      ),
      yAcos: buildAxis(
        [...dailySeries.map((row) => row.acos), ...dailySeries.map((row) => row.tacos)],
        'ACoS / TACoS (%)',
        '#111827',
        5,
        'right',
      ),
    },
  }), [buildAxis, dailySeries])

  const metricCards = [
    { label: '点击', value: summary.clicks.toLocaleString(), accent: 'blue' },
    { label: 'impressions', value: summary.impressions.toLocaleString(), accent: 'purple' },
    { label: '广告花费', value: `$${summary.ad_cost.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`, accent: 'orange' },
    { label: '广告销售额', value: `$${summary.sales_1d.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`, accent: 'green' },
    { label: '广告ASIN总销售额', value: `$${summary.order_item_sales.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`, accent: 'teal' },
    { label: 'TACoS', value: `${summary.tacos.toFixed(1)}%`, accent: 'black' },
    { label: '投广告ASIN数', value: summary.ad_asin_count.toLocaleString(), accent: 'purple' },
    { label: 'CPC', value: `$${summary.cpc.toFixed(2)}`, accent: 'teal' },
    { label: 'ACoS', value: `${summary.acos.toFixed(1)}%`, accent: 'black' },
    { label: 'CVR', value: `${summary.cvr.toFixed(1)}%`, accent: 'red' },
    { label: '广告订单', value: summary.purchases.toLocaleString(), accent: 'sky' },
  ]

  return (
    <div className="app">
      <h1>Ad-Sales</h1>
      <p className="monitor-desc">
        从本地 <code>daily_ad_cost_sales</code> 查询点击/曝光/花费等指标；汇总卡片中的「广告 ASIN 总销售额」与「广告订单」来自线上{' '}
        <code>order_item</code> INNER JOIN <code>amazon_ads_ad_group_ad</code>（按 PST 日历日过滤），销售额为 DISTINCT 行上{' '}
        <code>item_price_amount * quantity_ordered</code> 之和，广告订单为上述 DISTINCT 行数。下方表格仅显示当前店铺/日期下 <code>
          purchases &gt; 0
        </code>{' '}
        的 ad_asin。
      </p>

      <div className="monitor-controls" style={{ alignItems: 'flex-end' }}>
        <label>
          <span>store_id</span>
          <input
            className="monitor-select"
            value={storeId}
            onChange={(e) => setStoreId(e.target.value)}
            placeholder="例如 1"
          />
        </label>
        <label>
          <span>start</span>
          <input
            className="monitor-select"
            type="date"
            value={startDate}
            onChange={(e) => setStartDate(e.target.value)}
          />
        </label>
        <label>
          <span>end</span>
          <input
            className="monitor-select"
            type="date"
            value={endDate}
            onChange={(e) => setEndDate(e.target.value)}
          />
        </label>
        <button className="btn" onClick={handleSearch} disabled={loading}>Search</button>
        <button className="btn" onClick={() => void triggerLatestRefresh()} disabled={loading || syncRefreshing}>
          {syncRefreshing ? 'Refreshing…' : '后台刷新最新数据'}
        </button>
        <button className="btn" onClick={handleDownloadSelected} disabled={loading || selected.size === 0}>
          Download selected ({selected.size})
        </button>
      </div>

      <div style={{ marginTop: 10, display: 'flex', gap: 8, alignItems: 'center' }}>
        <button className="btn" onClick={handlePrev} disabled={loading || page <= 1}>Prev</button>
        <span className="muted">page {page} / {totalPages} · total {total}</span>
        <button className="btn" onClick={handleNext} disabled={loading || page >= totalPages}>Next</button>
      </div>

      {error && <div className="error" style={{ marginTop: 10 }}>{error}</div>}
      {syncNotice && <div className="empty-hint" style={{ marginTop: 10 }}>{syncNotice}</div>}
      {loading && <div className="empty-hint" style={{ marginTop: 10 }}>Loading…</div>}

      <div className="ads-kpi-grid" style={{ marginTop: 14 }}>
        {metricCards.map((card) => (
          <div key={card.label} className={`ads-kpi-card ads-kpi-card--${card.accent}`}>
            <div className="ads-kpi-label">{card.label}</div>
            <div className="ads-kpi-value">{card.value}</div>
          </div>
        ))}
      </div>

      <div className="trend-chart-card" style={{ marginTop: 14 }}>
        <div className="trend-chart-header">
          <div>
            <h3>关键指标</h3>
            <p className="trend-chart-hint">按筛选区间汇总每日点击、CPC、ACoS 与广告订单</p>
          </div>
        </div>
        <div className="ads-line-chart-wrap">
          <Chart type="line" data={trendChartData} options={trendChartOptions} />
        </div>
      </div>

      <div className="monitor-tables" style={{ marginTop: 10 }}>
        <table className="data-table monitor-track-table">
          <thead>
            <tr>
              <th>
                <input
                  type="checkbox"
                  checked={items.length > 0 && items.every((x) => selected.has(x.id))}
                  onChange={toggleAllOnPage}
                />
              </th>
              <th>ad_asin</th>
              <th>clicks</th>
              <th>impressions</th>
              <th>purchases</th>
              <th {...sortableThProps('ad_cost')}>ad_cost{sortMark('ad_cost')}</th>
              <th {...sortableThProps('sales_1d')}>sales_1d{sortMark('sales_1d')}</th>
              <th {...sortableThProps('ad_sales_1d')}>ad_sales_1d{sortMark('ad_sales_1d')}</th>
              <th {...sortableThProps('tad_sales')}>Tad_sales{sortMark('tad_sales')}</th>
              <th {...sortableThProps('tsales')}>tsales{sortMark('tsales')}</th>
            </tr>
          </thead>
          <tbody>
            {items.map((r) => (
              <tr key={r.id}>
                <td>
                  <input type="checkbox" checked={selected.has(r.id)} onChange={() => toggleSelected(r.id)} />
                </td>
                <td>{r.ad_asin ?? '–'}</td>
                <td>{r.clicks ?? '–'}</td>
                <td>{r.impressions ?? '–'}</td>
                <td>{r.purchases ?? '–'}</td>
                <td>{r.ad_cost ?? '–'}</td>
                <td>{r.sales_1d ?? '–'}</td>
                <td>{r.ad_sales_1d ?? '–'}</td>
                <td>{r.tad_sales ?? '–'}</td>
                <td>{r.tsales ?? '–'}</td>
              </tr>
            ))}
            {!loading && items.length === 0 && (
              <tr>
                <td colSpan={10} className="empty-hint">No data.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
