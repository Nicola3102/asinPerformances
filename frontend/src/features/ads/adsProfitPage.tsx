import { useCallback, useEffect, useMemo, useState } from "react"
import type { ChartOptions, Plugin, TooltipItem } from "chart.js"
import {
  getAdsProfit,
  type AdsProfitResponse,
  type AdsProfitSummary,
  type AdsProfitWeeklyPoint,
} from "../../api/client"
import { Chart } from "../../lib/chartRegister"
import "./adsRoutes.css"

function deriveRefundParts(row: AdsProfitWeeklyPoint): { actual: number; predicted: number } {
  const actual = Number(row.refund_amount_actual ?? row.refund_amount ?? 0)
  const predictedFromApi = Number(row.refund_amount_predicted ?? 0)
  if (Number.isFinite(predictedFromApi) && predictedFromApi > 0) {
    return { actual: Number.isFinite(actual) ? actual : 0, predicted: predictedFromApi }
  }
  // 兼容旧后端：未返回 refund_amount_predicted 时，在“预测周”用 退货率(展示)*净收益额 - 真实退货额 估算
  const rate = Number(row.return_rate)
  const sales = Number(row.sales_amount)
  const actualSafe = Number.isFinite(actual) ? actual : 0
  if (row.return_rate_curve_type === 'predicted' && Number.isFinite(rate) && Number.isFinite(sales) && sales > 0) {
    const estTotal = (rate / 100) * sales
    return { actual: actualSafe, predicted: Math.max(0, estTotal - actualSafe) }
  }
  return { actual: actualSafe, predicted: 0 }
}

function deriveAdjustedSalesAmount(row: AdsProfitWeeklyPoint): number {
  const sales = Number(row.sales_amount)
  const refund = deriveRefundParts(row)
  return (Number.isFinite(sales) ? sales : 0) - refund.actual - refund.predicted
}

function deriveWeeklyProfitAmount(row: AdsProfitWeeklyPoint): number {
  // row.gross_profit 为“已扣当周广告费用”的毛利；页面展示口径需再扣真实+预估退货金额
  const grossProfitAfterAd = Number(row.gross_profit)
  const refund = deriveRefundParts(row)
  return (Number.isFinite(grossProfitAfterAd) ? grossProfitAfterAd : 0) - refund.actual - refund.predicted
}

function deriveActualReturnRate(row: AdsProfitWeeklyPoint): number | null {
  if (row.return_rate_curve_type === 'predicted') return null
  const actualFromApi = Number(row.return_rate_actual)
  if (Number.isFinite(actualFromApi)) return actualFromApi
  const sales = Number(row.sales_amount)
  const actualRefund = deriveRefundParts(row).actual
  if (!(Number.isFinite(sales) && sales > 0)) return null
  return (actualRefund / sales) * 100
}

function derivePredictedReturnRate(row: AdsProfitWeeklyPoint): number | null {
  if (row.return_rate_curve_type && row.return_rate_curve_type !== 'predicted') return null
  const predictedFromApi = Number(row.return_rate_predicted)
  if (Number.isFinite(predictedFromApi)) return predictedFromApi
  return null
}

function alignBoundsToZeroRatio(rawMin: number, rawMax: number, zeroRatio: number): { min: number; max: number } {
  const safeMin = Number.isFinite(rawMin) ? rawMin : 0
  const safeMax = Number.isFinite(rawMax) ? rawMax : 0
  const ratio = Number.isFinite(zeroRatio) ? Math.min(0.95, Math.max(0.05, zeroRatio)) : 0.5

  if (safeMin === 0 && safeMax === 0) return { min: -1, max: 1 }

  if (safeMin >= 0) {
    const max = safeMax > 0 ? safeMax : 1
    return { min: -max * ratio / (1 - ratio), max }
  }

  if (safeMax <= 0) {
    const min = safeMin < 0 ? safeMin : -1
    return { min, max: (-min * (1 - ratio)) / ratio }
  }

  const maxFromMin = (-safeMin * (1 - ratio)) / ratio
  if (maxFromMin >= safeMax) {
    return { min: safeMin, max: maxFromMin }
  }

  return {
    min: (-safeMax * ratio) / (1 - ratio),
    max: safeMax,
  }
}

const salesTopBorderPlugin: Plugin<'bar'> = {
  id: 'ads-profit-sales-top-border',
  afterDatasetsDraw(chart) {
    const dsIndex = chart.data.datasets.findIndex((ds) => ds.label === '销售收入')
    if (dsIndex < 0) return
    const meta = chart.getDatasetMeta(dsIndex)
    if (!meta?.data?.length) return
    const ctx = chart.ctx
    ctx.save()
    ctx.strokeStyle = '#1d4ed8'
    ctx.lineWidth = 4
    ctx.lineCap = 'round'
    for (const el of meta.data as Array<{ x: number; y: number; width?: number }>) {
      const width = Number(el?.width ?? 0)
      const x = Number(el?.x ?? 0)
      const y = Number(el?.y ?? 0)
      if (!Number.isFinite(width) || width <= 0 || !Number.isFinite(x) || !Number.isFinite(y)) continue
      const half = width / 2
      ctx.beginPath()
      ctx.moveTo(x - half, y)
      ctx.lineTo(x + half, y)
      ctx.stroke()
    }
    ctx.restore()
  },
}

export function AdsProfitPage() {
  const emptySummary: AdsProfitSummary = {
    start_date: '2026-02-23',
    end_date: '',
    store_id: null,
    order_count: 0,
    returned_order_count: 0,
    return_row_count: 0,
    sales_amount: 0,
    refund_amount: 0,
    gross_profit: 0,
    gross_profit_after_return: 0,
    gross_margin_rate: 0,
    gross_margin_after_return_rate: 0,
    return_rate: 0,
    ad_cost: 0,
    ad_cost_to_sales_pct: 0,
    ad_cost_usd: 0,
  }
  const [storeId, setStoreId] = useState<string>('all')
  const [storeIds, setStoreIds] = useState<number[]>([])
  const [startDate, setStartDate] = useState<string>('2026-02-23')
  const [endDate, setEndDate] = useState<string>('')
  const [latestInvoiceDate, setLatestInvoiceDate] = useState<string>('')
  const [loading, setLoading] = useState<boolean>(false)
  const [error, setError] = useState<string | null>(null)
  const [summary, setSummary] = useState<AdsProfitSummary>(emptySummary)
  const [weeklySeries, setWeeklySeries] = useState<AdsProfitWeeklyPoint[]>([])

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const sid = storeId === 'all' ? null : Number(storeId)
      const res: AdsProfitResponse = await getAdsProfit({
        store_id: Number.isFinite(sid as number) ? (sid as number) : null,
        start_date: startDate || null,
        end_date: endDate || null,
      })
      setStoreIds(Array.isArray(res.store_ids) ? res.store_ids : [])
      setSummary(res.summary || emptySummary)
      setWeeklySeries(Array.isArray(res.weekly_series) ? res.weekly_series : [])
      setLatestInvoiceDate(res.latest_invoice_date || '')
      if (!endDate && res.end_date) setEndDate(res.end_date)
      if (!startDate && res.start_date) setStartDate(res.start_date)
    } catch (e) {
      setError(e instanceof Error ? e.message : '加载失败')
    } finally {
      setLoading(false)
    }
  }, [endDate, startDate, storeId])

  useEffect(() => {
    void load()
  }, [])

  const profitChartData = useMemo(() => {
    const labels = weeklySeries.map((row) => row.week_start || '–')
    const salesBarData = weeklySeries.map((row) => deriveAdjustedSalesAmount(row))
    const weeklyProfitData = weeklySeries.map((row) => deriveWeeklyProfitAmount(row))
    const adCostData = weeklySeries.map((row) => Number(row.ad_cost ?? 0))
    const refundAmountData = weeklySeries.map((row) => {
      const refund = Number(row.refund_amount ?? 0)
      return Number.isFinite(refund) ? refund : 0
    })
    const grossMarginAfterReturnData = weeklySeries.map((row) => {
      const adjustedSales = deriveAdjustedSalesAmount(row)
      if (!(adjustedSales > 0)) return null
      return (deriveWeeklyProfitAmount(row) / adjustedSales) * 100
    })
    const returnRateActualData = weeklySeries.map((row) => deriveActualReturnRate(row))
    const returnRatePredictedData = weeklySeries.map((row) => derivePredictedReturnRate(row))
    const hasGrossMarginAfterReturnData = grossMarginAfterReturnData.some((value) => value != null && Number.isFinite(value))
    const hasReturnRateActualData = returnRateActualData.some((value) => value != null && Number.isFinite(value))
    const hasReturnRatePredictedData = returnRatePredictedData.some((value) => value != null && Number.isFinite(value))
    return {
      labels,
      datasets: [
        {
          type: 'bar' as const,
          label: '销售收入',
          data: salesBarData,
          backgroundColor: 'rgba(96, 165, 250, 0.85)',
          grouped: false,
          barPercentage: 0.72,
          categoryPercentage: 0.72,
          yAxisID: 'yMoney',
          order: 10,
        },
        {
          type: 'line' as const,
          label: '当周毛利',
          data: weeklyProfitData,
          borderColor: '#22c55e',
          backgroundColor: 'rgba(34, 197, 94, 0.12)',
          yAxisID: 'yMoney',
          tension: 0.25,
          pointRadius: 3,
          pointHoverRadius: 5,
          order: 2,
        },
        {
          type: 'line' as const,
          label: '当周广告费用',
          data: adCostData,
          borderColor: '#8b5cf6',
          backgroundColor: 'rgba(139, 92, 246, 0.12)',
          yAxisID: 'yMoney',
          tension: 0.25,
          pointRadius: 3,
          pointHoverRadius: 5,
          order: 2,
        },
        {
          type: 'line' as const,
          label: '当周退货金额',
          data: refundAmountData,
          borderColor: '#f59e0b',
          backgroundColor: 'rgba(245, 158, 11, 0.12)',
          yAxisID: 'yMoney',
          tension: 0.25,
          pointRadius: 3,
          pointHoverRadius: 5,
          order: 2,
        },
        ...(hasGrossMarginAfterReturnData
          ? [{
              type: 'line' as const,
              label: '毛利率（含退货）',
              data: grossMarginAfterReturnData,
              borderColor: '#f87171',
              backgroundColor: 'rgba(248, 113, 113, 0.12)',
              yAxisID: 'yRate',
              tension: 0.25,
              pointRadius: 3,
              order: 0,
            }]
          : []),
        ...(hasReturnRateActualData
          ? [{
              type: 'line' as const,
              label: '退货率（真实）',
              data: returnRateActualData,
              borderColor: '#0f766e',
              backgroundColor: 'rgba(15, 118, 110, 0.12)',
              yAxisID: 'yRate',
              tension: 0.25,
              pointRadius: 3,
              pointHoverRadius: 5,
              order: 1,
            }]
          : []),
        ...(hasReturnRatePredictedData
          ? [{
              type: 'line' as const,
              label: '退货率（预估）',
              data: returnRatePredictedData,
              borderColor: '#14b8a6',
              backgroundColor: 'rgba(20, 184, 166, 0.12)',
              yAxisID: 'yRate',
              tension: 0.25,
              pointRadius: 3,
              pointHoverRadius: 5,
              borderDash: [6, 6],
              order: 1,
            }]
          : []),
      ],
      // Mixed bar/line：Chart.js 泛型过窄，用断言生成选项对象
      // eslint-disable-next-line @typescript-eslint/no-explicit-any -- ChartConfiguration 混合数据集
    } as any
  }, [weeklySeries])

  const moneyAxisBounds = useMemo(() => {
    const values = weeklySeries.flatMap((row) => [
      deriveAdjustedSalesAmount(row),
      deriveWeeklyProfitAmount(row),
      Number(row.ad_cost ?? 0),
      Number(row.refund_amount ?? 0),
    ]).filter((v) => Number.isFinite(v))
    if (!values.length) return { min: -1000, max: 1000, zeroRatio: 0.5 }

    const rawMin = Math.min(...values, 0)
    const rawMax = Math.max(...values, 0)
    const span = Math.max(rawMax - rawMin, 1)
    const paddedMin = rawMin < 0 ? rawMin - span * 0.08 : 0
    const paddedMax = rawMax > 0 ? rawMax + span * 0.08 : 0
    const maxAbs = Math.max(Math.abs(paddedMin), Math.abs(paddedMax))
    const step = maxAbs >= 100000 ? 50000 : maxAbs >= 20000 ? 10000 : maxAbs >= 5000 ? 5000 : 1000
    const min = paddedMin < 0 ? Math.floor(paddedMin / step) * step : 0
    const max = paddedMax > 0 ? Math.ceil(paddedMax / step) * step : step
    const zeroRatio = min < 0 && max > 0 ? (-min / (max - min)) : 0.5
    return { min, max, zeroRatio }
  }, [weeklySeries])

  const rateAxisBounds = useMemo(() => {
    const vals = weeklySeries
      .flatMap((row) => {
        const adjustedSales = deriveAdjustedSalesAmount(row)
        const grossMargin = adjustedSales > 0 ? (deriveWeeklyProfitAmount(row) / adjustedSales) * 100 : null
        return [grossMargin, deriveActualReturnRate(row), derivePredictedReturnRate(row)]
      })
      .filter((v): v is number => v != null && Number.isFinite(v))
    if (!vals.length) return { min: -10, max: 10 }

    const rawMin = Math.min(...vals)
    const rawMax = Math.max(...vals)
    const aligned = alignBoundsToZeroRatio(rawMin, rawMax, moneyAxisBounds.zeroRatio)
    return {
      min: Math.floor(aligned.min / 5) * 5,
      max: Math.ceil(aligned.max / 5) * 5,
    }
  }, [moneyAxisBounds.zeroRatio, weeklySeries])

  const profitChartOptions = useMemo<ChartOptions<'bar'>>(() => ({
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index' as const, intersect: false },
    plugins: {
      legend: {
        position: 'top' as const,
      },
      tooltip: {
        callbacks: {
          label: (ctx: TooltipItem<'bar'>) => {
            const label = ctx.dataset.label || ''
            const idx = Number(ctx.dataIndex ?? -1)
            const row = idx >= 0 ? weeklySeries[idx] : null
            const value = Number(ctx.parsed.y ?? 0)
            if (label.includes('毛利率') || label.includes('退货率') || label.includes('费销比')) return `${label}: ${value.toFixed(2)}%`
            if (row && label === '销售收入') {
              const displayValue = deriveAdjustedSalesAmount(row)
              return `${label}: ${displayValue.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
            }
            const money = value
            return `${label}: ${money.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
          },
          afterBody: (items) => {
            const idx = Number(items?.[0]?.dataIndex ?? -1)
            const row = idx >= 0 ? weeklySeries[idx] : null
            if (!row) return []
            const refundParts = deriveRefundParts(row)
            return [
              `订单数: ${row.order_count.toLocaleString()}`,
              `退货订单数: ${row.returned_order_count.toLocaleString()}`,
              `退货金额(真实): ${refundParts.actual.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
              `退货金额(预估): ${refundParts.predicted.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
              `当周加权汇率: ${(row.ad_fx_rate ?? 1).toFixed(6)}`,
            ]
          },
        },
      },
    },
    scales: {
      x: {
        ticks: { maxRotation: 45, minRotation: 0 },
      },
      yMoney: {
        type: 'linear' as const,
        position: 'left',
        min: moneyAxisBounds.min,
        max: moneyAxisBounds.max,
        title: { display: true, text: '销售收入（已扣真实+预估退货）' },
      },
      yRate: {
        type: 'linear' as const,
        position: 'right',
        min: rateAxisBounds.min,
        max: rateAxisBounds.max,
        grid: { drawOnChartArea: false },
        title: { display: true, text: '毛利率 / 退货率 (%)' },
        ticks: {
          callback: (value) => `${value}%`,
        },
      },
    },
  }), [moneyAxisBounds.max, moneyAxisBounds.min, rateAxisBounds.max, rateAxisBounds.min, weeklySeries])

  const refundAmountTotal = useMemo(() => {
    if (weeklySeries.length === 0) return Number(summary.refund_amount ?? 0)
    return weeklySeries.reduce((acc, row) => {
      const refund = Number(row.refund_amount ?? 0)
      return acc + (Number.isFinite(refund) ? refund : 0)
    }, 0)
  }, [summary.refund_amount, weeklySeries])

  const metricCards = [
    {
      label: '销售收入',
      value: summary.sales_amount.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
      accent: 'sales',
    },
    { label: '毛利', value: summary.gross_profit.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }), accent: 'profit' },
    { label: '毛利率', value: `${summary.gross_margin_rate.toFixed(2)}%`, accent: 'margin' },
    {
      label: '退货金额',
      // 页面顶部口径：按周汇总真实退货 + 预估退货（weekly_series.refund_amount）
      value: refundAmountTotal.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
      accent: 'refund',
    },
    {
      label: '广告费用',
      value: (summary.ad_cost ?? 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
      accent: 'ad-cost',
    },
  ]

  return (
    <div className="app">
      <h1>Revenue</h1>
      <p className="monitor-desc">
        <strong>销售收入</strong>为 <code>order_profit.net_revenue</code> 汇总扣除平台费后减去真实+预估退货金额；
        <br />
        <strong>毛利</strong>为 <code>order_profit.gross_profit</code> 汇总扣除运费后，再减去真实+预估退货金额，再减去广告费用。
        <br />
        <strong>退货金额</strong>为 45 天前真实退货金额与 45 天至今预估退货金额之和。
        <br />
        当前最新 invoice_date：<code>{latestInvoiceDate || '–'}</code>
      </p>

      <div className="monitor-controls" style={{ alignItems: 'flex-end' }}>
        <label>
          <span>store_id</span>
          <select className="monitor-select" value={storeId} onChange={(e) => setStoreId(e.target.value)}>
            <option value="all">全部店铺</option>
            {storeIds.map((id) => (
              <option key={id} value={String(id)}>{id}</option>
            ))}
          </select>
        </label>
        <label>
          <span>start</span>
          <input className="monitor-select" type="date" value={startDate} onChange={(e) => setStartDate(e.target.value)} />
        </label>
        <label>
          <span>end</span>
          <input className="monitor-select" type="date" value={endDate} onChange={(e) => setEndDate(e.target.value)} />
        </label>
        <button className="btn" onClick={() => void load()} disabled={loading}>Search</button>
      </div>

      {error && <div className="error" style={{ marginTop: 10 }}>{error}</div>}
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
            <h3>周营收趋势</h3>
          </div>
        </div>
        <div className="ads-line-chart-wrap">
          <Chart type="bar" data={profitChartData} options={profitChartOptions} plugins={[salesTopBorderPlugin]} />
        </div>
      </div>

      <div className="monitor-tables" style={{ marginTop: 10 }}>
        <table className="data-table monitor-track-table">
          <thead>
            <tr>
              <th>week_start</th>
              <th>week_end</th>
              <th>销售收入</th>
              <th>当周毛利</th>
              <th>广告费用</th>
              <th>退货金额(真实)</th>
              <th>退货金额(预估)</th>
              <th>毛利率(含退货)</th>
            </tr>
          </thead>
          <tbody>
            {weeklySeries.map((row, idx) => (
              <tr key={row.week_start || `week-${idx}`}>
                <td>{row.week_start ?? '–'}</td>
                <td>{row.week_end ?? '–'}</td>
                <td>{deriveAdjustedSalesAmount(row).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
                <td>{deriveWeeklyProfitAmount(row).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
                <td>{(row.ad_cost ?? 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
                <td>{(row.refund_amount_actual ?? 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
                <td>{(row.refund_amount_predicted ?? 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
                <td>{(deriveAdjustedSalesAmount(row) > 0 ? (deriveWeeklyProfitAmount(row) / deriveAdjustedSalesAmount(row) * 100) : 0).toFixed(2)}%</td>
              </tr>
            ))}
            {!loading && weeklySeries.length === 0 && (
              <tr>
                <td colSpan={7} className="empty-hint">No data.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
