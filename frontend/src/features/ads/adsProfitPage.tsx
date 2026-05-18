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
    const hasGrossMarginAfterReturnData = grossMarginAfterReturnData.some((value) => value != null && Number.isFinite(value))
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
      ],
      // Mixed bar/line：Chart.js 泛型过窄，用断言生成选项对象
      // eslint-disable-next-line @typescript-eslint/no-explicit-any -- ChartConfiguration 混合数据集
    } as any
  }, [weeklySeries])

  const moneyMax = useMemo(() => {
    const vals = weeklySeries
      .map((row) => deriveAdjustedSalesAmount(row))
      .filter((v) => Number.isFinite(v))
    const max = vals.length ? Math.max(...vals) : 0
    return Math.max(1000, Math.ceil(max / 50000) * 50000)
  }, [weeklySeries])

  const rateMinMax = useMemo(() => {
    const vals = weeklySeries
      .map((row) => {
        const adjustedSales = deriveAdjustedSalesAmount(row)
        if (!(adjustedSales > 0)) return null
        return (deriveWeeklyProfitAmount(row) / adjustedSales) * 100
      })
      .filter((v): v is number => v != null && Number.isFinite(v))
    if (!vals.length) return { min: -10, max: 20 }
    const min = Math.min(...vals, 0)
    const max = Math.max(...vals, 0)
    return {
      min: Math.floor(min / 5) * 5,
      max: Math.max(5, Math.ceil(max / 5) * 5),
    }
  }, [weeklySeries])

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
              `销售收入(原始): ${Number(row.sales_amount ?? 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
              `销售收入(扣退货): ${deriveAdjustedSalesAmount(row).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
              `当周毛利: ${deriveWeeklyProfitAmount(row).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
              `广告费用(本币): ${(row.ad_cost ?? 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
              `广告费用(USD): ${(row.ad_cost_usd ?? 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
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
        beginAtZero: true,
        max: moneyMax,
        title: { display: true, text: '销售收入（已扣真实+预估退货）' },
      },
      yRate: {
        type: 'linear' as const,
        position: 'right',
        min: rateMinMax.min,
        max: rateMinMax.max,
        grid: { drawOnChartArea: false },
        title: { display: true, text: '毛利率（含退货） (%)' },
        ticks: {
          callback: (value) => `${value}%`,
        },
      },
    },
  }), [moneyMax, rateMinMax.max, rateMinMax.min, weeklySeries])

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
      accent: 'blue',
    },
    { label: '毛利', value: summary.gross_profit.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }), accent: 'green' },
    { label: '毛利率', value: `${summary.gross_margin_rate.toFixed(2)}%`, accent: 'teal' },
    {
      label: '退货金额',
      // 页面顶部口径：按周汇总真实退货 + 预估退货（weekly_series.refund_amount）
      value: refundAmountTotal.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
      accent: 'orange',
    },
    {
      label: '广告费用',
      value: (summary.ad_cost ?? 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
      accent: 'purple',
    },
  ]

  return (
    <div className="app">
      <h1>Revenue</h1>
      <p className="monitor-desc">
        <strong>销售收入</strong>为 <code>order_profit.net_revenue</code> 汇总减去真实+预估退货金额；<strong>毛利</strong>为 <code>order_profit.gross_profit</code> 汇总减去真实+预估退货金额，再减去广告费用。
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
