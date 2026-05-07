import { useCallback, useEffect, useMemo, useState } from "react"
import type { ChartOptions, TooltipItem } from "chart.js"
import {
  getAdsProfit,
  type AdsProfitResponse,
  type AdsProfitSummary,
  type AdsProfitWeeklyPoint,
} from "../../api/client"
import { Chart } from "../../lib/chartRegister"
import "./adsRoutes.css"

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

  const returnCurveCutoffMs = useMemo(() => {
    const asOf = (endDate || latestInvoiceDate || '').trim()
    if (!asOf) return Number.NaN
    const d = new Date(asOf)
    if (Number.isNaN(d.getTime())) return Number.NaN
    return d.getTime() - (45 * 24 * 60 * 60 * 1000)
  }, [endDate, latestInvoiceDate])

  const deriveReturnCurveValue = useCallback((row: AdsProfitWeeklyPoint) => {
    const actualRaw = (typeof row.return_rate_actual === 'number' && Number.isFinite(row.return_rate_actual))
      ? row.return_rate_actual
      : null
    const predRaw = (typeof row.return_rate_predicted === 'number' && Number.isFinite(row.return_rate_predicted))
      ? row.return_rate_predicted
      : null
    if (actualRaw != null || predRaw != null) {
      // 后端明确标记为 predicted 的周，不展示真实曲线（避免 0% 贴地线）
      if (row.return_rate_curve_type === 'predicted') {
        return { actual: null, predicted: predRaw }
      }
      if (row.return_rate_curve_type === 'actual') {
        return { actual: actualRaw, predicted: null }
      }
      return { actual: actualRaw, predicted: predRaw }
    }
    if (!Number.isFinite(row.return_rate)) {
      return { actual: null, predicted: null }
    }
    // 兼容旧后端：未返回 actual/predicted 字段时，按 45 天分界拆分曲线
    const weekEnd = row.week_end ? new Date(row.week_end) : null
    if (!weekEnd || Number.isNaN(weekEnd.getTime()) || !Number.isFinite(returnCurveCutoffMs)) {
      return { actual: row.return_rate, predicted: null }
    }
    if (weekEnd.getTime() <= returnCurveCutoffMs) {
      return { actual: row.return_rate, predicted: null }
    }
    return { actual: null, predicted: row.return_rate }
  }, [returnCurveCutoffMs])

  const profitChartData = useMemo(() => {
    const labels = weeklySeries.map((row) => row.week_start || '–')
    const grossMarginAfterReturnData = weeklySeries.map((row) => (
      row.gross_margin_after_return_rate === 0 ? null : row.gross_margin_after_return_rate
    ))
    const hasGrossMarginAfterReturnData = grossMarginAfterReturnData.some((value) => value != null && Number.isFinite(value))
    const returnRateActualData = weeklySeries.map((row) => deriveReturnCurveValue(row).actual)
    const returnRatePredictedData = weeklySeries.map((row) => deriveReturnCurveValue(row).predicted)
    const hasReturnRateActualData = returnRateActualData.some((value) => value != null && Number.isFinite(value))
    const hasReturnRatePredictedData = returnRatePredictedData.some((value) => value != null && Number.isFinite(value))
    return {
      labels,
      datasets: [
        {
          type: 'bar' as const,
          label: '销售额',
          data: weeklySeries.map((row) => row.sales_amount),
          backgroundColor: '#60a5fa',
          stack: 'money',
          yAxisID: 'yMoney',
          order: 1,
        },
        {
          type: 'bar' as const,
          label: '退货金额',
          data: weeklySeries.map((row) => row.refund_amount),
          backgroundColor: '#fde047',
          stack: 'money',
          yAxisID: 'yMoney',
          order: 2,
        },
        {
          type: 'line' as const,
          label: '毛利率（不含退货）',
          data: weeklySeries.map((row) => row.gross_margin_rate),
          borderColor: '#4ade80',
          backgroundColor: 'rgba(74, 222, 128, 0.12)',
          yAxisID: 'yRate',
          tension: 0.25,
          pointRadius: 3,
          order: 0,
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
              borderColor: '#a855f7',
              backgroundColor: 'rgba(168, 85, 247, 0.12)',
              yAxisID: 'yReturnRate',
              tension: 0.25,
              pointRadius: 4,
              pointHoverRadius: 5,
              order: 0,
            }]
          : []),
        ...(hasReturnRatePredictedData
          ? [{
              type: 'line' as const,
              label: '退货率（预测）',
              data: returnRatePredictedData,
              borderColor: '#f59e0b',
              backgroundColor: 'rgba(245, 158, 11, 0.12)',
              yAxisID: 'yReturnRate',
              tension: 0.25,
              pointRadius: 4,
              pointHoverRadius: 5,
              borderDash: [6, 4],
              order: 0,
            }]
          : []),
      ],
    } as any
  }, [deriveReturnCurveValue, weeklySeries])

  const moneyMax = useMemo(() => {
    const vals = weeklySeries.map((row) => row.sales_amount + row.refund_amount).filter((v) => Number.isFinite(v))
    const max = vals.length ? Math.max(...vals) : 0
    return Math.max(1000, Math.ceil(max / 50000) * 50000)
  }, [weeklySeries])

  const rateMinMax = useMemo(() => {
    const vals = weeklySeries.flatMap((row) => [
      row.gross_margin_rate,
      ...(row.gross_margin_after_return_rate === 0 ? [] : [row.gross_margin_after_return_rate]),
      ...(deriveReturnCurveValue(row).actual != null ? [Number(deriveReturnCurveValue(row).actual)] : []),
      ...(deriveReturnCurveValue(row).predicted != null ? [Number(deriveReturnCurveValue(row).predicted)] : []),
    ]).filter((v) => Number.isFinite(v))
    if (!vals.length) return { min: -10, max: 20 }
    const min = Math.min(...vals, 0)
    const max = Math.max(...vals, 0)
    return {
      min: Math.floor(min / 5) * 5,
      max: Math.max(5, Math.ceil(max / 5) * 5),
    }
  }, [deriveReturnCurveValue, weeklySeries])

  const returnRateMinMax = useMemo(() => {
    const vals = weeklySeries.flatMap((row) => [
      ...(deriveReturnCurveValue(row).actual != null ? [Number(deriveReturnCurveValue(row).actual)] : []),
      ...(deriveReturnCurveValue(row).predicted != null ? [Number(deriveReturnCurveValue(row).predicted)] : []),
    ]).filter((v) => Number.isFinite(v))
    if (!vals.length) return { min: 0, max: 20 }
    const min = Math.min(...vals, 0)
    const max = Math.max(...vals, 5)
    return {
      min: Math.floor(min / 2) * 2,
      max: Math.max(6, Math.ceil(max / 2) * 2),
    }
  }, [deriveReturnCurveValue, weeklySeries])

  const profitChartOptions = useMemo<ChartOptions<'bar'>>(() => ({
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index' as const, intersect: false },
    plugins: {
      legend: { position: 'top' as const },
      tooltip: {
        callbacks: {
          label: (ctx: TooltipItem<'bar'>) => {
            const label = ctx.dataset.label || ''
            const value = Number(ctx.parsed.y ?? 0)
            if (label.includes('毛利率') || label.includes('退货率')) return `${label}: ${value.toFixed(2)}%`
            return `${label}: ${value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
          },
          afterBody: (items) => {
            const idx = Number(items?.[0]?.dataIndex ?? -1)
            const row = idx >= 0 ? weeklySeries[idx] : null
            if (!row) return []
            return [
              `订单数: ${row.order_count.toLocaleString()}`,
              `退货订单数: ${row.returned_order_count.toLocaleString()}`,
              `退货率(展示): ${row.return_rate.toFixed(2)}%`,
              `退货率(真实): ${deriveReturnCurveValue(row).actual != null ? `${Number(deriveReturnCurveValue(row).actual).toFixed(2)}%` : '–'}`,
              `退货率(预测): ${deriveReturnCurveValue(row).predicted != null ? `${Number(deriveReturnCurveValue(row).predicted).toFixed(2)}%` : '–'}`,
            ]
          },
        },
      },
    },
    scales: {
      x: {
        stacked: true,
        ticks: { maxRotation: 45, minRotation: 0 },
      },
      yMoney: {
        type: 'linear' as const,
        position: 'left',
        stacked: true,
        beginAtZero: true,
        max: moneyMax,
        title: { display: true, text: '销售额 / 退货金额' },
      },
      yRate: {
        type: 'linear' as const,
        position: 'right',
        min: rateMinMax.min,
        max: rateMinMax.max,
        grid: { drawOnChartArea: false },
        title: { display: true, text: '毛利率 (%)' },
        ticks: {
          callback: (value) => `${value}%`,
        },
      },
      yReturnRate: {
        type: 'linear' as const,
        position: 'right',
        offset: true,
        min: returnRateMinMax.min,
        max: returnRateMinMax.max,
        grid: { drawOnChartArea: false },
        title: { display: true, text: '退货率 (%)' },
        ticks: {
          callback: (value) => `${value}%`,
        },
      },
    },
  }), [moneyMax, rateMinMax.max, rateMinMax.min, returnRateMinMax.max, returnRateMinMax.min, weeklySeries])

  const metricCards = [
    {
      label: '销售额',
      value: summary.sales_amount.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
      accent: 'blue',
    },
    {
      label: '退货金额',
      value: summary.refund_amount.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
      accent: 'orange',
    },
    { label: '毛利', value: summary.gross_profit.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }), accent: 'green' },
    { label: '毛利率（不含退货）', value: `${summary.gross_margin_rate.toFixed(2)}%`, accent: 'teal' },
    { label: '毛利率（含退货）', value: `${summary.gross_margin_after_return_rate.toFixed(2)}%`, accent: 'red' },
  ]

  return (
    <div className="app">
      <h1>Total Profit</h1>
      <p className="monitor-desc">
        按 <code>order_profit.invoice_date</code> 从 2026-02-23 起按周聚合销售额、退货金额与毛利率；默认展示全部店铺，可按 <code>store_id</code> 切换。
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
            <h3>周利润趋势</h3>
            <p className="trend-chart-hint">柱状图为销售额 + 退货金额堆叠，折线为毛利率与每周退货率（真实/预测）</p>
          </div>
        </div>
        <div className="ads-line-chart-wrap">
          <Chart type="bar" data={profitChartData} options={profitChartOptions} />
        </div>
      </div>

      <div className="monitor-tables" style={{ marginTop: 10 }}>
        <table className="data-table monitor-track-table">
          <thead>
            <tr>
              <th>week_start</th>
              <th>week_end</th>
              <th>sales_amount</th>
              <th>refund_amount</th>
              <th>gross_profit</th>
              <th>gross_margin_rate</th>
              <th>gross_margin_after_return_rate</th>
              <th>return_rate</th>
              <th>return_rate_actual</th>
              <th>return_rate_predicted</th>
            </tr>
          </thead>
          <tbody>
            {weeklySeries.map((row, idx) => (
              <tr key={row.week_start || `week-${idx}`}>
                <td>{row.week_start ?? '–'}</td>
                <td>{row.week_end ?? '–'}</td>
                <td>{row.sales_amount.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
                <td>{row.refund_amount.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
                <td>{row.gross_profit.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
                <td>{row.gross_margin_rate.toFixed(2)}%</td>
                <td>{row.gross_margin_after_return_rate.toFixed(2)}%</td>
                <td>{row.return_rate.toFixed(2)}%</td>
                <td>{deriveReturnCurveValue(row).actual != null ? `${Number(deriveReturnCurveValue(row).actual).toFixed(2)}%` : '–'}</td>
                <td>{deriveReturnCurveValue(row).predicted != null ? `${Number(deriveReturnCurveValue(row).predicted).toFixed(2)}%` : '–'}</td>
              </tr>
            ))}
            {!loading && weeklySeries.length === 0 && (
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
