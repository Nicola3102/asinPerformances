import { useEffect, useMemo, useRef, useState, type FormEvent } from "react"
import { getTrendData, type TrendResponse, type TrendBatchOption } from "../../api/client"
import { formatDecimal } from "../../lib/formatters"
import { ZoomModal } from "../../components/searchQueryUi"
import {
  TrendBarOverviewCard,
  TrendChartFigure,
  TrendLineChartCard,
  EMPTY_TREND_FILTERS,
  type TrendFilterState,
  type TrendLineDef,
  TREND_WEEK_NO_MIN,
  buildListingTrackingWeekRange,
  parseOptionalInt,
} from "./trendShared"
import "./trendRoutes.css"

export function TrendPage() {
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
      const bo = options?.batch_options?.find((b: TrendBatchOption) => String(b.id) === bid)
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

  const handleApplyFilters = (e: FormEvent<HTMLFormElement>) => {
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
                  {(options?.store_ids ?? []).map((item: number) => (
                    <option key={item} value={String(item)}>{item}</option>
                  ))}
                </select>
              </label>
              <label className="trend-filter-quick-field">
                <span className="trend-filter-quick-label">used_model</span>
                <select value={filters.used_model} onChange={(e) => setFilters((prev) => ({ ...prev, used_model: e.target.value }))}>
                  <option value="">全部</option>
                  {(options?.used_models ?? []).map((item: string) => (
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
                  {(options?.batch_options ?? []).map((item: TrendBatchOption) => (
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
