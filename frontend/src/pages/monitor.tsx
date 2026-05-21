import { useEffect, useMemo, useState } from 'react'
import { getMonitorParents, getMonitorTrack, type MonitorParentItem, type MonitorTrackResponse } from '../api/client'
import { formatCreatedAt, formatNum } from '../lib/formatters'

const MONITOR_VOLUME_SPIKE_THRESHOLD = 200

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

export default function MonitorPage() {
  const [parents, setParents] = useState<MonitorParentItem[]>([])
  const [selectedParent, setSelectedParent] = useState('')
  const [parentSearch, setParentSearch] = useState('')
  const [track, setTrack] = useState<MonitorTrackResponse | null>(null)
  const [selectedIncompleteWeek, setSelectedIncompleteWeek] = useState<number | null>(null)
  const [loadingParents, setLoadingParents] = useState(true)
  const [loadingTrack, setLoadingTrack] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    setLoadingParents(true)
    setError(null)
    getMonitorParents()
      .then((list) => {
        if (cancelled) return
        setParents(list)
        setSelectedParent((prev) => prev || (list[0]?.parent_asin ?? ''))
      })
      .catch((e) => {
        if (!cancelled) setError(e instanceof Error ? e.message : 'Failed to load parents')
      })
      .finally(() => {
        if (!cancelled) setLoadingParents(false)
      })
    return () => {
      cancelled = true
    }
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
