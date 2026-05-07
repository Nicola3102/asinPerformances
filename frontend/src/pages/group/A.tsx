import { useEffect, useState } from 'react'
import {
  listGroupAWeeks,
  getGroupASummary,
  getGroupADetail,
  operateGroupA,
  downloadGroupAData,
  type GroupASummaryResponse,
  type GroupASummaryRow,
  type GroupADetailResponse,
  type GroupADetailChildRow,
} from '../../api/client'
import { formatCreatedAt, formatNum } from '../../lib/formatters'
import { SearchQueryTable, ZoomModal } from '../../components/searchQueryUi'
import { PaginationControls } from '../../components/paginationControls'

const SEARCH_QUERY_PREVIEW_LIMIT = 9




const GROUP_A_PAGE_SIZE = 30

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

export default function GroupAPage() {
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
                onChangePage={(pageNo: number) => handlePageChange(pageNo)}
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
