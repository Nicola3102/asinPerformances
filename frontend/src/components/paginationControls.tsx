import { useEffect, useState } from 'react'

export function PaginationControls({
  currentPage,
  totalPages,
  onChangePage,
}: {
  currentPage: number
  totalPages: number
  onChangePage: (page: number) => void | Promise<void>
}) {
  const [pageInput, setPageInput] = useState(String(currentPage))

  useEffect(() => {
    setPageInput(String(currentPage))
  }, [currentPage])

  const jumpToPage = () => {
    const next = Number(pageInput)
    if (Number.isNaN(next)) return
    const target = Math.min(Math.max(1, Math.trunc(next)), totalPages)
    void onChangePage(target)
  }

  return (
    <div className="pagination-btns">
      <button
        type="button"
        disabled={currentPage <= 1}
        onClick={() => { void onChangePage(Math.max(1, currentPage - 1)) }}
      >
        上一页
      </button>
      <span>第 {currentPage} / {totalPages} 页</span>
      <button
        type="button"
        disabled={currentPage >= totalPages}
        onClick={() => { void onChangePage(Math.min(totalPages, currentPage + 1)) }}
      >
        下一页
      </button>
      <label className="pagination-jump">
        <span>跳转到</span>
        <input
          type="number"
          min={1}
          max={totalPages}
          value={pageInput}
          onChange={(e) => setPageInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') jumpToPage()
          }}
        />
        <span>页</span>
      </label>
      <button type="button" onClick={jumpToPage}>
        确定
      </button>
    </div>
  )
}
