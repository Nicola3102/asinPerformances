import { type GroupFResponse, type GroupFRow } from '../api/client'
import { formatCreatedAt } from './formatters'

function escapeCsvCell(val: string | number): string {
  const s = String(val)
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`
  return s
}

export function groupFRowsToCsv(rows: GroupFRow[]): string {
  const header = ['variation_id', 'Parent ASIN', 'Created At', 'store_id', 'impression_count_asin', 'order_asin', 'sessions_asin']
  const lines = [header.map(escapeCsvCell).join(',')]
  for (const r of rows) {
    const created = formatCreatedAt(r.created_at)
    lines.push(
      [
        r.variation_id ?? '',
        r.parent_asin ?? '',
        created,
        r.store_id ?? '',
        r.impression_count_asin ?? '',
        r.order_asin ?? '',
        r.sessions_asin ?? '',
      ].map(escapeCsvCell).join(',')
    )
  }
  return lines.join('\r\n')
}

export function downloadGroupFCsv(rows: GroupFRow[], filename?: string): void {
  const csv = groupFRowsToCsv(rows)
  const blob = new Blob(['\uFEFF' + csv], { type: 'text/csv;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename ?? `group-f-${new Date().toISOString().slice(0, 10)}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

const GROUP_F_CACHE_STORAGE_KEY = 'group-f-cache-v1'

export type GroupFCacheEntry = {
  savedAt: string
  data: GroupFResponse
}

export function loadGroupFCache(): Record<string, GroupFCacheEntry> {
  if (typeof window === 'undefined') return {}
  try {
    const raw = window.localStorage.getItem(GROUP_F_CACHE_STORAGE_KEY)
    if (!raw) return {}
    const parsed = JSON.parse(raw)
    return parsed && typeof parsed === 'object' ? parsed as Record<string, GroupFCacheEntry> : {}
  } catch {
    return {}
  }
}

export function saveGroupFCache(cache: Record<string, GroupFCacheEntry>): void {
  if (typeof window === 'undefined') return
  try {
    window.localStorage.setItem(GROUP_F_CACHE_STORAGE_KEY, JSON.stringify(cache))
  } catch {
    /* ignore storage failures */
  }
}

export function formatCacheTime(v: string | null | undefined): string {
  if (!v) return ''
  try {
    const d = new Date(v)
    if (Number.isNaN(d.getTime())) return v
    return d.toLocaleString('zh-CN', { dateStyle: 'short', timeStyle: 'medium', hour12: false })
  } catch {
    return v
  }
}
