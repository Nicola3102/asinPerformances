import {
  type AdSalesEnsureLatestResponse,
  type AdSalesListResponse,
  type AdsProfitResponse,
  type AsinPerformance,
  type AsinPerformanceCreate,
  type DetailResponse,
  type GroupADetailResponse,
  type GroupASummaryResponse,
  type GroupFLockStatus,
  type GroupFReleaseLockResponse,
  type GroupFResponse,
  type MonitorParentItem,
  type MonitorTrackResponse,
  type RefreshQueryStatusResponse,
  type SummaryRow,
  type SummaryRowConsolidated,
  type SummaryStatsResponse,
  type SyncFromOnlineResponse,
  type TableStats,
  type TrendResponse,
  type AdCheckSummaryResponse,
  type OperateGroupAResponse,
  type OperateSummaryResponse,
} from './types'
import { apiFetch, apiFetchBlob } from './fetch'

export type * from './types'

export { API_BASE, ApiError, apiFetch, apiFetchBlob, parseErrorResponse } from './fetch'

export async function getTableStats(): Promise<TableStats> {
  return apiFetch<TableStats>('/asin-performances/stats')
}

export async function listAsinPerformances(skip = 0, limit = 100): Promise<AsinPerformance[]> {
  return apiFetch<AsinPerformance[]>(`/asin-performances?skip=${skip}&limit=${limit}`)
}

export async function listSummary(): Promise<SummaryRow[]> {
  return apiFetch<SummaryRow[]>('/asin-performances/summary')
}

export async function listSummaryByWeek(week_no: number): Promise<SummaryRow[]> {
  return apiFetch<SummaryRow[]>(
    `/asin-performances/summary?week_no=${encodeURIComponent(String(week_no))}`,
  )
}

export async function listSummaryConsolidatedByWeek(week_no: number): Promise<SummaryRowConsolidated[]> {
  return apiFetch<SummaryRowConsolidated[]>(
    `/asin-performances/summary/consolidated?week_no=${encodeURIComponent(String(week_no))}`,
  )
}

export async function listWeeks(): Promise<number[]> {
  return apiFetch<number[]>('/asin-performances/weeks')
}

export async function getSummaryStats(): Promise<SummaryStatsResponse> {
  return apiFetch<SummaryStatsResponse>('/asin-performances/summary-stats')
}

export async function getDetail(parent_asin: string, week_no: number, store_id?: number | null): Promise<DetailResponse> {
  const params = new URLSearchParams({ parent_asin, week_no: String(week_no) })
  if (store_id != null) params.set('store_id', String(store_id))
  return apiFetch<DetailResponse>(`/asin-performances/detail?${params}`)
}

export async function downloadWeekData(week_no: number, parentAsins?: string[]): Promise<void> {
  const params = new URLSearchParams({ week_no: String(week_no) })
  if (parentAsins && parentAsins.length > 0) {
    for (const asin of parentAsins) {
      const v = (asin || '').trim()
      if (v) params.append('parent_asins', v)
    }
  }
  const blob = await apiFetchBlob(`/asin-performances/export?${params.toString()}`)
  const url = window.URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `asin_performances_week_${week_no}.csv`
  document.body.appendChild(a)
  a.click()
  a.remove()
  window.URL.revokeObjectURL(url)
}

export async function operateSummary(parent_asin: string, week_no: number): Promise<OperateSummaryResponse> {
  return apiFetch<OperateSummaryResponse>('/asin-performances/operate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ parent_asin, week_no }),
  })
}

export async function adCheckSummary(parent_asin: string, week_no: number): Promise<AdCheckSummaryResponse> {
  return apiFetch<AdCheckSummaryResponse>('/asin-performances/ad-check', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ parent_asin, week_no }),
  })
}

export async function refreshQueryStatus(week_no: number): Promise<RefreshQueryStatusResponse> {
  return apiFetch<RefreshQueryStatusResponse>('/asin-performances/query-status/refresh', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ week_no }),
  })
}

export async function listAdSales(params: {
  store_id?: number | null
  start_date?: string | null
  end_date?: string | null
  ensure_latest?: boolean
  sort?: string | null
  page?: number
  page_size?: number
}): Promise<AdSalesListResponse> {
  const qs = new URLSearchParams()
  if (params.store_id != null && !Number.isNaN(Number(params.store_id))) qs.set('store_id', String(params.store_id))
  if (params.start_date) qs.set('start_date', params.start_date)
  if (params.end_date) qs.set('end_date', params.end_date)
  if (params.ensure_latest) qs.set('ensure_latest', '1')
  if (params.sort) qs.set('sort', params.sort)
  if (params.page != null) qs.set('page', String(params.page))
  if (params.page_size != null) qs.set('page_size', String(params.page_size))
  return apiFetch<AdSalesListResponse>(`/ads/ad-sales?${qs.toString()}`)
}

export async function triggerAdSalesEnsureLatest(): Promise<AdSalesEnsureLatestResponse> {
  return apiFetch<AdSalesEnsureLatestResponse>('/ads/ad-sales/ensure-latest', { method: 'POST' })
}

export async function downloadAdSales(ids: number[]): Promise<void> {
  const qs = new URLSearchParams()
  for (const id of ids) {
    if (Number.isFinite(id) && id > 0) qs.append('ids', String(id))
  }
  const blob = await apiFetchBlob(`/ads/ad-sales/export?${qs.toString()}`)
  const url = window.URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = 'ad_sales_selected.csv'
  document.body.appendChild(a)
  a.click()
  a.remove()
  window.URL.revokeObjectURL(url)
}

export async function getAdsProfit(params: {
  store_id?: number | null
  start_date?: string | null
  end_date?: string | null
}): Promise<AdsProfitResponse> {
  const qs = new URLSearchParams()
  if (params.store_id != null && !Number.isNaN(Number(params.store_id))) qs.set('store_id', String(params.store_id))
  if (params.start_date) qs.set('start_date', params.start_date)
  if (params.end_date) qs.set('end_date', params.end_date)
  return apiFetch<AdsProfitResponse>(`/ads/revenue?${qs.toString()}`)
}

export async function syncFromOnline(): Promise<SyncFromOnlineResponse> {
  return apiFetch<SyncFromOnlineResponse>('/sync-from-online', { method: 'POST' })
}

export async function getGroupFData(
  scanWeeks: number,
  weekNos?: number[] | null,
  signal?: AbortSignal,
): Promise<GroupFResponse> {
  const params = new URLSearchParams()
  if (weekNos != null && weekNos.length > 0) {
    weekNos.forEach((w) => params.append('week_nos', String(w)))
  } else {
    params.set('scan_weeks', String(scanWeeks))
  }
  return apiFetch<GroupFResponse>(`/asin-performances/group-f?${params.toString()}`, { signal })
}

export async function getGroupFLockStatus(signal?: AbortSignal): Promise<GroupFLockStatus> {
  return apiFetch<GroupFLockStatus>('/asin-performances/group-f/status', { signal })
}

export async function releaseGroupFLock(): Promise<GroupFReleaseLockResponse> {
  return apiFetch<GroupFReleaseLockResponse>('/asin-performances/group-f/release-lock', { method: 'POST' })
}

export async function listGroupAWeeks(): Promise<number[]> {
  return apiFetch<number[]>('/asin-performances/group-a/weeks')
}

export async function getGroupASummary(
  week_no?: number | null,
  page = 1,
  page_size = 30,
): Promise<GroupASummaryResponse> {
  const params = new URLSearchParams({
    page: String(page),
    page_size: String(page_size),
  })
  if (week_no != null) params.set('week_no', String(week_no))
  return apiFetch<GroupASummaryResponse>(`/asin-performances/group-a/summary?${params.toString()}`)
}

export async function getGroupADetail(
  parent_asin: string,
  week_no: number,
  store_id: number,
): Promise<GroupADetailResponse> {
  const params = new URLSearchParams({
    parent_asin,
    week_no: String(week_no),
    store_id: String(store_id),
  })
  return apiFetch<GroupADetailResponse>(`/asin-performances/group-a/detail?${params.toString()}`)
}

export async function operateGroupA(
  parent_asin: string,
  store_id: number,
  week_no: number,
): Promise<OperateGroupAResponse> {
  return apiFetch<OperateGroupAResponse>('/asin-performances/group-a/operate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ parent_asin, store_id, week_no }),
  })
}

export async function downloadGroupAData(week_no: number, parentStoreKeys?: string[]): Promise<void> {
  const params = new URLSearchParams({ week_no: String(week_no) })
  if (parentStoreKeys && parentStoreKeys.length > 0) {
    for (const key of parentStoreKeys) {
      const v = (key || '').trim()
      if (v) params.append('parent_store_keys', v)
    }
  }
  const blob = await apiFetchBlob(`/asin-performances/group-a/export?${params.toString()}`)
  const url = window.URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `group_a_week_${week_no}.csv`
  document.body.appendChild(a)
  a.click()
  a.remove()
  window.URL.revokeObjectURL(url)
}

export async function getMonitorParents(): Promise<MonitorParentItem[]> {
  return apiFetch<MonitorParentItem[]>('/asin-performances/monitor/parents')
}

export async function getMonitorTrack(parent_asin: string): Promise<MonitorTrackResponse> {
  return apiFetch<MonitorTrackResponse>(
    `/asin-performances/monitor/track?${new URLSearchParams({ parent_asin })}`,
  )
}

export async function getTrendData(filters?: {
  store_id?: number | null
  used_model?: string | null
  created_at_start?: string | null
  created_at_end?: string | null
  pid_min?: number | null
  pid_max?: number | null
  parent_asin?: string | null
  week_nos?: number[] | null
  batch_id?: number | null
}): Promise<TrendResponse> {
  const params = new URLSearchParams()
  if (filters?.store_id != null) params.set('store_id', String(filters.store_id))
  if (filters?.used_model) params.set('used_model', filters.used_model)
  if (filters?.created_at_start) params.set('created_at_start', filters.created_at_start)
  if (filters?.created_at_end) params.set('created_at_end', filters.created_at_end)
  if (filters?.pid_min != null) params.set('pid_min', String(filters.pid_min))
  if (filters?.pid_max != null) params.set('pid_max', String(filters.pid_max))
  if (filters?.parent_asin) params.set('parent_asin', filters.parent_asin)
  if (filters?.week_nos?.length) {
    for (const w of filters.week_nos) {
      params.append('week_no', String(w))
    }
  }
  if (filters?.batch_id != null) params.set('batch_id', String(filters.batch_id))
  const query = params.toString()
  return apiFetch<TrendResponse>(`/trend${query ? `?${query}` : ''}`)
}

export async function createAsinPerformance(data: AsinPerformanceCreate): Promise<AsinPerformance> {
  return apiFetch<AsinPerformance>('/asin-performances', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
}
