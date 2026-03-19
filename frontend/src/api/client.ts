const API_BASE = '/api';

export interface AsinPerformance {
  id: number;
  parent_asin: string | null;
  child_asin: string | null;
  parent_order_total: string | null;
  order_num: number | null;
  week_no: number | null;
  child_impression_count: number | null;
  child_session_count: number | null;
  search_query: string | null;
  search_query_volume: number | null;
  search_query_impression_count: number | null;
  search_query_purchase_count: number | null;
}

export interface AsinPerformanceCreate {
  parent_asin?: string;
  child_asin?: string;
  parent_order_total?: number;
  order_num?: number;
  week_no?: number;
  child_impression_count?: number;
  child_session_count?: number;
  search_query?: string;
  search_query_volume?: number;
  search_query_impression_count?: number;
  search_query_purchase_count?: number;
}

export interface SummaryRow {
  parent_asin: string | null;
  parent_asin_create_at: string | null;
  parent_order_total: string | number | null;
  week_no: number | null;
  store_id: number | null;
  operation_status?: boolean | null;
  operated_at?: string | null;
  checked_status?: string | null;
  checked_at?: string | null;
}

export interface SummaryRowConsolidated {
  parent_asin: string | null;
  parent_asin_create_at: string | null;
  parent_order_total: string | number | null;
  week_no: number | null;
  store_ids: number[];
  child_asins_with_orders: string[];
  operation_status?: boolean | null;
  operated_at?: string | null;
  checked_status?: string | null;
  checked_at?: string | null;
}

export interface SearchQueryRow {
  search_query: string | null;
  search_query_volume: number | null;
  search_query_impression_count: number | null;
  search_query_cart_count: number | null;
  search_query_total_impression: number | null;
  search_query_click_count: number | null;
  search_query_total_click: number | null;
  search_query_purchase_count: number | null;
}

export interface DetailChildRow {
  child_asin: string | null;
  child_impression_count: number | null;
  child_session_count: number | null;
  order_num: number | null;
  search_queries: SearchQueryRow[];
}

export interface DetailResponse {
  parent_asin: string | null;
  parent_order_total: string | number | null;
  week_no: number | null;
  children: DetailChildRow[];
}

export interface TableStats {
  count: number;
  table: string;
}

export interface WeekStatsRow {
  week_no: number | null;
  parent_asin_count: number;
  total_orders: number | null;
}

export interface SummaryStatsResponse {
  by_week: WeekStatsRow[];
}

export interface GroupFRow {
  variation_id: number | null;
  parent_asin: string | null;
  created_at: string | null;
  store_id: number | null;
  impression_count_asin: string | null;
  order_asin: string | null;
  sessions_asin: string | null;
}

export interface GroupFResponse {
  weeks: number[];
  business_weeks: number[];
  rows: GroupFRow[];
}

export interface GroupFLockStatus {
  lock_held: boolean;
  request_id: string | null;
  started_at: string | null;
  duration_seconds: number | null;
  is_stuck: boolean;
  message: string;
}

export interface GroupFReleaseLockResponse {
  released: boolean;
  had_lock: boolean;
  previous_request_id: string | null;
  message: string;
}

export interface GroupASummaryRow {
  parent_asin: string | null;
  store_id: number | null;
  created_at: string | null;
  week_no: number | null;
  total_impression_count: number;
  total_cart_count: number;
  total_session_count: number;
  operation_status?: boolean | null;
  operated_at?: string | null;
}

export interface GroupASummaryResponse {
  week_no: number | null;
  page: number;
  page_size: number;
  total: number;
  total_pages: number;
  rows: GroupASummaryRow[];
}

export interface GroupADetailChildRow {
  child_asin: string | null;
  child_impression_count: number | null;
  child_cart: number | null;
  child_session_count: number | null;
  search_queries: SearchQueryRow[];
}

export interface GroupADetailResponse {
  parent_asin: string | null;
  store_id: number | null;
  created_at: string | null;
  week_no: number | null;
  total_impression_count: number;
  total_cart_count: number;
  total_session_count: number;
  children: GroupADetailChildRow[];
}

export interface MonitorParentItem {
  parent_asin: string | null;
  operated_at: string | null;
}

export interface MonitorTrackRow {
  child_asin: string | null;
  week_no: number | null;
  search_query: string | null;
  search_query_volume: number | null;
  search_query_impression_count: number | null;
  search_query_click_count: number | null;
}

export interface MonitorTrackResponse {
  parent_asin: string | null;
  weeks: number[];
  rows: MonitorTrackRow[];
}

function parseErrorResponse(text: string, status: number): string {
  try {
    const err = text ? JSON.parse(text) : {}
    const d = err.detail
    if (typeof d === 'string') return d
    if (Array.isArray(d)) return d.map((x: { msg?: string }) => x?.msg || JSON.stringify(x)).join('; ')
    if (d != null) return JSON.stringify(d)
  } catch {
    /* ignore */
  }
  if (status === 502 || status === 504) return '后端超时或未就绪'
  if (status >= 500) return `后端错误 (${status})，请查看后端日志`
  return `请求失败 (${status})`
}

function buildApiError(text: string, status: number, fallback: string): Error & { status?: number } {
  const err = new Error(parseErrorResponse(text, status) || fallback) as Error & { status?: number }
  err.status = status
  return err
}

export async function getTableStats(): Promise<TableStats> {
  const res = await fetch(`${API_BASE}/asin-performances/stats`)
  if (!res.ok) {
    const text = await res.text()
    throw new Error(parseErrorResponse(text, res.status) || 'Failed to fetch stats')
  }
  return res.json()
}

export async function listAsinPerformances(skip = 0, limit = 100): Promise<AsinPerformance[]> {
  const res = await fetch(`${API_BASE}/asin-performances?skip=${skip}&limit=${limit}`)
  if (!res.ok) {
    const text = await res.text()
    throw new Error(parseErrorResponse(text, res.status) || 'Failed to fetch')
  }
  return res.json()
}

export async function listSummary(): Promise<SummaryRow[]> {
  const res = await fetch(`${API_BASE}/asin-performances/summary`)
  if (!res.ok) {
    const text = await res.text()
    throw new Error(parseErrorResponse(text, res.status) || 'Failed to fetch summary')
  }
  return res.json()
}

export async function listSummaryByWeek(week_no: number): Promise<SummaryRow[]> {
  const res = await fetch(`${API_BASE}/asin-performances/summary?week_no=${encodeURIComponent(String(week_no))}`)
  if (!res.ok) {
    const text = await res.text()
    throw new Error(parseErrorResponse(text, res.status) || 'Failed to fetch summary')
  }
  return res.json()
}

export async function listSummaryConsolidatedByWeek(week_no: number): Promise<SummaryRowConsolidated[]> {
  const res = await fetch(`${API_BASE}/asin-performances/summary/consolidated?week_no=${encodeURIComponent(String(week_no))}`)
  if (!res.ok) {
    const text = await res.text()
    throw new Error(parseErrorResponse(text, res.status) || 'Failed to fetch summary')
  }
  return res.json()
}

export async function listWeeks(): Promise<number[]> {
  const res = await fetch(`${API_BASE}/asin-performances/weeks`)
  if (!res.ok) {
    const text = await res.text()
    throw new Error(parseErrorResponse(text, res.status) || 'Failed to fetch weeks')
  }
  return res.json()
}

export async function getSummaryStats(): Promise<SummaryStatsResponse> {
  const res = await fetch(`${API_BASE}/asin-performances/summary-stats`)
  if (!res.ok) {
    const text = await res.text()
    throw new Error(parseErrorResponse(text, res.status) || 'Failed to fetch summary stats')
  }
  return res.json()
}

export async function getDetail(parent_asin: string, week_no: number, store_id?: number | null): Promise<DetailResponse> {
  const params = new URLSearchParams({ parent_asin, week_no: String(week_no) });
  if (store_id != null) params.set('store_id', String(store_id));
  const res = await fetch(`${API_BASE}/asin-performances/detail?${params}`);
  if (!res.ok) throw new Error('Failed to fetch detail');
  return res.json();
}

export async function downloadWeekData(week_no: number, parentAsins?: string[]): Promise<void> {
  const params = new URLSearchParams({ week_no: String(week_no) })
  if (parentAsins && parentAsins.length > 0) {
    for (const asin of parentAsins) {
      const v = (asin || '').trim()
      if (v) params.append('parent_asins', v)
    }
  }
  const res = await fetch(`${API_BASE}/asin-performances/export?${params.toString()}`)
  if (!res.ok) {
    const text = await res.text()
    throw new Error(parseErrorResponse(text, res.status) || 'Failed to export data')
  }
  const blob = await res.blob()
  const url = window.URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `asin_performances_week_${week_no}.csv`
  document.body.appendChild(a)
  a.click()
  a.remove()
  window.URL.revokeObjectURL(url)
}

export async function operateSummary(parent_asin: string, week_no: number): Promise<{ updated: number }> {
  const res = await fetch(`${API_BASE}/asin-performances/operate`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ parent_asin, week_no }),
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(parseErrorResponse(text, res.status) || '操作失败')
  }
  return res.json()
}

export async function refreshQueryStatus(week_no: number): Promise<{
  checked_groups: number
  completed_groups: number
  skipped_completed: number
  skipped_by_interval: number
}> {
  const res = await fetch(`${API_BASE}/asin-performances/query-status/refresh`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ week_no }),
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(parseErrorResponse(text, res.status) || '状态刷新失败')
  }
  return res.json()
}

export interface SyncCheck {
  rows_fetched_from_online: number;
  rows_inserted: number;
  local_table_count_after: number;
  table_name: string;
  insert_ok: boolean;
  step2_error?: string | null;
  message?: string | null;
}

export async function syncFromOnline(): Promise<{
  status: string;
  rows_synced: number;
  message?: string;
  check?: SyncCheck;
}> {
  let res: Response
  try {
    res = await fetch(`${API_BASE}/sync-from-online`, { method: 'POST' })
  } catch (e) {
    const msg = e instanceof Error ? e.message : '网络错误'
    throw new Error(`无法连接后端：${msg}。请确认后端已启动（如 docker compose up）。`)
  }
  if (!res.ok) {
    const text = await res.text()
    let msg = 'Sync failed'
    try {
      const err = text ? JSON.parse(text) : {}
      const detail = err.detail
      if (typeof detail === 'string') msg = detail
      else if (Array.isArray(detail)) msg = detail.map((d: { msg?: string }) => d?.msg || JSON.stringify(d)).join('; ')
      else if (detail != null) msg = JSON.stringify(detail)
    } catch {
      if (res.status === 502 || res.status === 504) msg = '后端超时或未就绪，请稍后重试'
      else if (res.status >= 500) msg = `后端错误 (${res.status})，请查看后端日志`
    }
    throw new Error(msg)
  }
  return res.json()
}

export async function getGroupFData(
  scanWeeks: number,
  weekNos?: number[] | null,
  signal?: AbortSignal
): Promise<GroupFResponse> {
  const params = new URLSearchParams()
  if (weekNos != null && weekNos.length > 0) {
    weekNos.forEach((w) => params.append('week_nos', String(w)))
  } else {
    params.set('scan_weeks', String(scanWeeks))
  }
  const res = await fetch(`${API_BASE}/asin-performances/group-f?${params.toString()}`, { signal })
  if (!res.ok) {
    const text = await res.text()
    throw buildApiError(text, res.status, 'Failed to fetch Group F data')
  }
  return res.json()
}

export async function getGroupFLockStatus(signal?: AbortSignal): Promise<GroupFLockStatus> {
  const res = await fetch(`${API_BASE}/asin-performances/group-f/status`, { signal })
  if (!res.ok) {
    const text = await res.text()
    throw buildApiError(text, res.status, 'Failed to fetch Group F lock status')
  }
  return res.json()
}

export async function releaseGroupFLock(): Promise<GroupFReleaseLockResponse> {
  const res = await fetch(`${API_BASE}/asin-performances/group-f/release-lock`, { method: 'POST' })
  if (!res.ok) {
    const text = await res.text()
    throw buildApiError(text, res.status, 'Failed to release Group F lock')
  }
  return res.json()
}

export async function listGroupAWeeks(): Promise<number[]> {
  const res = await fetch(`${API_BASE}/asin-performances/group-a/weeks`)
  if (!res.ok) {
    const text = await res.text()
    throw new Error(parseErrorResponse(text, res.status) || 'Failed to fetch Group A weeks')
  }
  return res.json()
}

export async function getGroupASummary(
  week_no?: number | null,
  page = 1,
  page_size = 30
): Promise<GroupASummaryResponse> {
  const params = new URLSearchParams({
    page: String(page),
    page_size: String(page_size),
  })
  if (week_no != null) params.set('week_no', String(week_no))
  const res = await fetch(`${API_BASE}/asin-performances/group-a/summary?${params.toString()}`)
  if (!res.ok) {
    const text = await res.text()
    throw new Error(parseErrorResponse(text, res.status) || 'Failed to fetch Group A summary')
  }
  return res.json()
}

export async function getGroupADetail(
  parent_asin: string,
  week_no: number,
  store_id: number
): Promise<GroupADetailResponse> {
  const params = new URLSearchParams({
    parent_asin,
    week_no: String(week_no),
    store_id: String(store_id),
  })
  const res = await fetch(`${API_BASE}/asin-performances/group-a/detail?${params.toString()}`)
  if (!res.ok) {
    const text = await res.text()
    throw new Error(parseErrorResponse(text, res.status) || 'Failed to fetch Group A detail')
  }
  return res.json()
}

export async function operateGroupA(
  parent_asin: string,
  store_id: number,
  week_no: number
): Promise<{ updated: number; operated_at?: string | null }> {
  const res = await fetch(`${API_BASE}/asin-performances/group-a/operate`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ parent_asin, store_id, week_no }),
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(parseErrorResponse(text, res.status) || '操作失败')
  }
  return res.json()
}

export async function downloadGroupAData(week_no: number, parentStoreKeys?: string[]): Promise<void> {
  const params = new URLSearchParams({ week_no: String(week_no) })
  if (parentStoreKeys && parentStoreKeys.length > 0) {
    for (const key of parentStoreKeys) {
      const v = (key || '').trim()
      if (v) params.append('parent_store_keys', v)
    }
  }
  const res = await fetch(`${API_BASE}/asin-performances/group-a/export?${params.toString()}`)
  if (!res.ok) {
    const text = await res.text()
    throw new Error(parseErrorResponse(text, res.status) || 'Failed to export Group A data')
  }
  const blob = await res.blob()
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
  const res = await fetch(`${API_BASE}/asin-performances/monitor/parents`)
  if (!res.ok) {
    const text = await res.text()
    throw new Error(parseErrorResponse(text, res.status) || 'Failed to fetch monitor parents')
  }
  return res.json()
}

export async function getMonitorTrack(parent_asin: string): Promise<MonitorTrackResponse> {
  const res = await fetch(
    `${API_BASE}/asin-performances/monitor/track?${new URLSearchParams({ parent_asin })}`
  )
  if (!res.ok) {
    const text = await res.text()
    throw new Error(parseErrorResponse(text, res.status) || 'Failed to fetch monitor track')
  }
  return res.json()
}

export async function createAsinPerformance(data: AsinPerformanceCreate): Promise<AsinPerformance> {
  const res = await fetch(`${API_BASE}/asin-performances`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  });
  if (!res.ok) throw new Error('Failed to create');
  return res.json();
}
