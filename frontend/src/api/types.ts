/**
 * HTTP API 契约类型（与后端 FastAPI 路由/Pydantic 模型对齐）。
 * 变更时请同步后端 `backend/app` 下对应路由与 schema。
 */

export interface AsinPerformance {
  id: number
  parent_asin: string | null
  child_asin: string | null
  parent_order_total: string | null
  order_num: number | null
  week_no: number | null
  child_impression_count: number | null
  child_session_count: number | null
  search_query: string | null
  search_query_volume: number | null
  search_query_impression_count: number | null
  search_query_purchase_count: number | null
}

export interface AsinPerformanceCreate {
  parent_asin?: string
  child_asin?: string
  parent_order_total?: number
  order_num?: number
  week_no?: number
  child_impression_count?: number
  child_session_count?: number
  search_query?: string
  search_query_volume?: number
  search_query_impression_count?: number
  search_query_purchase_count?: number
}

export interface SummaryRow {
  parent_asin: string | null
  parent_asin_create_at: string | null
  parent_order_total: string | number | null
  week_no: number | null
  store_id: number | null
  operation_status?: boolean | null
  last_operated_at?: string | null
  ad_check?: boolean | null
  ad_created_at?: string | null
  last_ad_created_at?: string | null
  operated_at?: string | null
  checked_status?: string | null
  checked_at?: string | null
}

export interface SummaryRowConsolidated {
  parent_asin: string | null
  parent_asin_create_at: string | null
  parent_order_total: string | number | null
  week_no: number | null
  store_ids: number[]
  child_asins_with_orders: string[]
  operation_status?: boolean | null
  last_operated_at?: string | null
  ad_check?: boolean | null
  ad_created_at?: string | null
  last_ad_created_at?: string | null
  operated_at?: string | null
  checked_status?: string | null
  checked_at?: string | null
}

export interface SearchQueryRow {
  search_query: string | null
  search_query_volume: number | null
  search_query_impression_count: number | null
  search_query_cart_count: number | null
  search_query_total_impression: number | null
  search_query_click_count: number | null
  search_query_total_click: number | null
  search_query_purchase_count: number | null
}

export interface DetailChildRow {
  child_asin: string | null
  child_impression_count: number | null
  child_session_count: number | null
  order_num: number | null
  search_queries: SearchQueryRow[]
}

export interface DetailResponse {
  parent_asin: string | null
  parent_order_total: string | number | null
  week_no: number | null
  children: DetailChildRow[]
}

export interface TableStats {
  count: number
  table: string
}

export interface WeekStatsRow {
  week_no: number | null
  parent_asin_count: number
  total_orders: number | null
}

export interface SummaryStatsResponse {
  by_week: WeekStatsRow[]
}

export interface GroupFRow {
  variation_id: number | null
  parent_asin: string | null
  created_at: string | null
  store_id: number | null
  impression_count_asin: string | null
  order_asin: string | null
  sessions_asin: string | null
}

export interface GroupFResponse {
  weeks: number[]
  business_weeks: number[]
  rows: GroupFRow[]
}

export interface GroupFLockStatus {
  lock_held: boolean
  request_id: string | null
  started_at: string | null
  duration_seconds: number | null
  is_stuck: boolean
  message: string
}

export interface GroupFReleaseLockResponse {
  released: boolean
  had_lock: boolean
  previous_request_id: string | null
  message: string
}

export interface GroupASummaryRow {
  parent_asin: string | null
  store_id: number | null
  created_at: string | null
  week_no: number | null
  total_impression_count: number
  total_cart_count: number
  total_session_count: number
  operation_status?: boolean | null
  operated_at?: string | null
}

export interface GroupASummaryResponse {
  week_no: number | null
  page: number
  page_size: number
  total: number
  total_pages: number
  rows: GroupASummaryRow[]
}

export interface GroupADetailChildRow {
  child_asin: string | null
  child_impression_count: number | null
  child_cart: number | null
  child_session_count: number | null
  search_queries: SearchQueryRow[]
}

export interface GroupADetailResponse {
  parent_asin: string | null
  store_id: number | null
  created_at: string | null
  week_no: number | null
  total_impression_count: number
  total_cart_count: number
  total_session_count: number
  children: GroupADetailChildRow[]
}

export interface MonitorParentItem {
  parent_asin: string | null
  operated_at: string | null
}

export interface MonitorTrackRow {
  child_asin: string | null
  week_no: number | null
  search_query: string | null
  search_query_volume: number | null
  search_query_impression_count: number | null
  search_query_click_count: number | null
}

export interface MonitorWeekStatus {
  week_no: number | null
  completed: boolean
  checked_at: string | null
  incomplete_count: number
  incomplete_child_asins: string[]
}

export interface MonitorTrackResponse {
  parent_asin: string | null
  weeks: number[]
  week_statuses: MonitorWeekStatus[]
  rows: MonitorTrackRow[]
}

export interface TrendBatchOption {
  id: number
  label: string
}

export interface TrendFilterOptions {
  store_ids: number[]
  batch_ids: number[]
  batch_options: TrendBatchOption[]
  week_nos: number[]
  used_models: string[]
}

export interface TrendWeekPoint {
  week_no: number
  new_asin_count: number
  total_impression: number
  total_sessions: number
  total_clicks: number
  total_asin_count: number
  active_asin_count: number
  impression_asin_count: number
  related_click: number
  impression_asin_rate: number
}

export interface TrendResponse {
  matched_row_count: number
  weeks: number[]
  filter_options: TrendFilterOptions
  series: TrendWeekPoint[]
}

export interface SyncCheck {
  rows_fetched_from_online: number
  rows_inserted: number
  local_table_count_after: number
  table_name: string
  insert_ok: boolean
  step2_error?: string | null
  message?: string | null
}

export interface SyncFromOnlineResponse {
  status: string
  rows_synced: number
  message?: string
  check?: SyncCheck
}

export interface RefreshQueryStatusResponse {
  checked_groups: number
  completed_groups: number
  skipped_completed: number
  skipped_by_interval: number
}

export interface OperateSummaryResponse {
  updated: number
}

export interface AdCheckSummaryResponse {
  updated: number
  ad_created_at?: string | null
}

export interface OperateGroupAResponse {
  updated: number
  operated_at?: string | null
}

export type AdSalesRow = {
  id: number
  ad_asin: string | null
  store_id: number | null
  purchase_date: string | null
  clicks: number
  impressions: number
  purchases: number
  ad_cost: number | null
  sales_1d: number | null
  ad_sales_1d: number | null
  tad_sales: number | null
  tsales: number | null
}

export type AdSalesSummary = {
  clicks: number
  impressions: number
  ad_cost: number
  sales_1d: number
  order_item_sales: number
  tacos: number
  ad_asin_count: number
  cpc: number
  acos: number
  cvr: number
  purchases: number
}

export type AdSalesDailyPoint = {
  date: string | null
  clicks: number
  impressions: number
  ad_cost: number
  sales_1d: number
  order_item_sales: number
  tacos: number
  ad_asin_count: number
  cpc: number
  acos: number
  cvr: number
  purchases: number
}

export type AdSalesListResponse = {
  items: AdSalesRow[]
  page: number
  page_size: number
  total: number
  summary: AdSalesSummary
  daily_series: AdSalesDailyPoint[]
  sync_info?: {
    mode?: string
    rows_upsert?: number
    skipped?: boolean
    reason?: string
    gap_days?: string[]
  }
}

export type AdSalesEnsureLatestResponse = {
  status: string
  message?: string
}

export type AdsProfitSummary = {
  start_date: string
  end_date: string
  store_id: number | null
  order_count: number
  returned_order_count: number
  return_row_count: number
  sales_amount: number
  refund_amount: number
  gross_profit: number
  gross_profit_after_return: number
  gross_margin_rate: number
  gross_margin_after_return_rate: number
  return_rate: number
}

export type AdsProfitWeeklyPoint = {
  week_start: string | null
  week_end: string | null
  order_count: number
  returned_order_count: number
  return_row_count: number
  sales_amount: number
  refund_amount: number
  gross_profit: number
  gross_profit_after_return: number
  gross_margin_rate: number
  gross_margin_after_return_rate: number
  return_rate: number
  return_rate_actual?: number
  return_rate_predicted?: number | null
  return_rate_curve_type?: 'actual' | 'predicted'
  return_rate_curve_color?: string
}

export type AdsProfitResponse = {
  start_date: string
  end_date: string
  latest_invoice_date: string
  store_id: number | null
  store_ids: number[]
  summary: AdsProfitSummary
  weekly_series: AdsProfitWeeklyPoint[]
}
