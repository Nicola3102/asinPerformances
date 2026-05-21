# Backend API And Data Source Guide

This document summarizes the backend HTTP interfaces in `backend/app/`, what each endpoint is responsible for, and where each piece of data comes from.

## 1. App Structure

Backend entry: `backend/app/main.py`

Mounted routers:

- `asin_router` -> `/api/asin-performances`
- `ads_router` -> `/api/ads`
- `revenue_router` -> `/api`
- `sync_router` -> `/api`
- `trend_reports_router` -> `/api/trend`
- `app.add_api_route("/api/trend", ...)` -> trend JSON endpoint

## 2. Data Source Glossary

### Local MySQL tables

- `asin_performances`
  Main local result table for ASIN search/query/order weekly data.
- `group_A`
  Local Group A summary/detail table.
- `listing_tracking`
  Local listing tracking / trend analysis table.
- `daily_ad_cost_sales`
  Local ad-sales daily aggregate table.
- `daily_upload_asin_dates`
  Local new-listing / daily-upload session source table.

### Online MySQL tables

- `order_item`
  Order-level item facts, used in order flags, ad-sales order-item sales, exports.
- `order_profit`
  Revenue / gross profit / FX source for `/api/revenue`.
- `order_return`
  Refund / return related source for `/api/revenue`.
- `amazon_listing`
  Listing metadata and listing date source.
- `amazon_variation`
  Variation / parent-child mapping source.
- `amazon_search`
  Search/query and impression source.
- `amazon_search_data`
  Search data / week mapping / trend source.
- `amazon_sales_traffic`, `amazon_sales_and_traffic_daily`, `amazon_sales_and_traffic`
  Sales/traffic/session sources used by trend and daily-upload reports.
- `amazon_ads_ad_group_ad_report`
  Ads report source for ad-sales and revenue ad-cost merge.
- `amazon_ads_ad_group_ad`
  Ad ASIN mapping source.
- `amazon_ads_campaign_placement_report`
  Placement ads source in session-impression report.
- `ai_test_batch`
  Trend batch labels.
- `ai_generated_amazon_listings`
  Export metadata enrichment.

### Non-database runtime state

- In-process locks for Group F and heavy New Listing builds
- In-memory HTML / JSON caches for Trend reports
- Disk HTML cache under `backend/app/log/`

## 3. App-Level Endpoints

### `GET /health`

- Handler: `health`
- Purpose:
  Health check for service liveness.
- Parameters:
  None.
- Response data:
  `{"status": "ok"}`
- Data source:
  None.

### `GET /reports/daily-upload-sessions`

- Handler: `daily_upload_sessions_report`
- Purpose:
  Render interactive HTML for daily-upload sessions and listing KPI information.
- Parameters:
  - `listing_since`
  - `session_start`
  - `session_end`
- Main implementation:
  - `build_report_payload(...)`
  - `render_html(...)`
- Data source:
  - Local: `daily_upload_asin_dates`
  - Online/derived inside builder: listing/session-related tables

### `GET /api/trend`

- Handler: `get_trend_data`
- Purpose:
  Return trend JSON for listing tracking, filter options, and weekly aggregate series.
- Parameters:
  - `store_id`
  - `used_model`
  - `created_at_start`
  - `created_at_end`
  - `pid_min`
  - `pid_max`
  - `parent_asin`
  - `week_no[]`
  - `batch_id`
- Main implementation:
  - inline `listing_tracking` SQLAlchemy aggregation
  - `_fetch_trend_batch_options(...)`
- Data source:
  - Local: `listing_tracking`
  - Online: `ai_test_batch`

## 4. Sync Endpoints

### `POST /api/daily-upload-ds`

- Handler: `trigger_daily_upload_ds`
- Purpose:
  Trigger DailyUploadDS background sync manually.
- Parameters:
  None.
- Main implementation:
  - `run_daily_upload_ds_scheduled(force=True)`
- Data source:
  - Online: listing / traffic source tables
  - Local write: `daily_upload_asin_dates`

### `POST /api/sync-from-online`

- Handler: `trigger_sync_from_online`
- Purpose:
  Sync online ASIN/search/order data into local `asin_performances`.
- Parameters:
  None.
- Main implementation:
  - `sync_from_online_db()`
  - `record_sync_run()`
- Data source:
  - Online: `order_item`, `amazon_listing`, `amazon_variation`, `amazon_search`, `amazon_sales_traffic`, `amazon_search_data`
  - Local write: `asin_performances`

## 5. ASIN Performance Endpoints

### `GET /api/asin-performances/stats`

- Handler: `get_table_stats`
- Purpose:
  Return local `asin_performances` row count.
- Data source:
  - Local: `asin_performances`

### `GET /api/asin-performances/summary-stats`

- Handler: `get_summary_stats`
- Purpose:
  Return latest week parent ASIN count and total orders.
- Data source:
  - Local: `asin_performances`

### `GET /api/asin-performances`

- Handler: `list_asin_performances`
- Purpose:
  Return raw local rows with `skip` / `limit`.
- Data source:
  - Local: `asin_performances`

### `GET /api/asin-performances/weeks`

- Handler: `list_weeks`
- Purpose:
  Return distinct local `week_no` list.
- Data source:
  - Local: `asin_performances`

### `GET /api/asin-performances/summary`

- Handler: `list_summary`
- Purpose:
  Return per-store parent summary for one week.
- Parameters:
  - `week_no`
- Data source:
  - Local: `asin_performances`

### `GET /api/asin-performances/summary/consolidated`

- Handler: `list_summary_consolidated`
- Purpose:
  Return consolidated parent summary merged across stores.
- Parameters:
  - `week_no`
- Data source:
  - Local: `asin_performances`

### `POST /api/asin-performances/operate`

- Handler: `operate_by_parent_week`
- Purpose:
  Mark parent/week rows as operated.
- Body:
  - `parent_asin`
  - `week_no`
- Data source:
  - Local: `asin_performances`

### `POST /api/asin-performances/ad-check`

- Handler: `ad_check_by_parent_week`
- Purpose:
  Mark ad check completion for parent/week rows.
- Body:
  - `parent_asin`
  - `week_no`
- Data source:
  - Local: `asin_performances`

### `POST /api/asin-performances/group-a`

- Handler: `trigger_group_a_sync`
- Purpose:
  Trigger Group A impression sync for a target week.
- Parameters:
  - `week_no` (optional)
- Main implementation:
  - `sync_group_a_impression(...)`
  - `_get_sync_date_range()`
  - `_group_a_date_to_week_no(...)`
- Data source:
  - Online: `amazon_search`, `amazon_search_data`, listing-related sources
  - Local write: `group_A`
  - Local read for skip logic: `asin_performances`

### `GET /api/asin-performances/group-a/weeks`

- Handler: `list_group_a_weeks`
- Purpose:
  Return available Group A weeks.
- Data source:
  - Local: `group_A`

### `GET /api/asin-performances/group-a/summary`

- Handler: `list_group_a_summary`
- Purpose:
  Return paginated Group A summary.
- Parameters:
  - `week_no`
  - `page`
  - `page_size`
- Data source:
  - Local: `group_A`

### `GET /api/asin-performances/group-a/detail`

- Handler: `get_group_a_detail`
- Purpose:
  Return Group A parent detail, children, and query detail.
- Parameters:
  - `parent_asin`
  - `week_no`
  - `store_id`
- Data source:
  - Local: `group_A`

### `POST /api/asin-performances/group-a/operate`

- Handler: `operate_group_a`
- Purpose:
  Mark Group A parent/store/week operated.
- Data source:
  - Local: `group_A`

### `POST /api/asin-performances/query-status/refresh`

- Handler: `refresh_query_status`
- Purpose:
  Poll online search completion and write `checked_status`.
- Parameters/body:
  - `week_no`
- Main implementation:
  - `check_parent_store_week_completed(...)`
- Data source:
  - Local: `asin_performances`
  - Online: `amazon_search`

### `GET /api/asin-performances/db-status`

- Handler: `get_online_db_status`
- Purpose:
  Show online DB connection/process status for diagnostics.
- Data source:
  - Online MySQL server metadata (`SHOW GLOBAL STATUS`, `SHOW PROCESSLIST`)

### `GET /api/asin-performances/group-f/status`

- Handler: `get_group_f_lock_status`
- Purpose:
  Return current Group F in-process slot occupancy.
- Data source:
  - Process memory only

### `POST /api/asin-performances/group-f/release-lock`

- Handler: `release_group_f_lock`
- Purpose:
  Force release the in-process Group F slot.
- Data source:
  - Process memory only

### `GET /api/asin-performances/group-f`

- Handler: `get_group_f_candidates`
- Purpose:
  Heavy Group F candidate search endpoint.
- Parameters:
  - `scan_weeks`
  - `week_nos[]`
- Main implementation:
  - `get_group_f(...)`
  - `compute_scan_weeks_list_for_api(...)`
  - `_group_f_current_week_no(...)`
  - `_group_f_to_mysql_week_no(...)`
- Data source:
  - Online: Group F related search / sales / variation / listing / order tables through `group_f_spark`

### `GET /api/asin-performances/export`

- Handler: `export_week_data`
- Purpose:
  Export week CSV for selected parent ASINs.
- Parameters:
  - `week_no`
  - `parent_asins[]`
- Main implementation:
  - `_fetch_listing_meta_for_export(...)`
- Data source:
  - Local: `asin_performances`
  - Online: `amazon_listing`, `ai_generated_amazon_listings`

### `GET /api/asin-performances/group-a/export`

- Handler: `export_group_a_data`
- Purpose:
  Export Group A CSV.
- Parameters:
  - `week_no`
  - `parent_store_keys[]`
- Main implementation:
  - `_fetch_listing_meta_for_export(...)`
- Data source:
  - Local: `group_A`
  - Online: `amazon_listing`, `ai_generated_amazon_listings`

### `GET /api/asin-performances/detail`

- Handler: `list_detail_by_parent_week`
- Purpose:
  Return child ASIN and search-query detail for one parent/week.
- Parameters:
  - `parent_asin`
  - `week_no`
  - `store_id`
- Data source:
  - Local: `asin_performances`

### `GET /api/asin-performances/monitor/parents`

- Handler: `list_monitor_parents`
- Purpose:
  Return all `operation_status=1` parent ASINs.
- Data source:
  - Local: `asin_performances`

### `GET /api/asin-performances/monitor/track`

- Handler: `get_monitor_track`
- Purpose:
  Return monitor grid data for one parent ASIN.
- Parameters:
  - `parent_asin`
- Main implementation:
  - `get_parent_week_status_details(...)`
- Data source:
  - Local: `asin_performances`
  - Online: `amazon_search`, `amazon_search_data`

### CRUD by item id

#### `GET /api/asin-performances/{item_id}`

- Handler: `get_asin_performance`
- Purpose:
  Fetch one local row by id.
- Data source:
  - Local: `asin_performances`

#### `POST /api/asin-performances`

- Handler: `create_asin_performance`
- Purpose:
  Create a local row manually.
- Data source:
  - Local: `asin_performances`

#### `PATCH /api/asin-performances/{item_id}`

- Handler: `update_asin_performance`
- Purpose:
  Partial update of one row.
- Data source:
  - Local: `asin_performances`

#### `DELETE /api/asin-performances/{item_id}`

- Handler: `delete_asin_performance`
- Purpose:
  Delete one row.
- Data source:
  - Local: `asin_performances`

## 6. Ads And Revenue Endpoints

### `GET /api/ads/ad-sales`

- Handler: `list_ad_sales`
- Purpose:
  Return ad-sales table rows, summary, and daily chart series.
- Parameters:
  - `store_id`
  - `start_date`
  - `end_date`
  - `ensure_latest`
  - `sort`
  - `page`
  - `page_size`
- Main implementation:
  - optional `ensure_latest_ad_cost_sales_data()`
  - local ORM aggregation on `daily_ad_cost_sales`
  - `_fetch_order_item_ad_asin_sales(...)`
  - `_fetch_ads_report_max_current_date(...)`
- Data source:
  - Local: `daily_ad_cost_sales`
  - Online: `order_item`, `amazon_ads_ad_group_ad_report`, `amazon_ads_ad_group_ad`

### `POST /api/ads/ad-sales/ensure-latest`

- Handler: `trigger_ad_sales_ensure_latest`
- Purpose:
  Trigger background latest ad-sales refresh.
- Main implementation:
  - background `_bg_ensure_latest_ad_sales()`
  - `ensure_latest_ad_cost_sales_data()`
- Data source:
  - Online -> Local refresh for `daily_ad_cost_sales`

### `GET /api/ads/ad-sales/export`

- Handler: `export_ad_sales`
- Purpose:
  Export selected ad-sales rows by id.
- Parameters:
  - `ids[]`
- Data source:
  - Local: `daily_ad_cost_sales`

### `GET /api/revenue`

- Handler: `get_ads_profit`
- Purpose:
  Main Revenue report endpoint. Returns weekly summary and merged ad cost.
- Parameters:
  - `store_id`
  - `start_date`
  - `end_date`
- Main implementation:
  - `fetch_profit_report(...)`
  - `fetch_profit_latest_invoice_date()`
  - `_weekly_ad_cost_local_usd_fx(...)`
  - `_merge_weekly_ad_cost_into_report(...)`
- Data source:
  - Online: `order_profit`, `order_return`, `order_item`, `amazon_ads_ad_group_ad_report`

### `GET /api/ads/revenue`

- Handler: `get_ads_profit`
- Purpose:
  Legacy/hidden alias to the same Revenue handler.
- Data source:
  - Same as `/api/revenue`

### `GET /api/ads/profit`

- Handler: `get_ads_profit_legacy`
- Purpose:
  Legacy alias for old frontend path.
- Data source:
  - Same as `/api/revenue`

## 7. Trend Report Endpoints

### `POST /api/trend/new-listing/order-flags`

- Handler: `trend_new_listing_order_flags`
- Purpose:
  Batch-check whether `(asin, store_id)` pairs have orders for UI highlight.
- Body:
  - `items[{asin, store_id}]`
- Data source:
  - Online: `order_item`

### `GET /api/trend/session-impression`

- Handler: `trend_session_impression_ads_html`
- Purpose:
  Render weekly traffic + impression + ads HTML report.
- Parameters:
  - `embed`
  - `rebuild`
  - `nocache`
- Main implementation:
  - `build_report_html_for_range(...)`
  - in-memory and disk HTML cache
- Data source:
  - Online: `amazon_sales_and_traffic_daily`, `amazon_search`, `amazon_ads_ad_group_ad_report`, `amazon_ads_campaign_placement_report`
  - Disk cache: `backend/app/log/session-impression-*.html`

### `GET /api/trend/new-listing/json-cache-stats`

- Handler: `trend_new_listing_json_cache_stats`
- Purpose:
  Return in-process cache stats for New Listing JSON.
- Data source:
  - Process memory only

### `GET /api/trend/new-listing/heavy-status`

- Handler: `trend_new_listing_heavy_status`
- Purpose:
  Return current heavy-build occupancy and timeout configuration.
- Data source:
  - Process memory only

### `GET /api/trend/new-listing`

- Handler: `trend_new_listing_report`
- Purpose:
  Return New Listing HTML or JSON report; optionally trigger sync first.
- Parameters:
  - `start_date`
  - `listing_since`
  - `session_start`
  - `session_end`
  - `sync_start`
  - `sync_end`
  - `skip_sync`
  - `format`
  - `nocache`
  - `profile`
  - `json_views`
  - `store_id`
- Main implementation:
  - `sync_range(...)` / `sync_with_default_date_range(...)`
  - `build_report_payload(...)`
  - `render_html(...)`
  - `fetch_amazon_listing_max_open_date_online(...)`
  - `matrix_bulk_cache_wait_ready(...)`
- Data source:
  - Local: `daily_upload_asin_dates`
  - Online: `amazon_listing`, `amazon_variation`, session/traffic data sources used by the PST builder

## 8. Route Aliases And Notes

- `/api/revenue` is the main revenue endpoint.
- `/api/ads/revenue` and `/api/ads/profit` are compatibility aliases.
- `/api/trend` is JSON trend data.
- `/api/trend/session-impression` and `/api/trend/new-listing` are report-style submodules under the same Trend namespace.

## 9. Known Caveats

- `ensure_latest_ad_cost_sales_data()` currently only fills missing report dates; it does not re-sync already existing recent dates.
- `POST /api/trend/new-listing/order-flags` may degrade to an empty result when the online reporting pool is exhausted.
- Root `requirements.txt` currently has a `spacy` / `en-core-web-sm` version conflict and cannot be installed cleanly in a fresh environment without adjustment.
