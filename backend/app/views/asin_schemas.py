from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, ConfigDict


class AsinPerformanceBase(BaseModel):
    parent_asin: Optional[str] = None
    child_asin: Optional[str] = None
    parent_asin_create_at: Optional[datetime] = None
    parent_order_total: Optional[Decimal] = None
    order_num: Optional[int] = None
    order_id: Optional[str] = None  # 逗号分隔的订单 id
    week_no: Optional[int] = None
    child_impression_count: Optional[int] = None
    child_session_count: Optional[int] = None
    search_query: Optional[str] = None
    search_query_volume: Optional[int] = None
    search_query_impression_count: Optional[int] = None
    search_query_purchase_count: Optional[int] = None
    search_query_total_impression: Optional[int] = None
    search_query_click_count: Optional[int] = None
    search_query_total_click: Optional[int] = None


class AsinPerformanceCreate(AsinPerformanceBase):
    pass


class AsinPerformanceUpdate(AsinPerformanceBase):
    pass


class AsinPerformanceResponse(AsinPerformanceBase):
    model_config = ConfigDict(from_attributes=True)
    id: int


class SummaryRow(BaseModel):
    parent_asin: Optional[str] = None
    parent_asin_create_at: Optional[datetime] = None
    parent_order_total: Optional[Decimal] = None
    week_no: Optional[int] = None
    store_id: Optional[int] = None
    operation_status: Optional[bool] = False
    operated_at: Optional[datetime] = None
    checked_status: Optional[str] = None
    checked_at: Optional[datetime] = None


class SummaryRowConsolidated(BaseModel):
    """同一 parent_asin + week_no 下多 store 汇总：parent_order_total 为各 store 之和，store_ids 罗列有订单的 store，child_asins_with_orders 罗列有订单的子 ASIN。"""
    parent_asin: Optional[str] = None
    parent_asin_create_at: Optional[datetime] = None
    parent_order_total: Optional[Decimal] = None
    week_no: Optional[int] = None
    store_ids: List[int] = []
    child_asins_with_orders: List[str] = []
    operation_status: Optional[bool] = False
    operated_at: Optional[datetime] = None
    checked_status: Optional[str] = None
    checked_at: Optional[datetime] = None


class WeekStatsRow(BaseModel):
    week_no: Optional[int] = None
    parent_asin_count: int = 0
    total_orders: Optional[Decimal] = None


class SummaryStatsResponse(BaseModel):
    by_week: List[WeekStatsRow] = []


class SearchQueryRow(BaseModel):
    search_query: Optional[str] = None
    search_query_volume: Optional[int] = None
    search_query_impression_count: Optional[int] = None
    search_query_cart_count: Optional[int] = None
    search_query_total_impression: Optional[int] = None
    search_query_click_count: Optional[int] = None
    search_query_total_click: Optional[int] = None
    search_query_purchase_count: Optional[int] = None


class DetailChildRow(BaseModel):
    child_asin: Optional[str] = None
    child_impression_count: Optional[int] = None
    child_session_count: Optional[int] = None
    order_num: Optional[int] = None
    order_id: Optional[str] = None  # 逗号分隔的订单 id
    search_queries: List[SearchQueryRow] = []


class DetailResponse(BaseModel):
    parent_asin: Optional[str] = None
    parent_order_total: Optional[Decimal] = None
    week_no: Optional[int] = None
    children: List[DetailChildRow] = []


class GroupFRow(BaseModel):
    """Group F 接口单行：指定周所有父 ASIN 的完整数据。"""
    variation_id: Optional[int] = None
    parent_asin: Optional[str] = None
    created_at: Optional[datetime] = None
    store_id: Optional[int] = None
    impression_count_asin: Optional[str] = None  # 有 impression 的任意子 ASIN，无则空
    order_asin: Optional[str] = None  # 有订单的任意子 ASIN，无则空
    sessions_asin: Optional[str] = None  # 有 sessions 的任意子 ASIN，无则空


class GroupFResponse(BaseModel):
    """Group F 接口响应"""
    weeks: List[int] = []
    business_weeks: List[int] = []
    rows: List[GroupFRow] = []


class GroupASummaryRow(BaseModel):
    parent_asin: Optional[str] = None
    store_id: Optional[int] = None
    created_at: Optional[datetime] = None
    week_no: Optional[int] = None
    total_impression_count: int = 0
    total_cart_count: int = 0
    total_session_count: int = 0
    operation_status: Optional[bool] = False
    operated_at: Optional[datetime] = None


class GroupASummaryResponse(BaseModel):
    week_no: Optional[int] = None
    page: int = 1
    page_size: int = 30
    total: int = 0
    total_pages: int = 0
    rows: List[GroupASummaryRow] = []


class GroupADetailChildRow(BaseModel):
    child_asin: Optional[str] = None
    child_impression_count: Optional[int] = None
    child_cart: Optional[int] = None
    child_session_count: Optional[int] = None
    search_queries: List[SearchQueryRow] = []


class GroupADetailResponse(BaseModel):
    parent_asin: Optional[str] = None
    store_id: Optional[int] = None
    created_at: Optional[datetime] = None
    week_no: Optional[int] = None
    total_impression_count: int = 0
    total_cart_count: int = 0
    total_session_count: int = 0
    children: List[GroupADetailChildRow] = []


class GroupAOperateBody(BaseModel):
    parent_asin: str
    store_id: int
    week_no: int | str


class MonitorParentItem(BaseModel):
    """operation_status=1 的父 ASIN 列表项"""
    parent_asin: Optional[str] = None
    operated_at: Optional[datetime] = None


class MonitorTrackRow(BaseModel):
    """监控追踪单行：子 ASIN + 周 + search_query 及三指标"""
    child_asin: Optional[str] = None
    week_no: Optional[int] = None
    search_query: Optional[str] = None
    search_query_volume: Optional[int] = None
    search_query_impression_count: Optional[int] = None
    search_query_click_count: Optional[int] = None


class MonitorWeekStatus(BaseModel):
    """监控追踪周状态：该周是否已完成抓取校验"""
    week_no: Optional[int] = None
    completed: bool = False
    checked_at: Optional[datetime] = None
    incomplete_count: int = 0
    incomplete_child_asins: List[str] = []


class MonitorTrackResponse(BaseModel):
    """监控追踪响应：某父 ASIN 下所有子 ASIN 各周的 search_query 数据"""
    parent_asin: Optional[str] = None
    weeks: List[int] = []
    week_statuses: List[MonitorWeekStatus] = []
    rows: List[MonitorTrackRow] = []
