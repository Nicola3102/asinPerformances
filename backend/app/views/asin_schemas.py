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
