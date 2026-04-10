import csv
from datetime import date, datetime
from io import StringIO
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import and_, func
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import DailyAdCostSales

router = APIRouter(prefix="/api/ads", tags=["ads"])

_SORT_FIELDS = {
    "ad_cost": DailyAdCostSales.ad_cost,
    "ad_sales_1d": DailyAdCostSales.ad_sales_1d,
    "ad_sales_7d": DailyAdCostSales.ad_sales_7d,
    "ad_sales_14d": DailyAdCostSales.ad_sales_14d,
    "ad_sales_30d": DailyAdCostSales.ad_sales_30d,
    "tad_sales": DailyAdCostSales.tad_sales,
    "tad_sales_7d": DailyAdCostSales.tad_sales_7d,
    "tad_sales_14d": DailyAdCostSales.tad_sales_14d,
    "tad_sales_30d": DailyAdCostSales.tad_sales_30d,
}


def _parse_sort_or_400(raw: str | None) -> list:
    """
    sort 格式：field:asc,field2:desc
    - field 必须在 _SORT_FIELDS 中
    - direction 缺省为 desc
    """
    if raw is None:
        return []
    s = str(raw).strip()
    if not s:
        return []
    parts = [p.strip() for p in s.split(",") if p.strip()]
    out = []
    for p in parts:
        if ":" in p:
            field, direction = [x.strip() for x in p.split(":", 1)]
        else:
            field, direction = p.strip(), "desc"
        if field not in _SORT_FIELDS:
            raise HTTPException(status_code=400, detail=f"sort 字段不支持: {field}")
        d = direction.lower()
        col = _SORT_FIELDS[field]
        if d in ("asc", "a", "1"):
            out.append(col.asc())
        elif d in ("desc", "d", "-1"):
            out.append(col.desc())
        else:
            raise HTTPException(status_code=400, detail=f"sort direction 不支持: {direction}")
    return out


def _parse_ymd_or_400(raw: str | None, field: str) -> date | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail=f"{field} 格式不合法，需 YYYY-MM-DD")


@router.get("/ad-sales")
def list_ad_sales(
    store_id: Optional[int] = Query(None),
    start_date: Optional[str] = Query(None, description="purchase_date 起始 YYYY-MM-DD（含）"),
    end_date: Optional[str] = Query(None, description="purchase_date 结束 YYYY-MM-DD（含）"),
    sort: Optional[str] = Query(None, description="排序：field:asc,field2:desc"),
    page: int = Query(1, ge=1),
    page_size: int = Query(30, ge=1, le=200),
    db: Session = Depends(get_db),
):
    sd = _parse_ymd_or_400(start_date, "start_date")
    ed = _parse_ymd_or_400(end_date, "end_date")
    if (sd is None) ^ (ed is None):
        raise HTTPException(status_code=400, detail="start_date 与 end_date 需同时提供或同时省略")
    if sd is not None and ed is not None and sd > ed:
        raise HTTPException(status_code=400, detail="start_date 不能晚于 end_date")

    conds = []
    if store_id is not None:
        conds.append(DailyAdCostSales.store_id == int(store_id))
    if sd is not None and ed is not None:
        conds.append(and_(DailyAdCostSales.purchase_date >= sd, DailyAdCostSales.purchase_date <= ed))

    q = db.query(DailyAdCostSales)
    if conds:
        q = q.filter(and_(*conds))

    sort_exprs = _parse_sort_or_400(sort)
    total = q.with_entities(func.count(DailyAdCostSales.id)).scalar() or 0
    rows = (
        q.order_by(
            *(
                sort_exprs
                if sort_exprs
                else [DailyAdCostSales.purchase_date.desc(), DailyAdCostSales.id.desc()]
            )
        )
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    items = [
        {
            "id": r.id,
            "ad_asin": r.ad_asin,
            "store_id": r.store_id,
            "purchase_date": r.purchase_date.isoformat() if r.purchase_date else None,
            "ad_cost": float(r.ad_cost) if r.ad_cost is not None else None,
            "ad_sales_1d": r.ad_sales_1d,
            "ad_sales_7d": r.ad_sales_7d,
            "ad_sales_14d": r.ad_sales_14d,
            "ad_sales_30d": r.ad_sales_30d,
            "tad_sales": r.tad_sales,
            "tad_sales_7d": r.tad_sales_7d,
            "tad_sales_14d": r.tad_sales_14d,
            "tad_sales_30d": r.tad_sales_30d,
        }
        for r in rows
    ]
    return {
        "items": items,
        "page": page,
        "page_size": page_size,
        "total": int(total),
    }


@router.get("/ad-sales/export")
def export_ad_sales(
    ids: List[int] = Query(..., description="选中的记录 id，可重复传参 ids=1&ids=2"),
    db: Session = Depends(get_db),
):
    wanted = [int(x) for x in ids if x is not None]
    wanted = [x for x in wanted if x > 0]
    if not wanted:
        raise HTTPException(status_code=400, detail="ids 不能为空")

    rows = (
        db.query(DailyAdCostSales)
        .filter(DailyAdCostSales.id.in_(wanted))
        .order_by(DailyAdCostSales.purchase_date.desc(), DailyAdCostSales.id.desc())
        .all()
    )
    if not rows:
        raise HTTPException(status_code=404, detail="未匹配到可导出的记录")

    headers = [
        "id",
        "ad_asin",
        "store_id",
        "pid",
        "variation_id",
        "purchase_date",
        "ad_cost",
        "sales_1d",
        "sales_7d",
        "sales_14d",
        "sales_30d",
        "ad_sales_1d",
        "ad_sales_7d",
        "ad_sales_14d",
        "ad_sales_30d",
        "tad_sales",
        "tad_sales_7d",
        "tad_sales_14d",
        "tad_sales_30d",
    ]
    output = StringIO()
    w = csv.writer(output)
    w.writerow(headers)
    for r in rows:
        w.writerow(
            [
                r.id,
                r.ad_asin,
                r.store_id,
                r.pid,
                r.variation_id,
                r.purchase_date.isoformat() if r.purchase_date else None,
                float(r.ad_cost) if r.ad_cost is not None else None,
                float(r.sales_1d) if r.sales_1d is not None else None,
                float(r.sales_7d) if r.sales_7d is not None else None,
                float(r.sales_14d) if r.sales_14d is not None else None,
                float(r.sales_30d) if r.sales_30d is not None else None,
                r.ad_sales_1d,
                r.ad_sales_7d,
                r.ad_sales_14d,
                r.ad_sales_30d,
                r.tad_sales,
                r.tad_sales_7d,
                r.tad_sales_14d,
                r.tad_sales_30d,
            ]
        )
    output.seek(0)
    filename = "ad_sales.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

