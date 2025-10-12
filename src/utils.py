import re
from datetime import datetime
from typing import Dict, Any

def parse_invoice_date(x):
    # Excel dates are strings like '12/1/2010 8:26'
    # Try multiple formats
    for fmt in ("%m/%d/%Y %H:%M", "%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(str(x), fmt)
        except Exception:
            continue
    return None

def base_event(record: Dict[str, Any]) -> Dict[str, Any]:
    # Map Online Retail fields to a normalized event
    return {
        "event_time": record.get("InvoiceDate"),
        "invoice_no": str(record.get("InvoiceNo")),
        "stock_code": str(record.get("StockCode")),
        "description": record.get("Description"),
        "quantity": int(record.get("Quantity")) if record.get("Quantity") is not None else 0,
        "unit_price": float(record.get("UnitPrice")) if record.get("UnitPrice") is not None else 0.0,
        "customer_id": None if pd.isna(record.get("CustomerID")) else int(record.get("CustomerID")),
        "country": record.get("Country"),
        "revenue": (float(record.get("UnitPrice")) if record.get("UnitPrice") is not None else 0.0) *                    (int(record.get("Quantity")) if record.get("Quantity") is not None else 0),
    }

try:
    import pandas as pd  # local import for speed in non-pandas contexts
except Exception:
    pd = None
