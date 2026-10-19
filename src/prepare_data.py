"""Extract + clean the UCI Online Retail workbook into a replayable event file.

Outputs
  data/events.jsonl          one event per invoice line, sorted by invoice time
  data/quality_report.json   row counts for every cleaning rule
"""
import json

import pandas as pd

from common import DATA

RAW = DATA / "Online_Retail.xlsx"
EVENTS = DATA / "events.jsonl"
REPORT = DATA / "quality_report.json"

REQUIRED = ["InvoiceNo", "StockCode", "InvoiceDate", "Quantity", "UnitPrice"]


def clean(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    report = {"raw_rows": len(df)}
    df = df.copy()
    df.columns = [c.strip().replace(" ", "") for c in df.columns]

    before = len(df)
    df = df.dropna(subset=REQUIRED)
    report["dropped_missing_required"] = before - len(df)

    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
    before = len(df)
    df = df[df["InvoiceDate"].notna()]
    report["dropped_bad_date"] = before - len(df)

    # Negative prices are bad-debt adjustments, not sales.
    before = len(df)
    df = df[df["UnitPrice"] >= 0]
    report["dropped_negative_price"] = before - len(df)

    before = len(df)
    df = df.drop_duplicates()
    report["dropped_exact_duplicates"] = before - len(df)

    df["InvoiceNo"] = df["InvoiceNo"].astype(str)
    df["is_cancellation"] = df["InvoiceNo"].str.startswith("C")
    df["is_return"] = df["Quantity"] < 0
    df["Country"] = df["Country"].fillna("Unknown").astype(str)
    df["revenue"] = (df["UnitPrice"].astype(float) * df["Quantity"].astype(int)).round(2)

    report["rows_kept"] = len(df)
    report["rows_missing_customer_id"] = int(df["CustomerID"].isna().sum())
    report["rows_returns"] = int(df["is_return"].sum())
    report["first_invoice"] = df["InvoiceDate"].min().isoformat()
    report["last_invoice"] = df["InvoiceDate"].max().isoformat()
    return df.sort_values("InvoiceDate", kind="stable"), report


def to_events(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "event_time": df["InvoiceDate"].dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "invoice_no": df["InvoiceNo"],
        "stock_code": df["StockCode"].astype(str),
        "description": df["Description"].astype("string").str.strip(),
        "quantity": df["Quantity"].astype(int),
        "unit_price": df["UnitPrice"].astype(float),
        "customer_id": df["CustomerID"].astype("Int64"),
        "country": df["Country"],
        "revenue": df["revenue"],
        "is_return": df["is_return"],
        "is_cancellation": df["is_cancellation"],
    })


def main() -> None:
    assert RAW.exists(), f"Dataset not found: {RAW}"
    print(f"Reading {RAW.name} ...")
    df, report = clean(pd.read_excel(RAW))
    events = to_events(df)
    events.to_json(EVENTS, orient="records", lines=True, force_ascii=False)
    REPORT.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    print(f"Wrote {len(events):,} events -> {EVENTS}")


if __name__ == "__main__":
    main()
