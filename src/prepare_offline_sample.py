
import json
import pandas as pd
from pathlib import Path
from utils import parse_invoice_date

DATA_XLSX = Path(__file__).resolve().parents[1] / "data" / "Online_Retail.xlsx"
OUT_CSV   = Path(__file__).resolve().parents[1] / "data" / "online_retail_clean.csv"
OUT_JSONL = Path(__file__).resolve().parents[1] / "data" / "events.jsonl"

def clean(df: pd.DataFrame) -> pd.DataFrame:
    # Basic cleaning
    df = df.copy()
    df.columns = [c.strip().replace(" ", "") for c in df.columns]  # e.g., 'Invoice No' -> 'InvoiceNo'
    # Drop rows with missing essential fields
    df = df.dropna(subset=["InvoiceNo", "StockCode", "InvoiceDate", "UnitPrice", "Quantity"])
    # Remove negative quantities except for returns; we'll keep returns with flag later
    # Here we keep all, but mark returns where Quantity < 0
    # Parse dates
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce", infer_datetime_format=True)
    df = df[~df["InvoiceDate"].isna()]
    # Normalize text
    if "Description" in df.columns:
        df["Description"] = df["Description"].astype(str).str.strip()
    # Compute revenue
    df["Revenue"] = df["UnitPrice"].astype(float) * df["Quantity"].astype(int)
    # Add flags
    df["IsReturn"] = (df["Quantity"] < 0).astype(int)
    df["HasCustomerID"] = (~df["CustomerID"].isna()).astype(int)
    return df

def export_events(df: pd.DataFrame, out_path: Path, max_rows: int = 100000):
    # Export as line-delimited JSON events suitable for Kafka
    cols = ["InvoiceDate","InvoiceNo","StockCode","Description","Quantity","UnitPrice","CustomerID","Country","Revenue","IsReturn"]
    count = 0
    with out_path.open("w", encoding="utf-8") as f:
        for _, r in df[cols].head(max_rows).iterrows():
            ev = {
                "event_time": r["InvoiceDate"].isoformat(),
                "invoice_no": str(r["InvoiceNo"]),
                "stock_code": str(r["StockCode"]),
                "description": None if pd.isna(r["Description"]) else str(r["Description"]),
                "quantity": int(r["Quantity"]),
                "unit_price": float(r["UnitPrice"]),
                "customer_id": None if pd.isna(r["CustomerID"]) else int(r["CustomerID"]),
                "country": None if pd.isna(r["Country"]) else str(r["Country"]),
                "revenue": float(r["Revenue"]),
                "is_return": int(r["IsReturn"]),
            }
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")
            count += 1
    return count

def main():
    assert DATA_XLSX.exists(), f"Dataset not found: {DATA_XLSX}"
    df = pd.read_excel(DATA_XLSX)
    df_clean = clean(df)
    df_clean.to_csv(OUT_CSV, index=False)
    n_events = export_events(df_clean, OUT_JSONL)
    print(f"Saved cleaned CSV → {OUT_CSV}")
    print(f"Wrote {n_events} JSONL events → {OUT_JSONL}")

if __name__ == "__main__":
    main()
