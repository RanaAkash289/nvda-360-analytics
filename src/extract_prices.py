import os
import pandas as pd

STOOQ_URL = "https://stooq.com/q/d/l/?s=nvda.us&i=d"

def fetch_prices(out_path: str):
    df = pd.read_csv(STOOQ_URL)
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_parquet(out_path, index=False)
    return out_path

if __name__ == "__main__":
    path = fetch_prices("data/raw/prices_nvda_daily.parquet")
    print("Saved:", path)
