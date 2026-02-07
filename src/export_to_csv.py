import os
import pandas as pd

EXPORT_DIR = "data/export"

def export_parquet_to_csv(parquet_path: str, csv_name: str):
    os.makedirs(EXPORT_DIR, exist_ok=True)
    df = pd.read_parquet(parquet_path)
    out_path = os.path.join(EXPORT_DIR, csv_name)
    df.to_csv(out_path, index=False)
    print("Exported:", out_path, "Rows:", len(df))

if __name__ == "__main__":
    export_parquet_to_csv("data/processed/nvda_prices_features.parquet", "nvda_prices_features.csv")
    export_parquet_to_csv("data/processed/nvda_financials_quarterly.parquet", "nvda_financials_quarterly.csv")
