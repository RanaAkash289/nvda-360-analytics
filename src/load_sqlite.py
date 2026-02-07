import os
import pandas as pd
from sqlalchemy import create_engine

DB_PATH = "db/nvda_360.sqlite"

def load_parquet(table: str, parquet_path: str):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    engine = create_engine(f"sqlite:///{DB_PATH}")
    df = pd.read_parquet(parquet_path)
    df.to_sql(table, engine, if_exists="replace", index=False)
    print(f"Loaded {table}: {len(df)} rows")

if __name__ == "__main__":
    load_parquet("prices_daily_raw", "data/raw/prices_nvda_daily.parquet")
    load_parquet("prices_daily_features", "data/processed/nvda_prices_features.parquet")
    load_parquet("financials_quarterly", "data/processed/nvda_financials_quarterly.parquet")
    print("SQLite DB ready:", DB_PATH)
