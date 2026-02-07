import os
import pandas as pd

def build_price_features(in_path: str) -> pd.DataFrame:
    df = pd.read_parquet(in_path).copy()
    df = df.sort_values("Date")
    df["Close"] = pd.to_numeric(df["Close"], errors="coerce")

    df["DailyReturn"] = df["Close"].pct_change()
    df["MA_30"] = df["Close"].rolling(30).mean()
    df["MA_90"] = df["Close"].rolling(90).mean()
    df["MA_200"] = df["Close"].rolling(200).mean()

    # Volatility (30d rolling std of returns)
    df["Volatility_30d"] = df["DailyReturn"].rolling(30).std()
    return df

if __name__ == "__main__":
    out = build_price_features("data/raw/prices_nvda_daily.parquet")
    os.makedirs("data/processed", exist_ok=True)
    out_path = "data/processed/nvda_prices_features.parquet"
    out.to_parquet(out_path, index=False)
    print("Saved:", out_path, "Rows:", len(out))
