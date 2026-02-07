import json
import os
import pandas as pd

# Metrics we want from SEC (US-GAAP tags)
METRICS = {
    "Revenue": ("us-gaap", "Revenues", ["USD"]),
    "NetIncome": ("us-gaap", "NetIncomeLoss", ["USD"]),
    "GrossProfit": ("us-gaap", "GrossProfit", ["USD"]),
    "OperatingIncome": ("us-gaap", "OperatingIncomeLoss", ["USD"]),
    "RAndD": ("us-gaap", "ResearchAndDevelopmentExpense", ["USD"]),
    "EPS_Diluted": ("us-gaap", "EarningsPerShareDiluted", ["USD/shares", "USD / shares"]),
}

def pick_unit(units: dict, preferred_units: list[str]) -> tuple[str, list] | tuple[None, None]:
    if not units:
        return None, None
    for u in preferred_units:
        if u in units:
            return u, units[u]
    # fallback: first available unit
    first_unit = next(iter(units.keys()))
    return first_unit, units[first_unit]

def extract_metric(sec: dict, taxonomy: str, tag: str, preferred_units: list[str]) -> pd.DataFrame:
    node = sec.get("facts", {}).get(taxonomy, {}).get(tag, {})
    units = node.get("units", {})
    unit, rows = pick_unit(units, preferred_units)
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    for col in ["start", "end", "filed"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    df["taxonomy"] = taxonomy
    df["tag"] = tag
    df["unit"] = unit
    return df

def build_quarterly_table(sec_path: str) -> pd.DataFrame:
    with open(sec_path, "r", encoding="utf-8") as f:
        sec = json.load(f)

    frames = []
    for friendly, (taxonomy, tag, pref_units) in METRICS.items():
        df = extract_metric(sec, taxonomy, tag, pref_units)
        if df.empty:
            continue
        df["metric"] = friendly
        frames.append(df)

    facts = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if facts.empty:
        return facts

    # Keep only 10-Q and 10-K
    if "form" in facts.columns:
        facts = facts[facts["form"].isin(["10-Q", "10-K"])]

    # Focus on quarterly points: fp often Q1/Q2/Q3/FY
    # We'll keep end date + metric + value and then take latest filing for duplicates.
    keep_cols = [c for c in ["end", "fy", "fp", "form", "filed", "val", "accn", "metric", "unit"] if c in facts.columns]
    facts = facts[keep_cols].rename(columns={"end": "period_end", "val": "value", "filed": "filed_date"})

    # Deduplicate: same metric + period_end → keep most recently filed
    facts = facts.sort_values(["metric", "period_end", "filed_date"])
    facts = facts.drop_duplicates(subset=["metric", "period_end"], keep="last")

    # Pivot to wide (one row per quarter)
    wide = facts.pivot_table(index=["period_end", "fy", "fp", "form"], columns="metric", values="value", aggfunc="first").reset_index()
    wide = wide.sort_values("period_end")

    # Add useful ratios
    if "GrossProfit" in wide.columns and "Revenue" in wide.columns:
        wide["GrossMargin"] = wide["GrossProfit"] / wide["Revenue"]
    if "NetIncome" in wide.columns and "Revenue" in wide.columns:
        wide["NetMargin"] = wide["NetIncome"] / wide["Revenue"]
    if "RAndD" in wide.columns and "Revenue" in wide.columns:
        wide["RnD_to_Revenue"] = wide["RAndD"] / wide["Revenue"]

    return wide

if __name__ == "__main__":
    out = build_quarterly_table("data/raw/sec_companyfacts_nvda.json")
    os.makedirs("data/processed", exist_ok=True)
    out_path = "data/processed/nvda_financials_quarterly.parquet"
    out.to_parquet(out_path, index=False)
    print("Saved:", out_path, "Rows:", len(out))
