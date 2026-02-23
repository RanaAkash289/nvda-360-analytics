import csv
import datetime as dt
import os
import time
import requests

BASE = "https://api.gdeltproject.org/api/v2/doc/doc"

# Start smaller to avoid 429; you can increase later (180 -> 365 -> 730)
DAYS_BACK = 90

QUERY = '(nvidia OR NVDA OR "NVIDIA Corporation")'

OUT_DAILY = r"data\external\gdelt_nvda_daily.csv"
OUT_HEADLINES = r"data\external\gdelt_nvda_headlines.csv"

def yyyymmddhhmmss(d: dt.datetime) -> str:
    return d.strftime("%Y%m%d%H%M%S")

def request_with_backoff(params: dict, max_retries: int = 6) -> dict:
    delay = 2
    for attempt in range(max_retries):
        r = requests.get(BASE, params=params, timeout=60)

        if r.status_code == 200:
            return r.json()

        # Rate limited
        if r.status_code == 429:
            print(f"[429] Rate limited. Sleeping {delay}s (attempt {attempt+1}/{max_retries})...")
            time.sleep(delay)
            delay = min(delay * 2, 60)
            continue

        # Other error
        r.raise_for_status()

    raise RuntimeError("GDELT rate limit: too many retries. Try again later or reduce DAYS_BACK further.")

def fetch_timeline(mode: str, start_dt: dt.datetime, end_dt: dt.datetime) -> list:
    params = {
        "query": QUERY,
        "mode": mode,  # "timelinevol" or "timelinetone" are usually lighter than raw
        "format": "json",
        "startdatetime": yyyymmddhhmmss(start_dt),
        "enddatetime": yyyymmddhhmmss(end_dt),
        "timelinesmooth": 0
    }
    data = request_with_backoff(params)
    if "error" in data:
        print(f"[GDELT ERROR] mode={mode}: {data['error']}")
        return []
    tl = data.get("timeline", [])
    return tl if isinstance(tl, list) else []

def fetch_articles(start_dt: dt.datetime, end_dt: dt.datetime, limit: int = 50) -> list:
    params = {
        "query": QUERY,
        "mode": "artlist",
        "format": "json",
        "startdatetime": yyyymmddhhmmss(start_dt),
        "enddatetime": yyyymmddhhmmss(end_dt),
        "maxrecords": limit,
        "sort": "datedesc"
    }
    data = request_with_backoff(params)
    if "error" in data:
        print(f"[GDELT ERROR] mode=artlist: {data['error']}")
        return []
    return data.get("articles", [])

def get_date_key(item: dict) -> str | None:
    # Common keys in timeline payloads
    return item.get("date") or item.get("day") or item.get("datetime")

def parse_date(dstr: str) -> str:
    for fmt in ("%Y%m%d%H%M%S", "%Y%m%d"):
        try:
            return dt.datetime.strptime(dstr, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return dstr

def main():
    os.makedirs(r"data\external", exist_ok=True)

    # Simple caching: if daily file exists and is recent, skip timeline calls
    if os.path.exists(OUT_DAILY):
        print("Daily file already exists:", OUT_DAILY)
        print("Delete it if you want to re-fetch.")
    else:
        end_dt = dt.datetime.utcnow()
        start_dt = end_dt - dt.timedelta(days=DAYS_BACK)

        # Use lighter modes to reduce rate limits
        vol = fetch_timeline("timelinevol", start_dt, end_dt)       # lighter than timelinevolraw
        tone = fetch_timeline("timelinetone", start_dt, end_dt)
        print("vol timeline rows:", len(vol))
        print("tone timeline rows:", len(tone))

        tone_map = {}
        for t in tone:
            if isinstance(t, dict):
                dk = get_date_key(t)
                if dk:
                    tone_map[dk] = t.get("value")

        with open(OUT_DAILY, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Date", "ArticleCount", "AvgTone"])
            for v in vol:
                if not isinstance(v, dict):
                    continue
                dk = get_date_key(v)
                if not dk:
                    continue
                w.writerow([parse_date(dk), v.get("value", 0), tone_map.get(dk)])

        print("Saved:", OUT_DAILY)

    # Headlines: always safe to refresh (but still rate-limited, so backoff helps)
    end_dt = dt.datetime.utcnow()
    articles = fetch_articles(end_dt - dt.timedelta(days=30), end_dt, limit=50)

    with open(OUT_HEADLINES, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["SeenDateUTC", "Title", "URL", "SourceCountry", "Language"])
        for a in articles:
            if isinstance(a, dict):
                w.writerow([
                    a.get("seendate"),
                    a.get("title"),
                    a.get("url"),
                    a.get("sourceCountry"),
                    a.get("language")
                ])

    print("Saved:", OUT_HEADLINES)

if __name__ == "__main__":
    main()
