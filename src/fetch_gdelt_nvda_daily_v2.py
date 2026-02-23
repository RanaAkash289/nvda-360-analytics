import csv
import datetime as dt
import os
import time
import requests

# GDELT DOC 2.0 endpoint
BASE = "https://api.gdeltproject.org/api/v2/doc/doc"

# Keep this moderate to avoid 429s. You can try 365 later.
DAYS_BACK = 180

# Query terms for NVIDIA
QUERY = '(nvidia OR "NVIDIA Corporation" OR NVDA)'

OUT_DAILY = r"data\external\gdelt_nvda_daily.csv"
OUT_HEADLINES = r"data\external\gdelt_nvda_headlines.csv"


def yyyymmddhhmmss(d: dt.datetime) -> str:
    return d.strftime("%Y%m%d%H%M%S")


def request_with_backoff(params: dict, max_retries: int = 7) -> dict:
    delay = 2
    for attempt in range(max_retries):
        r = requests.get(BASE, params=params, timeout=60)

        # Rate limiting
        if r.status_code == 429:
            print(f"[429] Rate limited. Sleeping {delay}s (attempt {attempt+1}/{max_retries})...")
            time.sleep(delay)
            delay = min(delay * 2, 90)
            continue

        # Other HTTP issues
        if r.status_code != 200:
            snippet = (r.text or "")[:250].replace("\n", " ")
            print(f"[HTTP {r.status_code}] Sleeping {delay}s. Snippet: {snippet}")
            time.sleep(delay)
            delay = min(delay * 2, 90)
            continue

        # Try parsing JSON
        try:
            return r.json()
        except Exception:
            snippet = (r.text or "")[:300].replace("\n", " ")
            print(f"[Non-JSON] Sleeping {delay}s. Snippet: {snippet}")
            time.sleep(delay)
            delay = min(delay * 2, 90)
            continue

    raise RuntimeError("GDELT failed too many times (rate limit / non-JSON). Reduce DAYS_BACK and retry.")


def fetch_timeline(mode: str, start_dt: dt.datetime, end_dt: dt.datetime) -> dict:
    """
    Returns a dict with a 'timeline' key.
    For timeline modes, GDELT often returns:
      {"timeline":[{"series":"...", "data":[{"date":"...","value":...}, ...]}]}
    """
    params = {
        "query": QUERY,
        "mode": mode,  # "timelinevolraw" or "timelinetone"
        "format": "json",
        "startdatetime": yyyymmddhhmmss(start_dt),
        "enddatetime": yyyymmddhhmmss(end_dt),
        "timelinesmooth": 0,
    }
    data = request_with_backoff(params)
    if "error" in data:
        print(f"[GDELT ERROR] mode={mode}: {data['error']}")
    return data


def fetch_articles(start_dt: dt.datetime, end_dt: dt.datetime, limit: int = 50) -> list:
    params = {
        "query": QUERY,
        "mode": "artlist",
        "format": "json",
        "startdatetime": yyyymmddhhmmss(start_dt),
        "enddatetime": yyyymmddhhmmss(end_dt),
        "maxrecords": limit,
        "sort": "datedesc",
    }
    data = request_with_backoff(params)
    if "error" in data:
        print(f"[GDELT ERROR] mode=artlist: {data['error']}")
        return []
    return data.get("articles", []) if isinstance(data.get("articles", []), list) else []


def parse_date(dstr: str) -> str:
    # Expected: 20250817T000000Z
    # We'll parse the first 8 digits as YYYYMMDD
    if not dstr:
        return ""
    try:
        ymd = dstr[:8]
        return dt.datetime.strptime(ymd, "%Y%m%d").strftime("%Y-%m-%d")
    except Exception:
        return dstr


def extract_series_points(timeline_payload: dict) -> list:
    """
    Extracts the list of daily points from:
      payload["timeline"][0]["data"]
    """
    tl = timeline_payload.get("timeline", [])
    if not isinstance(tl, list) or len(tl) == 0:
        return []
    first = tl[0]
    if not isinstance(first, dict):
        return []
    points = first.get("data", [])
    return points if isinstance(points, list) else []


def main():
    os.makedirs(r"data\external", exist_ok=True)

    end_dt = dt.datetime.utcnow()
    start_dt = end_dt - dt.timedelta(days=DAYS_BACK)

    # Fetch series
    vol_payload = fetch_timeline("timelinevolraw", start_dt, end_dt)
    tone_payload = fetch_timeline("timelinetone", start_dt, end_dt)

    vol_points = extract_series_points(vol_payload)
    tone_points = extract_series_points(tone_payload)

    print("vol daily points:", len(vol_points))
    print("tone daily points:", len(tone_points))
    if vol_points:
        print("sample vol point:", vol_points[0])
    if tone_points:
        print("sample tone point:", tone_points[0])

    # Build tone map by date
    tone_map = {}
    for p in tone_points:
        if isinstance(p, dict) and p.get("date"):
            tone_map[p["date"]] = p.get("value")

    # Write daily CSV
    with open(OUT_DAILY, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Date", "ArticleCount", "AvgTone"])
        for p in vol_points:
            if not isinstance(p, dict) or not p.get("date"):
                continue
            d = p["date"]
            w.writerow([parse_date(d), p.get("value", 0), tone_map.get(d)])

    # Headlines (last 30 days)
    articles = fetch_articles(end_dt - dt.timedelta(days=30), end_dt, limit=50)
    with open(OUT_HEADLINES, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["SeenDateUTC", "Title", "URL", "SourceCountry", "Language"])
        for a in articles:
            if not isinstance(a, dict):
                continue
            w.writerow([
                a.get("seendate"),
                a.get("title"),
                a.get("url"),
                a.get("sourceCountry"),
                a.get("language"),
            ])

    print("Saved:", OUT_DAILY)
    print("Saved:", OUT_HEADLINES)


if __name__ == "__main__":
    main()
