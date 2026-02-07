import os, json
import requests
from dotenv import load_dotenv

load_dotenv()

CIK_NVDA = "0001045810"
SEC_FACTS_URL = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{CIK_NVDA}.json"

def fetch_sec_companyfacts(out_path: str):
    ua = os.getenv("SEC_USER_AGENT")
    if not ua:
        raise RuntimeError("Set SEC_USER_AGENT in .env (SEC requires a descriptive User-Agent).")

    headers = {"User-Agent": ua, "Accept-Encoding": "gzip, deflate", "Host": "data.sec.gov"}
    r = requests.get(SEC_FACTS_URL, headers=headers, timeout=60)
    r.raise_for_status()

    data = r.json()
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return out_path

if __name__ == "__main__":
    path = fetch_sec_companyfacts("data/raw/sec_companyfacts_nvda.json")
    print("Saved:", path)
