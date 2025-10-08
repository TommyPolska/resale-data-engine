import os, time, json
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
APP_ID = os.getenv("EBAY_APP_ID")
FINDING_URL = "https://svcs.ebay.com/services/search/FindingService/v1"
GLOBAL_ID = "EBAY-US"

SAMPLE_COMPLETED = {
  "findCompletedItemsResponse": [{
    "ack": ["Success"],
    "searchResult": [{
      "item": [
        {
          "title": ["Air Jordan 1 Retro High 'Bred'"],
          "sellingStatus": [{"currentPrice": [{"__value__": "219.99", "@currencyId": "USD"}]}],
          "shippingInfo": [{"shippingServiceCost": [{"__value__": "14.99"}]}],
          "listingInfo": [{"endTime": ["2025-09-22T19:42:39.000Z"]}],
          "viewItemURL": ["https://www.ebay.com/itm/xxx1"]
        },
        {
          "title": ["Air Jordan 1 Retro High 'Royal'"],
          "sellingStatus": [{"currentPrice": [{"__value__": "199.00", "@currencyId": "USD"}]}],
          "shippingInfo": [{"shippingServiceCost": [{"__value__": "0.00"}]}],
          "listingInfo": [{"endTime": ["2025-09-21T18:01:02.000Z"]}],
          "viewItemURL": ["https://www.ebay.com/itm/xxx2"]
        }
      ]
    }]
  }]
}

def _rate_limited(payload: dict) -> bool:
    em = payload.get("errorMessage")
    if not em: return False
    errs = (em[0].get("error", []) if isinstance(em, list) else em.get("error", [])) or []
    for e in errs:
        if (str((e.get("errorId") or [""])[0]) == "10001"
            and (e.get("subdomain") or [""])[0] == "RateLimiter"):
            return True
    return False

def _fetch(operation: str, keywords: str, per_page=25, timeout=12,
           max_retries=2, max_wait=5.0):
    headers = {
        "X-EBAY-SOA-OPERATION-NAME": operation,
        "X-EBAY-SOA-SERVICE-VERSION": "1.13.0",
        "X-EBAY-SOA-SECURITY-APPNAME": APP_ID,
        "X-EBAY-SOA-RESPONSE-DATA-FORMAT": "JSON",
        "X-EBAY-SOA-GLOBAL-ID": GLOBAL_ID,
        "Accept": "application/json",
        "User-Agent": "local-prototype/1.0",
    }
    params = {
        "keywords": keywords,
        "paginationInput.entriesPerPage": per_page,
        "paginationInput.pageNumber": 1,
    }
    if operation == "findCompletedItems":
        params["itemFilter(0).name"] = "SoldItemsOnly"
        params["itemFilter(0).value"] = "true"

    slept, sleep_s = 0.0, 0.8
    last = {}
    for _ in range(max_retries + 1):
        r = requests.get(FINDING_URL, headers=headers, params=params, timeout=timeout)
        ct = (r.headers.get("Content-Type") or "").lower()
        data = r.json() if "json" in ct else {}
        last = {"status": r.status_code, "data": data}

        if r.status_code == 200 and not _rate_limited(data):
            root = data.get(operation + "Response")
            if isinstance(root, list): root = root[0]
            if root: return True, data, "ok"
            return False, data, "unexpected-shape"

        if r.status_code >= 500 or _rate_limited(data):
            if slept + sleep_s > max_wait: break
            time.sleep(sleep_s); slept += sleep_s
            sleep_s = min(sleep_s * 2, 2)
            continue
        break
    return False, last.get("data") or {}, f"blocked:{last.get('status')}"

def fetch_completed_or_live(keywords: str, per_page=25, offline=False):
    if offline:
        df = to_df_completed(SAMPLE_COMPLETED)
        return "sample", df, SAMPLE_COMPLETED, "offline-sample"

    ok, payload, why = _fetch("findCompletedItems", keywords, per_page=per_page)
    if ok:
        return "sold", to_df_completed(payload), payload, "ok"

    ok2, payload2, why2 = _fetch("findItemsByKeywords", keywords, per_page=per_page)
    if ok2:
        return "live", to_df_live(payload2), payload2, "sold-blocked:" + why
    return "sample", to_df_completed(SAMPLE_COMPLETED), SAMPLE_COMPLETED, f"both-blocked:{why2}"

def to_df_completed(data: dict) -> pd.DataFrame:
    resp = data.get("findCompletedItemsResponse")
    if isinstance(resp, list): resp = resp[0]
    sr = (resp or {}).get("searchResult"); sr = sr[0] if isinstance(sr, list) else sr
    items = (sr or {}).get("item", []) or []
    rows = []
    for it in items:
        title = (it.get("title") or [""])[0]
        price = float((it.get("sellingStatus") or [{}])[0].get("currentPrice",[{"__value__":"0"}])[0]["__value__"])
        ship  = float((it.get("shippingInfo") or [{}])[0].get("shippingServiceCost",[{"__value__":"0"}])[0]["__value__"])
        end   = (it.get("listingInfo") or [{}])[0].get("endTime", [""])[0]
        dt    = pd.to_datetime(end, errors="coerce")
        url   = (it.get("viewItemURL") or [""])[0]
        rows.append({
            "title": title, "sold_price": price, "shipping": ship,
            "total_price": price + ship, "date": dt.date() if pd.notna(dt) else pd.NaT,
            "url": url
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.dropna(subset=["total_price", "date"]).sort_values("date")
    return df

def to_df_live(data: dict) -> pd.DataFrame:
    resp = data.get("findItemsByKeywordsResponse")
    if isinstance(resp, list): resp = resp[0]
    sr = (resp or {}).get("searchResult"); sr = sr[0] if isinstance(sr, list) else sr
    items = (sr or {}).get("item", []) or []
    rows = []
    for it in items:
        title = (it.get("title") or [""])[0]
        selling = (it.get("sellingStatus") or [{}])[0]
        price = float((selling.get("currentPrice") or [{"__value__":"0"}])[0]["__value__"])
        ship  = float(((it.get("shippingInfo") or [{}])[0].get("shippingServiceCost") or [{"__value__":"0"}])[0]["__value__"])
        url   = (it.get("viewItemURL") or [""])[0]
        rows.append({"title":title,"ask_price":price,"shipping":ship,"total_ask":price+ship,"url":url})
    df = pd.DataFrame(rows)
    return df
