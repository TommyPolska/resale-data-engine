# jobs/backfill_ebay.py  — robust version
import os
import json
import time
import requests
from typing import Dict, Any, List

# --------- CONFIG ---------
QUERIES = [
    "Jordan 1",
    "Nike Dunk",
    "Yeezy",
    "Pokemon sealed booster",
    "LEGO Star Wars",
]
MAX_PAGES = 5           # per query
ENTRIES_PER_PAGE = 100  # max for Finding API

FINDING_ENDPOINT = "https://svcs.ebay.com/services/search/FindingService/v1"


# --------- EBAY CALL ---------
def call_finding(app_id: str, operation: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    headers = {
        "X-EBAY-SOA-OPERATION-NAME": operation,
        "X-EBAY-SOA-SERVICE-VERSION": "1.13.0",
        "X-EBAY-SOA-REQUEST-DATA-FORMAT": "JSON",
        "X-EBAY-SOA-GLOBAL-ID": "EBAY-US",
        "X-EBAY-SOA-SECURITY-APPNAME": app_id,
        "Content-Type": "application/json",
    }
    r = requests.post(FINDING_ENDPOINT, headers=headers, json=payload, timeout=45)
    r.raise_for_status()
    return r.json()


def find_completed_items(app_id: str, query: str, page: int) -> Dict[str, Any]:
    payload = {
        "findCompletedItemsRequest": {
            "keywords": query,
            "itemFilter": [{"name": "SoldItemsOnly", "value": True}],
            "paginationInput": {"entriesPerPage": ENTRIES_PER_PAGE, "pageNumber": page},
            "outputSelector": ["SellerInfo", "PictureURLSuperSize"],
        }
    }
    return call_finding(app_id, "findCompletedItems", payload)


# --------- NORMALIZE ---------
def _first(v, default=None):
    if isinstance(v, list) and v:
        return v[0]
    return v if v is not None else default


def normalize_item(item: Dict[str, Any]) -> Dict[str, Any]:
    item_id = _first(item.get("itemId"))
    title = _first(item.get("title"), "")
    category = _first(item.get("primaryCategory", [{}]).get("categoryName"), "")
    selling = item.get("sellingStatus", {})
    current_price = _first(selling.get("currentPrice", [{}]).get("__value__"), None)
    currency = _first(selling.get("currentPrice", [{}]).get("@currencyId"), "USD")
    seller = _first(item.get("sellerInfo", [{}]).get("sellerUserName"), "")
    feedback = int(_first(item.get("sellerInfo", [{}]).get("feedbackScore"), 0) or 0)
    condition_display = _first(item.get("condition", [{}]).get("conditionDisplayName"), "")
    gallery = _first(item.get("galleryURL"), "")
    end_time = _first(item.get("listingInfo", [{}]).get("endTime"), None)
    start_time = _first(item.get("listingInfo", [{}]).get("startTime"), None)

    return {
        "marketplace": "ebay",
        "status": "completed",
        "listing_id": item_id,
        "title": title,
        "category": category,
        "price": float(current_price) if current_price else None,
        "currency": currency,
        "seller": seller,
        "seller_feedback": feedback,
        "condition": condition_display,
        "image": gallery,
        "start_time": start_time,
        "end_time": end_time,
        "raw": item,
    }


# --------- FIRESTORE ---------
def firestore_client(project_id: str, creds_json: str | None):
    """
    Supports two auth paths:
      1) GOOGLE_APPLICATION_CREDENTIALS points to a json file (recommended in Actions)
      2) FIREBASE_CREDENTIALS_JSON env contains the raw JSON (with \\n newlines)
    """
    from google.cloud import firestore
    from google.oauth2 import service_account

    path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if path and os.path.exists(path):
        creds = service_account.Credentials.from_service_account_file(path)
    else:
        if not creds_json:
            raise RuntimeError("Missing FIREBASE_CREDENTIALS_JSON and no GOOGLE_APPLICATION_CREDENTIALS file provided.")
        info = json.loads(creds_json)
        # Fix escaped newlines in private_key (GitHub Secrets often escape them)
        if "private_key" in info and isinstance(info["private_key"], str):
            info["private_key"] = info["private_key"].replace("\\n", "\n")
        creds = service_account.Credentials.from_service_account_info(info)

    return firestore.Client(project=project_id, credentials=creds)


def save_listings(db, rows: List[Dict[str, Any]]):
    if not rows:
        return 0
    batch = db.batch()
    col_ref = db.collection("listings")
    count = 0
    for r in rows:
        item_id = r.get("listing_id")
        if not item_id:
            continue
        doc_id = f'{r["marketplace"]}_{r["status"]}_{item_id}'
        batch.set(col_ref.document(doc_id), r, merge=True)
        count += 1
    batch.commit()
    return count


# --------- MAIN ---------
def main():
    app_id = os.environ["EBAY_APP_ID"]
    project_id = os.environ["FIREBASE_PROJECT_ID"]
    firebase_creds = os.environ.get("FIREBASE_CREDENTIALS_JSON")  # may be None if using credentials file

    db = firestore_client(project_id, firebase_creds)
    total_saved = 0

    for q in QUERIES:
        for page in range(1, MAX_PAGES + 1):
            resp = find_completed_items(app_id, q, page)
            # Defensive parsing: handle missing keys gracefully
            root = resp.get("findCompletedItemsResponse", [{}])[0]
            search = root.get("searchResult", [{}])[0]
            arr = search.get("item", [])
            if not arr:
                break
            rows = [normalize_item(i) for i in arr]
            saved = save_listings(db, rows)
            total_saved += saved
            time.sleep(0.5)  # be nice to eBay API
        print(f"[OK] Query '{q}' saved so far. Total saved: {total_saved}")
    print(f"Saved {total_saved} completed listings.")


if __name__ == "__main__":
    main()

