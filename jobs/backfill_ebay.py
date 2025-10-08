# jobs/backfill_ebay.py — FILE-ONLY CREDENTIALS VERSION
import os, time, requests
from typing import Dict, Any, List

# --------- CONFIG ---------
QUERIES = ["Jordan 1", "Nike Dunk", "Yeezy", "Pokemon sealed booster", "LEGO Star Wars"]
MAX_PAGES = 5
ENTRIES_PER_PAGE = 100
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
    selling = item.get("sellingStatus", {})
    current_price = _first(selling.get("currentPrice", [{}]).get("__value__"), None)
    return {
        "marketplace": "ebay",
        "status": "completed",
        "listing_id": _first(item.get("itemId")),
        "title": _first(item.get("title"), ""),
        "category": _first(item.get("primaryCategory", [{}]).get("categoryName"), ""),
        "price": float(current_price) if current_price else None,
        "currency": _first(selling.get("currentPrice", [{}]).get("@currencyId"), "USD"),
        "seller": _first(item.get("sellerInfo", [{}]).get("sellerUserName"), ""),
        "seller_feedback": int(_first(item.get("sellerInfo", [{}]).get("feedbackScore"), 0) or 0),
        "condition": _first(item.get("condition", [{}]).get("conditionDisplayName"), ""),
        "image": _first(item.get("galleryURL"), ""),
        "start_time": _first(item.get("listingInfo", [{}]).get("startTime"), None),
        "end_time": _first(item.get("listingInfo", [{}]).get("endTime"), None),
        "raw": item,
    }

# --------- FIRESTORE (FILE-ONLY) ---------
def firestore_client(project_id: str):
    from google.cloud import firestore
    path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not path or not os.path.exists(path):
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS is not set or file not found")
    # The client will read the JSON pointed to by GOOGLE_APPLICATION_CREDENTIALS
    return firestore.Client(project=project_id)

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

    db = firestore_client(project_id)
    total_saved = 0

    for q in QUERIES:
        for page in range(1, MAX_PAGES + 1):
            resp = find_completed_items(app_id, q, page)
            root = resp.get("findCompletedItemsResponse", [{}])[0]
            search = root.get("searchResult", [{}])[0]
            arr = search.get("item", [])
            if not arr:
                break
            rows = [normalize_item(i) for i in arr]
            total_saved += save_listings(db, rows)
            time.sleep(0.5)
        print(f"[OK] '{q}' processed. Total saved so far: {total_saved}")
    print(f"Saved {total_saved} completed listings.")

if __name__ == "__main__":
    main()
