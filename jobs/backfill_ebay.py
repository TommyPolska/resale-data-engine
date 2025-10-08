# jobs/backfill_ebay.py — resilient eBay + Firestore (file-based creds)
import os, time, requests, sys
from typing import Dict, Any, List, Optional

# --------- CONFIG ---------
QUERIES = ["Jordan 1", "Nike Dunk", "Yeezy", "Pokemon sealed booster", "LEGO Star Wars"]
MAX_PAGES = 5
ENTRIES_PER_PAGE = 100  # will auto-reduce on repeated errors
FINDING_ENDPOINT = "https://svcs.ebay.com/services/search/FindingService/v1"
RETRY_STATUS = {429, 500, 502, 503, 504}
MAX_RETRIES = 5
BASE_SLEEP = 1.5  # seconds (exponential backoff)

# --------- EBAY CALL (with retry/backoff) ---------
def call_finding(app_id: str, operation: str, payload: Dict[str, Any], entries_per_page: int) -> Dict[str, Any]:
    headers = {
        "X-EBAY-SOA-OPERATION-NAME": operation,
        "X-EBAY-SOA-SERVICE-VERSION": "1.13.0",
        "X-EBAY-SOA-REQUEST-DATA-FORMAT": "JSON",
        "X-EBAY-SOA-GLOBAL-ID": "EBAY-US",
        "X-EBAY-SOA-SECURITY-APPNAME": app_id,
        "Content-Type": "application/json",
    }

    # Inject the page size if the caller forgot
    if operation == "findCompletedItems":
        payload.setdefault("findCompletedItemsRequest", {}).setdefault("paginationInput", {})["entriesPerPage"] = entries_per_page
    elif operation == "findItemsByKeywords":
        payload.setdefault("findItemsByKeywordsRequest", {}).setdefault("paginationInput", {})["entriesPerPage"] = entries_per_page

    attempt = 0
    while True:
        attempt += 1
        try:
            r = requests.post(FINDING_ENDPOINT, headers=headers, json=payload, timeout=60)
            if r.status_code in RETRY_STATUS:
                # Log and retry
                body = r.text[:1000]
                print(f"[WARN] eBay {operation} HTTP {r.status_code} (attempt {attempt}): {body}", file=sys.stderr)
                if attempt >= MAX_RETRIES:
                    r.raise_for_status()
                time.sleep(BASE_SLEEP * (2 ** (attempt - 1)))
                continue

            r.raise_for_status()
            try:
                return r.json()
            except Exception as je:
                # Sometimes 200 with html/plain error
                print(f"[ERROR] JSON parse failed (attempt {attempt}): {je}\nBody: {r.text[:1000]}", file=sys.stderr)
                if attempt >= MAX_RETRIES:
                    raise
                time.sleep(BASE_SLEEP * (2 ** (attempt - 1)))
        except requests.exceptions.RequestException as e:
            print(f"[WARN] Network/API error (attempt {attempt}): {e}", file=sys.stderr)
            if attempt >= MAX_RETRIES:
                raise
            time.sleep(BASE_SLEEP * (2 ** (attempt - 1)))

def find_completed_items(app_id: str, query: str, page: int, entries_per_page: int) -> Dict[str, Any]:
    payload = {
        "findCompletedItemsRequest": {
            "keywords": query,
            "itemFilter": [{"name": "SoldItemsOnly", "value": True}],
            "paginationInput": {"pageNumber": page},
            "outputSelector": ["SellerInfo", "PictureURLSuperSize"],
        }
    }
    return call_finding(app_id, "findCompletedItems", payload, entries_per_page)

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

# --------- FIRESTORE (file-based creds) ---------
def firestore_client(project_id: str):
    from google.cloud import firestore
    path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not path or not os.path.exists(path):
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS is not set or file not found")
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
        print(f"[INFO] Query: {q}")
        consecutive_errors = 0
        entries = ENTRIES_PER_PAGE
        for page in range(1, MAX_PAGES + 1):
            try:
                resp = find_completed_items(app_id, q, page, entries)
                root = resp.get("findCompletedItemsResponse", [{}])[0]
                # If API returned an error payload, log it
                ack = root.get("ack", [""])[0] if isinstance(root.get("ack"), list) else root.get("ack", "")
                if ack and str(ack).lower() != "success":
                    print(f"[WARN] Non-success ACK on page {page}: {ack} | resp snippet={str(resp)[:300]}", file=sys.stderr)

                search = root.get("searchResult", [{}])[0]
                arr = search.get("item", [])
                if not arr:
                    print(f"[INFO] No items on page {page}; stopping pages for '{q}'.")
                    break

                rows = [normalize_item(i) for i in arr]
                saved = save_listings(db, rows)
                total_saved += saved
                print(f"[OK] {q} p{page} saved {saved}; total={total_saved}")
                consecutive_errors = 0
                time.sleep(0.5)

            except Exception as e:
                consecutive_errors += 1
                print(f"[ERROR] {q} p{page}: {e}", file=sys.stderr)
                # After 2 consecutive failures for a query, reduce page size (some errors are payload-size related)
                if consecutive_errors == 2 and entries > 50:
                    entries = 50
                    print(f"[INFO] Reducing entries_per_page to {entries} and retrying next page.", file=sys.stderr)
                    continue
                # After 3 consecutive failures, skip to next query
                if consecutive_errors >= 3:
                    print(f"[WARN] Skipping remaining pages for '{q}' after repeated errors.", file=sys.stderr)
                    break

        print(f"[INFO] Finished '{q}'. Total saved so far: {total_saved}")

    print(f"Saved {total_saved} completed listings.")

if __name__ == "__main__":
    main()
