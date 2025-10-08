# jobs/backfill_ebay.py — resilient + rate-limit aware (file-based creds)
import os, time, requests, sys, random
from typing import Dict, Any, List

# --------- CONFIG (override via env) ---------
QUERIES = os.environ.get("EBAY_QUERIES", "Jordan 1|Nike Dunk|Yeezy|Pokemon sealed booster|LEGO Star Wars").split("|")
MAX_PAGES = int(os.environ.get("EBAY_MAX_PAGES", "3"))        # be conservative
ENTRIES_PER_PAGE = int(os.environ.get("EBAY_ENTRIES", "100")) # will reduce on errors
REQUEST_BUDGET = int(os.environ.get("EBAY_REQUEST_BUDGET", "60"))  # total API calls per run
COOLDOWN_SECS = int(os.environ.get("EBAY_COOLDOWN_SECS", "180"))   # sleep after rate limit
BASE_SLEEP = float(os.environ.get("EBAY_BASE_SLEEP", "0.6"))       # between successful pages
MAX_RETRIES = int(os.environ.get("EBAY_MAX_RETRIES", "4"))         # per request
FINDING_ENDPOINT = "https://svcs.ebay.com/services/search/FindingService/v1"
RETRY_STATUS = {429, 500, 502, 503, 504}

# --------- EBAY CALL (retry + rate-limit detect) ---------
def call_finding(app_id: str, operation: str, payload: Dict[str, Any], entries_per_page: int) -> Dict[str, Any]:
    headers = {
        "X-EBAY-SOA-OPERATION-NAME": operation,
        "X-EBAY-SOA-SERVICE-VERSION": "1.13.0",
        "X-EBAY-SOA-REQUEST-DATA-FORMAT": "JSON",
        "X-EBAY-SOA-GLOBAL-ID": "EBAY-US",
        "X-EBAY-SOA-SECURITY-APPNAME": app_id,
        "Content-Type": "application/json",
    }
    if operation == "findCompletedItems":
        payload.setdefault("findCompletedItemsRequest", {}).setdefault("paginationInput", {})["entriesPerPage"] = entries_per_page

    attempt = 0
    while True:
        attempt += 1
        try:
            r = requests.post(FINDING_ENDPOINT, headers=headers, json=payload, timeout=60)
            # Handle hard HTTP errors with retry
            if r.status_code in RETRY_STATUS:
                body = r.text[:1000]
                # Detect eBay RateLimiter errorId 10001
                if '"errorId":["10001"]' in body or '"subdomain":["RateLimiter"]' in body:
                    raise RateLimited(f"RateLimiter 10001: {body}")
                print(f"[WARN] HTTP {r.status_code} {operation} attempt {attempt}: {body}", file=sys.stderr)
                if attempt >= MAX_RETRIES:
                    r.raise_for_status()
                time.sleep(1.2 * attempt + random.random())
                continue

            r.raise_for_status()
            # Try to parse JSON; sometimes success returns text
            try:
                j = r.json()
            except Exception as je:
                print(f"[WARN] JSON parse fail (attempt {attempt}): {je} | body={r.text[:400]}", file=sys.stderr)
                if attempt >= MAX_RETRIES:
                    raise
                time.sleep(1.0 * attempt + random.random())
                continue

            # Also check inline ack errors (non-HTTP)
            root = j.get("findCompletedItemsResponse", [{}])[0]
            err = root.get("errorMessage", [])
            if err:
                body = str(err)[:600]
                if "10001" in body and "RateLimiter" in body:
                    raise RateLimited(f"Inline RateLimiter: {body}")
                print(f"[WARN] Inline API error attempt {attempt}: {body}", file=sys.stderr)
                if attempt >= MAX_RETRIES:
                    return j
                time.sleep(1.0 * attempt + random.random())
                continue

            return j

        except RateLimited as rl:
            # Bubble up to outer loop to handle cooldown
            raise rl

        except requests.exceptions.RequestException as e:
            print(f"[WARN] Network/API exception (attempt {attempt}): {e}", file=sys.stderr)
            if attempt >= MAX_RETRIES:
                raise
            time.sleep(1.0 * attempt + random.random())


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

# --------- Custom Exceptions ---------
class RateLimited(Exception):
    pass

# --------- MAIN ---------
def main():
    app_id = os.environ["EBAY_APP_ID"]
    project_id = os.environ["FIREBASE_PROJECT_ID"]
    db = firestore_client(project_id)

    total_saved = 0
    requests_used = 0

    # Randomize query order slightly to avoid hammering same query first
    random.shuffle(QUERIES)

    for q in QUERIES:
        print(f"[INFO] Query: {q}")
        consecutive_errors = 0
        entries = ENTRIES_PER_PAGE

        for page in range(1, MAX_PAGES + 1):
            if requests_used >= REQUEST_BUDGET:
                print(f"[INFO] Request budget {REQUEST_BUDGET} reached; stopping run.")
                print(f"Saved {total_saved} completed listings.")
                return

            try:
                resp = find_completed_items(app_id, q, page, entries)
                requests_used += 1

                root = resp.get("findCompletedItemsResponse", [{}])[0]
                search = root.get("searchResult", [{}])[0]
                arr = search.get("item", [])
                if not arr:
                    print(f"[INFO] No items on page {page}; stopping pages for '{q}'.")
                    break

                rows = [normalize_item(i) for i in arr]
                saved = save_listings(db, rows)
                total_saved += saved
                print(f"[OK] {q} p{page} saved {saved}; total={total_saved}; requests_used={requests_used}")

                consecutive_errors = 0
                time.sleep(BASE_SLEEP + random.random() * 0.4)  # polite pacing

            except RateLimited as rl:
                consecutive_errors += 1
                print(f"[RATE-LIMIT] {q} p{page}: {rl}", file=sys.stderr)
                # Back off with jitter, then move to NEXT QUERY to spread load
                backoff = COOLDOWN_SECS + random.randint(5, 30)
                print(f"[INFO] Cooling down for {backoff}s then skipping to next query.", file=sys.stderr)
                time.sleep(backoff)
                break  # move to next query after cooldown

            except Exception as e:
                consecutive_errors += 1
                print(f"[ERROR] {q} p{page}: {e}", file=sys.stderr)

                # After 2 consecutive failures, reduce page size once
                if consecutive_errors == 2 and entries > 50:
                    entries = 50
                    print(f"[INFO] Reducing entries_per_page to {entries}.", file=sys.stderr)
                    continue
                # After 3 consecutive failures, skip to next query
                if consecutive_errors >= 3:
                    print(f"[WARN] Skipping remaining pages for '{q}' after repeated errors.", file=sys.stderr)
                    break

        print(f"[INFO] Finished '{q}'. Total saved so far: {total_saved}")

    print(f"Saved {total_saved} completed listings.")

if __name__ == "__main__":
    main()
