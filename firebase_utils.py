# firebase_utils.py — fixed (no self-imports)
import json
import streamlit as st
from google.cloud import firestore
from google.oauth2 import service_account

@st.cache_resource
def _db():
    """
    Initializes Firestore using Streamlit secrets.
    Works with either:
      - [firebase] fields pasted directly (service account dict), or
      - [firebase].credentials_json = "{...}" (stringified JSON)
    """
    fb = st.secrets["firebase"]

    # If you stored the JSON as a single string
    if isinstance(fb.get("credentials_json", None), str):
        info = json.loads(fb["credentials_json"])
        project_id = fb.get("project_id") or info.get("project_id")
    else:
        # If you pasted the service account fields directly under [firebase]
        info = dict(fb)
        project_id = fb.get("project_id") or info.get("project_id")

    # Fix escaped newlines if present
    if "private_key" in info and isinstance(info["private_key"], str):
        info["private_key"] = info["private_key"].replace("\\n", "\n")

    creds = service_account.Credentials.from_service_account_info(info)
    return firestore.Client(project=project_id, credentials=creds)

def fetch_recent_listings(limit: int = 1000, status: str | None = None,
                          marketplace: str | None = None, query_contains: str | None = None):
    """
    Returns latest listings from Firestore. You can optionally filter by status/marketplace
    and a substring in the title (client-side).
    """
    db = _db()
    ref = db.collection("listings")

    if status:
        ref = ref.where("status", "==", status)
    if marketplace:
        ref = ref.where("marketplace", "==", marketplace)

    # Sort by end_time if present; your backfill writes it for completed items
    ref = ref.order_by("end_time", direction=firestore.Query.DESCENDING).limit(limit)

    docs = [d.to_dict() for d in ref.stream()]

    if query_contains:
        q = query_contains.strip().lower()
        docs = [d for d in docs if q in (d.get("title", "").lower())]

    return docs
