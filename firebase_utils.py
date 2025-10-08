# firebase_utils.py  — minimal + safe
import json
import streamlit as st
from google.cloud import firestore
from google.oauth2 import service_account

@st.cache_resource
def _db():
    # Read secrets
    if "firebase" not in st.secrets:
        raise RuntimeError("Missing [firebase] in Streamlit secrets.")

    fb = st.secrets["firebase"]

    # Support either a stringified JSON or raw fields
    info = None
    if isinstance(fb.get("credentials_json", None), str) and fb["credentials_json"].strip().startswith("{"):
        try:
            info = json.loads(fb["credentials_json"])
        except Exception:
            info = None

    if info is None:
        info = dict(fb)  # raw fields pasted directly

    # Normalize private_key newlines
    if "private_key" in info and isinstance(info["private_key"], str):
        info["private_key"] = info["private_key"].replace("\\n", "\n")

    project_id = fb.get("project_id") or info.get("project_id")
    if not project_id:
        raise RuntimeError("Firebase project_id not found in secrets.")

    creds = service_account.Credentials.from_service_account_info(info)
    return firestore.Client(project=project_id, credentials=creds)

def fetch_recent_listings(limit=1000, status=None, marketplace=None, query_contains=None):
    db = _db()
    ref = db.collection("listings")
    if status:
        ref = ref.where("status", "==", status)
    if marketplace:
        ref = ref.where("marketplace", "==", marketplace)
    ref = ref.order_by("end_time", direction=firestore.Query.DESCENDING).limit(int(limit))
    docs = [d.to_dict() for d in ref.stream()]
    if query_contains:
        q = str(query_contains).strip().lower()
        docs = [d for d in docs if q in str(d.get("title", "")).lower()]
    return docs
