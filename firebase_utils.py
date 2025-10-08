# firebase_utils.py  — minimal + compatible
import json
import streamlit as st
from typing import Optional, Dict, Any
from google.cloud import firestore
from google.oauth2 import service_account

@st.cache_resource
def _db():
    """
    Works with either Streamlit secrets format:

    A) RAW FIELDS (recommended)
       [firebase]
       type = "service_account"
       project_id = "..."
       private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
       client_email = "..."
       client_id = "..."
       token_uri = "https://oauth2.googleapis.com/token"
       ... (etc)

    B) STRINGIFIED JSON
       [firebase]
       project_id = "..."
       credentials_json = """{ ...full service account JSON... }"""
    """
    if "firebase" not in st.secrets:
        raise RuntimeError("Missing [firebase] section in Streamlit secrets.")

    fb = st.secrets["firebase"]

    info = None  # type: Optional[Dict[str, Any]]
    project_id = None  # type: Optional[str]

    # Try B) stringified JSON
    cred_str = fb.get("credentials_json")
    if isinstance(cred_str, str) and cred_str.strip().startswith("{"):
        try:
            info = json.loads(cred_str)
            project_id = fb.get("project_id") or info.get("project_id")
        except Exception:
            info = None  # fall through to RAW FIELDS

    # A) RAW FIELDS
    if info is None:
        info = dict(fb)  # copy all keys under [firebase]
        project_id = fb.get("project_id") or info.get("project_id")

    if not project_id:
        raise RuntimeError("Firebase project_id not found in [firebase] secrets.")

    # Normalize private_key newlines
    pk = info.get("private_key")
    if isinstance(pk, str):
        info["private_key"] = pk.replace("\\n", "\n")

    creds = service_account.Credentials.from_service_account_info(info)
    return firestore.Client(project=project_id, credentials=creds)

def fetch_recent_listings(limit=1000, status=None, marketplace=None, query_contains=None):
    db = _db()
    ref = db.collection("listings")
    if status:
        ref = ref.where("status", "==", status)
    if marketplace:
        ref = ref.where("marketplace", "==", marketplace)

    # Completed eBay docs have end_time set by your backfill
    ref = ref.order_by("end_time", direction=firestore.Query.DESCENDING).limit(int(limit))
    docs = [d.to_dict() for d in ref.stream()]

    if query_contains:
        q = str(query_contains).strip().lower()
        docs = [d for d in docs if q in str(d.get("title", "")).lower()]

    return docs
