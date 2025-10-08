# firebase_utils.py — robust secrets handling
import json
import streamlit as st
from google.cloud import firestore
from google.oauth2 import service_account

@st.cache_resource
def _db():
    """
    Supports TWO secrets formats:

    A) RAW FIELDS (recommended)
       [firebase]
       type="service_account"
       project_id="..."
       private_key="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
       client_email="..."
       ...

    B) STRINGIFIED JSON (optional)
       [firebase]
       project_id="..."
       credentials_json = """{ ... full JSON ... }"""
    """
    if "firebase" not in st.secrets:
        raise RuntimeError("Missing [firebase] in Streamlit secrets.")

    fb = st.secrets["firebase"]
    info = None
    project_id = None

    # Try B) stringified JSON first (but don't die if it's bad)
    cred_str = fb.get("credentials_json")
    if isinstance(cred_str, str) and cred_str.strip():
        try:
            info = json.loads(cred_str)
            project_id = fb.get("project_id") or info.get("project_id")
        except Exception:
            info = None  # fall through to RAW FIELDS

    # A) RAW FIELDS
    if info is None:
        info = dict(fb)  # clone; includes keys like type, private_key, etc.
        project_id = fb.get("project_id") or info.get("project_id")

    if not project_id:
        raise RuntimeError("Firebase project_id not found in [firebase] secrets.")

    # Fix escaped newlines if present
    if "private_key" in info and isinstance(info["private_key"], str):
        info["private_key"] = info["private_key"].replace("\\n", "\n")

    creds = service_account.Credentials.from_service_account_info(info)
    return firestore.Client(project=project_id, credentials=creds)

def fetch_recent_listings(limit: int = 1000,
                          status: str | None = None,
                          marketplace: str | None = None,
                          query_contains: str | None = None):
    db = _db()
    ref = db.collection("listings")
    if status:
        ref = ref.where("status", "==", status)
    if marketplace:
        ref = ref.where("marketplace", "==", marketplace)
    ref = ref.order_by("end_time", direction=firestore.Query.DESCENDING).limit(limit)
    docs = [d.to_dict() for d in ref.stream()]
    if query_contains:
        q = query_contains.strip().lower()
        docs = [d for d in docs if q in (d.get("title", "").lower())]
    return docs
