# firebase_utils.py — base64-only, minimal, robust
import json, base64
import streamlit as st
from google.cloud import firestore
from google.oauth2 import service_account

@st.cache_resource
def _db():
    """
    Requires in Streamlit secrets:
      [firebase]
      credentials_b64 = "<base64 of your full firebase.json>"

    We decode the exact bytes of your firebase.json, so there are no newline/escape issues.
    """
    if "firebase" not in st.secrets:
        raise RuntimeError("Missing [firebase] in Streamlit secrets.")

    fb = st.secrets["firebase"]
    cred_b64 = fb.get("credentials_b64")
    if not isinstance(cred_b64, str) or not cred_b64.strip():
        raise RuntimeError("firebase.credentials_b64 is missing or empty in secrets.")

    # Decode the full JSON
    try:
        raw = base64.b64decode(cred_b64)
        info = json.loads(raw.decode("utf-8"))
    except Exception as e:
        raise RuntimeError(f"Failed to decode/parse firebase.credentials_b64: {e}")

    # Validate minimal fields
    project_id = info.get("project_id")
    if not project_id:
        raise RuntimeError("Decoded firebase.json is missing 'project_id'.")

    pk = info.get("private_key", "")
    if not (isinstance(pk, str) and pk.startswith("-----BEGIN PRIVATE KEY-----") and "END PRIVATE KEY" in pk):
        raise RuntimeError("Decoded firebase.json has an invalid 'private_key' (PEM header/footer missing).")

    # Build client
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
