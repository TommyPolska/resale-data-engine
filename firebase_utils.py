# firebase_utils.py
import json
import streamlit as st
from google.cloud import firestore
from google.oauth2 import service_account

@st.cache_resource
def _db():
    """
    Works with either:
      [firebase].credentials_json  (stringified JSON)
      OR the entire service-account dict under [firebase]
    """
    fb = st.secrets["firebase"]
    if isinstance(fb.get("credentials_json", None), str):
        info = json.loads(fb["credentials_json"])
        project_id = fb["project_id"]
    else:
        # The secret itself is the service account dict
        info = dict(fb)
        project_id = fb.get("project_id") or info.get("project_id")
    creds = service_account.Credentials.from_service_account_info(info)
    return firestore.Client(project=project_id, credentials=creds)

def fetch_recent_listings(limit=1000):
    """Latest docs regardless of query; we’ll filter in Python."""
    db = _db()
    q = db.collection("listings").order_by("end_time", direction=firestore.Query.DESCENDING).limit(limit)
    return [d.to_dict() for d in q.stream()]

