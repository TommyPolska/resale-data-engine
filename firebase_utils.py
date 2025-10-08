from firebase_utils import get_listings
import pandas as pd
import streamlit as st

st.title("🔥 Sneaker Resale Analytics")

# Load from Firestore
listings = get_listings(limit=100)
df = pd.DataFrame(listings)

st.write("Latest completed listings:")
st.dataframe(df[["title", "price", "seller", "end_time"]])



# firebase_utils.py
import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore
import json
import os

@st.cache_resource
def init_firebase():
    # Read service account from Streamlit secrets
    firebase_creds = st.secrets["firebase"]
    cred = credentials.Certificate(firebase_creds)
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    return firestore.client()

def get_listings(limit=50):
    db = init_firebase()
    listings_ref = db.collection("listings").order_by("end_time", direction=firestore.Query.DESCENDING).limit(limit)
    docs = listings_ref.stream()
    return [doc.to_dict() for doc in docs]
