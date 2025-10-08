# pages/1_Predict.py
import streamlit as st
import pandas as pd

from firebase_utils import fetch_recent_listings
from ml.predictor import prepare_df, train_time_trend, predict_now

st.set_page_config(page_title="Predict", page_icon="🔮", layout="wide")

st.title("🔮 Price Prediction (SOLD history → Today)")

# Controls
c1, c2 = st.columns([2,1])
with c1:
    query = st.text_input("Search item / keywords", "Air Jordan 1 Retro High")
with c2:
    lookback = st.slider("Lookback window (days)", 14, 365, 120, step=7)

st.caption("We train a simple time-trend model on SOLD history and estimate a 'today' price. Upgradeable later to richer ML.")

# Load data
with st.spinner("Loading Firestore data..."):
    rows = fetch_recent_listings(limit=1500)

df = prepare_df(rows, query, lookback)

if df.empty:
    st.warning("No recent SOLD results found for that query/window. Try a broader query or increase lookback.")
    st.stop()

st.write(f"Found **{len(df)}** sold results for **{query}** in the last **{lookback}** days.")

# Show basic stats
c3, c4, c5 = st.columns(3)
c3.metric("Median sold", f"${df['price'].median():,.2f}")
c4.metric("Avg sold", f"${df['price'].mean():,.2f}")
c5.metric("Most recent", f"${df['price'].iloc[-1]:,.2f}")

# Train model
try:
    params, mae = train_time_trend(df)
except ValueError as e:
    st.warning(str(e) + " Try increasing lookback or broadening your query.")
    st.stop()

pred_today = predict_now(params)
low = max(0.0, pred_today - 1.2*mae)
high = pred_today + 1.2*mae

st.subheader("🧠 Model estimate")
c6, c7, c8 = st.columns(3)
c6.metric("Predicted 'today' price", f"${pred_today:,.2f}")
c7.metric("Validation MAE", f"${mae:,.2f}")
c8.metric("Band (±1.2 × MAE)", f"${low:,.0f} — ${high:,.0f}")

# Chart
st.subheader("📈 Sold price over time")
chart_df = df[["dt","price"]].rename(columns={"dt":"Date","price":"Price"})
st.line_chart(chart_df, x="Date", y="Price", height=300)

# Table (latest 50)
st.subheader("Recent SOLD rows")
st.dataframe(df[["dt","title","price"]].sort_values("dt", ascending=False).head(50), use_container_width=True)
