import os
import io
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from data_fetch import fetch_completed_or_live

load_dotenv()
APP_ID = os.getenv("EBAY_APP_ID")

st.set_page_config(page_title="IRL Market Analytics", layout="wide")
st.title("📈 IRL Market Analytics (Sneakers → Everything)")
st.caption("Search any item. We’ll try SOLD history first, fall back to LIVE listings if eBay rate-limits. Toggle Offline if needed.")

colA, colB, colC, colD = st.columns([3,1.4,1.4,1.2])
with colA:
    q = st.text_input("Search item", value="Air Jordan 1 Retro High")
with colB:
    per_page = st.selectbox("Results per call", [10, 25, 50, 100], index=1)
with colC:
    offline = st.toggle("Offline mode (sample)", value=False)
with colD:
    go = st.button("Fetch", type="primary", use_container_width=True)

if not APP_ID and not offline:
    st.warning("No EBAY_APP_ID found in your .env. You can still use Offline mode.")
if go or offline:
    with st.status("Fetching data…", expanded=False) as s:
        mode, df, raw, note = fetch_completed_or_live(q, per_page=per_page, offline=offline)
        s.update(label=f"Mode: {mode.upper()} ({note})", state="complete")

    st.write(f"**Mode:** {mode} • **Rows:** {len(df)} • **Query:** {q}")
    st.divider()

    if df.empty:
        st.info("No rows to show. Try a broader query or use Offline mode.")
    else:
        price_col = "total_price" if "total_price" in df.columns else "total_ask"
        if "date" in df.columns and df["date"].notna().any():
            daily = df.dropna(subset=["date"]).groupby("date").median(numeric_only=True)[price_col].reset_index(name="median_price")
            fig, ax = plt.subplots(figsize=(8,4))
            ax.plot(daily["date"], daily["median_price"], label="Median")
            if len(daily) >= 3:
                ema = daily["median_price"].ewm(span=3, adjust=False).mean()
                ax.plot(daily["date"], ema, label="EMA-3")
            ax.set_title(f"Trend — {q}")
            ax.set_xlabel("Date"); ax.set_ylabel("USD"); ax.grid(True); ax.legend()
            st.pyplot(fig)
        else:
            st.info("No date field available (LIVE mode). Showing table only.")

        st.dataframe(df, use_container_width=True, height=420)

        buf = io.StringIO()
        df.to_csv(buf, index=False)
        st.download_button("Download CSV", buf.getvalue(), file_name=f"{mode}_{q.replace(' ','_')}.csv", mime="text/csv")
