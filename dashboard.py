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

# Two tabs: Search & Trends, Portfolio
tab1, tab2 = st.tabs(["Search & Trends", "Portfolio"])

# ---------------------------
# TAB 1: Search & Trends
# ---------------------------
with tab1:
    st.caption("Search any item. We try SOLD history first, fall back to LIVE listings. Use Offline if rate-limited.")

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
        st.warning("No EBAY_APP_ID set in secrets/.env. You can still use Offline mode.")

    if go or offline:
        with st.status("Fetching data…", expanded=False) as s:
            mode, df, raw, note = fetch_completed_or_live(q, per_page=per_page, offline=offline)
            s.update(label=f"Mode: {mode.upper()} ({note})", state="complete")

        st.write(f"**Mode:** `{mode}` • **Rows:** `{len(df)}` • **Query:** `{q}`")
        st.divider()

        if df.empty:
            st.info("No rows to show. Try a broader query or use Offline mode.")
        else:
            price_col = "total_price" if "total_price" in df.columns else "total_ask"

            # Chart only when we have dates (SOLD mode)
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

            # Download CSV
            buf = io.StringIO(); df.to_csv(buf, index=False)
            st.download_button("Download CSV", buf.getvalue(), file_name=f"{mode}_{q.replace(' ','_')}.csv", mime="text/csv")

# ---------------------------
# TAB 2: Portfolio
# ---------------------------
with tab2:
    st.caption("Upload what you own and get estimated value + P&L. Start from the template.")
    st.link_button("Download portfolio template CSV",
                   url="https://raw.githubusercontent.com/TommyPolska/resale-data-engine/main/portfolio_template.csv")

    up = st.file_uploader("Upload portfolio CSV", type=["csv"])
    default_query = st.text_input("Valuation keyword (used to fetch market prices)", value="Air Jordan 1 Retro High")
    per_page_val = st.selectbox("Results per call (valuation)", [10, 25, 50, 100], index=1, key="perpage_portfolio")
    offline_val = st.toggle("Offline mode for valuation (sample)", value=False, key="offline_portfolio")
    run_val = st.button("Value Portfolio", type="primary")

    if run_val and up is not None:
        port = pd.read_csv(up)

        # Validate columns
        cols_lower = {c.lower() for c in port.columns}
        required = {"title","acquisition_price","qty"}
        if not required.issubset(cols_lower):
            st.error("CSV must include: title, acquisition_price, qty (and optional acquisition_date).")
        else:
            # Fetch market data to get a fair value per item
            with st.status("Fetching market data for valuation…", expanded=False) as s:
                mode_v, dfm, rawm, note_v = fetch_completed_or_live(default_query, per_page=per_page_val, offline=offline_val)
                s.update(label=f"Valuation mode: {mode_v.upper()} ({note_v})", state="complete")

            if dfm.empty:
                st.warning("No market rows to compute a valuation. Try another keyword or Offline mode.")
            else:
                price_col = "total_price" if "total_price" in dfm.columns else "total_ask"
                if "date" in dfm.columns and dfm["date"].notna().any():
                    current_est = float(dfm.dropna(subset=["date"]).groupby("date")[price_col].median().iloc[-1])
                else:
                    current_est = float(dfm[price_col].median())

                st.metric("Estimated fair value (per item)", f"${current_est:,.2f}")

                # Normalize, compute portfolio metrics
                port.columns = [c.lower() for c in port.columns]
                port["qty"] = port["qty"].fillna(1).astype(float)
                port["acquisition_price"] = port["acquisition_price"].astype(float)

                port["est_value_each"] = current_est
                port["est_value_total"] = port["est_value_each"] * port["qty"]
                port["cost_total"] = port["acquisition_price"] * port["qty"]
                port["unrealized_pnl"] = port["est_value_total"] - port["cost_total"]
                port = port[["title","qty","acquisition_price","cost_total","est_value_each","est_value_total","unrealized_pnl"]]

                c1, c2, c3 = st.columns(3)
                with c1: st.metric("Portfolio cost", f"${port['cost_total'].sum():,.0f}")
                with c2: st.metric("Portfolio value", f"${port['est_value_total'].sum():,.0f}")
                with c3: st.metric("Unrealized P&L", f"${port['unrealized_pnl'].sum():,.0f}")

                st.dataframe(port, use_container_width=True, height=420)

                # Download valuation CSV
                bufp = io.StringIO(); port.to_csv(bufp, index=False)
                st.download_button("Download Valuation CSV", bufp.getvalue(),
                                   file_name="portfolio_valuation.csv", mime="text/csv")

    elif run_val and up is None:
        st.warning("Please upload your portfolio CSV first.")
