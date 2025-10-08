# predictor.py — simple time-trend model + prep
from __future__ import annotations
import pandas as pd
import numpy as np
from typing import Tuple, Dict

def prepare_df(rows, query_substr: str, lookback_days: int) -> pd.DataFrame:
    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    if df.empty:
        return df

    # choose a time column
    tcol = "end_time" if "end_time" in df.columns else ("start_time" if "start_time" in df.columns else None)
    if tcol is None:
        return pd.DataFrame()

    # keep completed + positive prices
    if "status" in df.columns:
        df = df[df["status"] == "completed"]
    if "price" in df.columns:
        df = df[df["price"].notna()]
        df = df[df["price"].astype(float) > 0]

    # substring filter on title
    q = (query_substr or "").strip().lower()
    if q and "title" in df.columns:
        df = df[df["title"].str.lower().str.contains(q, na=False)]

    # parse time + lookback window
    dt = pd.to_datetime(df[tcol], errors="coerce", utc=True)
    df = df.assign(dt=dt).dropna(subset=["dt"])
    cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=lookback_days)
    df = df[df["dt"] >= cutoff]

    # numeric time feature
    df["ts"] = df["dt"].astype("int64") // 10**9
    return df.sort_values("dt")[["dt", "ts", "price", "title"]]

def train_time_trend(df: pd.DataFrame) -> Tuple[Dict, float]:
    """
    Minimal baseline: linear regression on time (closed-form).
    Returns (params, mae) where params has slope & intercept.
    """
    if len(df) < 12:
        raise ValueError("Need at least 12 sold datapoints to train a meaningful trend.")

    n = len(df)
    split = max(int(n * 0.8), 1)
    train, test = df.iloc[:split], df.iloc[split:]

    # y = a*ts + b
    x = train["ts"].values.reshape(-1, 1).astype(float)
    y = train["price"].astype(float).values
    X = np.hstack([x, np.ones_like(x)])
    theta = np.linalg.pinv(X.T @ X) @ (X.T @ y)
    slope, intercept = float(theta[0]), float(theta[1])

    mae = float("nan")
    if len(test):
        xt = test["ts"].values.reshape(-1, 1).astype(float)
        Xt = np.hstack([xt, np.ones_like(xt)])
        pred = Xt @ theta
        mae = float(np.mean(np.abs(pred - test["price"].values)))

    return {"slope": slope, "intercept": intercept}, mae

def predict_now(model_params: Dict) -> float:
    now_ts = pd.Timestamp.utcnow().value // 10**9
    return float(model_params["slope"] * now_ts + model_params["intercept"])
