# ml/predictor.py
from __future__ import annotations
import pandas as pd
import numpy as np
from typing import Tuple, Dict

def prepare_df(rows, query_substr: str, lookback_days: int) -> pd.DataFrame:
    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    if df.empty:
        return df
    # Clean & filter
    # use end_time if present, else start_time
    tcol = np.where(df.get("end_time").notna() if "end_time" in df else False, "end_time", "start_time")
    tcol = "end_time" if ("end_time" in df.columns) else ("start_time" if "start_time" in df.columns else None)
    if tcol is None:
        return pd.DataFrame()

    df = df[df["status"] == "completed"]
    df = df[df["price"].notna()]
    df = df[df["price"].astype(float) > 0]

    # substring match on title
    q = query_substr.strip().lower()
    if q:
        df = df[df["title"].str.lower().str.contains(q, na=False)]

    # parse datetime and filter recent window
    dt = pd.to_datetime(df[tcol], errors="coerce", utc=True)
    df = df.assign(dt=dt).dropna(subset=["dt"])
    cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=lookback_days)
    df = df[df["dt"] >= cutoff]

    # build a time feature (seconds since epoch)
    df["ts"] = df["dt"].astype("int64") // 10**9
    # light cleanup
    df = df.sort_values("dt")
    return df[["dt", "ts", "price", "title"]]

def train_time_trend(df: pd.DataFrame) -> Tuple[Dict, float]:
    """
    Ultra-simple baseline: linear regression on time to capture trend.
    Returns (model_params, mae).
    model_params: dict with slope, intercept.
    """
    if len(df) < 12:
        raise ValueError("Need at least 12 sold datapoints to train a meaningful trend.")

    # train/test split (last 20% holdout)
    n = len(df)
    split = max(int(n * 0.8), 1)
    train = df.iloc[:split]
    test = df.iloc[split:]

    # Fit y = a*ts + b
    x = train["ts"].values.reshape(-1, 1).astype(float)
    y = train["price"].astype(float).values
    # closed-form least squares
    # add bias column
    X = np.hstack([x, np.ones_like(x)])
    # theta = (X^T X)^-1 X^T y
    theta = np.linalg.pinv(X.T @ X) @ (X.T @ y)
    slope, intercept = float(theta[0]), float(theta[1])

    # evaluate
    xt = test["ts"].values.reshape(-1, 1).astype(float)
    Xt = np.hstack([xt, np.ones_like(xt)])
    pred = Xt @ theta
    mae = float(np.mean(np.abs(pred - test["price"].values))) if len(test) else float("nan")

    return {"slope": slope, "intercept": intercept}, mae

def predict_now(model_params: Dict) -> float:
    now_ts = pd.Timestamp.utcnow().value // 10**9
    return float(model_params["slope"] * now_ts + model_params["intercept"])
