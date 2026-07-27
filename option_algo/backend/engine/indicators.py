# backend/engine/indicators.py
import pandas as pd
from typing import Optional


def ema(series: pd.Series, span: int) -> Optional[float]:
    if series is None or len(series) < span:
        return None
    try:
        return float(series.ewm(span=span, adjust=False).mean().iloc[-1])
    except Exception:
        return None


def rsi(series: pd.Series, period: int = 7) -> Optional[float]:
    if series is None or len(series) < period + 1:
        return None
    try:
        delta   = series.diff().dropna()
        up      = delta.clip(lower=0)
        down    = -delta.clip(upper=0)
        ma_up   = up.rolling(window=period).mean()
        ma_dn   = down.rolling(window=period).mean()
        if ma_dn.iloc[-1] == 0 or pd.isna(ma_dn.iloc[-1]):
            return 100.0 if ma_up.iloc[-1] > 0 else None
        rs  = ma_up / ma_dn
        val = 100 - (100 / (1 + rs))
        return float(val.iloc[-1]) if not val.isnull().any() else None
    except Exception:
        return None


def roc(series: pd.Series, period: int = 5) -> Optional[float]:
    if series is None or len(series) < period + 1:
        return None
    try:
        base = float(series.iloc[-(period + 1)])
        curr = float(series.iloc[-1])
        return ((curr - base) / base * 100) if base != 0 else None
    except Exception:
        return None


def vwap(df: pd.DataFrame) -> Optional[float]:
    if df.empty:
        return None
    try:
        tp  = (df["high"] + df["low"] + df["close"]) / 3
        vol = df["volume"] if "volume" in df.columns and df["volume"].sum() > 0 \
              else pd.Series([1.0] * len(df), index=df.index)
        return float((tp * vol).sum() / vol.sum())
    except Exception:
        return None


def atr(df: pd.DataFrame, period: int = 7) -> Optional[float]:
    if df is None or len(df) < period + 1:
        return None
    try:
        d = df.copy()
        d["H-L"]  = d["high"] - d["low"]
        d["H-PC"] = (d["high"] - d["close"].shift(1)).abs()
        d["L-PC"] = (d["low"]  - d["close"].shift(1)).abs()
        d["TR"]   = d[["H-L", "H-PC", "L-PC"]].max(axis=1)
        d["ATR"]  = d["TR"].rolling(period).mean()
        return float(d["ATR"].iloc[-1])
    except Exception:
        return None


def body_ratio(candle: dict) -> float:
    rng = candle["high"] - candle["low"]
    return abs(candle["close"] - candle["open"]) / rng if rng > 0 else 0.0


def upper_wick_ratio(candle: dict) -> float:
    rng = candle["high"] - candle["low"]
    uw  = candle["high"] - candle["close"]
    return uw / rng if rng > 0 else 1.0
