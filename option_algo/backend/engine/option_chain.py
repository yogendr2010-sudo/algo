# backend/engine/option_chain.py
# ================================================================
# Option Chain Analyzer  — with Dynamic Volume + OI Weighting
#
# Dynamic Weighting Logic:
#   For each analysis cycle, checks:
#     A) Highest volume strike is near spot (within ATM_WINDOW_POINTS)?
#     B) Highest OI strike is near spot?
#     C) Both hit same strike?
#
#   Weight grid:
#     Volume near spot only  → volume_weight=0.70, oi_weight=0.30
#     OI near spot only      → volume_weight=0.30, oi_weight=0.70
#     Both near spot + same  → volume_weight=0.50, oi_weight=0.50 (both equal)
#     Neither near spot      → volume_weight=0.30, oi_weight=0.70 (OI dominant)
#
# Engine v6 integration:
#   SymbolEngine._oc → OptionChainAnalyzer
#   _all_filters_pass checks is_bullish() → gates CE/PE entries
#   is_bullish() returns True(CE), False(PE), None(no filter)
# ================================================================

from collections import deque
from datetime import datetime
from typing import Optional
import threading
import time

import pandas as pd
import upstox_client
from upstox_client.rest import ApiException

# ================================================================
# CONFIG
# ================================================================

REFRESH_SEC       = 30
HISTORY_SIZE      = 240      # 240 × 30s ≈ 2 hours
COG_5M_BARS       = 10
COG_15M_BARS      = 30
COG_30M_BARS      = 60
TOP_FLOW_COUNT    = 10
ATM_WINDOW_POINTS = 200      # ±200 pts from spot = near-ATM zone
NEAR_SPOT_POINTS  = 300      # if highest vol/OI strike within this, considered "near spot"


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ================================================================
# DYNAMIC WEIGHT CALCULATOR
# ================================================================

def _calc_weights(df: pd.DataFrame, underlying_ltp: float) -> dict:
    """
    Decide dynamic weighting between volume analysis and OI analysis.

    Returns dict with:
      volume_weight  float  0.0-1.0
      oi_weight      float  0.0-1.0
      vol_near_spot  bool
      oi_near_spot   bool
      both_same      bool   highest vol and OI on same strike
      reason         str
    """
    try:
        # CE side
        ce_vol_top_strike = float(df.loc[df["ce_volume"].idxmax(), "strike"])
        ce_oi_top_strike  = float(df.loc[df["ce_oi"].idxmax(),     "strike"])
        # PE side
        pe_vol_top_strike = float(df.loc[df["pe_volume"].idxmax(), "strike"])
        pe_oi_top_strike  = float(df.loc[df["pe_oi"].idxmax(),     "strike"])

        ce_vol_near = abs(ce_vol_top_strike - underlying_ltp) <= NEAR_SPOT_POINTS
        ce_oi_near  = abs(ce_oi_top_strike  - underlying_ltp) <= NEAR_SPOT_POINTS
        pe_vol_near = abs(pe_vol_top_strike - underlying_ltp) <= NEAR_SPOT_POINTS
        pe_oi_near  = abs(pe_oi_top_strike  - underlying_ltp) <= NEAR_SPOT_POINTS

        vol_near_spot = ce_vol_near or pe_vol_near
        oi_near_spot  = ce_oi_near  or pe_oi_near

        ce_both_same = (ce_vol_top_strike == ce_oi_top_strike)
        pe_both_same = (pe_vol_top_strike == pe_oi_top_strike)
        both_same    = ce_both_same and pe_both_same

        if vol_near_spot and oi_near_spot and both_same:
            vw, ow = 0.50, 0.50
            reason = "Vol+OI both near spot on same strike → equal weight"
        elif vol_near_spot and oi_near_spot:
            vw, ow = 0.45, 0.55
            reason = "Vol+OI both near spot (diff strikes) → slight OI bias"
        elif vol_near_spot and not oi_near_spot:
            vw, ow = 0.70, 0.30
            reason = "Volume cluster near spot → volume dominant"
        elif oi_near_spot and not vol_near_spot:
            vw, ow = 0.30, 0.70
            reason = "OI cluster near spot → OI dominant"
        else:
            vw, ow = 0.30, 0.70
            reason = "Neither near spot → default OI dominant"

        return {
            "volume_weight":  vw,
            "oi_weight":      ow,
            "vol_near_spot":  vol_near_spot,
            "oi_near_spot":   oi_near_spot,
            "both_same":      both_same,
            "reason":         reason,
            "ce_vol_top":     ce_vol_top_strike,
            "ce_oi_top":      ce_oi_top_strike,
            "pe_vol_top":     pe_vol_top_strike,
            "pe_oi_top":      pe_oi_top_strike,
        }
    except Exception:
        return {
            "volume_weight": 0.30, "oi_weight": 0.70,
            "vol_near_spot": False, "oi_near_spot": False,
            "both_same": False, "reason": "default",
            "ce_vol_top": 0, "ce_oi_top": 0,
            "pe_vol_top": 0, "pe_oi_top": 0,
        }


# ================================================================
# OPTION CHAIN ANALYZER
# ================================================================

class OptionChainAnalyzer:
    """
    Polls Upstox option chain every REFRESH_SEC seconds.
    Computes composite flow score (volume + OI, dynamically weighted).
    Thread-safe via self.lock.
    """

    def __init__(self, symbol: str, underlying_key: str, expiry: str,
                 access_token: str, stop_event: threading.Event,
                 on_update: Optional["callable"] = None):
        self.symbol         = symbol
        self.underlying_key = underlying_key
        self.expiry         = expiry
        self.access_token   = access_token
        self.stop_event     = stop_event
        # Optional callback fired after each successful refresh() —
        # used by SymbolEngine to push the analysis snapshot to Redis
        # (state_store.set_oc_snapshot_sync) for the worker/web split.
        # Signature: on_update(analysis: dict, chain_df: pd.DataFrame)
        self.on_update      = on_update

        self.history          = deque(maxlen=HISTORY_SIZE)
        self.ce_cog_history   = deque(maxlen=HISTORY_SIZE)
        self.pe_cog_history   = deque(maxlen=HISTORY_SIZE)
        self.max_pain_history = deque(maxlen=HISTORY_SIZE)
        self.rotation_history = deque(maxlen=HISTORY_SIZE)
        self.signal_history   = deque(maxlen=HISTORY_SIZE)
        self.flow_5m          = deque(maxlen=HISTORY_SIZE)  # (ce_leader, pe_leader)

        self.latest_analysis: Optional[dict] = None
        self.thread: Optional[threading.Thread] = None
        self.lock = threading.Lock()

    # ================================================================
    # LIFECYCLE
    # ================================================================

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.thread = threading.Thread(
            target=self._loop, daemon=True, name=f"oc-{self.symbol}")
        self.thread.start()
        print(f"[OC {self.symbol}] started refresh={REFRESH_SEC}s expiry={self.expiry}")

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=5)

    def _loop(self):
        while not self.stop_event.is_set():
            try:
                self.refresh()
            except Exception as e:
                print(f"[OC {self.symbol}] loop error: {e}")
            time.sleep(REFRESH_SEC)

    # ================================================================
    # REFRESH
    # ================================================================

    def refresh(self):
        df = self.fetch_option_chain()
        if df.empty:
            return
        if len(self.history) == 0:
            self.history.append({"timestamp": _now_str(), "df": df.copy()})
            return
        prev_df = self.history[-1]["df"]
        result  = self.analyze(df, prev_df)
        with self.lock:
            self.latest_analysis = result
        self.history.append({"timestamp": _now_str(), "df": df.copy(), "analysis": result})
        self.signal_history.append(result["signal"])
        print(f"[OC {self.symbol}] {result['signal']} "
              f"Score={result['flow_score']} PCR={result['pcr']} "
              f"Vol_w={result['volume_weight']:.0%} OI_w={result['oi_weight']:.0%} "
              f"| {result['weight_reason']}")

        if self.on_update:
            try:
                self.on_update(result, df)
            except Exception as e:
                print(f"[OC {self.symbol}] on_update error: {e}")

    # ================================================================
    # UPSTOX FETCH
    # ================================================================

    def fetch_option_chain(self) -> pd.DataFrame:
        try:
            cfg = upstox_client.Configuration()
            cfg.access_token = self.access_token
            client = upstox_client.ApiClient(cfg)
            # SDK differences:
            # - Some versions expose OptionChainApi.get_option_chain_data
            # - Others expose OptionsApi.get_put_call_option_chain
            resp = None
            try:
                if hasattr(upstox_client, "OptionChainApi"):
                    api = upstox_client.OptionChainApi(client)
                    resp = api.get_option_chain_data(self.underlying_key, self.expiry)
                else:
                    if not hasattr(upstox_client, "OptionsApi"):
                        print(f"[OC {self.symbol}] fetch: OptionsApi missing in SDK")
                        return pd.DataFrame()
                    options_api = upstox_client.OptionsApi(client)
                    # signature: get_put_call_option_chain(symbol, expiry)
                    # underlying_key already includes NSE_INDEX|...
                    resp = options_api.get_put_call_option_chain(self.underlying_key, self.expiry)
            except (AttributeError, ApiException) as e:
                print(f"[OC {self.symbol}] fetch: {e}")
                return pd.DataFrame()

            if not resp or not hasattr(resp, "data") or not resp.data:

                return pd.DataFrame()
            chain = None
            if hasattr(resp.data, "option_chain_data"):
                chain = getattr(resp.data, "option_chain_data")
            # OptionsApi response shape varies; try common fields.
            if chain is None and hasattr(resp.data, "put_call_option_chain"):
                chain = getattr(resp.data, "put_call_option_chain")
            if chain is None:
                chain = getattr(resp.data, "__iter__", lambda: [])()

            rows          = []
            underlying_ltp = 0.0
            for item in chain:
                try:
                    strike = float(getattr(item, "strike_price", 0))
                    ce_obj = getattr(item, "call_options", None) or getattr(item, "ce", None)
                    pe_obj = getattr(item, "put_options",  None) or getattr(item, "pe", None)
                    def _s(obj, *attrs):
                        if obj is None: return 0.0
                        for src in [obj,
                                    getattr(obj, "market_data",   None),
                                    getattr(obj, "option_greeks", None)]:
                            if src is None: continue
                            for a in attrs:
                                v = getattr(src, a, None)
                                if v is not None:
                                    try: return float(v)
                                    except: pass
                        return 0.0
                    ul = _s(item, "underlying_spot_price", "last_price", "underlying_ltp")
                    if ul > 0:
                        underlying_ltp = ul
                    rows.append({
                        "strike":         strike,
                        "ce_oi":          _s(ce_obj, "oi", "open_interest"),
                        "ce_volume":      _s(ce_obj, "volume"),
                        "ce_ltp":         _s(ce_obj, "last_price", "ltp", "close"),
                        "pe_oi":          _s(pe_obj, "oi", "open_interest"),
                        "pe_volume":      _s(pe_obj, "volume"),
                        "pe_ltp":         _s(pe_obj, "last_price", "ltp", "close"),
                        "underlying_ltp": underlying_ltp,
                    })
                except Exception:
                    continue
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame(rows).sort_values("strike").reset_index(drop=True)
            df["underlying_ltp"] = df["underlying_ltp"].replace(0, None).ffill().bfill()
            return df
        except Exception as e:
            print(f"[OC {self.symbol}] fetch exception: {e}")
            return pd.DataFrame()

    # ================================================================
    # ANALYZE
    # ================================================================

    def analyze(self, df: pd.DataFrame, prev_df: pd.DataFrame) -> dict:
        underlying_ltp = float(df["underlying_ltp"].dropna().iloc[0])
        atm_strike     = self.get_atm_strike(df, underlying_ltp)

        # Dynamic weights
        weights = _calc_weights(df, underlying_ltp)
        vw, ow  = weights["volume_weight"], weights["oi_weight"]

        pcr, atm_pcr = self.calculate_pcr(df, atm_strike)
        merged       = self.prepare_delta_frame(df, prev_df)

        # ── OI analysis ──────────────────────────────────────────
        oi_change        = self.analyze_oi_change(merged)
        oi_shift         = self.analyze_oi_shift(merged)
        oi_concentration = self.analyze_oi_concentration(merged)
        oi_momentum      = self.oi_momentum(merged)
        oi_score_raw     = self.oi_bias_score(oi_change)

        # ── Volume analysis ──────────────────────────────────────
        vol_change        = self.analyze_volume_change(merged)
        vol_concentration = self.analyze_volume_concentration(df, underlying_ltp)
        vol_momentum      = self.volume_momentum(merged)
        vol_score_raw     = self.volume_bias_score(vol_change, underlying_ltp)

        # ── Dynamic combined score ────────────────────────────────
        combined_oi_vol_score = round(
            oi_score_raw * ow + vol_score_raw * vw)

        # ── Premium flow (uses both vol+OI, internally weighted) ─
        (merged, total_ce_flow, total_pe_flow,
         dominant_side, flow_dominance) = self.calculate_premium_flow(merged, vw, ow)
        premium_score    = self.premium_flow_score(total_ce_flow, total_pe_flow)
        top_ce, top_pe   = self.get_top_flows(merged)
        ce_leader, pe_leader = self.get_flow_leaders(merged)
        ce_center, pe_center = self.premium_concentration(merged)
        self.track_flow_leaders(ce_leader, pe_leader)

        # ── COG ──────────────────────────────────────────────────
        ce_cog, pe_cog   = self.calculate_cog(df, vw, ow)
        self.ce_cog_history.append(ce_cog)
        self.pe_cog_history.append(pe_cog)
        ce_shift_5m,  pe_shift_5m  = self.cog_shift_5m()
        ce_shift_15m, pe_shift_15m = self.cog_shift_15m()
        ce_shift_30m, pe_shift_30m = self.cog_shift_30m()
        cog_score = self.cog_score(ce_shift_5m, ce_shift_15m, pe_shift_5m, pe_shift_15m)
        cog_trend = self.cog_trend(ce_shift_15m, pe_shift_15m)

        # ── Rotation ─────────────────────────────────────────────
        rotation          = self.detect_rotation(top_ce, top_pe)
        self.rotation_history.append(rotation)
        rotation_strength = self.rotation_strength()
        ce_rot_shift      = self.ce_leader_shift()
        pe_rot_shift      = self.pe_leader_shift()
        rotation_score    = self.rotation_score(rotation, ce_rot_shift, pe_rot_shift)
        rotation_signal   = self.rotation_signal(rotation, rotation_strength)
        smart_ce, smart_pe = self.smart_money_target(top_ce, top_pe)

        # ── ATM dominance ─────────────────────────────────────────
        atm_dom = self.atm_dominance(df, underlying_ltp)

        # ── Support / Resistance ──────────────────────────────────
        support, resistance = self.support_resistance(df, vw, ow)
        ss, rs              = self.sr_strength(df, vw, ow)
        sr_score_val        = self.sr_score(underlying_ltp, support, resistance, ss, rs)
        wall_dom            = self.wall_dominance(ss, rs)

        # ── Positions ─────────────────────────────────────────────
        positions = self._classify_positions(merged)

        # ── Max Pain ──────────────────────────────────────────────
        max_pain          = self.calculate_max_pain(df)
        self.max_pain_history.append(max_pain)
        max_pain_shift_v  = self.max_pain_shift()
        max_pain_dist     = self.max_pain_distance(underlying_ltp, max_pain)
        max_pain_bias_v   = self.max_pain_bias(underlying_ltp, max_pain)

        # ── MTF flow ──────────────────────────────────────────────
        trend_5m    = self.flow_trend_5m()
        trend_15m   = self.flow_trend_15m()
        trend_30m   = self.flow_trend_30m()
        mtf_s       = self.mtf_score()
        inst_conf   = self.institutional_confirmation()
        accel       = self.flow_acceleration()

        # ── Final score ───────────────────────────────────────────
        score, reasons = self.calculate_score(
            pcr, atm_pcr,
            combined_oi_vol_score,
            premium_score, cog_score, rotation_score,
            sr_score_val, mtf_s,
            max_pain_bias_v, atm_dom, positions,
            weights)
        signal            = self.signal_from_score(score)
        conf_val          = self.confidence(score)
        tq                = self.trend_quality(score)
        fs                = self.flow_strength(total_ce_flow, total_pe_flow)
        ir                = self.institutional_rating(fs, conf_val)

        return {
            "timestamp":              _now_str(),
            "signal":                 signal,
            "flow_score":             score,
            "confidence":             conf_val,
            "trend_quality":          tq,
            "institutional_rating":   ir,
            "underlying_ltp":         underlying_ltp,
            # Dynamic weights
            "volume_weight":          round(vw, 2),
            "oi_weight":              round(ow, 2),
            "weight_reason":          weights["reason"],
            "vol_near_spot":          weights["vol_near_spot"],
            "oi_near_spot":           weights["oi_near_spot"],
            "both_same_strike":       weights["both_same"],
            "ce_vol_top_strike":      weights["ce_vol_top"],
            "ce_oi_top_strike":       weights["ce_oi_top"],
            "pe_vol_top_strike":      weights["pe_vol_top"],
            "pe_oi_top_strike":       weights["pe_oi_top"],
            # Separate raw scores
            "oi_score_raw":           oi_score_raw,
            "vol_score_raw":          vol_score_raw,
            "combined_oi_vol_score":  combined_oi_vol_score,
            # Outputs
            "pcr":                    round(pcr, 3),
            "atm_pcr":                round(atm_pcr, 3),
            "max_pain":               max_pain,
            "max_pain_shift":         max_pain_shift_v,
            "max_pain_distance":      max_pain_dist,
            "support":                support,
            "resistance":             resistance,
            "support_strength":       ss,
            "resistance_strength":    rs,
            "wall_dominance":         wall_dom,
            "ce_flow_total":          total_ce_flow,
            "pe_flow_total":          total_pe_flow,
            "dominant_side":          dominant_side,
            "flow_dominance":         round(flow_dominance, 3),
            "ce_cog":                 ce_cog,
            "pe_cog":                 pe_cog,
            "ce_shift_5m":            ce_shift_5m,
            "ce_shift_15m":           ce_shift_15m,
            "ce_shift_30m":           ce_shift_30m,
            "pe_shift_5m":            pe_shift_5m,
            "pe_shift_15m":           pe_shift_15m,
            "pe_shift_30m":           pe_shift_30m,
            "cog_trend":              cog_trend,
            "rotation":               rotation,
            "rotation_signal":        rotation_signal,
            "ce_rotation_shift":      ce_rot_shift,
            "pe_rotation_shift":      pe_rot_shift,
            "smart_target_ce":        smart_ce,
            "smart_target_pe":        smart_pe,
            "atm_dominance":          atm_dom,
            "oi_change":              oi_change,
            "oi_shift":               oi_shift,
            "oi_concentration":       oi_concentration,
            "oi_momentum":            oi_momentum,
            "vol_change":             vol_change,
            "vol_concentration":      vol_concentration,
            "vol_momentum":           vol_momentum,
            "long_buildup":           positions["long_buildup"],
            "short_buildup":          positions["short_buildup"],
            "short_covering":         positions["short_covering"],
            "long_unwinding":         positions["long_unwinding"],
            "trend_5m":               trend_5m,
            "trend_15m":              trend_15m,
            "trend_30m":              trend_30m,
            "flow_acceleration":      accel,
            "institutional_confirmation": inst_conf,
            "top_ce_flows":           top_ce,
            "top_pe_flows":           top_pe,
            "reasons":                reasons,
        }

    # ================================================================
    # PUBLIC API  (engine v6 uses these)
    # ================================================================

    def get_analysis(self) -> Optional[dict]:
        with self.lock:
            return self.latest_analysis

    def get_signal(self) -> Optional[str]:
        """BULLISH variants → CE. BEARISH variants → PE. NEUTRAL → no filter."""
        with self.lock:
            if not self.latest_analysis:
                return None
            return self.latest_analysis["signal"]

    def is_bullish(self) -> Optional[bool]:
        """True=CE, False=PE, None=no OC filter."""
        sig = self.get_signal()
        if sig is None:      return None
        if "BULLISH" in sig: return True
        if "BEARISH" in sig: return False
        return None

    def get_history(self) -> list:
        return list(self.history)

    # ================================================================
    # ATM + PCR
    # ================================================================

    def get_atm_strike(self, df: pd.DataFrame, spot: float) -> float:
        return float(df.loc[df["strike"].sub(spot).abs().idxmin(), "strike"])

    def calculate_pcr(self, df: pd.DataFrame, atm_strike: float):
        total_ce = float(df["ce_oi"].sum())
        total_pe = float(df["pe_oi"].sum())
        pcr      = total_pe / max(total_ce, 1)
        atm_df   = df[df["strike"].between(atm_strike - ATM_WINDOW_POINTS,
                                           atm_strike + ATM_WINDOW_POINTS)]
        atm_pcr  = float(atm_df["pe_oi"].sum()) / max(float(atm_df["ce_oi"].sum()), 1)
        return round(pcr, 3), round(atm_pcr, 3)

    # ================================================================
    # DELTA FRAME
    # ================================================================

    def prepare_delta_frame(self, df: pd.DataFrame, prev_df: pd.DataFrame) -> pd.DataFrame:
        merged = df.merge(prev_df, on="strike", suffixes=("", "_prev"), how="left")
        for col in ["ce_volume_prev","pe_volume_prev","ce_oi_prev",
                    "pe_oi_prev","ce_ltp_prev","pe_ltp_prev"]:
            if col in merged.columns:
                merged[col] = merged[col].fillna(0)
        merged["ce_vol_delta"]   = merged["ce_volume"] - merged.get("ce_volume_prev", merged["ce_volume"])
        merged["pe_vol_delta"]   = merged["pe_volume"] - merged.get("pe_volume_prev", merged["pe_volume"])
        merged["ce_oi_delta"]    = merged["ce_oi"]     - merged.get("ce_oi_prev", merged["ce_oi"])
        merged["pe_oi_delta"]    = merged["pe_oi"]     - merged.get("pe_oi_prev", merged["pe_oi"])
        merged["ce_price_delta"] = merged["ce_ltp"]    - merged.get("ce_ltp_prev", merged["ce_ltp"])
        merged["pe_price_delta"] = merged["pe_ltp"]    - merged.get("pe_ltp_prev", merged["pe_ltp"])
        return merged

    # ================================================================
    # OI ANALYSIS
    # ================================================================

    def analyze_oi_change(self, merged: pd.DataFrame) -> dict:
        ce_add  = float(merged.loc[merged["ce_oi_delta"]>0,"ce_oi_delta"].sum())
        ce_exit = abs(float(merged.loc[merged["ce_oi_delta"]<0,"ce_oi_delta"].sum()))
        pe_add  = float(merged.loc[merged["pe_oi_delta"]>0,"pe_oi_delta"].sum())
        pe_exit = abs(float(merged.loc[merged["pe_oi_delta"]<0,"pe_oi_delta"].sum()))
        return {
            "ce_oi_add": ce_add, "ce_oi_exit": ce_exit,
            "pe_oi_add": pe_add, "pe_oi_exit": pe_exit,
            "net_ce_oi": ce_add-ce_exit, "net_pe_oi": pe_add-pe_exit,
        }

    def analyze_oi_shift(self, merged: pd.DataFrame) -> dict:
        return {
            "ce_top_add":  merged.nlargest(10,"ce_oi_delta")[["strike","ce_oi_delta"]].to_dict("records"),
            "pe_top_add":  merged.nlargest(10,"pe_oi_delta")[["strike","pe_oi_delta"]].to_dict("records"),
            "ce_top_exit": merged.nsmallest(10,"ce_oi_delta")[["strike","ce_oi_delta"]].to_dict("records"),
            "pe_top_exit": merged.nsmallest(10,"pe_oi_delta")[["strike","pe_oi_delta"]].to_dict("records"),
        }

    def analyze_oi_concentration(self, merged: pd.DataFrame):
        ce_c = ((merged["strike"]*merged["ce_oi_delta"].clip(lower=0)).sum()
                / max(merged["ce_oi_delta"].clip(lower=0).sum(), 1))
        pe_c = ((merged["strike"]*merged["pe_oi_delta"].clip(lower=0)).sum()
                / max(merged["pe_oi_delta"].clip(lower=0).sum(), 1))
        return ce_c, pe_c

    def oi_bias_score(self, oi_stats: dict) -> int:
        score = 0
        nc, np_ = oi_stats["net_ce_oi"], oi_stats["net_pe_oi"]
        if nc > np_ * 1.5:  score += 10
        elif np_ > nc * 1.5: score -= 10
        return score

    def oi_momentum(self, merged: pd.DataFrame) -> dict:
        ce = float(merged["ce_oi_delta"].clip(lower=0).sum())
        pe = float(merged["pe_oi_delta"].clip(lower=0).sum())
        return {"ce_momentum": ce, "pe_momentum": pe,
                "leader": "CE" if ce>pe else ("PE" if pe>ce else "BALANCED")}

    # ================================================================
    # VOLUME ANALYSIS  (new — mirrors OI analysis)
    # ================================================================

    def analyze_volume_change(self, merged: pd.DataFrame) -> dict:
        """
        Volume delta analysis — mirrors OI analysis.
        Positive vol_delta = new volume came in this snapshot.
        """
        ce_add  = float(merged.loc[merged["ce_vol_delta"]>0,"ce_vol_delta"].sum())
        ce_exit = abs(float(merged.loc[merged["ce_vol_delta"]<0,"ce_vol_delta"].sum()))
        pe_add  = float(merged.loc[merged["pe_vol_delta"]>0,"pe_vol_delta"].sum())
        pe_exit = abs(float(merged.loc[merged["pe_vol_delta"]<0,"pe_vol_delta"].sum()))
        return {
            "ce_vol_add":  ce_add,  "ce_vol_exit": ce_exit,
            "pe_vol_add":  pe_add,  "pe_vol_exit": pe_exit,
            "net_ce_vol":  ce_add - ce_exit,
            "net_pe_vol":  pe_add - pe_exit,
        }

    def analyze_volume_concentration(self, df: pd.DataFrame,
                                      underlying_ltp: float) -> dict:
        """
        Measure how concentrated volume is near the spot price.
        Returns % of total CE/PE volume within ATM_WINDOW_POINTS.
        """
        atm_df = df[df["strike"].between(underlying_ltp - ATM_WINDOW_POINTS,
                                         underlying_ltp + ATM_WINDOW_POINTS)]
        total_ce_vol = max(float(df["ce_volume"].sum()), 1)
        total_pe_vol = max(float(df["pe_volume"].sum()), 1)
        atm_ce_pct   = round(float(atm_df["ce_volume"].sum()) / total_ce_vol * 100, 2)
        atm_pe_pct   = round(float(atm_df["pe_volume"].sum()) / total_pe_vol * 100, 2)
        return {
            "atm_ce_vol_pct": atm_ce_pct,
            "atm_pe_vol_pct": atm_pe_pct,
            "ce_vol_top_strike": float(df.loc[df["ce_volume"].idxmax(),"strike"]),
            "pe_vol_top_strike": float(df.loc[df["pe_volume"].idxmax(),"strike"]),
            "ce_vol_max":        float(df["ce_volume"].max()),
            "pe_vol_max":        float(df["pe_volume"].max()),
        }

    def volume_momentum(self, merged: pd.DataFrame) -> dict:
        """Which side is absorbing more new volume this snapshot."""
        ce = float(merged["ce_vol_delta"].clip(lower=0).sum())
        pe = float(merged["pe_vol_delta"].clip(lower=0).sum())
        return {"ce_vol_momentum": ce, "pe_vol_momentum": pe,
                "leader": "CE" if ce>pe else ("PE" if pe>ce else "BALANCED")}

    def volume_bias_score(self, vol_stats: dict, underlying_ltp: float) -> int:
        """
        Score from volume analysis — mirrors oi_bias_score.
        Gives bonus if volume momentum sides agree with each other.
        """
        score = 0
        nc = vol_stats["net_ce_vol"]
        np_ = vol_stats["net_pe_vol"]
        if nc > np_ * 1.5:  score += 10
        elif np_ > nc * 1.5: score -= 10
        # Additional: if CE add dominates and CE volume came near spot → stronger signal
        if nc > 0 and np_ > 0:
            ratio = nc / max(np_, 1)
            if ratio > 2.0:   score += 5
            elif ratio < 0.5: score -= 5
        return score

    def volume_support_resistance(self, df: pd.DataFrame, vw: float, ow: float):
        """
        Dynamic support/resistance using weighted average of OI-max and Volume-max strikes.
        """
        oi_support    = float(df.loc[df["pe_oi"].idxmax(),     "strike"])
        oi_resistance = float(df.loc[df["ce_oi"].idxmax(),     "strike"])
        vol_support   = float(df.loc[df["pe_volume"].idxmax(), "strike"])
        vol_resistance = float(df.loc[df["ce_volume"].idxmax(),"strike"])
        # Weighted average
        support    = round(oi_support    * ow + vol_support    * vw)
        resistance = round(oi_resistance * ow + vol_resistance * vw)
        return float(support), float(resistance)

    def volume_sr_strength(self, df: pd.DataFrame, vw: float, ow: float):
        """Strength = weighted % of OI + Volume at max-strike vs total."""
        # OI strength
        oi_ss = df["pe_oi"].max() / max(df["pe_oi"].sum(), 1) * 100
        oi_rs = df["ce_oi"].max() / max(df["ce_oi"].sum(), 1) * 100
        # Volume strength
        vol_ss = df["pe_volume"].max() / max(df["pe_volume"].sum(), 1) * 100
        vol_rs = df["ce_volume"].max() / max(df["ce_volume"].sum(), 1) * 100
        # Weighted
        ss = round(oi_ss * ow + vol_ss * vw, 2)
        rs = round(oi_rs * ow + vol_rs * vw, 2)
        return ss, rs

    # ================================================================
    # PREMIUM FLOW  (volume+OI weighted internally)
    # ================================================================

    def calculate_premium_flow(self, merged: pd.DataFrame,
                                vw: float, ow: float):
        """
        Premium flow = weighted sum of (volume × price) and (OI × price).
        vw/ow control the mix dynamically.
        """
        merged["ce_flow"] = (
            merged["ce_vol_delta"].clip(lower=0) * merged["ce_ltp"] * vw +
            merged["ce_oi_delta"].clip(lower=0)  * merged["ce_ltp"] * ow)
        merged["pe_flow"] = (
            merged["pe_vol_delta"].clip(lower=0) * merged["pe_ltp"] * vw +
            merged["pe_oi_delta"].clip(lower=0)  * merged["pe_ltp"] * ow)
        total_ce   = float(merged["ce_flow"].sum())
        total_pe   = float(merged["pe_flow"].sum())
        net_flow   = total_ce - total_pe
        total_flow = total_ce + total_pe
        flow_dom   = abs(net_flow) / max(total_flow, 1)
        dominant   = "CE" if total_ce > total_pe else "PE"
        return merged, total_ce, total_pe, dominant, flow_dom

    def get_top_flows(self, merged: pd.DataFrame):
        ce = merged.nlargest(TOP_FLOW_COUNT,"ce_flow")[
            ["strike","ce_flow","ce_vol_delta","ce_oi_delta"]].to_dict("records")
        pe = merged.nlargest(TOP_FLOW_COUNT,"pe_flow")[
            ["strike","pe_flow","pe_vol_delta","pe_oi_delta"]].to_dict("records")
        return ce, pe

    def get_flow_leaders(self, merged: pd.DataFrame):
        return (float(merged.loc[merged["ce_flow"].idxmax(),"strike"]),
                float(merged.loc[merged["pe_flow"].idxmax(),"strike"]))

    def premium_concentration(self, merged: pd.DataFrame):
        ce = (merged["strike"]*merged["ce_flow"]).sum() / max(merged["ce_flow"].sum(), 1)
        pe = (merged["strike"]*merged["pe_flow"]).sum() / max(merged["pe_flow"].sum(), 1)
        return ce, pe

    def premium_flow_score(self, ce_flow: float, pe_flow: float) -> int:
        ratio = ce_flow / max(pe_flow, 1)
        if ratio > 3:    return 30
        if ratio > 2:    return 20
        if ratio > 1.5:  return 10
        if ratio < 0.33: return -30
        if ratio < 0.50: return -20
        if ratio < 0.70: return -10
        return 0

    def track_flow_leaders(self, ce: float, pe: float):
        self.flow_5m.append((ce, pe))

    # ================================================================
    # COG  (weighted OI+Volume)
    # ================================================================

    def calculate_cog(self, df: pd.DataFrame, vw: float, ow: float):
        """Center of Gravity using weighted (OI×price + Volume×price)."""
        ce_w = df["ce_oi"] * df["ce_ltp"] * ow + df["ce_volume"] * df["ce_ltp"] * vw
        pe_w = df["pe_oi"] * df["pe_ltp"] * ow + df["pe_volume"] * df["pe_ltp"] * vw
        ce_cog = (df["strike"]*ce_w).sum() / max(ce_w.sum(), 1)
        pe_cog = (df["strike"]*pe_w).sum() / max(pe_w.sum(), 1)
        return round(ce_cog, 2), round(pe_cog, 2)

    def cog_shift_5m(self):
        if len(self.ce_cog_history) < COG_5M_BARS:  return 0, 0
        return (round(self.ce_cog_history[-1]-self.ce_cog_history[-COG_5M_BARS],  2),
                round(self.pe_cog_history[-1]-self.pe_cog_history[-COG_5M_BARS],  2))

    def cog_shift_15m(self):
        if len(self.ce_cog_history) < COG_15M_BARS: return 0, 0
        return (round(self.ce_cog_history[-1]-self.ce_cog_history[-COG_15M_BARS], 2),
                round(self.pe_cog_history[-1]-self.pe_cog_history[-COG_15M_BARS], 2))

    def cog_shift_30m(self):
        if len(self.ce_cog_history) < COG_30M_BARS: return 0, 0
        return (round(self.ce_cog_history[-1]-self.ce_cog_history[-COG_30M_BARS], 2),
                round(self.pe_cog_history[-1]-self.pe_cog_history[-COG_30M_BARS], 2))

    def cog_trend(self, ce: float, pe: float) -> str:
        if ce >  100 and pe >  100: return "STRONG_UP"
        if ce >   50 and pe >   50: return "UP"
        if ce < -100 and pe < -100: return "STRONG_DOWN"
        if ce <  -50 and pe <  -50: return "DOWN"
        return "SIDEWAYS"

    def cog_score(self, ce5: float, ce15: float, pe5: float, pe15: float) -> int:
        s = 0
        if ce5  >  50:  s += 5
        elif ce5  < -50:  s -= 5
        if ce15 >  100: s += 10
        elif ce15 < -100: s -= 10
        if pe5  < -50:  s += 5
        elif pe5  >  50:  s -= 5
        if pe15 < -100: s += 10
        elif pe15 >  100: s -= 10
        return s

    # ================================================================
    # ROTATION
    # ================================================================

    def detect_rotation(self, top_ce: list, top_pe: list) -> str:
        if not top_ce or not top_pe: return "SIDEWAYS"
        ce_avg = sum(x["strike"] for x in top_ce) / len(top_ce)
        pe_avg = sum(x["strike"] for x in top_pe) / len(top_pe)
        if ce_avg > pe_avg + 100: return "UP"
        if pe_avg > ce_avg + 100: return "DOWN"
        return "SIDEWAYS"

    def rotation_strength(self, lookback: int = 10) -> int:
        if len(self.rotation_history) < lookback: return 0
        r = list(self.rotation_history)[-lookback:]
        if r.count("UP")   >= lookback*0.7: return 1
        if r.count("DOWN") >= lookback*0.7: return -1
        return 0

    def ce_leader_shift(self, bars: int = 10) -> float:
        if len(self.flow_5m) < bars: return 0
        return self.flow_5m[-1][0] - self.flow_5m[-bars][0]

    def pe_leader_shift(self, bars: int = 10) -> float:
        if len(self.flow_5m) < bars: return 0
        return self.flow_5m[-1][1] - self.flow_5m[-bars][1]

    def rotation_score(self, rotation: str, ce_shift: float, pe_shift: float) -> int:
        s = 0
        if rotation == "UP":   s += 10
        elif rotation == "DOWN": s -= 10
        if ce_shift >  100: s += 10
        elif ce_shift < -100: s -= 10
        if pe_shift < -100: s += 10
        elif pe_shift >  100: s -= 10
        return s

    def rotation_signal(self, rotation: str, strength: int) -> str:
        if rotation == "UP"   and strength > 0: return "BULLISH"
        if rotation == "DOWN" and strength < 0: return "BEARISH"
        return "NEUTRAL"

    def smart_money_target(self, top_ce: list, top_pe: list):
        ce = max(top_ce, key=lambda x: x["ce_flow"])["strike"] if top_ce else None
        pe = max(top_pe, key=lambda x: x["pe_flow"])["strike"] if top_pe else None
        return ce, pe

    # ================================================================
    # ATM DOMINANCE  (weighted volume + OI)
    # ================================================================

    def atm_dominance(self, df: pd.DataFrame, underlying_ltp: float) -> str:
        atm_idx = df["strike"].sub(underlying_ltp).abs().idxmin()
        row     = df.loc[atm_idx]
        # Include both volume and OI contribution
        ce_val  = row["ce_volume"] * row["ce_ltp"] + row["ce_oi"] * row["ce_ltp"] * 0.3
        pe_val  = row["pe_volume"] * row["pe_ltp"] + row["pe_oi"] * row["pe_ltp"] * 0.3
        ratio   = ce_val / max(pe_val, 1)
        if ratio > 1.5:  return "CALL_DOMINANT"
        if ratio < 0.67: return "PUT_DOMINANT"
        return "BALANCED"

    # ================================================================
    # SUPPORT / RESISTANCE  (weighted)
    # ================================================================

    def support_resistance(self, df: pd.DataFrame, vw: float = 0.30, ow: float = 0.70):
        return self.volume_support_resistance(df, vw, ow)

    def sr_strength(self, df: pd.DataFrame, vw: float = 0.30, ow: float = 0.70):
        return self.volume_sr_strength(df, vw, ow)

    def sr_score(self, spot: float, sup: float, res: float, ss: float, rs: float) -> int:
        s = 0
        if spot > res: s += 15
        elif spot < sup: s -= 15
        if ss > rs:  s += 5
        elif rs > ss: s -= 5
        return s

    def wall_dominance(self, ss: float, rs: float) -> str:
        if ss > rs * 1.25: return "PUT_WALL_DOMINANT"
        if rs > ss * 1.25: return "CALL_WALL_DOMINANT"
        return "BALANCED"

    # ================================================================
    # POSITIONS
    # ================================================================

    def _classify_positions(self, merged: pd.DataFrame) -> dict:
        lb = float(merged[(merged["ce_oi_delta"]>0)&(merged["ce_price_delta"]>0)]["ce_oi_delta"].sum())
        sb = float(merged[(merged["ce_oi_delta"]>0)&(merged["ce_price_delta"]<0)]["ce_oi_delta"].sum())
        sc = float(merged[(merged["ce_oi_delta"]<0)&(merged["ce_price_delta"]>0)]["ce_oi_delta"].abs().sum())
        lu = float(merged[(merged["ce_oi_delta"]<0)&(merged["ce_price_delta"]<0)]["ce_oi_delta"].abs().sum())
        return {"long_buildup": lb, "short_buildup": sb,
                "short_covering": sc, "long_unwinding": lu}

    # ================================================================
    # MAX PAIN
    # ================================================================

    def calculate_max_pain(self, df: pd.DataFrame) -> float:
        strikes  = sorted(df["strike"].unique())
        pain_map = {}
        for s in strikes:
            call_pain = (df[df["strike"]<s]["ce_oi"] *
                         (s - df[df["strike"]<s]["strike"])).sum()
            put_pain  = (df[df["strike"]>s]["pe_oi"] *
                         (df[df["strike"]>s]["strike"] - s)).sum()
            pain_map[s] = call_pain + put_pain
        return float(min(pain_map, key=pain_map.get))

    def max_pain_shift(self, bars: int = 10) -> float:
        if len(self.max_pain_history) < bars: return 0
        return self.max_pain_history[-1] - self.max_pain_history[-bars]

    def max_pain_distance(self, spot: float, mp: float) -> float:
        return round((mp - spot) / max(spot, 1) * 100, 2)

    def max_pain_bias(self, spot: float, mp: float) -> str:
        diff = mp - spot
        if diff >  100: return "BULLISH"
        if diff < -100: return "BEARISH"
        return "NEUTRAL"

    # ================================================================
    # MTF FLOW
    # ================================================================

    def flow_change(self, bars: int) -> Optional[dict]:
        if len(self.history) < bars: return None
        cur = self.history[-1].get("analysis")
        old = self.history[-bars].get("analysis")
        if not cur or not old: return None
        return {
            "ce_flow_change": cur["ce_flow_total"] - old["ce_flow_total"],
            "pe_flow_change": cur["pe_flow_total"] - old["pe_flow_total"],
            "score_change":   cur["flow_score"]    - old["flow_score"],
        }

    def _flow_trend(self, bars: int) -> str:
        d = self.flow_change(bars)
        if not d: return "UNKNOWN"
        if d["ce_flow_change"] > d["pe_flow_change"]: return "BULLISH"
        if d["ce_flow_change"] < d["pe_flow_change"]: return "BEARISH"
        return "NEUTRAL"

    def flow_trend_5m(self)  -> str: return self._flow_trend(10)
    def flow_trend_15m(self) -> str: return self._flow_trend(30)
    def flow_trend_30m(self) -> str: return self._flow_trend(60)

    def flow_acceleration(self) -> float:
        f5, f15 = self.flow_change(10), self.flow_change(30)
        if not f5 or not f15: return 0
        return f5["score_change"] - f15["score_change"]

    def institutional_confirmation(self) -> str:
        t5, t15, t30 = self.flow_trend_5m(), self.flow_trend_15m(), self.flow_trend_30m()
        if t5 == t15 == t30 == "BULLISH": return "STRONG_BULLISH"
        if t5 == t15 == t30 == "BEARISH": return "STRONG_BEARISH"
        return "MIXED"

    def mtf_score(self) -> int:
        s = 0
        for val, pts in [(self.flow_trend_5m(),5),(self.flow_trend_15m(),10),(self.flow_trend_30m(),15)]:
            if val == "BULLISH": s += pts
            elif val == "BEARISH": s -= pts
        return s

    # ================================================================
    # FINAL SCORE
    # ================================================================

    def calculate_score(self, pcr, atm_pcr, combined_oi_vol_score,
                         premium_score, cog_score, rotation_score,
                         sr_score_v, mtf_score_v, max_pain_bias,
                         atm_dom, positions, weights) -> tuple:
        score, reasons = 0, []

        # PCR
        if pcr > 1.30:   score += 12; reasons.append("PCR bullish")
        elif pcr > 1.10: score += 6;  reasons.append("PCR mildly bullish")
        elif pcr < 0.70: score -= 12; reasons.append("PCR bearish")
        elif pcr < 0.90: score -= 6;  reasons.append("PCR mildly bearish")

        # ATM PCR
        if atm_pcr > 1.20:   score += 8; reasons.append("ATM PCR bullish")
        elif atm_pcr < 0.80: score -= 8; reasons.append("ATM PCR bearish")

        # Dynamic OI + Volume combined
        score += combined_oi_vol_score
        if weights["vol_near_spot"]:
            reasons.append(f"Volume near spot (w={weights['volume_weight']:.0%})")
        if weights["both_same"]:
            reasons.append("Vol+OI max on same strike — strong signal")

        score += premium_score + cog_score + rotation_score + sr_score_v + mtf_score_v

        if max_pain_bias == "BULLISH":  score += 5; reasons.append("Max pain above spot")
        elif max_pain_bias == "BEARISH": score -= 5; reasons.append("Max pain below spot")

        if atm_dom == "CALL_DOMINANT":   score += 5; reasons.append("ATM Call dominant")
        elif atm_dom == "PUT_DOMINANT":  score -= 5; reasons.append("ATM Put dominant")

        lb, sb, sc, lu = (positions["long_buildup"], positions["short_buildup"],
                           positions["short_covering"], positions["long_unwinding"])
        if lb > sb * 1.5: score += 8; reasons.append("Long buildup dominant")
        if sc > lu * 1.5: score += 5; reasons.append("Short covering dominant")
        if sb > lb * 1.5: score -= 8; reasons.append("Short buildup dominant")
        if lu > sc * 1.5: score -= 5; reasons.append("Long unwinding dominant")

        return max(min(score, 100), -100), reasons

    # ================================================================
    # SIGNAL / RATINGS
    # ================================================================

    def signal_from_score(self, score: int) -> str:
        if score >= 80:  return "VERY_STRONG_BULLISH"
        if score >= 50:  return "STRONG_BULLISH"
        if score >= 20:  return "BULLISH"
        if score <= -80: return "VERY_STRONG_BEARISH"
        if score <= -50: return "STRONG_BEARISH"
        if score <= -20: return "BEARISH"
        return "NEUTRAL"

    def confidence(self, score: int) -> int:
        return min(100, abs(score))

    def trend_quality(self, score: int) -> str:
        s = abs(score)
        if s >= 80: return "A+"
        if s >= 60: return "A"
        if s >= 40: return "B"
        if s >= 20: return "C"
        return "D"

    def flow_strength(self, ce: float, pe: float) -> float:
        total = ce + pe
        if total <= 0: return 0
        return round(abs(ce - pe) / total * 100, 2)

    def institutional_rating(self, fs: float, conf: int) -> str:
        s = (fs + conf) / 2
        if s >= 80: return "VERY_HIGH"
        if s >= 60: return "HIGH"
        if s >= 40: return "MEDIUM"
        return "LOW"
