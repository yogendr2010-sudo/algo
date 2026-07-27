# backend/services/upstox_oauth.py
# ================================================================
# Upstox OAuth2 (authorization code) flow.
#
# One-time setup (per user): user creates an app at
# https://account.upstox.com/developer/apps, gets an API Key
# (client_id) + API Secret (client_secret), and sets the Redirect
# URI to settings.UPSTOX_REDIRECT_URI (same URL for every user —
# the user is identified via the signed `state` param).
#
# Daily refresh (one click): Upstox access tokens are valid only
# until ~3:30 AM IST the next day — there is no refresh-token grant,
# so the user must re-authenticate via Upstox's login dialog once
# per day. This module makes that a single button click:
#
#   1. GET /api/users/upstox/login-url  -> returns Upstox's
#      authorization dialog URL (with this user's client_id +
#      a signed `state` containing their user_id)
#   2. Browser navigates there, user logs into Upstox (handles
#      password/TOTP themselves — Upstox does not allow us to
#      automate this step)
#   3. Upstox redirects to UPSTOX_REDIRECT_URI?code=...&state=...
#   4. GET /api/users/upstox/callback exchanges `code` for an
#      access_token using this user's stored client_id/secret,
#      computes the next 3:30 AM IST expiry, and stores both
#      encrypted in the User row.
# ================================================================

from datetime import datetime, timedelta, time as dt_time
from zoneinfo import ZoneInfo
from typing import Optional

import httpx
from jose import jwt, JWTError

from backend.config import get_settings

settings = get_settings()

AUTH_DIALOG_URL = "https://api.upstox.com/v2/login/authorization/dialog"
TOKEN_URL       = "https://api.upstox.com/v2/login/authorization/token"

STATE_TYPE       = "upstox_oauth_state"
STATE_EXPIRE_MIN = 10   # state token short-lived — just covers the login redirect

IST = ZoneInfo("Asia/Kolkata")


# ================================================================
# STATE TOKEN  (identifies the user across the Upstox redirect)
# ================================================================

def make_state(user_id: int) -> str:
    payload = {
        "sub":  str(user_id),
        "type": STATE_TYPE,
        "exp":  datetime.utcnow() + timedelta(minutes=STATE_EXPIRE_MIN),
    }
    return jwt.encode(payload, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def decode_state(state: str) -> Optional[int]:
    try:
        payload = jwt.decode(state, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        if payload.get("type") != STATE_TYPE:
            return None
        return int(payload["sub"])
    except (JWTError, KeyError, ValueError, TypeError):
        return None


# ================================================================
# LOGIN URL
# ================================================================

def build_login_url(user_id: int, api_key: str) -> str:
    state = make_state(user_id)
    return (
        f"{AUTH_DIALOG_URL}"
        f"?response_type=code"
        f"&client_id={api_key}"
        f"&redirect_uri={settings.UPSTOX_REDIRECT_URI}"
        f"&state={state}"
    )


# ================================================================
# CODE -> ACCESS TOKEN EXCHANGE
# ================================================================

async def exchange_code_for_token(code: str, api_key: str, api_secret: str) -> dict:
    """
    POSTs to Upstox's token endpoint. Returns the raw JSON response
    on success (contains "access_token"), or raises httpx.HTTPStatusError
    on failure — caller should catch and surface a friendly message.
    """
    data = {
        "code":          code,
        "client_id":     api_key,
        "client_secret": api_secret,
        "redirect_uri":  settings.UPSTOX_REDIRECT_URI,
        "grant_type":    "authorization_code",
    }
    headers = {"accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(TOKEN_URL, data=data, headers=headers)
        resp.raise_for_status()
        return resp.json()


# ================================================================
# TOKEN EXPIRY
# ================================================================

def next_token_expiry(now: Optional[datetime] = None) -> datetime:
    """
    Upstox access tokens are valid until ~3:30 AM IST the NEXT day,
    regardless of generation time. Returns that expiry as a naive
    UTC datetime (matches the DateTime column type used elsewhere
    in this project).
    """
    now_ist = (now or datetime.utcnow()).replace(tzinfo=ZoneInfo("UTC")).astimezone(IST)
    expiry_ist = datetime.combine(now_ist.date() + timedelta(days=1), dt_time(3, 30), tzinfo=IST)
    return expiry_ist.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)


def is_token_valid(expires_at: Optional[datetime]) -> bool:
    if not expires_at:
        return False
    return datetime.utcnow() < expires_at
