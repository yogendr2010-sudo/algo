# backend/services/google_oauth.py
# ================================================================
# Google OAuth 2.0 (Authorization Code flow).
#
# Setup (one-time, per deployment):
#   1. Go to console.cloud.google.com/apis/credentials
#   2. Create → OAuth 2.0 Client ID (type: Web application)
#   3. Add Authorised redirect URI:
#        {APP_BASE_URL}/api/auth/google/callback
#   4. Copy Client ID + Client Secret into .env:
#        GOOGLE_CLIENT_ID=...
#        GOOGLE_CLIENT_SECRET=...
#        APP_BASE_URL=https://yourdomain.com
#
# Flow:
#   GET  /api/auth/google/login           → redirects to Google
#   GET  /api/auth/google/callback?code=. → exchanges code for ID token
#                                           → upserts User → issues JWT
# ================================================================

from datetime import datetime, timedelta
from typing import Optional

import httpx
from jose import jwt, JWTError

from backend.config import get_settings

settings = get_settings()

GOOGLE_AUTH_URL  = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_CERTS_URL = "https://www.googleapis.com/oauth2/v3/certs"
GOOGLE_USERINFO  = "https://www.googleapis.com/oauth2/v3/userinfo"

STATE_TYPE       = "google_oauth_state"
STATE_EXPIRE_MIN = 10


# ── State token ─────────────────────────────────────────────────

def make_state(nonce: str = "") -> str:
    from backend.services.auth_service import create_access_token as _tok
    from jose import jwt as _jwt
    payload = {
        "nonce": nonce,
        "type":  STATE_TYPE,
        "exp":   datetime.utcnow() + timedelta(minutes=STATE_EXPIRE_MIN),
    }
    return jwt.encode(payload, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def verify_state(state: str) -> bool:
    try:
        payload = jwt.decode(state, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        return payload.get("type") == STATE_TYPE
    except JWTError:
        return False


# ── Login URL ───────────────────────────────────────────────────

def build_login_url() -> str:
    import urllib.parse
    state  = make_state()
    params = {
        "client_id":     settings.GOOGLE_CLIENT_ID,
        "redirect_uri":  settings.google_redirect_uri,
        "response_type": "code",
        "scope":         "openid email profile",
        "state":         state,
        "access_type":   "online",
        "prompt":        "select_account",   # always show account picker
    }
    return f"{GOOGLE_AUTH_URL}?{urllib.parse.urlencode(params)}"


# ── Code → tokens ───────────────────────────────────────────────

async def exchange_code(code: str) -> dict:
    """
    Exchanges the authorization code for an ID token + access token.
    Returns the raw token response dict.
    Raises httpx.HTTPStatusError on failure.
    """
    data = {
        "code":          code,
        "client_id":     settings.GOOGLE_CLIENT_ID,
        "client_secret": settings.GOOGLE_CLIENT_SECRET,
        "redirect_uri":  settings.google_redirect_uri,
        "grant_type":    "authorization_code",
    }
    async with httpx.AsyncClient(timeout=15) as c:
        resp = await c.post(GOOGLE_TOKEN_URL, data=data)
        resp.raise_for_status()
        return resp.json()


# ── ID token → user info ─────────────────────────────────────────

async def get_user_info(access_token: str) -> dict:
    """
    Fetches user profile from Google's userinfo endpoint.
    Returns dict with: sub, email, name, picture, email_verified.
    """
    async with httpx.AsyncClient(timeout=10) as c:
        resp = await c.get(GOOGLE_USERINFO,
                           headers={"Authorization": f"Bearer {access_token}"})
        resp.raise_for_status()
        return resp.json()
