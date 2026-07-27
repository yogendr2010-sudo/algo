# backend/config.py
import os
from functools import lru_cache
from dotenv import load_dotenv

load_dotenv()  # reads .env file automatically


class Settings:
    SECRET_KEY: str                   = os.getenv("SECRET_KEY", "dev-secret-change-me-32chars-min!")
    ALGORITHM: str                    = os.getenv("ALGORITHM", "HS256")
    ACCESS_TOKEN_EXPIRE_MINUTES: int  = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))
    REFRESH_TOKEN_EXPIRE_DAYS: int    = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7"))
    FERNET_KEY: str                   = os.getenv("FERNET_KEY", "")
    DATABASE_URL: str                 = os.getenv("DATABASE_URL", "postgresql+asyncpg://algo_bot:algo_bot@localhost:5432/algo_bot")
    REDIS_URL: str                    = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    ADMIN_EMAIL: str                  = os.getenv("ADMIN_EMAIL", "admin@algo_bot.com")
    ADMIN_PASSWORD: str               = os.getenv("ADMIN_PASSWORD", "admin123")
    HOST: str                         = os.getenv("HOST", "0.0.0.0")
    PORT: int                         = int(os.getenv("PORT", "8000"))
    DEBUG: bool                       = os.getenv("DEBUG", "true").lower() == "true"
    ALLOWED_ORIGINS: str              = os.getenv("ALLOWED_ORIGINS", "http://localhost:8000,http://localhost:3000")

    # Upstox OAuth — this exact URL must be registered as the
    # "Redirect URI" in EVERY user's Upstox developer app (same URL
    # for all users; the user is identified via a signed `state`
    # parameter, not the URL itself).
    UPSTOX_REDIRECT_URI: str          = os.getenv("UPSTOX_REDIRECT_URI",
                                                    "http://localhost:8000/api/users/upstox/callback")

    # Web Push (VAPID) — for browser push notifications on live trade
    # events. Generate once with: python scripts/generate_vapid_keys.py
    VAPID_PUBLIC_KEY:  str            = os.getenv("VAPID_PUBLIC_KEY", "")
    VAPID_PRIVATE_KEY: str            = os.getenv("VAPID_PRIVATE_KEY", "")
    VAPID_CLAIM_EMAIL: str            = os.getenv("VAPID_CLAIM_EMAIL", "admin@algo_bot.com")

    # Upstox webhook signature secret (set in Upstox developer app
    # → Webhook → Postback Secret). Leave blank in dev to skip
    # signature verification (NOT safe for production).
    WEBHOOK_SECRET: str               = os.getenv("WEBHOOK_SECRET", "")

    # ── Google OAuth ─────────────────────────────────────────────
    # Create at https://console.cloud.google.com/apis/credentials
    # Set Authorised redirect URI to:
    #   {APP_BASE_URL}/api/auth/google/callback
    GOOGLE_CLIENT_ID:     str         = os.getenv("GOOGLE_CLIENT_ID", "")
    GOOGLE_CLIENT_SECRET: str         = os.getenv("GOOGLE_CLIENT_SECRET", "")
    APP_BASE_URL:         str         = os.getenv("APP_BASE_URL", "http://localhost:8000")

    @property
    def google_redirect_uri(self) -> str:
        return f"{self.APP_BASE_URL}/api/auth/google/callback"

    @property
    def google_enabled(self) -> bool:
        return bool(self.GOOGLE_CLIENT_ID and self.GOOGLE_CLIENT_SECRET)

    # ── Email (SMTP) — for verification emails ───────────────────
    # Works with Gmail (use an App Password, not your main password)
    # or any SMTP provider (Mailgun, SendGrid, etc.)
    SMTP_HOST:     str                = os.getenv("SMTP_HOST", "smtp.gmail.com")
    SMTP_PORT:     int                = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USER:     str                = os.getenv("SMTP_USER", "")
    SMTP_PASSWORD: str                = os.getenv("SMTP_PASSWORD", "")
    SMTP_FROM:     str                = os.getenv("SMTP_FROM", "")   # defaults to SMTP_USER if blank

    @property
    def email_enabled(self) -> bool:
        return bool(self.SMTP_USER and self.SMTP_PASSWORD)

    @property
    def push_enabled(self) -> bool:
        return bool(self.VAPID_PUBLIC_KEY and self.VAPID_PRIVATE_KEY)

    @property
    def origins_list(self):
        return [o.strip() for o in self.ALLOWED_ORIGINS.split(",")]

    # ── Razorpay — subscription payments ──────────────────────────
    # Create an app at https://dashboard.razorpay.com/app/keys
    # Webhook secret is set separately under Settings → Webhooks.
    RAZORPAY_KEY_ID:         str        = os.getenv("RAZORPAY_KEY_ID", "")
    RAZORPAY_KEY_SECRET:     str        = os.getenv("RAZORPAY_KEY_SECRET", "")
    RAZORPAY_WEBHOOK_SECRET: str        = os.getenv("RAZORPAY_WEBHOOK_SECRET", "")

    @property
    def razorpay_enabled(self) -> bool:
        return bool(self.RAZORPAY_KEY_ID and self.RAZORPAY_KEY_SECRET)

    @property
    def fernet(self):
        from cryptography.fernet import Fernet
        if not self.FERNET_KEY:
            raise RuntimeError(
                "FERNET_KEY not set in .env\n"
                "Run: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\"\n"
                "Then paste into .env as FERNET_KEY=..."
            )
        return Fernet(self.FERNET_KEY.encode())


@lru_cache()
def get_settings() -> Settings:
    return Settings()
