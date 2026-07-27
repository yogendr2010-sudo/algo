# OptionSignalTool — Multi-User Trading Bot

## Project Structure
```
trading_bot_project/
├── backend/
│   ├── main.py                  # FastAPI app entry point
│   ├── config.py                # All settings / env vars
│   ├── db/
│   │   ├── database.py          # SQLAlchemy setup
│   │   └── models.py            # User, Trade, BotConfig, TradeLog models
│   ├── routers/
│   │   ├── auth.py              # Register, login, token refresh
│   │   ├── users.py             # Profile, settings, Upstox token
│   │   ├── bot.py               # Start/stop/status bot per user
│   │   ├── trades.py            # Trade history, P&L
│   │   ├── admin.py             # Admin: all users, force-stop, stats
│   │   └── ws.py                # WebSocket live feed endpoint
│   ├── services/
│   │   ├── auth_service.py      # JWT creation/validation
│   │   ├── bot_manager.py       # Per-user engine thread manager
│   │   └── broadcaster.py       # WebSocket connection manager
│   └── engine/
│       ├── engine_v6.py         # Core trading engine (per-user instance)
│       ├── instruments.py       # Instrument loader, ITM selector, strike step
│       └── indicators.py        # EMA, RSI, ROC, VWAP, ATR
├── frontend/
│   ├── templates/
│   │   ├── base.html
│   │   ├── index.html           # Landing / login
│   │   ├── dashboard.html       # Live trading dashboard
│   │   ├── settings.html        # Bot config, Upstox token
│   │   ├── trades.html          # Trade history + P&L chart
│   │   └── admin.html           # Admin panel
│   └── static/
│       ├── css/style.css
│       └── js/
│           ├── dashboard.js     # WebSocket live feed, chart updates
│           └── trades.js        # Trade history chart
├── scripts/
│   ├── init_db.py               # Create tables + default admin user
│   └── run.sh                   # Start server
├── requirements.txt
└── .env.example
```

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Copy and fill environment file
cp .env.example .env

# 3. Init database
python scripts/init_db.py

# 4. Run (laptop)
bash scripts/run.sh

# For VPS production (with nginx + systemd):
# See scripts/run.sh for uvicorn command
```

## Features
- JWT auth (register / login / token refresh)
- Per-user Upstox access token storage (encrypted at rest)
- Per-user bot config: symbol, strategy, qty, ITM depth, trail mode
- Per-user risk limits: max trades/day, max loss/day, allowed hours
- Live WebSocket feed: ticks, signals, SL moves, exits
- Trade history with P&L, per strategy breakdown
- Admin panel: view all users, force-stop any bot, global stats
- Auto ITM strike selection, direction flip, market hours guard

## Architecture
- **Web process** (`backend.main:app`, uvicorn) — stateless API + frontend
- **Worker process** (`worker.py`) — runs all trading engines, Telegram,
  Option Chain monitors; exactly one instance
- **Redis** — command queue + live state snapshots + pub/sub connecting
  the two processes (see SETUP_GUIDE.txt "PRODUCTION / MULTI-USER
  ARCHITECTURE")

## Deployment (Hostinger VPS)

1. Upload project via SFTP / git
2. Install Python 3.10+, PostgreSQL, Redis, nginx
3. Set up systemd services: `scripts/algo_bot-web.service` and
   `scripts/algo_bot-worker.service` (see SETUP_GUIDE.txt
   "PRODUCTION / MULTI-USER ARCHITECTURE")
4. Point nginx to uvicorn on port 8000
5. SSL via Let's Encrypt (certbot)

## Production checklist
- [ ] `SECRET_KEY`, `FERNET_KEY`, `ADMIN_PASSWORD` changed from defaults
- [ ] `DEBUG=false`, `ALLOWED_ORIGINS` set to real domain(s)
- [ ] Both `algo_bot-web` and `algo_bot-worker` running (exactly one worker)
- [ ] `GET /health` returns `{"status":"ok","redis":"ok","database":"ok"}`
- [ ] Postgres reachable, `python scripts/init_db.py` run
- [ ] Redis reachable (`redis-cli ping` → `PONG`)
