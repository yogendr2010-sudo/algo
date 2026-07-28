# Semi-Auto Trade Approval from Telegram

## Steps:

- [x] 1. `telegram_alerts.py` - Add `alert_pending_trade()` function
- [x] 2. `telegram_bot.py` - Add `/approve` and `/reject` command handlers
- [x] 3. `telegram_bot.py` - Register new commands in COMMANDS dict
- [x] 4. `telegram_bot.py` - Fix command dispatch to parse `/approve_42` format
- [x] 5. `worker.py` - Send Telegram notification on PENDING_TRADE event
- [x] 6. `telegram_bot.py` - Update `/help` with approve/reject commands
- [x] 7. Verify the implementation

