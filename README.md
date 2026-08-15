# Copy Simulator v2 — Equity accounting

Paper only. No real orders.

Default wallet:
`0x13e0d447520ebe7f8eeaf7817211201b2c585204`

Fixes:
- cash is no longer treated as portfolio value;
- reports Cash + Open Position Value + Equity;
- conservative mark-to-market uses best bid for long positions;
- exact 5-minute market slug is stored;
- ended markets are resolved through Gamma event slug fallback;
- settlement payouts are credited to cash;
- hourly ZIP includes portfolio.csv;
- health endpoint shows equity, realized and unrealized PnL.

Keep the same persistent disk `/var/data` if upgrading the existing service so the bot can migrate the current DB and settle positions already collected.
