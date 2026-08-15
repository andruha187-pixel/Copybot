# Powerwinner Paper Copy Simulator

This is a **paper-only** copy simulator. It does not place real orders.

## What it measures

- Powerwinner public trade detection
- detection latency
- current public CLOB order book
- execution across multiple ask/bid levels
- partial fills
- average simulated fill price
- slippage vs Powerwinner
- crypto taker fees
- resolved-market PnL
- virtual cash balance

## Speed

Default leader polling:

`0.10 seconds`

Polymarket currently documents the public Data API `/trades` limit as 200 requests / 10 seconds. A 0.10s interval is 100 requests / 10 seconds, leaving headroom.

The public Market WebSocket is used continuously for order-book updates.

## Important limitation

A public observer cannot receive Powerwinner's private order events.

The bot can only react after Powerwinner's executed trade becomes visible through the public Data API. Therefore this measures **copyable performance**, which is exactly what we want.

## Copy multiplier

`COPY_MULTIPLIER=1.0`

means copy the same number of shares.

Use:

`COPY_MULTIPLIER=0.1`

for 10% of Powerwinner's size.

## Balance modes

For pure execution research:

`ENFORCE_BALANCE=false`

This allows an unrestricted virtual portfolio and shows the true theoretical copy result.

To test a fixed bankroll:

`ENFORCE_BALANCE=true`

and set:

`INITIAL_BALANCE=100`

or another amount.

## Telegram report

Every completed UTC hour, after a 5-minute delay, Telegram receives a ZIP containing:

- `leader_trades.csv`
- `copy_attempts.csv`
- `market_results.csv`
- `report.txt`

## Recommended deployment

Run this as a **separate Render service/repository** from Wallet Observer.

Do not replace the Observer. We want both datasets independently.

Add environment variables:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

For persistent history, mount a disk at:

`/var/data`

## Fee model

For crypto markets:

`fee = shares × 0.07 × price × (1 - price)`

The simulator applies that fee to each simulated fill level.

## What to send back to ChatGPT

After several hours, upload the ZIP reports from Telegram.

We can compare:
- Powerwinner price
- simulated copy price
- slippage
- missed/partial fills
- fees
- realized PnL
- copy vs leader economics
