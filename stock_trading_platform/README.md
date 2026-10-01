# Design a Stock Trading(Broker) Platform like Zerodha or Groww

## functional requirements

- user signup and kyc validation
- user can view prices of a stock in near real-time, as well as view price history of the stock over a period of time
- user can place an order to buy/sell a particular stock + cancelling + limit order value
- user should be able to create custom watchlist with realtime updates
- provide dashboard to view current holdings, trade history, P&L, performance insights

## non functional requirements

- Scale: High frequency trades, millions of users, 8-10k stocks
- CAP theorem: buying/selling of stocks should be highly consistent, viewing stock details should be highly available
- latency: stock price changes track < 50ms, buy/sell (order placement) < 100ms

## Core Entities

- User
- Stock
- Order
- Trade with Exchange (BSE/NSE)
- Portfolio
- Watchlist

## API endpoints

### 1. User data

- POST /v1/auth/signup
- POST /v1/auth/login
- POST /v1/auth/verify-kyc

### 2. Market data

- GET /v1/stocks - get all stocks (paginated response)
- WS /v1/stocks/:stock_id - realtime details of a stock
- GET /v1/stocks/:stock_id/history - historical data of the stock (upto a certain time period)

### 3. Fund endpoints

- POST /v1/funds/deposit
- POST /v1/funds/withdraw
- GET /v1/funds/history

### 4. Trading endpoints (orders)

- POST /v1/orders - buy/sell stocks
- GET /v1/orders - list user orders
- GET /v1/orders/:order_id - a particular order details
- DELETE /v1/orders/:order_id - cancel an order

### 5. Portfolio endpoints

- GET /v1/portfolio
- GET /v1/portfolio/holdings
- GET /v1/portfolio/performance
- GET /v1/portfolio/positions

### 6. Watchlist endpoints

- GET /v1/watchlists
- POST /v1/watchlists
- POST /v1/watchlists/:id/symbols

## Deep Dive

![alt text](stock_trading.png)

### 0. The 30-second pitch

> "The broker is a middleman between millions of users and the exchange (NSE/BSE). **Market data** flows in one direction: an **Exchange Gateway** holds a few WebSocket connections to the exchange, pushes ticks into **Kafka**, and a **Price Ingestor** writes them to **InfluxDB** (history) and **Redis Pub/Sub** (latest price). A **Price Tracker** WebSocket server fans those prices out to users. **Orders** flow the other way: they go into Kafka as `raw_orders`, a **validator** checks KYC, funds and market hours, and the **Order Service** records the order and sends it to the exchange. Fills come back on an `order_status` topic, which updates the Order DB and **Trade DB** and notifies the user. Orders need **consistency**, so they use Postgres and a single source of truth. Prices need **availability and low latency**, so they use Redis and WebSockets."

---

### 1. Flow 1: Signup and KYC

```
User → ALB + API Gateway → User Service ─→ 3rd-party KYC validation
                                        └─→ UserDB (Postgres) {name, email, contact, isKYCVerified, ...metadata}
```

- A user can't trade until `isKYCVerified = true`. The order validator checks this later.

---

### 2. Flow 2: Funds (deposit/withdraw)

```
User → API GW → Payment Service → Payment Gateway (Razorpay etc.)
                                ← ACK (success/failure)
                → Payment DB (Postgres) {user_id, transaction_id, currency, amount, status, timestamp}
```

- The order validator reads the user's balance from here to check funds and margin.
- Use `transaction_id` as an **idempotency key** so a retried deposit is never credited twice.

---

### 3. Flow 3: Market data ingestion (always running, the core of "real-time")

```
Exchange (NSE/BSE) ══WS══> Exchange Gateway        (each WS carries ~400–500 stocks, so ~20 connections cover 8–10k stocks)
                               │ every ~1s, ticks for every stock
                               ▼
                    Kafka topic: stock_price
                               │
                               ▼
                    Price Ingestor (Kafka consumer)
                        ├─→ InfluxDB (time-series)  : every price, every second → history charts
                        └─→ Redis Pub/Sub           : latest price per stock   → live push
                                   │
                                   ▼ (subscriber)
                    Price Tracker Service (WebSocket server)
                                   │
User ◄══WS (via WS Gateway)════════┘  user subscribes to the stocks they're viewing
```

- **Why Kafka between the gateway and the ingestor?** It absorbs bursts, since 10k ticks/sec is spiky at market open. It decouples the exchange connection from slow consumers, and it can replay if the ingestor crashes.
- **Why InfluxDB?** The data is append-only and keyed by time. It supports fast range queries like "price of X over 1 month" and built-in downsampling (1s → 1m → 1d candles).
- **Why Redis Pub/Sub?** It fans out in memory in under a millisecond, which is what keeps the **< 50ms price latency** requirement. Pub/Sub is fire-and-forget, which is fine here: a lost tick is replaced by the next one a second later.
- **History graph:** `GET /stocks/:id/history`. The Price Tracker reads InfluxDB to build the chart.
- **Why WebSockets for users?** The server pushes to the client. Polling 10k stocks × millions of users is impossible.

---

### 4. Flow 4: Watchlist

```
User → API GW → Watchlist Service → Watchlist DB (Postgres)
                                      user_watch_map {user_id, watchlist_id}
                                      list          {watch_id, stocks}
                Watchlist Service ──→ Price Tracker : subscribe to live prices of every stock in the watchlist
```

- The watchlist is just a set of stock IDs. The live prices come from the same Price Tracker / Redis Pub/Sub path as Flow 3.

---

### 5. Flow 5: Order placement (the core deep dive)

`POST /v1/orders {stock_id, side, qty, order_type: market|limit, price}`

```
User → API GW → Kafka topic: raw_orders                      (ack the user fast with order_id + status PENDING)
                    │
                    ▼
        Order Validator (consumer of raw_orders)
          - KYC check
          - account status (active, not blocked)
          - fund and margin validation (reads User/Payment DB)
          - time and session validation (market open? pre-open? holiday?)
                    │
          ┌─────────┴─────────┐
          ▼                   ▼
   verified_orders      rejected_orders       (Kafka topics)
          └─────────┬─────────┘
                    ▼
        Order Service (consumer of verified + rejected)
          ├─→ Order DB (Postgres)       : write order, trade_id = NULL, status
          ├─→ Notification Service      : "order placed" / "order rejected" → user
          ├─→ Trade DB                  : verified orders
          └─→ Exchange Gateway → Exchange : place the order
```

#### Order table (Order DB, Postgres)

`order_id, user_id, stock_id, order_type (market/limit), price, qty, trade_id (NULL until the exchange responds), status`

**Status lifecycle:** `PENDING → VERIFIED | REJECTED → PLACED → EXECUTED | PARTIALLY_FILLED | CANCELLED`

---

### 6. Flow 6: Order status and execution (return path)

```
Exchange → Exchange Gateway → Kafka topic: order_status {order_id, trade_id, status}
                                   │
                                   ▼
                    Order Tracker (consumer of order_status)
                        ├─→ Order DB     : set trade_id + status
                        ├─→ Trade DB     : only verified/executed orders
                        └─→ Notification Service → user ("your limit order was filled")
```

- **Why mainly limit orders?** A market order fills almost immediately. A limit order can sit on the exchange's order book for hours until the price reaches the limit, so its fill arrives **asynchronously** and has to be tracked.
- **Cancel (`DELETE /orders/:id`):** send the cancel to the exchange through the gateway. The order counts as cancelled only when the exchange confirms on `order_status`, because it may already be filled.

#### Trade DB: why a separate copy?

- It has the same schema as Order DB but holds **only verified/executed trades**.
- **End-of-day reconciliation:** after market close, compare the Trade DB with the exchange's trade file to confirm the two are in sync. This is the broker's settlement source of truth.
- It's also the clean input for the Portfolio / P&L service, which then doesn't have to filter out rejected or pending orders.

---

### 7. Flow 7: Portfolio, P&L and dashboard

```
User → API GW → Portfolio Service ─→ Trade DB        : what the user owns (qty, buy price)
                                  └─→ Price Tracker   : current price of each holding
                P&L = Σ (current_price − avg_buy_price) × qty
```

- Holdings change only when trades happen, so they can be cached. Live P&L is recomputed from the live price stream.

---

### 8. Data stores at a glance

| Store        | Tech            | Holds                                                                             | Why                                   |
| ------------ | --------------- | --------------------------------------------------------------------------------- | ------------------------------------- |
| UserDB       | Postgres        | Profile, KYC flag                                                                 | Relational, low write rate            |
| Payment DB   | Postgres        | Deposits/withdrawals, balance                                                     | ACID; money must be correct           |
| Order DB     | Postgres        | Every order and its status                                                        | Strong consistency                    |
| Trade DB     | Postgres        | Only executed trades                                                              | Reconciliation and P&L source         |
| Watchlist DB | Postgres        | user → watchlist → stocks                                                         | Simple relational data                |
| InfluxDB     | Time-series     | Per-second price history                                                          | Fast time-range queries, downsampling |
| Redis        | Pub/Sub + cache | Latest price per stock                                                            | Sub-ms fan-out for < 50ms latency     |
| Kafka        | Stream          | `stock_price`, `order_status`, `raw_orders`, `verified_orders`, `rejected_orders` | Decoupling, buffering, replay         |

---

### 9. CAP mapping (say this explicitly)

- **Orders, funds and trades → CP.** Never double-spend a balance or lose an order. Use Postgres transactions, idempotency and reconciliation.
- **Prices, watchlist and charts → AP.** A price that's 1 second stale is fine, and a blank screen is not. Use Redis, WebSockets and Influx.

---

### 10. Quick-fire answers to likely follow-ups

- **"Kafka in the order path, but < 100ms latency?"** The 100ms is for **acknowledging** the order. The API writes to Kafka (a few ms) and returns `order_id` with status PENDING. Validation and placement happen asynchronously, and the user gets the final status by push. Kafka also protects the system during the 9:15 AM market-open spike.
- **"How do you stop a user spending the same balance twice?"** The validator must **block (reserve) funds atomically**, for example `UPDATE balance SET available = available - x WHERE available >= x`, in one Postgres transaction. If the order is rejected or cancelled, release the block. Partition `raw_orders` by `user_id` so one user's orders are processed in sequence.
- **"What if the user retries POST /orders?"** The client sends an **idempotency key**, stored with a unique constraint in Order DB, so the retry doesn't create a duplicate order.
- **"How do 10k stocks × millions of users scale on WebSockets?"** Run many stateless Price Tracker nodes behind the WS Gateway. Each node subscribes to Redis channels only for the stocks its connected users want. Users are sharded across nodes, and **throttling/conflation** sends at most 1 update/sec per stock.
- **"What if Price Tracker / Redis drops a tick?"** Nothing is lost that matters. The next tick replaces it, and the history is safe in Influx through Kafka.
- **"What if the Exchange Gateway crashes after placing an order but before the status is written?"** `order_status` comes from the exchange. On restart, query the exchange for open orders, and EOD reconciliation against Trade DB catches anything missed.
- **"Why is the Exchange Gateway a separate service?"** The exchange limits connections (~400–500 symbols per WS). A dedicated gateway owns those few connections, the protocol translation and the rate limits.
- **"How are price-history charts fast over years of data?"** InfluxDB retention and downsampling keep raw 1s data for recent days and 1m/1h/1d candles for older ranges.

## Exaclidraw link

https://excalidraw.com/#json=Q_74JG9Lyxlp8D5o3MPq3,OUczIzWjWi4TFFg_KYrjlw
