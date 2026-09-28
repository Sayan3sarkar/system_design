# Design a ticket booking system like BookMyShow or District

## functional requirements

- user registration
- search event based on title/location/date
- view the selected event details (seat, description, venue, metadata etc)
- book/reserve a seat for the event

## non functional requirements

- scale: 100M DAU
- CAP theorem: Highly consistent for ticket booking. Highly available for searching and viewing event details
- confirm how many users browse through events vs those who actually book tickets: 80% search-only, 20% search + booking

## core entity

- User
- Event
- Venue
- Ticket

## API creation

- `POST /v1/user/register` - for user registration with user metadata like username, password, email etc
- `GET /v1/search?q=:search_text&location=:location&date={date}` - return paginated list of events
- `GET /v1/event/:id` - return event details + location details + available seats
- `POST /v1/booking/reseve` - body: `{seats: list[Seat]}` -> response bookingID
- `POST /v1/booking/confirm` - body: `{bookingID: str, paymentDetails: dict}`

## HLD

- given the scale of the application we go ahead by splitting this into microservices so that each service can have a separate reponsibility, aned even more important than that, each part of the service can be scaled independently depending on load

![alt text](<Screenshot 2026-09-04 at 9.00.49 AM.png>)

## Deep Dive

### Choice of Database

- For booking, consistency is key. So we opt for PostgreSQL
- For metadata storage regarding events, movies, etc (massive volume), high availability is important - we can go for either MongoDB or Cassandra. Choose MongoDB, if metadata needs rich nested structure with adhoch querying flexibility. Choose Cassandra, if write_optimization is crucial + a masterless fault-tolerant architecture support built-in. For our case, we go with **Cassandra**
- We introduce an elasticsearch cluster for our search service, which basically would replicate the data from our cassandra DB but elasticsearch would allow efficient indexing on multiple fields like name, date, actors etc
  - We need a mechanism to ingest the cassandra data on write to the elasticsearch cluster and then index it accordingly
  - We can implement this via a CDC event (Change Data Capture). This means that whenever there's a change in CDC, a change event gets triggered, and whatever the changes be, it gets ingested into ElasticSearch
  - Identifying real-time change attributes. Like `seats`
- Implement Redis Distributed locking for booking optimisation and prevent 2 users booking same ticket
  ![alt text](<Screenshot 2026-09-04 at 11.53.16 PM.png>)

  By the way, the redis lock release must happen via a LUA script so as to ensure the deletion does not happen for another user but for the one requesting it

  The key_value pair would look something like `{"lock:seat:<event_id>:<seat_id>" : <uuid_generated_for_user_id>`

  Sample LUA script

  ```lua
  -- KEYS[1] = lock key  (e.g. "lock:seat:evt_12345:A12")
  -- ARGV[1] = lock token the caller believes it holds
  if redis.call("GET", KEYS[1]) == ARGV[1] then
      return redis.call("DEL", KEYS[1])
  else
      return 0
  end
  ```

---

### 0. The 30-second pitch

> "Users search for events, open an event to see its seat map, pick seats and pay. An **API Gateway** handles auth, rate limiting and routing to three services: **Search**, **Event** and **Booking**. Event and movie metadata lives in **Cassandra** (high volume, highly available). **CDC with Debezium → Kafka** streams every change into **Elasticsearch**, which powers full-text search. Booking is the part that must be consistent. When a user picks seats, the Booking service takes a **Redis lock per seat with `SET NX EX 300`**, so only one user can hold a seat for 5 minutes. The user then pays through the **Payment Gateway**. On success, the booking and seat status are committed in **Postgres** (the source of truth) and the lock is released with a **Lua compare-and-delete**. If payment fails or the user walks away, the TTL frees the seats automatically."

---

### 1. Flow 1: Search events

`GET /v1/search?q=:search_text&location=:location&date={date}`

```
Client → API Gateway (auth, rate limit, routing) → Search Service
   └─→ Elasticsearch cluster
         ├─ full-text match on title / artist / actors / description
         ├─ filter on location (city / venue) and date range
         └─ sort by relevance / date, paginate
← paginated List<Event summary> (id, title, venue, date, poster)
```

- **Key point:** Search never touches Cassandra or Postgres. It only reads Elasticsearch, so the heavy search traffic (80% of users only browse) is isolated from booking.
- **Why Elasticsearch?** Cassandra is fast when you query by partition key, but it can't do "fuzzy text match on title + filter by city + date range". Elasticsearch has an inverted index for text and can filter on many fields at once.
- **Why paginate?** A query like "movies in Bangalore" can return thousands of results. Use cursor-based pagination (`search_after`) for deep pages.
- **Availability over consistency:** a newly added event showing up a second or two late in search results is fine.

---

### 2. Flow 2: Keeping search in sync (CDC pipeline, always running)

```
Event Service / admin writes → Cassandra (Event, Movie tables)
   └─→ Debezium (CDC connector reads Cassandra's commit log)
         └─→ Kafka topic (one change event per insert / update / delete)
               └─→ Kafka consumer (indexer) → Elasticsearch (upsert / delete the document)
```

- **Why CDC instead of writing to both stores?** A "dual write" from the app can half-fail (Cassandra succeeds, Elasticsearch doesn't), and the two drift apart. With CDC, Cassandra is the single source and Elasticsearch catches up from the change log.
- **Why Kafka in the middle?** It buffers bursts, and if Elasticsearch is down the events wait in Kafka and are replayed later. Nothing is lost.
- **Idempotent indexing:** use the event `id` as the Elasticsearch document id, so a replayed message just overwrites the same document.
- **What not to index:** seat availability changes every second during a hot sale. Don't push it through CDC into Elasticsearch. Search shows event-level info only, and live seats are fetched in Flow 3.

---

### 3. Flow 3: View event details and seat map

`GET /v1/event/:id`

```
Client → API Gateway → Event Service
   ├─→ Cache (Redis / CDN) → event metadata (rarely changes, long TTL)
   ├─→ Cassandra           → on cache miss: event + venue + seat layout
   └─→ Seat availability    → Postgres (BOOKED seats) + Redis (seats currently locked / HELD)
← event details + venue details + seat map with status per seat (AVAILABLE / HELD / BOOKED)
```

- **Event record (Cassandra):** id, artist_id, venue, date, description, seat (seat layout / categories / price)
- **Movie record (Cassandra):** id, title, actors, seat (layout reference for its shows)
- **Key point:** static metadata and live seat status come from different places. Metadata is cached aggressively. Seat status is read live and can be slightly stale. The real check happens at reserve time (Flow 4), so a stale "available" here is harmless.
- **Why Cassandra partitioned by event id?** Each event page is one partition lookup, and the store keeps serving reads even if a node goes down.

---

### 4. Flow 4: Reserve seats (the core deep dive)

`POST /v1/booking/reserve {seats: list[Seat]}` → `bookingID`

```
Client → API Gateway → Booking Service
  1. For each requested seat, try to take a lock in Redis:
        SET lock:seat:<event_id>:<seat_id> <token> NX EX 300
        (token = a UUID generated for this user's booking attempt)
  2. Did every SET succeed?
        ├─ YES → create booking in Postgres: status = PENDING, expires_at = now + 5 min
        │        → return bookingID (client shows a 5-minute countdown)
        └─ NO  → release the seats this request did lock (Lua compare-and-delete)
                 → return failure: "seat(s) no longer available"
  3. Nobody confirms within 300s → keys expire on their own → seats are free again
```

#### Postgres tables (source of truth for bookings)

- **Booking table:** booking_id, user_id, event_id, status (PENDING / CONFIRMED / EXPIRED / CANCELLED), amount, expires_at, created_at
- **Booking_seat table:** booking_id, event_id, seat_id, with a **UNIQUE constraint on (event_id, seat_id)** for confirmed seats
- **Seat table:** event_id, seat_id, category, price, status (AVAILABLE / BOOKED), version

#### The race condition (the classic interview question)

- **Problem:** two users see seat A12 as available and both click "book" at the same moment. Without coordination, both get charged for the same seat.
- **Solution:** a **short-lived distributed lock per seat** in Redis.
  - `SET ... NX` is atomic. Only one caller can create the key, so the second user fails immediately and gets a clear error instead of a surprise after paying.
  - `EX 300` gives the user 5 minutes to pay. If they close the tab, or payment fails, the lock expires by itself and the seat goes back on sale. No cleanup job is needed.
  - The key is **per seat**, not per user. Locking `(user_id, seats)` under one key wouldn't stop a second user from locking an overlapping seat.
  - **Multi-seat bookings are all-or-nothing:** take the locks in a fixed order (sorted seat_id), and if any one fails, release the ones you got. Alternatively, do the whole check-and-set in one Lua script so it's atomic in Redis.
  - **Release with Lua compare-and-delete** (the script above): only the holder of the token can delete the key. Otherwise user A, whose lock already expired, could delete user B's fresh lock.
- **Belt and braces:** Redis is a fast gate, not the source of truth. The final write in Postgres is guarded too (the UNIQUE constraint, or `UPDATE seat SET status='BOOKED' WHERE status='AVAILABLE'`). Even if a lock is lost (Redis failover, TTL ran out mid-payment), two bookings for one seat can never both commit.

#### CAP mapping (say this explicitly)

- **Booking needs consistency (CP):** a seat must never be sold twice. That's why there's a Redis lock plus an ACID commit in Postgres. If we can't be sure, we reject the booking.
- **Search and event details need availability (AP):** Cassandra + Elasticsearch + cache keep browsing up even during partial failures. Slightly stale data is fine.

---

### 5. Flow 5: Confirm booking and payment

`POST /v1/booking/confirm {bookingID, paymentDetails}`

```
Client → API Gateway → Booking Service
  1. Check booking is PENDING and not expired, and the Redis locks still hold this token
  2. Booking Service → Payment Gateway (Stripe / Razorpay), idempotency key = bookingID
  3. Payment Gateway → ACK (success / failure), also via webhook
        ├─ SUCCESS → one Postgres transaction:
        │              booking.status = CONFIRMED, seat.status = BOOKED
        │           → THEN release the Redis locks (Lua compare-and-delete)
        │           → send ticket (email / SMS / push)
        └─ FAILURE → booking.status = CANCELLED → release locks right away
                     (if nothing comes back at all, the 300s TTL frees the seats)
```

- **Order matters:** commit to Postgres first, then delete the Redis key. If you delete first and crash, the seat looks free in Redis but isn't booked yet, and another user can grab it.
- **Why an idempotency key?** Clients and the gateway both retry. Using `bookingID` as the key means a retry never charges twice.
- **Why a webhook as well?** The user can close the app mid-payment. The gateway's webhook still tells the Booking service the final result.
- **Payment succeeded but the hold already expired?** The guarded Postgres update fails if someone else has booked the seat. Refund automatically, and tell the user. To make this rare, stop accepting payment a little before `expires_at`.

---

### 6. Data stores at a glance

| Store | Tech | Holds | Why |
|---|---|---|---|
| Event / Movie metadata | Cassandra | Events, movies, venues, seat layouts | Huge volume, write-optimised, masterless and highly available |
| Search index | Elasticsearch | Denormalised copy of event metadata | Full-text search plus multi-field filters |
| Change stream | Debezium + Kafka | Change events from Cassandra | Keeps Elasticsearch in sync without dual writes |
| Booking DB | PostgreSQL | Bookings, booking seats, seat status | ACID transactions and unique constraints, so no double selling |
| Seat locks | Redis | `lock:seat:<event_id>:<seat_id>` → token, TTL 300s | Atomic `SET NX`, auto-expiry, very fast |
| Cache | Redis / CDN | Event details, posters | Read-heavy traffic, data rarely changes |

---

### 7. Quick-fire answers to likely follow-ups

- **"Why not just use a Postgres row lock (`SELECT ... FOR UPDATE`)?"** A hold lasts up to 5 minutes while the user pays. Keeping DB transactions or row locks open that long would starve the connection pool. Redis gives the same exclusivity with a TTL and no open transaction.
- **"What if Redis goes down?"** New reservations fail fast (consistency over availability for booking). The Postgres guard still prevents double booking. Use Redis with replicas and Sentinel/Cluster for failover.
- **"How do you handle a hot event, like a big concert sale?"** Put users in a **virtual waiting queue** in front of the Booking service and let them in at a controlled rate. Rate limit per user at the gateway, and serve the event page from cache/CDN.
- **"How does the seat map stay fresh while people are booking?"** Poll every few seconds, or push seat status changes over WebSocket/SSE on hot events. Either way, the reserve call is the real check.
- **"Why is the TTL 5 minutes?"** It's long enough to finish payment and short enough that abandoned holds come back on sale quickly. It's a product choice and can be tuned.
- **"Can a user hold every seat in a venue?"** Cap seats per booking (for example, 10) and the number of active holds per user.
- **"How do you scale Postgres at 100M DAU?"** Only about 20% of users book, and writes are per event. Shard the booking DB by `event_id` so one event's seats live on one shard and transactions stay local. Add read replicas for booking history.
