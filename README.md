# Lyft Clone — Real-Time Ride-Sharing & Food Delivery Platform

A full-stack simulation of a ride-sharing / food-delivery platform (think
Lyft + DoorDash): real-time driver dispatch, live GPS tracking over the
actual road network, and a rider/driver web app, backed by PostgreSQL and
Redis.

## Features

- **Rider app** — request a ride or food delivery on an interactive map,
  track your assigned driver in real time. Payment is simulated: the driver
  confirms the fare was collected (no payment gateway, no money moves).
- **Driver app** — go online, receive ride offers, accept/decline, follow
  turn-by-turn trip status, track earnings.
- **Real-time dispatch** — nearby drivers are found and offered rides over
  Socket.IO in well under a second.
- **Food delivery mode** — order from a seeded restaurant menu; the driver
  route is extended to include the restaurant pickup leg.
- **Ride simulation engine** — an internal service animates each accepted
  ride along the real road network (via OSRM) so the map shows a driver
  actually moving along streets, not a straight line.

## Architecture

```
                        ┌──────────────────┐
        HTTPS REST      │                  │   JWT-authenticated
    ┌─────────────────▶ │   Flask REST     │◀── login / signup /
    │                   │   (blueprints)   │    menu
┌───┴────┐              └────────┬─────────┘
│ Rider /│                       │
│ Driver │               ┌────────▼─────────┐        ┌───────────────┐
│  Web   │  WebSocket    │  Flask-SocketIO  │◀──────▶│  Redis        │
│  App   │◀─────────────▶│  (JWT-auth'd,    │        │  - GEO index  │
└────────┘   room-scoped │   room-scoped)   │        │  - pub/sub    │
                         └────────┬─────────┘        │    (multi-    │
                                  │                  │    instance)  │
                    ┌─────────────┴─────────────┐    │  - rate-limit │
                    │                           │    │    counters   │
           ┌────────▼─────────┐         ┌────────▼────────────┐
           │ services/        │         │  Postgres           │
           │  matching_service│◀───────▶│  (connection pool)  │
           │  routing_service │         │  users, drivers,    │
           │  pricing_service │         │  rides, restaurants │
           │  auth_service    │         └─────────────────────┘
           └──────────────────┘
                    │
                    ▼
        OSRM (road-network routing,
        with haversine fallback)

           ┌───────────────────────────┐
           │ ride_simulation_engine.js │  internal service (own auth),
           │ animates accepted rides   │  drives simulated GPS + status
           │ along real OSRM routes    │  for the demo
           └───────────────────────────┘
```

**Request flow (ride dispatch):**
1. Rider's browser requests a ride over a JWT-authenticated Socket.IO connection.
2. The backend computes fare/ETA via OSRM routing, then queries Redis's
   geospatial index (`GEOSEARCH`) for nearby available drivers.
3. Candidates are scored on distance *and* rating; the best match is offered
   the ride in a Socket.IO room containing only that driver.
4. On acceptance, the ride is persisted to Postgres, and both driver and
   rider sockets join a shared per-ride room for all further updates
   (progress, location, payment) — no data goes to unrelated clients.
5. A ride simulator (exactly one, chosen by `RIDE_SIMULATOR`) drives the
   accepted ride along the road route and reports status/location through
   the same guarded transitions a real driver client uses; the backend
   relays them into that same room.

Ride state (`requested → accepted → in_progress → completed`, or
`cancelled`), driver offers (`pending → accepted | rejected | expired`) and
payment confirmation are persisted in Postgres, so a reconnecting rider or
driver gets the current ride back. Offers expire after
`OFFER_TIMEOUT_SECONDS` and the ride moves to the next driver.

## Tech stack

| Layer | Choice | Why |
|---|---|---|
| API | Flask + Blueprints | Modular REST routes, easy to extend |
| Real-time | Flask-SocketIO + eventlet | Cooperative concurrency for many concurrent websocket connections on one process |
| Database | PostgreSQL | Relational integrity for users/rides/payments; connection-pooled via psycopg2 |
| Geospatial matching | Redis GEO (`GEOADD`/`GEOSEARCH`) | O(log N) nearest-driver queries |
| Horizontal scaling | Redis pub/sub as Socket.IO's `message_queue` | `emit(room=...)` works correctly across multiple backend instances |
| Auth | JWT (PyJWT) + bcrypt | Stateless session tokens; salted, adaptive-cost password hashing |
| Routing | OSRM, haversine fallback | Real road-network distance/ETA, with graceful degradation if OSRM is unreachable |
| Rate limiting | Flask-Limiter (Redis-backed) | Brute-force protection on login/signup |
| Containerization | Docker + docker-compose | One-command local stack (Postgres + Redis + backend) |

## Project structure

```
backend/
  app.py                     # Flask app factory + entrypoint
  config.py                  # env-driven configuration
  db.py                      # Postgres pool + Redis client
  extensions.py               # SocketIO / rate-limiter instances
  schema.sql                  # tables + indexes
  init_db.py / seed_db.py     # DB setup scripts
  services/
    auth_service.py           # bcrypt hashing, JWT issue/verify
    matching_service.py       # Redis-GEO driver matching + scoring
    pricing_service.py        # fare calculation (pure functions)
    routing_service.py        # OSRM + haversine fallback
  routes/
    auth_routes.py            # /api/login, /api/signup
    menu_routes.py             # /api/menu
    health_routes.py           # /api/health
  sockets/
    handlers.py                 # all Socket.IO event handlers
  ride_simulation_engine.js    # internal service: animates accepted rides
  tests/                      # pytest suite + a live socket-flow check
frontend/
  login.html, driver.html, new_map.html
scripts/
  load_test.js                # concurrency load test for the matching pipeline
docker-compose.yml
```

## Getting started

Before starting the app, copy the example environment file and replace the placeholder values with your own local settings:

```bash
cp backend/.env.example backend/.env
```

The app reads its configuration from environment variables, so no secrets should be committed to the repository.

### Option A: Docker Compose (recommended)

Requires Docker and Docker Compose.

```bash
cp backend/.env.example backend/.env
# edit backend/.env: set JWT_SECRET, FLASK_SECRET_KEY, SIMULATION_SERVICE_TOKEN
docker compose up --build
```

Once the backend is running, create the tables and seed some demo data:

```bash
docker compose exec backend python init_db.py
docker compose exec backend python seed_db.py
```

Serve the frontend (any static file server works):

```bash
python3 -m http.server 8000 --directory frontend
```

Open `http://localhost:8000/login.html`.

### Option B: Manual setup

Requires Python 3.12+, Node.js 18+, PostgreSQL, and Redis running locally.

```bash
cd backend
cp .env.example .env          # fill in your DB/Redis/JWT values
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

python init_db.py             # create tables (DROPS existing ones), then apply migrations/
python seed_db.py             # demo accounts + restaurants (password: password123)

python app.py                  # backend on http://127.0.0.1:5001
```

Upgrading an existing database instead? Keep your data and only apply the
migrations: `python init_db.py --migrate`.

In a separate terminal, serve the frontend:

```bash
python3 -m http.server 8000 --directory frontend
```

#### Ride simulator

Accepted rides are animated by exactly one simulator, chosen with
`RIDE_SIMULATOR` in `backend/.env`:

| Value | Simulator |
|---|---|
| `python` (default) | In-process simulator inside the backend. Nothing else to run. |
| `node` | `backend/ride_simulation_engine.js`. Start it with `cd backend && npm install && node ride_simulation_engine.js`. |
| `none` | No simulator; driver clients report progress with `ride_status_update`. |

The backend refuses the Node engine's connection unless `RIDE_SIMULATOR=node`,
so the two simulators never drive the same rides.

Open `http://localhost:8000/login.html`.

### Demo accounts

Seeded by `seed_db.py`, all with password `password123`:

- Riders: `user0@test.com` … `user4@test.com`
- Drivers: `driver0@test.com` … `driver2@test.com`
- Admin: `admin@test.com` (pre-flagged `is_admin`, sign in on `admin.html`)

### Food delivery mode

The rider map's "Food Delivery" mode looks up a restaurant/menu by name via
`/api/menu`. Try `Meghana Foods`, `Truffles`, `CTR`, `Empire`, or `Corner
House` (seeded in `seed_db.py`).

## Testing

```bash
cd backend
pytest tests -v
```

Without extra setup this runs the unit tests and skips the integration tests.
The integration tests exercise the ride lifecycle, offers, payment
confirmation, the driver pool and Socket.IO flows against a real Postgres and
Redis. To run them, point them at a disposable database and Redis db, which
they wipe:

```bash
createdb lyft_clone_test
LYFT_TEST_DB_NAME=lyft_clone_test LYFT_TEST_REDIS_URL=redis://localhost:6379/15 pytest tests -v
```

For a full live check against a running backend (real Postgres/Redis, real
JWT-authenticated sockets), see `backend/tests/manual_socket_flow_check.py`.

### Load testing

`scripts/load_test.js` brings a fleet of real driver accounts online and
fires concurrent ride requests through the actual dispatch pipeline. It
reports each stage separately (ride created, driver offered, driver accepted,
no driver available, completed, payment confirmed), with latency percentiles
for the stages that succeeded.

It creates all its accounts from one IP, so start the backend with
`LOAD_TEST_MODE=true`. That setting raises the login/signup rate limits for
this purpose only, and the backend refuses to start with it when
`FLASK_ENV=production`:

```bash
# backend
LOAD_TEST_MODE=true python app.py

# load test
cd scripts
npm install
node load_test.js --drivers=50 --riders=20 --url=http://127.0.0.1:5001
```

## Admin dashboard

`frontend/admin.html` shows operational state. It covers backend,
Postgres and Redis health; connected sockets; driver pool freshness and
eligibility; rides by status; stuck or inconsistent rides; offers; Redis key
counts; table sizes; and payment-confirmation totals. It is read-only and
refreshes every 10 seconds.

`seed_db.py` flags one demo account (`admin@test.com` / `password123`) as
admin, so a freshly seeded database always has one you can sign in with
right away.

Admin access is a flag on a rider account, and it can only be granted from
the command line. Run `set_admin.py` in the same place your database is
actually running:

```bash
# Docker Compose
docker compose exec backend python set_admin.py someone@example.com           # grant
docker compose exec backend python set_admin.py someone@example.com --revoke  # revoke

# Manual setup (venv, local Postgres)
cd backend
python set_admin.py someone@example.com           # grant
python set_admin.py someone@example.com --revoke  # revoke
```

Running it the other way round (e.g. from your host venv while Postgres is
actually inside Docker) silently targets whatever database your local `.env`
points at, which can be a different, unmigrated database — the account gets
"granted" there instead of in the one the app is using, or `is_admin` may not
exist there yet if it predates `migrations/001_ride_lifecycle.sql`.

The admin then opens `http://localhost:8000/admin.html` and signs in with
that account's password. The page is not the security boundary.
`GET /api/admin/overview` requires a valid JWT whose role is `admin`, and it
re-checks `users.is_admin` in Postgres on every request. Missing or invalid
tokens get 401; rider, driver and revoked-admin tokens get 403. Signup and
login payloads cannot grant the role. The response contains aggregates and
ids only: no passwords, hashes, emails, tokens or secrets.

## Recent frontend updates

- **Admin login link** — the login page has an "Admin Login" link in the
  top-right corner that goes to `admin.html`. The admin sign-in screen itself
  now uses the same red gradient / glass-card theme as the rider/driver login
  page, with a link back to it.
- **Place names on the map** — the rider map reverse-geocodes the pickup and
  drop points you click (via the public OSM Nominatim API) and shows the
  resolved place name instead of raw coordinates; it falls back to the
  coordinates if the lookup fails.
- **Map tiles** — switched from CartoDB's basemap (which now shows a
  "for evaluation only" watermark without an API key) to standard
  OpenStreetMap tiles, which are free, watermark-free and need no key.
- **Driver map: restaurant + correct route leg** — the driver's map now shows
  a 🍔 marker for the restaurant on food orders, and the route line always
  leads from the driver's current position to wherever they're actually
  headed next (restaurant → rider pickup → drop-off), instead of always
  showing the pickup→drop leg regardless of where the driver is.
- **Fare breakdown emphasis** — the total fare, food cost and food delivery
  fee are bold/prominent in the fare breakdown (rider map and driver ride
  card); the ride-distance cost stays in the regular detailed breakdown.
- **Demo admin account** — `seed_db.py` now also seeds `admin@test.com`
  (password `password123`) with `is_admin` set, so a freshly seeded database
  always has a working admin login out of the box.

## Configuration

All configuration is environment-driven — see `backend/.env.example` for
the full list (database, Redis, JWT secret, CORS origins, rate limits,
matching radius, OSRM endpoint, etc.). Nothing sensitive is hardcoded in
source.

## API overview

| Endpoint | Method | Description |
|---|---|---|
| `/api/signup` | POST | Create a rider or driver account, returns a JWT |
| `/api/login` | POST | Authenticate, returns a JWT |
| `/api/menu` | POST | Look up a restaurant's menu by name |
| `/api/health` | GET | Postgres/Redis dependency health check |
| `/api/admin/overview` | GET | Admin-only operational overview (`Authorization: Bearer <admin JWT>`) |

Key Socket.IO events: `driver_online`, `driver_heartbeat`, `driver_status`,
`update_location`, `request_ride`, `ride_requested`, `driver_request`,
`driver_response`, `ride_accept_failed`, `offer_expired`, `ride_assigned`,
`ride_state` (sent on reconnect), `ride_status_update`, `ride_progress`,
`payment_collected`, `payment_failed`, `earnings_update`. All connections
authenticate via a JWT passed in the `auth` payload at connect time.

## Possible next steps

- PostGIS for exact-radius geo queries as an alternative to the SQL
  bounding-box fallback.
- A background worker (Celery/RQ) for anything that shouldn't block a
  socket handler, e.g. receipt emails or analytics events.
- Structured request logging / distributed tracing across the REST +
  socket + Redis + Postgres hops.
- Refresh tokens instead of a single long-lived JWT.

## License

MIT
