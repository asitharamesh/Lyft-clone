# Lyft Clone — Real-Time Ride-Sharing & Food Delivery Platform

A full-stack simulation of a ride-sharing / food-delivery platform (think
Lyft + DoorDash): real-time driver dispatch, live GPS tracking over the
actual road network, and a rider/driver web app, backed by PostgreSQL and
Redis.

## Features

- **Rider app** — request a ride or food delivery on an interactive map,
  track your assigned driver in real time, pay in-app.
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
5. The ride-simulation engine drives the accepted ride along the real road
   route and streams back status/location, which the backend relays into
   that same room.

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

Once the backend is healthy, seed some demo data:

```bash
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

python init_db.py             # create tables
python seed_db.py             # demo accounts + restaurants (password: password123)

python app.py                  # backend on http://127.0.0.1:5001
```

In separate terminals:

```bash
# static frontend
python3 -m http.server 8000 --directory frontend

# ride simulation engine (animates accepted rides along real roads)
cd backend && npm install && node ride_simulation_engine.js
```

Open `http://localhost:8000/login.html`.

### Demo accounts

Seeded by `seed_db.py`, all with password `password123`:

- Riders: `user0@test.com` … `user4@test.com`
- Drivers: `driver0@test.com` … `driver2@test.com`

### Food delivery mode

The rider map's "Food Delivery" mode looks up a restaurant/menu by name via
`/api/menu`. Try `Meghana Foods`, `Truffles`, `CTR`, `Empire`, or `Corner
House` (seeded in `seed_db.py`).

## Testing

```bash
cd backend
pytest tests/test_pricing.py tests/test_routing.py tests/test_matching.py -v
```

For a full live check against a running backend (real Postgres/Redis, real
JWT-authenticated sockets), see `backend/tests/manual_socket_flow_check.py`.

### Load testing

`scripts/load_test.js` brings a fleet of real driver accounts online and
fires concurrent ride requests through the actual matching pipeline,
reporting p50/p95 latency for `request_ride → ride_assigned`:

```bash
cd scripts
npm install
node load_test.js --drivers=50 --riders=20 --url=http://127.0.0.1:5001
```

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

Key Socket.IO events: `driver_online`, `update_location`, `request_ride`,
`driver_request`, `driver_response`, `ride_assigned`, `ride_status_update`,
`ride_progress`, `payment_collected`, `earnings_update`. All connections
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
