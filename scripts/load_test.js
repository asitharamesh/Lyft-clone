/**
 * Load test for the ride-dispatch pipeline.
 *
 * Brings N virtual drivers online (real accounts + real websocket
 * connections at randomized locations around a center point) and fires M
 * concurrent ride requests from virtual riders through the exact REST +
 * Socket.IO code paths a real client uses. Virtual drivers accept every
 * offer, keep their heartbeat alive, and confirm payment when a ride
 * completes.
 *
 * Every stage is counted separately, so "no driver available" is never
 * reported as a successful match:
 *   request acknowledged -> ride created -> driver offered -> driver accepted
 *   -> ride completed -> payment confirmed
 *
 * Rate limits: the script creates drivers + riders accounts from one IP,
 * which the normal signup limit (5/minute) rejects. Run the backend with
 * LOAD_TEST_MODE=true for a controlled load test (the backend refuses that
 * setting when FLASK_ENV=production).
 *
 * No results are committed with this script; numbers depend on the machine,
 * configuration, simulator mode and OSRM availability.
 *
 * Usage:
 *   node load_test.js --drivers=50 --riders=20 --url=http://127.0.0.1:5001 [--timeout=180]
 *   --timeout: seconds to wait per ride for it to finish (the default
 *   in-process simulator takes tens of seconds per ride).
 */
const axios = require('axios');
const io = require('socket.io-client');

const args = Object.fromEntries(
    process.argv.slice(2).map((a) => {
        const [k, v] = a.replace(/^--/, '').split('=');
        return [k, v];
    })
);

const BASE_URL = args.url || process.env.BACKEND_URL || 'http://127.0.0.1:5001';
const NUM_DRIVERS = parseInt(args.drivers || '50', 10);
const NUM_RIDERS = parseInt(args.riders || '20', 10);
const TIMEOUT_MS = parseInt(args.timeout || '180', 10) * 1000;
const CENTER = { lat: 12.9716, lng: 77.5946 }; // Bangalore, matches seed data
const SPREAD = 0.05; // ~5km jitter box

const jitter = () => (Math.random() - 0.5) * SPREAD;
const runId = Date.now();
const now = () => Number(process.hrtime.bigint()) / 1e6;

// Driver-side observations, keyed by request id.
const firstOfferAt = new Map();
const assignedDrivers = new Map(); // request_id -> Set of driver ids told they won it
const driverCounters = { offers: 0, acceptRefused: 0, offersExpired: 0, paymentsConfirmed: 0, paymentsRefused: 0 };

async function signup(type, i) {
    let res;
    try {
        res = await axios.post(`${BASE_URL}/api/signup`, {
            type,
            name: `LoadTest ${type} ${i}`,
            email: `loadtest_${type}_${runId}_${i}@test.com`,
            password: 'loadtestpass1',
        });
    } catch (err) {
        if (err.response && err.response.status === 429) {
            throw new Error(
                'signup was rate limited (HTTP 429). Restart the backend with LOAD_TEST_MODE=true ' +
                'for a controlled load test (never in production).'
            );
        }
        throw err;
    }
    if (!res.data.success) throw new Error(`signup failed: ${JSON.stringify(res.data)}`);
    return res.data;
}

function connectSocket(token) {
    return new Promise((resolve, reject) => {
        const socket = io(BASE_URL, { transports: ['websocket'], auth: { token } });
        socket.on('connect', () => resolve(socket));
        socket.on('connect_error', reject);
    });
}

async function setupDriver(i) {
    const { token, user } = await signup('driver', i);
    const socket = await connectSocket(token);
    const position = { lat: CENTER.lat + jitter(), lng: CENTER.lng + jitter() };
    let heartbeat = null;

    socket.on('driver_status', (status) => {
        if (status.online && !heartbeat) {
            const seconds = status.heartbeat_interval_seconds || 30;
            heartbeat = setInterval(() => socket.emit('driver_heartbeat', position), seconds * 1000);
        }
    });
    socket.on('driver_request', (data) => {
        driverCounters.offers++;
        if (!firstOfferAt.has(data.request_id)) firstOfferAt.set(data.request_id, now());
        // Auto-accept immediately, like a driver tapping "Accept".
        socket.emit('driver_response', { request_id: data.request_id, accepted: true });
    });
    socket.on('ride_assigned', (data) => {
        if (data.success && data.driver && data.driver.id === user.id) {
            if (!assignedDrivers.has(data.request_id)) assignedDrivers.set(data.request_id, new Set());
            assignedDrivers.get(data.request_id).add(user.id);
        }
    });
    socket.on('ride_accept_failed', () => driverCounters.acceptRefused++);
    socket.on('offer_expired', () => driverCounters.offersExpired++);
    socket.on('ride_progress', (data) => {
        if (data.status === 'completed') {
            socket.emit('payment_collected', { driver_id: user.id, request_id: data.request_id, amount: data.fare });
        }
    });
    socket.on('earnings_update', () => driverCounters.paymentsConfirmed++);
    socket.on('payment_failed', () => driverCounters.paymentsRefused++);

    socket.emit('driver_online', position);
    return {
        stop() {
            clearInterval(heartbeat);
            socket.disconnect();
        },
    };
}

async function setupRider(i) {
    const { token } = await signup('rider', i);
    return connectSocket(token);
}

function percentile(sorted, p) {
    const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
    return sorted[idx];
}

function summarize(label, samples) {
    if (samples.length === 0) {
        console.log(`${label}: no samples`);
        return;
    }
    const sorted = [...samples].sort((a, b) => a - b);
    const sum = sorted.reduce((a, b) => a + b, 0);
    console.log(
        `${label}: n=${sorted.length} min=${sorted[0].toFixed(1)}ms ` +
        `p50=${percentile(sorted, 50).toFixed(1)}ms ` +
        `p95=${percentile(sorted, 95).toFixed(1)}ms ` +
        `max=${sorted[sorted.length - 1].toFixed(1)}ms ` +
        `mean=${(sum / sorted.length).toFixed(1)}ms`
    );
}

function runRiderRequest(socket) {
    return new Promise((resolve) => {
        const r = {
            startedAt: now(), requestId: null, acknowledged: false, created: false, accepted: false,
            noDriver: false, completed: false, paid: false, timedOut: false, failure: null, t: {},
        };
        let done = false;
        const finish = () => {
            if (done) return;
            done = true;
            clearTimeout(timer);
            socket.disconnect();
            resolve(r);
        };
        const elapsed = () => now() - r.startedAt;

        socket.on('ride_assigned', (data) => {
            if (data.success) {
                r.accepted = true;
                r.t.accepted = elapsed();
            } else {
                // Server-side refusal or no driver available: never counted as a match.
                if (r.created) r.noDriver = true;
                r.failure = data.message || 'ride_assigned success=false';
                finish();
            }
        });
        socket.on('ride_progress', (data) => {
            if (data.status === 'completed') {
                r.completed = true;
                r.t.completed = elapsed();
            }
        });
        socket.on('payment_confirmed', () => {
            r.paid = true;
            r.t.paid = elapsed();
            finish();
        });

        socket.emit('request_ride', {
            pickup_lat: CENTER.lat + jitter(),
            pickup_lng: CENTER.lng + jitter(),
            drop_lat: CENTER.lat + jitter(),
            drop_lng: CENTER.lng + jitter(),
            pickup_name: 'Load Test Pickup',
            drop_name: 'Load Test Drop',
        }, (ack) => {
            r.acknowledged = true;
            r.t.ack = elapsed();
            if (ack && ack.success && ack.request_id) {
                r.created = true;
                r.requestId = ack.request_id;
            } else {
                r.failure = (ack && ack.message) || 'request refused';
                finish();
            }
        });

        const timer = setTimeout(() => {
            r.timedOut = true;
            finish();
        }, TIMEOUT_MS);
    });
}

async function main() {
    console.log(`Setting up ${NUM_DRIVERS} virtual drivers...`);
    const driverSetupStart = Date.now();
    const drivers = await Promise.all(Array.from({ length: NUM_DRIVERS }, (_, i) => setupDriver(i)));
    console.log(`  ${drivers.length} drivers connected in ${Date.now() - driverSetupStart}ms`);

    console.log(`Setting up ${NUM_RIDERS} virtual riders...`);
    const riders = await Promise.all(Array.from({ length: NUM_RIDERS }, (_, i) => setupRider(i)));

    await new Promise((r) => setTimeout(r, 500)); // let geo-index writes settle

    console.log(`Firing ${NUM_RIDERS} concurrent ride requests (waiting up to ${TIMEOUT_MS / 1000}s per ride)...`);
    const overallStart = Date.now();
    const results = await Promise.all(riders.map((socket) => runRiderRequest(socket)));
    const overallMs = Date.now() - overallStart;

    const count = (pred) => results.filter(pred).length;
    const n = results.length;
    const doubleAssigned = [...assignedDrivers.values()].filter((ids) => ids.size > 1).length;
    const failures = {};
    results.filter((r) => r.failure && !r.noDriver).forEach((r) => {
        failures[r.failure] = (failures[r.failure] || 0) + 1;
    });

    console.log('\n--- Results ---');
    console.log(`Concurrent ride requests: ${n}, virtual drivers: ${NUM_DRIVERS}`);
    console.log(`Request acknowledged by server:   ${count((r) => r.acknowledged)}/${n}`);
    console.log(`Ride created (persisted):          ${count((r) => r.created)}/${n}`);
    console.log(`Driver offered the ride:           ${count((r) => r.requestId && firstOfferAt.has(r.requestId))}/${n}`);
    console.log(`Driver accepted (ride assigned):   ${count((r) => r.accepted)}/${n}`);
    console.log(`No driver available:               ${count((r) => r.noDriver)}/${n}`);
    console.log(`Ride completed:                    ${count((r) => r.completed)}/${n}`);
    console.log(`Payment confirmed:                 ${count((r) => r.paid)}/${n}`);
    console.log(`Timed out:                         ${count((r) => r.timedOut)}/${n}`);
    Object.entries(failures).forEach(([message, c]) => console.log(`Other failure "${message}": ${c}`));
    console.log(
        `Driver side: offers=${driverCounters.offers} accepts_refused=${driverCounters.acceptRefused} ` +
        `offers_expired=${driverCounters.offersExpired} payments_confirmed=${driverCounters.paymentsConfirmed} ` +
        `payments_refused=${driverCounters.paymentsRefused}`
    );
    console.log(`Rides assigned to more than one driver: ${doubleAssigned}`);
    console.log(`Total wall-clock time: ${overallMs}ms`);

    summarize('request_ride -> ride created (ack)', results.filter((r) => r.created).map((r) => r.t.ack));
    summarize(
        'request_ride -> first driver offer',
        results.filter((r) => r.requestId && firstOfferAt.has(r.requestId)).map((r) => firstOfferAt.get(r.requestId) - r.startedAt)
    );
    summarize('request_ride -> driver accepted', results.filter((r) => r.accepted).map((r) => r.t.accepted));
    summarize('request_ride -> ride completed', results.filter((r) => r.completed).map((r) => r.t.completed));

    drivers.forEach((d) => d.stop());
    process.exit(0);
}

main().catch((err) => {
    console.error('Load test failed:', err.message);
    process.exit(1);
});
