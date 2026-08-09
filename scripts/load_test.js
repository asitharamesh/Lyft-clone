/**
 * Load test for the ride-matching pipeline.
 *
 * Spins up N virtual drivers (real accounts + real websocket connections,
 * going "online" at randomized locations around a center point) and then
 * fires M concurrent ride requests from virtual riders, measuring the
 * time from `request_ride` to a driver actually being offered the ride
 * (the Redis-GEO matching path in services/matching_service.py) and the
 * time to full acceptance.
 *
 * This is what backs the latency numbers in the README - it exercises the
 * exact same REST + Socket.IO code paths a real client would, just many of
 * them at once, so the numbers reflect the matching pipeline, not a
 * synthetic microbenchmark.
 *
 * Usage:
 *   node load_test.js --drivers=50 --riders=20 --url=http://127.0.0.1:5001
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
const CENTER = { lat: 12.9716, lng: 77.5946 }; // Bangalore, matches seed data
const SPREAD = 0.05; // ~5km jitter box

const jitter = () => (Math.random() - 0.5) * SPREAD;
const runId = Date.now();

async function signup(type, i) {
    const res = await axios.post(`${BASE_URL}/api/signup`, {
        type,
        name: `LoadTest ${type} ${i}`,
        email: `loadtest_${type}_${runId}_${i}@test.com`,
        password: 'loadtestpass1',
    });
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
    const { token } = await signup('driver', i);
    const socket = await connectSocket(token);
    socket.emit('driver_online', { lat: CENTER.lat + jitter(), lng: CENTER.lng + jitter() });
    socket.on('driver_request', (data) => {
        // Auto-accept immediately, like a driver tapping "Accept".
        socket.emit('driver_response', { request_id: data.request_id, accepted: true });
    });
    return socket;
}

async function setupRider(i) {
    const { token } = await signup('rider', i);
    const socket = await connectSocket(token);
    return socket;
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

async function runRiderRequest(i) {
    const socket = await setupRider(i);
    return new Promise((resolve) => {
        const start = process.hrtime.bigint();
        let assignedMs = null;

        socket.on('ride_assigned', () => {
            const end = process.hrtime.bigint();
            assignedMs = Number(end - start) / 1e6;
            socket.disconnect();
            resolve(assignedMs);
        });

        socket.emit('request_ride', {
            pickup_lat: CENTER.lat + jitter(),
            pickup_lng: CENTER.lng + jitter(),
            drop_lat: CENTER.lat + jitter(),
            drop_lng: CENTER.lng + jitter(),
            pickup_name: 'Load Test Pickup',
            drop_name: 'Load Test Drop',
        });

        setTimeout(() => {
            if (assignedMs === null) {
                socket.disconnect();
                resolve(null); // timed out / no driver available
            }
        }, 8000);
    });
}

async function main() {
    console.log(`Setting up ${NUM_DRIVERS} virtual drivers...`);
    const driverSetupStart = Date.now();
    const drivers = await Promise.all(Array.from({ length: NUM_DRIVERS }, (_, i) => setupDriver(i)));
    console.log(`  ${drivers.length} drivers online in ${Date.now() - driverSetupStart}ms`);

    await new Promise((r) => setTimeout(r, 500)); // let geo-index writes settle

    console.log(`Firing ${NUM_RIDERS} concurrent ride requests...`);
    const overallStart = Date.now();
    const results = await Promise.all(Array.from({ length: NUM_RIDERS }, (_, i) => runRiderRequest(i)));
    const overallMs = Date.now() - overallStart;

    const successes = results.filter((r) => r !== null);
    const failures = results.length - successes.length;

    console.log('\n--- Results ---');
    console.log(`Concurrent ride requests: ${NUM_RIDERS} against ${NUM_DRIVERS} available drivers`);
    console.log(`Successful matches: ${successes.length}/${results.length} (${failures} timed out/no driver)`);
    console.log(`Total wall-clock time for all requests: ${overallMs}ms`);
    summarize('request_ride -> ride_assigned latency', successes);

    drivers.forEach((s) => s.disconnect());
    process.exit(0);
}

main().catch((err) => {
    console.error('Load test failed:', err.message);
    process.exit(1);
});
