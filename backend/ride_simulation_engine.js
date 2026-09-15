/**
 * Ride simulation engine.
 *
 * This is an internal, trusted backend service - not a browser client. Once
 * a ride is accepted, the Flask backend emits `start_simulation_ride` to
 * this service (only), and this service animates the driver's position
 * along the *real road route* (via OSRM, the same routing engine used to
 * draw the route on the map) and reports progress back over the same
 * socket, which the backend then relays to the two ride participants.
 *
 * It authenticates with a separate internal service token (see
 * SIMULATION_SERVICE_TOKEN in the backend's config), not a per-user JWT -
 * that's what lets it report status/location "on behalf of" any driver_id,
 * something a real driver's own browser session is deliberately NOT allowed
 * to do (see sockets/handlers.py `_is_driver_or_trusted_service`).
 */
const io = require('socket.io-client');

const SERVER_URL = process.env.BACKEND_URL || 'http://127.0.0.1:5001';
const SERVICE_TOKEN = process.env.SIMULATION_SERVICE_TOKEN || 'dev-sim-token-change-me';
const OSRM_BASE_URL = process.env.OSRM_BASE_URL || 'https://router.project-osrm.org';

const socket = io(SERVER_URL, {
    transports: ['websocket'],
    reconnection: true,
    auth: { service_token: SERVICE_TOKEN },
});

// The backend only accepts this engine when it runs with RIDE_SIMULATOR=node,
// so it can never animate rides alongside the in-process Python simulator.
socket.on('connect_error', (err) => {
    console.error('Simulation engine connection refused by backend:', err.message);
});

console.log('Ride simulation engine connected, waiting for accepted rides...');

const drivers = new Map(); // driver_id -> driver state

async function getRoadPath(start, end) {
    try {
        const url = `${OSRM_BASE_URL}/route/v1/driving/${start.lng},${start.lat};${end.lng},${end.lat}?overview=full&geometries=geojson`;
        const res = await fetch(url);
        const data = await res.json();
        if (data.routes && data.routes[0]) {
            return data.routes[0].geometry.coordinates.map((c) => ({ lat: c[1], lng: c[0] }));
        }
    } catch (e) {
        console.error('OSRM routing error:', e.message);
    }
    return null;
}

socket.on('start_simulation_ride', async (data) => {
    let driver = drivers.get(data.driver_id);
    if (!driver) {
        driver = {
            id: data.driver_id,
            lat: data.current_loc.lat,
            lng: data.current_loc.lng,
            state: 'IDLE',
            path: [],
            pathIndex: 0,
        };
        drivers.set(data.driver_id, driver);
    }

    driver.requestId = data.request_id;
    driver.hasFood = false; // per ride - otherwise a later non-food ride reports food_picked
    driver.missionQueue = [];
    if (data.restaurant) {
        driver.missionQueue.push({ type: 'TO_RESTAURANT', target: data.restaurant });
        driver.missionQueue.push({ type: 'TO_PICKUP', target: data.pickup });
        driver.missionQueue.push({ type: 'TO_DROP', target: data.drop });
    } else {
        driver.missionQueue.push({ type: 'TO_PICKUP', target: data.pickup });
        driver.missionQueue.push({ type: 'TO_DROP', target: data.drop });
    }

    startNextLeg(driver);
});

function emitStatus(driver, status) {
    socket.emit('ride_status_update', {
        driver_id: driver.id,
        request_id: driver.requestId,
        status,
    });
}

async function startNextLeg(driver) {
    if (driver.missionQueue.length === 0) {
        driver.state = 'IDLE';
        emitStatus(driver, 'completed');
        return;
    }

    const leg = driver.missionQueue.shift();
    driver.state = leg.type;

    let statusMsg = '';
    // Same step names as the in-process simulator; the backend only accepts
    // forward progress through these (see services/ride_service.py).
    if (leg.type === 'TO_RESTAURANT') statusMsg = 'heading_to_pickup';
    if (leg.type === 'TO_PICKUP') statusMsg = driver.hasFood ? 'food_picked' : 'heading_to_pickup';
    if (leg.type === 'TO_DROP') statusMsg = 'trip_started';

    emitStatus(driver, statusMsg);

    const path = await getRoadPath({ lat: driver.lat, lng: driver.lng }, leg.target);
    if (path) {
        driver.path = path;
        driver.pathIndex = 0;
    } else {
        driver.path = [];
    }
}

function handleArrival(driver) {
    driver.path = [];
    if (driver.state === 'TO_RESTAURANT') {
        driver.state = 'WAITING';
        driver.hasFood = true;
        emitStatus(driver, 'at_restaurant');
        setTimeout(() => startNextLeg(driver), 5000);
    } else if (driver.state === 'TO_PICKUP') {
        driver.state = 'WAITING';
        emitStatus(driver, 'picked_up');
        setTimeout(() => startNextLeg(driver), 3000);
    } else if (driver.state === 'TO_DROP') {
        startNextLeg(driver);
    }
}

setInterval(() => {
    drivers.forEach((driver) => {
        if (driver.state === 'IDLE' || driver.state === 'WAITING') {
            return; // no movement, no need to spam location updates either
        }
        if (driver.path.length > 0) {
            if (driver.pathIndex < driver.path.length) {
                const point = driver.path[driver.pathIndex];
                driver.lat = point.lat;
                driver.lng = point.lng;
                driver.pathIndex += 4; // playback speed multiplier
            } else {
                handleArrival(driver);
            }
        }
        socket.emit('update_location', { driver_id: driver.id, latitude: driver.lat, longitude: driver.lng });
    });
}, 500);
