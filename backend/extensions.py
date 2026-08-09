"""
Shared extension instances, created here (not in app.py) so blueprints and
socket handlers can import them without circular imports.
"""
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_socketio import SocketIO

from config import Config

# async_mode="eventlet" lets a single worker handle thousands of concurrent
# websocket connections cooperatively instead of one-OS-thread-per-client.
#
# message_queue=Config.REDIS_URL is what makes this horizontally scalable:
# without it, `emit(..., room=...)` only reaches clients connected to the
# *same* process, so running more than one backend worker/instance would
# silently break room- and broadcast-based delivery. With it, all workers
# publish/subscribe through Redis, so it doesn't matter which instance a
# given client's websocket landed on.
socketio = SocketIO(
    cors_allowed_origins=Config.CORS_ORIGINS,
    async_mode="eventlet",
    message_queue=Config.REDIS_URL,
)

limiter = Limiter(
    key_func=get_remote_address,
    storage_uri=Config.RATELIMIT_STORAGE_URI,
    default_limits=[],
)
