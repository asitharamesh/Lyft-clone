import re

from flask import Blueprint, jsonify, request
from psycopg2 import errors as pg_errors

from config import Config
from db import get_db_conn
from extensions import limiter
from services.auth_service import hash_password, issue_token, verify_password

auth_bp = Blueprint("auth", __name__, url_prefix="/api")

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _error(message, status=400):
    return jsonify({"success": False, "message": message}), status


def _validate_credentials(data):
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""
    if not EMAIL_RE.match(email):
        return None, None, "Invalid email address"
    if len(password) < 8:
        return None, None, "Password must be at least 8 characters"
    return email, password, None


@auth_bp.route("/signup", methods=["POST"])
@limiter.limit(Config.SIGNUP_RATE_LIMIT)
def signup():
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    user_type = data.get("type", "rider")

    email, password, err = _validate_credentials(data)
    if err:
        return _error(err)
    if not name:
        return _error("Name is required")
    if user_type not in ("rider", "driver"):
        return _error("type must be 'rider' or 'driver'")

    password_hash = hash_password(password)

    try:
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                if user_type == "driver":
                    cur.execute(
                        """
                        INSERT INTO drivers
                            (name, email, password, license_plate, latitude, longitude, is_available, earnings, rating)
                        VALUES (%s, %s, %s, 'TEMP-PLATE', 12.9716, 77.5946, FALSE, 0.00, 4.8)
                        RETURNING id
                        """,
                        (name, email, password_hash),
                    )
                else:
                    cur.execute(
                        "INSERT INTO users (name, email, password) VALUES (%s, %s, %s) RETURNING id",
                        (name, email, password_hash),
                    )
                new_id = cur.fetchone()[0]
            conn.commit()
    except pg_errors.UniqueViolation:
        return _error("An account with that email already exists", 409)
    except Exception:
        return _error("Could not create account", 500)

    token = issue_token(new_id, user_type, name)
    return jsonify({
        "success": True,
        "token": token,
        "user": {"id": new_id, "name": name, "email": email, "earnings": 0},
    })


@auth_bp.route("/login", methods=["POST"])
@limiter.limit(Config.LOGIN_RATE_LIMIT)
def login():
    data = request.get_json(silent=True) or {}
    user_type = data.get("type", "rider")
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    if not email or not password:
        return _error("Email and password are required")
    # `type` becomes the token's role claim, so it must never be free text.
    if user_type not in ("rider", "driver", "admin"):
        return _error("type must be 'rider', 'driver' or 'admin'")

    if user_type == "driver":
        query = "SELECT id, name, email, password, earnings FROM drivers WHERE email = %s"
    elif user_type == "admin":
        # Admin is a flag on an existing users row, set only via set_admin.py
        # (never from a request payload) and re-checked on every admin call.
        query = "SELECT id, name, email, password FROM users WHERE email = %s AND is_admin = TRUE"
    else:
        query = "SELECT id, name, email, password FROM users WHERE email = %s"

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (email,))
            row = cur.fetchone()

    # Same generic error whether the email doesn't exist or the password is
    # wrong, and verify_password still runs bcrypt either way (constant
    # shape response) so we don't leak which emails are registered via
    # response-time/content differences.
    if not row or not verify_password(password, row[3]):
        return _error("Invalid email or password", 401)

    if user_type == "driver":
        user_data = {"id": row[0], "name": row[1], "email": row[2], "earnings": float(row[4])}
    else:
        user_data = {"id": row[0], "name": row[1], "email": row[2]}

    token = issue_token(row[0], user_type, row[1])
    return jsonify({"success": True, "token": token, "user": user_data})
