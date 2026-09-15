"""Admin authorization is enforced server-side; roles can't be self-assigned.
Unit tests: no database or Redis needed."""
import datetime
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import jwt
import pytest
from flask import Flask

from config import Config
from routes import admin_routes, auth_routes
from services.auth_service import decode_token, issue_token


@pytest.fixture
def admin_client():
    app = Flask(__name__)
    app.register_blueprint(admin_routes.admin_bp)
    with patch.object(admin_routes, "_section", lambda builder: {"ok": True}):
        yield app.test_client()


def _get(client, token=None, raw_header=None):
    headers = {}
    if raw_header is not None:
        headers["Authorization"] = raw_header
    elif token:
        headers["Authorization"] = f"Bearer {token}"
    return client.get("/api/admin/overview", headers=headers)


def test_unauthenticated_request_is_rejected(admin_client):
    with patch.object(admin_routes, "_is_admin_in_db") as db_check:
        assert _get(admin_client).status_code == 401
        assert _get(admin_client, raw_header="Bearer not-a-jwt").status_code == 401
        assert _get(admin_client, raw_header="Basic abc").status_code == 401
        db_check.assert_not_called()


def test_expired_or_forged_admin_tokens_are_rejected(admin_client):
    now = datetime.datetime.utcnow()
    expired = jwt.encode(
        {"sub": "1", "role": "admin", "name": "x", "iat": now - datetime.timedelta(hours=2), "exp": now - datetime.timedelta(hours=1)},
        Config.JWT_SECRET, algorithm="HS256",
    )
    forged = jwt.encode({"sub": "1", "role": "admin", "name": "x", "exp": now + datetime.timedelta(hours=1)}, "wrong-secret", algorithm="HS256")
    with patch.object(admin_routes, "_is_admin_in_db", return_value=True):
        assert _get(admin_client, expired).status_code == 401
        assert _get(admin_client, forged).status_code == 401


@pytest.mark.parametrize("role", ["rider", "driver", "service", ""])
def test_non_admin_roles_are_forbidden(admin_client, role):
    with patch.object(admin_routes, "_is_admin_in_db", return_value=True) as db_check:
        response = _get(admin_client, issue_token(1, role, "Someone"))
    assert response.status_code == 403
    db_check.assert_not_called()  # a valid rider/driver token never reaches the DB check


def test_admin_role_claim_alone_is_not_enough(admin_client):
    # e.g. the flag was revoked after the token was issued
    with patch.object(admin_routes, "_is_admin_in_db", return_value=False) as db_check:
        assert _get(admin_client, issue_token(3, "admin", "Former admin")).status_code == 403
    db_check.assert_called_once_with(3)


def test_admin_check_fails_closed_on_database_error(admin_client):
    with patch.object(admin_routes, "_is_admin_in_db", side_effect=RuntimeError("db down")):
        assert _get(admin_client, issue_token(3, "admin", "Admin")).status_code == 403


def test_admin_gets_overview(admin_client):
    with patch.object(admin_routes, "_is_admin_in_db", return_value=True):
        response = _get(admin_client, issue_token(3, "admin", "Admin"))
    assert response.status_code == 200
    body = response.get_json()
    assert body["success"] is True
    assert set(body) >= {"system", "users", "matching", "rides", "offers", "redis", "postgres", "payments"}


# --- login / signup can't grant admin ------------------------------------------

class FakeCursor:
    def __init__(self, row, executed):
        self.row, self.executed = row, executed

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self.row


def _fake_db(row):
    executed = []
    conn = MagicMock()
    conn.cursor.side_effect = lambda: FakeCursor(row, executed)

    @contextmanager
    def fake_get_db_conn():
        yield conn

    return fake_get_db_conn, executed


@pytest.fixture
def auth_client():
    app = Flask(__name__)
    app.register_blueprint(auth_routes.auth_bp)
    return app.test_client()


def test_login_rejects_unknown_type(auth_client):
    response = auth_client.post("/api/login", json={"type": "superuser", "email": "a@b.co", "password": "password123"})
    assert response.status_code == 400


def test_admin_login_requires_admin_flag_in_database(auth_client):
    fake, executed = _fake_db(row=None)
    with patch.object(auth_routes, "get_db_conn", fake):
        response = auth_client.post("/api/login", json={"type": "admin", "email": "rider@test.com", "password": "password123"})
    assert response.status_code == 401
    assert "is_admin = TRUE" in executed[0][0]


def test_admin_login_issues_admin_token_only_for_flagged_user(auth_client):
    fake, _ = _fake_db(row=(3, "Admin", "admin@test.com", "hash"))
    with patch.object(auth_routes, "get_db_conn", fake), patch.object(auth_routes, "verify_password", return_value=True):
        response = auth_client.post("/api/login", json={"type": "admin", "email": "admin@test.com", "password": "password123"})
    assert response.status_code == 200
    assert decode_token(response.get_json()["token"])["role"] == "admin"


def test_rider_login_yields_rider_role(auth_client):
    fake, executed = _fake_db(row=(3, "Admin", "admin@test.com", "hash"))
    with patch.object(auth_routes, "get_db_conn", fake), patch.object(auth_routes, "verify_password", return_value=True):
        response = auth_client.post("/api/login", json={"type": "rider", "email": "admin@test.com", "password": "password123", "is_admin": True})
    assert decode_token(response.get_json()["token"])["role"] == "rider"
    assert "is_admin" not in executed[0][0]


def test_signup_cannot_create_admins(auth_client):
    fake, executed = _fake_db(row=(9,))
    with patch.object(auth_routes, "get_db_conn", fake), patch.object(auth_routes, "hash_password", return_value="hash"):
        refused = auth_client.post("/api/signup", json={"type": "admin", "name": "M", "email": "m@test.com", "password": "password123"})
        created = auth_client.post(
            "/api/signup", json={"type": "rider", "name": "M", "email": "m@test.com", "password": "password123", "is_admin": True, "role": "admin"}
        )
    assert refused.status_code == 400
    assert created.status_code == 200
    assert decode_token(created.get_json()["token"])["role"] == "rider"
    assert all("is_admin" not in sql for sql, _ in executed)
