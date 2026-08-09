"""
Auth: password hashing (bcrypt, salted, adaptive cost) + stateless JWT
session tokens.

Why bcrypt: it's a slow, salt-built-in, adaptive hash function designed
specifically for passwords (unlike SHA-256/MD5, which are fast general
hashes and unsuitable for password storage because they're brute-forceable
at billions of guesses/sec on GPUs). The original project stored passwords
in plaintext - this is the single most important fix for interview
credibility.
"""
import datetime

import bcrypt
import jwt

from config import Config


def hash_password(plain_password: str) -> str:
    salt = bcrypt.gensalt(rounds=Config.BCRYPT_ROUNDS)
    return bcrypt.hashpw(plain_password.encode("utf-8"), salt).decode("utf-8")


def verify_password(plain_password: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain_password.encode("utf-8"), hashed.encode("utf-8"))
    except (ValueError, TypeError):
        # Covers legacy/plaintext rows if migrating an old DB - treat as mismatch.
        return False


def issue_token(user_id: int, role: str, name: str) -> str:
    """role is 'rider' or 'driver'. Returned token is what the client stores
    (e.g. localStorage) and sends back as `Authorization: Bearer <token>` on
    REST calls, and in the `auth` payload on socket.io connect."""
    now = datetime.datetime.utcnow()
    payload = {
        "sub": str(user_id),
        "role": role,
        "name": name,
        "iat": now,
        "exp": now + Config.JWT_EXPIRY,
    }
    return jwt.encode(payload, Config.JWT_SECRET, algorithm=Config.JWT_ALGORITHM)


def decode_token(token: str) -> dict | None:
    try:
        return jwt.decode(token, Config.JWT_SECRET, algorithms=[Config.JWT_ALGORITHM])
    except jwt.PyJWTError:
        return None
