"""Grant or revoke the admin role for an existing rider account.

Usage:
  python set_admin.py user@example.com           # grant
  python set_admin.py user@example.com --revoke  # revoke

Admin access is never granted through signup or login payloads. It can only
be set here, by someone with database access, and every admin API call
re-checks the flag in the database, so revoking takes effect immediately.
The admin then signs in on frontend/admin.html with that account's password.
"""
import sys

from init_db import connect


def main(argv):
    args = [a for a in argv if not a.startswith("--")]
    if len(args) != 1:
        print(__doc__)
        return 2
    email = args[0].strip().lower()
    make_admin = "--revoke" not in argv

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET is_admin = %s WHERE email = %s RETURNING id", (make_admin, email))
            row = cur.fetchone()
        conn.commit()
    finally:
        conn.close()

    if not row:
        print(f"No rider account with email {email}")
        return 1
    print(f"{'Granted' if make_admin else 'Revoked'} admin for user id {row[0]} ({email})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
