"""Create or upgrade the database schema.

Usage:
  python init_db.py            # fresh install: runs schema.sql (DROPS and recreates
                               # all tables), then every migrations/*.sql file
  python init_db.py --migrate  # existing database: only applies migrations/*.sql
                               # (idempotent, keeps existing data)
"""
import glob
import os
import sys

import psycopg2

from config import Config

BASE_DIR = os.path.dirname(__file__)


def connect():
    return psycopg2.connect(
        host=Config.DB_HOST,
        port=Config.DB_PORT,
        dbname=Config.DB_NAME,
        user=Config.DB_USER,
        password=Config.DB_PASSWORD,
    )


def run_schema(conn):
    schema_path = os.path.join(BASE_DIR, "schema.sql")
    with open(schema_path) as f:
        schema_sql = f.read()
    with conn.cursor() as cur:
        cur.execute(schema_sql)
    conn.commit()
    print("Database tables created successfully.")


def run_migrations(conn):
    for path in sorted(glob.glob(os.path.join(BASE_DIR, "migrations", "*.sql"))):
        with open(path) as f:
            sql = f.read()
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print(f"Applied migration {os.path.basename(path)}")


if __name__ == "__main__":
    conn = connect()
    try:
        if "--migrate" not in sys.argv[1:]:
            run_schema(conn)
        run_migrations(conn)
    finally:
        conn.close()
