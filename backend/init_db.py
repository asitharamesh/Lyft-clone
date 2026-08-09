"""Run schema.sql against the configured database. Usage: python init_db.py"""
import os

import psycopg2

from config import Config


def run_schema():
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    if not os.path.exists(schema_path):
        print(f"Error: '{schema_path}' not found.")
        return

    conn = psycopg2.connect(
        host=Config.DB_HOST,
        port=Config.DB_PORT,
        dbname=Config.DB_NAME,
        user=Config.DB_USER,
        password=Config.DB_PASSWORD,
    )
    try:
        with open(schema_path) as f:
            schema_sql = f.read()
        with conn.cursor() as cur:
            cur.execute(schema_sql)
        conn.commit()
        print("Database tables created successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    run_schema()
