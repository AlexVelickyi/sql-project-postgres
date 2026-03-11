from __future__ import annotations

import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

from init_sql_project import DB_NAME, SCHEMA_NAME


TARGET_TABLES = [
    "stg_transactions",
    "stg_terminals",
    "stg_passport_blacklist",
    "dwh_fact_transactions",
    "dwh_fact_passport_blacklist",
    "dwh_dim_terminals",
    "rep_fraud",
    "meta_file_load",
]


def get_connection():
    env_path = Path(__file__).with_name(".env")
    load_dotenv(dotenv_path=env_path if env_path.exists() else None)

    db_user = os.getenv("DATABASE_USER")
    db_password = os.getenv("DATABASE_PASSWORD")
    if not db_user or not db_password:
        raise RuntimeError(
            "DATABASE_USER/DATABASE_PASSWORD are not set. "
            "Create .env near reset_data_load.py or export env vars."
        )

    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname=DB_NAME,
        user=db_user,
        password=db_password,
    )


def get_existing_tables(cur) -> list[str]:
    cur.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
        """,
        (SCHEMA_NAME,),
    )
    existing = {row[0] for row in cur.fetchall()}
    return [t for t in TARGET_TABLES if t in existing]


def reset_data_load_tables() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            tables = get_existing_tables(cur)
            if not tables:
                print(f"No target tables found in schema '{SCHEMA_NAME}'.")
                return

            full_names = ", ".join(f"{SCHEMA_NAME}.{t}" for t in tables)
            cur.execute(f"TRUNCATE TABLE {full_names} CASCADE;")
            conn.commit()

    print("Reset completed. Cleared tables:")
    for name in tables:
        print(f"- {SCHEMA_NAME}.{name}")
    print("Now main.py will not skip processed file tokens.")


if __name__ == "__main__":
    reset_data_load_tables()
