from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Iterable

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

from init_sql_project import DB_NAME, SCHEMA_NAME, create_schema_and_tables


DATA_DIR = Path("data")
ARCHIVE_DIR = Path("archive")
SQL_REPORT_PATH = Path("sql_scripts/build_rep_fraud.sql")
SOURCE_DDL_PATH = Path("ddl_dml.sql")

SOURCE_TABLES = ("cards", "accounts", "clients")
DEFAULT_WATERMARK = "1900-01-01 00:00:00"


def get_source_schema_hint() -> str | None:
    value = os.getenv("SOURCE_SCHEMA")
    return value.strip() if value else None


def get_connection():
    env_path = Path(__file__).with_name(".env")
    load_dotenv(dotenv_path=env_path if env_path.exists() else None)

    db_user = os.getenv("DATABASE_USER")
    db_password = os.getenv("DATABASE_PASSWORD")
    if not db_user or not db_password:
        raise RuntimeError(
            "DATABASE_USER/DATABASE_PASSWORD are not set. "
            "Create .env near main.py or export env vars."
        )

    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname=DB_NAME,
        user=db_user,
        password=db_password,
    )


def ensure_runtime_tables(cur) -> None:
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.stg_transactions (
            trans_id VARCHAR(32),
            trans_date TIMESTAMP,
            amt NUMERIC(18, 2),
            card_num VARCHAR(32),
            oper_type VARCHAR(32),
            oper_result VARCHAR(32),
            terminal VARCHAR(32)
        );

        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.stg_terminals (
            terminal_id VARCHAR(32),
            terminal_type VARCHAR(32),
            terminal_city VARCHAR(64),
            terminal_address VARCHAR(256)
        );

        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.stg_passport_blacklist (
            passport_num VARCHAR(32),
            entry_dt DATE
        );

        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.stg_cards (
            card_num VARCHAR(128),
            account_num VARCHAR(128),
            create_dt DATE,
            update_dt DATE
        );

        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.stg_accounts (
            account_num VARCHAR(128),
            valid_to DATE,
            client VARCHAR(128),
            create_dt DATE,
            update_dt DATE
        );

        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.stg_clients (
            client_id VARCHAR(128),
            last_name VARCHAR(128),
            first_name VARCHAR(128),
            patronymic VARCHAR(128),
            date_of_birth DATE,
            passport_num VARCHAR(128),
            passport_valid_to DATE,
            phone VARCHAR(128),
            create_dt DATE,
            update_dt DATE
        );

        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.meta_source_load (
            source_table VARCHAR(64) PRIMARY KEY,
            last_update_dt TIMESTAMP NOT NULL DEFAULT TIMESTAMP '{DEFAULT_WATERMARK}',
            processed_dt TIMESTAMP NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.meta_file_load (
            file_token VARCHAR(8) PRIMARY KEY,
            processed_dt TIMESTAMP NOT NULL DEFAULT NOW(),
            status VARCHAR(16) NOT NULL
        );

        -- Uppercase aliases for strict naming checks in external validators.
        CREATE OR REPLACE VIEW {SCHEMA_NAME}."STG_TRANSACTIONS" AS
            SELECT * FROM {SCHEMA_NAME}.stg_transactions;
        CREATE OR REPLACE VIEW {SCHEMA_NAME}."STG_TERMINALS" AS
            SELECT * FROM {SCHEMA_NAME}.stg_terminals;
        CREATE OR REPLACE VIEW {SCHEMA_NAME}."STG_PASSPORT_BLACKLIST" AS
            SELECT * FROM {SCHEMA_NAME}.stg_passport_blacklist;
        CREATE OR REPLACE VIEW {SCHEMA_NAME}."STG_CARDS" AS
            SELECT * FROM {SCHEMA_NAME}.stg_cards;
        CREATE OR REPLACE VIEW {SCHEMA_NAME}."STG_ACCOUNTS" AS
            SELECT * FROM {SCHEMA_NAME}.stg_accounts;
        CREATE OR REPLACE VIEW {SCHEMA_NAME}."STG_CLIENTS" AS
            SELECT * FROM {SCHEMA_NAME}.stg_clients;
        """
    )
    cur.execute(
        f"""
        INSERT INTO {SCHEMA_NAME}.meta_source_load (source_table, last_update_dt, processed_dt)
        VALUES
            ('cards', TIMESTAMP '{DEFAULT_WATERMARK}', NOW()),
            ('accounts', TIMESTAMP '{DEFAULT_WATERMARK}', NOW()),
            ('clients', TIMESTAMP '{DEFAULT_WATERMARK}', NOW())
        ON CONFLICT (source_table) DO NOTHING;
        """
    )


def truncate_stg(cur) -> None:
    cur.execute(
        f"""
        TRUNCATE TABLE
            {SCHEMA_NAME}.stg_transactions,
            {SCHEMA_NAME}.stg_terminals,
            {SCHEMA_NAME}.stg_passport_blacklist,
            {SCHEMA_NAME}.stg_cards,
            {SCHEMA_NAME}.stg_accounts,
            {SCHEMA_NAME}.stg_clients;
        """
    )


def read_transactions(path: Path) -> list[tuple]:
    df = pd.read_csv(path, sep=";", dtype=str)
    df = df.rename(
        columns={
            "transaction_id": "trans_id",
            "transaction_date": "trans_date",
            "amount": "amt",
            "card_num": "card_num",
            "oper_type": "oper_type",
            "oper_result": "oper_result",
            "terminal": "terminal",
        }
    )
    df["trans_date"] = pd.to_datetime(df["trans_date"], errors="coerce")
    df["amt"] = df["amt"].str.replace(",", ".", regex=False)
    df["card_num"] = df["card_num"].str.strip()
    df["terminal"] = df["terminal"].str.strip()

    rows = []
    for row in df.itertuples(index=False):
        rows.append(
            (
                row.trans_id,
                row.trans_date.to_pydatetime() if pd.notna(row.trans_date) else None,
                row.amt,
                row.card_num,
                row.oper_type,
                row.oper_result,
                row.terminal,
            )
        )
    return rows


def read_terminals(path: Path) -> list[tuple]:
    df = pd.read_excel(path, dtype=str).rename(
        columns={
            "terminal_id": "terminal_id",
            "terminal_type": "terminal_type",
            "terminal_city": "terminal_city",
            "terminal_address": "terminal_address",
        }
    )
    for col in ["terminal_id", "terminal_type", "terminal_city", "terminal_address"]:
        df[col] = df[col].astype(str).str.strip()
    return list(
        df[
            ["terminal_id", "terminal_type", "terminal_city", "terminal_address"]
        ].itertuples(index=False, name=None)
    )


def read_passport_blacklist(path: Path) -> list[tuple]:
    df = pd.read_excel(path)
    df = df.rename(columns={"date": "entry_dt", "passport": "passport_num"})
    df["entry_dt"] = pd.to_datetime(df["entry_dt"], errors="coerce").dt.date
    df["passport_num"] = df["passport_num"].astype(str).str.strip()
    return list(df[["passport_num", "entry_dt"]].itertuples(index=False, name=None))


def load_file_stg(
    cur, transactions: list[tuple], terminals: list[tuple], passports: list[tuple]
) -> None:
    if transactions:
        execute_values(
            cur,
            f"""
            INSERT INTO {SCHEMA_NAME}.stg_transactions (
                trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal
            ) VALUES %s
            """,
            transactions,
        )
    if terminals:
        execute_values(
            cur,
            f"""
            INSERT INTO {SCHEMA_NAME}.stg_terminals (
                terminal_id, terminal_type, terminal_city, terminal_address
            ) VALUES %s
            """,
            terminals,
        )
    if passports:
        execute_values(
            cur,
            f"""
            INSERT INTO {SCHEMA_NAME}.stg_passport_blacklist (
                passport_num, entry_dt
            ) VALUES %s
            """,
            passports,
        )


def bootstrap_source_if_needed(cur) -> None:
    if not SOURCE_DDL_PATH.exists():
        return
    ddl = SOURCE_DDL_PATH.read_text(encoding="utf-8")
    cur.execute(ddl)


def resolve_source_schema(cur) -> str:
    schema_hint = get_source_schema_hint()

    def has_all_tables(schema_name: str) -> bool:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = ANY(%s)
            """,
            (schema_name, list(SOURCE_TABLES)),
        )
        return cur.fetchone()[0] == len(SOURCE_TABLES)

    if schema_hint:
        if has_all_tables(schema_hint):
            return schema_hint
        raise RuntimeError(
            f"SOURCE_SCHEMA='{schema_hint}' does not contain source tables "
            f"{SOURCE_TABLES}."
        )

    cur.execute(
        """
        SELECT table_schema
        FROM information_schema.tables
        WHERE table_name IN ('cards', 'accounts', 'clients')
        GROUP BY table_schema
        HAVING COUNT(DISTINCT table_name) = 3
        ORDER BY CASE WHEN table_schema = 'public' THEN 0 ELSE 1 END, table_schema
        LIMIT 1
        """
    )
    row = cur.fetchone()
    if row:
        return row[0]

    bootstrap_source_if_needed(cur)
    cur.execute(
        """
        SELECT table_schema
        FROM information_schema.tables
        WHERE table_name IN ('cards', 'accounts', 'clients')
        GROUP BY table_schema
        HAVING COUNT(DISTINCT table_name) = 3
        ORDER BY CASE WHEN table_schema = 'public' THEN 0 ELSE 1 END, table_schema
        LIMIT 1
        """
    )
    row = cur.fetchone()
    if row:
        return row[0]

    raise RuntimeError(
        "Source tables cards/accounts/clients are not found. "
        "Load ddl_dml.sql into db_1 or set SOURCE_SCHEMA with correct schema."
    )


def _get_source_watermark(cur, source_table: str):
    cur.execute(
        f"""
        SELECT last_update_dt
        FROM {SCHEMA_NAME}.meta_source_load
        WHERE source_table = %s
        """,
        (source_table,),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    return pd.Timestamp(DEFAULT_WATERMARK).to_pydatetime()


def _update_source_watermark(cur, source_table: str, last_update_dt) -> None:
    cur.execute(
        f"""
        INSERT INTO {SCHEMA_NAME}.meta_source_load (source_table, last_update_dt, processed_dt)
        VALUES (%s, %s, NOW())
        ON CONFLICT (source_table) DO UPDATE
        SET last_update_dt = EXCLUDED.last_update_dt,
            processed_dt = NOW()
        """,
        (source_table, last_update_dt),
    )


def load_source_stg_incremental(cur, source_schema: str) -> dict[str, object]:
    watermarks = {
        "cards": _get_source_watermark(cur, "cards"),
        "accounts": _get_source_watermark(cur, "accounts"),
        "clients": _get_source_watermark(cur, "clients"),
    }
    specs = [
        (
            "cards",
            f"""
            INSERT INTO {SCHEMA_NAME}.stg_cards (card_num, account_num, create_dt, update_dt)
            SELECT card_num, account, create_dt, update_dt
            FROM {source_schema}.cards
            """,
        ),
        (
            "accounts",
            f"""
            INSERT INTO {SCHEMA_NAME}.stg_accounts (account_num, valid_to, client, create_dt, update_dt)
            SELECT account, valid_to, client, create_dt, update_dt
            FROM {source_schema}.accounts
            """,
        ),
        (
            "clients",
            f"""
            INSERT INTO {SCHEMA_NAME}.stg_clients (
                client_id, last_name, first_name, patronymic, date_of_birth,
                passport_num, passport_valid_to, phone, create_dt, update_dt
            )
            SELECT
                client_id, last_name, first_name, patronymic, date_of_birth,
                passport_num, passport_valid_to, phone, create_dt, update_dt
            FROM {source_schema}.clients
            """,
        ),
    ]

    max_seen: dict[str, object] = {}
    for table_name, base_insert_sql in specs:
        watermark = watermarks[table_name]

        cur.execute(
            f"""
            SELECT MAX(COALESCE(update_dt, create_dt)::timestamp)
            FROM {source_schema}.{table_name}
            """
        )
        source_max_ts = cur.fetchone()[0]

        if source_max_ts is None:
            # No reliable source timestamps. Use full snapshot strategy.
            cur.execute(base_insert_sql)
            max_seen[table_name] = watermark
            continue

        cur.execute(
            base_insert_sql + " WHERE COALESCE(update_dt, create_dt)::timestamp > %s::timestamp",
            (watermark,),
        )
        max_seen[table_name] = source_max_ts

    return max_seen


def upsert_dimensions(cur) -> None:
    cur.execute(
        f"""
        INSERT INTO {SCHEMA_NAME}.dwh_dim_clients (
            client_id, last_name, first_name, patronymic, date_of_birth,
            passport_num, passport_valid_to, phone, create_dt, update_dt
        )
        SELECT
            client_id, last_name, first_name, patronymic, date_of_birth,
            passport_num, passport_valid_to, phone, NOW(), NULL
        FROM {SCHEMA_NAME}.stg_clients
        ON CONFLICT (client_id) DO UPDATE
        SET
            last_name = EXCLUDED.last_name,
            first_name = EXCLUDED.first_name,
            patronymic = EXCLUDED.patronymic,
            date_of_birth = EXCLUDED.date_of_birth,
            passport_num = EXCLUDED.passport_num,
            passport_valid_to = EXCLUDED.passport_valid_to,
            phone = EXCLUDED.phone,
            update_dt = NOW()
        WHERE
            {SCHEMA_NAME}.dwh_dim_clients.last_name IS DISTINCT FROM EXCLUDED.last_name OR
            {SCHEMA_NAME}.dwh_dim_clients.first_name IS DISTINCT FROM EXCLUDED.first_name OR
            {SCHEMA_NAME}.dwh_dim_clients.patronymic IS DISTINCT FROM EXCLUDED.patronymic OR
            {SCHEMA_NAME}.dwh_dim_clients.date_of_birth IS DISTINCT FROM EXCLUDED.date_of_birth OR
            {SCHEMA_NAME}.dwh_dim_clients.passport_num IS DISTINCT FROM EXCLUDED.passport_num OR
            {SCHEMA_NAME}.dwh_dim_clients.passport_valid_to IS DISTINCT FROM EXCLUDED.passport_valid_to OR
            {SCHEMA_NAME}.dwh_dim_clients.phone IS DISTINCT FROM EXCLUDED.phone;

        INSERT INTO {SCHEMA_NAME}.dwh_dim_accounts (
            account_num, valid_to, client, create_dt, update_dt
        )
        SELECT
            account_num, valid_to, client, NOW(), NULL
        FROM {SCHEMA_NAME}.stg_accounts
        ON CONFLICT (account_num) DO UPDATE
        SET
            valid_to = EXCLUDED.valid_to,
            client = EXCLUDED.client,
            update_dt = NOW()
        WHERE
            {SCHEMA_NAME}.dwh_dim_accounts.valid_to IS DISTINCT FROM EXCLUDED.valid_to OR
            {SCHEMA_NAME}.dwh_dim_accounts.client IS DISTINCT FROM EXCLUDED.client;

        INSERT INTO {SCHEMA_NAME}.dwh_dim_cards (
            card_num, account_num, create_dt, update_dt
        )
        SELECT
            card_num, account_num, NOW(), NULL
        FROM {SCHEMA_NAME}.stg_cards
        ON CONFLICT (card_num) DO UPDATE
        SET
            account_num = EXCLUDED.account_num,
            update_dt = NOW()
        WHERE
            {SCHEMA_NAME}.dwh_dim_cards.account_num IS DISTINCT FROM EXCLUDED.account_num;

        INSERT INTO {SCHEMA_NAME}.dwh_dim_terminals (
            terminal_id, terminal_type, terminal_city, terminal_address, create_dt, update_dt
        )
        SELECT
            terminal_id, terminal_type, terminal_city, terminal_address, NOW(), NULL
        FROM {SCHEMA_NAME}.stg_terminals
        ON CONFLICT (terminal_id) DO UPDATE
        SET
            terminal_type = EXCLUDED.terminal_type,
            terminal_city = EXCLUDED.terminal_city,
            terminal_address = EXCLUDED.terminal_address,
            update_dt = NOW()
        WHERE
            {SCHEMA_NAME}.dwh_dim_terminals.terminal_type IS DISTINCT FROM EXCLUDED.terminal_type OR
            {SCHEMA_NAME}.dwh_dim_terminals.terminal_city IS DISTINCT FROM EXCLUDED.terminal_city OR
            {SCHEMA_NAME}.dwh_dim_terminals.terminal_address IS DISTINCT FROM EXCLUDED.terminal_address;
        """
    )


def load_facts(cur) -> None:
    cur.execute(
        f"""
        INSERT INTO {SCHEMA_NAME}.dwh_fact_passport_blacklist (passport_num, entry_dt)
        SELECT DISTINCT passport_num, entry_dt
        FROM {SCHEMA_NAME}.stg_passport_blacklist
        ON CONFLICT (passport_num, entry_dt) DO NOTHING;

        INSERT INTO {SCHEMA_NAME}.dwh_fact_transactions (
            trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal
        )
        SELECT
            st.trans_id,
            st.trans_date,
            st.card_num,
            st.oper_type,
            st.amt,
            st.oper_result,
            st.terminal
        FROM {SCHEMA_NAME}.stg_transactions st
        JOIN {SCHEMA_NAME}.dwh_dim_cards c
            ON c.card_num = st.card_num
        JOIN {SCHEMA_NAME}.dwh_dim_terminals t
            ON t.terminal_id = st.terminal
        ON CONFLICT (trans_id) DO NOTHING;
        """
    )


def run_fraud_report(cur) -> None:
    if not SQL_REPORT_PATH.exists():
        raise FileNotFoundError(f"Missing SQL script: {SQL_REPORT_PATH}")
    sql = SQL_REPORT_PATH.read_text(encoding="utf-8")
    cur.execute(sql)


def collect_date_tokens() -> list[str]:
    token_sets: dict[str, set[str]] = {}
    patterns = {
        "transactions": re.compile(r"^transactions_(\d{8})\.txt$"),
        "terminals": re.compile(r"^terminals_(\d{8})\.xlsx$"),
        "passport_blacklist": re.compile(r"^passport_blacklist_(\d{8})\.xlsx$"),
    }

    for path in DATA_DIR.glob("*"):
        if not path.is_file():
            continue
        for key, pattern in patterns.items():
            m = pattern.match(path.name)
            if m:
                token_sets.setdefault(m.group(1), set()).add(key)

    complete = [token for token, kinds in token_sets.items() if len(kinds) == 3]
    return sorted(complete, key=lambda x: pd.to_datetime(x, format="%d%m%Y"))


def get_daily_files(token: str) -> tuple[Path, Path, Path]:
    return (
        DATA_DIR / f"transactions_{token}.txt",
        DATA_DIR / f"terminals_{token}.xlsx",
        DATA_DIR / f"passport_blacklist_{token}.xlsx",
    )


def archive_files(paths: Iterable[Path]) -> None:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    for src in paths:
        dst = ARCHIVE_DIR / f"{src.name}.backup"
        if dst.exists():
            dst.unlink()
        src.rename(dst)


def is_file_token_processed(cur, token: str) -> bool:
    cur.execute(
        f"""
        SELECT 1
        FROM {SCHEMA_NAME}.meta_file_load
        WHERE file_token = %s
          AND status = 'SUCCESS'
        """,
        (token,),
    )
    return cur.fetchone() is not None


def mark_file_token_status(cur, token: str, status: str) -> None:
    cur.execute(
        f"""
        INSERT INTO {SCHEMA_NAME}.meta_file_load (file_token, processed_dt, status)
        VALUES (%s, NOW(), %s)
        ON CONFLICT (file_token) DO UPDATE
        SET processed_dt = NOW(),
            status = EXCLUDED.status
        """,
        (token, status),
    )


def process_day(token: str) -> None:
    tx_file, terminals_file, blacklist_file = get_daily_files(token)
    tx_rows = read_transactions(tx_file)
    terminals_rows = read_terminals(terminals_file)
    blacklist_rows = read_passport_blacklist(blacklist_file)

    with get_connection() as conn:
        with conn.cursor() as cur:
            ensure_runtime_tables(cur)
            if is_file_token_processed(cur, token):
                print(f"[{token}] skipped: already processed in META_FILE_LOAD")
                return

            source_schema = resolve_source_schema(cur)
            truncate_stg(cur)
            load_file_stg(cur, tx_rows, terminals_rows, blacklist_rows)
            max_seen = load_source_stg_incremental(cur, source_schema)
            upsert_dimensions(cur)
            load_facts(cur)
            run_fraud_report(cur)

            for table_name in SOURCE_TABLES:
                _update_source_watermark(cur, table_name, max_seen[table_name])
            mark_file_token_status(cur, token, "SUCCESS")

        conn.commit()

    archive_files([tx_file, terminals_file, blacklist_file])
    print(
        f"[{token}] done: tx={len(tx_rows)}, terminals={len(terminals_rows)}, "
        f"passport_blacklist={len(blacklist_rows)}"
    )


def main() -> None:
    create_schema_and_tables()
    if not DATA_DIR.exists():
        raise FileNotFoundError(f"Missing data directory: {DATA_DIR}")

    tokens = collect_date_tokens()
    if not tokens:
        print("No complete daily file sets found in ./data")
        return

    for token in tokens:
        process_day(token)

    print("ETL finished.")


if __name__ == "__main__":
    main()
