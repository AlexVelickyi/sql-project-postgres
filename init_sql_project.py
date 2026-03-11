from __future__ import annotations

import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

DB_NAME = "db_1"
SCHEMA_NAME = "sql_project"


def get_connection():
    env_path = Path(__file__).with_name(".env")
    load_dotenv(dotenv_path=env_path if env_path.exists() else None)

    db_user = os.getenv("DATABASE_USER")
    db_password = os.getenv("DATABASE_PASSWORD")
    if not db_user or not db_password:
        raise RuntimeError(
            "DATABASE_USER/DATABASE_PASSWORD are not set. "
            "Create .env near init_sql_project.py or export env vars."
        )

    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname=DB_NAME,
        user=db_user,
        password=db_password,
    )


def create_schema_and_tables() -> None:
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};

    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.dwh_dim_terminals (
        terminal_id VARCHAR(32) PRIMARY KEY,
        terminal_type VARCHAR(32),
        terminal_city VARCHAR(64),
        terminal_address VARCHAR(256),
        create_dt TIMESTAMP NOT NULL DEFAULT NOW(),
        update_dt TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.dwh_dim_cards (
        card_num VARCHAR(32) PRIMARY KEY,
        account_num VARCHAR(32),
        create_dt TIMESTAMP NOT NULL DEFAULT NOW(),
        update_dt TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.dwh_dim_accounts (
        account_num VARCHAR(32) PRIMARY KEY,
        valid_to DATE,
        client VARCHAR(16),
        create_dt TIMESTAMP NOT NULL DEFAULT NOW(),
        update_dt TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.dwh_dim_clients (
        client_id VARCHAR(16) PRIMARY KEY,
        last_name VARCHAR(64),
        first_name VARCHAR(64),
        patronymic VARCHAR(64),
        date_of_birth DATE,
        passport_num VARCHAR(32),
        passport_valid_to DATE,
        phone VARCHAR(32),
        create_dt TIMESTAMP NOT NULL DEFAULT NOW(),
        update_dt TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.dwh_fact_passport_blacklist (
        passport_num VARCHAR(32) NOT NULL,
        entry_dt DATE NOT NULL,
        PRIMARY KEY (passport_num, entry_dt)
    );

    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.dwh_fact_transactions (
        trans_id VARCHAR(32) PRIMARY KEY,
        trans_date TIMESTAMP,
        card_num VARCHAR(32),
        oper_type VARCHAR(32),
        amt NUMERIC(18, 2),
        oper_result VARCHAR(32),
        terminal VARCHAR(32)
    );

    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.rep_fraud (
        event_dt TIMESTAMP NOT NULL,
        passport VARCHAR(32) NOT NULL,
        fio VARCHAR(255) NOT NULL,
        phone VARCHAR(32),
        event_type VARCHAR(255) NOT NULL,
        report_dt TIMESTAMP NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.meta_source_load (
        source_table VARCHAR(64) PRIMARY KEY,
        last_update_dt TIMESTAMP NOT NULL DEFAULT TIMESTAMP '1900-01-01 00:00:00',
        processed_dt TIMESTAMP NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.meta_file_load (
        file_token VARCHAR(8) PRIMARY KEY,
        processed_dt TIMESTAMP NOT NULL DEFAULT NOW(),
        status VARCHAR(16) NOT NULL
    );

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

    CREATE UNIQUE INDEX IF NOT EXISTS uq_rep_fraud_event
        ON {SCHEMA_NAME}.rep_fraud (event_dt, passport, event_type);

    ALTER TABLE {SCHEMA_NAME}.dwh_dim_cards
        DROP CONSTRAINT IF EXISTS fk_cards_accounts;
    ALTER TABLE {SCHEMA_NAME}.dwh_dim_cards
        ADD CONSTRAINT fk_cards_accounts
        FOREIGN KEY (account_num)
        REFERENCES {SCHEMA_NAME}.dwh_dim_accounts(account_num);

    ALTER TABLE {SCHEMA_NAME}.dwh_dim_accounts
        DROP CONSTRAINT IF EXISTS fk_accounts_clients;
    ALTER TABLE {SCHEMA_NAME}.dwh_dim_accounts
        ADD CONSTRAINT fk_accounts_clients
        FOREIGN KEY (client)
        REFERENCES {SCHEMA_NAME}.dwh_dim_clients(client_id);

    ALTER TABLE {SCHEMA_NAME}.dwh_fact_transactions
        DROP CONSTRAINT IF EXISTS fk_transactions_cards;
    ALTER TABLE {SCHEMA_NAME}.dwh_fact_transactions
        ADD CONSTRAINT fk_transactions_cards
        FOREIGN KEY (card_num)
        REFERENCES {SCHEMA_NAME}.dwh_dim_cards(card_num);

    ALTER TABLE {SCHEMA_NAME}.dwh_fact_transactions
        DROP CONSTRAINT IF EXISTS fk_transactions_terminals;
    ALTER TABLE {SCHEMA_NAME}.dwh_fact_transactions
        ADD CONSTRAINT fk_transactions_terminals
        FOREIGN KEY (terminal)
        REFERENCES {SCHEMA_NAME}.dwh_dim_terminals(terminal_id);

    -- Uppercase aliases for strict naming checks in external validators.
    CREATE OR REPLACE VIEW {SCHEMA_NAME}."DWH_DIM_TERMINALS" AS
        SELECT * FROM {SCHEMA_NAME}.dwh_dim_terminals;
    CREATE OR REPLACE VIEW {SCHEMA_NAME}."DWH_DIM_CARDS" AS
        SELECT * FROM {SCHEMA_NAME}.dwh_dim_cards;
    CREATE OR REPLACE VIEW {SCHEMA_NAME}."DWH_DIM_ACCOUNTS" AS
        SELECT * FROM {SCHEMA_NAME}.dwh_dim_accounts;
    CREATE OR REPLACE VIEW {SCHEMA_NAME}."DWH_DIM_CLIENTS" AS
        SELECT * FROM {SCHEMA_NAME}.dwh_dim_clients;
    CREATE OR REPLACE VIEW {SCHEMA_NAME}."DWH_FACT_TRANSACTIONS" AS
        SELECT * FROM {SCHEMA_NAME}.dwh_fact_transactions;
    CREATE OR REPLACE VIEW {SCHEMA_NAME}."DWH_FACT_PASSPORT_BLACKLIST" AS
        SELECT * FROM {SCHEMA_NAME}.dwh_fact_passport_blacklist;
    CREATE OR REPLACE VIEW {SCHEMA_NAME}."REP_FRAUD" AS
        SELECT * FROM {SCHEMA_NAME}.rep_fraud;
    CREATE OR REPLACE VIEW {SCHEMA_NAME}."META_SOURCE_LOAD" AS
        SELECT * FROM {SCHEMA_NAME}.meta_source_load;
    CREATE OR REPLACE VIEW {SCHEMA_NAME}."META_FILE_LOAD" AS
        SELECT * FROM {SCHEMA_NAME}.meta_file_load;
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

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()

    print(f"Schema '{SCHEMA_NAME}' and DWH tables are ready in database '{DB_NAME}'.")


if __name__ == "__main__":
    create_schema_and_tables()
