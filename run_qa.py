from __future__ import annotations

import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


DB_NAME = "db_1"
SQL_PATH = Path("sql_scripts/qa_checks.sql")


def get_connection():
    env_path = Path(__file__).with_name(".env")
    load_dotenv(dotenv_path=env_path if env_path.exists() else None)

    db_user = os.getenv("DATABASE_USER")
    db_password = os.getenv("DATABASE_PASSWORD")
    if not db_user or not db_password:
        raise RuntimeError(
            "DATABASE_USER/DATABASE_PASSWORD are not set. "
            "Create .env near run_qa.py or export env vars."
        )

    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname=DB_NAME,
        user=db_user,
        password=db_password,
    )


def split_sql_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    buffer: list[str] = []
    in_single_quote = False
    in_line_comment = False
    in_block_comment = False

    i = 0
    while i < len(sql_text):
        ch = sql_text[i]
        nxt = sql_text[i + 1] if i + 1 < len(sql_text) else ""

        if in_line_comment:
            buffer.append(ch)
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue

        if in_block_comment:
            buffer.append(ch)
            if ch == "*" and nxt == "/":
                buffer.append(nxt)
                i += 2
                in_block_comment = False
                continue
            i += 1
            continue

        if not in_single_quote and ch == "-" and nxt == "-":
            buffer.append(ch)
            buffer.append(nxt)
            i += 2
            in_line_comment = True
            continue

        if not in_single_quote and ch == "/" and nxt == "*":
            buffer.append(ch)
            buffer.append(nxt)
            i += 2
            in_block_comment = True
            continue

        if ch == "'":
            buffer.append(ch)
            # Handle escaped quote: ''
            if in_single_quote and nxt == "'":
                buffer.append(nxt)
                i += 2
                continue
            in_single_quote = not in_single_quote
            i += 1
            continue

        if ch == ";" and not in_single_quote:
            statement = "".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer = []
            i += 1
            continue

        buffer.append(ch)
        i += 1

    tail = "".join(buffer).strip()
    if tail:
        statements.append(tail)
    return statements


def render_table(headers: list[str], rows: list[tuple]) -> str:
    string_rows = [tuple("" if v is None else str(v) for v in row) for row in rows]
    widths = [len(h) for h in headers]

    for row in string_rows:
        for idx, value in enumerate(row):
            widths[idx] = max(widths[idx], len(value))

    def fmt_row(values: list[str]) -> str:
        return "| " + " | ".join(value.ljust(widths[i]) for i, value in enumerate(values)) + " |"

    sep = "+-" + "-+-".join("-" * w for w in widths) + "-+"
    lines = [sep, fmt_row(headers), sep]
    for row in string_rows:
        lines.append(fmt_row(list(row)))
    lines.append(sep)
    return "\n".join(lines)


def main() -> None:
    if not SQL_PATH.exists():
        raise FileNotFoundError(f"Missing SQL file: {SQL_PATH}")

    sql_text = SQL_PATH.read_text(encoding="utf-8")
    statements = split_sql_statements(sql_text)

    with get_connection() as conn:
        with conn.cursor() as cur:
            for idx, stmt in enumerate(statements, start=1):
                cur.execute(stmt)
                title = stmt.splitlines()[0].strip()
                print(f"\n[{idx}] {title}")
                if cur.description:
                    headers = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    print(render_table(headers, rows))
                else:
                    print("(no result set)")


if __name__ == "__main__":
    main()
