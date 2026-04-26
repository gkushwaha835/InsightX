from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Sequence

import psycopg
from flask import current_app, has_app_context
from psycopg.rows import dict_row

DEFAULT_DATABASE_URL = 'postgresql://postgres:postgres@localhost:5432/amazon_ads_analytics'
DEFAULT_CONNECT_TIMEOUT = 3


def normalize_database_url(value: str | None) -> str:
    raw = str(value or '').strip()
    if raw.startswith('postgresql+psycopg://'):
        return f"postgresql://{raw[len('postgresql+psycopg://') :]}"
    if raw.startswith('postgresql+psycopg2://'):
        return f"postgresql://{raw[len('postgresql+psycopg2://') :]}"
    return raw or DEFAULT_DATABASE_URL


def get_database_url() -> str:
    configured = ''
    if has_app_context():
        configured = str(current_app.config.get('DATABASE_URL') or '').strip()
    if not configured:
        configured = str(os.getenv('DATABASE_URL') or '').strip()
    return normalize_database_url(configured)


def get_connect_timeout() -> int:
    raw = str(os.getenv('DB_CONNECT_TIMEOUT', str(DEFAULT_CONNECT_TIMEOUT))).strip()
    try:
        timeout = int(raw)
    except ValueError:
        return DEFAULT_CONNECT_TIMEOUT
    return max(timeout, 1)


@contextmanager
def get_connection() -> Iterator[psycopg.Connection]:
    connection = psycopg.connect(
        get_database_url(),
        row_factory=dict_row,
        connect_timeout=get_connect_timeout(),
    )
    try:
        yield connection
    finally:
        connection.close()


@contextmanager
def transaction() -> Iterator[psycopg.Connection]:
    with get_connection() as connection:
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise


def fetch_all(
    query: str,
    params: Sequence[Any] | dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            return list(cursor.fetchall())


def fetch_one(
    query: str,
    params: Sequence[Any] | dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            row = cursor.fetchone()
            return dict(row) if row else None


def fetch_value(
    query: str,
    params: Sequence[Any] | dict[str, Any] | None = None,
    default: Any = None,
) -> Any:
    row = fetch_one(query, params)
    if not row:
        return default
    return next(iter(row.values()), default)


def execute(
    query: str,
    params: Sequence[Any] | dict[str, Any] | None = None,
) -> None:
    with transaction() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)


def execute_many(
    query: str,
    params_list: list[Sequence[Any] | dict[str, Any]],
) -> None:
    if not params_list:
        return
    with transaction() as connection:
        with connection.cursor() as cursor:
            cursor.executemany(query, params_list)


def upsert_app_setting(
    setting_key: str,
    setting_value: str | None,
    *,
    connection: psycopg.Connection | None = None,
) -> None:
    sql = """
        INSERT INTO app_settings (setting_key, setting_value, created_at, updated_at)
        VALUES (%s, %s, NOW(), NOW())
        ON CONFLICT (setting_key)
        DO UPDATE SET setting_value = EXCLUDED.setting_value, updated_at = NOW()
    """
    params = (setting_key, setting_value)
    if connection is not None:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
        return

    with transaction() as tx:
        with tx.cursor() as cursor:
            cursor.execute(sql, params)


def upsert_app_settings(
    settings: dict[str, str | None],
    *,
    connection: psycopg.Connection | None = None,
) -> None:
    if not settings:
        return
    if connection is not None:
        for key, value in settings.items():
            upsert_app_setting(key, value, connection=connection)
        return

    with transaction() as tx:
        for key, value in settings.items():
            upsert_app_setting(key, value, connection=tx)


def get_app_setting(setting_key: str, default: str = '') -> str:
    value = fetch_value(
        "SELECT setting_value FROM app_settings WHERE setting_key = %s",
        (setting_key,),
        default=None,
    )
    if value is None:
        return default
    return str(value)


def _split_sql_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    chunk: list[str] = []

    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped == '' or stripped.startswith('--'):
            continue
        chunk.append(line)
        if stripped.endswith(';'):
            stmt = '\n'.join(chunk).strip()
            statements.append(stmt[:-1] if stmt.endswith(';') else stmt)
            chunk = []

    tail = '\n'.join(chunk).strip()
    if tail:
        statements.append(tail)

    return statements


def ensure_schema() -> None:
    app_root = Path(__file__).resolve().parents[1]
    schema_path = app_root / 'database' / 'postgres_schema.sql'
    schema_sql = schema_path.read_text(encoding='utf-8')
    statements = _split_sql_statements(schema_sql)

    with transaction() as connection:
        with connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)

            for table_name in ('monthly_ads', 'weekly_ads', 'campaign_report', 'search_term_report'):
                cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS user_id INTEGER')
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS user_identity (
                    id BIGSERIAL PRIMARY KEY,
                    user_email VARCHAR(255) NOT NULL UNIQUE,
                    latest_phone VARCHAR(30) NULL,
                    brand_name VARCHAR(120) NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cursor.execute("ALTER TABLE user_data_logs ADD COLUMN IF NOT EXISTS user_identity_id BIGINT")
            cursor.execute("ALTER TABLE user_data_logs ADD COLUMN IF NOT EXISTS member_visit_id VARCHAR(64)")
            cursor.execute("ALTER TABLE user_data_logs ADD COLUMN IF NOT EXISTS brand_name VARCHAR(120)")
            cursor.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conname = 'user_data_logs_user_identity_id_fkey'
                    ) THEN
                        ALTER TABLE user_data_logs
                        ADD CONSTRAINT user_data_logs_user_identity_id_fkey
                        FOREIGN KEY (user_identity_id) REFERENCES user_identity(id) ON DELETE SET NULL;
                    END IF;
                END $$;
                """
            )
            cursor.execute(
                """
                INSERT INTO user_identity (user_email, latest_phone, brand_name, created_at, updated_at)
                SELECT
                    lower(user_email) AS user_email,
                    MAX(phone_number) AS latest_phone,
                    MAX(COALESCE(brand_name, '')) AS brand_name,
                    NOW(),
                    NOW()
                FROM user_data_logs
                GROUP BY lower(user_email)
                ON CONFLICT (user_email)
                DO UPDATE SET
                    latest_phone = EXCLUDED.latest_phone,
                    brand_name = CASE
                        WHEN EXCLUDED.brand_name IS NULL OR EXCLUDED.brand_name = '' THEN user_identity.brand_name
                        ELSE EXCLUDED.brand_name
                    END,
                    updated_at = NOW()
                """
            )
            cursor.execute(
                """
                UPDATE user_data_logs logs
                SET user_identity_id = ui.id
                FROM user_identity ui
                WHERE logs.user_identity_id IS NULL
                  AND lower(logs.user_email) = ui.user_email
                """
            )
