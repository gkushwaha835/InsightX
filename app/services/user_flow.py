from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from flask import current_app, has_app_context, session

from ..db import transaction


def issue_one_time_access(flow_key: str) -> str:
    tokens = dict(session.get('report_access_tokens') or {})
    token = uuid4().hex
    tokens[flow_key] = token
    session['report_access_tokens'] = tokens
    return token


def consume_one_time_access(flow_key: str, token: str) -> bool:
    if not token:
        return False
    tokens = dict(session.get('report_access_tokens') or {})
    expected = str(tokens.get(flow_key) or '')
    if expected != token:
        return False
    tokens.pop(flow_key, None)
    session['report_access_tokens'] = tokens
    return True


def log_user_uploaded_file(option_used: str, file_path: Path, source_name: str) -> None:
    email = str(session.get('login_email') or '').strip().lower()
    phone = str(session.get('login_phone') or '').strip()
    brand_name = str(session.get('brand_name') or '').strip()
    member_visit_id = str(session.get('member_visit_id') or uuid4().hex).strip()
    session['member_visit_id'] = member_visit_id
    if not email or not phone:
        return

    try:
        with transaction() as connection:
            with connection.cursor() as cursor:
                # Keep identity stable across email/password changes by matching same phone first.
                cursor.execute(
                    """
                    SELECT id
                    FROM user_identity
                    WHERE latest_phone = %s
                       OR user_email = %s
                    ORDER BY CASE WHEN latest_phone = %s THEN 0 ELSE 1 END, id ASC
                    LIMIT 1
                    """,
                    (phone, email, phone),
                )
                identity_row = cursor.fetchone() or {}
                user_identity_id = identity_row.get('id')

                if user_identity_id is None:
                    cursor.execute(
                        """
                        INSERT INTO user_identity (user_email, latest_phone, brand_name, created_at, updated_at)
                        VALUES (%s, %s, %s, NOW(), NOW())
                        RETURNING id
                        """,
                        (email, phone, brand_name),
                    )
                    identity_row = cursor.fetchone() or {}
                    user_identity_id = identity_row.get('id')
                else:
                    cursor.execute(
                        """
                        UPDATE user_identity
                        SET
                            latest_phone = %s,
                            user_email = CASE
                                WHEN EXISTS (
                                    SELECT 1 FROM user_identity other
                                    WHERE other.user_email = %s AND other.id <> user_identity.id
                                )
                                THEN user_identity.user_email
                                ELSE %s
                            END,
                            brand_name = COALESCE(NULLIF(%s, ''), user_identity.brand_name),
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (
                            phone,
                            email,
                            email,
                            brand_name,
                            user_identity_id,
                        ),
                    )

                cursor.execute(
                    """
                    INSERT INTO user_data_logs (
                        user_identity_id,
                        member_visit_id,
                        user_email,
                        phone_number,
                        brand_name,
                        option_used,
                        file_name,
                        file_path,
                        created_at,
                        updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    """,
                    (
                        user_identity_id,
                        member_visit_id,
                        email,
                        phone,
                        brand_name,
                        option_used,
                        (source_name or file_path.name),
                        str(file_path.resolve()),
                    ),
                )
    except Exception as exc:  # noqa: BLE001
        if has_app_context():
            current_app.logger.warning('Upload log save skipped due to DB error: %s', exc)


def log_user_login_activity() -> None:
    email = str(session.get('login_email') or '').strip().lower()
    phone = str(session.get('login_phone') or '').strip()
    brand_name = str(session.get('brand_name') or '').strip()
    member_visit_id = str(session.get('member_visit_id') or uuid4().hex).strip()
    session['member_visit_id'] = member_visit_id
    if not email or not phone:
        return

    app_root = Path(__file__).resolve().parents[2]
    login_root = app_root / 'uploads' / 'login_events'
    login_root.mkdir(parents=True, exist_ok=True)
    login_file = login_root / f'login_{member_visit_id}.txt'
    if not login_file.exists():
        login_file.write_text(
            f'Login event\nemail={email}\nphone={phone}\nvisit_id={member_visit_id}\n',
            encoding='utf-8',
        )

    try:
        with transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id
                    FROM user_identity
                    WHERE latest_phone = %s
                       OR user_email = %s
                    ORDER BY CASE WHEN latest_phone = %s THEN 0 ELSE 1 END, id ASC
                    LIMIT 1
                    """,
                    (phone, email, phone),
                )
                identity_row = cursor.fetchone() or {}
                user_identity_id = identity_row.get('id')

                if user_identity_id is None:
                    cursor.execute(
                        """
                        INSERT INTO user_identity (user_email, latest_phone, brand_name, created_at, updated_at)
                        VALUES (%s, %s, %s, NOW(), NOW())
                        RETURNING id
                        """,
                        (email, phone, brand_name),
                    )
                    identity_row = cursor.fetchone() or {}
                    user_identity_id = identity_row.get('id')
                else:
                    cursor.execute(
                        """
                        UPDATE user_identity
                        SET
                            latest_phone = %s,
                            user_email = CASE
                                WHEN EXISTS (
                                    SELECT 1 FROM user_identity other
                                    WHERE other.user_email = %s AND other.id <> user_identity.id
                                )
                                THEN user_identity.user_email
                                ELSE %s
                            END,
                            brand_name = COALESCE(NULLIF(%s, ''), user_identity.brand_name),
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (
                            phone,
                            email,
                            email,
                            brand_name,
                            user_identity_id,
                        ),
                    )

                cursor.execute(
                    """
                    INSERT INTO user_data_logs (
                        user_identity_id,
                        member_visit_id,
                        user_email,
                        phone_number,
                        brand_name,
                        option_used,
                        file_name,
                        file_path,
                        created_at,
                        updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    """,
                    (
                        user_identity_id,
                        member_visit_id,
                        email,
                        phone,
                        brand_name,
                        'Login',
                        'login_event.txt',
                        str(login_file.resolve()),
                    ),
                )
    except Exception as exc:  # noqa: BLE001
        if has_app_context():
            current_app.logger.warning('Login activity save skipped due to DB error: %s', exc)


def log_user_logout_activity() -> None:
    email = str(session.get('login_email') or '').strip().lower()
    phone = str(session.get('login_phone') or '').strip()
    brand_name = str(session.get('brand_name') or '').strip()
    member_visit_id = str(session.get('member_visit_id') or '').strip()
    if not email or not phone or not member_visit_id:
        return

    app_root = Path(__file__).resolve().parents[2]
    login_root = app_root / 'uploads' / 'login_events'
    login_root.mkdir(parents=True, exist_ok=True)
    logout_file = login_root / f'logout_{member_visit_id}.txt'
    if not logout_file.exists():
        logout_file.write_text(
            f'Logout event\nemail={email}\nphone={phone}\nvisit_id={member_visit_id}\n',
            encoding='utf-8',
        )

    try:
        with transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT id FROM user_identity WHERE lower(user_email) = %s LIMIT 1",
                    (email,),
                )
                identity_row = cursor.fetchone() or {}
                user_identity_id = identity_row.get('id')

                cursor.execute(
                    """
                    INSERT INTO user_data_logs (
                        user_identity_id,
                        member_visit_id,
                        user_email,
                        phone_number,
                        brand_name,
                        option_used,
                        file_name,
                        file_path,
                        created_at,
                        updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    """,
                    (
                        user_identity_id,
                        member_visit_id,
                        email,
                        phone,
                        brand_name,
                        'Logout',
                        'logout_event.txt',
                        str(logout_file.resolve()),
                    ),
                )
    except Exception as exc:  # noqa: BLE001
        if has_app_context():
            current_app.logger.warning('Logout activity save skipped due to DB error: %s', exc)
