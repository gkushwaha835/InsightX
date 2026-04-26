from functools import wraps
from types import SimpleNamespace
from typing import Any

from flask import current_app, flash, has_app_context, redirect, session, url_for

from .db import fetch_one


def _user_from_row(row: dict[str, Any] | None) -> SimpleNamespace | None:
    if not row:
        return None
    return SimpleNamespace(
        id=row.get('id'),
        username=str(row.get('username') or ''),
        password=str(row.get('password') or ''),
        role=str(row.get('role') or 'user'),
        can_create_users=bool(row.get('can_create_users')),
        session_version=int(row.get('session_version') or 1),
    )


def get_current_user() -> SimpleNamespace | None:
    email = (session.get('user') or '').strip()
    if not email:
        return None

    account_username = (session.get('account_username') or '').strip()
    if account_username:
        try:
            row = fetch_one(
                """
                SELECT id, username, password, role, can_create_users, session_version
                FROM users
                WHERE username = %s
                LIMIT 1
                """,
                (account_username,),
            )
        except Exception as exc:  # noqa: BLE001
            if has_app_context():
                current_app.logger.warning('Could not resolve session user from DB: %s', exc)
            return None
        user = _user_from_row(row)
        if not user:
            return None
        if int(session.get('session_version', 0)) != int(user.session_version):
            return None
        return user

    return SimpleNamespace(
        id=None,
        username=email,
        password='',
        role=(session.get('user_role') or 'user'),
        can_create_users=False,
        session_version=1,
    )


def login_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        user = get_current_user()
        if user is None:
            session.clear()
            flash('Please login to continue.', 'warning')
            return redirect(url_for('main.login'))
        session['user_role'] = user.role
        session['can_create_users'] = bool(user.can_create_users)
        return view_func(*args, **kwargs)

    return wrapper


def can_manage_users(user: SimpleNamespace | None) -> bool:
    return bool(user and user.role == 'admin')


def can_create_users(user: SimpleNamespace | None) -> bool:
    if not user:
        return False
    if user.role == 'admin':
        return True
    return user.role in ('co_admin', 'cd_admin') and bool(user.can_create_users)


def can_view_user_data(user: SimpleNamespace | None) -> bool:
    return bool(user and user.role in ('admin', 'co_admin', 'cd_admin'))
