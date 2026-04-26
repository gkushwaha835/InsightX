import json
from datetime import datetime
from pathlib import Path
from typing import Any


def load_history(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {'items': []}
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return {'items': []}

    if isinstance(data, dict) and isinstance(data.get('items'), list):
        return data

    if isinstance(data, dict) and isinstance(data.get('users'), dict):
        merged: list[dict[str, Any]] = []
        for username, user_items in data['users'].items():
            if not isinstance(user_items, list):
                continue
            for row in user_items:
                if not isinstance(row, dict):
                    continue
                row = dict(row)
                row['saved_by'] = str(username)
                merged.append(row)
        return {'items': merged}

    return {'items': []}


def save_history(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


def append_history(path: Path, item: dict[str, Any]) -> None:
    manifest = load_history(path)
    rows = manifest.get('items', [])
    if not isinstance(rows, list):
        rows = []

    row = dict(item)
    row.setdefault('id', datetime.utcnow().strftime('%Y%m%d%H%M%S%f'))
    row.setdefault('saved_at', datetime.utcnow().isoformat())

    rows.append(row)
    manifest['items'] = rows
    save_history(path, manifest)
