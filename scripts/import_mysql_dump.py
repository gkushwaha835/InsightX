import os
import re
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SQL = ROOT / '..' / 'database' / 'amazon_ads_analytics.sql'


def extract_insert_values(sql_text: str, table_name: str) -> list[str]:
    pattern = re.compile(rf"INSERT INTO `{table_name}` VALUES\s*(.+?);", re.S)
    chunks = []
    for match in pattern.finditer(sql_text):
        chunks.append(match.group(1))
    return chunks


def parse_rows(values_blob: str) -> list[list[str]]:
    rows = []
    current = ''
    depth = 0
    in_str = False

    for ch in values_blob:
        if ch == "'":
            in_str = not in_str
        if ch == '(' and not in_str:
            depth += 1
        if depth > 0:
            current += ch
        if ch == ')' and not in_str:
            depth -= 1
            if depth == 0:
                rows.append(current)
                current = ''
    return rows


def mysql_row_to_list(row: str):
    body = row.strip()[1:-1]
    parts = []
    cur = ''
    in_str = False

    i = 0
    while i < len(body):
        ch = body[i]
        if ch == "'":
            in_str = not in_str
            cur += ch
        elif ch == ',' and not in_str:
            parts.append(cur)
            cur = ''
        else:
            cur += ch
        i += 1
    parts.append(cur)
    out = []
    for part in parts:
        val = part.strip()
        if val.upper() == 'NULL':
            out.append(None)
        elif val.startswith("'") and val.endswith("'"):
            out.append(val[1:-1].replace("\\'", "'"))
        else:
            out.append(val)
    return out


def main():
    sql_path = Path(os.getenv('MYSQL_DUMP_PATH', DEFAULT_SQL)).resolve()
    db_url = os.getenv('DATABASE_URL', 'postgresql+psycopg://postgres:postgres@localhost:5432/amazon_ads_analytics')

    text = sql_path.read_text(encoding='utf-8', errors='ignore')
    inserts = extract_insert_values(text, 'monthly_ads')

    all_rows = []
    for blob in inserts:
        all_rows.extend(parse_rows(blob))

    parsed = [mysql_row_to_list(r) for r in all_rows]

    cols = [
        'id', 'sku', 'asin', 'category', 'month_name', 'impressions', 'clicks', 'page_views', 'sessions',
        'ctr', 'spend', 'sales', 'total_units', 'total_sales', 'acos', 'tacos', 'conversion_rate'
    ]

    df = pd.DataFrame(parsed, columns=cols)
    if not df.empty:
        df = df.drop(columns=['id'])

    engine = create_engine(db_url)
    df.to_sql('monthly_ads', engine, if_exists='append', index=False)
    print(f'Imported rows: {len(df)}')


if __name__ == '__main__':
    main()
