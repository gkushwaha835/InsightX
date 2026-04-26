from pathlib import Path
from app.db import ensure_schema, get_database_url
print('DB URL:', get_database_url())
print('Creating schema from database/postgres_schema.sql...')
ensure_schema()
print('Schema created successfully!')
print('\nRun: python demo_db_storage.py  # to verify tables/data')

