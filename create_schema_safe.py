from app.db import transaction, execute, get_database_url
print('DB:', get_database_url())

# Try create schema
try:
    execute("CREATE SCHEMA IF NOT EXISTS selleroptic;")
    execute("GRANT ALL ON SCHEMA selleroptic TO calibray;")
    print('✓ Schema selleroptic created/granted')
except Exception as e:
    print('Schema create failed (normal for hosted):', str(e)[:100])

# Set search_path
execute("SET search_path TO selleroptic, public;")

# Try create tables in selleroptic
tables_sql = """
CREATE TABLE IF NOT EXISTS selleroptic.app_settings (
  setting_key VARCHAR(100) PRIMARY KEY,
  setting_value TEXT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS selleroptic.users (
  id SERIAL PRIMARY KEY,
  username VARCHAR(100) UNIQUE NOT NULL,
  password VARCHAR(255) NOT NULL,
  role VARCHAR(50) NOT NULL DEFAULT 'user',
  can_create_users BOOLEAN NOT NULL DEFAULT FALSE,
  session_version INTEGER NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS selleroptic.monthly_ads (
  id BIGSERIAL PRIMARY KEY,
  user_id INTEGER REFERENCES selleroptic.users(id),
  sku VARCHAR(120),
  asin VARCHAR(120),
  category VARCHAR(200),
  month_name VARCHAR(20),
  impressions INTEGER DEFAULT 0,
  clicks INTEGER DEFAULT 0,
  page_views INTEGER DEFAULT 0,
  sessions INTEGER DEFAULT 0,
  ctr DOUBLE PRECISION DEFAULT 0,
  spend DOUBLE PRECISION DEFAULT 0,
  sales DOUBLE PRECISION DEFAULT 0,
  total_units INTEGER DEFAULT 0,
  total_sales DOUBLE PRECISION DEFAULT 0,
  acos DOUBLE PRECISION DEFAULT 0,
  tacos DOUBLE PRECISION DEFAULT 0,
  conversion_rate DOUBLE PRECISION DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Add more tables: campaign_report, search_term_report, user_data_logs, weekly_ads, category_ads
-- (abbreviated for brevity; full in postgres_schema.sql)
CREATE INDEX IF NOT EXISTS idx_monthly_ads_month ON selleroptic.monthly_ads(month_name);
PRINT 'Tables created in selleroptic schema';
"""

try:
    execute(tables_sql)
    print('✓ Tables created in selleroptic schema')
except Exception as e:
    print('Tables create failed:', str(e)[:100])

print('\\nRun python demo_db_storage.py -- but update queries to selleroptic.table if needed')

