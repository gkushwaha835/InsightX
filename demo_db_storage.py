from app.db import fetch_all, execute, get_database_url
print('Database URL (normalized):', get_database_url())
print()

# Check tables
tables_query = """
SELECT tablename FROM pg_tables 
WHERE schemaname = 'public' AND tablename IN ('monthly_ads', 'campaign_report', 'search_term_report', 'user_data_logs', 'users', 'app_settings')
ORDER BY tablename;
"""
tables = fetch_all(tables_query)
print('Tables found:')
for table in tables:
    print(f'  - {table["tablename"]}')
print()

# Row counts
count_query = "SELECT COUNT(*) AS count FROM %s;"
print('Row counts:')
counts = {}
for table in [t['tablename'] for t in tables]:
    try:
        row = fetch_all(count_query % table)
        count = row[0]['count'] if row else 0
        counts[table] = count
        print(f'  {table}: {count:,}')
    except Exception as e:
        print(f'  {table}: Error - {e}')
print()

# Sample data if exists
if counts.get('monthly_ads', 0) > 0:
    print('Sample monthly_ads (top 5, user_id anonymized):')
    samples = fetch_all('SELECT sku, asin, month_name, impressions, spend, sales, acos FROM monthly_ads ORDER BY created_at DESC LIMIT 5')
    for row in samples:
        row['user_id'] = '[redacted]'
        print(f'  {row}')
print('\nUpload logs (recent 3):')
logs = fetch_all('SELECT user_email, option_used, file_name FROM user_data_logs ORDER BY created_at DESC LIMIT 3')
for log in logs:
    print(f'  {log}')

