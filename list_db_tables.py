from app.db import fetch_all
print('All tables in calibray_free_audit:')
tables = fetch_all("""
SELECT schemaname, tablename FROM pg_tables 
WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
ORDER BY schemaname, tablename;
""")
for t in tables:
    print(f'  {t["schemaname"]}.{t["tablename"]}')
if not tables:
    print('No tables found - schema needed')
print()
print('Check monthly_ads:')
try:
    count = fetch_all('SELECT COUNT(*) as c FROM monthly_ads')[0]['c']
    print(f'monthly_ads rows: {count}')
except:
    print('monthly_ads table missing')

