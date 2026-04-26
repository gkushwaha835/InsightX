# Amazon Ads Analytics (Python + PostgreSQL)

This is a full Python/PostgreSQL version of your Amazon Ads Analytics project, with PHP route parity so existing flows can be opened on the same endpoint names.

## Stack
- Python 3.11+
- Flask
- psycopg (direct PostgreSQL)
- PostgreSQL
- Pandas/OpenPyXL

## Route parity
- `/index.php` (login)
- `/home.php`
- `/upload.php`
- `/dashboard.php`
- `/mom.php`
- `/wow.php`
- `/wow_dashboard.php`
- `/heatmap.php`
- `/wow_heatmap.php`
- `/advanced_feature.php`
- `/analyze_previous_ads.php`
- `/campaign_more_insights.php`
- `/campaign_performance_report.php`
- `/search_term_report.php`
- `/category.php`
- `/settings.php`
- `/logout.php`

## Setup
1. Create virtualenv and install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Copy `.env.example` to `.env` and update DB credentials.
3. Create database and run schema:
   ```bash
   psql -U postgres -d amazon_ads_analytics -f database/postgres_schema.sql
   ```
4. (Optional) Import old MySQL dump into PostgreSQL:
   ```bash
   python scripts/import_mysql_dump.py
   ```
5. Run app:
   ```bash
   python run.py
   ```

Default seeded admin:
- Username: `Kundan`
- Password: `admin123`

## Notes
- User roles, session-version invalidation, and settings module are included.
- Upload supports normalized monthly Excel files and stores data into `monthly_ads`.
- Reports are rendered through a shared analytics engine with month trends and dimension-level breakdowns.
