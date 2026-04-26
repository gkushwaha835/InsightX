import os
from pathlib import Path

from dotenv import load_dotenv

from .db import normalize_database_url

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(dotenv_path=PROJECT_ROOT / '.env', override=True)


class BaseConfig:
    SECRET_KEY = os.getenv('SECRET_KEY', 'replace-this-secret')
    DATABASE_URL = normalize_database_url(
        os.getenv(
            'DATABASE_URL',
            'postgresql://postgres:postgres@localhost:5432/amazon_ads_analytics',
        )
    )
    UPLOAD_FOLDER = os.getenv('UPLOAD_FOLDER', 'uploads')
    MAX_CONTENT_LENGTH = int(os.getenv('MAX_CONTENT_LENGTH', str(50 * 1024 * 1024)))
    SMTP_HOST = os.getenv('SMTP_HOST', '')
    SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
    SMTP_USERNAME = os.getenv('SMTP_USERNAME', '')
    SMTP_PASSWORD = os.getenv('SMTP_PASSWORD', '')
    SMTP_FROM_EMAIL = os.getenv('SMTP_FROM_EMAIL', '')
    SMTP_FROM_NAME = os.getenv('SMTP_FROM_NAME', 'InsightX Lite')
    SMTP_USE_TLS = os.getenv('SMTP_USE_TLS', '1')
    SMTP_USE_SSL = os.getenv('SMTP_USE_SSL', '0')
    GOOGLE_CLIENT_ID = os.getenv('GOOGLE_CLIENT_ID', '')
    GOOGLE_CLIENT_SECRET = os.getenv('GOOGLE_CLIENT_SECRET', '')


class DevelopmentConfig(BaseConfig):
    DEBUG = True


class ProductionConfig(BaseConfig):
    DEBUG = False


CONFIG_MAP = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
}
