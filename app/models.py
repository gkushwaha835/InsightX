from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass
class User:
    id: int | None = None
    username: str = ''
    password: str = ''
    role: str = 'user'
    can_create_users: bool = False
    session_version: int = 1
    created_at: datetime | None = None
    updated_at: datetime | None = None

    def role_label(self) -> str:
        if self.role in ('co_admin', 'cd_admin'):
            return 'CO-ADMIN'
        if self.role == 'admin':
            return 'ADMIN'
        return 'USER'


@dataclass
class AppSetting:
    setting_key: str = ''
    setting_value: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass
class MonthlyAd:
    id: int | None = None
    user_id: int | None = None
    sku: str | None = None
    asin: str | None = None
    category: str | None = None
    month_name: str | None = None
    impressions: int = 0
    clicks: int = 0
    page_views: int = 0
    sessions: int = 0
    ctr: float = 0.0
    spend: float = 0.0
    sales: float = 0.0
    total_units: int = 0
    total_sales: float = 0.0
    acos: float = 0.0
    tacos: float = 0.0
    conversion_rate: float = 0.0
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass
class WeeklyAd:
    id: int | None = None
    user_id: int | None = None
    sku: str | None = None
    week_range: str | None = None
    spend: float = 0.0
    sales: float = 0.0
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass
class CategoryAd:
    id: int | None = None
    category: str | None = None
    sessions: int = 0
    page_views: int = 0
    ad_spend: float = 0.0
    ad_sales: float = 0.0
    total_units: int = 0
    total_sales: float = 0.0
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass
class UserDataLog:
    id: int | None = None
    user_email: str = ''
    phone_number: str = ''
    option_used: str = ''
    file_name: str = ''
    file_path: str = ''
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass
class CampaignReport:
    id: int | None = None
    user_id: int | None = None
    campaign_name: str | None = None
    campaign_id: str | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None
    impressions: int = 0
    clicks: int = 0
    spend: float = 0.0
    sales: float = 0.0
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass
class SearchTermReport:
    id: int | None = None
    user_id: int | None = None
    search_term: str | None = None
    impressions: int = 0
    clicks: int = 0
    spend: float = 0.0
    sales: float = 0.0
    created_at: datetime | None = None
    updated_at: datetime | None = None
