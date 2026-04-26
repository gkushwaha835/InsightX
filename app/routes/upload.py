from __future__ import annotations

from datetime import datetime
from pathlib import Path
from uuid import uuid4

from flask import Blueprint, current_app, flash, redirect, render_template, request, session, url_for
from werkzeug.utils import secure_filename

from ..auth import get_current_user, login_required
from ..db import upsert_app_settings
from ..services.upload_service import parse_monthly_ads_excel
from ..services.user_flow import issue_one_time_access, log_user_uploaded_file

upload_bp = Blueprint('upload', __name__)


def allowed_file(filename: str) -> bool:
    return filename.lower().endswith(('.xlsx', '.xls'))


def allowed_business_file(filename: str) -> bool:
    return filename.lower().endswith(('.xlsx', '.xls', '.csv'))


def _redirect_back_after_upload_post() -> object:
    source_page = (request.form.get('source_page') or '').strip().lower()
    if source_page == 'home':
        return redirect(url_for('main.home'))
    if source_page == 'ads_overview':
        return redirect(url_for('reports.ads_overview'))
    return redirect(url_for('upload.upload_page'))


@upload_bp.route('/upload', methods=['GET', 'POST'])
@login_required
def upload_page():
    user = get_current_user()
    if user is None:
        return redirect(url_for('main.login'))

    if request.method == 'POST':
        file = request.files.get('file')
        business_file = request.files.get('business_file')
        if not file or not file.filename:
            flash('Please choose the Ads bulk Excel file.', 'danger')
            return _redirect_back_after_upload_post()

        if not allowed_file(file.filename):
            flash('Only .xlsx or .xls files are supported.', 'danger')
            return _redirect_back_after_upload_post()

        if not business_file or not business_file.filename:
            flash('Please choose the Business report file.', 'danger')
            return _redirect_back_after_upload_post()

        if not allowed_business_file(business_file.filename):
            flash('Business report must be .xlsx, .xls, or .csv.', 'danger')
            return _redirect_back_after_upload_post()

        upload_root = Path(current_app.config['UPLOAD_FOLDER'])
        upload_root.mkdir(parents=True, exist_ok=True)

        ext = Path(file.filename).suffix.lower()
        safe_name = secure_filename(Path(file.filename).stem)
        target = upload_root / f'{safe_name}_{uuid4().hex}{ext}'
        file.save(target)
        log_user_uploaded_file('MOM Dashboard', target, file.filename)

        business_root = upload_root / 'business_reports'
        business_root.mkdir(parents=True, exist_ok=True)
        business_ext = Path(business_file.filename).suffix.lower()
        business_safe_name = secure_filename(Path(business_file.filename).stem)
        business_target = business_root / f'{business_safe_name}_{uuid4().hex}{business_ext}'
        business_file.save(business_target)
        log_user_uploaded_file('MOM Business Report', business_target, business_file.filename)

        inserted, message = parse_monthly_ads_excel(target, business_target, user.id)
        if inserted > 0:
            session['dashboard_access_granted'] = False
            session['ads_overview_access_granted'] = False
            try:
                upsert_app_settings(
                    {
                        'last_ads_analysis_mode': 'mom',
                        'last_ads_analysis_target': '/dashboard',
                        'last_ads_analysis_time': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    }
                )
            except Exception as exc:  # noqa: BLE001
                current_app.logger.warning('Skipping app_settings update after MOM upload: %s', exc)
            flash(message, 'success')
            # If upload was submitted from Ads Overview, keep user on that page
            # and show analysis there instead of redirecting to /dashboard.
            source_page = (request.form.get('source_page') or '').strip().lower()
            if source_page == 'ads_overview':
                access_token = issue_one_time_access('ads_overview')
                return redirect(url_for('reports.ads_overview', access_token=access_token, analyze=1))

            access_token = issue_one_time_access('dashboard')
            return redirect(url_for('reports.dashboard', analyze=1, access_token=access_token))
        else:
            flash(message, 'danger')

        return _redirect_back_after_upload_post()

    return render_template('upload.html', user=user)
