from datetime import datetime
from pathlib import Path
from flask import Flask
from .config import CONFIG_MAP
from .db import ensure_schema


def create_app(env: str = 'production') -> Flask:
    app = Flask(__name__)
    app.config.from_object(CONFIG_MAP.get(env, CONFIG_MAP['production']))
    app_root = Path(__file__).resolve().parents[1]
    upload_folder = Path(str(app.config.get('UPLOAD_FOLDER') or 'uploads'))
    if not upload_folder.is_absolute():
        upload_folder = (app_root / upload_folder).resolve()
    upload_folder.mkdir(parents=True, exist_ok=True)
    app.config['UPLOAD_FOLDER'] = str(upload_folder)

    from .routes.main import main_bp
    from .routes.upload import upload_bp
    from .routes.reports import reports_bp

    app.register_blueprint(main_bp)
    app.register_blueprint(upload_bp)
    app.register_blueprint(reports_bp)

    @app.context_processor
    def inject_globals():
        return {
            'now': datetime.utcnow(),
        }

    with app.app_context():
        try:
            ensure_schema()
        except Exception as exc:  # noqa: BLE001
            app.logger.warning('Schema auto-init skipped due to DB error: %s', exc)

    return app
