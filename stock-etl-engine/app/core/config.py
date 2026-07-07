import os
from dotenv import load_dotenv

load_dotenv()


def _get_int_env(key: str, default: int) -> int:
    raw = os.environ.get(key)
    if raw is None or raw == "":
        return default
    return int(raw)


DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": _get_int_env("DB_PORT", 5432),
    "dbname": os.environ.get("DB_NAME", "stock_cache_system"),
    "user": os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", ""),
}

LOG_DIR = os.environ.get("SYNC_LOG_DIR", "/app/logs")
ETL_API_PORT = _get_int_env("ETL_API_PORT", 8082)
ETL_API_KEY = os.environ.get("ETL_ENGINE_API_KEY", "")
ETL_VERSION = "1.0.8"


def validate_config():
    """Validate required DB configuration values."""
    missing = [k for k, v in DB_CONFIG.items() if not v and k != "port"]
    if missing:
        raise ValueError(f"Missing required DB config: {', '.join(missing)}")
