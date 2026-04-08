from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOCAL_SECRETS_PATH = PROJECT_ROOT / ".streamlit" / "secrets.toml"


def load_local_secrets(path: Path | None = None) -> dict[str, Any]:
    target = path or LOCAL_SECRETS_PATH
    if not target.exists():
        return {}

    with target.open("rb") as handle:
        loaded = tomllib.load(handle)

    return loaded if isinstance(loaded, dict) else {}


def get_secret_section(section_name: str) -> dict[str, Any]:
    secrets = load_local_secrets()
    section = secrets.get(section_name, {})
    return section if isinstance(section, dict) else {}


def get_local_snowflake_connection_config() -> dict[str, Any]:
    secrets = get_secret_section("snowflake")
    env_overrides = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
    }

    config: dict[str, Any] = {}
    for key, env_value in env_overrides.items():
        if env_value:
            config[key] = env_value
        elif key in secrets:
            config[key] = secrets[key]

    required = ["account", "user", "password", "warehouse", "database", "schema"]
    missing = [key for key in required if not config.get(key)]
    if missing:
        raise RuntimeError(
            "Missing Snowflake connection settings: "
            + ", ".join(missing)
            + ". Provide them in .streamlit/secrets.toml or environment variables."
        )

    return config


def get_public_data_service_key() -> str:
    env_value = os.getenv("PUBLIC_DATA_API_SERVICE_KEY")
    if env_value:
        return env_value

    secrets = get_secret_section("public_data_api")
    service_key = secrets.get("service_key")
    if isinstance(service_key, str) and service_key.strip():
        return service_key.strip()

    raise RuntimeError(
        "Missing public_data_api.service_key. Add it to .streamlit/secrets.toml "
        "or set PUBLIC_DATA_API_SERVICE_KEY."
    )


def get_public_data_target_config(target: str) -> dict[str, Any]:
    if not target:
        raise ValueError("target is required.")

    normalized = target.strip().lower()
    if normalized not in {"trade", "rent"}:
        raise ValueError("target must be one of: trade, rent")

    secrets = get_secret_section("public_data_api")
    target_section = secrets.get(normalized, {})
    target_section = target_section if isinstance(target_section, dict) else {}

    prefix = normalized.upper()
    endpoint = os.getenv(f"PUBLIC_DATA_API_{prefix}_ENDPOINT") or target_section.get("endpoint")
    service_key = os.getenv(f"PUBLIC_DATA_API_{prefix}_SERVICE_KEY") or target_section.get("service_key")

    if not endpoint:
        default_endpoints = {
            "trade": "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade",
            "rent": "https://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent",
        }
        endpoint = default_endpoints[normalized]

    if not service_key:
        service_key = os.getenv("PUBLIC_DATA_API_SERVICE_KEY") or secrets.get("service_key")

    if not service_key:
        raise RuntimeError(
            f"Missing service key for public_data_api.{normalized}.service_key. "
            "Add it to .streamlit/secrets.toml or set the matching environment variable."
        )

    return {
        "endpoint": str(endpoint).strip(),
        "service_key": str(service_key).strip(),
    }


def get_public_data_lawd_codes() -> list[str]:
    env_value = os.getenv("PUBLIC_DATA_API_LAWD_CODES")
    if env_value:
        return [code.strip() for code in env_value.split(",") if code.strip()]

    secrets = get_secret_section("public_data_api")
    lawd_codes = secrets.get("lawd_codes")
    if isinstance(lawd_codes, list):
        return [str(code).strip() for code in lawd_codes if str(code).strip()]
    if isinstance(lawd_codes, str):
        return [code.strip() for code in lawd_codes.split(",") if code.strip()]

    return []
