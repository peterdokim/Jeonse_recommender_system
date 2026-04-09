from typing import Any, Dict

import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session


def _local_connection_config() -> Dict[str, Any]:
    if "snowflake" not in st.secrets:
        raise RuntimeError(
            "Missing Streamlit secrets. Create .streamlit/secrets.toml "
            "from .streamlit/secrets.toml.example before running locally."
        )

    snowflake_secrets = st.secrets["snowflake"]
    required = ["account", "user", "password", "warehouse", "database", "schema"]
    missing = [key for key in required if key not in snowflake_secrets]
    if missing:
        raise RuntimeError(
            "Missing Snowflake secrets: " + ", ".join(missing)
        )

    config = {key: snowflake_secrets[key] for key in required}
    if "role" in snowflake_secrets:
        config["role"] = snowflake_secrets["role"]

    return config


def _create_local_session() -> Session:
    config = _local_connection_config()
    session = Session.builder.configs(config).create()
    if "role" in config:
        session.sql(f'USE ROLE "{config["role"]}"').collect()
    session.sql(f'USE WAREHOUSE "{config["warehouse"]}"').collect()
    session.sql(f'USE DATABASE "{config["database"]}"').collect()
    session.sql(f'USE SCHEMA "{config["schema"]}"').collect()
    return session


@st.cache_resource(show_spinner=False)
def get_snowpark_session() -> Session:
    try:
        return get_active_session()
    except Exception:
        return _create_local_session()


def get_safe_session() -> Session:
    """만료된 세션을 감지하고 자동 재연결한다."""
    session = get_snowpark_session()
    try:
        session.sql("SELECT 1").collect()
        return session
    except Exception:
        st.cache_resource.clear()
        return get_snowpark_session()
