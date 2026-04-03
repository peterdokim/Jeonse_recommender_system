from typing import Any, Dict

import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session


def _local_connection_config() -> Dict[str, Any]:
    if "snowflake" not in st.secrets:
        raise RuntimeError(
            "Missing Streamlit secrets. Create snowflake/.streamlit/secrets.toml "
            "from secrets.toml.example before running locally."
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


@st.cache_resource(show_spinner=False)
def get_snowpark_session() -> Session:
    try:
        return get_active_session()
    except Exception:
        config = _local_connection_config()
        session = Session.builder.configs(config).create()

        if "role" in config:
            session.sql(f'USE ROLE "{config["role"]}"').collect()
        session.sql(f'USE WAREHOUSE "{config["warehouse"]}"').collect()
        session.sql(f'USE DATABASE "{config["database"]}"').collect()
        session.sql(f'USE SCHEMA "{config["schema"]}"').collect()

        return session
