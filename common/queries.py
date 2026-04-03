import pandas as pd
import streamlit as st
from snowflake.snowpark import Session

SCORE_TABLE = "HACKATHON_APP.RESILIENCE.JEONSE_SAFETY_SCORE"
BASE_VIEW = "HACKATHON_APP.RESILIENCE.RESILIENCE_BASE"


@st.cache_data(show_spinner=False)
def load_scores(_session: Session) -> pd.DataFrame:
    query = f"""
        SELECT
            SGG,
            EMD,
            MEME_LATEST,
            JEONSE_LATEST,
            JEONSE_RATE,
            JEONSE_DROP_PCT,
            HUG_RATE,
            POP_CHANGE_PCT,
            AVG_ASSET,
            AVG_DISTANCE_M,
            S1_RATE,
            S2_DROP,
            S3_HUG,
            S4_POP,
            S5_ASSET,
            S6_SUBWAY,
            TOTAL_SCORE,
            GRADE
        FROM {SCORE_TABLE}
        ORDER BY TOTAL_SCORE DESC, SGG, EMD
    """
    return _session.sql(query).to_pandas()


@st.cache_data(show_spinner=False)
def load_grade_summary(_session: Session) -> pd.DataFrame:
    query = f"""
        SELECT GRADE, COUNT(*) AS AREA_COUNT
        FROM {SCORE_TABLE}
        GROUP BY GRADE
        ORDER BY GRADE
    """
    return _session.sql(query).to_pandas()


@st.cache_data(show_spinner=False)
def load_area_history(_session: Session, sgg: str, emd: str) -> pd.DataFrame:
    safe_sgg = sgg.replace("'", "''")
    safe_emd = emd.replace("'", "''")
    query = f"""
        SELECT
            YYYYMMDD,
            PRICE,
            JEONSE_PRICE
        FROM {BASE_VIEW}
        WHERE SGG = '{safe_sgg}'
          AND EMD = '{safe_emd}'
        ORDER BY YYYYMMDD
    """
    return _session.sql(query).to_pandas()


@st.cache_data(show_spinner=False)
def load_market_summary(_session: Session) -> pd.DataFrame:
    query = f"""
        SELECT
            SGG,
            EMD,
            MIN(YYYYMMDD) AS START_DATE,
            MAX(YYYYMMDD) AS END_DATE,
            AVG(PRICE) AS AVG_SALE_PRICE,
            AVG(JEONSE_PRICE) AS AVG_JEONSE_PRICE
        FROM {BASE_VIEW}
        GROUP BY SGG, EMD
        ORDER BY SGG, EMD
    """
    return _session.sql(query).to_pandas()


@st.cache_data(show_spinner=False)
def load_latest_market_snapshot(_session: Session) -> pd.DataFrame:
    query = f"""
        WITH ranked AS (
            SELECT
                SGG,
                EMD,
                YYYYMMDD,
                PRICE,
                JEONSE_PRICE,
                AVG_ASSET,
                AVG_INCOME,
                AVG_CREDIT_SCORE,
                AVG_LOAN,
                RES_POP,
                WORK_POP,
                VISIT_POP,
                ROW_NUMBER() OVER (
                    PARTITION BY SGG, EMD
                    ORDER BY YYYYMMDD DESC
                ) AS RN
            FROM {BASE_VIEW}
        )
        SELECT
            SGG,
            EMD,
            YYYYMMDD,
            PRICE,
            JEONSE_PRICE,
            AVG_ASSET,
            AVG_INCOME,
            AVG_CREDIT_SCORE,
            AVG_LOAN,
            RES_POP,
            WORK_POP,
            VISIT_POP
        FROM ranked
        WHERE RN = 1
        ORDER BY SGG, EMD
    """
    return _session.sql(query).to_pandas()


@st.cache_data(show_spinner=False)
def load_all_area_history(_session: Session) -> pd.DataFrame:
    query = f"""
        SELECT
            SGG,
            EMD,
            YYYYMMDD,
            PRICE,
            JEONSE_PRICE
        FROM {BASE_VIEW}
        ORDER BY SGG, EMD, YYYYMMDD
    """
    return _session.sql(query).to_pandas()
