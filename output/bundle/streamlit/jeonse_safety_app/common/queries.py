import pandas as pd
import streamlit as st
from snowflake.snowpark import Session

SCORE_TABLE = "HACKATHON_APP.RESILIENCE.JEONSE_SAFETY_SCORE"
BASE_VIEW = "HACKATHON_APP.RESILIENCE.RESILIENCE_BASE"


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
def load_scores(_session: Session) -> pd.DataFrame:
    query = f"""
        WITH latest_snapshot AS (
            SELECT
                SGG,
                EMD,
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
            s.SGG,
            s.EMD,
            s.MEME_LATEST,
            s.JEONSE_LATEST,
            s.JEONSE_RATE,
            s.JEONSE_DROP_PCT,
            s.HUG_RATE,
            s.NET_MIG,
            s.SUBWAY_DIST,
            s.S_RATE,
            s.S_MIG,
            s.S_SUB,
            s.TOTAL_SCORE,
            s.GRADE,
            b.AVG_ASSET,
            b.AVG_INCOME,
            b.AVG_CREDIT_SCORE,
            b.AVG_LOAN,
            b.RES_POP,
            b.WORK_POP,
            b.VISIT_POP
        FROM {SCORE_TABLE} s
        LEFT JOIN latest_snapshot b
            ON s.SGG = b.SGG
           AND s.EMD = b.EMD
           AND b.RN = 1
        ORDER BY s.TOTAL_SCORE DESC, s.SGG, s.EMD
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
