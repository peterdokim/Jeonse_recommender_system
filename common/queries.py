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
            b.VISIT_POP,
            COALESCE(ml.ML_RISK_SCORE, 50.0) AS ML_RISK_SCORE,
            COALESCE(ml.ML_DROP_PROB, 0.5) AS ML_DROP_PROB
        FROM {SCORE_TABLE} s
        LEFT JOIN latest_snapshot b
            ON s.SGG = b.SGG
           AND s.EMD = b.EMD
           AND b.RN = 1
        LEFT JOIN HACKATHON_APP.RESILIENCE.ML_RISK_SCORES ml
            ON s.SGG = ml.SGG
           AND s.EMD = ml.EMD
        ORDER BY s.TOTAL_SCORE DESC, s.SGG, s.EMD
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


@st.cache_data(show_spinner=False, ttl=300)
def load_recent_transactions(_session: Session, sgg: str, emd: str, limit: int = 30) -> pd.DataFrame:
    """특정 동의 최근 실거래 내역 (매매 + 전세)."""
    safe_sgg = sgg.replace("'", "''")
    safe_emd = emd.replace("'", "''")
    query = f"""
        SELECT '매매' AS "거래유형", DEAL_DATE AS "거래일", APT_NM AS "단지명",
               ROUND(EXCL_AREA, 1) AS "면적(m²)",
               ROUND(EXCL_AREA / 3.305785, 0) AS "면적(평)",
               FLOOR AS "층", DEAL_AMOUNT AS "거래가(만원)",
               ROUND(PRICE_PER_PYEONG, 0) AS "평당가(만원)"
        FROM HACKATHON_APP.RESILIENCE.MOLIT_APT_TRADE_CLEAN
        WHERE SGG = '{safe_sgg}' AND EMD = '{safe_emd}'

        UNION ALL

        SELECT '전세',
               DEAL_DATE, APT_NM,
               ROUND(EXCL_AREA, 1),
               ROUND(EXCL_AREA / 3.305785, 0),
               FLOOR,
               DEPOSIT_AMOUNT,
               ROUND(DEPOSIT_PER_PYEONG, 0)
        FROM HACKATHON_APP.RESILIENCE.MOLIT_APT_RENT_CLEAN
        WHERE SGG = '{safe_sgg}' AND EMD = '{safe_emd}'
          AND COALESCE(MONTHLY_RENT_AMOUNT, 0) = 0

        ORDER BY "거래일" DESC
        LIMIT {limit}
    """
    return _session.sql(query).to_pandas()


@st.cache_data(show_spinner=False, ttl=300)
def load_complex_summary(_session: Session, sgg: str, emd: str) -> pd.DataFrame:
    """특정 동의 단지별 최근 매매/전세 요약."""
    safe_sgg = sgg.replace("'", "''")
    safe_emd = emd.replace("'", "''")
    query = f"""
        WITH trade_summary AS (
            SELECT APT_NM,
                   COUNT(*) AS "매매건수",
                   ROUND(MEDIAN(DEAL_AMOUNT), 0) AS "매매중위(만원)",
                   ROUND(MEDIAN(PRICE_PER_PYEONG), 0) AS "매매평당(만원)",
                   ROUND(MEDIAN(EXCL_AREA), 1) AS "주요면적(m²)",
                   MAX(DEAL_DATE) AS "최근매매일"
            FROM HACKATHON_APP.RESILIENCE.MOLIT_APT_TRADE_CLEAN
            WHERE SGG = '{safe_sgg}' AND EMD = '{safe_emd}'
              AND DEAL_DATE >= DATEADD(MONTH, -6, CURRENT_DATE())
            GROUP BY APT_NM
        ),
        rent_summary AS (
            SELECT APT_NM,
                   COUNT(*) AS "전세건수",
                   ROUND(MEDIAN(DEPOSIT_AMOUNT), 0) AS "전세중위(만원)",
                   ROUND(MEDIAN(DEPOSIT_PER_PYEONG), 0) AS "전세평당(만원)",
                   MAX(DEAL_DATE) AS "최근전세일"
            FROM HACKATHON_APP.RESILIENCE.MOLIT_APT_RENT_CLEAN
            WHERE SGG = '{safe_sgg}' AND EMD = '{safe_emd}'
              AND COALESCE(MONTHLY_RENT_AMOUNT, 0) = 0
              AND DEAL_DATE >= DATEADD(MONTH, -6, CURRENT_DATE())
            GROUP BY APT_NM
        )
        SELECT
            COALESCE(t.APT_NM, r.APT_NM) AS "단지명",
            t."매매건수", t."매매중위(만원)", t."매매평당(만원)",
            r."전세건수", r."전세중위(만원)", r."전세평당(만원)",
            t."주요면적(m²)",
            CASE WHEN t."매매중위(만원)" > 0 AND r."전세중위(만원)" > 0
                 THEN ROUND(r."전세중위(만원)" / t."매매중위(만원)" * 100, 1)
                 ELSE NULL END AS "전세가율(%)"
        FROM trade_summary t
        FULL OUTER JOIN rent_summary r ON t.APT_NM = r.APT_NM
        ORDER BY COALESCE(t."매매건수", 0) + COALESCE(r."전세건수", 0) DESC
    """
    return _session.sql(query).to_pandas()
