-- ============================================================
-- Public MOLIT apartment sale/rent setup for the Streamlit app
--
-- What this script does
-- 1. Creates raw landing tables for the public MOLIT apartment APIs.
-- 2. Creates Seoul lawd-code mapping and HUG accident reference data.
-- 3. Builds monthly sale/jeonse aggregates from the raw public tables.
-- 4. Recreates RESILIENCE_BASE and JEONSE_SAFETY_SCORE so the app
--    can keep using the same query interface as before.
--
-- Before running the app
-- 1. Run this SQL once.
-- 2. Populate RAW_MOLIT_APT_TRADE and RAW_MOLIT_APT_RENT with:
--      python scripts/load_molit_transactions.py
-- ============================================================

CREATE DATABASE IF NOT EXISTS HACKATHON_APP;
CREATE SCHEMA IF NOT EXISTS HACKATHON_APP.RESILIENCE;

-- ============================================================
-- Public reference tables
-- ============================================================

CREATE OR REPLACE TABLE HACKATHON_APP.RESILIENCE.LAWD_CODE_MASTER (
    LAWD_CD VARCHAR(5),
    SIDO VARCHAR,
    SGG VARCHAR
);

INSERT INTO HACKATHON_APP.RESILIENCE.LAWD_CODE_MASTER (LAWD_CD, SIDO, SGG) VALUES
    ('11110', '서울특별시', '종로구'),
    ('11140', '서울특별시', '중구'),
    ('11170', '서울특별시', '용산구'),
    ('11200', '서울특별시', '성동구'),
    ('11215', '서울특별시', '광진구'),
    ('11230', '서울특별시', '동대문구'),
    ('11260', '서울특별시', '중랑구'),
    ('11290', '서울특별시', '성북구'),
    ('11305', '서울특별시', '강북구'),
    ('11320', '서울특별시', '도봉구'),
    ('11350', '서울특별시', '노원구'),
    ('11380', '서울특별시', '은평구'),
    ('11410', '서울특별시', '서대문구'),
    ('11440', '서울특별시', '마포구'),
    ('11470', '서울특별시', '양천구'),
    ('11500', '서울특별시', '강서구'),
    ('11530', '서울특별시', '구로구'),
    ('11545', '서울특별시', '금천구'),
    ('11560', '서울특별시', '영등포구'),
    ('11590', '서울특별시', '동작구'),
    ('11620', '서울특별시', '관악구'),
    ('11650', '서울특별시', '서초구'),
    ('11680', '서울특별시', '강남구'),
    ('11710', '서울특별시', '송파구'),
    ('11740', '서울특별시', '강동구');

CREATE OR REPLACE TABLE HACKATHON_APP.RESILIENCE.HUG_ACCIDENT (
    GU_CODE VARCHAR(5),
    GU_NAME VARCHAR,
    ACCIDENT_COUNT NUMBER,
    ACCIDENT_AMOUNT NUMBER,
    ACCIDENT_RATE FLOAT
);

INSERT INTO HACKATHON_APP.RESILIENCE.HUG_ACCIDENT
    (GU_CODE, GU_NAME, ACCIDENT_COUNT, ACCIDENT_AMOUNT, ACCIDENT_RATE)
VALUES
    ('11110', '종로구', 3, 527000000, 1.0),
    ('11140', '중구', 2, 350000000, 0.4),
    ('11170', '용산구', 5, 1290000000, 0.7),
    ('11200', '성동구', 1, 300000000, 0.2),
    ('11215', '광진구', 10, 2350000000, 1.4),
    ('11230', '동대문구', 9, 2033690000, 1.0),
    ('11260', '중랑구', 24, 4903750000, 3.3),
    ('11290', '성북구', 5, 1445500000, 0.8),
    ('11305', '강북구', 6, 1365000000, 1.5),
    ('11320', '도봉구', 8, 1600000000, 1.3),
    ('11350', '노원구', 6, 1396750000, 0.6),
    ('11380', '은평구', 6, 1752000000, 0.6),
    ('11410', '서대문구', 7, 1682000000, 1.2),
    ('11440', '마포구', 4, 760000000, 0.3),
    ('11470', '양천구', 18, 4477000000, 1.8),
    ('11500', '강서구', 84, 18031270000, 2.9),
    ('11530', '구로구', 21, 4129400000, 2.1),
    ('11545', '금천구', 9, 1556000000, 1.0),
    ('11560', '영등포구', 30, 7456770000, 1.8),
    ('11590', '동작구', 5, 1111000000, 0.6),
    ('11620', '관악구', 16, 4199500000, 2.7),
    ('11650', '서초구', 5, 1573500000, 0.8),
    ('11680', '강남구', 5, 1585000000, 0.6),
    ('11710', '송파구', 16, 3253500000, 0.8),
    ('11740', '강동구', 13, 2727500000, 1.1);

-- ============================================================
-- Raw landing tables for the public APIs
-- ============================================================

CREATE TABLE IF NOT EXISTS HACKATHON_APP.RESILIENCE.RAW_MOLIT_APT_TRADE (
    LOAD_BATCH_ID VARCHAR,
    LOADED_AT TIMESTAMP_NTZ,
    SOURCE_MONTH VARCHAR(6),
    LAWD_CD VARCHAR(5),
    DEAL_YEAR NUMBER(4, 0),
    DEAL_MONTH NUMBER(2, 0),
    DEAL_DAY NUMBER(2, 0),
    DEAL_DATE DATE,
    SGG_CD VARCHAR(5),
    UMD_NM VARCHAR,
    APT_NM VARCHAR,
    APT_DONG VARCHAR,
    JIBUN VARCHAR,
    EXCL_AREA FLOAT,
    FLOOR VARCHAR,
    BUILD_YEAR NUMBER(4, 0),
    DEAL_AMOUNT NUMBER(18, 0),
    DEAL_AMOUNT_KRW NUMBER(18, 0),
    REGISTER_DATE DATE,
    CANCEL_YN VARCHAR,
    CANCEL_DATE DATE,
    BUYER_GBN VARCHAR,
    SELLER_GBN VARCHAR,
    ESTATE_AGENT_SGG_NM VARCHAR,
    LAND_LEASEHOLD_GBN VARCHAR,
    RAW_ITEM_JSON VARCHAR,
    UNIQUE_KEY VARCHAR
);

CREATE TABLE IF NOT EXISTS HACKATHON_APP.RESILIENCE.RAW_MOLIT_APT_RENT (
    LOAD_BATCH_ID VARCHAR,
    LOADED_AT TIMESTAMP_NTZ,
    SOURCE_MONTH VARCHAR(6),
    LAWD_CD VARCHAR(5),
    DEAL_YEAR NUMBER(4, 0),
    DEAL_MONTH NUMBER(2, 0),
    DEAL_DAY NUMBER(2, 0),
    DEAL_DATE DATE,
    SGG_CD VARCHAR(5),
    UMD_NM VARCHAR,
    APT_NM VARCHAR,
    APT_DONG VARCHAR,
    JIBUN VARCHAR,
    EXCL_AREA FLOAT,
    FLOOR VARCHAR,
    BUILD_YEAR NUMBER(4, 0),
    DEPOSIT_AMOUNT NUMBER(18, 0),
    DEPOSIT_AMOUNT_KRW NUMBER(18, 0),
    MONTHLY_RENT_AMOUNT NUMBER(18, 0),
    MONTHLY_RENT_AMOUNT_KRW NUMBER(18, 0),
    CONTRACT_TYPE VARCHAR,
    CONTRACT_TERM VARCHAR,
    USE_RR_RIGHT VARCHAR,
    PREV_DEPOSIT_AMOUNT NUMBER(18, 0),
    PREV_MONTHLY_RENT_AMOUNT NUMBER(18, 0),
    ESTATE_AGENT_SGG_NM VARCHAR,
    RAW_ITEM_JSON VARCHAR,
    UNIQUE_KEY VARCHAR
);

-- ============================================================
-- Cleansed and aggregated public views
-- ============================================================

CREATE OR REPLACE VIEW HACKATHON_APP.RESILIENCE.MOLIT_APT_TRADE_CLEAN AS
SELECT
    code.SGG,
    NULLIF(TRIM(raw.UMD_NM), '') AS EMD,
    DATE_TRUNC('MONTH', raw.DEAL_DATE)::DATE AS YYYYMMDD,
    raw.DEAL_DATE,
    raw.APT_NM,
    raw.APT_DONG,
    raw.JIBUN,
    raw.EXCL_AREA,
    raw.FLOOR,
    raw.BUILD_YEAR,
    raw.DEAL_AMOUNT,
    raw.DEAL_AMOUNT_KRW,
    ROUND(raw.DEAL_AMOUNT / NULLIF(raw.EXCL_AREA / 3.305785, 0), 1) AS PRICE_PER_PYEONG,
    raw.CANCEL_YN
FROM HACKATHON_APP.RESILIENCE.RAW_MOLIT_APT_TRADE raw
JOIN HACKATHON_APP.RESILIENCE.LAWD_CODE_MASTER code
    ON code.LAWD_CD = COALESCE(NULLIF(raw.SGG_CD, ''), raw.LAWD_CD)
WHERE raw.DEAL_DATE IS NOT NULL
  AND raw.DEAL_AMOUNT IS NOT NULL
  AND raw.EXCL_AREA > 0
  AND NULLIF(TRIM(raw.UMD_NM), '') IS NOT NULL
  AND COALESCE(NULLIF(TRIM(raw.CANCEL_YN), ''), 'N') NOT IN ('Y', '1');

CREATE OR REPLACE VIEW HACKATHON_APP.RESILIENCE.MOLIT_APT_RENT_CLEAN AS
SELECT
    code.SGG,
    NULLIF(TRIM(raw.UMD_NM), '') AS EMD,
    DATE_TRUNC('MONTH', raw.DEAL_DATE)::DATE AS YYYYMMDD,
    raw.DEAL_DATE,
    raw.APT_NM,
    raw.APT_DONG,
    raw.JIBUN,
    raw.EXCL_AREA,
    raw.FLOOR,
    raw.BUILD_YEAR,
    raw.DEPOSIT_AMOUNT,
    raw.DEPOSIT_AMOUNT_KRW,
    raw.MONTHLY_RENT_AMOUNT,
    raw.MONTHLY_RENT_AMOUNT_KRW,
    raw.CONTRACT_TYPE,
    ROUND(raw.DEPOSIT_AMOUNT / NULLIF(raw.EXCL_AREA / 3.305785, 0), 1) AS DEPOSIT_PER_PYEONG
FROM HACKATHON_APP.RESILIENCE.RAW_MOLIT_APT_RENT raw
JOIN HACKATHON_APP.RESILIENCE.LAWD_CODE_MASTER code
    ON code.LAWD_CD = COALESCE(NULLIF(raw.SGG_CD, ''), raw.LAWD_CD)
WHERE raw.DEAL_DATE IS NOT NULL
  AND raw.DEPOSIT_AMOUNT IS NOT NULL
  AND raw.EXCL_AREA > 0
  AND NULLIF(TRIM(raw.UMD_NM), '') IS NOT NULL;

CREATE OR REPLACE VIEW HACKATHON_APP.RESILIENCE.MOLIT_APT_TRADE_MONTHLY AS
SELECT
    SGG,
    EMD,
    YYYYMMDD,
    MEDIAN(PRICE_PER_PYEONG) AS PRICE,
    MEDIAN(DEAL_AMOUNT) AS MEME_MEDIAN_AMOUNT_MWAN,
    COUNT(*) AS TRADE_COUNT
FROM HACKATHON_APP.RESILIENCE.MOLIT_APT_TRADE_CLEAN
GROUP BY SGG, EMD, YYYYMMDD;

CREATE OR REPLACE VIEW HACKATHON_APP.RESILIENCE.MOLIT_APT_RENT_MONTHLY AS
SELECT
    SGG,
    EMD,
    YYYYMMDD,
    COUNT(*) AS RENT_COUNT,
    COUNT_IF(COALESCE(MONTHLY_RENT_AMOUNT, 0) = 0) AS JEONSE_COUNT,
    MEDIAN(CASE WHEN COALESCE(MONTHLY_RENT_AMOUNT, 0) = 0 THEN DEPOSIT_PER_PYEONG END) AS JEONSE_PRICE,
    MEDIAN(CASE WHEN COALESCE(MONTHLY_RENT_AMOUNT, 0) = 0 THEN DEPOSIT_AMOUNT END) AS JEONSE_MEDIAN_AMOUNT_MWAN
FROM HACKATHON_APP.RESILIENCE.MOLIT_APT_RENT_CLEAN
GROUP BY SGG, EMD, YYYYMMDD;

-- RESILIENCE_BASE keeps the original column interface expected by the app.
CREATE OR REPLACE VIEW HACKATHON_APP.RESILIENCE.RESILIENCE_BASE AS
WITH merged AS (
    SELECT
        COALESCE(t.SGG, r.SGG) AS SGG,
        COALESCE(t.EMD, r.EMD) AS EMD,
        COALESCE(t.YYYYMMDD, r.YYYYMMDD) AS YYYYMMDD,
        t.PRICE,
        r.JEONSE_PRICE
    FROM HACKATHON_APP.RESILIENCE.MOLIT_APT_TRADE_MONTHLY t
    FULL OUTER JOIN HACKATHON_APP.RESILIENCE.MOLIT_APT_RENT_MONTHLY r
        ON t.SGG = r.SGG
       AND t.EMD = r.EMD
       AND t.YYYYMMDD = r.YYYYMMDD
)
SELECT
    SGG,
    EMD,
    YYYYMMDD,
    PRICE,
    JEONSE_PRICE,
    CAST(NULL AS FLOAT) AS AVG_ASSET,
    CAST(NULL AS FLOAT) AS AVG_INCOME,
    CAST(NULL AS FLOAT) AS AVG_CREDIT_SCORE,
    CAST(NULL AS FLOAT) AS AVG_LOAN,
    CAST(NULL AS FLOAT) AS RES_POP,
    CAST(NULL AS FLOAT) AS WORK_POP,
    CAST(NULL AS FLOAT) AS VISIT_POP
FROM merged
WHERE SGG IS NOT NULL
  AND EMD IS NOT NULL
  AND (PRICE IS NOT NULL OR JEONSE_PRICE IS NOT NULL);

-- Notes on legacy column names:
--   NET_MIG     -> recent 6-month transaction count proxy
--   SUBWAY_DIST -> recent 12-month jeonse-rate volatility proxy
-- These names are kept so the existing app code can continue to query
-- the same columns without any frontend changes.
CREATE OR REPLACE VIEW HACKATHON_APP.RESILIENCE.JEONSE_SAFETY_SCORE AS
WITH monthly AS (
    SELECT
        base.SGG,
        base.EMD,
        base.YYYYMMDD,
        base.PRICE,
        base.JEONSE_PRICE,
        COALESCE(trade.TRADE_COUNT, 0) AS TRADE_COUNT,
        COALESCE(rent.RENT_COUNT, 0) AS RENT_COUNT,
        CASE
            WHEN base.PRICE > 0 AND base.JEONSE_PRICE > 0
                THEN base.JEONSE_PRICE / base.PRICE * 100
            ELSE NULL
        END AS JEONSE_RATE,
        LAG(base.JEONSE_PRICE, 12) OVER (
            PARTITION BY base.SGG, base.EMD
            ORDER BY base.YYYYMMDD
        ) AS JEONSE_PRICE_12M_AGO,
        SUM(COALESCE(trade.TRADE_COUNT, 0) + COALESCE(rent.RENT_COUNT, 0)) OVER (
            PARTITION BY base.SGG, base.EMD
            ORDER BY base.YYYYMMDD
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS RECENT_TX_COUNT,
        STDDEV_POP(
            CASE
                WHEN base.PRICE > 0 AND base.JEONSE_PRICE > 0
                    THEN base.JEONSE_PRICE / base.PRICE * 100
                ELSE NULL
            END
        ) OVER (
            PARTITION BY base.SGG, base.EMD
            ORDER BY base.YYYYMMDD
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS RATE_VOLATILITY,
        ROW_NUMBER() OVER (
            PARTITION BY base.SGG, base.EMD
            ORDER BY base.YYYYMMDD DESC
        ) AS RN
    FROM HACKATHON_APP.RESILIENCE.RESILIENCE_BASE base
    LEFT JOIN HACKATHON_APP.RESILIENCE.MOLIT_APT_TRADE_MONTHLY trade
        ON base.SGG = trade.SGG
       AND base.EMD = trade.EMD
       AND base.YYYYMMDD = trade.YYYYMMDD
    LEFT JOIN HACKATHON_APP.RESILIENCE.MOLIT_APT_RENT_MONTHLY rent
        ON base.SGG = rent.SGG
       AND base.EMD = rent.EMD
       AND base.YYYYMMDD = rent.YYYYMMDD
    WHERE base.PRICE IS NOT NULL
      AND base.JEONSE_PRICE IS NOT NULL
),
latest AS (
    SELECT
        SGG,
        EMD,
        ROUND(PRICE, 0) AS MEME_LATEST,
        ROUND(JEONSE_PRICE, 0) AS JEONSE_LATEST,
        ROUND(JEONSE_RATE, 1) AS JEONSE_RATE,
        ROUND(
            COALESCE(
                (JEONSE_PRICE - JEONSE_PRICE_12M_AGO)
                / NULLIF(JEONSE_PRICE_12M_AGO, 0) * 100,
                0
            ),
            1
        ) AS JEONSE_DROP_PCT,
        COALESCE(RECENT_TX_COUNT, 0) AS NET_MIG,
        COALESCE(RATE_VOLATILITY, 0) AS SUBWAY_DIST
    FROM monthly
    WHERE RN = 1
),
scored AS (
    SELECT
        latest.*,
        ROUND(
            PERCENT_RANK() OVER (
                ORDER BY COALESCE(JEONSE_RATE, 999999) DESC
            ) * 100,
            1
        ) AS S_RATE,
        ROUND(
            PERCENT_RANK() OVER (
                ORDER BY COALESCE(NET_MIG, -1) ASC
            ) * 100,
            1
        ) AS S_MIG,
        ROUND(
            PERCENT_RANK() OVER (
                ORDER BY COALESCE(SUBWAY_DIST, 999999) DESC
            ) * 100,
            1
        ) AS S_SUB
    FROM latest
),
hug AS (
    SELECT GU_NAME, ACCIDENT_RATE
    FROM HACKATHON_APP.RESILIENCE.HUG_ACCIDENT
)
SELECT
    scored.SGG,
    scored.EMD,
    scored.MEME_LATEST,
    scored.JEONSE_LATEST,
    scored.JEONSE_RATE,
    scored.JEONSE_DROP_PCT,
    hug.ACCIDENT_RATE AS HUG_RATE,
    scored.NET_MIG,
    scored.SUBWAY_DIST,
    scored.S_RATE,
    scored.S_MIG,
    scored.S_SUB,
    ROUND(scored.S_RATE * 0.50 + scored.S_MIG * 0.25 + scored.S_SUB * 0.25, 1) AS TOTAL_SCORE,
    CASE
        WHEN ROUND(scored.S_RATE * 0.50 + scored.S_MIG * 0.25 + scored.S_SUB * 0.25, 1) >= 80 THEN 'A'
        WHEN ROUND(scored.S_RATE * 0.50 + scored.S_MIG * 0.25 + scored.S_SUB * 0.25, 1) >= 60 THEN 'B'
        WHEN ROUND(scored.S_RATE * 0.50 + scored.S_MIG * 0.25 + scored.S_SUB * 0.25, 1) >= 40 THEN 'C'
        ELSE 'D'
    END AS GRADE
FROM scored
LEFT JOIN hug
    ON scored.SGG = hug.GU_NAME;

-- ============================================================
-- Quick checks
-- ============================================================

SELECT COUNT(*) AS TRADE_ROWS
FROM HACKATHON_APP.RESILIENCE.RAW_MOLIT_APT_TRADE;

SELECT COUNT(*) AS RENT_ROWS
FROM HACKATHON_APP.RESILIENCE.RAW_MOLIT_APT_RENT;

SELECT
    SGG,
    EMD,
    JEONSE_RATE,
    TOTAL_SCORE,
    GRADE
FROM HACKATHON_APP.RESILIENCE.JEONSE_SAFETY_SCORE
ORDER BY TOTAL_SCORE DESC, SGG, EMD;
