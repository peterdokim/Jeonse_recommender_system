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

-- 평형대별 전세 평당가 (소형/중형/대형/특대)
CREATE OR REPLACE VIEW HACKATHON_APP.RESILIENCE.MOLIT_APT_RENT_BY_PYEONG AS
SELECT
    SGG,
    EMD,
    YYYYMMDD,
    CASE
        WHEN EXCL_AREA <= 50 THEN 'SMALL'
        WHEN EXCL_AREA <= 85 THEN 'MID'
        WHEN EXCL_AREA <= 135 THEN 'LARGE'
        ELSE 'XLARGE'
    END AS PYEONG_BUCKET,
    COUNT(*) AS BUCKET_RENT_COUNT,
    MEDIAN(DEPOSIT_PER_PYEONG) AS BUCKET_JEONSE_PRICE,
    MEDIAN(EXCL_AREA) AS BUCKET_MEDIAN_AREA
FROM HACKATHON_APP.RESILIENCE.MOLIT_APT_RENT_CLEAN
WHERE COALESCE(MONTHLY_RENT_AMOUNT, 0) = 0
  AND EXCL_AREA > 0
GROUP BY SGG, EMD, YYYYMMDD,
    CASE
        WHEN EXCL_AREA <= 50 THEN 'SMALL'
        WHEN EXCL_AREA <= 85 THEN 'MID'
        WHEN EXCL_AREA <= 135 THEN 'LARGE'
        ELSE 'XLARGE'
    END;

-- 평형대별 매매 평당가
CREATE OR REPLACE VIEW HACKATHON_APP.RESILIENCE.MOLIT_APT_TRADE_BY_PYEONG AS
SELECT
    SGG,
    EMD,
    YYYYMMDD,
    CASE
        WHEN EXCL_AREA <= 50 THEN 'SMALL'
        WHEN EXCL_AREA <= 85 THEN 'MID'
        WHEN EXCL_AREA <= 135 THEN 'LARGE'
        ELSE 'XLARGE'
    END AS PYEONG_BUCKET,
    COUNT(*) AS BUCKET_TRADE_COUNT,
    MEDIAN(PRICE_PER_PYEONG) AS BUCKET_SALE_PRICE
FROM HACKATHON_APP.RESILIENCE.MOLIT_APT_TRADE_CLEAN
WHERE EXCL_AREA > 0
GROUP BY SGG, EMD, YYYYMMDD,
    CASE
        WHEN EXCL_AREA <= 50 THEN 'SMALL'
        WHEN EXCL_AREA <= 85 THEN 'MID'
        WHEN EXCL_AREA <= 135 THEN 'LARGE'
        ELSE 'XLARGE'
    END;

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
),
-- SPH 데이터: 중구/영등포구/서초구만 제공 (구 단위 최신 월 집계)
sph_asset AS (
    SELECT
        m.CITY_KOR_NAME AS SGG,
        AVG(a.AVERAGE_ASSET_AMOUNT) AS AVG_ASSET,
        AVG(a.AVERAGE_INCOME) AS AVG_INCOME,
        AVG(a.AVERAGE_SCORE) AS AVG_CREDIT_SCORE,
        AVG(a.AVERAGE_BALANCE_AMOUNT) AS AVG_LOAN
    FROM SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.ASSET_INCOME_INFO a
    JOIN SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.M_SCCO_MST m
        ON a.DISTRICT_CODE = m.DISTRICT_CODE
    WHERE a.STANDARD_YEAR_MONTH = (
        SELECT MAX(STANDARD_YEAR_MONTH)
        FROM SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.ASSET_INCOME_INFO
    )
    GROUP BY m.CITY_KOR_NAME
),
sph_pop AS (
    SELECT
        m.CITY_KOR_NAME AS SGG,
        SUM(f.RESIDENTIAL_POPULATION) AS RES_POP,
        SUM(f.WORKING_POPULATION) AS WORK_POP,
        SUM(f.VISITING_POPULATION) AS VISIT_POP
    FROM SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.FLOATING_POPULATION_INFO f
    JOIN SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.M_SCCO_MST m
        ON f.DISTRICT_CODE = m.DISTRICT_CODE
    WHERE f.STANDARD_YEAR_MONTH = (
        SELECT MAX(STANDARD_YEAR_MONTH)
        FROM SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.FLOATING_POPULATION_INFO
    )
    GROUP BY m.CITY_KOR_NAME
)
SELECT
    merged.SGG,
    merged.EMD,
    merged.YYYYMMDD,
    merged.PRICE,
    merged.JEONSE_PRICE,
    sa.AVG_ASSET,
    sa.AVG_INCOME,
    sa.AVG_CREDIT_SCORE,
    sa.AVG_LOAN,
    sp.RES_POP,
    sp.WORK_POP,
    sp.VISIT_POP
FROM merged
LEFT JOIN sph_asset sa ON merged.SGG = sa.SGG
LEFT JOIN sph_pop sp ON merged.SGG = sp.SGG
WHERE merged.SGG IS NOT NULL
  AND merged.EMD IS NOT NULL
  AND (merged.PRICE IS NOT NULL OR merged.JEONSE_PRICE IS NOT NULL);

-- ============================================================
-- Feature layer: FEATURE_AREA_MONTH
-- 파이프라인: RAW → CLEAN → MONTHLY → FEATURE → SCORE → STREAMLIT
-- 월별 시계열에 파생 지표(전세가율, 변동률, 거래량, 변동성)를 계산한다.
-- ============================================================

CREATE OR REPLACE VIEW HACKATHON_APP.RESILIENCE.FEATURE_AREA_MONTH AS
SELECT
    base.SGG,
    base.EMD,
    base.YYYYMMDD,
    base.PRICE,
    base.JEONSE_PRICE,
    COALESCE(trade.TRADE_COUNT, 0) AS TRADE_COUNT,
    COALESCE(rent.RENT_COUNT, 0) AS RENT_COUNT,
    -- 전세가율
    CASE
        WHEN base.PRICE > 0 AND base.JEONSE_PRICE > 0
            THEN ROUND(base.JEONSE_PRICE / base.PRICE * 100, 1)
        ELSE NULL
    END AS JEONSE_RATE,
    -- 12개월 전 전세가 (변동률 계산용)
    LAG(base.JEONSE_PRICE, 12) OVER (
        PARTITION BY base.SGG, base.EMD
        ORDER BY base.YYYYMMDD
    ) AS JEONSE_PRICE_12M_AGO,
    -- 최근 6개월 거래 건수 (거래 활발도)
    SUM(COALESCE(trade.TRADE_COUNT, 0) + COALESCE(rent.RENT_COUNT, 0)) OVER (
        PARTITION BY base.SGG, base.EMD
        ORDER BY base.YYYYMMDD
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) AS RECENT_TX_COUNT,
    -- 12개월 전세가율 변동성 (가격 안정성)
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
    -- 최신 행 식별용
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
  AND base.JEONSE_PRICE IS NOT NULL;

-- ============================================================
-- Scoring layer: JEONSE_SAFETY_SCORE
-- FEATURE_AREA_MONTH의 최신 행을 읽어 백분위 점수 + 등급을 산출한다.
-- 컬럼명 NET_MIG, SUBWAY_DIST는 앱 호환용 alias.
-- ============================================================

CREATE OR REPLACE VIEW HACKATHON_APP.RESILIENCE.JEONSE_SAFETY_SCORE AS
WITH latest AS (
    SELECT
        SGG,
        EMD,
        ROUND(PRICE, 0) AS MEME_LATEST,
        ROUND(JEONSE_PRICE, 0) AS JEONSE_LATEST,
        JEONSE_RATE,
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
    FROM HACKATHON_APP.RESILIENCE.FEATURE_AREA_MONTH
    WHERE RN = 1
      AND (JEONSE_RATE IS NULL OR (JEONSE_RATE > 0 AND JEONSE_RATE <= 100))
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
        CASE
            WHEN NET_MIG < 10 THEN 50.0
            ELSE ROUND(
                PERCENT_RANK() OVER (
                    ORDER BY COALESCE(SUBWAY_DIST, 999999) DESC
                ) * 100,
                1
            )
        END AS S_SUB
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
-- 평형대별 최신 시세 (앱이 직접 조회)
-- ============================================================

-- 최근 6개월 거래의 평형대별 평당가 (raw 직접 집계로 거래량 충분히 확보)
CREATE OR REPLACE VIEW HACKATHON_APP.RESILIENCE.JEONSE_BY_PYEONG_LATEST AS
WITH rent6m AS (
    SELECT
        SGG, EMD,
        CASE
            WHEN EXCL_AREA <= 50 THEN 'SMALL'
            WHEN EXCL_AREA <= 85 THEN 'MID'
            WHEN EXCL_AREA <= 135 THEN 'LARGE'
            ELSE 'XLARGE'
        END AS PYEONG_BUCKET,
        DEPOSIT_PER_PYEONG,
        EXCL_AREA
    FROM HACKATHON_APP.RESILIENCE.MOLIT_APT_RENT_CLEAN
    WHERE COALESCE(MONTHLY_RENT_AMOUNT, 0) = 0
      AND EXCL_AREA > 0
      AND DEAL_DATE >= DATEADD(MONTH, -6, CURRENT_DATE())
),
trade6m AS (
    SELECT
        SGG, EMD,
        CASE
            WHEN EXCL_AREA <= 50 THEN 'SMALL'
            WHEN EXCL_AREA <= 85 THEN 'MID'
            WHEN EXCL_AREA <= 135 THEN 'LARGE'
            ELSE 'XLARGE'
        END AS PYEONG_BUCKET,
        PRICE_PER_PYEONG
    FROM HACKATHON_APP.RESILIENCE.MOLIT_APT_TRADE_CLEAN
    WHERE EXCL_AREA > 0
      AND DEAL_DATE >= DATEADD(MONTH, -6, CURRENT_DATE())
),
rent_agg AS (
    SELECT SGG, EMD, PYEONG_BUCKET,
           COUNT(*) AS BUCKET_RENT_COUNT,
           ROUND(MEDIAN(DEPOSIT_PER_PYEONG), 0) AS BUCKET_JEONSE_PRICE,
           ROUND(MEDIAN(EXCL_AREA), 1) AS BUCKET_MEDIAN_AREA
    FROM rent6m
    GROUP BY SGG, EMD, PYEONG_BUCKET
),
trade_agg AS (
    SELECT SGG, EMD, PYEONG_BUCKET,
           COUNT(*) AS BUCKET_TRADE_COUNT,
           ROUND(MEDIAN(PRICE_PER_PYEONG), 0) AS BUCKET_SALE_PRICE
    FROM trade6m
    GROUP BY SGG, EMD, PYEONG_BUCKET
)
SELECT
    r.SGG, r.EMD, r.PYEONG_BUCKET,
    r.BUCKET_JEONSE_PRICE,
    t.BUCKET_SALE_PRICE,
    r.BUCKET_RENT_COUNT,
    COALESCE(t.BUCKET_TRADE_COUNT, 0) AS BUCKET_TRADE_COUNT,
    r.BUCKET_MEDIAN_AREA,
    CASE
        WHEN t.BUCKET_SALE_PRICE > 0 AND r.BUCKET_JEONSE_PRICE > 0
            THEN ROUND(r.BUCKET_JEONSE_PRICE / t.BUCKET_SALE_PRICE * 100, 1)
        ELSE NULL
    END AS BUCKET_JEONSE_RATE
FROM rent_agg r
LEFT JOIN trade_agg t
    ON r.SGG = t.SGG AND r.EMD = t.EMD AND r.PYEONG_BUCKET = t.PYEONG_BUCKET;

-- ============================================================
-- ML Feature table: ML_TRAIN_FEATURES
-- 각 동/월에 대해 "6개월 후 전세가 하락 여부"를 타겟으로 하는
-- 학습 데이터셋을 생성한다.
-- ============================================================

CREATE OR REPLACE VIEW HACKATHON_APP.RESILIENCE.ML_TRAIN_FEATURES AS
WITH feat AS (
    SELECT
        SGG,
        EMD,
        YYYYMMDD,
        PRICE,
        JEONSE_PRICE,
        JEONSE_RATE,
        TRADE_COUNT,
        RENT_COUNT,
        RECENT_TX_COUNT,
        RATE_VOLATILITY,
        -- 전세가 변동률 (12개월)
        CASE
            WHEN JEONSE_PRICE_12M_AGO > 0
                THEN (JEONSE_PRICE - JEONSE_PRICE_12M_AGO) / JEONSE_PRICE_12M_AGO * 100
            ELSE 0
        END AS JEONSE_CHANGE_12M_PCT,
        -- 매매 대비 전세 쿠션
        CASE
            WHEN PRICE > 0
                THEN (PRICE - JEONSE_PRICE) / PRICE * 100
            ELSE 0
        END AS SALE_CUSHION_PCT,
        -- 전세 비중 (전세 / 전체 임대)
        CASE
            WHEN RENT_COUNT > 0
                THEN TRADE_COUNT * 1.0 / (TRADE_COUNT + RENT_COUNT) * 100
            ELSE 50
        END AS TRADE_RATIO_PCT,
        -- 6개월 후 전세가 (타겟 생성용)
        LEAD(JEONSE_PRICE, 6) OVER (
            PARTITION BY SGG, EMD ORDER BY YYYYMMDD
        ) AS JEONSE_PRICE_6M_LATER
    FROM HACKATHON_APP.RESILIENCE.FEATURE_AREA_MONTH
    WHERE JEONSE_RATE IS NOT NULL
      AND JEONSE_RATE > 0
      AND JEONSE_RATE <= 100
)
SELECT
    SGG,
    EMD,
    YYYYMMDD,
    -- Features
    JEONSE_RATE,
    SALE_CUSHION_PCT,
    RATE_VOLATILITY,
    JEONSE_CHANGE_12M_PCT,
    RECENT_TX_COUNT,
    TRADE_COUNT,
    RENT_COUNT,
    TRADE_RATIO_PCT,
    -- Target: 6개월 후 전세가가 5% 이상 하락했는지
    CASE
        WHEN JEONSE_PRICE_6M_LATER IS NOT NULL
             AND JEONSE_PRICE > 0
             AND (JEONSE_PRICE_6M_LATER - JEONSE_PRICE) / JEONSE_PRICE * 100 <= -5
        THEN 1
        ELSE 0
    END AS DROP_RISK_LABEL,
    -- 연속 타겟: 6개월 후 변동률
    CASE
        WHEN JEONSE_PRICE_6M_LATER IS NOT NULL AND JEONSE_PRICE > 0
            THEN ROUND((JEONSE_PRICE_6M_LATER - JEONSE_PRICE) / JEONSE_PRICE * 100, 2)
        ELSE NULL
    END AS JEONSE_CHANGE_6M_PCT
FROM feat
WHERE JEONSE_PRICE_6M_LATER IS NOT NULL;

-- ML 추론 결과 저장 테이블
CREATE TABLE IF NOT EXISTS HACKATHON_APP.RESILIENCE.ML_RISK_SCORES (
    SGG          VARCHAR(50),
    EMD          VARCHAR(100),
    ML_RISK_SCORE FLOAT,
    ML_DROP_PROB  FLOAT,
    SCORED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    MODEL_VERSION VARCHAR(50)
);

-- ============================================================
-- Quick checks
-- ============================================================

-- Optional Richgo/Marketplace enrichment:
-- Run setup_richgo_overlay.sql after this file if you want to restore
-- Richgo-based structural signals in parallel with the public MOLIT pipeline.

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
