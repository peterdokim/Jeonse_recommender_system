-- ============================================================
-- Optional Richgo overlay for the MOLIT-based app
--
-- Purpose
-- 1. Restore Richgo-based structural signals in parallel.
-- 2. Keep the current public MOLIT pipeline untouched.
-- 3. Expose a merged view that the app can adopt step-by-step.
--
-- Prerequisites
-- 1. Run setup.sql first.
-- 2. Enable the Snowflake Marketplace data shares below:
--    - Korea_Real_Estate_Apartment_Market_Intelligence.HACKATHON_2026
--    - SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA
--
-- Notes
-- - This file intentionally does not overwrite RESILIENCE_BASE or
--   JEONSE_SAFETY_SCORE.
-- - Richgo-derived columns are exposed with a RICHGO_ prefix to avoid
--   colliding with the current MOLIT semantics.
-- ============================================================

CREATE DATABASE IF NOT EXISTS HACKATHON_APP;
CREATE SCHEMA IF NOT EXISTS HACKATHON_APP.RESILIENCE;

-- ============================================================
-- Richgo base view
-- Mirrors the old marketplace-backed monthly price layer, but keeps it
-- isolated from the public MOLIT base view.
-- ============================================================

CREATE OR REPLACE VIEW HACKATHON_APP.RESILIENCE.RESILIENCE_BASE_RICHGO AS
SELECT
    r.SGG,
    r.EMD,
    r.YYYYMMDD,
    r.MEME_PRICE_PER_SUPPLY_PYEONG AS PRICE,
    r.JEONSE_PRICE_PER_SUPPLY_PYEONG AS JEONSE_PRICE,
    AVG(a.AVERAGE_ASSET_AMOUNT) AS AVG_ASSET,
    AVG(a.AVERAGE_INCOME) AS AVG_INCOME,
    AVG(a.AVERAGE_SCORE) AS AVG_CREDIT_SCORE,
    AVG(a.AVERAGE_BALANCE_AMOUNT) AS AVG_LOAN,
    AVG(f.RESIDENTIAL_POPULATION) AS RES_POP,
    AVG(f.WORKING_POPULATION) AS WORK_POP,
    AVG(f.VISITING_POPULATION) AS VISIT_POP
FROM Korea_Real_Estate_Apartment_Market_Intelligence.HACKATHON_2026.REGION_APT_RICHGO_MARKET_PRICE_M_H r
LEFT JOIN SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.M_SCCO_MST m
    ON r.SGG = m.CITY_KOR_NAME
   AND r.EMD = m.DISTRICT_KOR_NAME
LEFT JOIN SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.ASSET_INCOME_INFO a
    ON m.PROVINCE_CODE = a.PROVINCE_CODE
   AND m.CITY_CODE = a.CITY_CODE
   AND m.DISTRICT_CODE = a.DISTRICT_CODE
   AND TO_CHAR(r.YYYYMMDD, 'YYYYMM') = TO_CHAR(a.STANDARD_YEAR_MONTH)
LEFT JOIN SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.FLOATING_POPULATION_INFO f
    ON m.PROVINCE_CODE = f.PROVINCE_CODE
   AND m.CITY_CODE = f.CITY_CODE
   AND m.DISTRICT_CODE = f.DISTRICT_CODE
   AND TO_CHAR(r.YYYYMMDD, 'YYYYMM') = TO_CHAR(f.STANDARD_YEAR_MONTH)
WHERE r.EMD IS NOT NULL
  AND r.EMD <> ''
GROUP BY
    r.SGG,
    r.EMD,
    r.YYYYMMDD,
    r.MEME_PRICE_PER_SUPPLY_PYEONG,
    r.JEONSE_PRICE_PER_SUPPLY_PYEONG;

-- ============================================================
-- Richgo structural safety score
-- Preserves the old structural logic:
-- - Jeonse rate
-- - Population movement
-- - Train/subway distance
--
-- Unlike the historical version, this overlay does not hard-code a
-- 3-district filter. It keeps the full Seoul coverage from the share.
-- ============================================================

CREATE OR REPLACE VIEW HACKATHON_APP.RESILIENCE.JEONSE_SAFETY_SCORE_RICHGO AS
WITH richgo_series AS (
    SELECT
        SGG,
        EMD,
        YYYYMMDD,
        PRICE,
        JEONSE_PRICE,
        CASE
            WHEN PRICE > 0 AND JEONSE_PRICE > 0
                THEN JEONSE_PRICE / PRICE * 100
            ELSE NULL
        END AS JEONSE_RATE,
        LAG(JEONSE_PRICE, 12) OVER (
            PARTITION BY SGG, EMD
            ORDER BY YYYYMMDD
        ) AS JEONSE_PRICE_12M_AGO,
        ROW_NUMBER() OVER (
            PARTITION BY SGG, EMD
            ORDER BY YYYYMMDD DESC
        ) AS RN
    FROM HACKATHON_APP.RESILIENCE.RESILIENCE_BASE_RICHGO
    WHERE PRICE IS NOT NULL
      AND JEONSE_PRICE IS NOT NULL
),
latest AS (
    SELECT
        SGG,
        EMD,
        ROUND(PRICE, 0) AS RICHGO_MEME_LATEST,
        ROUND(JEONSE_PRICE, 0) AS RICHGO_JEONSE_LATEST,
        ROUND(JEONSE_RATE, 1) AS RICHGO_JEONSE_RATE,
        ROUND(
            COALESCE(
                (JEONSE_PRICE - JEONSE_PRICE_12M_AGO)
                / NULLIF(JEONSE_PRICE_12M_AGO, 0) * 100,
                0
            ),
            1
        ) AS RICHGO_JEONSE_DROP_PCT
    FROM richgo_series
    WHERE RN = 1
      AND JEONSE_RATE IS NOT NULL
      AND JEONSE_RATE > 0
      AND JEONSE_RATE <= 100
),
rate_scored AS (
    SELECT
        latest.*,
        ROUND(
            PERCENT_RANK() OVER (
                ORDER BY RICHGO_JEONSE_RATE DESC
            ) * 100,
            1
        ) AS RICHGO_S_RATE
    FROM latest
),
subway_scored AS (
    SELECT
        SGG,
        EMD,
        ROUND(AVG(MIN_DIST), 0) AS RICHGO_SUBWAY_DIST,
        ROUND(
            PERCENT_RANK() OVER (
                ORDER BY AVG(MIN_DIST) DESC
            ) * 100,
            1
        ) AS RICHGO_S_SUB
    FROM (
        SELECT
            SGG,
            EMD,
            DANJI_ID,
            MIN(DISTANCE) AS MIN_DIST
        FROM Korea_Real_Estate_Apartment_Market_Intelligence.HACKATHON_2026.APT_DANJI_AND_TRANSPORTATION_TRAIN_DISTANCE
        GROUP BY SGG, EMD, DANJI_ID
    )
    GROUP BY SGG, EMD
),
latest_mig_month AS (
    SELECT MAX(YYYYMMDD) AS LATEST_YYYYMMDD
    FROM Korea_Real_Estate_Apartment_Market_Intelligence.HACKATHON_2026.REGION_POPULATION_MOVEMENT
    WHERE SD = '서울'
      AND REGION_LEVEL = 'sgg'
      AND MOVEMENT_TYPE = '순이동'
),
mig_scored AS (
    SELECT
        mov.SGG,
        SUM(mov.POPULATION) AS RICHGO_NET_MIG,
        ROUND(
            PERCENT_RANK() OVER (
                ORDER BY SUM(mov.POPULATION) ASC
            ) * 100,
            1
        ) AS RICHGO_S_MIG
    FROM Korea_Real_Estate_Apartment_Market_Intelligence.HACKATHON_2026.REGION_POPULATION_MOVEMENT mov
    CROSS JOIN latest_mig_month lm
    WHERE mov.SD = '서울'
      AND mov.REGION_LEVEL = 'sgg'
      AND mov.MOVEMENT_TYPE = '순이동'
      AND mov.YYYYMMDD BETWEEN DATEADD(MONTH, -11, lm.LATEST_YYYYMMDD) AND lm.LATEST_YYYYMMDD
    GROUP BY mov.SGG
),
hug AS (
    SELECT
        GU_NAME,
        ACCIDENT_RATE
    FROM HACKATHON_APP.RESILIENCE.HUG_ACCIDENT
)
SELECT
    rate_scored.SGG,
    rate_scored.EMD,
    rate_scored.RICHGO_MEME_LATEST,
    rate_scored.RICHGO_JEONSE_LATEST,
    rate_scored.RICHGO_JEONSE_RATE,
    rate_scored.RICHGO_JEONSE_DROP_PCT,
    hug.ACCIDENT_RATE AS RICHGO_HUG_RATE,
    mig_scored.RICHGO_NET_MIG,
    subway_scored.RICHGO_SUBWAY_DIST,
    rate_scored.RICHGO_S_RATE,
    mig_scored.RICHGO_S_MIG,
    subway_scored.RICHGO_S_SUB,
    ROUND(
        rate_scored.RICHGO_S_RATE * 0.50
        + mig_scored.RICHGO_S_MIG * 0.25
        + subway_scored.RICHGO_S_SUB * 0.25,
        1
    ) AS RICHGO_TOTAL_SCORE,
    CASE
        WHEN ROUND(
            rate_scored.RICHGO_S_RATE * 0.50
            + mig_scored.RICHGO_S_MIG * 0.25
            + subway_scored.RICHGO_S_SUB * 0.25,
            1
        ) >= 80 THEN 'A'
        WHEN ROUND(
            rate_scored.RICHGO_S_RATE * 0.50
            + mig_scored.RICHGO_S_MIG * 0.25
            + subway_scored.RICHGO_S_SUB * 0.25,
            1
        ) >= 60 THEN 'B'
        WHEN ROUND(
            rate_scored.RICHGO_S_RATE * 0.50
            + mig_scored.RICHGO_S_MIG * 0.25
            + subway_scored.RICHGO_S_SUB * 0.25,
            1
        ) >= 40 THEN 'C'
        ELSE 'D'
    END AS RICHGO_GRADE
FROM rate_scored
LEFT JOIN subway_scored
    ON rate_scored.SGG = subway_scored.SGG
   AND rate_scored.EMD = subway_scored.EMD
LEFT JOIN mig_scored
    ON rate_scored.SGG = mig_scored.SGG
LEFT JOIN hug
    ON rate_scored.SGG = hug.GU_NAME;

-- ============================================================
-- Merged score view for step-by-step adoption in the app
-- Keeps the current MOLIT score as the primary contract and appends
-- Richgo-only structural columns on the side.
-- ============================================================

CREATE OR REPLACE VIEW HACKATHON_APP.RESILIENCE.JEONSE_SCORE_ENRICHED AS
SELECT
    molit.*,
    rich.RICHGO_MEME_LATEST,
    rich.RICHGO_JEONSE_LATEST,
    rich.RICHGO_JEONSE_RATE,
    rich.RICHGO_JEONSE_DROP_PCT,
    rich.RICHGO_HUG_RATE,
    rich.RICHGO_NET_MIG,
    rich.RICHGO_SUBWAY_DIST,
    rich.RICHGO_S_RATE,
    rich.RICHGO_S_MIG,
    rich.RICHGO_S_SUB,
    rich.RICHGO_TOTAL_SCORE,
    rich.RICHGO_GRADE,
    IFF(rich.SGG IS NOT NULL, TRUE, FALSE) AS HAS_RICHGO_SIGNAL
FROM HACKATHON_APP.RESILIENCE.JEONSE_SAFETY_SCORE molit
LEFT JOIN HACKATHON_APP.RESILIENCE.JEONSE_SAFETY_SCORE_RICHGO rich
    ON molit.SGG = rich.SGG
   AND molit.EMD = rich.EMD;

-- ============================================================
-- Quick checks
-- ============================================================

SELECT COUNT(*) AS RICHGO_BASE_ROWS
FROM HACKATHON_APP.RESILIENCE.RESILIENCE_BASE_RICHGO;

SELECT COUNT(*) AS ENRICHED_ROWS
FROM HACKATHON_APP.RESILIENCE.JEONSE_SCORE_ENRICHED;

SELECT
    SGG,
    EMD,
    TOTAL_SCORE,
    RICHGO_TOTAL_SCORE,
    HAS_RICHGO_SIGNAL
FROM HACKATHON_APP.RESILIENCE.JEONSE_SCORE_ENRICHED
ORDER BY TOTAL_SCORE DESC, SGG, EMD;
