"""
Snowpark ML: 6개월 전세가 하락 위험 예측 모델

파이프라인:
  ML_TRAIN_FEATURES (학습 데이터) → XGBoost 학습 → Snowflake Model Registry 등록
  → FEATURE_AREA_MONTH 최신 행에 추론 → ML_RISK_SCORES 테이블 적재

사용법:
  python scripts/train_risk_model.py
  python scripts/train_risk_model.py --skip-train   # 학습 건너뛰고 추론만
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

# 프로젝트 루트를 path에 추가
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.session import get_snowpark_session  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

FEATURE_COLS = [
    "JEONSE_RATE",
    "SALE_CUSHION_PCT",
    "RATE_VOLATILITY",
    "JEONSE_CHANGE_12M_PCT",
    "RECENT_TX_COUNT",
    "TRADE_COUNT",
    "RENT_COUNT",
    "TRADE_RATIO_PCT",
]
TARGET_COL = "DROP_RISK_LABEL"
MODEL_NAME = "JEONSE_DROP_RISK_MODEL"
MODEL_VERSION = "v1"


def load_train_data(session) -> pd.DataFrame:
    """ML_TRAIN_FEATURES에서 학습 데이터 로드."""
    log.info("Loading training data from ML_TRAIN_FEATURES...")
    df = session.sql(
        "SELECT * FROM HACKATHON_APP.RESILIENCE.ML_TRAIN_FEATURES"
    ).to_pandas()
    log.info(f"Loaded {len(df)} rows, DROP_RISK=1: {df[TARGET_COL].sum()} ({df[TARGET_COL].mean()*100:.1f}%)")
    return df


def train_model(df: pd.DataFrame):
    """XGBoost 모델 학습."""
    from sklearn.model_selection import train_test_split
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.metrics import classification_report, roc_auc_score

    X = df[FEATURE_COLS].fillna(0)
    y = df[TARGET_COL]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = GradientBoostingClassifier(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.05,
        random_state=42,
    )
    model.fit(X_train, y_train)

    # 평가
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]

    log.info("=== Model Evaluation ===")
    log.info(f"\n{classification_report(y_test, y_pred)}")
    try:
        auc = roc_auc_score(y_test, y_prob)
        log.info(f"ROC AUC: {auc:.3f}")
    except ValueError:
        log.info("ROC AUC: N/A (single class in test set)")

    # Feature importance
    log.info("=== Feature Importance ===")
    for feat, imp in sorted(
        zip(FEATURE_COLS, model.feature_importances_), key=lambda x: -x[1]
    ):
        log.info(f"  {feat}: {imp:.3f}")

    return model


INFERENCE_SQL = """
    SELECT
        SGG, EMD,
        ROUND(CASE WHEN PRICE > 0 AND JEONSE_PRICE > 0
            THEN JEONSE_PRICE / PRICE * 100 ELSE 0 END, 1) AS JEONSE_RATE,
        ROUND(CASE WHEN PRICE > 0
            THEN (PRICE - JEONSE_PRICE) / PRICE * 100 ELSE 0 END, 1) AS SALE_CUSHION_PCT,
        COALESCE(RATE_VOLATILITY, 0) AS RATE_VOLATILITY,
        ROUND(CASE WHEN JEONSE_PRICE_12M_AGO > 0
            THEN (JEONSE_PRICE - JEONSE_PRICE_12M_AGO) / JEONSE_PRICE_12M_AGO * 100
            ELSE 0 END, 2) AS JEONSE_CHANGE_12M_PCT,
        COALESCE(RECENT_TX_COUNT, 0) AS RECENT_TX_COUNT,
        COALESCE(TRADE_COUNT, 0) AS TRADE_COUNT,
        COALESCE(RENT_COUNT, 0) AS RENT_COUNT,
        CASE WHEN RENT_COUNT > 0
            THEN ROUND(TRADE_COUNT * 1.0 / (TRADE_COUNT + RENT_COUNT) * 100, 1)
            ELSE 50 END AS TRADE_RATIO_PCT
    FROM HACKATHON_APP.RESILIENCE.FEATURE_AREA_MONTH
    WHERE RN = 1
      AND JEONSE_PRICE > 0 AND PRICE > 0
"""


def register_model(session, model, train_df: pd.DataFrame):
    """Snowflake Model Registry에 등록하고 ModelVersion 반환."""
    from snowflake.ml.registry import Registry

    registry = Registry(session=session, database_name="HACKATHON_APP", schema_name="RESILIENCE")

    # 샘플 입력 데이터 (Registry가 signature 추론에 사용)
    sample = train_df[FEATURE_COLS].head(10).fillna(0)

    mv = registry.log_model(
        model,
        model_name=MODEL_NAME,
        version_name=MODEL_VERSION,
        sample_input_data=sample,
        comment="6개월 전세가 하락 위험 예측 (GradientBoosting)",
    )
    log.info(f"Model registered: {MODEL_NAME}/{MODEL_VERSION}")
    return mv


def score_with_registry(session):
    """Registry에서 모델을 불러와 Snowflake 안에서 inference 실행."""
    from snowflake.ml.registry import Registry

    registry = Registry(session=session, database_name="HACKATHON_APP", schema_name="RESILIENCE")
    mv = registry.get_model(MODEL_NAME).version(MODEL_VERSION)
    log.info(f"Loaded model from registry: {MODEL_NAME}/{MODEL_VERSION}")

    # Snowpark DataFrame으로 inference (Snowflake 안에서 실행)
    input_df = session.sql(INFERENCE_SQL)
    result_sp = mv.run(input_df, function_name="predict_proba")

    # 결과를 pandas로 변환하여 ML_RISK_SCORES에 적재
    result_pd = result_sp.to_pandas()

    # predict_proba 출력 컬럼 찾기 (모델에 따라 다름)
    prob_cols = [c for c in result_pd.columns if "predict_proba" in c.lower() or c.startswith("output_")]
    if len(prob_cols) >= 2:
        prob_col = prob_cols[1]  # 클래스 1 확률
    elif prob_cols:
        prob_col = prob_cols[0]
    else:
        # fallback: 마지막 컬럼
        prob_col = result_pd.columns[-1]

    result_pd["ML_RISK_SCORE"] = (result_pd[prob_col].astype(float) * 100).round(1)
    result_pd["ML_DROP_PROB"] = result_pd[prob_col].astype(float).round(4)
    result_pd["MODEL_VERSION"] = MODEL_VERSION

    output = result_pd[["SGG", "EMD", "ML_RISK_SCORE", "ML_DROP_PROB", "MODEL_VERSION"]]

    session.sql("DELETE FROM HACKATHON_APP.RESILIENCE.ML_RISK_SCORES").collect()
    session.write_pandas(
        output,
        table_name="ML_RISK_SCORES",
        database="HACKATHON_APP",
        schema="RESILIENCE",
        auto_create_table=False,
        overwrite=False,
    )
    log.info(f"Registry inference: scored {len(output)} areas")
    return output


def score_local(session, model) -> pd.DataFrame:
    """로컬 모델로 추론 (Registry 사용 불가 시 fallback)."""
    log.info("Scoring with local model...")

    latest_df = session.sql(INFERENCE_SQL).to_pandas()
    log.info(f"Scoring {len(latest_df)} areas...")

    X = latest_df[FEATURE_COLS].fillna(0)
    probs = model.predict_proba(X)[:, 1]

    latest_df["ML_RISK_SCORE"] = (probs * 100).round(1)
    latest_df["ML_DROP_PROB"] = probs.round(4)
    latest_df["MODEL_VERSION"] = MODEL_VERSION

    result = latest_df[["SGG", "EMD", "ML_RISK_SCORE", "ML_DROP_PROB", "MODEL_VERSION"]]

    session.sql("DELETE FROM HACKATHON_APP.RESILIENCE.ML_RISK_SCORES").collect()
    session.write_pandas(
        result,
        table_name="ML_RISK_SCORES",
        database="HACKATHON_APP",
        schema="RESILIENCE",
        auto_create_table=False,
        overwrite=False,
    )
    log.info(f"Local inference: scored {len(result)} areas")

    log.info("=== Top 10 High-Risk Areas ===")
    for _, r in result.nlargest(10, "ML_RISK_SCORE").iterrows():
        log.info(f"  {r['SGG']} {r['EMD']}: risk={r['ML_RISK_SCORE']}, prob={r['ML_DROP_PROB']:.3f}")

    return result


def main():
    parser = argparse.ArgumentParser(description="Train jeonse drop risk model")
    parser.add_argument("--skip-train", action="store_true", help="Skip training, use registry model for inference")
    parser.add_argument("--local-only", action="store_true", help="Skip registry, local train + score only")
    args = parser.parse_args()

    session = get_snowpark_session()
    df = load_train_data(session)

    if len(df) < 50:
        log.error(f"Training data too small: {len(df)} rows. Need at least 50.")
        sys.exit(1)

    if args.skip_train:
        # Registry에서 모델 불러와서 Snowflake 안에서 inference
        log.info("Using registry model for inference...")
        try:
            score_with_registry(session)
        except Exception as e:
            log.warning(f"Registry inference 실패, 로컬 fallback: {e}")
            model = train_model(df)
            score_local(session, model)
    elif args.local_only:
        # 로컬만: 학습 + 로컬 추론
        model = train_model(df)
        score_local(session, model)
    else:
        # 전체: 학습 + Registry 등록 + Registry inference
        model = train_model(df)
        try:
            register_model(session, model, df)
            score_with_registry(session)
        except Exception as e:
            log.warning(f"Registry 사용 실패, 로컬 fallback: {e}")
            score_local(session, model)

    log.info("Done.")


if __name__ == "__main__":
    main()
