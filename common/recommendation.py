from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd
import streamlit as st

DIMENSION_LABELS = {
    "S_RATE": "전세가율",
    "S_MIG": "거래 활발도",
    "S_SUB": "가격 안정성",
}

GRADE_MEANINGS = {
    "A": "안전",
    "B": "보통",
    "C": "주의",
    "D": "위험",
}

SEARCH_SCOPE_OPTIONS = [
    "현재 관심 구 우선",
    "출퇴근권 우선",
    "전체 후보",
]

SURVEY_QUESTIONS: List[Dict[str, str]] = [
    {
        "key": "survey_safe_over_distance",
        "label": "출퇴근이 좀 멀어지더라도, 보증금이 더 안전한 집을 고르겠다.",
    },
    {
        "key": "survey_safe_over_convenience",
        "label": "주변 편의시설이 좀 부족해도, 보증금을 안전하게 돌려받을 수 있는 게 낫다.",
    },
    {
        "key": "survey_avoid_large_loss",
        "label": "보증금을 크게 잃을 수 있는 집은 무조건 피하고 싶다.",
    },
    {
        "key": "survey_accept_risk_for_commute",
        "label": "출퇴근이 편하다면, 위험이 좀 있어도 고려할 수 있다.",
    },
    {
        "key": "survey_accept_risk_for_familiarity",
        "label": "지금 보고 있는 집이랑 비슷한 조건이면, 약간의 위험은 괜찮다.",
    },
    {
        "key": "survey_prefer_candidate_similarity",
        "label": "새로운 동네보다는, 지금 보고 있는 곳과 비슷한 동네가 편하다.",
    },
]

PROFILE_COEFFICIENTS: Dict[str, Dict[str, float]] = {
    "보수형": {"alpha": 1.4, "beta": 1.2, "gamma": 0.8, "delta": 0.7},
    "중도위험형": {"alpha": 1.0, "beta": 1.0, "gamma": 1.0, "delta": 1.0},
    "모험형": {"alpha": 0.7, "beta": 0.8, "gamma": 1.2, "delta": 1.2},
}

PROFILE_DESCRIPTIONS = {
    "보수형": "보증금을 안전하게 지키는 걸 가장 중요하게 생각해요.",
    "중도위험형": "안전성과 생활 편의를 균형 있게 고려해요.",
    "모험형": "편리한 생활권이라면 어느 정도 위험은 감수할 수 있어요.",
}

PROFILE_ICONS = {
    "보수형": "security",
    "중도위험형": "balance",
    "모험형": "explore",
}

NUMERIC_COLUMNS = [
    "MEME_LATEST",
    "JEONSE_LATEST",
    "JEONSE_RATE",
    "JEONSE_DROP_PCT",
    "HUG_RATE",
    "NET_MIG",
    "SUBWAY_DIST",
    "S_RATE",
    "S_MIG",
    "S_SUB",
    "TOTAL_SCORE",
    "AVG_ASSET",
    "AVG_INCOME",
    "AVG_CREDIT_SCORE",
    "AVG_LOAN",
    "RES_POP",
    "WORK_POP",
    "VISIT_POP",
    "RICHGO_JEONSE_RATE",
    "RICHGO_JEONSE_DROP_PCT",
    "RICHGO_NET_MIG",
    "RICHGO_SUBWAY_DIST",
    "RICHGO_S_RATE",
    "RICHGO_S_MIG",
    "RICHGO_S_SUB",
    "RICHGO_TOTAL_SCORE",
    "ML_RISK_SCORE",
    "ML_DROP_PROB",
]


def build_area_label(df: pd.DataFrame) -> pd.Series:
    return df["SGG"].astype(str) + " " + df["EMD"].astype(str)


def clamp_score(value: float) -> float:
    return max(0.0, min(100.0, value))


def to_eok(value: float) -> float:
    return value / 100_000_000


def from_eok(value: float) -> int:
    return int(value * 100_000_000)


def format_currency_krw(value: float) -> str:
    """금액을 억/만원 단위로 자연스럽게 표시한다."""
    value = float(value)
    abs_val = abs(value)
    sign = "-" if value < 0 else ""
    if abs_val >= 100_000_000:
        eok = abs_val / 100_000_000
        if eok == int(eok):
            return f"{sign}{int(eok)}억원"
        return f"{sign}{eok:.1f}억원"
    if abs_val >= 10_000:
        man = abs_val / 10_000
        if man == int(man):
            return f"{sign}{int(man)}만원"
        return f"{sign}{man:.0f}만원"
    return f"{sign}{abs_val:,.0f}원"


def percentile_score(series: pd.Series, higher_is_better: bool = True, fill_value: float = 50.0) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.dropna().empty:
        return pd.Series(fill_value, index=series.index, dtype="float64")
    ranked = numeric.rank(pct=True, method="average", ascending=higher_is_better) * 100
    return ranked.fillna(fill_value).round(1)


def weighted_available_score(parts) -> pd.Series:
    if not parts:
        return pd.Series(dtype="float64")

    base_index = parts[0][0].index
    weighted_sum = pd.Series(0.0, index=base_index, dtype="float64")
    weight_sum = pd.Series(0.0, index=base_index, dtype="float64")

    for series, weight in parts:
        numeric = pd.to_numeric(series, errors="coerce")
        available = numeric.notna()
        weighted_sum = weighted_sum + numeric.fillna(0) * weight
        weight_sum = weight_sum + available.astype(float) * weight

    result = weighted_sum / weight_sum.where(weight_sum > 0)
    return result.round(1)


def grade_from_score(score: float) -> str:
    if pd.isna(score):
        return "-"
    if score >= 80:
        return "A"
    if score >= 60:
        return "B"
    if score >= 40:
        return "C"
    return "D"


def structure_data_label(has_richgo: bool, has_grandata: bool) -> str:
    if has_richgo and has_grandata:
        return "리치고 + SPH/Grandata"
    if has_richgo:
        return "리치고"
    if has_grandata:
        return "SPH/Grandata"
    return "비교 데이터 부족"


def score_gap_label(gap: float) -> str:
    if pd.isna(gap):
        return "비교 데이터 부족"
    if gap >= 5:
        return "구조 우세"
    if gap <= -5:
        return "시장 우세"
    return "유사"


def comparison_label(market_score: float, structural_score: float) -> str:
    if pd.isna(structural_score):
        return "구조 비교 데이터 부족"

    market_safe = market_score >= 65
    structural_safe = structural_score >= 65

    if market_safe and structural_safe:
        return "시장·구조 모두 안전"
    if market_safe and not structural_safe:
        return "시장 강세·구조 보완 필요"
    if not market_safe and structural_safe:
        return "구조 우수·시장 약세"
    return "시장·구조 모두 주의"


def comparison_detail(
    market_score: float,
    structural_score: float,
    data_label: str,
) -> str:
    if pd.isna(structural_score):
        return "리치고 또는 SPH 구조 신호가 부족해 현재는 국토부 실거래 기반 시장 점수 중심으로 해석합니다."

    gap = structural_score - market_score
    label = comparison_label(market_score, structural_score)

    if label == "시장·구조 모두 안전":
        detail = "최근 실거래 흐름과 구조 신호가 함께 양호한 후보입니다."
    elif label == "시장 강세·구조 보완 필요":
        detail = "최근 시장 흐름은 괜찮지만 구조 체력은 조금 더 보수적으로 볼 필요가 있습니다."
    elif label == "구조 우수·시장 약세":
        detail = "리치고와 SPH 기준 구조 체력은 양호하지만 최근 시장 흐름은 조금 더 확인이 필요합니다."
    else:
        detail = "시장 흐름과 구조 신호 모두 보수적으로 점검해야 하는 후보입니다."

    if abs(gap) >= 10:
        if gap > 0:
            detail += " 구조 점수가 더 높아 장기 체력 관점의 재평가 여지가 있습니다."
        else:
            detail += " 시장 점수보다 구조 점수가 낮아 장기 안전성은 한 번 더 점검하는 편이 좋습니다."

    return f"{detail} 구조 비교는 {data_label} 신호를 사용했습니다."


def detect_price_unit_multiplier(df: pd.DataFrame) -> int:
    median_price = pd.to_numeric(df["JEONSE_LATEST"], errors="coerce").median()
    if pd.isna(median_price):
        return 1
    return 10_000 if median_price < 100_000 else 1


def estimate_loss_amount(row: pd.Series, deposit_amount: float) -> float:
    return deposit_amount * abs(float(row["JEONSE_DROP_PCT"])) / 100


def normalize_likert(answer: int) -> float:
    answer = max(1, min(5, int(answer)))
    return round((answer - 1) / 4 * 100, 1)


def classify_survey_profile(answers: Dict[str, int]) -> Dict[str, Any]:
    safe_over_distance = normalize_likert(answers["survey_safe_over_distance"])
    safe_over_convenience = normalize_likert(answers["survey_safe_over_convenience"])
    avoid_large_loss = normalize_likert(answers["survey_avoid_large_loss"])
    accept_risk_for_commute = normalize_likert(answers["survey_accept_risk_for_commute"])
    accept_risk_for_familiarity = normalize_likert(answers["survey_accept_risk_for_familiarity"])
    prefer_candidate_similarity = normalize_likert(answers["survey_prefer_candidate_similarity"])

    # Q1~Q3: 높을수록 안전 지향 (동의 = 보수적)
    safety_preference = round(
        (safe_over_distance + safe_over_convenience + avoid_large_loss) / 3,
        1,
    )

    # Q4: 높을수록 편의 지향 (동의 = 모험적)
    convenience_preference = round(accept_risk_for_commute, 1)

    # Q5~Q6: 높을수록 유사성 지향 (동의 = 모험적)
    similarity_preference = round(
        (accept_risk_for_familiarity + prefer_candidate_similarity) / 2,
        1,
    )

    # risk_tolerance: 안전 선호가 높으면 낮고, 편의/유사성이 높으면 높음
    risk_tolerance = round(
        (100 - safety_preference) * 0.50
        + convenience_preference * 0.25
        + similarity_preference * 0.25,
        1,
    )

    if risk_tolerance < 35:
        profile = "보수형"
    elif risk_tolerance < 65:
        profile = "중도위험형"
    else:
        profile = "모험형"

    return {
        "profile": profile,
        "safety_preference": safety_preference,
        "convenience_preference": convenience_preference,
        "similarity_preference": similarity_preference,
        "risk_tolerance": risk_tolerance,
        "coefficients": PROFILE_COEFFICIENTS[profile],
        "description": PROFILE_DESCRIPTIONS[profile],
    }


def get_area_history(history_df: pd.DataFrame, sgg: str, emd: str) -> pd.DataFrame:
    area_history = history_df[(history_df["SGG"] == sgg) & (history_df["EMD"] == emd)].copy()
    if area_history.empty:
        return area_history
    area_history["YYYYMMDD"] = pd.to_datetime(area_history["YYYYMMDD"], errors="coerce")
    area_history["PRICE"] = pd.to_numeric(area_history["PRICE"], errors="coerce")
    area_history["JEONSE_PRICE"] = pd.to_numeric(area_history["JEONSE_PRICE"], errors="coerce")
    return area_history.sort_values("YYYYMMDD").reset_index(drop=True)


@st.cache_data(show_spinner=False)
def compute_backtest_metrics(history_df: pd.DataFrame) -> pd.DataFrame:
    if history_df.empty:
        return pd.DataFrame(
            columns=[
                "SGG",
                "EMD",
                "BACKTEST_SCORE",
                "BT_WORST_DRAWDOWN_PCT",
                "BT_RECOVERY_RATE",
                "BT_DOWNSIDE_HIT_RATE",
            ]
        )

    working_df = history_df.copy()
    working_df["YYYYMMDD"] = pd.to_datetime(working_df["YYYYMMDD"], errors="coerce")
    working_df["JEONSE_PRICE"] = pd.to_numeric(working_df["JEONSE_PRICE"], errors="coerce")

    rows = []
    for (sgg, emd), group in working_df.groupby(["SGG", "EMD"], sort=False):
        prices = group.sort_values("YYYYMMDD")["JEONSE_PRICE"].dropna().reset_index(drop=True)
        if len(prices) < 3:
            continue

        worst_drops = []
        forward_returns = []
        for start_idx in range(len(prices) - 1):
            window = prices.iloc[start_idx : min(start_idx + 13, len(prices))]
            start_price = prices.iloc[start_idx]
            if start_price <= 0:
                continue
            worst_drops.append((window.min() / start_price - 1) * 100)
            forward_returns.append((window.iloc[-1] / start_price - 1) * 100)

        if not worst_drops:
            continue

        worst_drawdown = min(worst_drops)
        median_drawdown = float(pd.Series(worst_drops).median())
        recovery_rate = float((pd.Series(forward_returns) >= 0).mean()) * 100
        downside_hit_rate = float((pd.Series(worst_drops) <= -10).mean()) * 100

        backtest_score = round(
            clamp_score(100 + worst_drawdown * 2.5) * 0.35
            + clamp_score(100 + median_drawdown * 4.0) * 0.25
            + recovery_rate * 0.25
            + (100 - downside_hit_rate) * 0.15,
            1,
        )

        rows.append(
            {
                "SGG": sgg,
                "EMD": emd,
                "BACKTEST_SCORE": backtest_score,
                "BT_WORST_DRAWDOWN_PCT": round(worst_drawdown, 1),
                "BT_RECOVERY_RATE": round(recovery_rate, 1),
                "BT_DOWNSIDE_HIT_RATE": round(downside_hit_rate, 1),
            }
        )

    return pd.DataFrame(rows)


def pyeong_to_bucket(pyeong: int) -> str:
    """평수를 평형대 버킷으로 변환 (㎡ 기준 SQL 뷰와 일치)."""
    # SMALL: ≤50㎡ (≈15평), MID: ≤85㎡ (≈25평), LARGE: ≤135㎡ (≈40평), XLARGE: 그 이상
    if pyeong <= 15:
        return "SMALL"
    if pyeong <= 25:
        return "MID"
    if pyeong <= 40:
        return "LARGE"
    return "XLARGE"


def build_recommendation_dataset(
    scores_df: pd.DataFrame,
    history_df: pd.DataFrame,
    deposit_amount: int,
    workplace_sgg: str,
    survey_result: Dict[str, Any],
    preferred_pyeong: int,
    candidate_area: str,
    search_scope: str,
    budget_tolerance_pct: int,
    pyeong_bucket_df: pd.DataFrame = None,
) -> pd.DataFrame:
    if scores_df.empty:
        return pd.DataFrame()

    coefficients = survey_result["coefficients"]
    alpha = float(coefficients["alpha"])
    beta = float(coefficients["beta"])
    gamma = float(coefficients["gamma"])
    delta = float(coefficients["delta"])

    unit_multiplier = detect_price_unit_multiplier(scores_df)
    backtest_df = compute_backtest_metrics(history_df)

    df = scores_df.copy()
    for column in NUMERIC_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    df["AREA_LABEL"] = build_area_label(df)
    df = df.merge(backtest_df, on=["SGG", "EMD"], how="left")
    df["BACKTEST_SCORE"] = df["BACKTEST_SCORE"].fillna(df["TOTAL_SCORE"] * 0.9).round(1)
    df["BT_WORST_DRAWDOWN_PCT"] = df["BT_WORST_DRAWDOWN_PCT"].fillna(df["JEONSE_DROP_PCT"])
    df["BT_RECOVERY_RATE"] = df["BT_RECOVERY_RATE"].fillna(50.0)
    df["BT_DOWNSIDE_HIT_RATE"] = df["BT_DOWNSIDE_HIT_RATE"].fillna(50.0)

    # 평형대 데이터가 있으면 우선 사용 (해당 평형대의 평당가로 계산)
    bucket = pyeong_to_bucket(preferred_pyeong)
    df["PYEONG_BUCKET"] = bucket
    df["BUCKET_JEONSE_PRICE"] = pd.NA
    df["BUCKET_SALE_PRICE"] = pd.NA
    df["BUCKET_RENT_COUNT"] = 0
    if pyeong_bucket_df is not None and not pyeong_bucket_df.empty:
        bucket_filtered = pyeong_bucket_df[pyeong_bucket_df["PYEONG_BUCKET"] == bucket]
        df = df.drop(columns=["BUCKET_JEONSE_PRICE", "BUCKET_SALE_PRICE", "BUCKET_RENT_COUNT"]).merge(
            bucket_filtered[["SGG", "EMD", "BUCKET_JEONSE_PRICE", "BUCKET_SALE_PRICE", "BUCKET_RENT_COUNT"]],
            on=["SGG", "EMD"], how="left"
        )

    # 평형대 평당가가 있으면 사용, 없으면 동 전체 평균(JEONSE_LATEST) 사용
    df["EFFECTIVE_JEONSE_PP"] = pd.to_numeric(df["BUCKET_JEONSE_PRICE"], errors="coerce").fillna(df["JEONSE_LATEST"])
    df["EFFECTIVE_SALE_PP"] = pd.to_numeric(df["BUCKET_SALE_PRICE"], errors="coerce").fillna(df["MEME_LATEST"])
    df["USES_BUCKET_DATA"] = df["BUCKET_JEONSE_PRICE"].notna()

    df["ESTIMATED_TOTAL_JEONSE"] = (df["EFFECTIVE_JEONSE_PP"] * preferred_pyeong * unit_multiplier).round(0)
    df["ESTIMATED_TOTAL_SALE"] = (df["EFFECTIVE_SALE_PP"] * preferred_pyeong * unit_multiplier).round(0)
    df["LOSS_EXPOSURE_AMOUNT"] = df.apply(
        lambda row: estimate_loss_amount(row, deposit_amount),
        axis=1,
    )

    sale_cushion = ((df["ESTIMATED_TOTAL_SALE"] - df["ESTIMATED_TOTAL_JEONSE"]) / df["ESTIMATED_TOTAL_SALE"]) * 100
    sale_cushion = sale_cushion.replace([float("inf"), float("-inf")], pd.NA)
    df["SALE_CUSHION_PCT"] = sale_cushion.fillna(0).round(1)

    df["WORK_POP_SCORE"] = percentile_score(df["WORK_POP"], higher_is_better=True)
    df["VISIT_POP_SCORE"] = percentile_score(df["VISIT_POP"], higher_is_better=True)
    df["RES_POP_SCORE"] = percentile_score(df["RES_POP"], higher_is_better=True)
    df["INCOME_SCORE"] = percentile_score(df["AVG_INCOME"], higher_is_better=True)
    df["ASSET_SCORE"] = percentile_score(df["AVG_ASSET"], higher_is_better=True)
    df["LOSS_EXPOSURE_SCORE"] = percentile_score(df["LOSS_EXPOSURE_AMOUNT"], higher_is_better=False)
    df["PRICE_CONTEXT_SCORE"] = (
        percentile_score(df["JEONSE_RATE"], higher_is_better=False) * 0.65
        + percentile_score(df["SALE_CUSHION_PCT"], higher_is_better=True) * 0.35
    ).round(1)

    below_budget = df["ESTIMATED_TOTAL_JEONSE"] <= deposit_amount
    df["BUDGET_FIT_SCORE"] = 0.0
    df.loc[below_budget, "BUDGET_FIT_SCORE"] = (
        70 + df.loc[below_budget, "ESTIMATED_TOTAL_JEONSE"] / max(deposit_amount, 1) * 30
    ).clip(0, 100)
    df.loc[~below_budget, "BUDGET_FIT_SCORE"] = (
        100 - (df.loc[~below_budget, "ESTIMATED_TOTAL_JEONSE"] - deposit_amount) / max(deposit_amount, 1) * 140
    ).clip(0, 100)
    df["BUDGET_FIT_SCORE"] = df["BUDGET_FIT_SCORE"].round(1)

    base_commute = pd.Series(55.0, index=df.index)
    base_commute[df["SGG"] == workplace_sgg] = 85.0
    df["COMMUTE_FIT_SCORE"] = (
        base_commute
        + df["S_SUB"].fillna(50) * 0.10
        + df["WORK_POP_SCORE"].fillna(50) * 0.05
    ).clip(0, 100).round(1)

    df["LIFESTYLE_FIT_SCORE"] = (
        df["VISIT_POP_SCORE"].fillna(50) * 0.35
        + df["WORK_POP_SCORE"].fillna(50) * 0.20
        + df["INCOME_SCORE"].fillna(50) * 0.15
        + df["RES_POP_SCORE"].fillna(50) * 0.10
        + df["S_MIG"].fillna(50) * 0.10
        + df["S_SUB"].fillna(50) * 0.10
    ).round(1)

    df["HAS_GRANDATA_SIGNAL"] = df[["AVG_ASSET", "AVG_INCOME", "RES_POP", "WORK_POP", "VISIT_POP"]].notna().any(axis=1)

    richgo_sub = pd.to_numeric(df["RICHGO_S_SUB"], errors="coerce").where(df["HAS_RICHGO_SIGNAL"].fillna(False))
    richgo_mig = pd.to_numeric(df["RICHGO_S_MIG"], errors="coerce").where(df["HAS_RICHGO_SIGNAL"].fillna(False))
    grandata_work = df["WORK_POP_SCORE"].where(df["WORK_POP"].notna())
    grandata_visit = df["VISIT_POP_SCORE"].where(df["VISIT_POP"].notna())
    grandata_res = df["RES_POP_SCORE"].where(df["RES_POP"].notna())
    grandata_income = df["INCOME_SCORE"].where(df["AVG_INCOME"].notna())
    grandata_asset = df["ASSET_SCORE"].where(df["AVG_ASSET"].notna())

    df["RICHGO_STRUCTURE_SCORE"] = weighted_available_score(
        [
            (richgo_sub, 0.55),
            (richgo_mig, 0.45),
        ]
    )
    df["SPH_ACTIVITY_SCORE"] = weighted_available_score(
        [
            (grandata_work, 0.45),
            (grandata_visit, 0.35),
            (grandata_res, 0.20),
        ]
    )
    df["SPH_FINANCE_SCORE"] = weighted_available_score(
        [
            (grandata_income, 0.60),
            (grandata_asset, 0.40),
        ]
    )

    rate_consistency = (
        100
        - (
            pd.to_numeric(df["JEONSE_RATE"], errors="coerce")
            - pd.to_numeric(df["RICHGO_JEONSE_RATE"], errors="coerce")
        ).abs()
        * 4
    ).clip(0, 100)
    df["RATE_CONSISTENCY_SCORE"] = rate_consistency.where(
        pd.to_numeric(df["RICHGO_JEONSE_RATE"], errors="coerce").notna()
    ).round(1)

    df["MARKET_SCORE"] = df["TOTAL_SCORE"].fillna(0).round(1)
    df["STRUCTURAL_SCORE"] = weighted_available_score(
        [
            (df["RICHGO_STRUCTURE_SCORE"], 0.45),
            (df["SPH_ACTIVITY_SCORE"], 0.25),
            (df["SPH_FINANCE_SCORE"], 0.20),
            (df["RATE_CONSISTENCY_SCORE"], 0.10),
        ]
    )
    df["HAS_STRUCTURE_SIGNAL"] = df["STRUCTURAL_SCORE"].notna()
    df["STRUCTURAL_GRADE"] = df["STRUCTURAL_SCORE"].apply(grade_from_score)
    df["MARKET_STRUCTURE_GAP"] = (df["STRUCTURAL_SCORE"] - df["MARKET_SCORE"]).round(1)
    df["STRUCTURE_DATA_LABEL"] = df.apply(
        lambda row: structure_data_label(
            bool(row.get("HAS_RICHGO_SIGNAL", False)),
            bool(row.get("HAS_GRANDATA_SIGNAL", False)),
        ),
        axis=1,
    )
    df["GAP_DIRECTION_LABEL"] = df["MARKET_STRUCTURE_GAP"].apply(score_gap_label)
    df["COMPARISON_LABEL"] = df.apply(
        lambda row: comparison_label(
            float(row.get("MARKET_SCORE", 0)),
            pd.to_numeric(pd.Series([row.get("STRUCTURAL_SCORE")]), errors="coerce").iloc[0],
        ),
        axis=1,
    )
    df["COMPARISON_DETAIL"] = df.apply(
        lambda row: comparison_detail(
            float(row.get("MARKET_SCORE", 0)),
            pd.to_numeric(pd.Series([row.get("STRUCTURAL_SCORE")]), errors="coerce").iloc[0],
            str(row.get("STRUCTURE_DATA_LABEL", "비교 데이터 부족")),
        ),
        axis=1,
    )

    candidate_mask = df["AREA_LABEL"] == candidate_area
    candidate_row = df.loc[candidate_mask].iloc[0]
    candidate_sgg = str(candidate_row["SGG"])
    candidate_jeonse = float(candidate_row["ESTIMATED_TOTAL_JEONSE"])
    candidate_rate = float(candidate_row["JEONSE_RATE"])

    price_similarity = (
        100 - (df["ESTIMATED_TOTAL_JEONSE"] - candidate_jeonse).abs() / max(candidate_jeonse, 1) * 120
    ).clip(0, 100)
    rate_similarity = (100 - (df["JEONSE_RATE"] - candidate_rate).abs() * 2).clip(0, 100)
    area_similarity = pd.Series(60.0, index=df.index)
    area_similarity[df["SGG"] == candidate_sgg] = 100.0
    df["SIMILARITY_SCORE"] = (
        price_similarity * 0.50 + rate_similarity * 0.30 + area_similarity * 0.20
    ).round(1)

    # ── Hard Filter ──

    # 1. 예산: 보증금 이하 + 허용 범위만큼 위까지 포함
    upper_budget = deposit_amount * (1 + budget_tolerance_pct / 100)
    df["BUDGET_BAND_MATCH"] = df["ESTIMATED_TOTAL_JEONSE"] <= upper_budget

    # 2. 지역 범위
    if search_scope == "현재 관심 구 우선":
        df["AREA_SCOPE_MATCH"] = df["SGG"] == candidate_sgg
    elif search_scope == "출퇴근권 우선":
        df["AREA_SCOPE_MATCH"] = df["SGG"].isin([candidate_sgg, workplace_sgg])
    else:
        df["AREA_SCOPE_MATCH"] = True

    # 3. 실거래 존재 여부: 최근 6개월 거래 5건 미만이면 데이터 부족으로 제외
    df["HAS_ENOUGH_TX"] = df["NET_MIG"].fillna(0) >= 5

    # 4. 절대 위험 제외: 전세가율 90% 이상은 성향 무관하게 제외
    df["NOT_ABSOLUTE_RISK"] = df["JEONSE_RATE"].fillna(0) < 90

    convenience_weight = 0.35 + float(survey_result["convenience_preference"]) / 100 * 0.30
    lifestyle_weight = 1 - convenience_weight

    df["SAFETY_SCORE"] = df["MARKET_SCORE"]
    df["LOSS_PENALTY_SCORE"] = (100 - df["LOSS_EXPOSURE_SCORE"]).clip(0, 100).round(1)
    df["PRICE_OVERHEAT_PENALTY_SCORE"] = (100 - df["PRICE_CONTEXT_SCORE"]).clip(0, 100).round(1)
    df["PREFERENCE_FIT_SCORE"] = (
        df["COMMUTE_FIT_SCORE"] * convenience_weight
        + df["LIFESTYLE_FIT_SCORE"] * lifestyle_weight
    ).round(1)

    # 하이브리드 추천점수
    # 룰 기반: 가중평균 (합 = 1.0) → 안전 60% + 손실회피 15% + 가격적정 5% + 선호적합 12% + 유사도 8%
    # 모든 항목이 100점이면 룰 점수도 100점, 0점이면 0점 (절대 점수)
    # 성향(α/β/γ/δ)은 원래 가중치에 곱해지지만 합이 1.0 유지되도록 정규화
    base_weights = {"safety": 0.60, "loss": 0.15, "price": 0.05, "pref": 0.12, "sim": 0.08}
    w_loss = base_weights["loss"] * alpha
    w_price = base_weights["price"] * beta
    w_pref = base_weights["pref"] * gamma
    w_sim = base_weights["sim"] * delta
    w_total = base_weights["safety"] + w_loss + w_price + w_pref + w_sim
    # 정규화: 합이 1.0이 되도록
    w_safety_n = base_weights["safety"] / w_total
    w_loss_n = w_loss / w_total
    w_price_n = w_price / w_total
    w_pref_n = w_pref / w_total
    w_sim_n = w_sim / w_total

    raw_rule = (
        df["SAFETY_SCORE"] * w_safety_n
        + (100 - df["LOSS_PENALTY_SCORE"]) * w_loss_n
        + (100 - df["PRICE_OVERHEAT_PENALTY_SCORE"]) * w_price_n
        + df["PREFERENCE_FIT_SCORE"] * w_pref_n
        + df["SIMILARITY_SCORE"] * w_sim_n
    ).clip(0, 100)

    # ML: ML_RISK_SCORE가 높을수록 위험 → (100 - ML_RISK_SCORE)가 안전 점수
    ml_safety = (100 - df["ML_RISK_SCORE"].fillna(50)).clip(0, 100)

    # 최종 추천점수: 룰(80%) + ML(20%)
    # 유저가 보는 round된 RULE_SCORE/ML_SAFETY_SCORE로 계산해서 일관성 보장
    df["RULE_SCORE"] = raw_rule.round(1)
    df["ML_SAFETY_SCORE"] = ml_safety.round(1)
    df["RECOMMENDATION_SCORE"] = (df["RULE_SCORE"] * 0.80 + df["ML_SAFETY_SCORE"] * 0.20).clip(0, 100).round(1)

    df["PROFILE"] = survey_result["profile"]
    df["ALPHA"] = alpha
    df["BETA"] = beta
    df["GAMMA"] = gamma
    df["DELTA"] = delta

    df["IS_CANDIDATE"] = candidate_mask
    df["FILTER_MATCH"] = df["IS_CANDIDATE"] | (
        df["BUDGET_BAND_MATCH"]
        & df["AREA_SCOPE_MATCH"]
        & df["HAS_ENOUGH_TX"]
        & df["NOT_ABSOLUTE_RISK"]
    )

    sorted_df = df.sort_values(
        ["FILTER_MATCH", "RECOMMENDATION_SCORE", "SAFETY_SCORE", "BACKTEST_SCORE"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)
    sorted_df["RECOMMENDATION_RANK"] = sorted_df.index + 1

    candidate_score = float(
        sorted_df.loc[sorted_df["AREA_LABEL"] == candidate_area, "RECOMMENDATION_SCORE"].iloc[0]
    )
    sorted_df["VS_CANDIDATE_DELTA"] = (
        sorted_df["RECOMMENDATION_SCORE"] - candidate_score
    ).round(1)

    candidate_loss = float(
        sorted_df.loc[sorted_df["AREA_LABEL"] == candidate_area, "LOSS_EXPOSURE_AMOUNT"].iloc[0]
    )
    sorted_df["LOSS_EXPOSURE_DELTA"] = (
        candidate_loss - sorted_df["LOSS_EXPOSURE_AMOUNT"]
    ).round(0)
    sorted_df["PRICE_DELTA_TO_CANDIDATE"] = (
        sorted_df["ESTIMATED_TOTAL_JEONSE"] - candidate_jeonse
    ).round(0)

    sorted_df["BETTER_ALTERNATIVE"] = (
        (~sorted_df["IS_CANDIDATE"])
        & sorted_df["FILTER_MATCH"]
        & (sorted_df["RECOMMENDATION_SCORE"] > candidate_score)
        & (sorted_df["LOSS_EXPOSURE_AMOUNT"] <= candidate_loss)
    )

    return sorted_df


def pick_typed_alternatives(
    better_df: pd.DataFrame,
    candidate_row: pd.Series,
) -> Dict[str, Any]:
    """3가지 유형의 대안을 선별한다.

    Returns:
        dict with keys "safest", "balanced", "similar" — 각각 Series or None.
    """
    result: Dict[str, Any] = {"safest": None, "balanced": None, "similar": None}
    if better_df.empty:
        return result

    # 1. 가장 안전한 대안: 손실 노출이 가장 낮은 후보
    safest = better_df.sort_values("LOSS_EXPOSURE_AMOUNT", ascending=True).iloc[0]
    result["safest"] = safest

    # 2. 가장 균형 잡힌 대안: 추천점수(안전+선호 종합) 1위
    balanced = better_df.sort_values("RECOMMENDATION_SCORE", ascending=False).iloc[0]
    # safest와 같으면 2위로
    if balanced["AREA_LABEL"] == safest["AREA_LABEL"] and len(better_df) > 1:
        balanced = better_df.sort_values("RECOMMENDATION_SCORE", ascending=False).iloc[1]
    result["balanced"] = balanced

    # 3. 현재 후보와 가장 비슷하지만 더 안전한 대안: 유사도 점수 1위
    similar = better_df.sort_values("SIMILARITY_SCORE", ascending=False).iloc[0]
    # 위 2개와 중복되면 다음 후보
    used = {safest["AREA_LABEL"], balanced["AREA_LABEL"]}
    similar_candidates = better_df.sort_values("SIMILARITY_SCORE", ascending=False)
    for _, row in similar_candidates.iterrows():
        if row["AREA_LABEL"] not in used:
            similar = row
            break
    result["similar"] = similar

    return result


def build_card_description(
    row: pd.Series,
    candidate_row: pd.Series,
) -> List[str]:
    """추천 카드에 표시할 핵심 개선 포인트 리스트를 반환한다."""
    points: List[str] = []

    cand_rate = float(candidate_row.get("JEONSE_RATE", 0))
    row_rate = float(row.get("JEONSE_RATE", 0))
    if cand_rate > 0 and row_rate < cand_rate:
        points.append(f"전세가율 {cand_rate:.1f}% → **{row_rate:.1f}%**로 더 낮음")

    grade_order = {"A": 0, "B": 1, "C": 2, "D": 3}
    if grade_order.get(str(row.get("GRADE", "")), 9) < grade_order.get(str(candidate_row.get("GRADE", "")), 9):
        points.append(f"안전등급 **{row['GRADE']}**로 더 안전")

    row_structural = pd.to_numeric(pd.Series([row.get("STRUCTURAL_SCORE")]), errors="coerce").iloc[0]
    cand_structural = pd.to_numeric(pd.Series([candidate_row.get("STRUCTURAL_SCORE")]), errors="coerce").iloc[0]
    if pd.notna(row_structural) and pd.notna(cand_structural) and row_structural >= cand_structural + 8:
        points.append(f"구조 비교 점수 **{row_structural:.1f}점**으로 장기 체력이 더 안정적")

    if str(row.get("SGG", "")) == str(candidate_row.get("SGG", "")):
        points.append("같은 구라 **생활권 유지**")

    bucket_count = row.get("BUCKET_RENT_COUNT", 0)
    if bucket_count and bucket_count >= 10:
        points.append(f"최근 6개월 거래 **{int(bucket_count)}건**으로 활발함")

    return points[:4]


def build_candidate_summary(row: pd.Series, survey_result: Dict[str, Any]) -> str:
    grade_meaning = GRADE_MEANINGS.get(row["GRADE"], "참고")
    summary = (
        f"{row['AREA_LABEL']}은 공통 안전등급 {row['GRADE']}({grade_meaning})이며 "
        f"객관 레이어인 안전점수는 {row['SAFETY_SCORE']:.1f}점입니다. "
        f"이 위에 설문으로 분류된 {survey_result['profile']} 성향을 반영해 "
        f"손실 패널티와 선호 적합도를 다시 계산한 최종 추천점수는 {row['RECOMMENDATION_SCORE']:.1f}점입니다."
    )
    structural_score = pd.to_numeric(pd.Series([row.get("STRUCTURAL_SCORE")]), errors="coerce").iloc[0]
    if pd.notna(structural_score):
        summary += (
            f" 국토부 시장 점수는 {row['MARKET_SCORE']:.1f}점, "
            f"리치고+SPH 구조 비교 점수는 {structural_score:.1f}점입니다."
        )
    return summary


def build_profile_summary(survey_result: Dict[str, Any]) -> str:
    profile = survey_result["profile"]
    desc = PROFILE_DESCRIPTIONS.get(profile, "")
    return f"{profile} — {desc}"


def build_recommendation_reasons(
    row: pd.Series,
    candidate_row: pd.Series,
    survey_result: Dict[str, Any],
) -> List[str]:
    reasons: List[str] = []
    profile = survey_result["profile"]

    if profile == "보수형" and float(row["LOSS_EXPOSURE_DELTA"]) > 0:
        reasons.append(
            f"보수형 기준에서 손실 노출이 {format_currency_krw(row['LOSS_EXPOSURE_DELTA'])} 낮아 우선순위가 올라갔습니다."
        )
    elif profile == "모험형" and float(row["PREFERENCE_FIT_SCORE"]) >= float(candidate_row["PREFERENCE_FIT_SCORE"]):
        reasons.append("모험형 기준에서 생활권 적합도와 현재 후보 유사성이 더 잘 맞습니다.")
    elif float(row["VS_CANDIDATE_DELTA"]) > 0:
        reasons.append(f"현재 후보 대비 최종 추천점수가 {row['VS_CANDIDATE_DELTA']:.1f}점 높습니다.")

    if float(row["LOSS_EXPOSURE_DELTA"]) > 0:
        reasons.append(
            f"입력 보증금 기준 예상 손실 노출이 {format_currency_krw(row['LOSS_EXPOSURE_DELTA'])} 낮습니다."
        )

    if float(row["PRICE_OVERHEAT_PENALTY_SCORE"]) < float(candidate_row["PRICE_OVERHEAT_PENALTY_SCORE"]):
        reasons.append("가격 과열 패널티가 현재 후보보다 낮아 가격 부담이 덜합니다.")

    if float(row["PREFERENCE_FIT_SCORE"]) > float(candidate_row["PREFERENCE_FIT_SCORE"]):
        reasons.append("생활권 적합도가 더 높아 설문 응답 기준 선호 조건과 더 잘 맞습니다.")

    row_structural = pd.to_numeric(pd.Series([row.get("STRUCTURAL_SCORE")]), errors="coerce").iloc[0]
    cand_structural = pd.to_numeric(pd.Series([candidate_row.get("STRUCTURAL_SCORE")]), errors="coerce").iloc[0]
    if pd.notna(row_structural) and pd.notna(cand_structural) and row_structural > cand_structural + 8:
        reasons.append(f"구조 비교 점수도 현재 후보보다 {row_structural - cand_structural:.1f}점 높습니다.")

    if str(row["SGG"]) == str(candidate_row["SGG"]):
        reasons.append("현재 관심 구를 유지하면서 더 안전한 대안을 제시합니다.")

    return reasons[:3]


def build_exclusion_reasons(row: pd.Series, candidate_row: pd.Series) -> List[str]:
    """해당 지역이 추천 대안에서 제외된 다양한 이유를 반환한다."""
    reasons: List[str] = []

    # 1. 예산 초과
    if not row.get("BUDGET_BAND_MATCH", True):
        reasons.append(
            f"💰 예상 전세가가 {format_currency_krw(row['ESTIMATED_TOTAL_JEONSE'])}로 보증금 한도를 초과"
        )

    # 2. 탐색 범위 밖
    if not row.get("AREA_SCOPE_MATCH", True):
        reasons.append("📍 설정한 탐색 범위(구/생활권) 밖에 있음")

    # 3. 거래 부족
    if not row.get("HAS_ENOUGH_TX", True):
        tx_count = int(row.get('NET_MIG', 0))
        reasons.append(f"📉 최근 6개월 거래 {tx_count}건뿐이라 시세 신뢰도 낮음")

    # 4. 절대 위험
    if not row.get("NOT_ABSOLUTE_RISK", True):
        rate = float(row.get('JEONSE_RATE', 0))
        reasons.append(f"⚠️ 전세가율 {rate:.1f}%로 깡통전세 위험 구간")

    # 5. 안전 등급 낮음
    cand_grade = str(candidate_row.get("GRADE", ""))
    row_grade = str(row.get("GRADE", ""))
    grade_order = {"A": 0, "B": 1, "C": 2, "D": 3}
    if grade_order.get(row_grade, 0) > grade_order.get(cand_grade, 0):
        reasons.append(
            f"🏷️ 안전등급 {row_grade}({GRADE_MEANINGS.get(row_grade, '')})로 현재 후보({cand_grade})보다 낮음"
        )

    # 6. 적합도 낮음
    if float(row["RECOMMENDATION_SCORE"]) <= float(candidate_row["RECOMMENDATION_SCORE"]):
        delta = float(candidate_row["RECOMMENDATION_SCORE"]) - float(row["RECOMMENDATION_SCORE"])
        if delta > 0.5:
            reasons.append(f"📊 적합도가 현재 후보보다 {delta:.1f}점 낮음")

    # 7. 전세가율 부담
    jeonse_rate = float(row.get("JEONSE_RATE", 0))
    cand_rate = float(candidate_row.get("JEONSE_RATE", 0))
    if 70 < jeonse_rate <= 90 and jeonse_rate > cand_rate + 5:
        reasons.append(f"💸 전세가율 {jeonse_rate:.1f}%로 매매가 대비 전세가가 부담스러운 수준")

    # 8. AI 하락 위험도 높음
    ml_risk = float(row.get("ML_RISK_SCORE", 50))
    cand_ml = float(candidate_row.get("ML_RISK_SCORE", 50))
    if ml_risk > 60 and ml_risk > cand_ml + 10:
        reasons.append(f"🤖 AI 예측 하락 위험도 {ml_risk:.0f}%로 향후 6개월 가격 하락 가능성 큼")

    # 9. 과거 가격 변동성
    backtest = float(row.get("BACKTEST_SCORE", 50))
    cand_backtest = float(candidate_row.get("BACKTEST_SCORE", 50))
    if backtest < 30 and backtest < cand_backtest:
        reasons.append("📈 과거 전세가 변동폭이 커서 가격 안정성 부족")

    # 10. 평형대 거래 부족 (해당 평형 데이터 없음)
    bucket_count = row.get("BUCKET_RENT_COUNT", None)
    if bucket_count is not None and bucket_count < 5:
        reasons.append("🏠 해당 평형대 최근 거래가 적어 시세 추정 정확도 낮음")

    # 11. 다른 구
    if str(row.get("SGG", "")) != str(candidate_row.get("SGG", "")):
        reasons.append(f"🚇 현재 관심 구({candidate_row.get('SGG', '')})와 다른 지역")

    return reasons[:5]
