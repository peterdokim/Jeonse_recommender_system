from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd
import streamlit as st

DIMENSION_LABELS = {
    "S_RATE": "전세가율",
    "S_MIG": "전입전출",
    "S_SUB": "지하철",
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
    return f"{value:,.0f}원"


def percentile_score(series: pd.Series, higher_is_better: bool = True, fill_value: float = 50.0) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.dropna().empty:
        return pd.Series(fill_value, index=series.index, dtype="float64")
    ranked = numeric.rank(pct=True, method="average", ascending=higher_is_better) * 100
    return ranked.fillna(fill_value).round(1)


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

    df["ESTIMATED_TOTAL_JEONSE"] = (df["JEONSE_LATEST"] * preferred_pyeong * unit_multiplier).round(0)
    df["ESTIMATED_TOTAL_SALE"] = (df["MEME_LATEST"] * preferred_pyeong * unit_multiplier).round(0)
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

    # 보증금 이하 + 허용 범위만큼 위까지 포함
    # 예: 보증금 5억, 허용 10% → 0원 ~ 5.5억까지 매칭
    upper_budget = deposit_amount * (1 + budget_tolerance_pct / 100)
    df["BUDGET_BAND_MATCH"] = df["ESTIMATED_TOTAL_JEONSE"] <= upper_budget

    if search_scope == "현재 관심 구 우선":
        df["AREA_SCOPE_MATCH"] = df["SGG"] == candidate_sgg
    elif search_scope == "출퇴근권 우선":
        df["AREA_SCOPE_MATCH"] = df["SGG"].isin([candidate_sgg, workplace_sgg])
    else:
        df["AREA_SCOPE_MATCH"] = True

    convenience_weight = 0.35 + float(survey_result["convenience_preference"]) / 100 * 0.30
    lifestyle_weight = 1 - convenience_weight

    df["SAFETY_SCORE"] = df["TOTAL_SCORE"].fillna(0).round(1)
    df["LOSS_PENALTY_SCORE"] = (100 - df["LOSS_EXPOSURE_SCORE"]).clip(0, 100).round(1)
    df["PRICE_OVERHEAT_PENALTY_SCORE"] = (100 - df["PRICE_CONTEXT_SCORE"]).clip(0, 100).round(1)
    df["PREFERENCE_FIT_SCORE"] = (
        df["COMMUTE_FIT_SCORE"] * convenience_weight
        + df["LIFESTYLE_FIT_SCORE"] * lifestyle_weight
    ).round(1)

    # README 수식과 일치:
    # 추천점수 = 안전점수 - α*손실패널티 - β*가격과열패널티 + γ*선호적합도 + δ*유사도
    df["RECOMMENDATION_SCORE"] = (
        df["SAFETY_SCORE"]
        - df["LOSS_PENALTY_SCORE"] * alpha
        - df["PRICE_OVERHEAT_PENALTY_SCORE"] * beta
        + df["PREFERENCE_FIT_SCORE"] * gamma
        + df["SIMILARITY_SCORE"] * delta
    ).clip(0, 100).round(1)

    df["PROFILE"] = survey_result["profile"]
    df["ALPHA"] = alpha
    df["BETA"] = beta
    df["GAMMA"] = gamma
    df["DELTA"] = delta

    df["IS_CANDIDATE"] = candidate_mask
    df["FILTER_MATCH"] = df["IS_CANDIDATE"] | (df["BUDGET_BAND_MATCH"] & df["AREA_SCOPE_MATCH"])

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

    delta = float(row["VS_CANDIDATE_DELTA"])
    if delta > 0:
        points.append(f"현재 후보보다 종합 점수 **+{delta:.1f}점**")

    loss_diff = float(candidate_row["LOSS_EXPOSURE_AMOUNT"]) - float(row["LOSS_EXPOSURE_AMOUNT"])
    if loss_diff > 0:
        points.append(f"입력 보증금 기준 손실 노출 **{format_currency_krw(loss_diff)} 감소**")

    cand_rate = float(candidate_row.get("JEONSE_RATE", 0))
    row_rate = float(row.get("JEONSE_RATE", 0))
    if cand_rate > 0 and row_rate < cand_rate:
        points.append(f"전세가율 **{cand_rate:.1f}% → {row_rate:.1f}%**로 가격 부담 완화")

    grade_order = {"A": 0, "B": 1, "C": 2, "D": 3}
    if grade_order.get(str(row.get("GRADE", "")), 9) < grade_order.get(str(candidate_row.get("GRADE", "")), 9):
        points.append(f"안전등급 **{row['GRADE']}** (현재 후보 {candidate_row['GRADE']})")

    if str(row.get("SGG", "")) == str(candidate_row.get("SGG", "")):
        points.append("현재 관심 구 **생활권 유지**")

    pref_diff = float(row.get("PREFERENCE_FIT_SCORE", 0)) - float(candidate_row.get("PREFERENCE_FIT_SCORE", 0))
    if pref_diff > 5:
        points.append(f"생활권 적합도 **+{pref_diff:.1f}점** 개선")

    return points[:5]


def build_candidate_summary(row: pd.Series, survey_result: Dict[str, Any]) -> str:
    grade_meaning = GRADE_MEANINGS.get(row["GRADE"], "참고")
    return (
        f"{row['AREA_LABEL']}은 공통 안전등급 {row['GRADE']}({grade_meaning})이며 "
        f"객관 레이어인 안전점수는 {row['SAFETY_SCORE']:.1f}점입니다. "
        f"이 위에 설문으로 분류된 {survey_result['profile']} 성향을 반영해 "
        f"손실 패널티와 선호 적합도를 다시 계산한 최종 추천점수는 {row['RECOMMENDATION_SCORE']:.1f}점입니다."
    )


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

    if str(row["SGG"]) == str(candidate_row["SGG"]):
        reasons.append("현재 관심 구를 유지하면서 더 안전한 대안을 제시합니다.")

    return reasons[:3]


def build_exclusion_reasons(row: pd.Series, candidate_row: pd.Series) -> List[str]:
    """해당 지역이 추천 대안에서 제외된 이유를 반환한다."""
    reasons: List[str] = []

    # 필터 조건
    if not row.get("BUDGET_BAND_MATCH", True):
        reasons.append(
            f"추정 전세가({format_currency_krw(row['ESTIMATED_TOTAL_JEONSE'])})가 "
            f"입력 보증금보다 높음"
        )

    if not row.get("AREA_SCOPE_MATCH", True):
        reasons.append("설정한 탐색 범위(구/생활권) 밖")

    # 안전 등급
    cand_grade = str(candidate_row.get("GRADE", ""))
    row_grade = str(row.get("GRADE", ""))
    grade_order = {"A": 0, "B": 1, "C": 2, "D": 3}
    if grade_order.get(row_grade, 0) > grade_order.get(cand_grade, 0):
        reasons.append(
            f"안전등급 {row_grade}({GRADE_MEANINGS.get(row_grade, '')})로 "
            f"현재 후보({cand_grade})보다 낮음"
        )

    # 추천점수
    if float(row["RECOMMENDATION_SCORE"]) <= float(candidate_row["RECOMMENDATION_SCORE"]):
        delta = float(candidate_row["RECOMMENDATION_SCORE"]) - float(row["RECOMMENDATION_SCORE"])
        reasons.append(f"최종 추천점수가 현재 후보보다 {delta:.1f}점 낮음")

    # 손실 노출
    if float(row["LOSS_EXPOSURE_AMOUNT"]) > float(candidate_row["LOSS_EXPOSURE_AMOUNT"]):
        diff = float(row["LOSS_EXPOSURE_AMOUNT"]) - float(candidate_row["LOSS_EXPOSURE_AMOUNT"])
        reasons.append(f"예상 손실 노출이 {format_currency_krw(diff)} 더 높음")

    # 전세가율 과열
    jeonse_rate = float(row.get("JEONSE_RATE", 0))
    if jeonse_rate > 80:
        reasons.append(f"전세가율 {jeonse_rate:.1f}%로 가격 부담이 큼")

    # 과거 가격 하락 이력
    backtest = float(row.get("BACKTEST_SCORE", 50))
    cand_backtest = float(candidate_row.get("BACKTEST_SCORE", 50))
    if backtest < 30 and backtest < cand_backtest:
        reasons.append("과거 전세가 하락폭이 커서 가격 안정성이 낮음")

    return reasons[:4]
