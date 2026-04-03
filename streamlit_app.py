import altair as alt
import pandas as pd
import streamlit as st

from common.queries import (
    load_all_area_history,
    load_area_history,
    load_grade_summary,
    load_latest_market_snapshot,
    load_market_summary,
    load_scores,
)
from common.session import get_snowpark_session

st.set_page_config(
    page_title="전세 안심 검진소",
    page_icon="house",
    layout="wide",
)

DIMENSION_LABELS = {
    "S1_RATE": "전세가율",
    "S2_DROP": "과거 하락폭",
    "S3_HUG": "HUG 사고율",
    "S4_POP": "거주인구 변화",
    "S5_ASSET": "주민 자산",
    "S6_SUBWAY": "지하철 접근성",
}


GRADE_MEANINGS = {
    "A": "안전",
    "B": "보통",
    "C": "주의",
    "D": "위험",
}


def build_area_label(df: pd.DataFrame) -> pd.Series:
    return df["SGG"].astype(str) + " " + df["EMD"].astype(str)


def get_selected_row(df: pd.DataFrame, area_label: str) -> pd.Series:
    return df.loc[df["AREA_LABEL"] == area_label].iloc[0]


def format_currency_krw(value: float) -> str:
    return f"{value:,.0f}원"


def estimate_loss(row: pd.Series, deposit_amount: float) -> float:
    return deposit_amount * abs(float(row["JEONSE_DROP_PCT"])) / 100


def detect_price_unit_multiplier(df: pd.DataFrame) -> int:
    median_price = pd.to_numeric(df["JEONSE_LATEST"], errors="coerce").median()
    return 10_000 if median_price < 100_000 else 1


def estimate_total_jeonse(price_per_pyeong: float, preferred_pyeong: float, unit_multiplier: int) -> float:
    return float(price_per_pyeong) * preferred_pyeong * unit_multiplier


def grade_color(grade: str) -> str:
    return {
        "A": "#1b5e20",
        "B": "#2e7d32",
        "C": "#ef6c00",
        "D": "#c62828",
    }.get(grade, "#455a64")


def grade_bg_color(grade: str) -> str:
    return {
        "A": "#e8f5e9",
        "B": "#edf7ed",
        "C": "#fff3e0",
        "D": "#ffebee",
    }.get(grade, "#eceff1")


def risk_band(score: float) -> tuple[str, str]:
    if score >= 80:
        return "안심권", "#1b5e20"
    if score >= 60:
        return "관심권", "#2e7d32"
    if score >= 40:
        return "주의권", "#ef6c00"
    return "위험권", "#c62828"


def to_eok(value: float) -> float:
    return value / 100_000_000


def from_eok(value: float) -> int:
    return int(value * 100_000_000)


def sync_deposit_from_slider() -> None:
    st.session_state["deposit_amount"] = from_eok(st.session_state["deposit_slider_eok"])


def sync_slider_from_input() -> None:
    st.session_state["deposit_slider_eok"] = to_eok(st.session_state["deposit_amount"])


def set_deposit_amount(amount: int) -> None:
    st.session_state["deposit_amount"] = amount
    st.session_state["deposit_slider_eok"] = to_eok(amount)


def initialize_deposit_state(default_amount: int = 500_000_000) -> None:
    if "deposit_amount" not in st.session_state:
        st.session_state["deposit_amount"] = default_amount
    if "deposit_slider_eok" not in st.session_state:
        st.session_state["deposit_slider_eok"] = to_eok(st.session_state["deposit_amount"])


def build_ai_commentary(row: pd.Series, deposit_amount: float) -> str:
    estimated_loss = estimate_loss(row, deposit_amount)
    grade_meaning = GRADE_MEANINGS.get(row["GRADE"], "참고")

    if row["GRADE"] in ["A", "B"]:
        stance = "현재 데이터 기준으로는 상대적으로 방어력이 있는 편입니다."
    elif row["GRADE"] == "C":
        stance = "주의 구간으로 해석하는 것이 적절하며, 보증보험 검토가 특히 중요합니다."
    else:
        stance = "위험 구간으로 보이며, 비슷한 가격대 대안 비교가 우선입니다."

    return (
        f"{row['SGG']} {row['EMD']}의 종합 등급은 {row['GRADE']}({grade_meaning})입니다. "
        f"전세가율은 {row['JEONSE_RATE']:.1f}%이고, 과거 최대 하락폭 기준 보증금 손실 가능 금액은 "
        f"약 {format_currency_krw(estimated_loss)}입니다. "
        f"HUG 사고율은 {row['HUG_RATE']:.2f}%이며, 지하철 평균 접근성은 {row['AVG_DISTANCE_M']:.0f}m입니다. "
        f"{stance}"
    )


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        :root {
            --brand-ink: #123524;
            --brand-soft: #eef6f0;
            --brand-line: #d6e6d9;
            --brand-accent: #2f7d4a;
            --warn-soft: #fff3e0;
            --risk-soft: #ffebee;
        }
        .block-container {
            padding-top: 2rem;
        }
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #f4f8f5 0%, #edf4ef 100%);
            border-right: 1px solid #d9e6dd;
        }
        .hero-card {
            background: linear-gradient(135deg, #173b2b 0%, #2f6e4d 100%);
            border-radius: 22px;
            padding: 1.15rem 1.2rem;
            color: white;
            box-shadow: 0 18px 38px rgba(18, 53, 36, 0.16);
            margin-bottom: 1rem;
        }
        .hero-kicker {
            font-size: 0.78rem;
            opacity: 0.8;
            letter-spacing: 0.04em;
            text-transform: uppercase;
        }
        .hero-title {
            font-size: 1.2rem;
            font-weight: 700;
            margin-top: 0.25rem;
        }
        .hero-sub {
            font-size: 0.9rem;
            opacity: 0.92;
            margin-top: 0.3rem;
            line-height: 1.45;
        }
        .sidebar-card {
            background: rgba(255,255,255,0.9);
            border: 1px solid var(--brand-line);
            border-radius: 18px;
            padding: 0.95rem 1rem;
            margin: 0.7rem 0;
            box-shadow: 0 8px 24px rgba(16, 24, 40, 0.05);
        }
        .sidebar-card-title {
            color: var(--brand-ink);
            font-size: 0.84rem;
            font-weight: 700;
            letter-spacing: 0.02em;
            margin-bottom: 0.5rem;
        }
        .sidebar-value {
            color: #173b2b;
            font-size: 1.1rem;
            font-weight: 700;
        }
        .sidebar-label {
            color: #52635a;
            font-size: 0.78rem;
        }
        .badge {
            display: inline-block;
            padding: 0.24rem 0.56rem;
            border-radius: 999px;
            font-size: 0.75rem;
            font-weight: 700;
            letter-spacing: 0.02em;
        }
        .metric-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 0.6rem;
            margin-top: 0.75rem;
        }
        .metric-pill {
            background: #f7fbf8;
            border: 1px solid #e1ece4;
            border-radius: 14px;
            padding: 0.65rem 0.75rem;
        }
        .metric-pill .label {
            color: #65756d;
            font-size: 0.72rem;
            margin-bottom: 0.12rem;
        }
        .metric-pill .value {
            color: #173b2b;
            font-size: 0.95rem;
            font-weight: 700;
        }
        .deposit-note {
            background: #f7fbf8;
            border: 1px solid #dbe9df;
            border-radius: 16px;
            padding: 0.75rem 0.9rem;
            margin: 0.5rem 0 0.9rem 0;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar_hero() -> None:
    st.sidebar.markdown(
        """
        <div class="hero-card">
            <div class="hero-kicker">Jeonse Safety Lab</div>
            <div class="hero-title">전세 안심 검진소</div>
            <div class="hero-sub">가격대가 비슷한 동네 중 어디가 더 안전한지 빠르게 설명하는 발표형 데모입니다.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar_selection(row: pd.Series, selected_sgg: str, selected_emd: str, deposit_amount: int) -> None:
    band_label, band_color = risk_band(float(row["TOTAL_SCORE"]))
    loss_amount = estimate_loss(row, deposit_amount)
    st.sidebar.markdown(
        f"""
        <div class="sidebar-card">
            <div class="sidebar-card-title">현재 리스크 요약</div>
            <div style="display:flex; align-items:center; justify-content:space-between; gap:0.5rem;">
                <div class="sidebar-value">{selected_sgg} {selected_emd}</div>
                <span class="badge" style="background:{grade_bg_color(row['GRADE'])}; color:{grade_color(row['GRADE'])};">
                    {row['GRADE']} · {GRADE_MEANINGS[row['GRADE']]}
                </span>
            </div>
            <div class="hero-sub" style="color:#52635a; margin-top:0.45rem;">
                총점 {int(row['TOTAL_SCORE'])}점 · {band_label}
            </div>
            <div class="metric-grid">
                <div class="metric-pill">
                    <div class="label">보증금</div>
                    <div class="value">{format_currency_krw(deposit_amount)}</div>
                </div>
                <div class="metric-pill">
                    <div class="label">예상 손실</div>
                    <div class="value">{format_currency_krw(loss_amount)}</div>
                </div>
                <div class="metric-pill">
                    <div class="label">전세가율</div>
                    <div class="value">{row['JEONSE_RATE']:.1f}%</div>
                </div>
                <div class="metric-pill">
                    <div class="label">HUG 사고율</div>
                    <div class="value">{row['HUG_RATE']:.2f}%</div>
                </div>
            </div>
            <div style="margin-top:0.7rem;">
                <span class="badge" style="background:{band_color}18; color:{band_color};">
                    리스크 밴드: {band_label}
                </span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar_info_cards() -> None:
    st.sidebar.markdown(
        """
        <div class="sidebar-card">
            <div class="sidebar-card-title">점수 가이드</div>
            <div class="metric-grid">
                <div class="metric-pill"><div class="label">A</div><div class="value">80~100</div></div>
                <div class="metric-pill"><div class="label">B</div><div class="value">60~79</div></div>
                <div class="metric-pill"><div class="label">C</div><div class="value">40~59</div></div>
                <div class="metric-pill"><div class="label">D</div><div class="value">0~39</div></div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.sidebar.markdown(
        """
        <div class="sidebar-card">
            <div class="sidebar-card-title">발표 포인트</div>
            <div class="sidebar-label">1. 전세가율과 과거 하락폭을 함께 설명합니다.</div>
            <div class="sidebar-label" style="margin-top:0.45rem;">2. 같은 가격대에서 더 안전한 대안을 제안합니다.</div>
            <div class="sidebar-label" style="margin-top:0.45rem;">3. 비교와 심층 분석으로 스토리텔링을 이어갑니다.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def percentile_score(series: pd.Series, higher_is_better: bool = True) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if not higher_is_better:
        numeric = -numeric
    return numeric.rank(pct=True, method="average") * 100


def clamp_score(value: float) -> float:
    return max(0.0, min(100.0, value))


def compute_backtest_metrics(all_history_df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for (sgg, emd), group in all_history_df.groupby(["SGG", "EMD"], sort=False):
        ordered = group.sort_values("YYYYMMDD")
        prices = pd.to_numeric(ordered["JEONSE_PRICE"], errors="coerce").dropna().reset_index(drop=True)

        if len(prices) < 3:
            continue

        worst_drops = []
        forward_returns = []
        horizon = 12

        for start_idx in range(len(prices) - 1):
            end_idx = min(start_idx + horizon, len(prices) - 1)
            window = prices.iloc[start_idx:end_idx + 1]
            start_price = prices.iloc[start_idx]
            if start_price <= 0:
                continue

            worst_drop_pct = (window.min() / start_price - 1) * 100
            forward_return_pct = (window.iloc[-1] / start_price - 1) * 100
            worst_drops.append(worst_drop_pct)
            forward_returns.append(forward_return_pct)

        if not worst_drops:
            continue

        worst_drawdown_pct = min(worst_drops)
        median_drawdown_pct = float(pd.Series(worst_drops).median())
        downside_hit_rate = float((pd.Series(worst_drops) <= -10).mean())
        recovery_rate = float((pd.Series(forward_returns) >= 0).mean())
        median_return_12m = float(pd.Series(forward_returns).median())

        drawdown_component = clamp_score(100 + worst_drawdown_pct * 2.5)
        stability_component = clamp_score(100 + median_drawdown_pct * 4.0)
        recovery_component = recovery_rate * 100
        crash_avoidance_component = 100 - downside_hit_rate * 100
        return_component = clamp_score(50 + median_return_12m * 2.0)

        backtest_score = round(
            drawdown_component * 0.30
            + stability_component * 0.25
            + recovery_component * 0.20
            + crash_avoidance_component * 0.15
            + return_component * 0.10,
            1,
        )

        rows.append(
            {
                "SGG": sgg,
                "EMD": emd,
                "BT_WORST_DRAWDOWN_PCT": round(worst_drawdown_pct, 1),
                "BT_MEDIAN_DRAWDOWN_PCT": round(median_drawdown_pct, 1),
                "BT_DOWNSIDE_HIT_RATE": round(downside_hit_rate * 100, 1),
                "BT_RECOVERY_RATE": round(recovery_rate * 100, 1),
                "BT_MEDIAN_RETURN_12M": round(median_return_12m, 1),
                "BACKTEST_SCORE": backtest_score,
            }
        )

    return pd.DataFrame(rows)


def build_recommendation_dataset(
    scores_df: pd.DataFrame,
    latest_snapshot_df: pd.DataFrame,
    backtest_df: pd.DataFrame,
    deposit_amount: int,
    workplace_sgg: str,
    preference_profile: str,
    preferred_pyeong: float,
    candidate_area: str,
) -> pd.DataFrame:
    unit_multiplier = detect_price_unit_multiplier(scores_df)

    merged = scores_df.merge(
        latest_snapshot_df,
        on=["SGG", "EMD"],
        how="left",
        suffixes=("", "_LATEST"),
    ).merge(
        backtest_df,
        on=["SGG", "EMD"],
        how="left",
    )

    merged["AREA_LABEL"] = build_area_label(merged)
    merged["ESTIMATED_TOTAL_JEONSE"] = merged["JEONSE_LATEST"].apply(
        lambda value: estimate_total_jeonse(value, preferred_pyeong, unit_multiplier)
    )
    merged["BUDGET_USAGE_PCT"] = merged["ESTIMATED_TOTAL_JEONSE"] / max(deposit_amount, 1) * 100

    def budget_fit_score(row: pd.Series) -> float:
        estimated_total = float(row["ESTIMATED_TOTAL_JEONSE"])
        if estimated_total <= 0:
            return 0.0
        if estimated_total <= deposit_amount:
            usage_ratio = estimated_total / deposit_amount
            return round(clamp_score(70 + usage_ratio * 30), 1)

        excess_ratio = (estimated_total - deposit_amount) / deposit_amount
        return round(clamp_score(70 - excess_ratio * 140), 1)

    merged["BUDGET_FIT_SCORE"] = merged.apply(budget_fit_score, axis=1)

    merged["WORK_POP_SCORE"] = percentile_score(merged["WORK_POP"], higher_is_better=True).fillna(50)
    merged["VISIT_POP_SCORE"] = percentile_score(merged["VISIT_POP"], higher_is_better=True).fillna(50)
    merged["RES_POP_SCORE"] = percentile_score(merged["RES_POP"], higher_is_better=True).fillna(50)
    merged["INCOME_SCORE"] = percentile_score(merged["AVG_INCOME"], higher_is_better=True).fillna(50)

    merged["COMMUTE_SCORE"] = merged.apply(
        lambda row: round(
            clamp_score(
                (80 if row["SGG"] == workplace_sgg else 35)
                + float(row["S6_SUBWAY"]) * 0.15
                + float(row["WORK_POP_SCORE"]) * 0.10
            ),
            1,
        ),
        axis=1,
    )

    merged["LIFESTYLE_SCORE"] = (
        merged["VISIT_POP_SCORE"] * 0.40
        + merged["WORK_POP_SCORE"] * 0.30
        + merged["S6_SUBWAY"] * 0.15
        + merged["INCOME_SCORE"] * 0.15
    ).round(1)

    preference_weights = {
        "안정성 우선": {
            "safety": 0.35,
            "backtest": 0.35,
            "budget": 0.15,
            "commute": 0.10,
            "lifestyle": 0.05,
        },
        "출퇴근 우선": {
            "safety": 0.20,
            "backtest": 0.20,
            "budget": 0.10,
            "commute": 0.40,
            "lifestyle": 0.10,
        },
        "예산 효율 우선": {
            "safety": 0.20,
            "backtest": 0.20,
            "budget": 0.40,
            "commute": 0.10,
            "lifestyle": 0.10,
        },
        "생활 편의 우선": {
            "safety": 0.20,
            "backtest": 0.20,
            "budget": 0.15,
            "commute": 0.15,
            "lifestyle": 0.30,
        },
    }
    weights = preference_weights[preference_profile]

    merged["BACKTEST_SCORE"] = merged["BACKTEST_SCORE"].fillna(merged["TOTAL_SCORE"] * 0.9)
    merged["RECOMMENDATION_SCORE"] = (
        merged["TOTAL_SCORE"] * weights["safety"]
        + merged["BACKTEST_SCORE"] * weights["backtest"]
        + merged["BUDGET_FIT_SCORE"] * weights["budget"]
        + merged["COMMUTE_SCORE"] * weights["commute"]
        + merged["LIFESTYLE_SCORE"] * weights["lifestyle"]
    ).round(1)

    candidate_score = float(
        merged.loc[merged["AREA_LABEL"] == candidate_area, "RECOMMENDATION_SCORE"].iloc[0]
    )
    merged["VS_CANDIDATE_DELTA"] = (merged["RECOMMENDATION_SCORE"] - candidate_score).round(1)
    merged["CANDIDATE_MATCH"] = merged["AREA_LABEL"] == candidate_area
    merged = merged.sort_values(
        ["RECOMMENDATION_SCORE", "TOTAL_SCORE", "BUDGET_FIT_SCORE"],
        ascending=[False, False, False],
    ).reset_index(drop=True)
    merged["RECOMMENDATION_RANK"] = merged.index + 1

    return merged


def make_history_chart(history_df: pd.DataFrame) -> alt.Chart:
    history_long = history_df.melt(
        id_vars=["YYYYMMDD"],
        value_vars=["PRICE", "JEONSE_PRICE"],
        var_name="Series",
        value_name="Price",
    )
    history_long["Series"] = history_long["Series"].map(
        {
            "PRICE": "매매가",
            "JEONSE_PRICE": "전세가",
        }
    )

    return (
        alt.Chart(history_long)
        .mark_line(point=True)
        .encode(
            x=alt.X("YYYYMMDD:T", title="월"),
            y=alt.Y("Price:Q", title="평당 가격"),
            color=alt.Color("Series:N", title="시계열"),
            tooltip=["YYYYMMDD:T", "Series:N", alt.Tooltip("Price:Q", format=",.0f")],
        )
    )


inject_styles()
st.title("전세 안심 검진소")
st.caption("모든 데이터 최대 활용 · Percentile 기반 점수")

session = get_snowpark_session()
scores_df = load_scores(session)
grade_df = load_grade_summary(session)
market_df = load_market_summary(session)
latest_snapshot_df = load_latest_market_snapshot(session)
all_area_history_df = load_all_area_history(session)
initialize_deposit_state()

if scores_df.empty:
    st.error(
        "No rows were returned from HACKATHON_APP.RESILIENCE.JEONSE_SAFETY_SCORE. "
        "Run setup.sql first, then refresh."
    )
    st.stop()

scores_df["AREA_LABEL"] = build_area_label(scores_df)
market_df["AREA_LABEL"] = build_area_label(market_df)

top_area = scores_df.iloc[0]
area_count = len(scores_df)
avg_score = round(float(scores_df["TOTAL_SCORE"].mean()), 1)

hero_1, hero_2, hero_3 = st.columns(3)
hero_1.metric("분석 동네 수", area_count)
hero_2.metric("평균 안전 점수", avg_score)
hero_3.metric("현재 1위", f'{top_area["SGG"]} {top_area["EMD"]}')

render_sidebar_hero()
selected_view = st.sidebar.radio(
    "메뉴",
    [
        "맞춤 추천 엔진 🤖",
        "전세 안전 진단 🏥",
        "위기 시뮬레이션 ⚠️",
        "동네 비교 📊",
        "동네 심층 분석 🔍",
    ],
)

with st.expander("점수 기준 요약", expanded=False):
    st.markdown(
        """
        - 총점 = 전세가율(30%) + 하락폭(15%) + HUG사고(15%) + 유동인구(10%) + 집주인건전성(10%) + 지하철(10%)
        - A: 80~100, B: 60~79, C: 40~59, D: 0~39
        - 과거 하락폭은 예측이 아니라 과거 실적 기준입니다.
        - 본 서비스는 투자 조언이 아닌 참고 정보입니다.
        """
    )

selected_sgg_col, selected_emd_col, deposit_col = st.columns([0.9, 1.1, 0.9])
selected_sgg = selected_sgg_col.selectbox("구 선택", sorted(scores_df["SGG"].unique().tolist()))
emd_options = (
    scores_df[scores_df["SGG"] == selected_sgg]["EMD"].sort_values().unique().tolist()
)
selected_emd = selected_emd_col.selectbox("동 선택", emd_options)
deposit_col.markdown("**보증금 입력**")
deposit_col.slider(
    "보증금 슬라이더 (억원)",
    min_value=0.0,
    max_value=20.0,
    value=st.session_state["deposit_slider_eok"],
    step=0.5,
    key="deposit_slider_eok",
    on_change=sync_deposit_from_slider,
)
deposit_col.number_input(
    "보증금 직접 입력 (원)",
    min_value=0,
    step=10_000_000,
    key="deposit_amount",
    on_change=sync_slider_from_input,
)
deposit_col.markdown(
    f"""
    <div class="deposit-note">
        <div class="sidebar-label">현재 설정</div>
        <div class="sidebar-value">{to_eok(st.session_state["deposit_amount"]):.1f}억</div>
    </div>
    """,
    unsafe_allow_html=True,
)

deposit_amount = st.session_state["deposit_amount"]

recommend_work_col, recommend_pref_col, recommend_size_col = st.columns([1, 1, 1])
workplace_sgg = recommend_work_col.selectbox(
    "근무지(구)",
    sorted(scores_df["SGG"].unique().tolist()),
)
preference_profile = recommend_pref_col.selectbox(
    "선호",
    ["안정성 우선", "출퇴근 우선", "예산 효율 우선", "생활 편의 우선"],
)
preferred_pyeong = recommend_size_col.slider(
    "선호 평형(평)",
    min_value=10,
    max_value=40,
    value=24,
    step=1,
)

selected_area = f"{selected_sgg} {selected_emd}"
selected_row = get_selected_row(scores_df, selected_area)
selected_history_df = load_area_history(session, selected_row["SGG"], selected_row["EMD"])
backtest_df = compute_backtest_metrics(all_area_history_df)
recommendation_df = build_recommendation_dataset(
    scores_df=scores_df,
    latest_snapshot_df=latest_snapshot_df,
    backtest_df=backtest_df,
    deposit_amount=deposit_amount,
    workplace_sgg=workplace_sgg,
    preference_profile=preference_profile,
    preferred_pyeong=preferred_pyeong,
    candidate_area=selected_area,
)
candidate_reco_row = recommendation_df.loc[
    recommendation_df["AREA_LABEL"] == selected_area
].iloc[0]

danji_col, note_col = st.columns([1, 2])
danji_col.selectbox(
    "단지 선택",
    ["동 단위 MVP에서는 미지원"],
    disabled=True,
)
note_col.caption(
    "현재 앱은 README 구조에 맞춘 동 단위 MVP입니다. "
    "평형은 평당 전세가 x 선호 평형으로 추정 보증금을 계산하며, 리치고 DANJI 시세와 DANJI INFO를 연결하면 단지 단위까지 확장할 수 있습니다."
)

st.caption(
    "면책 문구: 본 서비스는 투자 조언이 아닌 참고 정보입니다. "
    "탭2의 손실 계산은 과거 하락폭을 현재 보증금에 적용한 데모입니다."
)

render_sidebar_selection(candidate_reco_row, selected_sgg, selected_emd, deposit_amount)
render_sidebar_info_cards()

with st.sidebar.expander("데이터 범위", expanded=False):
    st.markdown(
        """
        - 현재 MVP는 `JEONSE_SAFETY_SCORE`, `RESILIENCE_BASE`, `HUG_ACCIDENT` 기반입니다.
        - `단지 선택`, `CARD_SALES`, `GENDER_AGE5`, `아정당 V01/V05/V06`는 확장 대상입니다.
        """
    )

with st.sidebar.expander("발표 포인트", expanded=False):
    st.markdown(
        """
        - 전세가율과 과거 하락폭을 함께 보여줍니다.
        - 같은 가격대에서 더 안전한 대안을 추천합니다.
        - 동네 비교와 심층 분석으로 스토리텔링이 가능합니다.
        """
    )

if selected_view == "맞춤 추천 엔진 🤖":
    st.subheader("맞춤 추천 엔진 🤖")
    st.caption(
        "보증금, 근무지, 선호, 평형, 현재 보고 있는 후보를 기준으로 "
        "역사적 전세가 백테스트와 현재 안전 지표를 함께 반영한 추천 점수를 계산합니다."
    )

    reco_1, reco_2, reco_3, reco_4 = st.columns(4)
    reco_1.metric("현재 후보 점수", f"{candidate_reco_row['RECOMMENDATION_SCORE']:.1f}")
    reco_2.metric("현재 후보 순위", f"{int(candidate_reco_row['RECOMMENDATION_RANK'])}위")
    reco_3.metric(
        "추정 보증금",
        format_currency_krw(float(candidate_reco_row["ESTIMATED_TOTAL_JEONSE"])),
    )
    reco_4.metric("백테스트 점수", f"{candidate_reco_row['BACKTEST_SCORE']:.1f}")

    top_recommendations_df = recommendation_df[
        (recommendation_df["RECOMMENDATION_RANK"] <= 7)
    ].copy()
    better_alternatives_df = recommendation_df[
        (recommendation_df["AREA_LABEL"] != selected_area)
        & (recommendation_df["VS_CANDIDATE_DELTA"] > 0)
    ].copy()

    chart_df = top_recommendations_df.head(7).copy()
    chart = (
        alt.Chart(chart_df)
        .mark_bar(cornerRadiusEnd=8)
        .encode(
            x=alt.X("RECOMMENDATION_SCORE:Q", title="추천 점수", scale=alt.Scale(domain=[0, 100])),
            y=alt.Y("AREA_LABEL:N", sort="-x", title="동네"),
            color=alt.condition(
                alt.datum.CANDIDATE_MATCH,
                alt.value("#ef6c00"),
                alt.value("#2f7d4a"),
            ),
            tooltip=[
                "AREA_LABEL",
                alt.Tooltip("RECOMMENDATION_SCORE:Q", format=".1f"),
                alt.Tooltip("BACKTEST_SCORE:Q", format=".1f"),
                alt.Tooltip("BUDGET_FIT_SCORE:Q", format=".1f"),
                alt.Tooltip("COMMUTE_SCORE:Q", format=".1f"),
            ],
        )
    )
    st.altair_chart(chart, use_container_width=True)

    explain_left, explain_right = st.columns([1.1, 1])
    with explain_left:
        st.markdown("#### 현재 후보 진단")
        current_summary_df = pd.DataFrame(
            [
                ("현재 후보", selected_area),
                ("선호 프로필", preference_profile),
                ("근무지", workplace_sgg),
                ("선호 평형", f"{preferred_pyeong}평"),
                ("예상 손실", format_currency_krw(estimate_loss(candidate_reco_row, deposit_amount))),
                ("추정 보증금", format_currency_krw(float(candidate_reco_row["ESTIMATED_TOTAL_JEONSE"]))),
                ("백테스트 최악 하락", f"{candidate_reco_row['BT_WORST_DRAWDOWN_PCT']:.1f}%"),
                ("12개월 회복률", f"{candidate_reco_row['BT_RECOVERY_RATE']:.1f}%"),
            ],
            columns=["항목", "내용"],
        )
        st.dataframe(current_summary_df, use_container_width=True, hide_index=True)

    with explain_right:
        st.markdown("#### 추천 점수 구성")
        profile_text = {
            "안정성 우선": "안전 점수와 백테스트 비중이 가장 큽니다.",
            "출퇴근 우선": "근무지 일치와 지하철 접근성 비중이 큽니다.",
            "예산 효율 우선": "추정 보증금과 사용 예산 적합도를 가장 강하게 반영합니다.",
            "생활 편의 우선": "방문/근무 인구와 접근성을 더 높게 반영합니다.",
        }[preference_profile]
        st.info(
            f"{profile_text} 현재 평형은 데이터 제약상 평당 전세가 x 선호 평형으로 추정했습니다."
        )

    st.markdown("#### 추천 동네 Top 5")
    st.dataframe(
        top_recommendations_df[
            [
                "RECOMMENDATION_RANK",
                "AREA_LABEL",
                "RECOMMENDATION_SCORE",
                "TOTAL_SCORE",
                "BACKTEST_SCORE",
                "BUDGET_FIT_SCORE",
                "COMMUTE_SCORE",
                "LIFESTYLE_SCORE",
                "VS_CANDIDATE_DELTA",
            ]
        ].head(5),
        use_container_width=True,
        hide_index=True,
    )

    if better_alternatives_df.empty:
        st.success("현재 후보가 입력 조건 기준으로 이미 상위권입니다.")
    else:
        best_alt = better_alternatives_df.iloc[0]
        st.markdown("#### 현재 후보보다 더 나은 대안")
        st.success(
            f"{best_alt['AREA_LABEL']}은(는) 현재 후보 대비 추천 점수가 "
            f"{best_alt['VS_CANDIDATE_DELTA']:.1f}점 높습니다. "
            f"백테스트 {best_alt['BACKTEST_SCORE']:.1f}점, 예산 적합도 {best_alt['BUDGET_FIT_SCORE']:.1f}점, "
            f"출퇴근 적합도 {best_alt['COMMUTE_SCORE']:.1f}점입니다."
        )
        st.dataframe(
            better_alternatives_df[
                [
                    "AREA_LABEL",
                    "RECOMMENDATION_SCORE",
                    "VS_CANDIDATE_DELTA",
                    "BACKTEST_SCORE",
                    "BT_WORST_DRAWDOWN_PCT",
                    "BT_RECOVERY_RATE",
                    "ESTIMATED_TOTAL_JEONSE",
                    "BUDGET_FIT_SCORE",
                ]
            ].head(5),
            use_container_width=True,
            hide_index=True,
        )

elif selected_view == "전세 안전 진단 🏥":
    st.subheader("전세 안전 진단 🏥")
    card_1, card_2, card_3, card_4 = st.columns(4)
    card_1.metric("종합 등급", selected_row["GRADE"])
    card_2.metric("총점", int(selected_row["TOTAL_SCORE"]))
    card_3.metric("전세가율", f'{selected_row["JEONSE_RATE"]:.1f}%')
    card_4.metric("HUG 사고율", f'{selected_row["HUG_RATE"]:.2f}%')

    dimension_df = pd.DataFrame(
        {
            "차원": [DIMENSION_LABELS[key] for key in DIMENSION_LABELS],
            "점수": [selected_row[key] for key in DIMENSION_LABELS],
        }
    )

    left, right = st.columns([1, 1.1])
    with left:
        st.markdown("#### 6차원 점수")
        score_chart = (
            alt.Chart(dimension_df)
            .mark_bar(cornerRadiusEnd=8)
            .encode(
                x=alt.X("점수:Q", scale=alt.Scale(domain=[0, 100])),
                y=alt.Y("차원:N", sort="-x"),
                color=alt.Color("점수:Q", legend=None),
                tooltip=["차원", "점수"],
            )
        )
        st.altair_chart(score_chart, use_container_width=True)

    with right:
        estimated_loss = estimate_loss(selected_row, deposit_amount)
        summary_lines = [
            f"- `{selected_area}`의 현재 안전 등급은 `{selected_row['GRADE']}` 입니다.",
            f"- 과거 하락폭 기준 최대 손실 시나리오는 약 `{format_currency_krw(estimated_loss)}` 입니다.",
            f"- 전세가율은 `{selected_row['JEONSE_RATE']:.1f}%`, 지하철 평균 거리는 `{selected_row['AVG_DISTANCE_M']:.0f}m` 입니다.",
        ]
        if selected_row["GRADE"] in ["A", "B"]:
            summary_lines.append("- 현재 데이터 기준으로는 상대적으로 방어력이 있는 편입니다.")
        else:
            summary_lines.append("- 보증보험 검토와 대안 비교를 함께 보여주는 흐름이 적합합니다.")

        st.markdown("#### 설명 요약")
        st.markdown("\n".join(summary_lines))

        st.markdown("#### 기준값")
        st.dataframe(
            pd.DataFrame(
                [
                    ("현재 전세가", selected_row["JEONSE_LATEST"]),
                    ("현재 매매가", selected_row["MEME_LATEST"]),
                    ("거주인구 변화율", selected_row["POP_CHANGE_PCT"]),
                    ("평균 자산", selected_row["AVG_ASSET"]),
                ],
                columns=["지표", "값"],
            ),
            use_container_width=True,
            hide_index=True,
        )

        st.markdown("#### Cortex AI 해설")
        st.info(build_ai_commentary(selected_row, deposit_amount))

elif selected_view == "위기 시뮬레이션 ⚠️":
    st.subheader("위기 시뮬레이션 ⚠️")
    estimated_loss = estimate_loss(selected_row, deposit_amount)
    ratio_flag = "주의" if float(selected_row["JEONSE_RATE"]) >= 100 else "양호"

    sim_1, sim_2, sim_3 = st.columns(3)
    sim_1.metric("과거 최대 하락폭", f'{selected_row["JEONSE_DROP_PCT"]:.1f}%')
    sim_2.metric("보증금 기준 손실", format_currency_krw(estimated_loss))
    sim_3.metric("깡통전세 가능성", ratio_flag)

    st.caption('주의: "예측"이 아니라 "과거에 이 정도까지 떨어진 적 있음"을 보여주는 시뮬레이션입니다.')

    lower_bound = float(selected_row["JEONSE_LATEST"]) * 0.9
    upper_bound = float(selected_row["JEONSE_LATEST"]) * 1.1
    alternatives_df = scores_df[
        (scores_df["AREA_LABEL"] != selected_area)
        & (scores_df["JEONSE_LATEST"] >= lower_bound)
        & (scores_df["JEONSE_LATEST"] <= upper_bound)
        & (scores_df["TOTAL_SCORE"] > selected_row["TOTAL_SCORE"])
    ].copy()

    st.markdown(
        f"""
        `{selected_area}`의 현재 전세가를 기준으로 `±10%` 범위 안에서
        더 높은 점수를 받은 동네를 추천합니다.
        """
    )

    if alternatives_df.empty:
        st.info("현재 데이터 범위에서는 더 높은 점수의 유사 가격대 대안이 없습니다.")
    else:
        st.markdown("#### 안전한 대안 추천")
        st.dataframe(
            alternatives_df[
                [
                    "AREA_LABEL",
                    "TOTAL_SCORE",
                    "GRADE",
                    "JEONSE_LATEST",
                    "JEONSE_RATE",
                    "JEONSE_DROP_PCT",
                ]
            ].head(10),
            use_container_width=True,
            hide_index=True,
        )

elif selected_view == "동네 비교 📊":
    st.subheader("동네 비교 📊")
    default_compare = [selected_area] + [
        label for label in scores_df["AREA_LABEL"].tolist() if label != selected_area
    ][:2]
    compare_areas = st.multiselect(
        "비교할 동네 2~3개",
        scores_df["AREA_LABEL"].tolist(),
        default=default_compare[:3],
        max_selections=3,
    )

    if len(compare_areas) < 2:
        st.info("비교 탭은 최소 2개 동네를 선택해야 합니다.")
    else:
        compare_scores = scores_df[scores_df["AREA_LABEL"].isin(compare_areas)].copy()

        history_frames = []
        for _, row in compare_scores.iterrows():
            area_history_df = load_area_history(session, row["SGG"], row["EMD"])
            if area_history_df.empty:
                continue
            area_history_df = area_history_df.copy()
            area_history_df["AREA_LABEL"] = row["AREA_LABEL"]
            area_history_df["JEONSE_RATIO"] = (
                area_history_df["JEONSE_PRICE"] / area_history_df["PRICE"] * 100
            )
            history_frames.append(area_history_df)

        if history_frames:
            combined_history_df = pd.concat(history_frames, ignore_index=True)

            trend_left, trend_right = st.columns(2)
            with trend_left:
                jeonse_chart = (
                    alt.Chart(combined_history_df)
                    .mark_line(point=True)
                    .encode(
                        x=alt.X("YYYYMMDD:T", title="월"),
                        y=alt.Y("JEONSE_PRICE:Q", title="전세가"),
                        color=alt.Color("AREA_LABEL:N", title="동네"),
                        tooltip=[
                            "AREA_LABEL",
                            "YYYYMMDD:T",
                            alt.Tooltip("JEONSE_PRICE:Q", format=",.0f"),
                        ],
                    )
                )
                st.markdown("#### 전세가 추이 비교")
                st.altair_chart(jeonse_chart, use_container_width=True)

            with trend_right:
                ratio_chart = (
                    alt.Chart(combined_history_df)
                    .mark_line(point=True)
                    .encode(
                        x=alt.X("YYYYMMDD:T", title="월"),
                        y=alt.Y("JEONSE_RATIO:Q", title="전세가율 (%)"),
                        color=alt.Color("AREA_LABEL:N", title="동네"),
                        tooltip=[
                            "AREA_LABEL",
                            "YYYYMMDD:T",
                            alt.Tooltip("JEONSE_RATIO:Q", format=".1f"),
                        ],
                    )
                )
                st.markdown("#### 전세가율 변화 비교")
                st.altair_chart(ratio_chart, use_container_width=True)

        dimension_compare_df = compare_scores.melt(
            id_vars=["AREA_LABEL"],
            value_vars=list(DIMENSION_LABELS.keys()),
            var_name="차원코드",
            value_name="점수",
        )
        dimension_compare_df["차원"] = dimension_compare_df["차원코드"].map(DIMENSION_LABELS)

        st.markdown("#### 6차원 비교")
        dimension_compare_chart = (
            alt.Chart(dimension_compare_df)
            .mark_line(point=True)
            .encode(
                x=alt.X("차원:N", sort=list(DIMENSION_LABELS.values())),
                y=alt.Y("점수:Q", scale=alt.Scale(domain=[0, 100])),
                color=alt.Color("AREA_LABEL:N", title="동네"),
                tooltip=["AREA_LABEL", "차원", "점수"],
            )
        )
        st.altair_chart(dimension_compare_chart, use_container_width=True)

        st.dataframe(
            compare_scores[
                [
                    "AREA_LABEL",
                    "TOTAL_SCORE",
                    "GRADE",
                    "JEONSE_RATE",
                    "JEONSE_DROP_PCT",
                    "HUG_RATE",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

        st.caption(
            "업종별 소비 비교는 README의 SPH CARD_SALES 연동 범위입니다. "
            "현재 MVP는 점수 및 시세 비교까지 우선 반영했습니다."
        )

else:
    st.subheader("동네 심층 분석 🔍")
    insight_left, insight_right = st.columns([1.2, 0.8])

    with insight_left:
        st.markdown("#### 시세 트렌드")
        if selected_history_df.empty:
            st.info("선택한 동네의 시계열 데이터가 없습니다.")
        else:
            st.altair_chart(make_history_chart(selected_history_df), use_container_width=True)

    with insight_right:
        st.markdown("#### 주민 프로필")
        profile_df = pd.DataFrame(
            [
                ("종합 등급", selected_row["GRADE"]),
                ("총점", int(selected_row["TOTAL_SCORE"])),
                ("전세가율", f'{selected_row["JEONSE_RATE"]:.1f}%'),
                ("과거 하락폭", f'{selected_row["JEONSE_DROP_PCT"]:.1f}%'),
                ("HUG 사고율", f'{selected_row["HUG_RATE"]:.2f}%'),
                ("거주인구 변화", f'{selected_row["POP_CHANGE_PCT"]:.1f}%'),
                ("평균 자산", f'{selected_row["AVG_ASSET"]:,.0f}'),
                ("역 접근성", f'{selected_row["AVG_DISTANCE_M"]:.0f}m'),
            ],
            columns=["항목", "값"],
        )
        st.dataframe(profile_df, use_container_width=True, hide_index=True)

    deep_left, deep_right = st.columns(2)
    with deep_left:
        st.markdown("#### 인구 트렌드")
        population_df = pd.DataFrame(
            [
                ("거주인구 변화율", f'{selected_row["POP_CHANGE_PCT"]:.1f}%'),
                ("현재 구현 범위", "SPH FLOATING_POPULATION 기반"),
                ("확장 예정", "전입/전출/순이동, 근무/방문 인구"),
            ],
            columns=["항목", "내용"],
        )
        st.dataframe(population_df, use_container_width=True, hide_index=True)

        st.markdown("#### 교통")
        transit_df = pd.DataFrame(
            [
                ("평균 역 거리", f'{selected_row["AVG_DISTANCE_M"]:.0f}m'),
                ("현재 구현 범위", "TRAIN_DISTANCE 기반 접근성 점수"),
                ("확장 예정", "역 이름, 승하차 정보, 단지별 접근성"),
            ],
            columns=["항목", "내용"],
        )
        st.dataframe(transit_df, use_container_width=True, hide_index=True)

    with deep_right:
        st.markdown("#### 소비 트렌드")
        st.info("SPH CARD_SALES 연동 시 업종별 소비 히트맵과 필수소비 안정성 분석을 추가할 수 있습니다.")

        st.markdown("#### 연령별 이동 / 이사 수요")
        expansion_df = pd.DataFrame(
            [
                ("연령별 이동", "GENDER_AGE5 연결 시 20~30대 유출입 분석 가능"),
                ("이사 수요", "아정당 V01/V05/V06 적재 후 통신/렌탈 트렌드 표시 가능"),
            ],
            columns=["섹션", "확장 방향"],
        )
        st.dataframe(expansion_df, use_container_width=True, hide_index=True)

st.divider()
st.subheader("등급 분포")
grade_chart = (
    alt.Chart(grade_df)
    .mark_bar(cornerRadiusTopLeft=8, cornerRadiusTopRight=8)
    .encode(
        x=alt.X("GRADE:N", sort=["A", "B", "C", "D"]),
        y=alt.Y("AREA_COUNT:Q", title="동네 수"),
        color=alt.Color("GRADE:N", legend=None, sort=["A", "B", "C", "D"]),
        tooltip=["GRADE", "AREA_COUNT"],
    )
)
st.altair_chart(grade_chart, use_container_width=True)
