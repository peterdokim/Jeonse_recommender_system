import altair as alt
import pandas as pd
import streamlit as st

from common.queries import load_all_area_history, load_grade_summary, load_scores
from common.recommendation import (
    DIMENSION_LABELS,
    GRADE_MEANINGS,
    build_candidate_summary,
    build_recommendation_dataset,
    build_recommendation_reasons,
    format_currency_krw,
    from_eok,
    get_area_history,
    to_eok,
)
from common.session import get_snowpark_session

st.set_page_config(
    page_title="전세 안심 추천",
    page_icon="house",
    layout="wide",
)


def init_state() -> None:
    if "deposit_amount" not in st.session_state:
        st.session_state["deposit_amount"] = 500_000_000
    if "deposit_slider_eok" not in st.session_state:
        st.session_state["deposit_slider_eok"] = to_eok(st.session_state["deposit_amount"])


def sync_deposit_from_slider() -> None:
    st.session_state["deposit_amount"] = from_eok(st.session_state["deposit_slider_eok"])


def sync_slider_from_input() -> None:
    st.session_state["deposit_slider_eok"] = to_eok(st.session_state["deposit_amount"])


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        :root {
            --ink: #163126;
            --soft: #f4f8f5;
            --line: #d8e5dc;
            --accent: #2d7a52;
            --warn: #f57c00;
            --card: #ffffff;
        }
        .block-container {
            padding-top: 1.8rem;
        }
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #f6faf7 0%, #eef5f0 100%);
            border-right: 1px solid var(--line);
        }
        .hero {
            padding: 1.2rem 1.25rem;
            border-radius: 22px;
            background: linear-gradient(135deg, #153527 0%, #2f6f4d 100%);
            color: white;
            box-shadow: 0 16px 36px rgba(21, 53, 39, 0.18);
            margin-bottom: 1rem;
        }
        .hero-kicker {
            font-size: 0.8rem;
            opacity: 0.82;
            letter-spacing: 0.04em;
            text-transform: uppercase;
        }
        .hero-title {
            font-size: 1.45rem;
            font-weight: 700;
            margin-top: 0.2rem;
        }
        .hero-sub {
            margin-top: 0.35rem;
            font-size: 0.95rem;
            line-height: 1.5;
            opacity: 0.94;
        }
        .summary-card {
            background: var(--card);
            border: 1px solid var(--line);
            border-radius: 18px;
            padding: 1rem 1.05rem;
            height: 100%;
        }
        .summary-title {
            color: #5d7166;
            font-size: 0.8rem;
            letter-spacing: 0.02em;
            text-transform: uppercase;
            margin-bottom: 0.45rem;
        }
        .summary-value {
            color: var(--ink);
            font-size: 1.25rem;
            font-weight: 700;
        }
        .summary-body {
            color: #4f6259;
            font-size: 0.92rem;
            line-height: 1.5;
            margin-top: 0.5rem;
        }
        .pill {
            display: inline-block;
            padding: 0.2rem 0.55rem;
            border-radius: 999px;
            background: #eef7f1;
            color: var(--accent);
            font-size: 0.78rem;
            font-weight: 700;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def make_history_chart(history_df: pd.DataFrame) -> alt.Chart:
    long_df = history_df.melt(
        id_vars=["YYYYMMDD"],
        value_vars=["PRICE", "JEONSE_PRICE"],
        var_name="series",
        value_name="price",
    )
    long_df["series"] = long_df["series"].map({"PRICE": "매매가", "JEONSE_PRICE": "전세가"})
    return alt.Chart(long_df).mark_line(point=True).encode(
        x=alt.X("YYYYMMDD:T", title="월"),
        y=alt.Y("price:Q", title="평당 가격"),
        color=alt.Color("series:N", title="시계열"),
        tooltip=["YYYYMMDD:T", "series:N", alt.Tooltip("price:Q", format=",.0f")],
    )


def build_comparison_table(candidate_row: pd.Series, alternative_row: pd.Series) -> pd.DataFrame:
    return pd.DataFrame(
        [
            ("추천 점수", f"{candidate_row['RECOMMENDATION_SCORE']:.1f}", f"{alternative_row['RECOMMENDATION_SCORE']:.1f}", f"{alternative_row['VS_CANDIDATE_DELTA']:+.1f}"),
            ("구조 기반 안전 점수", f"{candidate_row['TOTAL_SCORE']:.1f}", f"{alternative_row['TOTAL_SCORE']:.1f}", f"{alternative_row['TOTAL_SCORE'] - candidate_row['TOTAL_SCORE']:+.1f}"),
            ("백테스트 점수", f"{candidate_row['BACKTEST_SCORE']:.1f}", f"{alternative_row['BACKTEST_SCORE']:.1f}", f"{alternative_row['BACKTEST_SCORE'] - candidate_row['BACKTEST_SCORE']:+.1f}"),
            ("예상 손실 노출", format_currency_krw(candidate_row["LOSS_EXPOSURE_AMOUNT"]), format_currency_krw(alternative_row["LOSS_EXPOSURE_AMOUNT"]), format_currency_krw(candidate_row["LOSS_EXPOSURE_AMOUNT"] - alternative_row["LOSS_EXPOSURE_AMOUNT"])),
            ("전세 추정 총액", format_currency_krw(candidate_row["ESTIMATED_TOTAL_JEONSE"]), format_currency_krw(alternative_row["ESTIMATED_TOTAL_JEONSE"]), format_currency_krw(alternative_row["ESTIMATED_TOTAL_JEONSE"] - candidate_row["ESTIMATED_TOTAL_JEONSE"])),
            ("전세가율", f"{candidate_row['JEONSE_RATE']:.1f}%", f"{alternative_row['JEONSE_RATE']:.1f}%", f"{alternative_row['JEONSE_RATE'] - candidate_row['JEONSE_RATE']:+.1f}%p"),
        ],
        columns=["지표", "현재 후보", "추천 대안", "차이"],
    )


init_state()
inject_styles()

session = get_snowpark_session()
scores_df = load_scores(session)
grade_df = load_grade_summary(session)
all_area_history_df = load_all_area_history(session)

if scores_df.empty:
    st.error("JEONSE_SAFETY_SCORE에서 데이터를 읽지 못했습니다. setup.sql을 먼저 실행해 주세요.")
    st.stop()

scores_df = scores_df.copy()
sgg_options = sorted(scores_df["SGG"].dropna().unique().tolist())

st.markdown(
    """
    <div class="hero">
        <div class="hero-kicker">Jeonse Safety Recommender</div>
        <div class="hero-title">전세 안심 추천</div>
        <div class="hero-sub">
            현재 보고 있는 후보보다, 같은 예산과 생활 조건 안에서 더 안전한 전세 대안을 찾기 위한 개인화 추천 MVP입니다.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.caption(
    "현재 버전은 Snowflake 내부 구조 데이터와 시계열을 사용합니다. "
    "MOLIT 실거래가 API는 가격 기준선과 설명력을 더 강화할 수 있도록 이후 단계에 연결할 수 있게 구조를 분리했습니다."
)

st.sidebar.title("추천 조건")
selected_sgg = st.sidebar.selectbox("현재 관심 구", sgg_options)
emd_options = sorted(scores_df.loc[scores_df["SGG"] == selected_sgg, "EMD"].dropna().unique().tolist())
selected_emd = st.sidebar.selectbox("현재 관심 동", emd_options)
st.sidebar.slider(
    "보증금 슬라이더 (억원)",
    0.0,
    20.0,
    st.session_state["deposit_slider_eok"],
    0.5,
    key="deposit_slider_eok",
    on_change=sync_deposit_from_slider,
)
st.sidebar.number_input(
    "보증금 직접 입력 (원)",
    min_value=0,
    step=10_000_000,
    key="deposit_amount",
    on_change=sync_slider_from_input,
)
workplace_sgg = st.sidebar.selectbox("주요 생활권 / 출근 구", sgg_options, index=min(1, len(sgg_options) - 1))
risk_profile = st.sidebar.selectbox("위험 성향", ["보수형", "균형형", "유연형"], index=1)
preferred_pyeong = st.sidebar.slider("희망 평형 (평)", 10, 40, 24, 1)
search_scope = st.sidebar.selectbox("탐색 범위", ["현재 관심 구 우선", "출퇴근권 우선", "전체 후보"], index=1)
budget_tolerance_pct = st.sidebar.slider("예산 허용 범위 (±%)", 5, 20, 10, 1)

deposit_amount = st.session_state["deposit_amount"]
selected_area = f"{selected_sgg} {selected_emd}"

recommendation_df = build_recommendation_dataset(
    scores_df=scores_df,
    history_df=all_area_history_df,
    deposit_amount=deposit_amount,
    workplace_sgg=workplace_sgg,
    risk_profile=risk_profile,
    preferred_pyeong=preferred_pyeong,
    candidate_area=selected_area,
    search_scope=search_scope,
    budget_tolerance_pct=budget_tolerance_pct,
)

candidate_row = recommendation_df.loc[recommendation_df["AREA_LABEL"] == selected_area].iloc[0]
filtered_df = recommendation_df[recommendation_df["FILTER_MATCH"]].copy()
better_df = recommendation_df[recommendation_df["BETTER_ALTERNATIVE"]].copy()
best_alternative = better_df.iloc[0] if not better_df.empty else None
selected_history_df = get_area_history(all_area_history_df, candidate_row["SGG"], candidate_row["EMD"])

hero_1, hero_2, hero_3, hero_4 = st.columns(4)
hero_1.metric("분석 후보 수", len(recommendation_df))
hero_2.metric("현재 후보 순위", f"{int(candidate_row['RECOMMENDATION_RANK'])}위")
hero_3.metric("조건 충족 대안", len(filtered_df) - 1 if len(filtered_df) > 0 else 0)
hero_4.metric(
    "현재 후보 예상 손실",
    format_currency_krw(candidate_row["LOSS_EXPOSURE_AMOUNT"]),
)

summary_left, summary_right = st.columns(2)
with summary_left:
    st.markdown(
        f"""
        <div class="summary-card">
            <div class="summary-title">현재 후보</div>
            <div class="summary-value">{selected_area}</div>
            <div class="summary-body">
                <span class="pill">등급 {candidate_row['GRADE']} · {GRADE_MEANINGS[candidate_row['GRADE']]}</span><br/><br/>
                추천 점수 {candidate_row['RECOMMENDATION_SCORE']:.1f}점<br/>
                구조 점수 {candidate_row['TOTAL_SCORE']:.1f}점<br/>
                추정 전세 총액 {format_currency_krw(candidate_row['ESTIMATED_TOTAL_JEONSE'])}<br/>
                예상 손실 노출 {format_currency_krw(candidate_row['LOSS_EXPOSURE_AMOUNT'])}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with summary_right:
    if best_alternative is None:
        st.markdown(
            """
            <div class="summary-card">
                <div class="summary-title">추천 결과</div>
                <div class="summary-value">현재 후보가 상위권입니다</div>
                <div class="summary-body">
                    현재 입력한 조건에서는 현재 후보보다 손실 노출이 낮고 추천 점수가 더 높은 대안이 보이지 않습니다.
                    범위를 넓히거나 위험 성향을 바꿔서 다시 탐색해 보세요.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f"""
            <div class="summary-card">
                <div class="summary-title">가장 유력한 대안</div>
                <div class="summary-value">{best_alternative['AREA_LABEL']}</div>
                <div class="summary-body">
                    <span class="pill">추천 점수 +{best_alternative['VS_CANDIDATE_DELTA']:.1f}</span><br/><br/>
                    구조 점수 {best_alternative['TOTAL_SCORE']:.1f}점<br/>
                    예상 손실 노출 {format_currency_krw(best_alternative['LOSS_EXPOSURE_AMOUNT'])}<br/>
                    전세 추정 총액 {format_currency_krw(best_alternative['ESTIMATED_TOTAL_JEONSE'])}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

tabs = st.tabs(["개인화 추천", "후보 진단", "비교 분석", "시장 흐름"])

with tabs[0]:
    st.subheader("현재 후보보다 더 안전한 대안")

    if best_alternative is None:
        st.success("현재 입력 조건에서는 현재 후보가 이미 상위권으로 보입니다.")
    else:
        st.info(
            f"{best_alternative['AREA_LABEL']}은(는) 현재 후보보다 추천 점수가 {best_alternative['VS_CANDIDATE_DELTA']:.1f}점 높고, "
            f"예상 손실 노출은 {format_currency_krw(candidate_row['LOSS_EXPOSURE_AMOUNT'] - best_alternative['LOSS_EXPOSURE_AMOUNT'])} 낮습니다."
        )
        for reason in build_recommendation_reasons(best_alternative, candidate_row):
            st.markdown(f"- {reason}")

    top_chart_df = filtered_df.head(8).copy()
    if not top_chart_df.empty:
        chart = alt.Chart(top_chart_df).mark_bar(cornerRadiusEnd=8).encode(
            x=alt.X("RECOMMENDATION_SCORE:Q", title="추천 점수", scale=alt.Scale(domain=[0, 100])),
            y=alt.Y("AREA_LABEL:N", sort="-x", title="동네"),
            color=alt.condition(alt.datum.IS_CANDIDATE, alt.value("#ef6c00"), alt.value("#2d7a52")),
            tooltip=[
                "AREA_LABEL",
                alt.Tooltip("RECOMMENDATION_SCORE:Q", format=".1f"),
                alt.Tooltip("LOSS_EXPOSURE_AMOUNT:Q", format=",.0f"),
                alt.Tooltip("ESTIMATED_TOTAL_JEONSE:Q", format=",.0f"),
            ],
        )
        st.altair_chart(chart, use_container_width=True)

    recommendation_table = filtered_df[
        [
            "RECOMMENDATION_RANK",
            "AREA_LABEL",
            "RECOMMENDATION_SCORE",
            "TOTAL_SCORE",
            "BACKTEST_SCORE",
            "LOSS_EXPOSURE_AMOUNT",
            "ESTIMATED_TOTAL_JEONSE",
            "COMMUTE_FIT_SCORE",
            "VS_CANDIDATE_DELTA",
        ]
    ].head(10).copy()
    recommendation_table = recommendation_table.rename(
        columns={
            "RECOMMENDATION_RANK": "순위",
            "AREA_LABEL": "동네",
            "RECOMMENDATION_SCORE": "추천 점수",
            "TOTAL_SCORE": "구조 점수",
            "BACKTEST_SCORE": "백테스트",
            "LOSS_EXPOSURE_AMOUNT": "손실 노출",
            "ESTIMATED_TOTAL_JEONSE": "전세 추정 총액",
            "COMMUTE_FIT_SCORE": "출퇴근 적합도",
            "VS_CANDIDATE_DELTA": "후보 대비",
        }
    )
    recommendation_table["손실 노출"] = recommendation_table["손실 노출"].map(format_currency_krw)
    recommendation_table["전세 추정 총액"] = recommendation_table["전세 추정 총액"].map(format_currency_krw)
    st.dataframe(recommendation_table, use_container_width=True, hide_index=True)

    st.caption(
        f"Hard Filter: 보증금 ±{budget_tolerance_pct}% · {search_scope} · 희망 평형 {preferred_pyeong}평 기준 추정"
    )

with tabs[1]:
    st.subheader("현재 후보 진단")
    diag_1, diag_2, diag_3, diag_4 = st.columns(4)
    diag_1.metric("등급", f"{candidate_row['GRADE']} · {GRADE_MEANINGS[candidate_row['GRADE']]}")
    diag_2.metric("구조 기반 안전 점수", f"{candidate_row['TOTAL_SCORE']:.1f}")
    diag_3.metric("전세가율", f"{candidate_row['JEONSE_RATE']:.1f}%")
    diag_4.metric("백테스트 점수", f"{candidate_row['BACKTEST_SCORE']:.1f}")

    dim_df = pd.DataFrame(
        {
            "차원": [DIMENSION_LABELS[key] for key in DIMENSION_LABELS],
            "점수": [candidate_row[key] for key in DIMENSION_LABELS],
        }
    )
    left, right = st.columns([1, 1])
    with left:
        st.altair_chart(
            alt.Chart(dim_df).mark_bar(cornerRadiusEnd=8).encode(
                x=alt.X("점수:Q", scale=alt.Scale(domain=[0, 100])),
                y=alt.Y("차원:N", sort="-x"),
                color=alt.Color("점수:Q", legend=None),
                tooltip=["차원", "점수"],
            ),
            use_container_width=True,
        )
    with right:
        st.markdown("#### 해석 요약")
        st.write(build_candidate_summary(candidate_row))
        st.markdown("#### 개인화 신호")
        st.dataframe(
            pd.DataFrame(
                [
                    ("예상 손실 노출", format_currency_krw(candidate_row["LOSS_EXPOSURE_AMOUNT"])),
                    ("전세 추정 총액", format_currency_krw(candidate_row["ESTIMATED_TOTAL_JEONSE"])),
                    ("가격 맥락 점수", f"{candidate_row['PRICE_CONTEXT_SCORE']:.1f}"),
                    ("예산 적합도", f"{candidate_row['BUDGET_FIT_SCORE']:.1f}"),
                    ("출퇴근 적합도", f"{candidate_row['COMMUTE_FIT_SCORE']:.1f}"),
                    ("생활권 적합도", f"{candidate_row['LIFESTYLE_FIT_SCORE']:.1f}"),
                    ("12개월 회복률", f"{candidate_row['BT_RECOVERY_RATE']:.1f}%"),
                    ("하방 히트율", f"{candidate_row['BT_DOWNSIDE_HIT_RATE']:.1f}%"),
                ],
                columns=["지표", "값"],
            ),
            use_container_width=True,
            hide_index=True,
        )

with tabs[2]:
    st.subheader("현재 후보와 대안 비교")
    compare_candidates = filtered_df["AREA_LABEL"].tolist()
    default_compare = [selected_area]
    if best_alternative is not None:
        default_compare.append(best_alternative["AREA_LABEL"])
    default_compare.extend([area for area in compare_candidates if area not in default_compare][:1])

    compare_areas = st.multiselect(
        "비교할 동네",
        compare_candidates,
        default=default_compare[:3],
        max_selections=4,
    )

    if len(compare_areas) < 2:
        st.info("최소 2개 동네를 선택해 주세요.")
    else:
        compare_rows = recommendation_df[recommendation_df["AREA_LABEL"].isin(compare_areas)].copy()
        history_frames = []
        for _, row in compare_rows.iterrows():
            history = get_area_history(all_area_history_df, row["SGG"], row["EMD"])
            if history.empty:
                continue
            history["AREA_LABEL"] = row["AREA_LABEL"]
            history["JEONSE_RATIO"] = (history["JEONSE_PRICE"] / history["PRICE"] * 100).round(1)
            history_frames.append(history)

        if history_frames:
            combined_history = pd.concat(history_frames, ignore_index=True)
            chart_left, chart_right = st.columns(2)
            chart_left.altair_chart(
                alt.Chart(combined_history).mark_line(point=True).encode(
                    x=alt.X("YYYYMMDD:T", title="월"),
                    y=alt.Y("JEONSE_PRICE:Q", title="전세가"),
                    color=alt.Color("AREA_LABEL:N", title="동네"),
                    tooltip=["AREA_LABEL", "YYYYMMDD:T", alt.Tooltip("JEONSE_PRICE:Q", format=",.0f")],
                ),
                use_container_width=True,
            )
            chart_right.altair_chart(
                alt.Chart(combined_history).mark_line(point=True).encode(
                    x=alt.X("YYYYMMDD:T", title="월"),
                    y=alt.Y("JEONSE_RATIO:Q", title="전세가율 (%)"),
                    color=alt.Color("AREA_LABEL:N", title="동네"),
                    tooltip=["AREA_LABEL", "YYYYMMDD:T", alt.Tooltip("JEONSE_RATIO:Q", format=".1f")],
                ),
                use_container_width=True,
            )

        if best_alternative is not None:
            st.markdown("#### 현재 후보 vs 대표 대안")
            st.dataframe(
                build_comparison_table(candidate_row, best_alternative),
                use_container_width=True,
                hide_index=True,
            )

        compare_table = compare_rows[
            [
                "AREA_LABEL",
                "RECOMMENDATION_SCORE",
                "TOTAL_SCORE",
                "BACKTEST_SCORE",
                "LOSS_EXPOSURE_AMOUNT",
                "ESTIMATED_TOTAL_JEONSE",
                "JEONSE_RATE",
                "COMMUTE_FIT_SCORE",
            ]
        ].rename(
            columns={
                "AREA_LABEL": "동네",
                "RECOMMENDATION_SCORE": "추천 점수",
                "TOTAL_SCORE": "구조 점수",
                "BACKTEST_SCORE": "백테스트",
                "LOSS_EXPOSURE_AMOUNT": "손실 노출",
                "ESTIMATED_TOTAL_JEONSE": "전세 추정 총액",
                "JEONSE_RATE": "전세가율",
                "COMMUTE_FIT_SCORE": "출퇴근 적합도",
            }
        )
        compare_table["손실 노출"] = compare_table["손실 노출"].map(format_currency_krw)
        compare_table["전세 추정 총액"] = compare_table["전세 추정 총액"].map(format_currency_krw)
        compare_table["전세가율"] = compare_table["전세가율"].map(lambda value: f"{value:.1f}%")
        st.dataframe(compare_table, use_container_width=True, hide_index=True)

with tabs[3]:
    st.subheader("시장 흐름과 구현 구조")
    market_left, market_right = st.columns([1.2, 0.8])

    with market_left:
        if selected_history_df.empty:
            st.info("선택한 후보의 시계열 데이터가 없습니다.")
        else:
            st.altair_chart(make_history_chart(selected_history_df), use_container_width=True)

    with market_right:
        st.dataframe(
            pd.DataFrame(
                [
                    ("현재 후보", selected_area),
                    ("위험 성향", risk_profile),
                    ("탐색 범위", search_scope),
                    ("예산 허용 범위", f"±{budget_tolerance_pct}%"),
                    ("주요 생활권", workplace_sgg),
                    ("희망 평형", f"{preferred_pyeong}평"),
                ],
                columns=["조건", "값"],
            ),
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("#### 추천 파이프라인")
    pipeline_df = pd.DataFrame(
        [
            ("Hard Filter", "보증금 밴드, 탐색 범위", "현재 후보와 비교 가능한 대안군 축소"),
            ("Safety Ranking", "구조 점수, 백테스트, 손실 노출, 가격 맥락", "보증금 회수 위험이 더 낮은 후보 우선"),
            ("Preference Re-ranking", "출퇴근, 생활권, 후보 유사성", "안전성은 유지하면서 실제 거주 가능성 반영"),
            ("MOLIT 연동 준비", "실거래가 API 테이블 추가 예정", "매매/전월세 기준선과 과열 판정 정교화"),
        ],
        columns=["단계", "현재 반영", "역할"],
    )
    st.dataframe(pipeline_df, use_container_width=True, hide_index=True)

    st.markdown("#### 등급 분포")
    st.altair_chart(
        alt.Chart(grade_df).mark_bar(cornerRadiusTopLeft=8, cornerRadiusTopRight=8).encode(
            x=alt.X("GRADE:N", sort=["A", "B", "C", "D"]),
            y=alt.Y("AREA_COUNT:Q", title="동네 수"),
            color=alt.Color("GRADE:N", legend=None, sort=["A", "B", "C", "D"]),
            tooltip=["GRADE", "AREA_COUNT"],
        ),
        use_container_width=True,
    )
