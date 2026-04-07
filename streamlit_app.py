import altair as alt
import pandas as pd
import streamlit as st

from common.queries import load_all_area_history, load_grade_summary, load_scores
from common.recommendation import (
    DIMENSION_LABELS,
    GRADE_MEANINGS,
    SEARCH_SCOPE_OPTIONS,
    PROFILE_DESCRIPTIONS,
    PROFILE_ICONS,
    SURVEY_QUESTIONS,
    build_candidate_summary,
    build_card_description,
    build_exclusion_reasons,
    build_recommendation_dataset,
    pick_typed_alternatives,
    classify_survey_profile,
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

# ── Helpers ──────────────────────────────────────────────────────────────────


def _icon(name: str, size: int = 20, color: str = "currentColor") -> str:
    """Return an inline Material Symbols Rounded icon span."""
    return (
        f'<span class="material-symbols-rounded" '
        f'style="font-size:{size}px;vertical-align:middle;color:{color}">'
        f"{name}</span>"
    )


def _profile_card_html(profile: str) -> str:
    icon_name = PROFILE_ICONS.get(profile, "person")
    desc = PROFILE_DESCRIPTIONS.get(profile, "")
    return (
        f'<div class="profile-card">'
        f'<span class="material-symbols-rounded profile-card-icon">{icon_name}</span>'
        f"<div>"
        f'<div class="profile-card-name">{profile}</div>'
        f'<div class="profile-card-desc">{desc}</div>'
        f"</div></div>"
    )


def init_state() -> None:
    if "deposit_amount" not in st.session_state:
        st.session_state["deposit_amount"] = 500_000_000
    if "deposit_slider_eok" not in st.session_state:
        st.session_state["deposit_slider_eok"] = to_eok(st.session_state["deposit_amount"])
    if "survey_completed" not in st.session_state:
        st.session_state["survey_completed"] = False
    if "conditions_confirmed" not in st.session_state:
        st.session_state["conditions_confirmed"] = False
    for question in SURVEY_QUESTIONS:
        if question["key"] not in st.session_state:
            st.session_state[question["key"]] = 3
        landing_key = f"landing_{question['key']}"
        if landing_key not in st.session_state:
            st.session_state[landing_key] = 3  # center of 5-point scale


_MAX_DEPOSIT_EOK = 30.0  # 최대 30억


def sync_deposit_from_slider() -> None:
    st.session_state["deposit_amount"] = from_eok(st.session_state["deposit_slider_eok"])


def sync_slider_from_input() -> None:
    # 직접 입력이 슬라이더 범위를 넘지 않도록 클램핑
    eok = to_eok(st.session_state["deposit_amount"])
    st.session_state["deposit_slider_eok"] = min(eok, _MAX_DEPOSIT_EOK)
    st.session_state["deposit_amount"] = from_eok(st.session_state["deposit_slider_eok"])


def get_survey_answers() -> dict[str, int]:
    return {question["key"]: int(st.session_state[question["key"]]) for question in SURVEY_QUESTIONS}


# ── Survey ───────────────────────────────────────────────────────────────────




def _on_circle_click(landing_key: str, val: int) -> None:
    st.session_state[landing_key] = val


def render_initial_survey() -> None:
    # Streamlit 1.52: 버튼은 styled-component로 렌더링됨.
    # data-testid="stBaseButton-secondary" 가 버튼 엘리먼트 자체에 붙음 (자식이 아님).
    st.markdown(
        """<style>
        [data-testid="stBaseButton-secondary"] {
            background: transparent !important;
            background-color: transparent !important;
            border: none !important;
            border-color: transparent !important;
            box-shadow: none !important;
            outline: none !important;
            padding: 0 !important;
            min-height: 0 !important;
            cursor: pointer !important;
            transition: transform 0.12s ease !important;
        }
        [data-testid="stBaseButton-secondary"]:hover {
            background: transparent !important;
            background-color: transparent !important;
            border: none !important;
            border-color: transparent !important;
            box-shadow: none !important;
            transform: scale(1.15) !important;
        }
        [data-testid="stBaseButton-secondary"]:focus,
        [data-testid="stBaseButton-secondary"]:focus-visible,
        [data-testid="stBaseButton-secondary"]:active:focus {
            outline: none !important;
            box-shadow: none !important;
            border: none !important;
            border-color: transparent !important;
        }
        [data-testid="stBaseButton-secondary"]:active {
            transform: scale(0.92) !important;
        }
        /* 동그라미 텍스트 크기 */
        [data-testid="stBaseButton-secondary"] p,
        [data-testid="stBaseButton-secondary"] span {
            font-size: 3.2rem !important;
            line-height: 1 !important;
            margin: 0 !important;
            padding: 0 !important;
        }
        </style>""",
        unsafe_allow_html=True,
    )

    st.markdown(
        f'<div style="text-align:center;padding:1.5rem 0 0.3rem">'
        f'{_icon("quiz", 36, "#2d7a52")}'
        f'<h3 style="margin:0.4rem 0 0.15rem;color:#1a1a1a">나의 투자 성향 알아보기</h3>'
        f'<p style="color:#888;font-size:0.9rem">간단한 질문 6개에 답해주시면, 회원님의 성향에 맞춰 추천해드릴게요.</p>'
        f"</div>",
        unsafe_allow_html=True,
    )

    for idx, question in enumerate(SURVEY_QUESTIONS, 1):
        landing_key = f"landing_{question['key']}"
        current = st.session_state.get(landing_key, 3)

        st.markdown(
            f'<p style="font-size:0.95rem;color:#444;margin:0.6rem 0 0.2rem;line-height:1.6">'
            f"{question['label']}</p>",
            unsafe_allow_html=True,
        )

        cols = st.columns([0.8, 1, 1, 1, 1, 1, 0.8])

        with cols[0]:
            st.markdown(
                '<p style="font-size:0.72rem;color:#33a474;font-weight:600;'
                'padding-top:14px;white-space:nowrap">동의</p>',
                unsafe_allow_html=True,
            )

        for i in range(5):
            val = i + 1
            is_sel = current == val
            # 선택: 채워진 원(●) + 진한 색, 미선택: 빈 원(○) + 연한 색
            if is_sel:
                label = f":gray[●]" if i == 2 else f":green[●]" if i < 2 else f":violet[●]"
            else:
                label = f":gray[○]"

            with cols[i + 1]:
                st.button(
                    label,
                    key=f"_cb_{question['key']}_{val}",
                    use_container_width=True,
                    on_click=_on_circle_click,
                    args=(landing_key, val),
                )

        with cols[6]:
            st.markdown(
                '<p style="font-size:0.72rem;color:#7b4e9e;font-weight:600;'
                'padding-top:14px;text-align:right;white-space:nowrap">비동의</p>',
                unsafe_allow_html=True,
            )

        if idx < len(SURVEY_QUESTIONS):
            st.divider()

    st.markdown("")
    if st.button("다음 단계로", type="primary", use_container_width=True):
        for question in SURVEY_QUESTIONS:
            raw = int(st.session_state.get(f"landing_{question['key']}", 3))
            st.session_state[question["key"]] = 6 - raw
        st.session_state["survey_completed"] = True
        st.rerun()


def reset_survey() -> None:
    st.session_state["survey_completed"] = False
    st.session_state["conditions_confirmed"] = False
    for question in SURVEY_QUESTIONS:
        st.session_state[f"landing_{question['key']}"] = 3  # center of 5-point scale


# ── Global styles ────────────────────────────────────────────────────────────


def inject_styles() -> None:
    # Load Material Symbols font separately
    st.markdown(
        '<link href="https://fonts.googleapis.com/css2?family=Material+Symbols+Rounded:opsz,wght,FILL@20..48,100..700,0..1" rel="stylesheet">',
        unsafe_allow_html=True,
    )
    st.markdown(
        """<style>
        /* ── Base ── */
        .block-container { padding-top: 1.5rem; }

        [data-testid="stSidebar"] {
            background: #fafafa;
            border-right: 1px solid #eee;
        }

        /* ── Section divider ── */
        .section-gap { margin: 1.5rem 0 1rem; border-top: 1px solid #eee; padding-top: 1rem; }

        /* ── Profile card ── */
        .profile-card {
            display: flex;
            align-items: center;
            gap: 0.8rem;
            background: #f7f9f8;
            border: 1px solid #e4e8e5;
            border-radius: 12px;
            padding: 1rem 1.2rem;
            margin-bottom: 1rem;
        }
        .profile-card-icon {
            font-size: 26px;
            color: #2d7a52;
            background: #e8f2ec;
            border-radius: 10px;
            padding: 8px;
            line-height: 1;
        }
        .profile-card-name {
            font-size: 1rem;
            font-weight: 700;
            color: #1a1a1a;
        }
        .profile-card-desc {
            font-size: 0.84rem;
            color: #777;
            margin-top: 2px;
        }

        /* ── Summary cards ── */
        .s-card {
            background: #fff;
            border: 1px solid #e4e8e5;
            border-radius: 12px;
            padding: 1.1rem 1.2rem;
            height: 100%;
        }
        .s-card-label {
            font-size: 0.72rem;
            color: #999;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            margin-bottom: 0.4rem;
        }
        .s-card-value {
            font-size: 1.15rem;
            font-weight: 700;
            color: #1a1a1a;
        }
        .s-card-body {
            font-size: 0.88rem;
            color: #555;
            line-height: 1.55;
            margin-top: 0.4rem;
        }
        .s-card-pill {
            display: inline-block;
            padding: 2px 10px;
            border-radius: 999px;
            background: #eef6f1;
            color: #2d7a52;
            font-size: 0.78rem;
            font-weight: 600;
        }

        /* ── Metric row ── */
        [data-testid="stMetric"] {
            background: #f7f9f8;
            border: 1px solid #e4e8e5;
            border-radius: 10px;
            padding: 0.8rem 1rem;
        }
        [data-testid="stMetric"] label {
            font-size: 0.72rem !important;
            color: #999 !important;
            text-transform: uppercase;
            letter-spacing: 0.03em;
        }
        [data-testid="stMetric"] [data-testid="stMetricValue"] {
            font-size: 1.1rem !important;
            font-weight: 700 !important;
            color: #1a1a1a !important;
        }

        /* ── Tabs ── */
        .stTabs [data-baseweb="tab-list"] {
            gap: 0;
            border-bottom: 2px solid #eee;
        }
        .stTabs [data-baseweb="tab"] {
            padding: 0.6rem 1.2rem;
            font-weight: 500;
            font-size: 0.9rem;
        }

        </style>
        """,
        unsafe_allow_html=True,
    )


# ── Charts & tables ──────────────────────────────────────────────────────────


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



# ── Main ─────────────────────────────────────────────────────────────────────

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

# ── Header ──
st.markdown(
    f'<div style="margin-bottom:1rem">'
    f'<span style="font-size:0.8rem;color:#999;letter-spacing:0.04em;text-transform:uppercase">Jeonse Safety Recommender</span>'
    f'<h2 style="margin:0.1rem 0 0.3rem;color:#1a1a1a">전세 안심 추천</h2>'
    f'<p style="color:#777;font-size:0.9rem;margin:0">'
    f"설문으로 위험 성향을 분류하고, 회원님에게 맞는 안전한 전세를 추천합니다.</p>"
    f"</div>",
    unsafe_allow_html=True,
)

# ── Step 1: 설문 ──
if not st.session_state["survey_completed"]:
    render_initial_survey()
    st.stop()

# ── Step 2: 조건 입력 ──
if not st.session_state.get("conditions_confirmed"):
    survey_answers = get_survey_answers()
    survey_result = classify_survey_profile(survey_answers)

    st.markdown(_profile_card_html(survey_result["profile"]), unsafe_allow_html=True)

    st.markdown(
        f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:0.5rem">'
        f'{_icon("tune", 22, "#1a1a1a")}'
        f'<span style="font-size:1.1rem;font-weight:700;color:#1a1a1a">추천 조건 입력</span>'
        f"</div>"
        f'<p style="color:#888;font-size:0.88rem;margin:0 0 1rem">관심 지역과 보증금을 입력하면 맞춤 추천을 시작합니다.</p>',
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)
    with col1:
        selected_sgg = st.selectbox("현재 관심 구", sgg_options, key="setup_sgg")
    with col2:
        emd_options = sorted(scores_df.loc[scores_df["SGG"] == selected_sgg, "EMD"].dropna().unique().tolist())
        selected_emd = st.selectbox("현재 관심 동", emd_options, key="setup_emd")

    col3, col4 = st.columns(2)
    with col3:
        st.number_input(
            "보증금 (원)",
            min_value=0,
            max_value=from_eok(_MAX_DEPOSIT_EOK),
            step=10_000_000,
            value=200_000_000,
            key="setup_deposit",
        )
    with col4:
        workplace_sgg = st.selectbox("주요 생활권 / 출근 구", sgg_options, index=min(1, len(sgg_options) - 1), key="setup_workplace")

    col5, col6, col7 = st.columns(3)
    with col5:
        preferred_pyeong = st.slider("희망 평형 (평)", 10, 40, 24, 1, key="setup_pyeong")
    with col6:
        search_scope = st.selectbox("탐색 범위", SEARCH_SCOPE_OPTIONS, index=1, key="setup_scope")
    with col7:
        budget_tolerance_pct = st.slider("예산 허용 범위 (±%)", 5, 20, 10, 1, key="setup_budget")

    st.markdown("")
    if st.button("추천 결과 보기", type="primary", use_container_width=True):
        st.session_state["deposit_amount"] = st.session_state["setup_deposit"]
        st.session_state["deposit_slider_eok"] = to_eok(st.session_state["setup_deposit"])
        st.session_state["confirmed_sgg"] = st.session_state["setup_sgg"]
        st.session_state["confirmed_emd"] = st.session_state["setup_emd"]
        st.session_state["confirmed_workplace"] = st.session_state["setup_workplace"]
        st.session_state["confirmed_pyeong"] = st.session_state["setup_pyeong"]
        st.session_state["confirmed_scope"] = st.session_state["setup_scope"]
        st.session_state["confirmed_budget"] = st.session_state["setup_budget"]
        st.session_state["conditions_confirmed"] = True
        st.rerun()

    st.markdown("")
    if st.button("설문 다시 하기"):
        reset_survey()
        st.rerun()
    st.stop()

# ── Step 3: 결과 ──
st.sidebar.title("추천 조건")
selected_sgg = st.sidebar.selectbox("현재 관심 구", sgg_options, index=sgg_options.index(st.session_state.get("confirmed_sgg", sgg_options[0])))
emd_options = sorted(scores_df.loc[scores_df["SGG"] == selected_sgg, "EMD"].dropna().unique().tolist())
_default_emd = st.session_state.get("confirmed_emd", emd_options[0])
selected_emd = st.sidebar.selectbox("현재 관심 동", emd_options, index=emd_options.index(_default_emd) if _default_emd in emd_options else 0)
st.sidebar.slider(
    "보증금 (억원)",
    min_value=0.0,
    max_value=_MAX_DEPOSIT_EOK,
    step=0.5,
    key="deposit_slider_eok",
    on_change=sync_deposit_from_slider,
)
st.sidebar.number_input(
    "보증금 직접 입력 (원)",
    min_value=0,
    max_value=from_eok(_MAX_DEPOSIT_EOK),
    step=10_000_000,
    key="deposit_amount",
    on_change=sync_slider_from_input,
)
workplace_sgg = st.sidebar.selectbox("주요 생활권 / 출근 구", sgg_options, index=sgg_options.index(st.session_state.get("confirmed_workplace", sgg_options[min(1, len(sgg_options) - 1)])))
preferred_pyeong = st.sidebar.slider("희망 평형 (평)", 10, 40, st.session_state.get("confirmed_pyeong", 24), 1)
search_scope = st.sidebar.selectbox("탐색 범위", SEARCH_SCOPE_OPTIONS, index=SEARCH_SCOPE_OPTIONS.index(st.session_state.get("confirmed_scope", SEARCH_SCOPE_OPTIONS[1])))
budget_tolerance_pct = st.sidebar.slider("예산 허용 범위 (±%)", 5, 20, st.session_state.get("confirmed_budget", 10), 1)

survey_answers = get_survey_answers()
survey_result = classify_survey_profile(survey_answers)

st.sidebar.divider()
st.sidebar.markdown(
    f'{_icon(PROFILE_ICONS.get(survey_result["profile"], "person"), 18, "#2d7a52")} '
    f'**{survey_result["profile"]}**',
    unsafe_allow_html=True,
)
st.sidebar.caption(survey_result["description"])
if st.sidebar.button("성향 다시 측정", use_container_width=True):
    reset_survey()
    st.rerun()

deposit_amount = st.session_state["deposit_amount"]
selected_area = f"{selected_sgg} {selected_emd}"

recommendation_df = build_recommendation_dataset(
    scores_df=scores_df,
    history_df=all_area_history_df,
    deposit_amount=deposit_amount,
    workplace_sgg=workplace_sgg,
    survey_result=survey_result,
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

# ── Profile + metrics ──
st.markdown(_profile_card_html(survey_result["profile"]), unsafe_allow_html=True)

hero_1, hero_2, hero_3, hero_4 = st.columns(4)
hero_1.metric("분류 성향", survey_result["profile"])
hero_2.metric("현재 후보 순위", f"{int(candidate_row['RECOMMENDATION_RANK'])}위")
hero_3.metric("조건 충족 대안", len(filtered_df) - 1 if len(filtered_df) > 0 else 0)
hero_4.metric("현재 후보 예상 손실", format_currency_krw(candidate_row["LOSS_EXPOSURE_AMOUNT"]))

st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)

summary_left, summary_right = st.columns(2)
with summary_left:
    st.markdown(
        f"""
        <div class="s-card">
            <div class="s-card-label">현재 후보</div>
            <div class="s-card-value">{selected_area}</div>
            <div class="s-card-body">
                <span class="s-card-pill">등급 {candidate_row['GRADE']} · {GRADE_MEANINGS[candidate_row['GRADE']]}</span><br><br>
                공통 안전점수 {candidate_row['SAFETY_SCORE']:.1f}점<br>
                최종 추천점수 {candidate_row['RECOMMENDATION_SCORE']:.1f}점<br>
                추정 전세 총액 {format_currency_krw(candidate_row['ESTIMATED_TOTAL_JEONSE'])}<br>
                예상 손실 노출 {format_currency_krw(candidate_row['LOSS_EXPOSURE_AMOUNT'])}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with summary_right:
    if best_alternative is None:
        st.markdown(
            f"""
            <div class="s-card">
                <div class="s-card-label">성향 반영 결과</div>
                <div class="s-card-value">{survey_result['profile']} 기준 현재 후보 유지</div>
                <div class="s-card-body">
                    현재 입력 조건에서는 현재 후보보다 손실 노출이 낮고 최종 추천점수가 더 높은 대안이 보이지 않습니다.<br>
                    {survey_result['description']}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f"""
            <div class="s-card">
                <div class="s-card-label">가장 유력한 대안</div>
                <div class="s-card-value">{best_alternative['AREA_LABEL']}</div>
                <div class="s-card-body">
                    <span class="s-card-pill">최종 추천점수 +{best_alternative['VS_CANDIDATE_DELTA']:.1f}</span><br><br>
                    공통 안전점수 {best_alternative['SAFETY_SCORE']:.1f}점<br>
                    예상 손실 노출 {format_currency_krw(best_alternative['LOSS_EXPOSURE_AMOUNT'])}<br>
                    선호 적합도 {best_alternative['PREFERENCE_FIT_SCORE']:.1f}점
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)

tabs = st.tabs(["개인화 추천", "후보 진단", "비교 분석", "시장 흐름"])

with tabs[0]:
    st.subheader("설문 성향을 반영한 추천 결과")

    if best_alternative is None:
        st.success("현재 입력 조건에서는 현재 후보가 이미 상위권으로 보입니다.")
    else:
        # 15번: 3가지 유형 대안 선별
        typed = pick_typed_alternatives(better_df, candidate_row)
        type_labels = {
            "safest": ("가장 안전한 대안", "손실 노출을 최소화한 후보"),
            "balanced": ("가장 균형 잡힌 대안", "안전성과 생활권을 함께 고려한 후보"),
            "similar": ("가장 비슷하지만 더 안전한 대안", "현재 후보와 유사하면서 더 안전한 후보"),
        }

        card_cols = st.columns(3)
        for col, key in zip(card_cols, ["safest", "balanced", "similar"]):
            alt_row = typed[key]
            title, subtitle = type_labels[key]
            with col:
                if alt_row is None:
                    st.markdown(
                        f'<div class="s-card">'
                        f'<div class="s-card-label">{title}</div>'
                        f'<div class="s-card-value">해당 없음</div>'
                        f'<div class="s-card-body">{subtitle}</div>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
                else:
                    # 14번: 추천 카드 — 개선 포인트 리스트
                    points = build_card_description(alt_row, candidate_row)
                    points_html = "".join(f"<li>{p}</li>" for p in points)
                    st.markdown(
                        f'<div class="s-card">'
                        f'<div class="s-card-label">{title}</div>'
                        f'<div class="s-card-value">{alt_row["AREA_LABEL"]}</div>'
                        f'<div class="s-card-body" style="margin-bottom:0.4rem">{subtitle}</div>'
                        f'<ul class="s-card-body" style="padding-left:1.2rem;margin:0">'
                        f'{points_html}</ul>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )

        st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)

    top_chart_df = filtered_df.head(8).copy()
    if not top_chart_df.empty:
        chart = alt.Chart(top_chart_df).mark_bar(cornerRadiusEnd=8).encode(
            x=alt.X("RECOMMENDATION_SCORE:Q", title="최종 추천점수", scale=alt.Scale(domain=[0, 100])),
            y=alt.Y("AREA_LABEL:N", sort="-x", title="동네"),
            color=alt.condition(alt.datum.IS_CANDIDATE, alt.value("#ef6c00"), alt.value("#2d7a52")),
            tooltip=[
                "AREA_LABEL",
                alt.Tooltip("RECOMMENDATION_SCORE:Q", format=".1f"),
                alt.Tooltip("SAFETY_SCORE:Q", format=".1f"),
                alt.Tooltip("LOSS_EXPOSURE_AMOUNT:Q", format=",.0f"),
            ],
        )
        st.altair_chart(chart, use_container_width=True)

    recommendation_table = filtered_df[
        [
            "RECOMMENDATION_RANK",
            "AREA_LABEL",
            "GRADE",
            "RECOMMENDATION_SCORE",
            "JEONSE_RATE",
            "LOSS_EXPOSURE_AMOUNT",
            "VS_CANDIDATE_DELTA",
        ]
    ].head(10).copy()
    recommendation_table = recommendation_table.rename(
        columns={
            "RECOMMENDATION_RANK": "순위",
            "AREA_LABEL": "동네",
            "GRADE": "안전등급",
            "RECOMMENDATION_SCORE": "추천점수",
            "JEONSE_RATE": "전세가율",
            "LOSS_EXPOSURE_AMOUNT": "예상 손실 노출",
            "VS_CANDIDATE_DELTA": "현재 후보 대비",
        }
    )
    recommendation_table["예상 손실 노출"] = recommendation_table["예상 손실 노출"].map(format_currency_krw)
    recommendation_table["전세가율"] = recommendation_table["전세가율"].map(lambda v: f"{v:.1f}%")
    st.dataframe(recommendation_table, use_container_width=True, hide_index=True)

    # 제외된 주요 후보와 제외 사유
    excluded_df = recommendation_df[
        (~recommendation_df["FILTER_MATCH"]) | (~recommendation_df["BETTER_ALTERNATIVE"] & ~recommendation_df["IS_CANDIDATE"])
    ].head(5)
    if not excluded_df.empty:
        st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)
        st.markdown("#### 제외된 주요 후보")
        for _, ex_row in excluded_df.iterrows():
            reasons = build_exclusion_reasons(ex_row, candidate_row)
            if reasons:
                reasons_html = "".join(f"<li>{r}</li>" for r in reasons)
                st.markdown(
                    f'<div class="s-card" style="margin-bottom:0.6rem">'
                    f'<div class="s-card-label">제외</div>'
                    f'<div class="s-card-value">{ex_row["AREA_LABEL"]}</div>'
                    f'<ul class="s-card-body" style="padding-left:1.2rem;margin:0.3rem 0 0">'
                    f"{reasons_html}</ul></div>",
                    unsafe_allow_html=True,
                )

with tabs[1]:
    st.subheader("현재 후보 진단")

    grade_label = f"{candidate_row['GRADE']} · {GRADE_MEANINGS[candidate_row['GRADE']]}"
    diag_1, diag_2, diag_3 = st.columns(3)
    diag_1.metric("안전 등급", grade_label)
    diag_2.metric("전세가율", f"{candidate_row['JEONSE_RATE']:.1f}%")
    diag_3.metric("예상 손실 노출", format_currency_krw(candidate_row["LOSS_EXPOSURE_AMOUNT"]))

    dim_df = pd.DataFrame(
        {
            "항목": [DIMENSION_LABELS[key] for key in DIMENSION_LABELS],
            "점수": [candidate_row[key] for key in DIMENSION_LABELS],
        }
    )
    left, right = st.columns([1, 1])
    with left:
        st.altair_chart(
            alt.Chart(dim_df).mark_bar(cornerRadiusEnd=8).encode(
                x=alt.X("점수:Q", scale=alt.Scale(domain=[0, 100])),
                y=alt.Y("항목:N", sort="-x"),
                color=alt.Color("점수:Q", legend=None),
                tooltip=["항목", "점수"],
            ),
            use_container_width=True,
        )
    with right:
        st.markdown("#### 이 동네는 어떤가요?")
        # Cortex AI 자동 해설
        cortex_prompt = (
            f"당신은 전세 안전 전문가입니다. 아래 데이터를 보고, "
            f"이 동네의 전세 안전성을 일반인이 이해할 수 있게 3~4문장으로 해설해주세요. "
            f"전문 용어는 쓰지 마세요.\n\n"
            f"동네: {selected_area}\n"
            f"안전등급: {grade_label}\n"
            f"전세가율: {candidate_row['JEONSE_RATE']:.1f}%\n"
            f"추정 전세 총액: {format_currency_krw(candidate_row['ESTIMATED_TOTAL_JEONSE'])}\n"
            f"예상 손실 노출: {format_currency_krw(candidate_row['LOSS_EXPOSURE_AMOUNT'])}\n"
            f"전세가율 점수(100점 만점): {candidate_row['S_RATE']:.1f}\n"
            f"전입전출 점수(100점 만점): {candidate_row['S_MIG']:.1f}\n"
            f"지하철 접근성 점수(100점 만점): {candidate_row['S_SUB']:.1f}\n"
        )
        try:
            cortex_result = session.sql(
                "SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', ?)",
                params=[cortex_prompt],
            ).collect()
            ai_summary = cortex_result[0][0] if cortex_result else ""
            st.write(ai_summary)
        except Exception:
            # Cortex 사용 불가 시 기본 해설
            st.write(build_candidate_summary(candidate_row, survey_result))

        st.markdown("#### 상세 정보")
        st.dataframe(
            pd.DataFrame(
                [
                    ("추정 전세 총액", format_currency_krw(candidate_row["ESTIMATED_TOTAL_JEONSE"])),
                    ("예상 손실 노출", format_currency_krw(candidate_row["LOSS_EXPOSURE_AMOUNT"])),
                    ("전세가율", f"{candidate_row['JEONSE_RATE']:.1f}%"),
                    ("안전 등급", grade_label),
                ],
                columns=["항목", "값"],
            ),
            use_container_width=True,
            hide_index=True,
        )

with tabs[2]:
    st.subheader("동네 비교")
    # 전체 동네 목록에서 검색 가능
    all_areas = sorted(recommendation_df["AREA_LABEL"].tolist())
    default_compare = [selected_area]
    if best_alternative is not None:
        default_compare.append(best_alternative["AREA_LABEL"])
    default_compare.extend([a for a in all_areas if a not in default_compare][:1])

    compare_areas = st.multiselect(
        "비교할 동네를 검색하세요",
        all_areas,
        default=[a for a in default_compare[:3] if a in all_areas],
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

        compare_table = compare_rows[
            [
                "AREA_LABEL",
                "GRADE",
                "RECOMMENDATION_SCORE",
                "JEONSE_RATE",
                "LOSS_EXPOSURE_AMOUNT",
                "ESTIMATED_TOTAL_JEONSE",
            ]
        ].copy()
        compare_table = compare_table.rename(
            columns={
                "AREA_LABEL": "동네",
                "GRADE": "안전등급",
                "RECOMMENDATION_SCORE": "추천점수",
                "JEONSE_RATE": "전세가율",
                "LOSS_EXPOSURE_AMOUNT": "예상 손실 노출",
                "ESTIMATED_TOTAL_JEONSE": "추정 전세 총액",
            }
        )
        compare_table["예상 손실 노출"] = compare_table["예상 손실 노출"].map(format_currency_krw)
        compare_table["추정 전세 총액"] = compare_table["추정 전세 총액"].map(format_currency_krw)
        compare_table["전세가율"] = compare_table["전세가율"].map(lambda v: f"{v:.1f}%")
        st.dataframe(compare_table, use_container_width=True, hide_index=True)

with tabs[3]:
    st.subheader("시장 흐름")

    if selected_history_df.empty:
        st.info("선택한 후보의 시세 데이터가 없습니다.")
    else:
        st.markdown(f"**{selected_area}** 매매가·전세가 추이")
        st.altair_chart(make_history_chart(selected_history_df), use_container_width=True)

    st.markdown("#### 서울 전체 안전등급 분포")
    grade_chart_df = grade_df.copy()
    grade_chart_df["등급설명"] = grade_chart_df["GRADE"].map(
        {"A": "A (안전)", "B": "B (보통)", "C": "C (주의)", "D": "D (위험)"}
    )
    st.altair_chart(
        alt.Chart(grade_chart_df).mark_bar(cornerRadiusTopLeft=8, cornerRadiusTopRight=8).encode(
            x=alt.X("등급설명:N", sort=["A (안전)", "B (보통)", "C (주의)", "D (위험)"], title="안전등급"),
            y=alt.Y("AREA_COUNT:Q", title="동네 수"),
            color=alt.Color("GRADE:N", legend=None, sort=["A", "B", "C", "D"]),
            tooltip=["등급설명", "AREA_COUNT"],
        ),
        use_container_width=True,
    )

# ── 안내 문구 ──
st.divider()
st.caption(
    "본 서비스의 추천 결과는 공개 데이터 기반의 참고 정보이며, "
    "법률·세무·투자 판단을 대체하지 않습니다. "
    "실제 계약 전 반드시 전문가 상담을 받으시기 바랍니다."
)
