import json

import altair as alt
import pandas as pd
import streamlit as st

from common.queries import load_all_area_history, load_complex_summary, load_pyeong_bucket_data, load_recent_transactions, load_scores
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
from common.session import get_safe_session

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


def _format_pct(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.1f}%"


def _safe_pct_change(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None:
        return None
    if pd.isna(current) or pd.isna(previous) or float(previous) == 0:
        return None
    return round((float(current) - float(previous)) / float(previous) * 100, 1)


def build_market_flow_snapshot(history_df: pd.DataFrame) -> dict[str, object]:
    if history_df.empty:
        return {}

    ordered = history_df.sort_values("YYYYMMDD").copy()
    ordered["PRICE"] = pd.to_numeric(ordered["PRICE"], errors="coerce")
    ordered["JEONSE_PRICE"] = pd.to_numeric(ordered["JEONSE_PRICE"], errors="coerce")
    ordered = ordered.dropna(subset=["PRICE", "JEONSE_PRICE"], how="all").reset_index(drop=True)
    if ordered.empty:
        return {}

    latest = ordered.iloc[-1]
    basis_row = ordered.iloc[-7] if len(ordered) > 6 else ordered.iloc[0]
    latest_price = float(latest["PRICE"]) if pd.notna(latest["PRICE"]) else None
    latest_jeonse = float(latest["JEONSE_PRICE"]) if pd.notna(latest["JEONSE_PRICE"]) else None
    latest_ratio = round(latest_jeonse / latest_price * 100, 1) if latest_price and latest_jeonse else None
    price_change = _safe_pct_change(latest_price, float(basis_row["PRICE"]) if pd.notna(basis_row["PRICE"]) else None)
    jeonse_change = _safe_pct_change(
        latest_jeonse,
        float(basis_row["JEONSE_PRICE"]) if pd.notna(basis_row["JEONSE_PRICE"]) else None,
    )

    return {
        "latest_month": pd.to_datetime(latest["YYYYMMDD"]).strftime("%Y-%m"),
        "latest_price": latest_price,
        "latest_jeonse": latest_jeonse,
        "latest_ratio": latest_ratio,
        "price_change": price_change,
        "jeonse_change": jeonse_change,
        "history_points": int(len(ordered)),
    }


def build_market_flow_summary(selected_area: str, snapshot: dict[str, object]) -> str:
    if not snapshot:
        return f"{selected_area}의 최근 시세 요약을 만들 데이터가 아직 충분하지 않습니다."

    price_text = _format_pct(snapshot.get("price_change"))
    jeonse_text = _format_pct(snapshot.get("jeonse_change"))
    ratio = snapshot.get("latest_ratio")
    ratio_text = f"{ratio:.1f}%" if isinstance(ratio, (int, float)) else "-"

    return (
        f"{selected_area}의 최신 기준월은 {snapshot['latest_month']}이고, "
        f"최근 흐름 기준 매매가는 {price_text}, 전세가는 {jeonse_text} 움직였습니다. "
        f"현재 전세가율은 {ratio_text}입니다."
    )


def _extract_cortex_text(value: object) -> str:
    if value is None:
        return ""

    text = str(value).strip()
    if not text:
        return ""

    try:
        payload = json.loads(text)
    except Exception:
        return text

    if isinstance(payload, dict):
        choices = payload.get("choices")
        if isinstance(choices, list) and choices:
            first = choices[0]
            if isinstance(first, dict):
                for key in ("messages", "message", "text", "content"):
                    candidate = first.get(key)
                    if isinstance(candidate, str) and candidate.strip():
                        return candidate.strip()
                    if isinstance(candidate, list):
                        collected = []
                        for item in candidate:
                            if isinstance(item, str) and item.strip():
                                collected.append(item.strip())
                            elif isinstance(item, dict):
                                text_value = item.get("text") or item.get("content")
                                if isinstance(text_value, str) and text_value.strip():
                                    collected.append(text_value.strip())
                        if collected:
                            return "\n".join(collected)
        for key in ("text", "content", "response"):
            candidate = payload.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()

    return text


@st.cache_data(show_spinner=False)
def get_candidate_ai_summary(_session, prompt: str) -> str:
    try:
        result = _session.sql(
            "SELECT SNOWFLAKE.CORTEX.TRY_COMPLETE('mistral-large2', ?)",
            params=[prompt],
        ).collect()
    except Exception:
        return ""

    if not result:
        return ""

    return _extract_cortex_text(result[0][0])


def build_candidate_ai_prompt(
    selected_area: str,
    grade_label: str,
    candidate_row: pd.Series,
    survey_result: dict[str, object],
    history_snapshot: dict[str, object],
) -> str:
    return (
        "당신은 전세 안전을 설명하는 부동산 데이터 분석가입니다. "
        "아래 정보를 바탕으로 사용자의 질문 '이 동네 어떤가요?'에 답해주세요. "
        "응답은 한국어 4문장 이내로 작성하고, 첫 문장은 한줄 총평, "
        "다음 문장들은 근거와 주의할 점을 쉽게 설명하세요. 전문용어는 최소화하세요.\n\n"
        f"동네: {selected_area}\n"
        f"안전등급: {grade_label}\n"
        f"전세가율: {candidate_row['JEONSE_RATE']:.1f}%\n"
        f"예상 손실 노출: {format_currency_krw(candidate_row['LOSS_EXPOSURE_AMOUNT'])}\n"
        f"추정 전세 총액: {format_currency_krw(candidate_row['ESTIMATED_TOTAL_JEONSE'])}\n"
        f"전세가율 점수: {candidate_row['S_RATE']:.1f}/100\n"
        f"거래활발도 점수: {candidate_row['S_MIG']:.1f}/100\n"
        f"안정성 보조 점수: {candidate_row['S_SUB']:.1f}/100\n"
        f"사용자 성향: {survey_result['profile']}\n"
        f"최근 시장 요약: {build_market_flow_summary(selected_area, history_snapshot)}"
    )


def init_state() -> None:
    if "deposit_amount" not in st.session_state:
        st.session_state["deposit_amount"] = 500_000_000
    if "deposit_input_eok" not in st.session_state:
        st.session_state["deposit_input_eok"] = to_eok(st.session_state["deposit_amount"])
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

        /* Form submit 안내 메시지 + Enter to apply 숨김 */
        [data-testid="InputInstructions"],
        [data-testid="stFormSubmitButton"] + div,
        .stForm [data-testid="InputInstructions"],
        .stNumberInput [data-testid="InputInstructions"],
        [data-testid="stNumberInput"] [data-testid="InputInstructions"] {
            display: none !important;
        }

        /* number_input 영어 에러 메시지 숨김 (한국어로 대체) */
        [data-testid="stNumberInput"] .stAlert,
        .stNumberInput [data-baseweb="notification"] {
            display: none !important;
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
    long_df["YYYYMMDD"] = pd.to_datetime(long_df["YYYYMMDD"])
    long_df["월"] = long_df["YYYYMMDD"].dt.strftime("%Y년 %m월")
    return alt.Chart(long_df).mark_line(point=True).encode(
        x=alt.X("YYYYMMDD:T", title="기간", axis=alt.Axis(format="%Y-%m")),
        y=alt.Y("price:Q", title="평당가 (만원)"),
        color=alt.Color("series:N", title="구분"),
        tooltip=["월:N", "series:N", alt.Tooltip("price:Q", format=",.0f", title="만원")],
    )



# ── Main ─────────────────────────────────────────────────────────────────────

init_state()
inject_styles()

session = get_safe_session()
scores_df = load_scores(session)
all_area_history_df = load_all_area_history(session)
pyeong_bucket_df = load_pyeong_bucket_data(session)

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
        _setup_dep = st.number_input(
            "보증금 (억원)",
            min_value=0.0,
            step=0.1,
            value=2.0,
            format="%.1f",
            key="setup_deposit_eok",
        )
        if _setup_dep > _MAX_DEPOSIT_EOK:
            st.warning(f"최대 {_MAX_DEPOSIT_EOK:.0f}억원까지 입력 가능합니다.")
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
        st.session_state["deposit_amount"] = from_eok(min(st.session_state["setup_deposit_eok"], _MAX_DEPOSIT_EOK))
        st.session_state["deposit_input_eok"] = st.session_state["setup_deposit_eok"]
        st.session_state["confirmed_sgg"] = st.session_state["setup_sgg"]
        st.session_state["confirmed_emd"] = st.session_state["setup_emd"]
        st.session_state["confirmed_workplace"] = st.session_state["setup_workplace"]
        st.session_state["confirmed_pyeong"] = st.session_state["setup_pyeong"]
        st.session_state["confirmed_scope"] = st.session_state["setup_scope"]
        st.session_state["confirmed_budget"] = st.session_state["setup_budget"]
        st.session_state["conditions_confirmed"] = True
        st.rerun()

    st.markdown("")
    if st.button(":material/refresh: 설문 다시 하기"):
        reset_survey()
        st.rerun()
    st.stop()

# ── Step 3: 결과 ──
# @st.fragment: 구/동 변경 시 이 블록만 rerun (전체 페이지 rerun 안 함)
@st.fragment
def sidebar_controls():
    st.title("추천 조건")

    # form 박스의 기본 border를 숨기고 외부 container border만 사용
    st.markdown("""<style>
    [data-testid="stSidebar"] [data-baseweb="select"] {
        width: 100% !important;
        max-width: 100% !important;
    }
    [data-testid="stSidebar"] [data-baseweb="select"] > div {
        width: 100% !important;
    }
    /* 사이드바 안 form의 기본 border 제거 (외부 container가 border 담당) */
    [data-testid="stSidebar"] [data-testid="stForm"] {
        border: none !important;
        padding: 0 !important;
    }
    </style>""", unsafe_allow_html=True)

    with st.container(border=True):
        _sgg = st.selectbox(
            "현재 관심 구", sgg_options,
            index=sgg_options.index(st.session_state.get("confirmed_sgg", sgg_options[0])),
        )
        _emd_options = sorted(scores_df.loc[scores_df["SGG"] == _sgg, "EMD"].dropna().unique().tolist())
        _default_emd = st.session_state.get("confirmed_emd", _emd_options[0] if _emd_options else "")
        _emd = st.selectbox(
            "현재 관심 동", _emd_options,
            index=_emd_options.index(_default_emd) if _default_emd in _emd_options else 0,
        )

        with st.form("sidebar_conditions"):
            deposit_input = st.number_input(
                "보증금 (억원)",
                min_value=0.0,
                step=0.1,
                value=st.session_state.get("deposit_input_eok", to_eok(st.session_state["deposit_amount"])),
                format="%.1f",
            )
            if deposit_input > _MAX_DEPOSIT_EOK:
                deposit_input = _MAX_DEPOSIT_EOK
                st.warning(f"최대 {_MAX_DEPOSIT_EOK:.0f}억원까지 입력 가능합니다.")
            workplace_sgg = st.selectbox("주요 생활권 / 출근 구", sgg_options, index=sgg_options.index(st.session_state.get("confirmed_workplace", sgg_options[min(1, len(sgg_options) - 1)])))
            preferred_pyeong = st.slider("희망 평형 (평)", 10, 40, st.session_state.get("confirmed_pyeong", 24), 1)
            search_scope = st.selectbox("탐색 범위", SEARCH_SCOPE_OPTIONS, index=SEARCH_SCOPE_OPTIONS.index(st.session_state.get("confirmed_scope", SEARCH_SCOPE_OPTIONS[1])))
            budget_tolerance_pct = st.slider("예산 허용 범위 (±%)", 5, 20, st.session_state.get("confirmed_budget", 10), 1)

            if st.form_submit_button("조건 적용", type="primary", use_container_width=True):
                st.session_state["deposit_amount"] = from_eok(min(deposit_input, _MAX_DEPOSIT_EOK))
                st.session_state["deposit_input_eok"] = min(deposit_input, _MAX_DEPOSIT_EOK)
                st.session_state["confirmed_sgg"] = _sgg
                st.session_state["confirmed_emd"] = _emd
                st.session_state["confirmed_workplace"] = workplace_sgg
                st.session_state["confirmed_pyeong"] = preferred_pyeong
                st.session_state["confirmed_scope"] = search_scope
                st.session_state["confirmed_budget"] = budget_tolerance_pct
                st.rerun()

with st.sidebar:
    sidebar_controls()

# 확정된 값 읽기
selected_sgg = st.session_state.get("confirmed_sgg", sgg_options[0])
selected_emd_options = sorted(scores_df.loc[scores_df["SGG"] == selected_sgg, "EMD"].dropna().unique().tolist())
selected_emd = st.session_state.get("confirmed_emd", selected_emd_options[0] if selected_emd_options else "")
workplace_sgg = st.session_state.get("confirmed_workplace", sgg_options[min(1, len(sgg_options) - 1)])
preferred_pyeong = st.session_state.get("confirmed_pyeong", 24)
search_scope = st.session_state.get("confirmed_scope", SEARCH_SCOPE_OPTIONS[1])
budget_tolerance_pct = st.session_state.get("confirmed_budget", 10)

survey_answers = get_survey_answers()
survey_result = classify_survey_profile(survey_answers)

st.sidebar.divider()
st.sidebar.markdown(
    f'{_icon(PROFILE_ICONS.get(survey_result["profile"], "person"), 18, "#2d7a52")} '
    f'**{survey_result["profile"]}**',
    unsafe_allow_html=True,
)
st.sidebar.caption(survey_result["description"])
if st.sidebar.button(":material/refresh: 성향 다시 측정", use_container_width=True):
    reset_survey()
    st.rerun()

deposit_amount = st.session_state["deposit_amount"]
selected_area = f"{selected_sgg} {selected_emd}"

# 페이지 중앙 로딩 스피너
_loading_ph = st.empty()
_loading_ph.markdown(
    """
    <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:50vh">
        <style>@keyframes _spin{0%{transform:rotate(0deg)}100%{transform:rotate(360deg)}}</style>
        <div style="width:48px;height:48px;border-radius:50%;border:4px solid #e0e0e0;
             border-top-color:#2d7a52;animation:_spin .8s linear infinite"></div>
        <p style="margin-top:14px;color:#888;font-size:0.9rem">추천 결과를 계산하고 있습니다...</p>
    </div>
    """,
    unsafe_allow_html=True,
)

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
    pyeong_bucket_df=pyeong_bucket_df,
)

candidate_row = recommendation_df.loc[recommendation_df["AREA_LABEL"] == selected_area].iloc[0]
filtered_df = recommendation_df[recommendation_df["FILTER_MATCH"]].copy()
better_df = recommendation_df[recommendation_df["BETTER_ALTERNATIVE"]].copy()
best_alternative = better_df.iloc[0] if not better_df.empty else None
selected_history_df = get_area_history(all_area_history_df, candidate_row["SGG"], candidate_row["EMD"])
market_snapshot = build_market_flow_snapshot(selected_history_df)

# 계산 완료 → 스피너 제거
_loading_ph.empty()

# ── Profile + metrics ──
st.markdown(_profile_card_html(survey_result["profile"]), unsafe_allow_html=True)

# 데이터 활용 안내 (SPH 3개 구 여부)
_SPH_DISTRICTS = {"중구", "영등포구", "서초구"}
_has_sph = selected_sgg in _SPH_DISTRICTS
if _has_sph:
    st.markdown(
        f'{_icon("verified", 16, "#2d7a52")} '
        f'<span style="font-size:0.82rem;color:#2d7a52">'
        f'{selected_sgg}는 인구·소득·신용 데이터가 추가 반영되어 더 정밀한 추천이 제공됩니다.</span>',
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        f'{_icon("info", 16, "#999")} '
        f'<span style="font-size:0.82rem;color:#999">'
        f'{selected_sgg}는 거래·가격 데이터 기반으로 추천합니다. '
        f'중구·영등포구·서초구는 인구·소득 데이터가 추가 반영됩니다.</span>',
        unsafe_allow_html=True,
    )

# 3개 점수 분리 표시: 룰 기반 / AI 예측 / 최종 하이브리드
score_1, score_2, score_3 = st.columns(3)
score_1.metric(
    "룰 기반 안전점수",
    f"{candidate_row.get('RULE_SCORE', candidate_row['RECOMMENDATION_SCORE']):.1f}점",
    help="전세가율, 거래량, 가격안정성 등 공식 기반 점수",
)
_ml_risk = candidate_row.get('ML_RISK_SCORE', 50)
score_2.metric(
    "AI 하락 위험도",
    f"{_ml_risk:.0f}%",
    delta=f"{'낮음' if _ml_risk < 30 else '보통' if _ml_risk < 60 else '높음'}",
    delta_color="inverse",
    help="6개월 내 전세가 5% 이상 하락할 확률 (ML 예측)",
)
score_3.metric(
    "최종 추천점수",
    f"{candidate_row['RECOMMENDATION_SCORE']:.1f}점",
    help="룰 기반 80% + AI 예측 20% 하이브리드",
)

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
                추정 전세 ({preferred_pyeong}평 기준) {format_currency_krw(candidate_row['ESTIMATED_TOTAL_JEONSE'])}<br>
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
    st.subheader(f"설문 성향을 반영한 추천 결과 ({preferred_pyeong}평 기준)")
    _used_bucket = filtered_df["USES_BUCKET_DATA"].sum() if "USES_BUCKET_DATA" in filtered_df.columns else 0
    _total_filt = len(filtered_df)
    if _total_filt > 0:
        st.caption(
            f"{_used_bucket}/{_total_filt}개 동에서 해당 평형대(±5평) 실거래로 정밀 계산, "
            f"나머지는 동 평균 사용"
        )

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
        cortex_prompt = build_candidate_ai_prompt(
            selected_area=selected_area,
            grade_label=grade_label,
            candidate_row=candidate_row,
            survey_result=survey_result,
            history_snapshot=market_snapshot,
        )
        ai_summary = get_candidate_ai_summary(session, cortex_prompt)
        st.write(ai_summary or build_candidate_summary(candidate_row, survey_result))

        st.markdown("#### 상세 정보")
        st.dataframe(
            pd.DataFrame(
                [
                    (f"추정 전세 ({preferred_pyeong}평 기준)", format_currency_krw(candidate_row["ESTIMATED_TOTAL_JEONSE"])),
                    ("예상 손실 노출", format_currency_krw(candidate_row["LOSS_EXPOSURE_AMOUNT"])),
                    ("전세가율", f"{candidate_row['JEONSE_RATE']:.1f}%"),
                    ("안전 등급", grade_label),
                ],
                columns=["항목", "값"],
            ),
            use_container_width=True,
            hide_index=True,
        )

    # ── 위기 시뮬레이션 ──
    st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)
    st.markdown(
        f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:0.3rem">'
        f'{_icon("trending_down", 22, "#e65100")}'
        f'<span style="font-size:1.05rem;font-weight:700;color:#1a1a1a">전세가 하락 시뮬레이션</span>'
        f"</div>"
        f'<p style="color:#666;font-size:0.85rem;margin:0 0 0.3rem;line-height:1.6">'
        f"만약 이 동네 전세가가 떨어지면, 계약 만기 때 보증금을 전액 돌려받지 못할 수 있습니다.<br>"
        f"아래는 하락폭별로 <b>돌려받지 못할 수 있는 금액</b>을 계산한 것입니다.</p>",
        unsafe_allow_html=True,
    )

    _total_jeonse = float(candidate_row["ESTIMATED_TOTAL_JEONSE"])
    _jeonse_rate = float(candidate_row["JEONSE_RATE"])

    # 시뮬레이션 기준: 보증금과 추정 전세 중 작은 값 (실제 계약 기준)
    _sim_deposit = min(deposit_amount, _total_jeonse)

    if deposit_amount > _total_jeonse:
        st.markdown(
            f'<p style="color:#2d7a52;font-size:0.85rem;margin:0 0 0.5rem">'
            f'{_icon("check_circle", 16, "#2d7a52")} '
            f'내 보증금({format_currency_krw(deposit_amount)})이 이 동네 추정 전세({format_currency_krw(_total_jeonse)})보다 높아, '
            f'추정 전세 기준으로 시뮬레이션합니다.</p>',
            unsafe_allow_html=True,
        )

    sim_scenarios = [5, 10, 15, 20, 30]

    sim_cols = st.columns(len(sim_scenarios))
    for i, col in enumerate(sim_cols):
        drop_pct = sim_scenarios[i]
        new_jeonse = _total_jeonse * (1 - drop_pct / 100)
        loss_val = max(0, _sim_deposit - new_jeonse)
        recovery_rate = min(100, new_jeonse / max(_sim_deposit, 1) * 100)

        if loss_val == 0:
            bg = "#e8f5e9"; border = "#4caf50"; label = "안전"; label_color = "#2e7d32"
        elif loss_val < _sim_deposit * 0.05:
            bg = "#fff8e1"; border = "#ffc107"; label = "주의"; label_color = "#f57f17"
        elif loss_val < _sim_deposit * 0.15:
            bg = "#fff3e0"; border = "#ff9800"; label = "경고"; label_color = "#e65100"
        else:
            bg = "#fbe9e7"; border = "#f44336"; label = "위험"; label_color = "#c62828"

        col.markdown(
            f'<div style="background:{bg};border:2px solid {border};border-radius:12px;'
            f'text-align:center;padding:0.8rem 0.4rem">'
            f'<div style="font-size:0.7rem;font-weight:700;color:{label_color};'
            f'text-transform:uppercase;letter-spacing:0.05em">{label}</div>'
            f'<div style="font-size:1rem;font-weight:700;color:#1a1a1a;margin:0.2rem 0">-{drop_pct}%</div>'
            f'<div style="font-size:0.8rem;color:#555">'
            f'못 받는 돈<br><b style="color:{label_color}">{format_currency_krw(loss_val)}</b></div>'
            f'<div style="font-size:0.72rem;color:#999;margin-top:0.2rem">회수율 {recovery_rate:.0f}%</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    _bucket_label = {
        "SMALL": "소형 (≤15평)", "MID": "중형 (15~25평)",
        "LARGE": "대형 (25~40평)", "XLARGE": "특대 (40평+)"
    }.get(candidate_row.get("PYEONG_BUCKET", "MID"), "")
    _uses_bucket = bool(candidate_row.get("USES_BUCKET_DATA", False))
    _bucket_note = f"{_bucket_label} 평형대 시세 반영" if _uses_bucket else "동 평균 시세 (해당 평형대 거래 부족)"

    st.caption(
        f"{preferred_pyeong}평 기준 · {_bucket_note} · "
        f"보증금 {format_currency_krw(_sim_deposit)} · "
        f"추정 전세 {format_currency_krw(_total_jeonse)} · "
        f"전세가율 {_jeonse_rate:.1f}%"
    )

    # ── 계약 전 체크리스트 ──
    st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)
    st.markdown(
        f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:0.3rem">'
        f'{_icon("checklist", 22, "#1a1a1a")}'
        f'<span style="font-size:1.05rem;font-weight:700;color:#1a1a1a">계약 전 체크리스트</span>'
        f"</div>"
        f'<p style="color:#888;font-size:0.85rem;margin:0 0 0.5rem">'
        f"전세 계약 전 반드시 확인해야 할 항목입니다.</p>",
        unsafe_allow_html=True,
    )

    checklist = [
        ("등기부등본 확인", "소유자, 근저당, 가압류 등 권리관계를 확인하세요. 계약 당일 다시 발급받아 확인하는 것이 안전합니다."),
        ("선순위 보증금 확인", "이미 설정된 근저당이나 선순위 전세가 있는지 확인하세요. 내 보증금보다 선순위 합계가 크면 위험합니다."),
        ("전세보증금 반환보증 가입", "HUG, SGI 등에서 보증보험에 가입할 수 있는지 확인하세요. 보험 가입이 불가한 매물은 피하는 것이 좋습니다."),
        ("건축물대장 확인", "불법 증축이나 용도 변경이 없는지 확인하세요. 위반 건축물은 보증보험 가입이 거절될 수 있습니다."),
        ("임대인 세금 체납 확인", "국세·지방세 체납 여부를 확인하세요. 체납이 있으면 보증금 회수가 어려울 수 있습니다."),
        ("확정일자·전입신고", "계약 후 즉시 전입신고와 확정일자를 받으세요. 대항력과 우선변제권 확보에 필수입니다."),
    ]

    for title, desc in checklist:
        st.markdown(
            f'<div class="s-card" style="margin-bottom:0.5rem;padding:0.8rem 1rem">'
            f'<div style="display:flex;align-items:flex-start;gap:0.6rem">'
            f'{_icon("task_alt", 20, "#2d7a52")}'
            f'<div>'
            f'<div style="font-weight:600;color:#1a1a1a;font-size:0.9rem">{title}</div>'
            f'<div style="color:#666;font-size:0.82rem;margin-top:2px;line-height:1.5">{desc}</div>'
            f'</div></div></div>',
            unsafe_allow_html=True,
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
            combined_history["YYYYMMDD"] = pd.to_datetime(combined_history["YYYYMMDD"])
            combined_history["월"] = combined_history["YYYYMMDD"].dt.strftime("%Y년 %m월")
            chart_left, chart_right = st.columns(2)
            chart_left.altair_chart(
                alt.Chart(combined_history).mark_line(point=True).encode(
                    x=alt.X("YYYYMMDD:T", title="기간", axis=alt.Axis(format="%Y-%m")),
                    y=alt.Y("JEONSE_PRICE:Q", title="전세 평당가 (만원)"),
                    color=alt.Color("AREA_LABEL:N", title="동네"),
                    tooltip=["AREA_LABEL", "월:N", alt.Tooltip("JEONSE_PRICE:Q", format=",.0f", title="만원")],
                ),
                use_container_width=True,
            )
            chart_right.altair_chart(
                alt.Chart(combined_history).mark_line(point=True).encode(
                    x=alt.X("YYYYMMDD:T", title="기간", axis=alt.Axis(format="%Y-%m")),
                    y=alt.Y("JEONSE_RATIO:Q", title="전세가율 (%)"),
                    color=alt.Color("AREA_LABEL:N", title="동네"),
                    tooltip=["AREA_LABEL", "월:N", alt.Tooltip("JEONSE_RATIO:Q", format=".1f", title="%")],
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
        def _fmt_pyeong_price(val):
            """평당가(만원 단위)를 자연스럽게 표시."""
            if not val:
                return "-"
            v = float(val)
            if v >= 10000:
                return f"{v/10000:.1f}억/평"
            return f"{v:,.0f}만/평"

        metric_1, metric_2, metric_3, metric_4 = st.columns(4)
        metric_1.metric("기준월", str(market_snapshot.get("latest_month", "-")))
        metric_2.metric(
            "매매 평당가",
            _fmt_pyeong_price(market_snapshot.get("latest_price")),
            _format_pct(market_snapshot.get("price_change")),
        )
        metric_3.metric(
            "전세 평당가",
            _fmt_pyeong_price(market_snapshot.get("latest_jeonse")),
            _format_pct(market_snapshot.get("jeonse_change")),
        )
        metric_4.metric(
            "전세가율",
            f"{market_snapshot['latest_ratio']:.1f}%" if market_snapshot.get("latest_ratio") else "-",
            f"이력 {market_snapshot.get('history_points', 0)}건",
        )

        st.caption(build_market_flow_summary(selected_area, market_snapshot))
        st.markdown(f"**{selected_area}** 매매가·전세가 추이")
        st.altair_chart(make_history_chart(selected_history_df), use_container_width=True)

    # ── 최근 실거래 내역 ──
    st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)
    st.markdown(
        f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:0.5rem">'
        f'{_icon("receipt_long", 22, "#1a1a1a")}'
        f'<span style="font-size:1.05rem;font-weight:700;color:#1a1a1a">{selected_area} 최근 실거래</span>'
        f"</div>",
        unsafe_allow_html=True,
    )

    tx_type = st.radio("거래 유형 필터", ["전체", "매매", "전세"], horizontal=True, label_visibility="collapsed")
    raw_tx = load_recent_transactions(session, selected_sgg, selected_emd, limit=50)

    if raw_tx.empty:
        st.info("최근 거래 내역이 없습니다.")
    else:
        if tx_type != "전체":
            raw_tx = raw_tx[raw_tx["거래유형"] == tx_type]

        raw_tx["거래가(만원)"] = raw_tx["거래가(만원)"].apply(lambda v: f"{v:,.0f}" if pd.notna(v) else "-")
        raw_tx["평당가(만원)"] = raw_tx["평당가(만원)"].apply(lambda v: f"{v:,.0f}" if pd.notna(v) else "-")
        st.dataframe(raw_tx, use_container_width=True, hide_index=True, height=350)
        st.caption(f"총 {len(raw_tx)}건 · 매매=매매가, 전세=보증금 (단위: 만원)")

    # ── 단지별 비교 ──
    st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)
    st.markdown(
        f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:0.5rem">'
        f'{_icon("apartment", 22, "#1a1a1a")}'
        f'<span style="font-size:1.05rem;font-weight:700;color:#1a1a1a">{selected_area} 단지별 시세</span>'
        f"</div>"
        f'<p style="color:#888;font-size:0.85rem;margin:0 0 0.5rem">'
        f"최근 6개월 거래 기준, 단지별 매매·전세 중위가격을 비교합니다.</p>",
        unsafe_allow_html=True,
    )

    complex_df = load_complex_summary(session, selected_sgg, selected_emd)
    if complex_df.empty:
        st.info("최근 6개월 내 거래가 있는 단지가 없습니다.")
    else:
        # 금액 포맷
        for col in ["매매중위(만원)", "매매평당(만원)", "전세중위(만원)", "전세평당(만원)"]:
            if col in complex_df.columns:
                complex_df[col] = complex_df[col].apply(lambda v: f"{v:,.0f}" if pd.notna(v) else "-")
        if "전세가율(%)" in complex_df.columns:
            complex_df["전세가율(%)"] = complex_df["전세가율(%)"].apply(lambda v: f"{v:.1f}" if pd.notna(v) else "-")
        if "주요면적(m²)" in complex_df.columns:
            complex_df["주요면적(m²)"] = complex_df["주요면적(m²)"].apply(lambda v: f"{v:.0f}" if pd.notna(v) else "-")
        complex_df = complex_df.fillna("-")
        st.dataframe(complex_df, use_container_width=True, hide_index=True)
        st.caption(f"총 {len(complex_df)}개 단지")

# ── 안내 문구 ──
st.divider()
st.caption(
    "본 서비스의 추천 결과는 공개 데이터 기반의 참고 정보이며, "
    "법률·세무·투자 판단을 대체하지 않습니다. "
    "실제 계약 전 반드시 전문가 상담을 받으시기 바랍니다."
)
