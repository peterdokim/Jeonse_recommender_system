import json
from concurrent.futures import ThreadPoolExecutor

import altair as alt
import pandas as pd
import streamlit as st

from common.queries import (
    load_all_area_history,
    load_complex_summary,
    load_market_briefing,
    load_market_rankings,
    load_pyeong_bucket_data,
    load_recent_transactions,
    load_scores,
)
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


LIKERT_SCALE_LABELS = [
    "매우 비동의",
    "비동의",
    "보통",
    "동의",
    "매우 동의",
]


def _ai_loading_card_html(message: str = "AI가 분석하고 있어요...") -> str:
    """Animated 'AI is thinking' placeholder card. Reusable across tabs."""
    return (
        '<div style="background:linear-gradient(135deg,#f7faf8 0%,#eef5f1 100%);'
        'border:1px solid #d4e4dc;border-radius:14px;padding:1.5rem 1.6rem;'
        'margin-bottom:1.0rem;min-height:130px;'
        'display:flex;align-items:center;justify-content:center;flex-direction:column;gap:0.9rem">'
        '<div style="display:flex;gap:7px">'
        '<span style="width:11px;height:11px;border-radius:50%;background:#2d7a52;'
        'animation:ai-bounce 1.2s infinite ease-in-out"></span>'
        '<span style="width:11px;height:11px;border-radius:50%;background:#2d7a52;'
        'animation:ai-bounce 1.2s infinite ease-in-out;animation-delay:0.15s"></span>'
        '<span style="width:11px;height:11px;border-radius:50%;background:#2d7a52;'
        'animation:ai-bounce 1.2s infinite ease-in-out;animation-delay:0.3s"></span>'
        '</div>'
        f'<div style="font-size:0.92rem;color:#3a5347;letter-spacing:-0.2px;font-weight:500">{message}</div>'
        '</div>'
        '<style>@keyframes ai-bounce{0%,80%,100%{transform:scale(0.55);opacity:0.4}'
        '40%{transform:scale(1);opacity:1}}</style>'
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


@st.cache_data(show_spinner=False, ttl=3600, max_entries=200)
def get_ai_structured_analysis(_session, cache_key: str, prompt: str) -> dict:  # noqa: ARG001
    """Cortex AI_COMPLETE로 구조화된 JSON 분석 결과를 반환한다.
    cache_key는 캐시 식별 전용 (함수 내부에서 사용 안 함). 동/평형/성향만 포함.

    모델 우선순위 (속도·품질 균형):
      1) claude-3-5-sonnet — 빠르고 JSON 출력 품질 우수
      2) llama3.1-70b — 거의 모든 Snowflake 리전에서 사용 가능
      3) mistral-large2 — 최후의 폴백 (느리지만 항상 가능)
    """
    _ = cache_key  # cache key marker
    result = None
    for model_name in ("claude-3-5-sonnet", "llama3.1-70b", "mistral-large2"):
        try:
            result = _session.sql(
                f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model_name}', ?)",
                params=[prompt],
            ).collect()
            if result:
                break
        except Exception:
            continue
    if result is None:
        return {}

    if not result:
        return {}

    raw_text = _extract_cortex_text(result[0][0])
    if not raw_text:
        return {}

    # JSON 파싱 시도
    try:
        # 코드블록 제거
        cleaned = raw_text.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("```")[1]
            if cleaned.startswith("json"):
                cleaned = cleaned[4:]
            cleaned = cleaned.strip()
        return json.loads(cleaned)
    except Exception:
        return {"summary": raw_text, "strengths": [], "risks": [], "recommended_action": "", "confidence": "medium"}


@st.cache_data(show_spinner=False, ttl=3600)
def call_cortex_analyst(_session, question: str) -> dict:
    """Cortex Analyst REST API 호출 → 자연어 질문을 SQL로 변환."""
    import requests
    conn = _session.connection
    url = f"https://{conn.host}/api/v2/cortex/analyst/message"
    headers = {
        "Authorization": f'Snowflake Token="{conn.rest.token}"',
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    body = {
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": question}]}
        ],
        "semantic_model_file": "@HACKATHON_APP.RESILIENCE.SEMANTIC_MODELS/jeonse_model.yaml",
    }
    try:
        r = requests.post(url, headers=headers, json=body, timeout=60)
        if r.status_code != 200:
            return {"error": f"API 오류 ({r.status_code}): {r.text[:200]}"}
        result = r.json()
        sql = None
        text = ""
        for item in result.get("message", {}).get("content", []):
            if item.get("type") == "sql":
                sql = item.get("statement")
            elif item.get("type") == "text":
                text = item.get("text", "")
        return {"sql": sql, "text": text}
    except Exception as e:
        return {"error": str(e)}


def run_analyst_question(_session, question: str) -> dict:
    """질문을 받아 Analyst → SQL 실행 → 결과 반환."""
    response = call_cortex_analyst(_session, question)
    if "error" in response:
        return response
    sql = response.get("sql")
    if not sql:
        return {"error": "SQL을 생성하지 못했습니다.", "text": response.get("text", "")}
    try:
        df = _session.sql(sql).to_pandas()
        return {"sql": sql, "df": df, "text": response.get("text", "")}
    except Exception as e:
        return {"error": f"SQL 실행 실패: {e}", "sql": sql}


def _interpret_activity(score: float) -> str:
    """거래 활발도 점수를 자연어로 변환."""
    if score >= 80:
        return "매우 활발"
    if score >= 60:
        return "활발한 편"
    if score >= 40:
        return "보통"
    if score >= 20:
        return "다소 부족"
    return "매우 부족"


def _interpret_volatility(score: float) -> str:
    if score >= 80:
        return "전세가가 매우 안정적"
    if score >= 60:
        return "전세가 변동이 적은 편"
    if score >= 40:
        return "보통 수준의 변동성"
    if score >= 20:
        return "전세가 변동이 큰 편"
    return "전세가 변동이 매우 큼"


def _interpret_risk(risk: float) -> str:
    if risk < 20:
        return "6개월 내 전세가 하락 가능성이 낮음"
    if risk < 40:
        return "6개월 내 전세가 하락 가능성이 다소 있음"
    if risk < 60:
        return "6개월 내 전세가 하락 가능성이 중간 정도"
    if risk < 80:
        return "6개월 내 전세가 하락 가능성이 높은 편"
    return "6개월 내 전세가 하락 위험이 매우 높음"


def _interpret_jeonse_rate(rate: float) -> str:
    if rate < 30:
        return "전세가가 매매가의 매우 낮은 비율 (보증금 회수에 매우 안전)"
    if rate < 50:
        return "전세가가 매매가의 낮은 비율 (보증금 회수에 안전한 편)"
    if rate < 70:
        return "전세가가 매매가의 보통 비율"
    if rate < 85:
        return "전세가가 매매가에 가까워 보증금 회수에 주의 필요"
    return "전세가가 매매가에 매우 근접해 보증금 회수 위험 큼"


def build_candidate_ai_prompt(
    selected_area: str,
    grade_label: str,
    candidate_row: pd.Series,
    survey_result: dict[str, object],
    history_snapshot: dict[str, object],
) -> str:
    emd = candidate_row.get("EMD", "")
    rate = float(candidate_row['JEONSE_RATE'])
    s_mig = float(candidate_row['S_MIG'])
    s_sub = float(candidate_row['S_SUB'])
    ml_risk = float(candidate_row.get('ML_RISK_SCORE', 50))

    return (
        f"당신은 서울 부동산 시장의 전세 데이터를 해석해 고객에게 안내하는 분석가입니다.\n"
        f"아래 제공된 데이터만을 근거로 **{selected_area}**의 전세 환경을 분석해 주세요.\n\n"
        "**반드시 지켜야 할 규칙:**\n"
        f"1. **검증 불가능한 구체 사실을 절대 지어내지 마세요.** 다음은 모두 금지입니다:\n"
        "   - 지하철 노선 번호 (예: '2호선이 지나는', '7호선 역세권')\n"
        "   - 특정 학교명, 학군 이름, 명문 학원가\n"
        "   - 재건축 단지명, 재개발 계획, 정비구역 지정 여부\n"
        "   - 특정 회사·업무지구·랜드마크의 정확한 위치 관계\n"
        "   - 도로명, 인접 시설명\n"
        "   당신은 이런 정보를 정확히 모릅니다. 잘못 말하면 사용자에게 큰 피해가 갑니다.\n"
        "2. 대신 **아래 제공된 데이터(전세가율, 거래 활발도, 가격 안정성, 등급, 시장 흐름)만**을 자연어로 풀어 설명하세요. "
        f"동네 이름 '{emd}'은 언급하되, 그 동네의 일반적 분위기(예: '주거 밀집 지역', '거래가 활발한 동네')를 데이터 기반으로만 표현하세요.\n"
        "3. **모든 문장은 정중한 존댓말 (~입니다, ~합니다, ~보입니다, ~해보세요)을 사용**하세요. 반말·단정조 금지.\n"
        "4. **표현 톤은 부드럽고 차분하게**. 자극적·과장된 단어 금지.\n"
        "   ❌ 너무 강함: '집주인이 보증금을 돌려주지 못할 위험이 큽니다'\n"
        "   ✅ 좋음: '전세가가 매매가에 거의 근접해 깡통전세 위험에 유의가 필요한 상황입니다'\n"
        "5. **점수, 퍼센트, /100 같은 수치 표현을 직접 사용하지 마세요**.\n"
        "   ❌ 나쁨: '거래 활발도 91/100', 'AI 하락 위험도 77%'\n"
        "   ✅ 좋음: '최근 거래가 매우 활발하게 이루어지고 있습니다', '향후 전세가 하락 가능성이 다소 있는 편입니다'\n"
        "6. '안전한 지역입니다', '주의가 필요합니다' 같은 일반론·상투구만으로 끝내지 말고 데이터 근거를 풀어 설명해 주세요.\n"
        "7. 따뜻하고 정중한 상담 톤으로, 부동산 전문가가 고객에게 차분히 안내하는 느낌.\n\n"
        "**반드시 JSON 형식만** 출력하세요:\n"
        "{\n"
        f'  "summary": "{emd}의 전세 환경에 대한 총평 (80~120자, 데이터 기반, 수치 표현 금지, 존댓말)",\n'
        '  "strengths": ["데이터 기반 강점 (~입니다)", ...3개],\n'
        '  "risks": ["데이터 기반 주의점 (~입니다)", ...2개],\n'
        '  "recommended_action": "사용자 성향에 맞는 구체적 행동 권장 (수치 없이, 정중한 존댓말 권유)",\n'
        '  "confidence": "high|medium|low"\n'
        "}\n\n"
        f"=== 사용자 ===\n"
        f"성향: {survey_result['profile']} - {survey_result.get('description', '')}\n\n"
        f"=== {selected_area} 데이터 (이것만 사용하세요) ===\n"
        f"안전 등급: {grade_label}\n"
        f"추정 전세 총액: {format_currency_krw(candidate_row['ESTIMATED_TOTAL_JEONSE'])}\n"
        f"전세가율 해석: {_interpret_jeonse_rate(rate)}\n"
        f"거래 활발도 해석: 최근 거래가 {_interpret_activity(s_mig)}함\n"
        f"가격 안정성 해석: {_interpret_volatility(s_sub)}\n"
        f"AI 예측: {_interpret_risk(ml_risk)}\n"
        f"최근 시장 흐름: {build_market_flow_summary(selected_area, history_snapshot)}\n\n"
        f"위 데이터만을 근거로 분석을 작성해 주세요. "
        f"지하철 노선·학교명·재건축 계획 등 검증 불가능한 사실은 절대 지어내지 마시고, "
        f"숫자는 직접 쓰지 마시고, 모든 문장은 부드러운 존댓말로 작성하세요. JSON만 출력:"
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
    st.caption("5점 척도입니다. 왼쪽부터 매우 비동의, 비동의, 보통, 동의, 매우 동의입니다.")

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
                '<p style="font-size:0.9rem;color:#7b4e9e;font-weight:700;'
                'padding-top:12px;white-space:nowrap">비동의</p>',
                unsafe_allow_html=True,
            )

        for i in range(5):
            val = i + 1
            is_sel = current == val
            # 왼쪽은 비동의, 오른쪽은 동의 방향으로 색을 맞춘다.
            if is_sel:
                label = f":gray[●]" if i == 2 else f":violet[●]" if i < 2 else f":green[●]"
            else:
                label = f":gray[○]"

            with cols[i + 1]:
                st.button(
                    label,
                    key=f"_cb_{question['key']}_{val}",
                    width="stretch",
                    help=f"{val}점: {LIKERT_SCALE_LABELS[i]}",
                    on_click=_on_circle_click,
                    args=(landing_key, val),
                )
                st.markdown(
                    (
                        '<p style="font-size:0.67rem;color:#666;text-align:center;'
                        'line-height:1.25;min-height:34px;margin:0.15rem 0 0">'
                        f'{val}<br>{LIKERT_SCALE_LABELS[i]}</p>'
                    ),
                    unsafe_allow_html=True,
                )

        with cols[6]:
            st.markdown(
                '<p style="font-size:0.9rem;color:#33a474;font-weight:700;'
                'padding-top:12px;text-align:right;white-space:nowrap">동의</p>',
                unsafe_allow_html=True,
            )

        if idx < len(SURVEY_QUESTIONS):
            st.divider()

    st.markdown("")
    if st.button("다음 단계로", type="primary", width="stretch"):
        for question in SURVEY_QUESTIONS:
            raw = int(st.session_state.get(f"landing_{question['key']}", 3))
            st.session_state[question["key"]] = raw
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
    if st.button("추천 결과 보기", type="primary", width="stretch"):
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

            if st.form_submit_button("조건 적용", type="primary", width="stretch"):
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
if st.sidebar.button(":material/refresh: 성향 다시 측정", width="stretch"):
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

# 보증금 부족 케이스: 후보가 보증금보다 비싸면 BETTER_ALTERNATIVE가 비어있을 수 있음
# → 필터 통과 + 후보 제외한 동에서 안전점수 높은 순으로 fallback
if better_df.empty:
    fallback_pool = recommendation_df[
        recommendation_df["FILTER_MATCH"] & (~recommendation_df["IS_CANDIDATE"])
    ].copy()
    if not fallback_pool.empty:
        better_df = fallback_pool.nlargest(10, "RECOMMENDATION_SCORE").copy()
best_alternative = better_df.iloc[0] if not better_df.empty else None
selected_history_df = get_area_history(all_area_history_df, candidate_row["SGG"], candidate_row["EMD"])
market_snapshot = build_market_flow_snapshot(selected_history_df)

# 계산 완료 → 스피너 제거
_loading_ph.empty()

# ── Profile + metrics ──
st.markdown(_profile_card_html(survey_result["profile"]), unsafe_allow_html=True)

# ── 보증금 vs 추정 전세 차이 경고 (보증금 부족 시 결과 숨김) ──
_cand_jeonse = float(candidate_row["ESTIMATED_TOTAL_JEONSE"])
if deposit_amount > 0 and _cand_jeonse > deposit_amount * 1.5:
    _shortfall = _cand_jeonse - deposit_amount
    st.markdown(
        f'<div style="background:#fff8e1;border-left:5px solid #f57f17;border-radius:10px;'
        f'padding:1.5rem;margin:1rem 0">'
        f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:0.5rem">'
        f'{_icon("warning", 24, "#e65100")}'
        f'<span style="font-size:1.1rem;font-weight:700;color:#e65100">보증금이 부족해요</span>'
        f'</div>'
        f'<div style="font-size:0.95rem;color:#1a1a1a;line-height:1.7">'
        f'<b>{selected_area}</b>의 {preferred_pyeong}평 예상 전세는 '
        f'<b>{format_currency_krw(_cand_jeonse)}</b>인데, '
        f'입력하신 보증금은 <b>{format_currency_krw(deposit_amount)}</b>이에요.<br>'
        f'약 <b style="color:#c62828">{format_currency_krw(_shortfall)}</b> 부족해서 이 동네는 추천하기 어려워요.'
        f'</div>'
        f'<div style="margin-top:1rem;padding-top:1rem;border-top:1px solid #ffe082;'
        f'font-size:0.9rem;color:#5d4037">'
        f'💡 <b>왼쪽 사이드바에서 보증금을 올린 후 "조건 적용"을 눌러보세요.</b><br>'
        f'또는 다른 동네를 선택하시면 맞춤 추천을 받으실 수 있어요.'
        f'</div>'
        f'</div>',
        unsafe_allow_html=True,
    )
    # 안내 문구 + 면책 표시 후 stop
    st.divider()
    st.caption(
        "본 서비스의 추천 결과는 공개 데이터 기반의 참고 정보이며, "
        "법률·세무·투자 판단을 대체하지 않습니다."
    )
    st.stop()

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

# 메인 점수: "나의 적합도" 하나만 크게
_main_score = candidate_row['RECOMMENDATION_SCORE']
_ml_risk = candidate_row.get('ML_RISK_SCORE', 50)

# 점수에 따른 색상
if _main_score >= 75:
    _score_color = "#2d7a52"
    _score_label = "매우 적합"
elif _main_score >= 55:
    _score_color = "#1565c0"
    _score_label = "적합한 편"
elif _main_score >= 35:
    _score_color = "#f57f17"
    _score_label = "주의 필요"
else:
    _score_color = "#c62828"
    _score_label = "위험"

st.markdown(
    f'<div style="background:linear-gradient(135deg,#fff 0%,#f7faf8 100%);'
    f'border:1px solid #d4e4dc;border-left:5px solid {_score_color};border-radius:14px;'
    f'padding:1.3rem 1.6rem;margin-bottom:0.8rem">'
    f'<div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:0.5rem">'
    f'<div>'
    f'<div style="font-size:0.78rem;color:#888;text-transform:uppercase;letter-spacing:0.05em;font-weight:600">'
    f'나의 적합도 점수</div>'
    f'<div style="margin-top:0.3rem"><span style="font-size:2.6rem;font-weight:800;color:{_score_color}">{_main_score:.1f}</span>'
    f'<span style="font-size:1.1rem;color:#666;margin-left:0.3rem">/ 100</span>'
    f'<span style="background:{_score_color};color:#fff;padding:4px 12px;border-radius:999px;'
    f'font-size:0.78rem;font-weight:700;margin-left:0.8rem;vertical-align:middle">{_score_label}</span></div>'
    f'</div>'
    f'<div style="text-align:right">'
    f'<div style="font-size:0.78rem;color:#888;font-weight:600">현재 후보 순위</div>'
    f'<div style="font-size:1.6rem;font-weight:800;color:#1a1a1a;margin-top:0.2rem">{int(candidate_row["RECOMMENDATION_RANK"])}<span style="font-size:1rem;color:#888">위 / {len(recommendation_df)}</span></div>'
    f'</div>'
    f'</div>'
    f'<div style="margin-top:0.7rem;padding-top:0.7rem;border-top:1px solid #e8eee9;'
    f'font-size:0.8rem;color:#666;line-height:1.55">'
    f'동네 안전도, AI 하락 예측, 회원님의 보증금·성향·평형을 모두 반영한 종합 점수입니다.'
    f'</div>'
    f'</div>',
    unsafe_allow_html=True,
)

# 보조 정보 메트릭
hero_1, hero_2, hero_3, hero_4 = st.columns(4)
hero_1.metric("안전 등급", f"{candidate_row['GRADE']} · {GRADE_MEANINGS[candidate_row['GRADE']]}")
hero_2.metric("AI 하락 위험도", f"{_ml_risk:.0f}%", help="AI가 예측한 6개월 내 전세가 5% 이상 하락할 확률")
hero_3.metric("조건 충족 대안", len(filtered_df) - 1 if len(filtered_df) > 0 else 0)
hero_4.metric(
    "예상 전세가",
    format_currency_krw(candidate_row["ESTIMATED_TOTAL_JEONSE"]),
    help=f"{preferred_pyeong}평 기준 이 동네 추정 전세 보증금",
)

st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)

summary_left, summary_right = st.columns(2)
with summary_left:
    st.markdown(
        f"""
        <div class="s-card">
            <div class="s-card-label">현재 후보</div>
            <div class="s-card-value">{selected_area}</div>
            <div class="s-card-body">
                <span class="s-card-pill">{candidate_row['GRADE']}등급 · {GRADE_MEANINGS[candidate_row['GRADE']]}</span><br><br>
                예상 전세가 ({preferred_pyeong}평) {format_currency_krw(candidate_row['ESTIMATED_TOTAL_JEONSE'])}<br>
                전세가율 {candidate_row['JEONSE_RATE']:.1f}%
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
                <div class="s-card-value">현재 후보가 이미 좋아요</div>
                <div class="s-card-body">
                    이 조건에서는 현재 후보보다 더 나은 대안이 보이지 않습니다.<br>
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
                    <span class="s-card-pill">{best_alternative['GRADE']}등급 · {GRADE_MEANINGS[best_alternative['GRADE']]}</span><br><br>
                    예상 전세가 ({preferred_pyeong}평) {format_currency_krw(best_alternative['ESTIMATED_TOTAL_JEONSE'])}<br>
                    전세가율 {best_alternative['JEONSE_RATE']:.1f}%
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────
# 2-pass lazy AI 로딩
# - 1차 렌더: 빠른 콘텐츠(추천 결과, 비교 분석, 시장 랭킹, 선택동 상세)는 즉시 표시
#             AI 부분은 로딩 카드만 보여줌 → 사용자가 추천 페이지를 바로 볼 수 있음
# - 2차 렌더: 백그라운드 AI 호출 완료 후 st.rerun()으로 자동 새로고침
#             캐시에서 즉시 응답하므로 두 번째 렌더는 빠름
# - 같은 동/성향이면 캐시 hit으로 첫 렌더부터 ai_warm = True
# ──────────────────────────────────────────────────────────────────────
grade_label = f"{candidate_row['GRADE']} · {GRADE_MEANINGS[candidate_row['GRADE']]}"
cortex_prompt = build_candidate_ai_prompt(
    selected_area=selected_area,
    grade_label=grade_label,
    candidate_row=candidate_row,
    survey_result=survey_result,
    history_snapshot=market_snapshot,
)
_ai_cache_key = f"{selected_area}|{preferred_pyeong}|{survey_result['profile']}"

# AI 결과가 이번 입력 조합에 대해 이미 캐시되어 있는지 추적
_ai_warm = st.session_state.get("_ai_warm_key") == _ai_cache_key

# 빠른 데이터 (랭킹)는 항상 즉시 호출
market_rankings = load_market_rankings(session)

if _ai_warm:
    # 캐시 hit — 즉시 응답 (실제로는 ms 단위)
    analysis = get_ai_structured_analysis(session, _ai_cache_key, cortex_prompt)
else:
    # 1차 렌더: 빈 placeholder
    analysis = None
briefing = None

tabs = st.tabs(["개인화 추천", "후보 진단", "비교 분석", "시장 흐름", "AI 질문"])

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
            "safest": ("가장 안전한 대안", "전세가율이 가장 낮은 후보"),
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
                    import re as _re
                    points = build_card_description(alt_row, candidate_row)
                    # 마크다운 **bold** → HTML <strong>
                    _bold_pattern = r"\*\*(.+?)\*\*"
                    _bold_replace = r"<strong>\1</strong>"
                    points_html = "".join(
                        "<li>" + _re.sub(_bold_pattern, _bold_replace, p) + "</li>"
                        for p in points
                    )
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
            x=alt.X("RECOMMENDATION_SCORE:Q", title="나의 적합도", scale=alt.Scale(domain=[0, 100])),
            y=alt.Y("AREA_LABEL:N", sort="-x", title="동네"),
            color=alt.condition(alt.datum.IS_CANDIDATE, alt.value("#ef6c00"), alt.value("#2d7a52")),
            tooltip=[
                alt.Tooltip("AREA_LABEL:N", title="동네"),
                alt.Tooltip("RECOMMENDATION_SCORE:Q", title="추천점수", format=".1f"),
                alt.Tooltip("GRADE:N", title="안전등급"),
                alt.Tooltip("MARKET_SCORE:Q", title="시장 점수", format=".1f"),
                alt.Tooltip("STRUCTURAL_SCORE:Q", title="구조 점수", format=".1f"),
                alt.Tooltip("COMPARISON_LABEL:N", title="비교 해석"),
                alt.Tooltip("JEONSE_RATE:Q", title="전세가율(%)", format=".1f"),
            ],
        )
        st.altair_chart(chart, width="stretch")

    # 안전등급 가이드
    st.caption(
        "💡 **안전등급 (A/B/C/D)**: 동네 자체의 객관 안전도. "
        "**나의 적합도**: 안전등급 + 회원님 보증금·평형·성향까지 반영한 종합 점수."
    )

    medal_map = {1: "🥇", 2: "🥈", 3: "🥉"}
    recommendation_table = filtered_df.head(5)[
        [
            "RECOMMENDATION_RANK",
            "AREA_LABEL",
            "GRADE",
            "RECOMMENDATION_SCORE",
            "MARKET_SCORE",
            "STRUCTURAL_SCORE",
            "MARKET_STRUCTURE_GAP",
            "COMPARISON_LABEL",
        ]
    ].copy()
    recommendation_table["RECOMMENDATION_RANK"] = recommendation_table["RECOMMENDATION_RANK"].apply(
        lambda r: f"{medal_map.get(int(r), '')} {int(r)}위"
    )
    recommendation_table["GRADE"] = recommendation_table["GRADE"].apply(
        lambda g: f"{g}등급 ({GRADE_MEANINGS.get(g, '')})"
    )
    recommendation_table = recommendation_table.rename(
        columns={
            "RECOMMENDATION_RANK": "순위",
            "AREA_LABEL": "동네",
            "GRADE": "안전등급",
            "RECOMMENDATION_SCORE": "나의 적합도",
            "MARKET_SCORE": "시장 점수",
            "STRUCTURAL_SCORE": "구조 점수",
            "MARKET_STRUCTURE_GAP": "점수 차이",
            "COMPARISON_LABEL": "비교 해석",
        }
    )
    recommendation_table["나의 적합도"] = recommendation_table["나의 적합도"].map(lambda v: f"{v:.1f}점")
    recommendation_table["시장 점수"] = recommendation_table["시장 점수"].map(lambda v: f"{v:.1f}점")
    recommendation_table["구조 점수"] = recommendation_table["구조 점수"].apply(
        lambda v: f"{float(v):.1f}점" if pd.notna(v) else "-"
    )
    recommendation_table["점수 차이"] = recommendation_table["점수 차이"].apply(
        lambda v: f"{float(v):+.1f}점" if pd.notna(v) else "-"
    )
    st.dataframe(recommendation_table, width="stretch", hide_index=True)
    st.caption("시장 점수는 국토부 실거래 기반, 구조 점수는 리치고+SPH/Grandata 기반 비교 지표입니다. 현재는 비교용으로만 사용하며 최종 추천 점수에는 직접 반영하지 않습니다.")

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
    # ── 헤더: 동네명 + 등급 ──
    st.markdown(
        f'<div style="display:flex;align-items:baseline;justify-content:space-between;'
        f'margin-bottom:0.8rem;flex-wrap:wrap;gap:0.5rem">'
        f'<div>'
        f'<div style="font-size:0.78rem;color:#999;text-transform:uppercase;letter-spacing:0.05em">현재 후보</div>'
        f'<h2 style="margin:0;color:#1a1a1a;font-size:1.6rem">{selected_area}</h2>'
        f'</div>'
        f'<div style="display:flex;gap:0.4rem;align-items:center">'
        f'<span style="background:#e8f2ec;color:#2d7a52;padding:6px 14px;border-radius:999px;'
        f'font-weight:700;font-size:0.85rem">{grade_label}</span>'
        f'<span style="background:#f5f7fa;color:#555;padding:6px 14px;border-radius:999px;'
        f'font-weight:600;font-size:0.85rem">{preferred_pyeong}평 기준</span>'
        f'</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── 핵심 지표 3개 (한 줄) ──
    diag_1, diag_2, diag_3 = st.columns(3)
    diag_1.metric(
        "전세가율", f"{candidate_row['JEONSE_RATE']:.1f}%",
        help="전세가가 매매가의 몇 %인지. 낮을수록 안전 (보증금 회수 쉬움)",
    )
    diag_2.metric(
        f"예상 전세가 ({preferred_pyeong}평)",
        format_currency_krw(candidate_row["ESTIMATED_TOTAL_JEONSE"]),
        help="이 동네 해당 평형대 최근 거래 기반 추정",
    )
    diag_3.metric(
        "AI 하락 위험도",
        f"{candidate_row.get('ML_RISK_SCORE', 50):.0f}%",
        help="AI가 예측한 6개월 내 전세가 5%+ 하락 확률",
    )

    market_score = float(candidate_row.get("MARKET_SCORE", candidate_row.get("TOTAL_SCORE", 0)))
    structural_score = pd.to_numeric(pd.Series([candidate_row.get("STRUCTURAL_SCORE")]), errors="coerce").iloc[0]
    score_gap = pd.to_numeric(pd.Series([candidate_row.get("MARKET_STRUCTURE_GAP")]), errors="coerce").iloc[0]

    cmp_top_left, cmp_top_right = st.columns(2)
    cmp_top_left.metric(
        "시장 점수",
        f"{market_score:.1f}점",
        help="국토부 실거래 기반 전세가율, 거래 활발도, 변동성으로 만든 시장 점수",
    )
    cmp_top_right.metric(
        "구조 점수",
        f"{float(structural_score):.1f}점" if pd.notna(structural_score) else "-",
        help="리치고 구조 신호와 SPH/Grandata 생활·재무 신호를 합친 비교용 구조 점수",
    )
    st.metric(
        "점수 차이",
        f"{abs(float(score_gap)):.1f}점" if pd.notna(score_gap) else "-",
        delta=str(candidate_row.get("GAP_DIRECTION_LABEL", "비교 데이터 부족")),
        delta_color="off",
        help="구조 점수 - 시장 점수. 두 점수의 방향 차이를 보여줍니다.",
    )

    comparison_label = str(candidate_row.get("COMPARISON_LABEL", "구조 비교 데이터 부족"))
    comparison_detail = str(candidate_row.get("COMPARISON_DETAIL", ""))
    structure_data_label = str(candidate_row.get("STRUCTURE_DATA_LABEL", "비교 데이터 부족"))
    st.markdown(
        f'<div style="background:linear-gradient(135deg,#f9fbfd 0%,#eef4f7 100%);'
        f'border:1px solid #d8e4ea;border-radius:14px;padding:1rem 1.2rem;margin-top:0.8rem">'
        f'<div style="display:flex;align-items:center;justify-content:space-between;gap:0.8rem;flex-wrap:wrap">'
        f'<div style="display:flex;align-items:center;gap:8px">'
        f'{_icon("compare_arrows", 20, "#1565c0")}'
        f'<span style="font-size:0.82rem;font-weight:700;color:#1565c0;letter-spacing:0.03em">시장 점수 vs 구조 점수</span>'
        f'</div>'
        f'<span style="background:#fff;border:1px solid #c7d8e5;color:#204b63;padding:4px 10px;border-radius:999px;font-size:0.76rem;font-weight:600">{comparison_label}</span>'
        f'</div>'
        f'<div style="margin-top:0.7rem;color:#24343f;font-size:0.92rem;line-height:1.7">{comparison_detail}</div>'
        f'<div style="margin-top:0.55rem;color:#6a7a86;font-size:0.76rem">구조 비교 구성: {structure_data_label}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    if bool(candidate_row.get("HAS_STRUCTURE_SIGNAL", False)):
        st.markdown("##### 구조 점수 구성")
        comp_top_1, comp_top_2 = st.columns(2)
        comp_top_1.metric(
            "리치고 구조",
            f"{float(candidate_row['RICHGO_STRUCTURE_SCORE']):.1f}점" if pd.notna(candidate_row.get("RICHGO_STRUCTURE_SCORE")) else "-",
            help="리치고의 지하철 거리와 순이동 신호를 합친 구조 점수",
        )
        comp_top_2.metric(
            "SPH 활동",
            f"{float(candidate_row['SPH_ACTIVITY_SCORE']):.1f}점" if pd.notna(candidate_row.get("SPH_ACTIVITY_SCORE")) else "-",
            help="SPH/Grandata의 근무·방문·거주 인구 신호를 합친 점수",
        )
        comp_bottom_1, comp_bottom_2 = st.columns(2)
        comp_bottom_1.metric(
            "SPH 재무",
            f"{float(candidate_row['SPH_FINANCE_SCORE']):.1f}점" if pd.notna(candidate_row.get("SPH_FINANCE_SCORE")) else "-",
            help="SPH/Grandata의 소득·자산 신호를 합친 점수",
        )
        comp_bottom_2.metric(
            "시세 일치도",
            f"{float(candidate_row['RATE_CONSISTENCY_SCORE']):.1f}점" if pd.notna(candidate_row.get("RATE_CONSISTENCY_SCORE")) else "-",
            help="국토부 전세가율과 리치고 전세가율이 얼마나 비슷한지 보여주는 점수",
        )
        st.caption("구조 비교 점수는 리치고와 SPH/Grandata 신호를 재정규화해 만든 비교 지표입니다. 현재는 해석용으로만 사용하고, 최종 추천 점수에는 직접 반영하지 않습니다.")
    else:
        st.caption("리치고/SPH 구조 신호가 부족해 현재 후보는 국토부 시장 점수 중심으로 진단하고 있습니다.")

    # ── AI 분석 (전체 너비, 총평이 메인) ──
    # 분석은 탭 렌더 전에 미리 호출되어 `analysis` 변수에 저장됨
    st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)

    if analysis and analysis.get("summary"):
        confidence = analysis.get("confidence", "medium")
        conf_label = {"high": "신뢰도 높음", "medium": "신뢰도 보통", "low": "신뢰도 낮음"}.get(confidence, "신뢰도 보통")
        conf_color = {"high": "#2e7d32", "medium": "#f57f17", "low": "#c62828"}.get(confidence, "#666")

        # 헤더 + 총평 (단일 hero 카드)
        st.markdown(
            f'<div style="background:linear-gradient(135deg,#f7faf8 0%,#eef5f1 100%);'
            f'border:1px solid #d4e4dc;border-radius:14px;padding:1.3rem 1.5rem;margin-bottom:0.8rem">'
            f'<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.5rem">'
            f'<div style="display:flex;align-items:center;gap:8px">'
            f'{_icon("auto_awesome", 22, "#2d7a52")}'
            f'<span style="font-size:0.78rem;font-weight:700;color:#2d7a52;'
            f'text-transform:uppercase;letter-spacing:0.05em">Snowflake Cortex AI 분석</span>'
            f'</div>'
            f'<span style="background:#fff;border:1px solid {conf_color}33;color:{conf_color};'
            f'padding:3px 10px;border-radius:999px;font-size:0.7rem;font-weight:600">{conf_label}</span>'
            f'</div>'
            f'<div style="font-size:1.1rem;font-weight:600;color:#1a1a1a;line-height:1.6">'
            f'{analysis.get("summary", "")}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

        # 강점 / 주의 / 추천 행동 (3열)
        strengths = analysis.get("strengths", [])
        risks = analysis.get("risks", [])
        action = analysis.get("recommended_action", "")

        col_s, col_r, col_a = st.columns(3)
        with col_s:
            items = "".join(f'<li style="margin-bottom:6px;line-height:1.5">{s}</li>' for s in strengths[:3])
            st.markdown(
                f'<div style="background:#fff;border:1px solid #c8e0d2;border-left:4px solid #4caf50;'
                f'border-radius:10px;padding:1rem 1.1rem;height:100%">'
                f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:0.5rem">'
                f'{_icon("check_circle", 18, "#2e7d32")}'
                f'<span style="font-weight:700;color:#2e7d32;font-size:0.88rem">강점</span>'
                f'</div>'
                f'<ul style="padding-left:1.1rem;margin:0;color:#444;font-size:0.83rem">{items}</ul>'
                f'</div>',
                unsafe_allow_html=True,
            )
        with col_r:
            items = "".join(f'<li style="margin-bottom:6px;line-height:1.5">{r}</li>' for r in risks[:3])
            st.markdown(
                f'<div style="background:#fff;border:1px solid #f0d4c4;border-left:4px solid #ff9800;'
                f'border-radius:10px;padding:1rem 1.1rem;height:100%">'
                f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:0.5rem">'
                f'{_icon("warning", 18, "#e65100")}'
                f'<span style="font-weight:700;color:#e65100;font-size:0.88rem">주의</span>'
                f'</div>'
                f'<ul style="padding-left:1.1rem;margin:0;color:#444;font-size:0.83rem">{items}</ul>'
                f'</div>',
                unsafe_allow_html=True,
            )
        with col_a:
            st.markdown(
                f'<div style="background:#fff;border:1px solid #d5dde6;border-left:4px solid #1976d2;'
                f'border-radius:10px;padding:1rem 1.1rem;height:100%">'
                f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:0.5rem">'
                f'{_icon("lightbulb", 18, "#1565c0")}'
                f'<span style="font-weight:700;color:#1565c0;font-size:0.88rem">추천 행동</span>'
                f'</div>'
                f'<div style="color:#444;font-size:0.83rem;line-height:1.6">{action}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
    elif not _ai_warm:
        # 1차 렌더 — AI 분석 로딩 중
        st.markdown(
            _ai_loading_card_html(
                f"AI가 {selected_area}의 전세 환경을 살펴보고 있습니다. 잠시만 기다려 주세요."
            ),
            unsafe_allow_html=True,
        )
    else:
        # Fallback (AI 호출은 끝났는데 결과가 비어있는 경우)
        st.info(build_candidate_summary(candidate_row, survey_result))

    # ── 차원별 점수 차트 ──
    st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)
    st.markdown("#### 안전점수 구성")

    dim_df = pd.DataFrame(
        {
            "항목": [DIMENSION_LABELS[key] for key in DIMENSION_LABELS],
            "점수": [candidate_row[key] for key in DIMENSION_LABELS],
        }
    )
    st.altair_chart(
        alt.Chart(dim_df).mark_bar(cornerRadiusEnd=8, size=18).encode(
            x=alt.X("점수:Q", scale=alt.Scale(domain=[0, 100]), title=""),
            y=alt.Y("항목:N", sort="-x", title=""),
            color=alt.Color("점수:Q", legend=None,
                            scale=alt.Scale(scheme="greens", domain=[0, 100])),
            tooltip=["항목", alt.Tooltip("점수:Q", format=".0f")],
        ).properties(height=120),
        width="stretch",
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
                width="stretch",
            )
            chart_right.altair_chart(
                alt.Chart(combined_history).mark_line(point=True).encode(
                    x=alt.X("YYYYMMDD:T", title="기간", axis=alt.Axis(format="%Y-%m")),
                    y=alt.Y("JEONSE_RATIO:Q", title="전세가율 (%)"),
                    color=alt.Color("AREA_LABEL:N", title="동네"),
                    tooltip=["AREA_LABEL", "월:N", alt.Tooltip("JEONSE_RATIO:Q", format=".1f", title="%")],
                ),
                width="stretch",
            )

        compare_table = compare_rows[
            [
                "AREA_LABEL",
                "GRADE",
                "RECOMMENDATION_SCORE",
                "JEONSE_RATE",
                "ESTIMATED_TOTAL_JEONSE",
            ]
        ].copy()
        compare_table = compare_table.rename(
            columns={
                "AREA_LABEL": "동네",
                "GRADE": "안전등급",
                "RECOMMENDATION_SCORE": "나의 적합도",
                "JEONSE_RATE": "전세가율",
                "ESTIMATED_TOTAL_JEONSE": "예상 전세가",
            }
        )
        compare_table["예상 전세가"] = compare_table["예상 전세가"].map(format_currency_krw)
        compare_table["전세가율"] = compare_table["전세가율"].map(lambda v: f"{v:.1f}%")
        st.dataframe(compare_table, width="stretch", hide_index=True)

with tabs[3]:
    st.subheader("시장 흐름")

    if "_market_briefing_cache" not in st.session_state:
        st.session_state["_market_briefing_cache"] = {}
    briefing_cache = dict(st.session_state.get("_market_briefing_cache", {}))
    briefing = briefing_cache.get(survey_result["profile"])

    def _fmt_pyeong_price(val):
        """평당가(만원 단위)를 자연스럽게 표시."""
        if not val:
            return "-"
        v = float(val)
        if v >= 10000:
            return f"{v/10000:.1f}억/평"
        return f"{v:,.0f}만/평"

    # ══════════════════════════════════════════════════════════════════
    # SECTION 1 — 서울 시장 전체 (AI_AGG 브리핑 + TOP 리스트)
    # ══════════════════════════════════════════════════════════════════
    st.markdown(
        f'<div style="display:flex;align-items:center;gap:8px;margin:0.2rem 0 0.8rem">'
        f'{_icon("public", 22, "#1a2740")}'
        f'<span style="font-size:1.05rem;font-weight:700;color:#1a2740">서울 전체 시장 한눈에</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    briefing_col, briefing_meta = st.columns([1, 3])
    with briefing_col:
        if st.button(
            "서울 시장 AI 브리핑 보기" if briefing is None else "서울 시장 AI 브리핑 새로고침",
            key="market_briefing_button",
            width="stretch",
        ):
            with st.spinner("서울 시장 AI 브리핑을 생성하고 있습니다..."):
                briefing_cache[survey_result["profile"]] = load_market_briefing(session, survey_result["profile"])
            st.session_state["_market_briefing_cache"] = briefing_cache
            st.rerun()
  

    import html as _html
    if briefing is None:
        st.info("버튼을 누르면 서울 전체 시장 AI 브리핑을 불러옵니다.")
        st.markdown('<div style="height:0.6rem"></div>', unsafe_allow_html=True)

    headline = (briefing or {}).get("headline", "") or ""
    headline = headline.strip()
    market_mood = (briefing or {}).get("market_mood", "") or ""
    market_mood = market_mood.strip()
    watch_areas = (briefing or {}).get("watch_areas", []) or []
    opp_areas = (briefing or {}).get("opportunity_areas", []) or []
    user_action = ((briefing or {}).get("user_action") or "").strip()
    profile_for_briefing = (briefing or {}).get("user_profile", survey_result["profile"])

    if market_mood and not market_mood.startswith("__ERROR__"):
        # 헤드라인 + 분위기 (히어로)
        hero_html = (
            f'<div style="background:linear-gradient(135deg,#f7faf8 0%,#eef5f1 100%);'
            f'border:1px solid #d4e4dc;border-radius:14px;padding:1.6rem 1.7rem 1.4rem;'
            f'margin-bottom:1.2rem">'
            f'<div style="display:flex;align-items:center;justify-content:space-between;'
            f'gap:0.5rem;margin-bottom:0.8rem;flex-wrap:wrap">'
            f'<div style="display:flex;align-items:center;gap:8px">'
            f'{_icon("auto_awesome", 22, "#2d7a52")}'
            f'<span style="font-size:0.78rem;font-weight:700;color:#2d7a52;'
            f'text-transform:uppercase;letter-spacing:0.05em">'
            f'이달의 서울 전세시장 AI 브리핑</span>'
            f'</div>'
            f'<span style="background:#fff;border:1px solid #d4e4dc;color:#3a5347;'
            f'padding:3px 11px;border-radius:999px;font-size:0.72rem;font-weight:600">'
            f'{_html.escape(profile_for_briefing)} 맞춤</span>'
            f'</div>'
        )
        if headline:
            hero_html += (
                f'<h2 style="margin:0.2rem 0 0.7rem;color:#1a1a1a;font-size:1.45rem;'
                f'font-weight:700;letter-spacing:-0.5px;line-height:1.3">'
                f'{_html.escape(headline)}</h2>'
            )
        hero_html += (
            f'<div style="font-size:1.0rem;line-height:1.75;color:#1a1a1a;font-weight:500">'
            f'{_html.escape(market_mood)}</div>'
            f'</div>'
        )
        st.markdown(hero_html, unsafe_allow_html=True)

        # 주의 / 기회 2분할
        def _section_card(title: str, icon: str, accent: str, items: list, empty_msg: str) -> str:
            inner = ""
            if items:
                for it in items[:2]:
                    area = _html.escape(str(it.get("area", "")).strip())
                    why = _html.escape(str(it.get("why", "")).strip())
                    if not area:
                        continue
                    inner += (
                        f'<div style="background:#fff;border:1px solid #ecefef;border-radius:10px;'
                        f'padding:0.85rem 1rem;margin-top:0.55rem">'
                        f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:0.35rem">'
                        f'<span style="display:inline-block;width:6px;height:6px;border-radius:50%;'
                        f'background:{accent}"></span>'
                        f'<span style="font-size:0.92rem;font-weight:700;color:#1a1a1a">{area}</span>'
                        f'</div>'
                        f'<div style="font-size:0.87rem;color:#4a4a4a;line-height:1.55">{why}</div>'
                        f'</div>'
                    )
            if not inner:
                inner = (
                    f'<div style="color:#999;font-size:0.85rem;padding:0.6rem 0">{empty_msg}</div>'
                )

            return (
                f'<div style="background:#fafbfc;border:1px solid #eef0f3;border-radius:12px;'
                f'padding:1.1rem 1.2rem;height:100%">'
                f'<div style="display:flex;align-items:center;gap:6px">'
                f'{_icon(icon, 18, accent)}'
                f'<span style="font-size:0.92rem;font-weight:700;color:{accent};'
                f'text-transform:uppercase;letter-spacing:0.04em">{title}</span>'
                f'</div>'
                f'{inner}'
                f'</div>'
            )

        col_w, col_o = st.columns(2)
        with col_w:
            st.markdown(
                _section_card("이번 달 주의 신호", "warning", "#c62828", watch_areas, "특별한 주의 신호 없음"),
                unsafe_allow_html=True,
            )
        with col_o:
            st.markdown(
                _section_card("지금 눈여겨볼 곳", "lightbulb", "#1565c0", opp_areas, "주목할 기회 없음"),
                unsafe_allow_html=True,
            )

        # 사용자 액션
        if user_action:
            st.markdown(
                f'<div style="background:#1a2740;border-radius:12px;padding:1.1rem 1.3rem;'
                f'margin-top:1rem;display:flex;align-items:flex-start;gap:10px">'
                f'{_icon("rocket_launch", 20, "#ffd166")}'
                f'<div>'
                f'<div style="font-size:0.7rem;font-weight:700;color:#ffd166;'
                f'text-transform:uppercase;letter-spacing:0.06em;margin-bottom:0.25rem">'
                f'{_html.escape(profile_for_briefing)} · 지금 할 일</div>'
                f'<div style="font-size:0.97rem;color:#fff;line-height:1.6;font-weight:500">'
                f'{_html.escape(user_action)}</div>'
                f'</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

        st.markdown(
            f'<div style="margin-top:0.8rem;font-size:0.72rem;color:#7a8b82;'
            f'display:flex;align-items:center;gap:5px">'
            f'{_icon("bolt", 13, "#2d7a52")}'
            f'<span>Powered by Snowflake Cortex AI_AGG · 288개 동 단일 쿼리 요약 · 성향별 맞춤 분석 · 1시간 캐시</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
        st.markdown('<div style="height:1.4rem"></div>', unsafe_allow_html=True)
    elif market_mood.startswith("__ERROR__"):
        st.caption(f"AI 브리핑을 불러오지 못했습니다 ({market_mood.replace('__ERROR__:', '')[:80]})")

    # ── TOP 리스트 3분할 (위험 / 안전 / 활발) ──
    def _render_top_list(title: str, icon: str, accent: str, items: list, kind: str):
        if not items:
            return (
                f'<div style="background:#fafbfc;border:1px solid #eef0f3;border-radius:12px;'
                f'padding:1rem 1.1rem;height:100%">'
                f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:0.6rem">'
                f'{_icon(icon, 18, accent)}'
                f'<span style="font-size:0.92rem;font-weight:700;color:#1a1a1a">{title}</span>'
                f'</div>'
                f'<div style="color:#999;font-size:0.85rem">데이터 없음</div>'
                f'</div>'
            )

        rows_html = ""
        for i, item in enumerate(items[:5], 1):
            area = item["area"]
            grade = item["grade"] or "-"
            if kind == "risk":
                metric_label = f"전세가율 {item['metric_a']}%"
            elif kind == "safe":
                metric_label = f"안전점수 {item['metric_a']}"
            else:  # active
                metric_label = f"거래 {int(item['metric_a'])}건"

            rows_html += (
                f'<div style="display:flex;align-items:center;justify-content:space-between;'
                f'padding:0.55rem 0;border-bottom:1px solid #f0f2f5">'
                f'<div style="display:flex;align-items:center;gap:8px;min-width:0">'
                f'<span style="font-size:0.78rem;color:#999;width:14px;flex-shrink:0">{i}</span>'
                f'<span style="font-size:0.88rem;color:#1a1a1a;font-weight:600;'
                f'white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{area}</span>'
                f'<span style="font-size:0.7rem;font-weight:700;color:{accent};'
                f'background:{accent}1A;padding:1px 6px;border-radius:8px;flex-shrink:0">{grade}</span>'
                f'</div>'
                f'<span style="font-size:0.78rem;color:#666;flex-shrink:0;margin-left:8px">{metric_label}</span>'
                f'</div>'
            )

        return (
            f'<div style="background:#fff;border:1px solid #eef0f3;border-radius:12px;'
            f'padding:1.1rem 1.2rem;height:100%;box-shadow:0 1px 3px rgba(0,0,0,0.02)">'
            f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:0.7rem">'
            f'{_icon(icon, 18, accent)}'
            f'<span style="font-size:0.92rem;font-weight:700;color:#1a1a1a">{title}</span>'
            f'</div>'
            f'{rows_html}'
            f'</div>'
        )

    col_r, col_s, col_a = st.columns(3)
    with col_r:
        st.markdown(
            _render_top_list("주의가 필요한 동 TOP", "warning", "#c62828", market_rankings.get("risk", []), "risk"),
            unsafe_allow_html=True,
        )
    with col_s:
        st.markdown(
            _render_top_list("안전한 동 TOP", "verified", "#2e7d32", market_rankings.get("safe", []), "safe"),
            unsafe_allow_html=True,
        )
    with col_a:
        st.markdown(
            _render_top_list("거래 활발한 동 TOP", "trending_up", "#1565c0", market_rankings.get("active", []), "active"),
            unsafe_allow_html=True,
        )

    st.markdown('<div style="height:1.8rem"></div>', unsafe_allow_html=True)
    st.markdown(
        '<div style="border-top:1px dashed #e0e3e8;margin-bottom:1.4rem"></div>',
        unsafe_allow_html=True,
    )

    # ══════════════════════════════════════════════════════════════════
    # SECTION 2 — 선택한 동 상세
    # ══════════════════════════════════════════════════════════════════
    st.markdown(
        f'<div style="display:flex;align-items:center;gap:8px;margin:0.2rem 0 0.9rem">'
        f'{_icon("location_on", 22, "#1a2740")}'
        f'<span style="font-size:1.05rem;font-weight:700;color:#1a2740">{selected_area} 상세 분석</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    if selected_history_df.empty:
        st.info("선택한 후보의 시세 데이터가 없습니다.")
    else:
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
        st.markdown(
            f'<div style="margin:0.9rem 0 0.4rem;font-size:0.92rem;font-weight:600;color:#444">'
            f'{selected_area} 매매가·전세가 추이</div>',
            unsafe_allow_html=True,
        )
        st.altair_chart(make_history_chart(selected_history_df), width="stretch")

    # ── 최근 실거래 내역 ──
    st.markdown('<div style="height:1.6rem"></div>', unsafe_allow_html=True)
    st.markdown(
        f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:0.5rem">'
        f'{_icon("receipt_long", 20, "#1a1a1a")}'
        f'<span style="font-size:0.98rem;font-weight:700;color:#1a1a1a">{selected_area} 최근 실거래</span>'
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
        st.dataframe(raw_tx, width="stretch", hide_index=True, height=320)
        st.caption(f"총 {len(raw_tx)}건 · 매매=매매가, 전세=보증금 (단위: 만원)")

    # ── 단지별 비교 ──
    st.markdown('<div style="height:1.6rem"></div>', unsafe_allow_html=True)
    st.markdown(
        f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:0.4rem">'
        f'{_icon("apartment", 20, "#1a1a1a")}'
        f'<span style="font-size:0.98rem;font-weight:700;color:#1a1a1a">{selected_area} 단지별 시세</span>'
        f"</div>"
        f'<p style="color:#888;font-size:0.82rem;margin:0 0 0.5rem">'
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
        for col in ["매매건수", "전세건수"]:
            if col in complex_df.columns:
                complex_df[col] = complex_df[col].apply(lambda v: f"{int(v):,}" if pd.notna(v) else "-")
        if "전세가율(%)" in complex_df.columns:
            complex_df["전세가율(%)"] = complex_df["전세가율(%)"].apply(lambda v: f"{v:.1f}" if pd.notna(v) else "-")
        if "주요면적(m²)" in complex_df.columns:
            complex_df["주요면적(m²)"] = complex_df["주요면적(m²)"].apply(lambda v: f"{v:.0f}" if pd.notna(v) else "-")
        st.dataframe(complex_df, width="stretch", hide_index=True)
        st.caption(f"총 {len(complex_df)}개 단지")

with tabs[4]:
    # ── AI 질문 (Cortex Analyst) ──
    st.markdown(
        f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:0.3rem">'
        f'{_icon("psychology", 26, "#2d7a52")}'
        f'<span style="font-size:1.3rem;font-weight:700;color:#1a1a1a">AI에게 물어보기</span>'
        f'</div>'
        f'<p style="color:#666;font-size:0.9rem;margin:0 0 1rem;line-height:1.5">'
        f'궁금한 질문을 클릭하면 AI가 분석해서 답해드려요.'
        f'</p>',
        unsafe_allow_html=True,
    )

    # 추천 질문 카드
    preset_questions = [
        ("🏆", "가장 안전한 동 TOP 10", "서울에서 가장 안전한 동 10개를 안전점수가 높은 순으로 알려줘"),
        ("📍", f"{selected_sgg} 안전 동", f"{selected_sgg}에서 안전등급 A 또는 B인 동을 알려줘"),
        ("📊", "구별 평균 안전점수", "구별 평균 안전점수가 높은 순서로 보여줘"),
        ("💰", "전세가율 낮은 동", "전세가율이 30% 이하인 동을 전세가율 낮은 순으로 알려줘"),
        ("📈", "거래 활발한 동", "최근 거래가 가장 활발한 동 10개를 알려줘"),
        ("🤖", "AI 위험 예측 동", "AI가 가장 위험하다고 예측한 동 10개를 알려줘"),
        ("🥇", "구별 1등 동", "각 구에서 안전점수가 가장 높은 동을 알려줘"),
        ("⚠️", "깡통전세 위험", "전세가율이 80% 이상인 동을 알려줘"),
    ]

    if "_analyst_question" not in st.session_state:
        st.session_state["_analyst_question"] = None

    cols_per_row = 4
    rows = [preset_questions[i:i + cols_per_row] for i in range(0, len(preset_questions), cols_per_row)]
    for row in rows:
        cols = st.columns(len(row))
        for i, (icon, title, q) in enumerate(row):
            with cols[i]:
                if st.button(
                    f"{icon}\n\n**{title}**",
                    key=f"_analyst_q_{title}",
                    width="stretch",
                ):
                    st.session_state["_analyst_question"] = q

    st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)

    selected_question = st.session_state.get("_analyst_question")
    if selected_question:
        st.markdown(
            f'<div style="background:#f0f7f3;border-left:4px solid #2d7a52;padding:0.8rem 1.2rem;'
            f'border-radius:8px;margin-bottom:0.8rem">'
            f'<div style="font-size:0.75rem;color:#888;font-weight:600;text-transform:uppercase;letter-spacing:0.05em;'
            f'margin-bottom:0.3rem">회원님의 질문</div>'
            f'<div style="font-size:1rem;color:#1a1a1a;font-weight:600">{selected_question}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

        _analyst_slot = st.empty()
        _analyst_slot.markdown(
            _ai_loading_card_html("Cortex Analyst가 질문을 SQL로 변환 중이에요..."),
            unsafe_allow_html=True,
        )
        result = run_analyst_question(session, selected_question)
        _analyst_slot.empty()

        if "error" in result:
            st.error(f"분석 실패: {result['error']}")
        else:
            df = result.get("df")

            if df is not None and not df.empty:
                # 컬럼명을 한글로 매핑
                column_map = {
                    "SGG": "구",
                    "EMD": "동",
                    "GRADE": "안전등급",
                    "TOTAL_SCORE": "안전점수",
                    "JEONSE_RATE": "전세가율 (%)",
                    "JEONSE_LATEST": "평당 전세가 (만원)",
                    "MEME_LATEST": "평당 매매가 (만원)",
                    "JEONSE_DROP_PCT": "전세 변동률 (%)",
                    "NET_MIG": "최근 거래 건수",
                    "TX_COUNT": "최근 거래 건수",
                    "SUBWAY_DIST": "변동성",
                    "S_RATE": "전세가율 점수",
                    "S_MIG": "거래 활발도",
                    "S_SUB": "가격 안정성",
                    "ML_RISK_SCORE": "AI 하락 위험도 (%)",
                    "ML_DROP_PROB": "AI 하락 확률",
                    "AVG_SCORE": "평균 안전점수",
                    "AREA_COUNT": "동 개수",
                    "HUG_RATE": "HUG 사고율 (%)",
                }
                df_display = df.rename(columns=column_map)
                # 안전등급 보기 좋게
                if "안전등급" in df_display.columns:
                    df_display["안전등급"] = df_display["안전등급"].apply(
                        lambda g: f"{g}등급 ({GRADE_MEANINGS.get(g, '')})" if pd.notna(g) and g in GRADE_MEANINGS else g
                    )

                st.markdown(
                    f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:0.4rem">'
                    f'{_icon("insights", 18, "#2d7a52")}'
                    f'<span style="font-size:0.95rem;font-weight:700;color:#1a1a1a">결과</span>'
                    f'<span style="font-size:0.78rem;color:#888">총 {len(df)}건</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
                st.dataframe(df_display, width="stretch", hide_index=True)
            else:
                st.info("결과가 없습니다.")
    else:
        st.info("👆 위 추천 질문 중 하나를 클릭하면 AI가 분석을 시작합니다.")

# ──────────────────────────────────────────────────────────────────────
# 2-pass lazy AI 로딩의 2단계
# 모든 탭이 1차 렌더되어 사용자가 추천 결과를 볼 수 있는 상태에서,
# 후보 진단 AI만 백그라운드로 미리 호출하고 완료되면 st.rerun()으로 새로고침.
# 시장 브리핑은 시장 흐름 탭에서 버튼을 눌렀을 때만 실행한다.
# ──────────────────────────────────────────────────────────────────────
if not _ai_warm:
    with ThreadPoolExecutor(max_workers=1) as _ai_pool:
        _f_a = _ai_pool.submit(get_ai_structured_analysis, session, _ai_cache_key, cortex_prompt)
        _f_a.result()
    st.session_state["_ai_warm_key"] = _ai_cache_key
    st.rerun()

# ── 안내 문구 ──
st.divider()
st.caption(
    "본 서비스의 추천 결과는 공개 데이터 기반의 참고 정보이며, "
    "법률·세무·투자 판단을 대체하지 않습니다. "
    "실제 계약 전 반드시 전문가 상담을 받으시기 바랍니다."
)
