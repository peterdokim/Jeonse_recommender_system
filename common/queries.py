import json

import pandas as pd
import streamlit as st
from snowflake.snowpark import Session

SCORE_TABLE = "HACKATHON_APP.RESILIENCE.JEONSE_SAFETY_SCORE"
ENRICHED_SCORE_TABLE = "HACKATHON_APP.RESILIENCE.JEONSE_SCORE_ENRICHED"
BASE_VIEW = "HACKATHON_APP.RESILIENCE.RESILIENCE_BASE"


@st.cache_data(show_spinner=False)
def load_latest_market_snapshot(_session: Session) -> pd.DataFrame:
    query = f"""
        WITH ranked AS (
            SELECT
                SGG,
                EMD,
                YYYYMMDD,
                PRICE,
                JEONSE_PRICE,
                AVG_ASSET,
                AVG_INCOME,
                AVG_CREDIT_SCORE,
                AVG_LOAN,
                RES_POP,
                WORK_POP,
                VISIT_POP,
                ROW_NUMBER() OVER (
                    PARTITION BY SGG, EMD
                    ORDER BY YYYYMMDD DESC
                ) AS RN
            FROM {BASE_VIEW}
        )
        SELECT
            SGG,
            EMD,
            YYYYMMDD,
            PRICE,
            JEONSE_PRICE,
            AVG_ASSET,
            AVG_INCOME,
            AVG_CREDIT_SCORE,
            AVG_LOAN,
            RES_POP,
            WORK_POP,
            VISIT_POP
        FROM ranked
        WHERE RN = 1
        ORDER BY SGG, EMD
    """
    return _session.sql(query).to_pandas()


@st.cache_data(show_spinner=False)
def load_scores(_session: Session) -> pd.DataFrame:
    enriched_query = f"""
        WITH latest_snapshot AS (
            SELECT
                SGG,
                EMD,
                AVG_ASSET,
                AVG_INCOME,
                AVG_CREDIT_SCORE,
                AVG_LOAN,
                RES_POP,
                WORK_POP,
                VISIT_POP,
                ROW_NUMBER() OVER (
                    PARTITION BY SGG, EMD
                    ORDER BY YYYYMMDD DESC
                ) AS RN
            FROM {BASE_VIEW}
        )
        SELECT
            s.SGG,
            s.EMD,
            s.MEME_LATEST,
            s.JEONSE_LATEST,
            s.JEONSE_RATE,
            s.JEONSE_DROP_PCT,
            s.HUG_RATE,
            s.NET_MIG,
            s.SUBWAY_DIST,
            s.S_RATE,
            s.S_MIG,
            s.S_SUB,
            s.TOTAL_SCORE,
            s.GRADE,
            b.AVG_ASSET,
            b.AVG_INCOME,
            b.AVG_CREDIT_SCORE,
            b.AVG_LOAN,
            b.RES_POP,
            b.WORK_POP,
            b.VISIT_POP,
            COALESCE(s.RICHGO_JEONSE_RATE, NULL) AS RICHGO_JEONSE_RATE,
            COALESCE(s.RICHGO_JEONSE_DROP_PCT, NULL) AS RICHGO_JEONSE_DROP_PCT,
            COALESCE(s.RICHGO_NET_MIG, NULL) AS RICHGO_NET_MIG,
            COALESCE(s.RICHGO_SUBWAY_DIST, NULL) AS RICHGO_SUBWAY_DIST,
            COALESCE(s.RICHGO_S_RATE, NULL) AS RICHGO_S_RATE,
            COALESCE(s.RICHGO_S_MIG, NULL) AS RICHGO_S_MIG,
            COALESCE(s.RICHGO_S_SUB, NULL) AS RICHGO_S_SUB,
            COALESCE(s.RICHGO_TOTAL_SCORE, NULL) AS RICHGO_TOTAL_SCORE,
            COALESCE(s.RICHGO_GRADE, NULL) AS RICHGO_GRADE,
            COALESCE(s.HAS_RICHGO_SIGNAL, FALSE) AS HAS_RICHGO_SIGNAL,
            COALESCE(ml.ML_RISK_SCORE, 50.0) AS ML_RISK_SCORE,
            COALESCE(ml.ML_DROP_PROB, 0.5) AS ML_DROP_PROB
        FROM {ENRICHED_SCORE_TABLE} s
        LEFT JOIN latest_snapshot b
            ON s.SGG = b.SGG
           AND s.EMD = b.EMD
           AND b.RN = 1
        LEFT JOIN HACKATHON_APP.RESILIENCE.ML_RISK_SCORES ml
            ON s.SGG = ml.SGG
           AND s.EMD = ml.EMD
        ORDER BY s.TOTAL_SCORE DESC, s.SGG, s.EMD
    """
    fallback_query = f"""
        WITH latest_snapshot AS (
            SELECT
                SGG,
                EMD,
                AVG_ASSET,
                AVG_INCOME,
                AVG_CREDIT_SCORE,
                AVG_LOAN,
                RES_POP,
                WORK_POP,
                VISIT_POP,
                ROW_NUMBER() OVER (
                    PARTITION BY SGG, EMD
                    ORDER BY YYYYMMDD DESC
                ) AS RN
            FROM {BASE_VIEW}
        )
        SELECT
            s.SGG,
            s.EMD,
            s.MEME_LATEST,
            s.JEONSE_LATEST,
            s.JEONSE_RATE,
            s.JEONSE_DROP_PCT,
            s.HUG_RATE,
            s.NET_MIG,
            s.SUBWAY_DIST,
            s.S_RATE,
            s.S_MIG,
            s.S_SUB,
            s.TOTAL_SCORE,
            s.GRADE,
            b.AVG_ASSET,
            b.AVG_INCOME,
            b.AVG_CREDIT_SCORE,
            b.AVG_LOAN,
            b.RES_POP,
            b.WORK_POP,
            b.VISIT_POP,
            CAST(NULL AS FLOAT) AS RICHGO_JEONSE_RATE,
            CAST(NULL AS FLOAT) AS RICHGO_JEONSE_DROP_PCT,
            CAST(NULL AS FLOAT) AS RICHGO_NET_MIG,
            CAST(NULL AS FLOAT) AS RICHGO_SUBWAY_DIST,
            CAST(NULL AS FLOAT) AS RICHGO_S_RATE,
            CAST(NULL AS FLOAT) AS RICHGO_S_MIG,
            CAST(NULL AS FLOAT) AS RICHGO_S_SUB,
            CAST(NULL AS FLOAT) AS RICHGO_TOTAL_SCORE,
            CAST(NULL AS VARCHAR) AS RICHGO_GRADE,
            FALSE AS HAS_RICHGO_SIGNAL,
            COALESCE(ml.ML_RISK_SCORE, 50.0) AS ML_RISK_SCORE,
            COALESCE(ml.ML_DROP_PROB, 0.5) AS ML_DROP_PROB
        FROM {SCORE_TABLE} s
        LEFT JOIN latest_snapshot b
            ON s.SGG = b.SGG
           AND s.EMD = b.EMD
           AND b.RN = 1
        LEFT JOIN HACKATHON_APP.RESILIENCE.ML_RISK_SCORES ml
            ON s.SGG = ml.SGG
           AND s.EMD = ml.EMD
        ORDER BY s.TOTAL_SCORE DESC, s.SGG, s.EMD
    """
    try:
        return _session.sql(enriched_query).to_pandas()
    except Exception:
        return _session.sql(fallback_query).to_pandas()


@st.cache_data(show_spinner=False)
def load_area_history(_session: Session, sgg: str, emd: str) -> pd.DataFrame:
    safe_sgg = sgg.replace("'", "''")
    safe_emd = emd.replace("'", "''")
    query = f"""
        SELECT
            YYYYMMDD,
            PRICE,
            JEONSE_PRICE
        FROM {BASE_VIEW}
        WHERE SGG = '{safe_sgg}'
          AND EMD = '{safe_emd}'
        ORDER BY YYYYMMDD
    """
    return _session.sql(query).to_pandas()


@st.cache_data(show_spinner=False)
def load_market_summary(_session: Session) -> pd.DataFrame:
    query = f"""
        SELECT
            SGG,
            EMD,
            MIN(YYYYMMDD) AS START_DATE,
            MAX(YYYYMMDD) AS END_DATE,
            AVG(PRICE) AS AVG_SALE_PRICE,
            AVG(JEONSE_PRICE) AS AVG_JEONSE_PRICE
        FROM {BASE_VIEW}
        GROUP BY SGG, EMD
        ORDER BY SGG, EMD
    """
    return _session.sql(query).to_pandas()


@st.cache_data(show_spinner=False)
def load_all_area_history(_session: Session) -> pd.DataFrame:
    query = f"""
        SELECT
            SGG,
            EMD,
            YYYYMMDD,
            PRICE,
            JEONSE_PRICE
        FROM {BASE_VIEW}
        ORDER BY SGG, EMD, YYYYMMDD
    """
    return _session.sql(query).to_pandas()


@st.cache_data(show_spinner=False, ttl=300)
def load_recent_transactions(_session: Session, sgg: str, emd: str, limit: int = 30) -> pd.DataFrame:
    """특정 동의 최근 실거래 내역 (매매 + 전세)."""
    safe_sgg = sgg.replace("'", "''")
    safe_emd = emd.replace("'", "''")
    query = f"""
        SELECT '매매' AS "거래유형", DEAL_DATE AS "거래일", APT_NM AS "단지명",
               ROUND(EXCL_AREA, 1) AS "면적(m²)",
               ROUND(EXCL_AREA / 3.305785, 0) AS "면적(평)",
               FLOOR AS "층", DEAL_AMOUNT AS "거래가(만원)",
               ROUND(PRICE_PER_PYEONG, 0) AS "평당가(만원)"
        FROM HACKATHON_APP.RESILIENCE.MOLIT_APT_TRADE_CLEAN
        WHERE SGG = '{safe_sgg}' AND EMD = '{safe_emd}'

        UNION ALL

        SELECT '전세',
               DEAL_DATE, APT_NM,
               ROUND(EXCL_AREA, 1),
               ROUND(EXCL_AREA / 3.305785, 0),
               FLOOR,
               DEPOSIT_AMOUNT,
               ROUND(DEPOSIT_PER_PYEONG, 0)
        FROM HACKATHON_APP.RESILIENCE.MOLIT_APT_RENT_CLEAN
        WHERE SGG = '{safe_sgg}' AND EMD = '{safe_emd}'
          AND COALESCE(MONTHLY_RENT_AMOUNT, 0) = 0

        ORDER BY "거래일" DESC
        LIMIT {limit}
    """
    return _session.sql(query).to_pandas()


@st.cache_data(show_spinner=False)
def load_pyeong_bucket_data(_session: Session) -> pd.DataFrame:
    """평형대별 동/구 평당가 + 거래량 + 전세가율."""
    query = """
        SELECT SGG, EMD, PYEONG_BUCKET,
               BUCKET_JEONSE_PRICE, BUCKET_SALE_PRICE,
               BUCKET_RENT_COUNT, BUCKET_TRADE_COUNT,
               BUCKET_MEDIAN_AREA, BUCKET_JEONSE_RATE
        FROM HACKATHON_APP.RESILIENCE.JEONSE_BY_PYEONG_LATEST
    """
    return _session.sql(query).to_pandas()


@st.cache_data(show_spinner=False, ttl=3600)
def load_market_rankings(_session: Session) -> dict:
    """위험/안전/거래활발 카테고리별 TOP 동을 한 번의 SQL로 가져옴.
    AI 호출 없이 빠른 SQL aggregation만 사용 → 즉시 응답.
    AI 브리핑과 분리되어 있어 페이지 첫 로딩 시 즉시 표시 가능.
    """
    detail_query = f"""
        WITH base AS (
            SELECT SGG, EMD, GRADE, TOTAL_SCORE, JEONSE_RATE, JEONSE_DROP_PCT, NET_MIG
            FROM {SCORE_TABLE}
        ),
        risk_top AS (
            SELECT 'risk' AS bucket, SGG, EMD, GRADE,
                   ROUND(JEONSE_RATE, 1)     AS metric_a,
                   ROUND(JEONSE_DROP_PCT, 1) AS metric_b,
                   NET_MIG                   AS metric_c
            FROM base
            WHERE JEONSE_RATE IS NOT NULL AND JEONSE_RATE <= 100
            ORDER BY JEONSE_RATE DESC LIMIT 6
        ),
        safe_top AS (
            SELECT 'safe' AS bucket, SGG, EMD, GRADE,
                   ROUND(TOTAL_SCORE, 1) AS metric_a,
                   ROUND(JEONSE_RATE, 1) AS metric_b,
                   NET_MIG               AS metric_c
            FROM base WHERE NET_MIG >= 10
            ORDER BY TOTAL_SCORE DESC LIMIT 6
        ),
        active_top AS (
            SELECT 'active' AS bucket, SGG, EMD, GRADE,
                   NET_MIG               AS metric_a,
                   ROUND(JEONSE_RATE, 1) AS metric_b,
                   ROUND(TOTAL_SCORE, 1) AS metric_c
            FROM base
            ORDER BY NET_MIG DESC LIMIT 6
        )
        SELECT * FROM risk_top
        UNION ALL SELECT * FROM safe_top
        UNION ALL SELECT * FROM active_top
    """
    result: dict = {"risk": [], "safe": [], "active": []}
    try:
        df = _session.sql(detail_query).to_pandas()
        for _, row in df.iterrows():
            bucket = row["BUCKET"]
            item = {
                "area": f"{row['SGG']} {row['EMD']}",
                "grade": row["GRADE"],
                "metric_a": row["METRIC_A"],
                "metric_b": row["METRIC_B"],
                "metric_c": row["METRIC_C"],
            }
            if bucket in result:
                result[bucket].append(item)
    except Exception:  # noqa: BLE001
        pass
    return result


@st.cache_data(show_spinner=False, ttl=3600)
def load_market_briefing(_session: Session, user_profile: str = "중도위험형") -> dict:
    """Snowflake Cortex AI_AGG로 서울 전세시장을 사용자 맞춤 인사이트로 요약.

    데이터 나열식 일반론을 막기 위해:
      1) 위험/안전/거래활발 카테고리별 TOP 6을 미리 추려 LLM이 실명을 보게 함
      2) 사용자 성향(보수형/중도위험형/모험형)을 프롬프트에 주입해 톤·관점 차별화
      3) 구조화된 JSON 출력을 강제 → 카드에서 섹션별로 렌더링
      4) "수치 직접 사용 금지", "일반론 금지", "WHY 필수" 같은 강한 룰

    반환: {
        "headline": str, "market_mood": str,
        "watch_areas": [{"area","why"}], "opportunity_areas": [{"area","why"}],
        "user_action": str, "risk":[...], "safe":[...], "active":[...]
    }
    """
    profile_tone = {
        "보수형": "안전과 보증금 회수 가능성을 최우선으로 여기는 신중한",
        "중도위험형": "안전과 가격 균형을 중시하는 합리적인",
        "모험형": "약간의 위험을 감수하고서라도 가성비와 잠재력을 노리는 적극적인",
    }.get(user_profile, "합리적인")

    prompt_text = (
        f"당신은 서울 부동산 시장을 7년째 분석한 시장 분석가입니다. "
        f"입력 데이터에는 서울 25개 구 288개 동 중 '위험 후보', '안전 후보', '거래 활발' "
        f"세 카테고리의 대표 지역들이 들어 있습니다. "
        f"독자는 {profile_tone} 전세 세입자 한 분입니다. "
        f"이번 달 서울 전세시장의 흐름과 지금 어떻게 움직이시면 좋을지를 안내해 드리는 것이 목표입니다.\n\n"
        f"반드시 아래 JSON 형식만 출력하세요. 코드블록, 설명문, 마크다운 금지:\n"
        f'{{\n'
        f'  "headline": "이번 달 시장 분위기를 함축한 12~18자 헤드라인",\n'
        f'  "market_mood": "이번 달 전반 분위기 1~2문장 (모호한 표현 금지)",\n'
        f'  "watch_areas": [\n'
        f'    {{"area": "구 동 이름", "why": "왜 주의가 필요한지 1문장"}},\n'
        f'    {{"area": "구 동 이름", "why": "왜 주의가 필요한지 1문장"}}\n'
        f'  ],\n'
        f'  "opportunity_areas": [\n'
        f'    {{"area": "구 동 이름", "why": "왜 지금 눈여겨볼 만한지 1문장"}},\n'
        f'    {{"area": "구 동 이름", "why": "왜 지금 눈여겨볼 만한지 1문장"}}\n'
        f'  ],\n'
        f'  "user_action": "{profile_tone} 사용자가 지금부터 1~2주 내 해보시면 좋을 구체적 행동 1문장"\n'
        f'}}\n\n'
        f"반드시 지켜야 할 규칙:\n"
        f"1. **검증 불가능한 구체 사실을 절대 지어내지 마세요.** 다음은 모두 금지입니다:\n"
        f"   - 지하철 노선 번호 (예: '2호선이 지나는', '7호선 역세권')\n"
        f"   - 특정 학교명, 학군 이름, 명문 학원가\n"
        f"   - 재건축 단지명, 재개발 계획, 정비구역 지정 여부\n"
        f"   - 특정 회사·업무지구·랜드마크의 정확한 위치 관계\n"
        f"   - 도로명, 인접 시설명\n"
        f"   당신은 이런 정보를 정확히 모릅니다. 잘못 말하면 사용자에게 큰 피해가 갑니다.\n"
        f"2. 대신 **입력 데이터로 제공된 정보(전세가율, 등급, 안전점수, 거래건수, 변동률)만**을 근거로 "
        f"자연어로 풀어 설명하세요. 그 동네의 일반적 분위기는 데이터로부터 유추 가능한 범위로만 표현 "
        f"(예: '거래가 활발한 동네입니다', '전세가 안정세를 보이는 편입니다').\n"
        f"3. **모든 문장은 정중한 존댓말 (~입니다, ~합니다, ~보입니다, ~해보세요)을 사용**할 것. 반말·단정조 금지.\n"
        f"4. **표현 톤은 부드럽고 차분하게**. 자극적·과장된 단어 (예: '돌려주지 못함', '큰 위험', '매우 위험') 금지. "
        f"'~할 수 있는 상황입니다', '~한 편입니다', '~에 유의가 필요합니다' 같은 완곡한 표현을 사용할 것.\n"
        f"5. area는 입력 데이터에 등장한 실제 동 이름만 사용 (예: '강남구 대치동'). 없는 동을 지어내지 말 것.\n"
        f"6. why는 '전세가율이 높음', '거래가 많음' 같은 데이터 나열 금지. "
        f"실제 거주자 관점에서 부드럽게 풀어 쓸 것.\n"
        f"   ❌ 나쁨: '전세가율 92%로 위험'\n"
        f"   ❌ 너무 강함: '집주인이 보증금을 돌려주지 못할 위험이 큽니다'\n"
        f"   ❌ 환각: '7호선 역세권으로 출퇴근이 편리한 점이 매력입니다'\n"
        f"   ✅ 좋음: '전세가가 매매가에 거의 근접해 깡통전세 위험에 유의가 필요한 상황입니다'\n"
        f"   ✅ 좋음: '거래가 활발하게 이어지며 매물 선택지가 넓어지고 있는 분위기입니다'\n"
        f"7. 점수, 퍼센트, /100, 숫자 같은 수치 표현을 직접 쓰지 말고 모두 자연어로 풀어 쓸 것.\n"
        f"8. '안전한 지역입니다', '주의가 필요합니다', '안정적인 추세입니다' 같은 일반론·상투구만으로 끝내지 말고, "
        f"입력 데이터의 어떤 신호 때문에 그렇게 판단했는지 함께 풀어 설명할 것.\n"
        f"9. opportunity_areas는 단순히 안전점수가 높은 곳이 아니라, "
        f"'{profile_tone}' 사용자 입장에서 지금 가장 눈여겨볼 만한 후보를 골라 설명할 것.\n"
        f"10. user_action은 '신중하게 검토하세요' 같은 추상적 권유 금지. "
        f"'이번 주 안에 강남구 대치동과 송파구 잠실동을 직접 방문하셔서 단지별 전세가율을 비교해보시는 것을 권해드립니다' "
        f"같은 정중하고 구체적인 행동 제안.\n"
        f"11. watch_areas와 opportunity_areas에 같은 동을 동시에 넣지 말 것.\n"
        f"12. JSON 외의 어떤 텍스트, 마크다운, 코드블록도 절대 출력하지 말 것."
    )
    safe_prompt = prompt_text.replace("'", "''")

    # AI_AGG에는 카테고리당 TOP 3만 전달 (총 9줄) → 프롬프트가 작을수록 추론 빠름
    # UI 랭킹 카드는 detail_query에서 따로 TOP 6을 받아오므로 표시는 그대로 유지
    query = f"""
        WITH risk_top AS (
            SELECT
                'risk'  AS BUCKET,
                SGG || ' ' || EMD AS AREA,
                GRADE,
                ROUND(JEONSE_RATE, 1)     AS RATE,
                ROUND(JEONSE_DROP_PCT, 1) AS DROP_PCT,
                NET_MIG
            FROM {SCORE_TABLE}
            WHERE JEONSE_RATE IS NOT NULL AND JEONSE_RATE <= 100
            ORDER BY JEONSE_RATE DESC
            LIMIT 3
        ),
        safe_top AS (
            SELECT
                'safe'  AS BUCKET,
                SGG || ' ' || EMD AS AREA,
                GRADE,
                ROUND(TOTAL_SCORE, 1)     AS SCORE,
                ROUND(JEONSE_RATE, 1)     AS RATE,
                NET_MIG
            FROM {SCORE_TABLE}
            WHERE NET_MIG >= 10
            ORDER BY TOTAL_SCORE DESC
            LIMIT 3
        ),
        active_top AS (
            SELECT
                'active' AS BUCKET,
                SGG || ' ' || EMD AS AREA,
                GRADE,
                NET_MIG,
                ROUND(JEONSE_RATE, 1) AS RATE
            FROM {SCORE_TABLE}
            ORDER BY NET_MIG DESC
            LIMIT 3
        ),
        risk_lines AS (
            SELECT '[위험 카테고리] ' || AREA
                || ' / 등급 ' || GRADE
                || ' / 전세가가 매매가의 ' || RATE || '% 수준'
                || ' / 12개월간 전세가 변동률 ' || DROP_PCT || '%'
                || ' / 거래건수 ' || NET_MIG AS LINE
            FROM risk_top
        ),
        safe_lines AS (
            SELECT '[안전 카테고리] ' || AREA
                || ' / 등급 ' || GRADE
                || ' / 종합 안전점수 ' || SCORE
                || ' / 전세가율 ' || RATE || '%'
                || ' / 거래건수 ' || NET_MIG AS LINE
            FROM safe_top
        ),
        active_lines AS (
            SELECT '[거래 활발 카테고리] ' || AREA
                || ' / 등급 ' || GRADE
                || ' / 거래건수 ' || NET_MIG
                || ' / 전세가율 ' || RATE || '%' AS LINE
            FROM active_top
        ),
        combined AS (
            SELECT LINE FROM risk_lines
            UNION ALL SELECT LINE FROM safe_lines
            UNION ALL SELECT LINE FROM active_lines
        )
        SELECT AI_AGG(LINE, '{safe_prompt}') AS RESULT
        FROM combined
    """
    try:
        ai_df = _session.sql(query).to_pandas()

        raw_text = ""
        if not ai_df.empty and not pd.isna(ai_df.iloc[0, 0]):
            raw_text = str(ai_df.iloc[0, 0]).strip()

        # JSON 파싱 (코드블록·잡음 제거)
        parsed: dict = {}
        if raw_text:
            cleaned = raw_text
            if cleaned.startswith("```"):
                cleaned = cleaned.split("```")[1]
                if cleaned.lower().startswith("json"):
                    cleaned = cleaned[4:]
                cleaned = cleaned.strip()
            # JSON 본체만 추출 (앞뒤 잡 텍스트 방지)
            start = cleaned.find("{")
            end = cleaned.rfind("}")
            if start != -1 and end != -1 and end > start:
                cleaned = cleaned[start : end + 1]
            try:
                parsed = json.loads(cleaned)
            except Exception:  # noqa: BLE001
                parsed = {"market_mood": raw_text}

        return {
            "headline": parsed.get("headline", "") or "",
            "market_mood": parsed.get("market_mood", "") or "",
            "watch_areas": parsed.get("watch_areas", []) or [],
            "opportunity_areas": parsed.get("opportunity_areas", []) or [],
            "user_action": parsed.get("user_action", "") or "",
            "user_profile": user_profile,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "headline": "",
            "market_mood": f"__ERROR__:{exc}",
            "watch_areas": [],
            "opportunity_areas": [],
            "user_action": "",
            "user_profile": user_profile,
        }


@st.cache_data(show_spinner=False, ttl=300)
def load_complex_summary(_session: Session, sgg: str, emd: str) -> pd.DataFrame:
    """특정 동의 단지별 최근 매매/전세 요약."""
    safe_sgg = sgg.replace("'", "''")
    safe_emd = emd.replace("'", "''")
    query = f"""
        WITH trade_summary AS (
            SELECT APT_NM,
                   COUNT(*) AS "매매건수",
                   ROUND(MEDIAN(DEAL_AMOUNT), 0) AS "매매중위(만원)",
                   ROUND(MEDIAN(PRICE_PER_PYEONG), 0) AS "매매평당(만원)",
                   ROUND(MEDIAN(EXCL_AREA), 1) AS "주요면적(m²)",
                   MAX(DEAL_DATE) AS "최근매매일"
            FROM HACKATHON_APP.RESILIENCE.MOLIT_APT_TRADE_CLEAN
            WHERE SGG = '{safe_sgg}' AND EMD = '{safe_emd}'
              AND DEAL_DATE >= DATEADD(MONTH, -6, CURRENT_DATE())
            GROUP BY APT_NM
        ),
        rent_summary AS (
            SELECT APT_NM,
                   COUNT(*) AS "전세건수",
                   ROUND(MEDIAN(DEPOSIT_AMOUNT), 0) AS "전세중위(만원)",
                   ROUND(MEDIAN(DEPOSIT_PER_PYEONG), 0) AS "전세평당(만원)",
                   MAX(DEAL_DATE) AS "최근전세일"
            FROM HACKATHON_APP.RESILIENCE.MOLIT_APT_RENT_CLEAN
            WHERE SGG = '{safe_sgg}' AND EMD = '{safe_emd}'
              AND COALESCE(MONTHLY_RENT_AMOUNT, 0) = 0
              AND DEAL_DATE >= DATEADD(MONTH, -6, CURRENT_DATE())
            GROUP BY APT_NM
        )
        SELECT
            COALESCE(t.APT_NM, r.APT_NM) AS "단지명",
            t."매매건수", t."매매중위(만원)", t."매매평당(만원)",
            r."전세건수", r."전세중위(만원)", r."전세평당(만원)",
            t."주요면적(m²)",
            CASE WHEN t."매매중위(만원)" > 0 AND r."전세중위(만원)" > 0
                 THEN ROUND(r."전세중위(만원)" / t."매매중위(만원)" * 100, 1)
                 ELSE NULL END AS "전세가율(%)"
        FROM trade_summary t
        FULL OUTER JOIN rent_summary r ON t.APT_NM = r.APT_NM
        ORDER BY COALESCE(t."매매건수", 0) + COALESCE(r."전세건수", 0) DESC
    """
    return _session.sql(query).to_pandas()
