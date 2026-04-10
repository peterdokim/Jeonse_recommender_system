# 🏠 전세 안심 추천

> 보증금, 생활 조건, 위험 성향에 맞는 더 안전한 전세 대안을 찾아주는 개인화 추천 앱

서울 25개 구 288개 동의 실거래·구조 리스크 데이터를 기반으로, 사용자가 직접 입력한 보증금과 6문항 성향 설문에 따라 더 안전한 전세 후보를 추천합니다. Snowflake Cortex AI와 Snowpark ML이 백엔드에서 후보 진단·시장 브리핑·자연어 질의응답을 처리합니다.

Snowflake Hackathon 출품작.

---

## 무엇을 하는 앱인가

전세 계약을 앞둔 사용자가 가장 답하기 어려운 질문은 **"내가 보고 있는 이 집보다 더 안전한 곳이 어디인가?"** 입니다. 시세 정보는 흔하지만, 보증금 회수 위험을 내 조건 안에서 해석하고 비교해 주는 도구는 거의 없습니다.

이 앱은 다음 세 가지를 한 화면에서 해결합니다.

1. **위험 진단**: 현재 보고 있는 동의 전세 환경을 등급(A~D)·전세가율·거래 활발도·AI 하락 위험도로 정량 평가
2. **개인화 대안 추천**: 동일 예산·생활권 안에서 더 안전한 후보를 성향별로 3가지 유형(가장 안전 / 균형 / 가장 비슷)으로 제시
3. **시장 흐름 안내**: 서울 전세시장 전반의 분위기, 주의 지역, 기회 지역을 LLM 한 단락 브리핑으로 요약

모든 UI 텍스트는 한국어이며, 일반 사용자가 부동산 전문가와 상담받는 톤으로 설계되었습니다.

---

## 주요 기능

### 1. 6문항 성향 설문 → 위험 성향 분류
- 보수형 / 중도위험형 / 모험형 3가지 분류
- 안전 지향(Q1-Q3)과 위험 감내(Q4-Q6)를 독립적으로 채점해 상쇄 방지
- 결과는 추천 가중치(α/β/γ/δ)로 변환되어 랭킹에 반영

### 2. 5개 탭 구성

| 탭 | 내용 |
|---|---|
| **개인화 추천** | 입력 조건에 맞는 후보 랭킹. 가장 안전 / 가장 균형 / 가장 비슷 3가지 유형 카드. 제외된 동에 대한 11가지 사유 표시 |
| **후보 진단** | 선택한 동의 핵심 지표(전세가율·예상 전세가·AI 하락 위험도) + Cortex AI가 작성한 강점 / 주의점 / 추천 행동 카드 |
| **비교 분석** | 현재 후보 vs 추천 대안 표 비교 |
| **시장 흐름** | AI 시장 브리핑(헤드라인·분위기·주의 지역·기회 지역·행동 권유) + 위험·안전·거래활발 TOP 5 랭킹 + 선택 동 시세 추이·실거래·단지별 비교 |
| **AI 질문** | Cortex Analyst로 자연어 질문을 SQL로 변환해 응답. 8개 프리셋 질문 제공 |

### 3. Hybrid 추천 점수 (Rule + ML)
```
안전점수  = 0.5×전세가율 + 0.25×거래활발도 + 0.25×가격안정성
룰점수    = 안전 60% + 손실회피·가격적정·선호적합·유사도 (성향별 가중)
최종점수  = 룰점수 × 0.80 + (100 − ML 위험도) × 0.20
```
- ML 모델: GradientBoosting으로 6개월 후 5%+ 하락 확률 예측 (ROC AUC ≈ 0.74)
- Snowpark ML Registry에 등록 후 Snowflake 안에서 추론

### 4. 평형대 정밀 가격
- 사용자 입력 평형을 SMALL(≤50㎡) / MID(≤85㎡) / LARGE(≤135㎡) / XLARGE(>135㎡) 4 버킷으로 매핑
- 각 버킷별 최근 6개월 실거래 중위가로 예상 전세가 계산 → 동 평균보다 정확

---

## 기술 스택

- **Frontend**: Streamlit (Python)
- **Backend**: Snowflake (Snowpark Python)
- **AI/ML**:
  - Snowflake Cortex `AI_COMPLETE` (`claude-3-5-sonnet` → `llama3.1-70b` → `mistral-large2` 폴백)
  - Snowflake Cortex `AI_AGG` (시장 브리핑 단일 쿼리 LLM 집계)
  - Snowflake **Cortex Analyst** (자연어 → SQL, 시맨틱 모델 기반)
  - Snowpark ML Registry (`JEONSE_DROP_RISK_MODEL/v1` GradientBoosting)
- **데이터 소스**:
  - 국토교통부 아파트 매매·전월세 실거래가 공공 API
  - SPH (Seoul Population & Households) Marketplace 인구통계 (3개 구 한정)
- **시각화**: Altair

---

## 데이터 파이프라인

```
국토교통부 실거래가 API
    ↓ scripts/load_molit_transactions.py (SHA256 dedupe + MERGE)
RAW_MOLIT_APT_TRADE / RAW_MOLIT_APT_RENT
    ↓
*_CLEAN  →  *_MONTHLY  →  RESILIENCE_BASE  (+ SPH 인구통계 FULL OUTER JOIN)
    ↓
FEATURE_AREA_MONTH (전세가율·변동률·거래량·변동성)
    ↓
JEONSE_SAFETY_SCORE (백분위 점수 + A/B/C/D 등급)
    ↓
ML_RISK_SCORES (Snowpark ML 추론 결과)
    ↓
Streamlit 앱 + Cortex AI
```

모든 ETL·스코어링·ML 추론은 Snowflake 안에서 실행됩니다.

---

## 프로젝트 구조

```
.
├── streamlit_app.py              # 메인 Streamlit 앱 (5개 탭)
├── setup.sql                     # Snowflake 스키마·뷰·함수 정의
├── semantic_models/
│   └── jeonse_model.yaml         # Cortex Analyst 시맨틱 모델
├── common/
│   ├── session.py                # Snowflake 세션 (자동 재연결)
│   ├── queries.py                # 캐시된 데이터 로더 (Cortex AI 호출 포함)
│   ├── recommendation.py         # 추천 엔진 (필터·랭킹·재정렬·설명)
│   ├── molit_loader.py           # 국토교통부 API ETL
│   └── settings.py               # 환경변수·secrets 통합
├── scripts/
│   ├── load_molit_transactions.py  # 실거래가 일괄 적재
│   └── train_risk_model.py         # ML 학습 + Registry 등록 + 추론
├── output/bundle/                # Snowflake Native App 패키지
└── .streamlit/
    └── secrets.toml.example      # 자격 증명 템플릿
```

---

## 설치 및 실행

### 1. 의존성 설치
```bash
conda env create -f environment.yml
conda activate jeonse-safety-app
# 또는: pip install -r requirements.txt
```

### 2. 자격 증명 설정
```bash
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
```
`.streamlit/secrets.toml`에 다음을 입력합니다.
```toml
[snowflake]
account = "..."
user = "..."
password = "..."
warehouse = "..."
database = "HACKATHON_APP"
schema = "RESILIENCE"
role = "..."

[public_data_api]
service_key = "..."   # 공공데이터포털 발급 키
```
환경변수 `SNOWFLAKE_*`, `PUBLIC_DATA_API_*`로도 오버라이드 가능합니다.

### 3. Snowflake 스키마 생성
```bash
# Snowflake UI 또는 snowsql에서 setup.sql 실행
```

### 4. 실거래가 데이터 적재
```bash
python scripts/load_molit_transactions.py
# 옵션: --target trade|rent|all --start-month 202401 --end-month 202603
```

### 5. ML 모델 학습 + Registry 등록 + 추론
```bash
python scripts/train_risk_model.py
# 옵션: --skip-train (Registry 추론만) / --local-only (Registry 미사용)
```

### 6. 앱 실행
```bash
streamlit run streamlit_app.py
```

### Snowflake Native App으로 배포
```bash
snow app run
```

---

## 사용 흐름

1. **랜딩**: 메인 화면에서 6문항 설문 응답 → "결과 보러가기" 클릭
2. **조건 입력**: 사이드바에서 보증금·평형·관심 지역(구·동) 입력
3. **결과 확인**: 5개 탭을 자유롭게 탐색
   - 개인화 추천 → 더 안전한 대안 카드
   - 후보 진단 → AI가 작성한 동별 분석
   - 비교 분석 → 표로 비교
   - 시장 흐름 → 서울 시장 한눈에 + 선택 동 시세 추이
   - AI 질문 → 자연어로 데이터 질문

처음 로딩 시 추천·랭킹 등 기본 콘텐츠는 즉시 표시되고, AI 분석은 백그라운드에서 약 5~10초 후 자동으로 채워집니다 (2-pass lazy 로딩).

---

## 면책

본 서비스의 추천 결과는 공개 데이터 기반의 참고 정보이며, 법률·세무·투자 판단을 대체하지 않습니다. 실제 계약 전에는 등기부등본, 선순위 권리관계, 보증보험 가입 가능 여부 등을 반드시 별도로 확인해야 합니다.
