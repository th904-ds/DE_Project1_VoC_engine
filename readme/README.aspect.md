# aspect_mapping — 키워드 기반 Aspect 매핑

> 작성: 예원 · 2026-04-28
> 대상: 스트림릿 담당자
> 파이프라인 위치: **Task 5 (reviews_clean 의 text_clean → aspects)**

이 모듈은 정제된 리뷰 텍스트(`text_clean`)에서 **6개 aspect 카테고리**(소재·핏·사이즈·
색상·가격·배송) 별 매칭 문장을 추출합니다. 입력 텍스트의 정제는 자매 모듈
`preprocessing/` 이 담당하므로 본 모듈은 **정제된 평문만 받습니다**.

---

## 무엇을 하는가

1. 정제된 리뷰 텍스트를 한국어 문장 단위로 분리
2. 각 문장에서 6개 aspect 키워드 탐색
3. `{aspect: [매칭 문장, ...]}` 형태로 반환

매칭 문장이 하나도 없으면 빈 dict. 한 문장이 여러 aspect 에 동시 매칭될 수 있습니다.

---

## 두 가지 모드

### 모드 1 — 함수 import (스트림릿이 사용자 입력을 단건 처리)

```python
from preprocessing import clean_review_text
from aspect_mapping import (
    split_sentences,
    extract_aspects,
    aspects_mentioned,
)

# 1. 텍스트 정제 (preprocessing 모듈)
text_clean = clean_review_text(user_input)

# 2. aspect 매핑
aspects = extract_aspects(text_clean)
# → {"소재": ["원단이 부드럽고 두께감 적당해요"], "사이즈": ["...크게 나와요"]}

# 3. 어떤 aspect가 언급됐는지만 알고 싶을 때
keys = aspects_mentioned(text_clean)
# → ["소재", "사이즈"]

# (선택) 문장 분리만 따로 쓰고 싶을 때
sents = split_sentences(text_clean)
```

### 모드 2 — 스크립트 실행 (MongoDB 배치 파이프라인)

```bash
python aspect_mapping.py
```

- `reviews_clean` 의 모든 문서를 순회하며 `review_data.text_clean` 을 읽음
- 각 문서에 `review_data.aspects_sentences`, `review_data.aspects_mentioned` 필드 추가
- 종료 시 aspect 별 언급 빈도 출력

> **선행 조건**: 본 스크립트는 `text_clean` 필드가 이미 채워져 있다고 가정합니다. `preprocessing.py` 를 먼저 실행해 `reviews_clean` 을 작성한 뒤 이 스크립트를 돌리세요.

---

## 함수 레퍼런스

| 함수 | 시그니처 | 반환 | 설명 |
|---|---|---|---|
| `split_sentences` | `(text: str) -> list[str]` | 문장 리스트 | 구두점 + 한국어 연결어미 분리 |
| `extract_aspects` | `(text: str) -> dict[str, list[str]]` | aspect→문장 dict | 미매칭 시 빈 dict |
| `aspects_mentioned` | `(text: str) -> list[str]` | aspect 키 리스트 | 언급된 aspect 만 |

---

## Aspect 사전

```python
ASPECTS = {
    "소재": ["소재", "원단", "재질", "천", "면", "폴리", "두께", "얇", "두껍"],
    "핏":   ["핏", "핏감", "실루엣", "라인", "몸에 딱", "여유"],
    "사이즈": ["사이즈", "크기", "작", "크", "맞", "치수"],
    "색상": ["색상", "색깔", "컬러", "진하", "연하", "사진과"],
    "가격": ["가격", "가성비", "값", "비싸", "저렴", "합리적"],
    "배송": ["배송", "배달", "포장", "빠르", "느리"],
}
```

### 스펙 대비 의도적 보정 3건

| 변경 | 이유 |
|---|---|
| 색상에서 `"색"` 단독 키워드 제거 | `"색다른"`, `"특색"`, `"염색"` false positive 회피 |
| 사이즈에서 `"크다"`/`"맞다"` 대신 어간 `"크"`/`"맞"` | 한국어 활용형 커버 (`"커요"`, `"컸어요"`, `"맞아요"`) |
| 문장 분리에 `,` + 한국어 연결어미 추가 | aspect 혼재 문장(`"핏은 좋은데 사이즈가 작아요"`) 분해 |

---

## 문장 분리 규칙

```python
SENT_SPLIT = re.compile(
    r"[.!?\n,]+|(?:는데|은데|인데|지만|이지만|니까)(?=\s|[가-힣])"
)
```

- 분리 기준: `.` `!` `?` `\n` `,` + 연결어미 `는데/은데/인데/지만/이지만/니까`
- 분리 후 빈 문자열 제거 + 좌우 공백 strip

```python
split_sentences("핏은 예쁜데 사이즈는 작아요. 가격은 합리적이에요!")
# → ["핏은 예쁜", "사이즈는 작아요", "가격은 합리적이에요"]
```

---

## 출력 스키마 (배치 모드 결과)

```json
{
  "review_data": {
    "text_clean": "...",
    "aspects_sentences": {
      "소재": ["원단이 부드럽고 두께감 적당해요"],
      "사이즈": ["사이즈는 평소보다 한 치수 크게"]
    },
    "aspects_mentioned": ["소재", "사이즈"]
  }
}
```

매칭 0건인 문서는 두 필드 모두 **저장되지 않음** → 후속 단계에서
`{"review_data.aspects_sentences": {"$exists": True, "$ne": {}}}` 로 자연스럽게 필터.

---

## 환경 설정

### 의존성

```
python-dotenv>=1.0
pymongo>=4.0
```

스트림릿이 단건 함수만 import 한다면 `pymongo` · `python-dotenv` 도 불필요
(코드 상단 import 만 무시되거나, 미설치 시 import 실패하므로 가상환경엔 깔아두세요).

### 환경변수 (배치 모드만 필요)

```env
MONGO_URI=mongodb+srv://...
MONGO_DB=musinsa_db
MONGO_COLLECTION_CLEAN=reviews_clean
```

`load_dotenv()` 가 현재 작업 디렉터리부터 위로 자동 탐색.

---

## 30만 건 기준 결과 참고

| Aspect | 매칭 건수 | 비율 |
|---|---|---|
| 소재 | 107,926 | 35.8% |
| 사이즈 | 73,098 | 24.2% |
| 가격 | 51,449 | 17.1% |
| 핏 | 44,666 | 14.8% |
| 배송 | 14,622 | 4.9% |
| 색상 | 9,238 | 3.1% |

전체 매칭률 **68.6%** (206,665 / 301,480 docs).

---

## 설계 포인트 (질문 대비)

1. **부분 일치(substring)** — 활용형·조사로부터 자유로움. (`"사이즈"` 가 `"사이즈도"`, `"사이즈가"` 모두 매칭)
2. **여러 aspect 동시 매칭 허용** — 한 문장에 두 aspect 키워드가 있으면 양쪽 dict 에 모두 들어감.
3. **매칭 0건 시 필드 미생성** — 다운스트림 필터링 단순화 + 스토리지 절감.
4. **문장 단위 매칭** — 문서 단위로 매칭하면 "소재는 좋은데 사이즈가 별로" 같은 혼합 감성을 잃음. Task 6 ABSA 단계에서 aspect 별 감성을 분리 평가하기 위해 문장 단위 추출 필수.

---

## 한계·주의사항

1. **키워드 사전 기반의 경직성** — 신조어·간접 표현(예: `"핏"` 없이 `"어깨 라인이 예뻐요"`)은 놓침.
2. **부분 일치 부작용** — 드물지만 가능. 예: `"비싸"` 가 `"비싸지 않다"` 같은 부정 맥락에서도 가격으로 잡힘. 감성 판별은 Task 6 ABSA 가 담당하므로 매핑 단계에선 OK.
3. **빈 입력** — `extract_aspects("")` 는 `{}` 반환. UI 단 가드 권장.
4. **표본 적은 aspect** — 색상 3.1% (30만 기준 약 9,238건). 페르소나별 세분화 시 셀당 n 작아질 수 있음.

---

## 선행 단계 — `preprocessing`

본 모듈은 **정제된 텍스트** 를 입력으로 가정합니다. 정제 단계는 자매 모듈 `preprocessing/` 참고:

```python
from preprocessing import clean_review_text
from aspect_mapping import extract_aspects

text_clean = clean_review_text(raw)
aspects = extract_aspects(text_clean)
```

질문은 예원에게.
