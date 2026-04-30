# preprocessing — 텍스트 정제 + 페르소나 산출

> 작성: 예원 · 2026-04-28
> 대상: 스트림릿 담당자
> 파이프라인 위치: **Task 1~4 (reviews → reviews_clean)**

이 모듈은 무신사 리뷰의 **기본 전처리** 를 담당합니다. 키워드 매핑(Task 5)은
별도 모듈 `aspect_mapping/` 에서 처리합니다.

---

## 무엇을 하는가

1. 리뷰 원문에서 URL·이모지·특수문자를 제거하고 한글·숫자·공백만 남김 (`text_clean`)
2. 사용자 정보(성별·신장·체중)와 구매 사이즈로 **페르소나 라벨** 산출
3. (보조) DB에 zlib 압축으로 저장된 `text_compressed` 를 평문으로 복원

본 모듈만으로 `reviews_clean` 컬렉션을 작성할 수 있고, 그 결과 위에 `aspect_mapping` 이
이어서 동작합니다.

---

## 두 가지 모드

### 모드 1 — 함수 import (스트림릿이 사용자 입력을 단건 처리)

```python
from preprocessing import (
    clean_review_text,
    compute_persona,
    decompress_text,
)

# 1. 텍스트 정제
text_clean = clean_review_text(user_input)

# 2. 페르소나 산출 (BMI 가능하면 BMI, 아니면 사이즈 기반)
persona = compute_persona(
    gender="여",            # "남" / "여" / None
    height_cm=165.0,
    weight_kg=55.0,
    size_raw="M",           # 신장·체중 없을 때 fallback
)
# → "여_보통체형"

# 3. DB의 압축 텍스트 복원 (이미 reviews_clean에 저장된 것을 읽을 때)
plain = decompress_text(doc["review_data"]["text_compressed"])
```

### 모드 2 — 스크립트 실행 (MongoDB 배치 파이프라인)

```bash
python preprocessing.py
```

- `reviews` 원본을 읽어 `reviews_clean` 을 **slim 스키마**로 재생성 (기존 drop)
- Step 1: 복사  ·  Step 2: dedup  ·  Step 3: persona  ·  Step 4: text_clean
- 원본 `reviews` 는 절대 수정하지 않음

---

## 함수 레퍼런스

| 함수 | 시그니처 | 반환 | 설명 |
|---|---|---|---|
| `clean_review_text` | `(text: str) -> str` | 정제 텍스트 | URL/한글외 제거, 연속 공백 → 단일 |
| `compute_persona` | `(*, gender, height_cm, weight_kg, size_raw) -> str` | 페르소나 라벨 | 하이브리드 BMI/사이즈 |
| `decompress_text` | `(compressed: object) -> str` | 평문 | zlib bytes → utf-8 |

### `clean_review_text` 처리 규칙

1. `http(s)://` URL 제거
2. 한글·숫자·공백 외 문자 제거 (이모지·영어·특수문자 등)
3. 연속 공백 → 단일 공백
4. 앞뒤 공백 제거

```python
clean_review_text("정말 좋아요!! ★★★ http://x.com  여름에 굿굿")
# → "정말 좋아요   여름에 굿굿"
```

### `compute_persona` 로직

| 조건 | 결과 라벨 |
|---|---|
| 성별 + 신장 + 체중 모두 있음 | `"{성별}_{체형}"` (BMI 기반) |
| 위 조건 불충족 + 사이즈 있음 | `"{성별 or unknown}_{체형}"` (사이즈 기반) |
| 둘 다 없음 | `"unknown"` |

**BMI 체형**: <18.5 마른체형 / 18.5–22.9 보통체형 / 23–24.9 통통체형 / ≥25 풍만체형

**사이즈 체형**: XS·S·44·55·80·85 → 소형 / M·90·95·free → 중형 / L·100 → 대형 / XL·XXL·105+ → 특대형

---

## 환경 설정

### 의존성

```
python-dotenv>=1.0
pymongo>=4.0
```

PyTorch · transformers 는 **본 모듈에서 불필요**.

### 환경변수 (배치 모드만 필요)

```env
MONGO_URI=mongodb+srv://...
MONGO_DB=musinsa_db
MONGO_COLLECTION=reviews
MONGO_COLLECTION_CLEAN=reviews_clean
```

`load_dotenv()` 가 현재 작업 디렉터리부터 위로 자동 탐색합니다.
환경변수가 이미 export 되어 있다면 `.env` 가 없어도 동작.

스트림릿이 단건 함수만 import 한다면 환경변수 자체가 불필요합니다.

---

## 출력 스키마 (배치 모드 결과)

```json
{
  "_id": "...",
  "product_id": "1420730",
  "date": "2025-06-12",
  "rating": 5.0,
  "purchase_info": { "size": "M", "color": "WHITE" },
  "user_info": {
    "encrypted_id": "...",
    "gender": "여",
    "height_cm": 165.0,
    "weight_kg": 55.0
  },
  "review_data": {
    "text_compressed": "<bytes>",
    "is_compressed": true,
    "text_clean": "정제된 평문 텍스트"
  },
  "persona": "여_보통체형"
}
```

평문 `text` 는 **저장하지 않음**. 필요할 때만 `decompress_text()` 로 복원.

---

## 설계 포인트 (질문 대비)

1. **압축 텍스트 유지** — `reviews_clean` 에 평문 `text` 미저장. 무료 Atlas 512MB 한도 안에서 30만 건을 처리하기 위함. 필요할 때만 in-memory 로 복원.
2. **slim 스키마** — `nickname`, `option_raw`, `photo_urls`, `review_id`, `has_photo`, `like_count`, `type`, `level` 등 분석 무관 필드는 복사하지 않음.
3. **하이브리드 페르소나** — 신장·체중 미기재율 약 43%. BMI 불가 시 구매 사이즈로 fallback.
4. **dedup key** — `(encrypted_id, text_compressed bytes)` 로 비교. 압축 bytes 그대로 hashable 이라 decompress 불필요.

---

## 한계·주의사항

1. `clean_review_text` 는 한글·숫자·공백만 남깁니다. 영어·이모지·특수문자 분석이 필요하면 정규식 수정 필요.
2. 빈 입력은 빈 문자열 반환. 스트림릿 UI 단에서 가드 권장.
3. 페르소나 라벨은 한국어 문자열 (`"여_보통체형"`). 시각화 시 한글 폰트 설정 필요.
4. `compute_persona` 는 키워드 인자(`*` 이후) 만 받습니다 — 위치 인자 사용 시 `TypeError`.

---

## 다음 단계 — `aspect_mapping`

본 모듈이 만든 `review_data.text_clean` 은 자매 모듈 `aspect_mapping` 의 입력입니다.
스트림릿에서도 같은 흐름:

```python
from preprocessing import clean_review_text
from aspect_mapping import extract_aspects

text_clean = clean_review_text(raw)
aspects = extract_aspects(text_clean)
```

질문은 예원에게.
