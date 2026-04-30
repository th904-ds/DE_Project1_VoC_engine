# Task 6 — ABSA 본 실행

> 작성: 예원 · 2026-04-23
> 대상: DE Project 1 팀원
> 목적: Task 5에서 뽑아낸 aspect별 문장에 대해 **긍정/부정 감성 라벨**을 붙여 `reviews_absa` 컬렉션에 저장

---

## 1. 이 폴더의 구성

```
task6_absa/
├── task6_absa.py       # 본 실행 스크립트
├── README.md           # 이 문서
├── requirements.txt    # 파이썬 의존성
└── .env.example        # 환경변수 템플릿
```

---

## 2. 뭘 하는 스크립트인가

### 입력
- MongoDB `reviews_clean`
- 조건: `review_data.aspects_sentences`가 비어있지 않은 문서 (= Task 5 매칭된 문서, 13만 중 약 93,660건)

### 처리
- 문서마다 **각 aspect의 문장들을 " | " 로 합침**
- 합친 텍스트를 `matthewburke/korean_sentiment` 모델에 넣어 **긍정/부정 이진 분류**
- 가장 높은 확률의 레이블과 그 score를 저장

### 출력
새 컬렉션 **`reviews_absa`** (원본 `reviews_clean`은 절대 건드리지 않음)

```json
{
  "_id": "<reviews_clean._id 와 동일>",
  "product_id": "1420730",
  "persona": "여_마른체형",
  "rating": 5,
  "absa_version": "v1_130k",
  "absa_result": {
    "소재": {"label": "긍정", "score": 0.912},
    "핏":   {"label": "부정", "score": 0.784}
  }
}
```

- `absa_result`에는 **그 문서에서 매칭된 aspect만** 들어감 (6개 다 들어가는 게 아님)
- `absa_version` 필드로 13만/30만 버전을 구분 (기본값 `v1_130k`, 30만 실행 시 `--version v2_300k`)

---

## 3. 모델 선정 근거 (요약)

Phase 1에서 3개 후보 비교 → **`matthewburke/korean_sentiment`** 선정.

| 모델 | Acc | Macro F1 | ms/row |
|---|---|---|---|
| nsmc (mBERT) | 75.6% | 74.7% | 107.9 |
| **korean_sentiment** | 74.7% | 74.3% | **63.3** ⚡ |
| huffon_nli (zero-shot) | 62.3% | 59.5% | 139.6 |

- nsmc 대비 정확도 0.9%p 차이, 속도 1.7배 빠름 → 30만 건 확장 시 ~4시간 절약
- 전체 근거와 aspect별 브레이크다운은 `DB/phase1_model_eval/REPORT.md` 참고

---

## 4. 환경 설정

### 4.1 파이썬 버전
Python 3.10 이상 (테스트는 3.10에서 수행)

### 4.2 의존성 설치

**중요**: `torch`는 **2.6 이상**이 필요합니다 (CVE-2025-32434 대응으로 구버전은 일부 모델 로드가 차단됨).

```bash
# CPU 환경
pip install --index-url https://download.pytorch.org/whl/cpu "torch>=2.6"
pip install -r requirements.txt
```

```bash
# GPU (CUDA 11.8) 환경 — Colab Pro / 서버용
pip install torch --index-url https://download.pytorch.org/whl/cu118
pip install -r requirements.txt
```

Colab에서는 `torch`가 이미 깔려 있으니 `pip install -r requirements.txt`만 돌려도 됩니다.

### 4.3 환경변수 (`.env`)

`DB/` 디렉터리(이 폴더의 **상위**)에 `.env` 파일이 있어야 합니다. 템플릿은 `.env.example` 참고:

```
MONGO_URI=mongodb+srv://...
MONGO_DB=musinsa_db
MONGO_COLLECTION_CLEAN=reviews_clean
```

출력 컬렉션 이름(`reviews_absa`)은 스크립트 안에 하드코딩돼 있고, 필요하면 CLI `--output-collection` 로 오버라이드 가능.

---

## 5. 실행 방법

### 5.1 기본 실행
```bash
cd DB/task6_absa
python task6_absa.py
```

### 5.2 스모크 테스트 (먼저 100건만)
```bash
python task6_absa.py --limit 100
```
결과가 `reviews_absa` 컬렉션에 제대로 쌓이는지 확인 후 본 실행 권장.

### 5.3 30만 건 재실행 (크롤링 추가 후)
```bash
# 완전히 새로 시작하고 싶을 때
python task6_absa.py --reset --version v2_300k

# 기존 13만 건 결과 유지하면서 추가분만 처리하고 싶을 때
python task6_absa.py --version v2_300k
# → 이미 처리된 _id는 resume 로직에 의해 자동 skip
```

### 5.4 CLI 옵션 요약

| 옵션 | 기본값 | 설명 |
|---|---|---|
| `--limit N` | 없음 | 처음 N개 문서만 처리 (테스트용) |
| `--version STR` | `v1_130k` | `absa_version` 필드 값 |
| `--batch-docs N` | 64 | 한 번에 몇 개 문서씩 모아서 pipeline에 넣을지 |
| `--batch-infer N` | 8 | transformers pipeline 내부 배치 크기 |
| `--output-collection STR` | `reviews_absa` | 출력 컬렉션 이름 |
| `--reset` | 꺼짐 | **DESTRUCTIVE**: 출력 컬렉션을 전부 삭제 후 시작 |

---

## 6. 예상 소요 시간

Phase 1 기준 `korean_sentiment`는 **CPU에서 약 63ms/row**입니다. row는 (문서 × aspect) 쌍 단위이고, 문서당 평균 약 1.4개 aspect가 매칭됩니다.

| 환경 | 13만 건 (약 13만 rows) | 30만 건 (약 30만 rows) |
|---|---|---|
| 노트북 CPU (APU) | 약 2~3시간 | 약 5~7시간 |
| Colab Pro GPU (T4/A100) | 약 10~20분 | 약 20~40분 |

**권장**: 30만 건 실행은 Colab Pro GPU에서. 13만 건은 노트북 CPU에서도 하룻밤 사이 가능.

---

## 7. 재실행·중단 복구 (Resume)

- 스크립트는 시작 시 **`reviews_absa`의 모든 `_id`를 미리 읽어서 skip 집합**을 만듭니다
- 중간에 Ctrl+C로 끊거나 네트워크가 죽어도, 다시 실행하면 **남은 문서부터 이어서** 진행
- 로그 메시지 `already processed (resume): 12,345` 가 뜨면 정상 작동 중
- `--reset`은 이 resume 동작을 무력화하므로 주의 — 정말 처음부터 다시 할 때만 사용

---

## 8. 결과 확인 (간단 쿼리)

MongoDB shell 또는 mongosh에서:

```javascript
// 총 문서 수
db.reviews_absa.countDocuments({})

// 버전별 분포
db.reviews_absa.aggregate([
  { $group: { _id: "$absa_version", count: { $sum: 1 } } }
])

// 긍정/부정 분포 (소재 aspect 기준)
db.reviews_absa.aggregate([
  { $match: { "absa_result.소재": { $exists: true } } },
  { $group: { _id: "$absa_result.소재.label", count: { $sum: 1 } } }
])

// 저신뢰도(score<0.6) 샘플 몇 개 보기
db.reviews_absa.find(
  { "absa_result.소재.score": { $lt: 0.6 } },
  { "absa_result.소재": 1, rating: 1 }
).limit(5)
```

---

## 9. 알려진 제약·주의사항

1. **이진 분류(긍/부)만 지원, 중립 없음.**
   - Phase 1에서 의도적으로 폐기한 결정. threshold 자의성을 피하기 위함.
   - 필요하면 후처리에서 `score < 0.55`인 것을 "저신뢰"로 따로 분류 가능 (스크립트는 그대로 두세요).

2. **원본 `reviews_clean`은 절대 수정하지 않음.**
   - 모든 결과는 `reviews_absa` 별도 컬렉션에만 씀. 원본 보호 원칙.

3. **모델 입력 길이 제한.**
   - `truncation=True`로 pipeline이 자동 처리. 매우 긴 aspect_text는 앞부분 ~512 토큰만 사용.

4. **배치 실패 시 그 배치만 skip + 로그 출력.**
   - 개별 문서 실패가 전체 실행을 멈추지 않음. 스크립트 종료 후 `실패 배치 N` 수치를 확인.

5. **평가 스코프.**
   - 이 스크립트 자체는 품질 평가를 하지 않음. 품질 근거는 Phase 1 보고서 참고.
   - 본 실행 후에는 `rating × absa_result` 교차 검증으로 대규모 sanity check 가능 (Task 7 또는 분석팀에서).

---

## 10. Phase 1 → Task 6 맥락 요약

```
[완료] Phase 1 (DB/phase1_model_eval/)
  - 3개 후보 모델 비교
  - matthewburke/korean_sentiment 선정

[이 폴더] Task 6 본 실행
  - 선정 모델로 전체 문서 ABSA
  - reviews_absa 컬렉션 생성

[다음] Task 7 (분석팀)
  - reviews_absa + persona를 조인해 페르소나×aspect 감성 집계
  - 최종 히트맵/레이더/스택바 시각화
```

---

## 11. 문의

예원에게 바로 연락. 결과 샘플 품질이 이상하거나 실행 중 전에 없던 에러가 나면 로그 전체와 함께 공유 부탁드립니다.
