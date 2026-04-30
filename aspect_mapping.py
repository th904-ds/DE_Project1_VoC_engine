"""키워드 매핑 (Task 5) — 스트림릿 통합용 모듈.

이 모듈은 두 가지 용도를 모두 지원한다:

1. **스트림릿 단건 처리 (import 후 함수 호출)**
   - 정제된 리뷰 텍스트(``text_clean``) → aspect별 매칭 문장
   - 함수: ``split_sentences``, ``extract_aspects``

2. **MongoDB 배치 파이프라인 (스크립트 실행)**
   - reviews_clean 의 모든 문서에 ``review_data.aspects_sentences``,
     ``review_data.aspects_mentioned`` 필드를 추가
   - 실행: ``python aspect_mapping.py``

파이프라인 위치
---------------
``preprocessing.py`` 가 ``review_data.text_clean`` 까지 채워둔 상태에서 실행한다.
같은 ``reviews_clean`` 문서에 aspect 필드를 in-place 로 덧붙인다.

스트림릿 사용 예
----------------
.. code-block:: python

   from preprocessing import clean_review_text
   from aspect_mapping import extract_aspects

   text_clean = clean_review_text(user_input)
   aspects = extract_aspects(text_clean)
   # → {"소재": ["원단이 부드럽고 두께감 적당해요"], "사이즈": [...], ...}

설계 결정 (스펙 대비 의도적 보정)
--------------------------------
- 색상 키워드에서 ``"색"`` 단독 제거 — ``"색다른"``·``"특색"`` 등 false positive
- 사이즈 키워드는 어간 ``"크"``·``"맞"`` 사용 — 한국어 활용형 커버리지 확보
- 문장 분리에 ``,`` + 한국어 연결어미(``는데``·``은데``·``지만`` 등) 포함
"""
from __future__ import annotations

import os
import re
import sys
from collections import Counter
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure
from pymongo.operations import UpdateOne

load_dotenv()  # cwd 부터 위로 탐색 + 환경변수 그대로 사용

# ---------------------------------------------------------------------------
# Aspect 사전 — 6개 카테고리
# ---------------------------------------------------------------------------
ASPECTS: dict[str, list[str]] = {
    "소재": ["소재", "원단", "재질", "천", "면", "폴리", "두께", "얇", "두껍"],
    "핏":   ["핏", "핏감", "실루엣", "라인", "몸에 딱", "여유"],
    "사이즈": ["사이즈", "크기", "작", "크", "맞", "치수"],
    "색상": ["색상", "색깔", "컬러", "진하", "연하", "사진과"],
    "가격": ["가격", "가성비", "값", "비싸", "저렴", "합리적"],
    "배송": ["배송", "배달", "포장", "빠르", "느리"],
}

# 마침표·느낌표·물음표·줄바꿈·쉼표 + 한국어 연결어미(음절 단위)
SENT_SPLIT: re.Pattern[str] = re.compile(
    r"[.!?\n,]+|(?:는데|은데|인데|지만|이지만|니까)(?=\s|[가-힣])"
)


# ---------------------------------------------------------------------------
# 단건 처리 함수 — 스트림릿이 직접 import해서 사용
# ---------------------------------------------------------------------------
def split_sentences(text: str) -> list[str]:
    """정제된 리뷰 텍스트를 문장 단위로 분리.

    분리 기준
    ---------
    - 문장부호: ``.`` ``!`` ``?`` ``\\n`` ``,``
    - 한국어 연결어미: ``는데``, ``은데``, ``인데``, ``지만``, ``이지만``, ``니까``
    """
    if not isinstance(text, str) or not text:
        return []
    parts = SENT_SPLIT.split(text)
    return [s.strip() for s in parts if s.strip()]


def extract_aspects(text: str) -> dict[str, list[str]]:
    """정제된 리뷰 텍스트 → ``{aspect: [매칭 문장, ...]}`` dict 반환.

    매칭이 하나도 없으면 빈 dict 반환. 한 문장이 여러 aspect 키워드를
    포함하면 해당 모든 aspect 에 동일 문장이 들어간다.
    """
    sentences = split_sentences(text)
    result: dict[str, list[str]] = {}
    for sent in sentences:
        for aspect, keywords in ASPECTS.items():
            if any(kw in sent for kw in keywords):
                result.setdefault(aspect, []).append(sent)
    return result


def aspects_mentioned(text: str) -> list[str]:
    """정제된 리뷰 텍스트에서 언급된 aspect 키 리스트만 반환 (순서 유지)."""
    return list(extract_aspects(text).keys())


# ---------------------------------------------------------------------------
# 이하 — MongoDB 배치 파이프라인 (스크립트 실행 시에만 사용)
# 스트림릿에서는 위 함수들만 import하면 충분.
# ---------------------------------------------------------------------------
BATCH: int = 1000


def _get_collection() -> tuple[MongoClient, Collection]:
    uri: str | None = os.environ.get("MONGO_URI")
    db_name: str = os.environ.get("MONGO_DB", "musinsa_db")
    coll_name: str = os.environ.get("MONGO_COLLECTION_CLEAN", "reviews_clean")

    if not uri:
        raise RuntimeError("MONGO_URI not set in .env")

    client: MongoClient = MongoClient(uri, serverSelectionTimeoutMS=5_000)
    try:
        client.admin.command("ping")
    except ConnectionFailure as e:
        raise RuntimeError(f"MongoDB connection failed: {e}") from e

    return client, client[db_name][coll_name]


def run_aspect_mapping() -> None:
    client, collection = _get_collection()
    try:
        total: int = collection.count_documents({})
        print(f"대상: {total:,}건")

        operations: list[UpdateOne] = []
        aspect_freq: Counter = Counter()
        processed = 0
        matched = 0

        for doc in collection.find({}, {"review_data.text_clean": 1}):
            rd = doc.get("review_data") or {}
            text = rd.get("text_clean", "") if isinstance(rd, dict) else ""
            if not isinstance(text, str):
                text = ""

            mapped = extract_aspects(text)
            mentioned = list(mapped.keys())

            for asp in mentioned:
                aspect_freq[asp] += 1
            if mentioned:
                matched += 1

            operations.append(
                UpdateOne(
                    {"_id": doc["_id"]},
                    {
                        "$set": {
                            "review_data.aspects_sentences": mapped,
                            "review_data.aspects_mentioned": mentioned,
                        }
                    },
                )
            )

            if len(operations) == BATCH:
                collection.bulk_write(operations, ordered=False)
                processed += len(operations)
                operations = []
                print(f"  {processed:,} / {total:,}", end="\r")

        if operations:
            collection.bulk_write(operations, ordered=False)
            processed += len(operations)

        match_pct = matched / total * 100 if total else 0
        print(f"\n완료: {processed:,}건 처리 / aspect 매칭: {matched:,}건 ({match_pct:.1f}%)")

        print("\n[Aspect 언급 빈도]")
        for asp, cnt in sorted(aspect_freq.items(), key=lambda x: -x[1]):
            pct = cnt / total * 100 if total else 0
            print(f"  {asp:<6}: {cnt:>7,}건  ({pct:.1f}%)")
    finally:
        client.close()


if __name__ == "__main__":
    run_aspect_mapping()
