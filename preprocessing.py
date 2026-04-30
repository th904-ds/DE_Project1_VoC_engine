"""기본 전처리 (Task 1~4) — 스트림릿 통합용 모듈.

이 모듈은 두 가지 용도를 모두 지원한다:

1. **스트림릿 단건 처리 (import 후 함수 호출)**
   - 사용자가 입력한 리뷰 텍스트를 정제 → 페르소나 산출
   - 함수: ``clean_review_text``, ``compute_persona``

2. **MongoDB 배치 파이프라인 (스크립트 실행)**
   - reviews → reviews_clean 슬림 스키마 재구성 + dedup + persona + text_clean
   - 실행: ``python preprocessing.py``

스트림릿 사용 예
----------------
.. code-block:: python

   from preprocessing import clean_review_text, compute_persona

   cleaned = clean_review_text(user_input)
   persona = compute_persona(
       gender="여",
       height_cm=165.0,
       weight_kg=55.0,
       size_raw="M",
   )

데이터 파이프라인 흐름
---------------------
- 원본 ``reviews`` 컬렉션은 절대 수정하지 않는다.
- ``reviews_clean`` 은 항상 drop 후 재생성 (멱등성 보장).
- ``review_data.text`` 평문은 reviews_clean 에 저장하지 않는다 (용량 절약).
  필요할 때만 ``zlib.decompress(review_data.text_compressed)`` 로 복원.
"""
from __future__ import annotations

import os
import re
import sys
import zlib
from collections import Counter
from pathlib import Path
from typing import Mapping

sys.stdout.reconfigure(encoding="utf-8")

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure
from pymongo.operations import DeleteOne, UpdateOne

load_dotenv()  # cwd 부터 위로 탐색 + 환경변수 그대로 사용

# ---------------------------------------------------------------------------
# 단건 처리 함수 — 스트림릿이 직접 import해서 사용
# ---------------------------------------------------------------------------
URL_PATTERN: re.Pattern[str] = re.compile(r"https?://\S+")
KOREAN_ONLY: re.Pattern[str] = re.compile(r"[^가-힣ᄀ-ᇿ㄰-㆏0-9\s]")
MULTI_SPACE: re.Pattern[str] = re.compile(r"\s+")


def clean_review_text(text: str) -> str:
    """리뷰 원문 → 정제된 평문(text_clean) 반환.

    규칙
    -----
    1. ``http(s)://`` URL 제거
    2. 한글·숫자·공백 외 문자 제거 (이모지·영어·특수문자 등)
    3. 연속 공백 → 단일 공백
    4. 앞뒤 공백 제거
    """
    if not isinstance(text, str):
        return ""
    text = URL_PATTERN.sub(" ", text)
    text = KOREAN_ONLY.sub(" ", text)
    text = MULTI_SPACE.sub(" ", text)
    return text.strip()


# ---------------------------------------------------------------------------
# 페르소나 산출 — Task 3 핵심 로직 (단건)
# ---------------------------------------------------------------------------
SIZE_MAP: dict[str, str] = {
    "xs": "소형", "xxs": "소형",
    "44": "소형", "55": "소형",
    "85": "소형", "80": "소형",
    "s": "소형",
    "m": "중형", "90": "중형", "95": "중형",
    "l": "대형", "100": "대형",
    "xl": "특대형", "xxl": "특대형", "3xl": "특대형",
    "105": "특대형", "110": "특대형", "115": "특대형", "120": "특대형",
    "free": "중형",
}


def _bmi_to_body_type(bmi: float) -> str:
    if bmi < 18.5:
        return "마른체형"
    if bmi < 23.0:
        return "보통체형"
    if bmi < 25.0:
        return "통통체형"
    return "풍만체형"


def _size_to_body_type(size_raw: str) -> str:
    normalized = size_raw.strip().lower()
    for key in SIZE_MAP:
        if key in normalized:
            return SIZE_MAP[key]
    return "중형"


def compute_persona(
    *,
    gender: str | None,
    height_cm: float | None,
    weight_kg: float | None,
    size_raw: str | None,
) -> str:
    """성별·신장·체중·구매 사이즈로 페르소나 라벨 반환.

    하이브리드 로직
    ---------------
    1. 신장·체중 모두 있고 양수면 → BMI 기반 (``성별_체형``)
    2. 그렇지 않으면 → 구매 사이즈 기반 (``성별or unknown_체형``)
    3. 사이즈도 없으면 → ``unknown``
    """
    if gender and isinstance(height_cm, (int, float)) and isinstance(weight_kg, (int, float)):
        try:
            bmi = float(weight_kg) / ((float(height_cm) / 100.0) ** 2)
            return f"{gender}_{_bmi_to_body_type(bmi)}"
        except (ValueError, ZeroDivisionError):
            pass

    if size_raw:
        body_type = _size_to_body_type(size_raw)
        prefix = gender if gender else "unknown"
        return f"{prefix}_{body_type}"

    return "unknown"


# ---------------------------------------------------------------------------
# (보조) text_compressed bytes 해제 — DB에 저장된 압축 텍스트 복원
# ---------------------------------------------------------------------------
def decompress_text(compressed: object) -> str:
    """``review_data.text_compressed`` (zlib bytes) → 평문 텍스트.

    bytes 가 아니거나 해제 실패 시 빈 문자열 반환.
    """
    if not isinstance(compressed, (bytes, bytearray)):
        return ""
    try:
        return zlib.decompress(bytes(compressed)).decode("utf-8")
    except (zlib.error, UnicodeDecodeError):
        return ""


# ---------------------------------------------------------------------------
# 이하 — MongoDB 배치 파이프라인 (스크립트 실행 시에만 사용)
# 스트림릿에서는 위 함수들만 import하면 충분.
# ---------------------------------------------------------------------------
BATCH: int = 1000


def _connect() -> tuple[MongoClient, Collection, Collection]:
    uri: str | None = os.environ.get("MONGO_URI")
    db_name: str = os.environ.get("MONGO_DB", "musinsa_db")
    src_name: str = os.environ.get("MONGO_COLLECTION", "reviews")
    dst_name: str = os.environ.get("MONGO_COLLECTION_CLEAN", "reviews_clean")

    if not uri:
        raise RuntimeError("MONGO_URI not set in .env")

    client: MongoClient = MongoClient(uri, serverSelectionTimeoutMS=10_000)
    try:
        client.admin.command("ping")
    except ConnectionFailure as e:
        raise RuntimeError(f"MongoDB connection failed: {e}") from e

    db = client[db_name]
    return client, db[src_name], db[dst_name]


def _slim(doc: Mapping[str, object]) -> dict[str, object]:
    """원본 nested 문서에서 분석에 필요한 최소 필드만 뽑아 slim dict 반환.

    제거 필드: review_id, nickname, level, option_raw, type, has_photo,
    like_count, photo_urls.
    """
    review_data = doc.get("review_data")
    user_info = doc.get("user_info")
    purchase_info = doc.get("purchase_info")

    rd_in: Mapping[str, object] = review_data if isinstance(review_data, Mapping) else {}
    ui_in: Mapping[str, object] = user_info if isinstance(user_info, Mapping) else {}
    pi_in: Mapping[str, object] = purchase_info if isinstance(purchase_info, Mapping) else {}

    return {
        "_id": doc["_id"],
        "product_id": doc.get("product_id"),
        "date": doc.get("date"),
        "rating": doc.get("rating"),
        "purchase_info": {
            "size": pi_in.get("size", ""),
            "color": pi_in.get("color", ""),
        },
        "user_info": {
            "encrypted_id": ui_in.get("encrypted_id", ""),
            "gender": ui_in.get("gender"),
            "height_cm": ui_in.get("height_cm"),
            "weight_kg": ui_in.get("weight_kg"),
        },
        "review_data": {
            "text_compressed": rd_in.get("text_compressed"),
            "is_compressed": rd_in.get("is_compressed", False),
        },
    }


def step_copy(src: Collection, dst: Collection) -> None:
    print("\n[Step 1] reviews → reviews_clean 복사 (slim)")
    db = src.database
    if dst.name in db.list_collection_names():
        print(f"  '{dst.name}' 기존 데이터 삭제 후 재생성")
        dst.drop()

    total: int = src.count_documents({})
    print(f"  복사 대상: {total:,}건")

    projection: dict[str, int] = {
        "_id": 1,
        "product_id": 1,
        "date": 1,
        "rating": 1,
        "purchase_info.size": 1,
        "purchase_info.color": 1,
        "user_info.encrypted_id": 1,
        "user_info.gender": 1,
        "user_info.height_cm": 1,
        "user_info.weight_kg": 1,
        "review_data.text_compressed": 1,
        "review_data.is_compressed": 1,
    }

    copied = 0
    batch: list[dict[str, object]] = []
    for doc in src.find({}, projection, batch_size=BATCH):
        batch.append(_slim(doc))
        if len(batch) == BATCH:
            dst.insert_many(batch)
            copied += len(batch)
            print(f"  {copied:,} / {total:,}", end="\r")
            batch = []
    if batch:
        dst.insert_many(batch)
        copied += len(batch)
    print(f"\n  완료: {copied:,}건 복사")


def step_dedup(dst: Collection) -> None:
    print("\n[Step 2] 중복 제거 (encrypted_id + text_compressed bytes)")
    before: int = dst.count_documents({})
    print(f"  Before: {before:,}")

    from collections import defaultdict
    groups: dict[tuple[str, bytes], list[tuple[object, object]]] = defaultdict(list)

    fetched = 0
    for doc in dst.find(
        {},
        {"_id": 1, "date": 1, "user_info.encrypted_id": 1,
         "review_data.text_compressed": 1},
        batch_size=2000,
    ):
        ui = doc.get("user_info") or {}
        rd = doc.get("review_data") or {}
        enc_id: str = str(ui.get("encrypted_id", ""))
        tc = rd.get("text_compressed")
        tc_bytes: bytes = bytes(tc) if isinstance(tc, (bytes, bytearray)) else b""
        groups[(enc_id, tc_bytes)].append((doc.get("date") or "", doc["_id"]))
        fetched += 1
        if fetched % 10000 == 0:
            print(f"  스캔: {fetched:,} / {before:,}", end="\r")
    print(f"\n  스캔 완료: {fetched:,}건, 고유 키: {len(groups):,}")

    delete_ids: list[object] = []
    for docs in groups.values():
        if len(docs) <= 1:
            continue
        docs_sorted = sorted(docs, key=lambda d: str(d[0]))
        for _, _id in docs_sorted[1:]:
            delete_ids.append(_id)

    if not delete_ids:
        print("  중복 없음.")
        return
    print(f"  삭제 대상: {len(delete_ids):,}건")
    operations = [DeleteOne({"_id": _id}) for _id in delete_ids]
    removed = 0
    for i in range(0, len(operations), BATCH):
        result = dst.bulk_write(operations[i : i + BATCH], ordered=False)
        removed += result.deleted_count
    print(f"  제거: {removed:,}건  After: {dst.count_documents({}):,}")


def step_persona(dst: Collection) -> None:
    print("\n[Step 3] 페르소나 태깅")
    total: int = dst.count_documents({})
    print(f"  대상: {total:,}건")

    operations: list[UpdateOne] = []
    distribution: Counter = Counter()
    processed = 0

    for doc in dst.find({}, {"user_info": 1, "purchase_info": 1}):
        ui = doc.get("user_info") or {}
        pi = doc.get("purchase_info") or {}
        gender_raw = ui.get("gender") if isinstance(ui, Mapping) else None
        gender: str | None = gender_raw if isinstance(gender_raw, str) and gender_raw else None

        height = ui.get("height_cm") if isinstance(ui, Mapping) else None
        weight = ui.get("weight_kg") if isinstance(ui, Mapping) else None
        height_f: float | None = float(height) if isinstance(height, (int, float)) else None
        weight_f: float | None = float(weight) if isinstance(weight, (int, float)) else None

        size_raw = pi.get("size") if isinstance(pi, Mapping) else None
        size_str: str | None = size_raw if isinstance(size_raw, str) else None

        persona = compute_persona(
            gender=gender,
            height_cm=height_f,
            weight_kg=weight_f,
            size_raw=size_str,
        )
        distribution[persona] += 1
        operations.append(UpdateOne({"_id": doc["_id"]}, {"$set": {"persona": persona}}))

        if len(operations) == BATCH:
            dst.bulk_write(operations, ordered=False)
            processed += len(operations)
            operations = []
            print(f"  {processed:,} / {total:,}", end="\r")

    if operations:
        dst.bulk_write(operations, ordered=False)
        processed += len(operations)

    print(f"\n  완료: {processed:,}건 태깅")
    print("\n  [페르소나 분포 Top 10]")
    for persona, count in sorted(distribution.items(), key=lambda x: -x[1])[:10]:
        pct = count / total * 100 if total else 0
        print(f"    {persona:<20}: {count:>6,}  ({pct:.1f}%)")


def step_text_clean(dst: Collection) -> None:
    print("\n[Step 4] 텍스트 전처리 (text_compressed → text_clean)")
    total: int = dst.count_documents({})
    print(f"  대상: {total:,}건")

    operations: list[UpdateOne] = []
    processed = 0
    decode_failures = 0
    for doc in dst.find({}, {"review_data.text_compressed": 1}):
        rd = doc.get("review_data") or {}
        tc = rd.get("text_compressed") if isinstance(rd, Mapping) else None
        text = decompress_text(tc)
        if not text:
            decode_failures += 1
        operations.append(
            UpdateOne(
                {"_id": doc["_id"]},
                {"$set": {"review_data.text_clean": clean_review_text(text)}},
            )
        )
        if len(operations) == BATCH:
            dst.bulk_write(operations, ordered=False)
            processed += len(operations)
            operations = []
            print(f"  {processed:,} / {total:,}", end="\r")
    if operations:
        dst.bulk_write(operations, ordered=False)
        processed += len(operations)
    print(f"\n  완료: {processed:,}건 적용 "
          f"(decompress 실패/빈텍스트: {decode_failures:,})")


def main() -> None:
    client, src, dst = _connect()
    print("MongoDB 연결 성공")
    try:
        step_copy(src, dst)
        step_dedup(dst)
        step_persona(dst)
        step_text_clean(dst)
        final_count: int = dst.count_documents({})
        print("\n=== 파이프라인 완료 ===")
        print(f"reviews_clean 최종: {final_count:,}건")
    finally:
        client.close()


if __name__ == "__main__":
    main()
