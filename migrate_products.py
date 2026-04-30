"""
products 컬렉션 마이그레이션.
  - parsed_시즌 필드 전체 삭제
  - parsed_성별 / parsed_누적판매 일괄 추가

실행:
    python migrate_products.py
"""
from __future__ import annotations

import os
import re
import sys
from typing import Optional

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.operations import UpdateOne

sys.stdout.reconfigure(encoding="utf-8")
load_dotenv()

BATCH = 500


# ── 파싱 헬퍼 ─────────────────────────────────────────────────────────────────

def parse_description_fields(desc_raw: str) -> dict:
    result = {"성별": None, "누적판매": None}
    if not desc_raw:
        return result
    lines = [ln.strip() for ln in desc_raw.splitlines() if ln.strip()]
    for i, line in enumerate(lines):
        if i + 1 >= len(lines):
            break
        if line == "성별":
            result["성별"] = lines[i + 1]
        elif line == "누적판매":
            result["누적판매"] = lines[i + 1]
    return result


def normalize_gender(g: Optional[str]) -> Optional[str]:
    if not g:
        return None
    g = g.strip()
    if "남녀" in g or "공용" in g or "unisex" in g.lower():
        return "남녀공용"
    if "여" in g:
        return "여성"
    if "남" in g:
        return "남성"
    return g


def parse_sales_count(s: Optional[str]) -> Optional[int]:
    """
    한국어 숫자 표현 → 정수 변환.
    예) '25만 개 이상' → 250000
        '2.5만'       → 25000
        '1만 2천'     → 12000
        '1,234개'     → 1234
    """
    if not s:
        return None
    s = s.strip()
    total = 0
    found = False

    m = re.search(r'(\d+(?:\.\d+)?)\s*억', s)
    if m:
        total += round(float(m.group(1)) * 100_000_000)
        found = True

    m = re.search(r'(\d+(?:\.\d+)?)\s*만', s)
    if m:
        total += round(float(m.group(1)) * 10_000)
        found = True

    m = re.search(r'(\d+(?:\.\d+)?)\s*천', s)
    if m:
        total += round(float(m.group(1)) * 1_000)
        found = True

    if found:
        return total

    # 순수 숫자 (콤마 포함)
    digits = re.sub(r'[^\d]', '', s)
    return int(digits) if digits else None


# ── 마이그레이션 ──────────────────────────────────────────────────────────────

def run():
    uri = os.environ.get("MONGO_URI")
    if not uri:
        print("❌ MONGO_URI가 .env에 없습니다.")
        return

    client = MongoClient(uri, serverSelectionTimeoutMS=10_000)
    try:
        client.admin.command("ping")
        print("✅ MongoDB 연결 성공")
    except Exception as e:
        print(f"❌ 연결 실패: {e}")
        return

    coll = client["musinsa_db"]["products"]
    total = coll.count_documents({})
    print(f"대상: {total:,}건")

    # ① parsed_시즌 필드 전체 삭제
    result = coll.update_many({}, {"$unset": {"parsed_시즌": ""}})
    print(f"parsed_시즌 제거: {result.modified_count:,}건")

    # ② parsed_성별 / parsed_누적판매 일괄 업데이트
    ops: list[UpdateOne] = []
    processed = 0
    skipped = 0

    for doc in coll.find({}, {"_id": 1, "description_raw": 1}):
        desc_raw = doc.get("description_raw") or ""
        fields = parse_description_fields(desc_raw)
        gender_norm = normalize_gender(fields.get("성별"))
        sales_count = parse_sales_count(fields.get("누적판매"))

        if gender_norm is None:
            skipped += 1
            continue

        ops.append(UpdateOne(
            {"_id": doc["_id"]},
            {"$set": {
                "parsed_성별": gender_norm,
                "parsed_누적판매": sales_count,
            }},
        ))

        if len(ops) == BATCH:
            coll.bulk_write(ops, ordered=False)
            processed += len(ops)
            ops = []
            print(f"  {processed:,} / {total:,} 처리 중...", end="\r")

    if ops:
        coll.bulk_write(ops, ordered=False)
        processed += len(ops)

    client.close()
    print(f"\n완료: {processed:,}건 업데이트, {skipped:,}건 스킵 (description_raw 없음)")


if __name__ == "__main__":
    run()
