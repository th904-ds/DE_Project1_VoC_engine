"""
무신사(Musinsa) 리뷰 전체보기 Playwright 크롤러 - 최종 정규화 버전
================================================================
핵심 전략:
  무신사 리뷰 전체보기 페이지는 가상 스크롤 방식이므로,
  DOM 대신 네트워크 응답(JSON)을 인터셉트해서 리뷰를 수집한다.

이번 최종 버전 특징:
  1) 리뷰 raw 스키마 2종 모두 대응
     - 구형 스키마: pastDate / userProfile / userProfileName / images[].image
     - 신형 스키마: createDate / userProfileInfo / images[].imageUrl
  2) 실행 마지막에 항목별 데이터 존재 비율만 출력

실행 방법:
    pip install playwright playwright-stealth
    playwright install chromium
    python Crawler_musinsa_review_final.py
"""

import asyncio
import json
import os
import re
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from playwright.async_api import async_playwright, Response


# ──────────────────────────────────────────────
# 설정값
# ──────────────────────────────────────────────
# 상품 URL -> 스냅,후기 탭 -> 아래 스크롤해서 N개 후기 전체 보기 클릭 -> 이 페이지의 URL을 넣어야 함!!
HEADLESS = False
SLOW_MO = 100
SCROLL_PAUSE = 1.2
MAX_SCROLL_NO_CHANGE = 15
MAX_REVIEWS = None  # None이면 전체 리뷰 수집

OUTPUT_DIR = r"C:\Users\김태희\Desktop\대학교 3-1\DE_Project1\Crawler\outputs"


# ──────────────────────────────────────────────
# 유틸리티
# ──────────────────────────────────────────────

def ensure_dirs() -> None:
    """출력 폴더를 미리 만든다."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)



def extract_product_id(url: str) -> str:
    """URL에서 기본 상품 번호를 추출한다."""
    m = re.search(r"/(?:goods|products)/(\d+)", url)
    return m.group(1) if m else "unknown"



def safe_int(v: Any, default: Optional[int] = None) -> Optional[int]:
    """정수 변환 실패 시 default를 반환한다."""
    if v is None:
        return default
    try:
        return int(v)
    except (ValueError, TypeError):
        return default



def safe_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    """실수 변환 실패 시 default를 반환한다."""
    if v is None:
        return default
    try:
        return float(v)
    except (ValueError, TypeError):
        return default



def clean_text(t: Any) -> str:
    """여러 줄/중복 공백을 한 줄 문자열로 정리한다."""
    if not t:
        return ""
    return re.sub(r"\s+", " ", str(t).strip())



def normalize_date_string(value: Any) -> Optional[str]:
    """
    날짜/시간 문자열을 YYYY-MM-DD 형태로 정규화한다.

    지원 예시:
      2026-03-30T22:46:11.000+09:00
      2025-04-24 14:17:39
      2025-04-24
    """
    if not value:
        return None

    text = clean_text(value)
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", text)
    if m:
        return m.group(1)
    return None



def pct(n: int, total: int) -> str:
    """백분율 문자열 포맷."""
    if total <= 0:
        return "0.0%"
    return f"{n / total * 100:.1f}%"


# ──────────────────────────────────────────────
# 문자열 수집 / 체형 fallback 후보 탐색
# ──────────────────────────────────────────────

def _collect_candidate_strings(obj: Any, bucket: List[Tuple[str, str]], path: str = "") -> None:
    """raw 내부의 모든 문자열을 재귀적으로 수집한다."""
    if obj is None:
        return

    if isinstance(obj, dict):
        for k, v in obj.items():
            next_path = f"{path}.{k}" if path else str(k)
            _collect_candidate_strings(v, bucket, next_path)
        return

    if isinstance(obj, list):
        for i, item in enumerate(obj):
            next_path = f"{path}[{i}]"
            _collect_candidate_strings(item, bucket, next_path)
        return

    if isinstance(obj, str):
        s = clean_text(obj)
        if s:
            bucket.append((path, s))



def _dedup_preserve_order(items: List[Any]) -> List[Any]:
    """순서는 유지하면서 중복만 제거한다."""
    seen = set()
    out = []
    for item in items:
        key = json.dumps(item, ensure_ascii=False, sort_keys=True) if isinstance(item, (dict, list, tuple)) else item
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out



def collect_all_strings(raw: Dict[str, Any]) -> List[Tuple[str, str]]:
    """리뷰 raw 객체 안의 모든 문자열을 path와 함께 수집한다."""
    bucket: List[Tuple[str, str]] = []
    _collect_candidate_strings(raw, bucket)
    return _dedup_preserve_order(bucket)



def get_profile_candidate_strings(raw: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    문자열 fallback용 체형 후보를 모은다.

    userProfileInfo / userProfile 둘 다 없거나 불완전할 때만 보조적으로 사용한다.
    """
    interesting: List[Tuple[str, str]] = []

    for path, s in collect_all_strings(raw):
        path_l = path.lower()
        has_measure = bool(re.search(r"(?<!\d)\d{2,3}\s*cm\b|(?<!\d)\d{2,3}\s*kg\b", s, re.I))
        path_suggests_profile = any(tok in path_l for tok in [
            "userprofile", "profile", "reviewsex", "userheight", "userweight"
        ])

        if has_measure or path_suggests_profile:
            interesting.append((path, s))

    return _dedup_preserve_order(interesting)


# ──────────────────────────────────────────────
# 필드별 파서
# ──────────────────────────────────────────────

def parse_user_profile(profile_str: str) -> Tuple[Optional[str], Optional[int], Optional[int]]:
    """
    문자열형 체형정보를 파싱한다.

    예시:
      - 여성 · 160cm · 47kg
      - 152cm · 50kg
      - 남성 · 179cm · 80kg
    """
    if not profile_str:
        return None, None, None

    text = clean_text(profile_str)
    gender, height, weight = None, None, None

    g = re.search(r"\b(남성|여성)\b", text)
    if g:
        gender = g.group(1)

    h = re.search(r"(?<!\d)(\d{2,3})\s*cm\b", text, re.I)
    if h:
        val = int(h.group(1))
        if 100 <= val <= 250:
            height = val

    w = re.search(r"(?<!\d)(\d{2,3})\s*kg\b", text, re.I)
    if w:
        val = int(w.group(1))
        if 20 <= val <= 200:
            weight = val

    return gender, height, weight



def parse_review_date(raw: Dict[str, Any]) -> Optional[str]:
    """
    리뷰 날짜를 파싱한다.

    우선순위:
      1) 구형 스키마 top-level pastDate
      2) 신형 스키마 top-level createDate

    주의:
      goods.goodsCreateDate는 상품 생성일이므로 리뷰 날짜로 쓰지 않는다.
    """
    for value in [raw.get("pastDate"), raw.get("createDate")]:
        normalized = normalize_date_string(value)
        if normalized:
            return normalized
    return None



def parse_option(raw: Dict[str, Any]) -> Optional[str]:
    """
    구매 옵션을 파싱한다.

    최종 출력 컬럼명은 사용자가 지정한 대로 option을 사용한다.
    """
    for k in ["goodsOption", "optionName", "option", "goodsOptionText", "optionContent"]:
        v = raw.get(k)
        if isinstance(v, str) and clean_text(v):
            return clean_text(v)
    return None



def parse_review_type(raw: Dict[str, Any]) -> Optional[str]:
    """
    리뷰 타입 코드를 파싱한다.

    예:
      - style
      - general
    """
    return clean_text(raw.get("type")) or None



def parse_rating(raw: Dict[str, Any]) -> Optional[float]:
    """평점을 float으로 변환한다."""
    return safe_float(raw.get("grade"))



def parse_review_text(raw: Dict[str, Any]) -> Optional[str]:
    """리뷰 본문을 파싱한다."""
    return clean_text(raw.get("content")) or None



def parse_reviewer_nickname(raw: Dict[str, Any]) -> Optional[str]:
    """
    리뷰어 닉네임을 파싱한다.

    우선순위:
      1) 구형 스키마 userProfileName
      2) 신형 스키마 userProfileInfo.userNickName
    """
    name = clean_text(raw.get("userProfileName")) or None
    if name:
        return name

    info = raw.get("userProfileInfo")
    if isinstance(info, dict):
        name = clean_text(info.get("userNickName")) or None
        if name:
            return name

    return None



def parse_reviewer_level(raw: Dict[str, Any]) -> Optional[int]:
    """
    리뷰어 레벨을 파싱한다.

    신형 스키마의 userProfileInfo.userLevel에 들어간다.
    """
    info = raw.get("userProfileInfo")
    if isinstance(info, dict):
        return safe_int(info.get("userLevel"))
    return None



def parse_profile(raw: Dict[str, Any]) -> Tuple[Optional[str], Optional[int], Optional[int]]:
    """
    체형 정보를 파싱한다.

    우선순위:
      1) 신형 스키마 userProfileInfo.reviewSex / userHeight / userWeight
      2) 구형 스키마 userProfile 문자열
      3) 예외적인 경우에만 문자열 fallback
    """
    show_profile = raw.get("showUserProfile", True)
    if not show_profile:
        return None, None, None

    info = raw.get("userProfileInfo")
    if isinstance(info, dict):
        gender = info.get("reviewSex") or info.get("userSex") or info.get("gender")
        gender = clean_text(gender) if isinstance(gender, str) else None
        if gender not in {"남성", "여성"}:
            gender = None

        height = safe_int(info.get("userHeight"))
        if height is not None and not (100 <= height <= 250):
            height = None

        weight = safe_int(info.get("userWeight"))
        if weight is not None and not (20 <= weight <= 200):
            weight = None

        if any(x is not None for x in (gender, height, weight)):
            return gender, height, weight

    up = raw.get("userProfile")
    if isinstance(up, str) and clean_text(up):
        g, h, w = parse_user_profile(up)
        if any(x is not None for x in (g, h, w)):
            return g, h, w

    for _path, text in get_profile_candidate_strings(raw):
        g, h, w = parse_user_profile(text)
        # fallback에서는 성별만 있는 값은 채택하지 않는다.
        if g is not None and h is None and w is None:
            continue
        if any(x is not None for x in (g, h, w)):
            return g, h, w

    return None, None, None



def parse_photo_urls(raw: Dict[str, Any]) -> List[str]:
    """
    리뷰 이미지 URL 목록을 파싱한다.

    스키마 2종 모두 대응:
      - 구형: images[].image
      - 신형: images[].imageUrl
    """
    photo_urls: List[str] = []

    images = raw.get("images") or []
    if not isinstance(images, list):
        return photo_urls

    for img in images:
        if not isinstance(img, dict):
            continue
        path = img.get("image") or img.get("imageUrl") or ""
        if not path:
            continue
        url = path if str(path).startswith("http") else f"https://image.musinsa.com{path}"
        photo_urls.append(url)

    return photo_urls



def parse_product_id_from_goods(raw: Dict[str, Any], fallback_product_id: str) -> str:
    """
    리뷰에 들어 있는 goods 객체에서 상품 ID를 뽑는다.

    같은 리뷰 페이지에서도 goodsNo가 다른 리뷰가 섞일 수 있으므로,
    URL의 상품 ID보다 goods.goodsNo를 우선한다.
    """
    goods = raw.get("goods") if isinstance(raw.get("goods"), dict) else {}
    return str(goods.get("goodsNo") or fallback_product_id)


# ──────────────────────────────────────────────
# 리뷰 단위 정규화
# ──────────────────────────────────────────────

def parse_review(raw: Dict[str, Any], fallback_product_id: str) -> Dict[str, Any]:
    """
    raw 리뷰 객체를 사용자가 지정한 최종 정규 스키마로 변환한다.

    최종 출력 컬럼:
      - product_id
      - review_id
      - encrypted_user_id
      - reviewer_nickname
      - date
      - rating
      - option
      - reviewer_level
      - reviewer_gender
      - reviewer_height_cm
      - reviewer_weight_kg
      - review_type
      - review_text
      - photo_urls
      - like_count
    """
    gender, height, weight = parse_profile(raw)

    return {
        # 상품 식별자: goods.goodsNo 우선, 없으면 URL 기반 fallback
        "product_id": parse_product_id_from_goods(raw, fallback_product_id),

        # 리뷰 식별자: 리뷰 고유 번호(no)
        "review_id": str(raw.get("no", "")),

        # 사용자 식별자: 암호화된 사용자 ID
        "encrypted_user_id": clean_text(raw.get("encryptedUserId")) or None,

        # 리뷰어 닉네임: 구형/신형 스키마 모두 대응
        "reviewer_nickname": parse_reviewer_nickname(raw),

        # 리뷰 날짜: pastDate → createDate 순으로 정규화
        "date": parse_review_date(raw),

        # 평점: grade를 float으로 변환
        "rating": parse_rating(raw),

        # 구매 옵션: 최종 컬럼명은 option
        "option": parse_option(raw),

        # 리뷰어 레벨: userProfileInfo.userLevel 중심
        "reviewer_level": parse_reviewer_level(raw),

        # 체형 정보: 신형 구조화 필드 → 구형 문자열 → fallback
        "reviewer_gender": gender,
        "reviewer_height_cm": height,
        "reviewer_weight_kg": weight,

        # 리뷰 타입 코드: style / general 등
        "review_type": parse_review_type(raw),

        # 리뷰 본문
        "review_text": parse_review_text(raw),

        # 리뷰 사진 URL 배열
        "photo_urls": parse_photo_urls(raw),

        # 좋아요 수
        "like_count": safe_int(raw.get("likeCount"), 0) or 0,
    }


# ──────────────────────────────────────────────
# 인터셉트 응답에서 리뷰 리스트 추출
# ──────────────────────────────────────────────

def extract_review_list_and_total(body: Any) -> Tuple[Optional[List[Dict[str, Any]]], Optional[int]]:
    """
    응답 JSON에서 리뷰 리스트와 총 개수를 최대한 유연하게 추출한다.
    """
    total_expected = None

    candidates: List[Any] = []
    if isinstance(body, dict):
        candidates.append(body)
        data = body.get("data")
        if isinstance(data, dict):
            candidates.append(data)
        elif isinstance(data, list):
            candidates.append({"list": data})
    elif isinstance(body, list):
        candidates.append({"list": body})

    for data in candidates:
        if not isinstance(data, dict):
            continue

        review_list = None
        for key in ["list", "content", "reviews", "items", "reviewList"]:
            candidate = data.get(key)
            if isinstance(candidate, list) and candidate and isinstance(candidate[0], dict):
                review_list = candidate
                break

        if total_expected is None:
            for pk in ["page", "pageInfo", "pagination"]:
                pinfo = data.get(pk)
                if isinstance(pinfo, dict):
                    for tk in ["totalElements", "totalCount", "total"]:
                        tv = pinfo.get(tk)
                        if tv is not None:
                            try:
                                total_expected = int(tv)
                            except Exception:
                                pass
                            break
                if total_expected is not None:
                    break

        if review_list is not None:
            return review_list, total_expected

    return None, total_expected


# ──────────────────────────────────────────────
# 중복 키
# ──────────────────────────────────────────────

def make_dedup_key(raw: Dict[str, Any]) -> str:
    """
    리뷰 중복 제거 키를 만든다.

    우선순위:
      1) no
      2) content + 날짜 조합
    """
    no = raw.get("no")
    if no is not None:
        return f"no:{no}"

    content = clean_text(raw.get("content"))[:80]
    date = normalize_date_string(raw.get("pastDate")) or normalize_date_string(raw.get("createDate")) or ""
    return f"{content}|{date}"


# ──────────────────────────────────────────────
# 항목별 데이터 존재 비율 집계
# ──────────────────────────────────────────────

def build_field_presence(parsed: List[Dict[str, Any]]) -> Dict[str, int]:
    """최종 정규화 컬럼 기준으로 데이터 존재 개수를 집계한다."""
    return {
        "product_id": sum(1 for r in parsed if r.get("product_id")),
        "review_id": sum(1 for r in parsed if r.get("review_id")),
        "encrypted_user_id": sum(1 for r in parsed if r.get("encrypted_user_id")),
        "reviewer_nickname": sum(1 for r in parsed if r.get("reviewer_nickname")),
        "date": sum(1 for r in parsed if r.get("date")),
        "rating": sum(1 for r in parsed if r.get("rating") is not None),
        "option": sum(1 for r in parsed if r.get("option")),
        "reviewer_level": sum(1 for r in parsed if r.get("reviewer_level") is not None),
        "reviewer_gender": sum(1 for r in parsed if r.get("reviewer_gender")),
        "reviewer_height_cm": sum(1 for r in parsed if r.get("reviewer_height_cm") is not None),
        "reviewer_weight_kg": sum(1 for r in parsed if r.get("reviewer_weight_kg") is not None),
        "review_type": sum(1 for r in parsed if r.get("review_type")),
        "review_text": sum(1 for r in parsed if r.get("review_text")),
        "photo_urls": sum(1 for r in parsed if r.get("photo_urls")),
        "like_count": sum(1 for r in parsed if r.get("like_count") is not None),
    }


# ──────────────────────────────────────────────
# 메인 크롤러
# ──────────────────────────────────────────────

async def main():
    ensure_dirs()

    # 사용자로부터 URL 입력받기
    print(f"\n{'=' * 60}")
    print("  무신사 리뷰 크롤러 (Playwright + 네트워크 인터셉트) - final")
    print(f"{'=' * 60}")
    TARGET_URL = input("  크롤링할 리뷰 전체보기 URL을 입력하세요: ").strip()

    if not TARGET_URL:
        print("  ⚠️ URL이 입력되지 않았습니다.")
        return
    if "/review/" not in TARGET_URL and "/products/" not in TARGET_URL:
        print("  ⚠️ 올바른 무신사 리뷰 URL이 아닙니다.")
        print("  (예: https://www.musinsa.com/review/goods/1420730?scrollToIndex=10&sort=goods_est_asc&gf=A)")
        return

    fallback_product_id = extract_product_id(TARGET_URL)

    print(f"\n{'=' * 60}")
    print("  무신사 리뷰 크롤러 (Playwright + 네트워크 인터셉트) - final")
    print(f"  URL: {TARGET_URL}")
    print(f"  기본 상품 ID: {fallback_product_id}")
    print(f"  최대 수집 개수: {MAX_REVIEWS if MAX_REVIEWS is not None else '전체'}")
    print(f"  시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 60}\n")

    captured_reviews: List[dict] = []
    seen_keys: set[str] = set()
    total_expected = [None]

    async def on_response(response: Response):
        try:
            if MAX_REVIEWS is not None and len(captured_reviews) >= MAX_REVIEWS:
                return

            url = response.url
            if response.status != 200:
                return
            if "review" not in url:
                return

            ctype = response.headers.get("content-type", "")
            if "json" not in ctype.lower():
                return

            if not any(kw in url for kw in ["list", "api", "view"]):
                return

            try:
                body = await response.json()
            except Exception:
                return

            review_list, total = extract_review_list_and_total(body)
            if review_list is None:
                return

            if total_expected[0] is None and total is not None:
                total_expected[0] = total
                print(f"[인터셉트] 총 리뷰 수: {total_expected[0]}개")

            new_count = 0
            for raw in review_list:
                if MAX_REVIEWS is not None and len(captured_reviews) >= MAX_REVIEWS:
                    break
                if not isinstance(raw, dict):
                    continue

                key = make_dedup_key(raw)
                if key in seen_keys:
                    continue

                seen_keys.add(key)
                captured_reviews.append(raw)
                new_count += 1

            if new_count > 0:
                limit_str = f"/{MAX_REVIEWS}" if MAX_REVIEWS is not None else ""
                total_str = f" (API 전체 {total_expected[0]}개)" if total_expected[0] else ""
                print(f"[인터셉트] +{new_count}개 → 누적 {len(captured_reviews)}{limit_str}{total_str}")

        except Exception:
            pass

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            slow_mo=SLOW_MO,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--window-size=1920,1080",
            ],
        )

        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="ko-KR",
            timezone_id="Asia/Seoul",
        )

        try:
            from playwright_stealth import stealth_async
            await stealth_async(context)
            print("[설정] playwright_stealth 적용 완료")
        except ImportError:
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                window.chrome = {runtime: {}};
            """)
            print("[설정] 수동 stealth 적용")

        page = await context.new_page()
        page.set_default_timeout(15000)
        page.on("response", on_response)

        try:
            print("[진입] 페이지 로딩 중...")
            await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(5000)

            # 팝업/드로어가 떠 있으면 인터셉트 흐름이 꼬일 수 있어 먼저 닫는다.
            for sel in [
                'button[aria-label="닫기"]',
                'button:has-text("닫기")',
                'button:has-text("확인")',
                'button:has-text("오늘 그만 보기")',
            ]:
                try:
                    for el in await page.query_selector_all(sel):
                        if await el.is_visible():
                            await el.click()
                            await page.wait_for_timeout(300)
                except Exception:
                    pass

            print(f"[초기] 캡처된 리뷰: {len(captured_reviews)}개")
            print("\n[스크롤] 시작")

            no_change_count = 0
            scroll_num = 0
            prev_count = len(captured_reviews)

            while True:
                if MAX_REVIEWS is not None and len(captured_reviews) >= MAX_REVIEWS:
                    print(f"\n[스크롤] ✅ 수집 제한 {MAX_REVIEWS}개 도달, 종료")
                    break

                if total_expected[0] and len(captured_reviews) >= total_expected[0]:
                    print(f"\n[스크롤] ✅ 전체 리뷰 수집 완료! ({len(captured_reviews)}/{total_expected[0]})")
                    break

                if no_change_count >= MAX_SCROLL_NO_CHANGE:
                    print(f"\n[스크롤] {MAX_SCROLL_NO_CHANGE}회 연속 새 리뷰 없음, 종료")
                    break

                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(SCROLL_PAUSE)
                scroll_num += 1

                current_count = len(captured_reviews)
                if current_count == prev_count:
                    no_change_count += 1
                else:
                    no_change_count = 0
                    prev_count = current_count

                if scroll_num % 10 == 0:
                    total_str = f"/전체 {total_expected[0]}" if total_expected[0] else ""
                    limit_part = f"/{MAX_REVIEWS}" if MAX_REVIEWS is not None else ""
                    print(f"[스크롤] #{scroll_num}: {len(captured_reviews)}{limit_part}{total_str}, 무변화={no_change_count}")

                if scroll_num % 25 == 0:
                    try:
                        for sel in ['button[aria-label="닫기"]', 'button:has-text("닫기")']:
                            for el in await page.query_selector_all(sel):
                                if await el.is_visible():
                                    await el.click()
                    except Exception:
                        pass

        except Exception as e:
            print(f"\n[오류] {e}")
            traceback.print_exc()
        finally:
            await browser.close()

    print(f"\n{'=' * 60}")
    print(f"[후처리] 캡처된 raw 리뷰: {len(captured_reviews)}개")

    if not captured_reviews:
        print("[결과] ⚠️ 리뷰 0개")
        with open(os.path.join(OUTPUT_DIR, f"{fallback_product_id}_reviews.json"), "w", encoding="utf-8") as f:
            json.dump({"reviews": []}, f, ensure_ascii=False, indent=2)
        return

    parsed: List[Dict[str, Any]] = []
    for raw in captured_reviews:
        review = parse_review(raw, fallback_product_id)
        if review["review_text"] or review["rating"] is not None:
            parsed.append(review)

    # 파싱 후 review_id 기반 중복 제거
    before_dedup = len(parsed)
    seen_review_ids: set = set()
    deduped: List[Dict[str, Any]] = []
    for r in parsed:
        rid = r.get("review_id")
        if rid:
            if rid in seen_review_ids:
                continue
            seen_review_ids.add(rid)
        else:
            # review_id 없으면 본문+날짜 조합으로 중복 체크
            fallback_key = f"{(r.get('review_text') or '')[:80]}|{r.get('date')}"
            if fallback_key in seen_review_ids:
                continue
            seen_review_ids.add(fallback_key)
        deduped.append(r)
    parsed = deduped

    if before_dedup != len(parsed):
        print(f"[후처리] 중복 제거: {before_dedup}개 → {len(parsed)}개 ({before_dedup - len(parsed)}개 제거)")
    print(f"[후처리] 유효 리뷰: {len(parsed)}개")

    # 평점 낮은 순 정렬
    parsed.sort(
        key=lambda r: (
            r["rating"] is None,
            r["rating"] if r["rating"] is not None else 999,
        )
    )

    output_path = os.path.join(OUTPUT_DIR, f"{fallback_product_id}_reviews.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({"reviews": parsed}, f, ensure_ascii=False, indent=2)

    total = len(parsed)
    ratings = [r["rating"] for r in parsed if r["rating"] is not None]
    avg = sum(ratings) / len(ratings) if ratings else 0
    field_presence = build_field_presence(parsed)

    print(f"\n{'=' * 60}")
    print("  최종 결과")
    print(f"{'=' * 60}")
    print(f"  총 리뷰:              {total}개")
    print(f"  수집 제한:            {MAX_REVIEWS if MAX_REVIEWS is not None else '전체'}")
    if total_expected[0]:
        print(f"  API 전체:             {total_expected[0]}개")
    print(f"  평균 평점:            {avg:.2f}")
    print("  ─── 항목별 데이터 존재 비율 ─────────────────")
    for key in [
        "product_id",
        "review_id",
        "encrypted_user_id",
        "reviewer_nickname",
        "date",
        "rating",
        "option",
        "reviewer_level",
        "reviewer_gender",
        "reviewer_height_cm",
        "reviewer_weight_kg",
        "review_type",
        "review_text",
        "photo_urls",
        "like_count",
    ]:
        print(f"  {key:<20} {field_presence[key]:>4}개  ({pct(field_presence[key], total)})")
    print("  정렬:                 평점 낮은 순 ✅")

    print(f"\n  📁 {os.path.abspath(output_path)}")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    asyncio.run(main())