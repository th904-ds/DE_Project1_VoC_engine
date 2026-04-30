"""
무신사(Musinsa) 상품 상세 정보 크롤러
======================================
Playwright로 상품 페이지에 진입하여:
1) 상품명, 브랜드, 가격, 평점, 리뷰수, 대표이미지 추출
2) "상품 정보 더보기" 버튼 클릭 → 펼쳐진 상세 설명 영역만 추출
3) description_raw + Description(요약) 포함한 JSON 출력
4) 필요 시 rating 디버깅 로그/HTML/스크린샷 저장

실행 방법:
    pip install playwright playwright-stealth
    playwright install chromium
    python Crawler_musinsa_product_optimized.py
"""

import asyncio
import json
import os
import re
import traceback
from datetime import datetime
from typing import Optional, Tuple, Dict, Any, List

from playwright.async_api import async_playwright, Page


# ──────────────────────────────────────────────
# 설정값
# ──────────────────────────────────────────────
HEADLESS = False
SLOW_MO = 150
OUTPUT_DIR = r"C:\Users\김태희\Desktop\대학교 3-1\DE_Project1\Crawler\outputs"
DEBUG_DIR = "debug_outputs"

# 평점이 다시 안 잡힐 때만 True로 켜기
DEBUG_RATING = False


# ──────────────────────────────────────────────
# 유틸리티 함수
# ──────────────────────────────────────────────
def ensure_dirs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(DEBUG_DIR, exist_ok=True)


def extract_product_id(url: str) -> str:
    m = re.search(r"/products/(\d+)", url)
    return m.group(1) if m else "unknown"


def clean_text(text: Optional[str]) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.strip())


def clean_multiline(text: Optional[str]) -> str:
    if not text:
        return ""
    lines = text.splitlines()
    result = []
    prev_empty = False
    for line in lines:
        s = line.strip()
        if not s:
            if not prev_empty:
                result.append("")
            prev_empty = True
        else:
            result.append(s)
            prev_empty = False
    return "\n".join(result).strip()


def parse_price(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", str(text))
    return int(digits) if digits else None


def parse_rating(text: Optional[str]) -> Optional[float]:
    if not text:
        return None

    text = str(text).strip().replace(",", ".")
    m = re.search(r"(?<!\d)([0-5](?:\.\d+)?)", text)
    if not m:
        return None

    value = float(m.group(1))
    return value if 0 < value <= 5 else None


def parse_review_count(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    m = re.search(r"([\d,]+)", str(text))
    return int(m.group(1).replace(",", "")) if m else None


def clean_product_name(text: str) -> str:
    if not text:
        return ""
    text = clean_text(text)
    text = re.sub(r"\s*-\s*사이즈\s*&\s*후기\s*\|\s*무신사$", "", text)
    text = re.sub(r"\s*\|\s*무신사$", "", text)
    return text.strip()


def normalize_image_url(url: Optional[str]) -> str:
    if not url:
        return ""
    url = url.strip()
    if url.startswith("//"):
        url = "https:" + url
    elif url.startswith("/"):
        url = "https://www.musinsa.com" + url

    # 리사이즈/쿼리 파라미터는 제거해서 원본 쪽에 가깝게 유지
    url = re.sub(r"\?.*$", "", url)
    return url


def summarize_description(raw_text: str) -> str:
    if not raw_text:
        return ""

    text = raw_text.strip()
    text = re.sub(r"\n{3,}", "\n\n", text)

    if len(text) <= 300:
        return text

    truncated = text[:300]
    cut = max(
        truncated.rfind("다."),
        truncated.rfind("요."),
        truncated.rfind("."),
        truncated.rfind("\n"),
    )
    if cut > 100:
        return truncated[:cut + 1].strip() + " ..."
    return truncated.strip() + " ..."


def clean_description_text(raw_text: str) -> str:
    if not raw_text:
        return ""

    text = clean_multiline(raw_text)
    lines = [line.strip() for line in text.splitlines()]
    cleaned_lines: List[str] = []

    blocklist_patterns = [
        r"^판매자가 카카오톡, SMS 등으로 무신사 외 사이트 구매 유도 시, 무신사 안전거래센터에 신고 해주세요\.?$",
        r"^판매자가 카카오톡,\s*SMS 등으로.*안전거래센터에 신고.*$",
        r"^무신사 안전거래센터에 신고.*$",
    ]

    for line in lines:
        if not line:
            cleaned_lines.append("")
            continue

        blocked = any(re.search(pat, line) for pat in blocklist_patterns)
        if blocked:
            continue
        cleaned_lines.append(line)

    text = "\n".join(cleaned_lines)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text


async def screenshot(page: Page, name: str):
    path = os.path.join(DEBUG_DIR, f"{name}.png")
    try:
        await page.screenshot(path=path, full_page=False)
        print(f"  [스크린샷] {path}")
    except Exception as e:
        print(f"  [스크린샷 실패] {name}: {e}")


async def save_debug_html(page: Page, name: str):
    path = os.path.join(DEBUG_DIR, f"{name}.html")
    try:
        html = await page.content()
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"  [HTML 저장] {path}")
    except Exception as e:
        print(f"  [HTML 저장 실패] {name}: {e}")


async def dismiss_popups(page: Page):
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


# ──────────────────────────────────────────────
# 구조화 데이터(JSON-LD) 파싱
# ──────────────────────────────────────────────
def _walk_jsonld_nodes(obj: Any):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk_jsonld_nodes(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from _walk_jsonld_nodes(item)


async def extract_product_schema(page: Page) -> Dict[str, Any]:
    scripts = await page.query_selector_all('script[type="application/ld+json"]')

    for sc in scripts:
        try:
            raw = await sc.inner_text()
            if not raw:
                continue

            data = json.loads(raw)
            for node in _walk_jsonld_nodes(data):
                node_type = node.get("@type")
                if node_type == "Product" or (isinstance(node_type, list) and "Product" in node_type):
                    agg = node.get("aggregateRating") or {}
                    offers = node.get("offers") or {}

                    image = node.get("image")
                    if isinstance(image, list):
                        image = image[0] if image else ""

                    return {
                        "name": node.get("name"),
                        "brand": node.get("brand", {}).get("name") if isinstance(node.get("brand"), dict) else node.get("brand"),
                        "rating": parse_rating(agg.get("ratingValue")),
                        "review_count": parse_review_count(agg.get("reviewCount") or agg.get("ratingCount")),
                        "image": normalize_image_url(image),
                        "price": parse_price(offers.get("price")),
                        "raw": node,
                    }
        except Exception:
            continue

    return {}


# ──────────────────────────────────────────────
# 상품 기본 정보 추출
# ──────────────────────────────────────────────
async def get_product_name(page: Page, schema: Optional[Dict[str, Any]] = None) -> str:
    if schema and schema.get("name"):
        return clean_product_name(schema["name"])

    for sel in ['h2[class*="title"]', 'h1', 'meta[property="og:title"]']:
        try:
            if sel.startswith("meta"):
                el = await page.query_selector(sel)
                if el:
                    return clean_product_name(await el.get_attribute("content"))
            else:
                el = await page.query_selector(sel)
                if el and await el.is_visible():
                    txt = await el.inner_text()
                    if txt and len(txt.strip()) > 1:
                        return clean_product_name(txt)
        except Exception:
            continue

    return clean_product_name(await page.title())


async def get_brand_name(page: Page, schema: Optional[Dict[str, Any]] = None) -> str:
    if schema and schema.get("brand"):
        return clean_text(schema["brand"])

    for sel in [
        '[class*="brand"] a',
        '[class*="Brand"] a',
        'a[href*="/brands/"]',
        'meta[property="product:brand"]',
    ]:
        try:
            if sel.startswith("meta"):
                el = await page.query_selector(sel)
                if el:
                    return clean_text(await el.get_attribute("content"))
            else:
                el = await page.query_selector(sel)
                if el and await el.is_visible():
                    txt = await el.inner_text()
                    if txt:
                        return clean_text(txt)
        except Exception:
            continue
    return ""


async def get_price(page: Page, schema: Optional[Dict[str, Any]] = None) -> Optional[int]:
    if schema and schema.get("price"):
        return schema["price"]

    for sel in [
        '[class*="sale"] [class*="price"]',
        '[class*="discount"] [class*="price"]',
        '[class*="Price"] [class*="final"]',
        '[class*="price"]:not([class*="origin"]):not([class*="original"])',
        'meta[property="product:price:amount"]',
    ]:
        try:
            if sel.startswith("meta"):
                el = await page.query_selector(sel)
                if el:
                    val = await el.get_attribute("content")
                    p = parse_price(val)
                    if p and p > 0:
                        return p
            else:
                for el in await page.query_selector_all(sel):
                    if await el.is_visible():
                        txt = await el.inner_text()
                        p = parse_price(txt)
                        if p and p > 100:
                            return p
        except Exception:
            continue
    return None


async def debug_rating_candidates(page: Page):
    print("\n[DEBUG][RATING] 후보 selector 점검 시작")

    selectors = [
        '[class*="rating"] [class*="score"]',
        '[class*="star-rating"]',
        '[class*="rating"]',
        '[aria-label*="평점"]',
        '[data-testid*="rating"]',
        '[class*="review"] [class*="score"]',
        '[class*="score"]',
        'script[type="application/ld+json"]',
    ]

    for sel in selectors:
        try:
            els = await page.query_selector_all(sel)
            print(f"  - selector: {sel} / count={len(els)}")

            for i, el in enumerate(els[:5]):
                try:
                    text = await el.inner_text()
                except Exception:
                    text = ""

                try:
                    outer = await el.evaluate("(el) => el.outerHTML ? el.outerHTML.slice(0, 300) : ''")
                except Exception:
                    outer = ""

                print(f"    [{i}] text={repr(text[:120])}")
                print(f"        outer={repr(outer[:200])}")
        except Exception as e:
            print(f"  - selector 점검 실패: {sel} / {e}")

    print("[DEBUG][RATING] 후보 selector 점검 종료\n")


async def get_rating_and_reviews(page: Page, schema: Optional[Dict[str, Any]] = None) -> Tuple[Optional[float], Optional[int]]:
    """
    최적화 포인트:
    1) JSON-LD(Product.aggregateRating) 우선 사용
    2) 없을 때만 DOM fallback 사용
    3) review_count도 JSON-LD 우선
    """
    if DEBUG_RATING:
        await debug_rating_candidates(page)

    rating = None
    review_count = None

    # 1) 가장 안정적인 경로: 구조화 데이터(JSON-LD)
    if schema:
        rating = schema.get("rating")
        review_count = schema.get("review_count")
        if DEBUG_RATING:
            print(f"[DEBUG][RATING][SCHEMA] rating={rating}, review_count={review_count}")

    # 2) DOM fallback
    if rating is None:
        rating_selectors = [
            '[class*="rating"] [class*="score"]',
            '[class*="star-rating"]',
            '[class*="rating"]',
            '[aria-label*="평점"]',
            '[data-testid*="rating"]',
            '[class*="review"] [class*="score"]',
            '[class*="score"]',
        ]

        for sel in rating_selectors:
            try:
                elements = await page.query_selector_all(sel)
                for idx, el in enumerate(elements):
                    candidates = []

                    try:
                        candidates.append(("inner_text", await el.inner_text()))
                    except Exception:
                        pass
                    try:
                        candidates.append(("aria-label", await el.get_attribute("aria-label")))
                    except Exception:
                        pass
                    try:
                        candidates.append(("title", await el.get_attribute("title")))
                    except Exception:
                        pass
                    try:
                        candidates.append(("data-score", await el.get_attribute("data-score")))
                    except Exception:
                        pass

                    for source_name, source_value in candidates:
                        r = parse_rating(source_value)
                        if DEBUG_RATING:
                            print(
                                f"[DEBUG][RATING][DOM] sel={sel} idx={idx} "
                                f"source={source_name} value={repr(source_value)} parsed={r}"
                            )
                        if r is not None:
                            rating = r
                            break

                    if rating is not None:
                        break
                if rating is not None:
                    break
            except Exception:
                continue

    if review_count is None:
        # 상단 제품 정보 영역에서 후기 숫자만 우선 탐색
        review_selectors = [
            'a:has-text("후기")',
            'button:has-text("후기")',
            'span:has-text("후기")',
            'a:has-text("리뷰")',
            'button:has-text("리뷰")',
            'span:has-text("리뷰")',
        ]

        for sel in review_selectors:
            try:
                for el in await page.query_selector_all(sel):
                    txt = await el.inner_text()
                    cnt = parse_review_count(txt)
                    if DEBUG_RATING:
                        print(f"[DEBUG][REVIEW_COUNT] sel={sel} txt={repr(txt)} parsed={cnt}")
                    if cnt and cnt > 0:
                        review_count = cnt
                        break
                if review_count is not None:
                    break
            except Exception:
                continue

    if DEBUG_RATING and rating is None:
        print("[DEBUG][RATING] 평점 추출 실패 → HTML/스크린샷 저장")
        await screenshot(page, "03_rating_debug")
        await save_debug_html(page, "03_rating_debug")

    return rating, review_count


async def get_main_image(page: Page, schema: Optional[Dict[str, Any]] = None) -> str:
    """
    페이지 첫 화면에 보이는 대표 상품 이미지를 가져온다.
    1) og:image 메타태그 (가장 안정적 — 항상 첫 번째 대표 이미지)
    2) 기존 DOM 스캔 방식 (fallback)
    """

    # 1) og:image — 무신사는 항상 대표 이미지를 og:image로 제공
    try:
        el = await page.query_selector('meta[property="og:image"]')
        if el:
            og_url = await el.get_attribute("content")
            if og_url:
                url = normalize_image_url(og_url)
                if url:
                    return url
    except Exception:
        pass

    # 2) 기존 DOM 스캔 방식 (fallback)
    candidates = await page.evaluate(
        """
        () => {
            const imgs = Array.from(document.querySelectorAll('img'));
            return imgs.map((img, idx) => {
                const rect = img.getBoundingClientRect();
                const src = img.currentSrc || img.src || '';
                const alt = (img.alt || '').trim();
                const cls = img.className || '';
                const visible = rect.width > 0 && rect.height > 0;
                return {
                    idx,
                    src,
                    alt,
                    cls: String(cls),
                    x: rect.x,
                    y: rect.y,
                    width: rect.width,
                    height: rect.height,
                    area: rect.width * rect.height,
                    visible,
                };
            });
        }
        """
    )

    scored = []
    for item in candidates:
        src = normalize_image_url(item.get("src"))
        alt = clean_text(item.get("alt"))
        cls = item.get("cls", "")
        x = item.get("x", 0)
        y = item.get("y", 0)
        w = item.get("width", 0)
        h = item.get("height", 0)
        area = item.get("area", 0)
        visible = item.get("visible", False)

        if not visible or not src:
            continue
        if w < 220 or h < 220:
            continue
        if y > 1200:
            continue
        if x > 1300:
            continue
        if any(bad in src.lower() for bad in ["/snap/", "snap/images", "/style/", "review", "icon", "badge"]):
            continue
        if any(bad in alt.lower() for bad in ["후기", "리뷰", "스냅"]):
            continue
        if any(bad in cls.lower() for bad in ["review", "snap", "style"]):
            continue

        score = area

        # 좌측 메인 히어로 이미지에 가점
        if x < 1050:
            score += 500000
        if y < 900:
            score += 300000

        # 상품 본 이미지 경로면 크게 가점
        if any(good in src.lower() for good in ["goods_img", "images/", "msscdn.net"]):
            score += 200000

        scored.append((score, src, alt, x, y, w, h))

    if scored:
        scored.sort(reverse=True, key=lambda t: t[0])
        best = scored[0]
        return best[1]

    # fallback 1: 좀 더 보수적인 selector
    for sel in [
        'main img',
        '[class*="product"] img',
        '[class*="gallery"] img',
        '[class*="detail"] img',
    ]:
        try:
            for el in await page.query_selector_all(sel):
                src = normalize_image_url(await el.get_attribute("src") or await el.get_attribute("data-src"))
                if src and "snap" not in src.lower():
                    box = await el.bounding_box()
                    if box and box.get("width", 0) >= 220 and box.get("height", 0) >= 220:
                        return src
        except Exception:
            continue

    # fallback 2: JSON-LD image
    if schema and schema.get("image"):
        return normalize_image_url(schema["image"])

    return ""


# ──────────────────────────────────────────────
# 상품 정보 더보기 클릭 + 상세설명 추출
# ──────────────────────────────────────────────
async def click_show_more_and_extract(page: Page) -> str:
    print("\n[상세설명] === '상품 정보 더보기' 버튼 탐색 시작 ===")

    button_found = False
    button_el = None

    search_texts = ["상품 정보 더보기", "상품정보 더보기"]
    for text in search_texts:
        try:
            locator = page.get_by_text(text, exact=False)
            count = await locator.count()
            if count > 0:
                for i in range(count):
                    el = locator.nth(i)
                    if await el.is_visible():
                        tag = await el.evaluate("el => el.tagName.toLowerCase()")
                        actual_text = await el.inner_text()
                        print(f"  [발견] <{tag}> '{actual_text.strip()[:50]}'")
                        button_el = el
                        button_found = True
                        break
            if button_found:
                break
        except Exception as e:
            print(f"  [탐색 오류] '{text}': {e}")

    if not button_found:
        print("  [fallback] selector 기반 탐색")
        for sel in [
            'button:has-text("상품 정보 더보기")',
            'a:has-text("상품 정보 더보기")',
            '[class*="more"]:has-text("더보기")',
            '[class*="expand"]:has-text("더보기")',
        ]:
            try:
                loc = page.locator(sel).first
                if await loc.count() > 0 and await loc.is_visible():
                    button_el = loc
                    button_found = True
                    print(f"  [발견] selector '{sel}'")
                    break
            except Exception:
                continue

    if not button_found:
        print("  [최종 fallback] 모든 클릭 요소에서 '더보기' 탐색")
        try:
            all_btns = await page.query_selector_all("button, a, [role='button']")
            for btn in all_btns:
                txt = (await btn.inner_text() or "").strip()
                if (
                    "더보기" in txt
                    and "문의" not in txt
                    and "리뷰" not in txt
                    and "후기" not in txt
                ):
                    if await btn.is_visible():
                        print(f"  [발견] fallback: '{txt[:50]}'")
                        button_el = btn
                        button_found = True
                        break
        except Exception:
            pass

    if not button_found:
        print("  ⚠️ '상품 정보 더보기' 버튼을 찾지 못했습니다")
        await screenshot(page, "02_show_more_not_found")
        return ""

    print("  [스크롤] 버튼 위치로 스크롤")
    try:
        await button_el.evaluate("el => el.scrollIntoView({behavior:'smooth', block:'center'})")
        await page.wait_for_timeout(800)
    except Exception:
        try:
            await button_el.scroll_into_view_if_needed()
            await page.wait_for_timeout(800)
        except Exception:
            pass

    await dismiss_popups(page)
    await page.wait_for_timeout(300)

    print("  [클릭] '상품 정보 더보기' 클릭")
    try:
        await button_el.click(timeout=5000)
        print("  [클릭] 일반 클릭 성공")
    except Exception:
        print("  [클릭] 일반 클릭 실패 → JS 강제 클릭")
        try:
            handle = await button_el.element_handle()
            if handle:
                await handle.evaluate("el => el.click()")
                print("  [클릭] JS 클릭 성공")
        except Exception as e:
            print(f"  [클릭 실패] {e}")
            await screenshot(page, "02_click_failed")
            return ""

    await page.wait_for_timeout(2000)
    await screenshot(page, "02_after_show_more_click")

    print("  [추출] 펼쳐진 상세 설명 영역 추출")

    description_raw = await page.evaluate(
        """
        () => {
            const allElements = document.querySelectorAll('*');
            let foldButton = null;

            for (const el of allElements) {
                const text = (el.innerText || '').trim();
                if (
                    (text === '상품 정보 접기' || text === '상품정보 접기' || text.includes('접기')) &&
                    el.tagName.match(/BUTTON|A|SPAN|DIV/) &&
                    text.length < 30 &&
                    !text.includes('문의')
                ) {
                    foldButton = el;
                    break;
                }
            }

            if (foldButton) {
                let container = foldButton.parentElement;

                for (let i = 0; i < 5 && container; i++) {
                    const text = container.innerText || '';
                    if (text.length > 120) {
                        let cleaned = text;
                        const foldIdx = cleaned.lastIndexOf('상품 정보 접기');
                        if (foldIdx > 0) cleaned = cleaned.substring(0, foldIdx);

                        const foldIdx2 = cleaned.lastIndexOf('접기');
                        if (foldIdx2 > cleaned.length - 50 && foldIdx2 > 0) {
                            cleaned = cleaned.substring(0, foldIdx2);
                        }
                        return cleaned.trim();
                    }
                    container = container.parentElement;
                }
            }

            const descSelectors = [
                '[class*="product-detail__description"]',
                '[class*="ProductDescription"]',
                '[class*="detail-description"]',
                '[class*="detail_cont"]',
                '[class*="detail-content"]',
                '[class*="product-content"]',
                'section[class*="detail"]',
            ];

            let bestText = '';
            for (const sel of descSelectors) {
                const els = document.querySelectorAll(sel);
                for (const el of els) {
                    const text = el.innerText || '';
                    if (text.length > bestText.length && text.length > 50) {
                        if (
                            !text.substring(0, 100).includes('문의') &&
                            !text.substring(0, 100).includes('Q&A')
                        ) {
                            bestText = text;
                        }
                    }
                }
            }

            if (bestText) return bestText.trim();

            for (const el of allElements) {
                const text = (el.innerText || '').trim();
                const tag = el.tagName.toLowerCase();
                if (
                    (tag === 'h2' || tag === 'h3' || tag === 'h4') &&
                    text.includes('상품 정보') &&
                    text.length < 30
                ) {
                    let parent = el.parentElement;
                    for (let i = 0; i < 3 && parent; i++) {
                        const pText = parent.innerText || '';
                        if (pText.length > 120) {
                            return pText.trim();
                        }
                        parent = parent.parentElement;
                    }
                }
            }

            return '';
        }
        """
    )

    raw = clean_description_text(description_raw)
    if raw:
        print(f"  [추출] {len(raw)}자 추출 성공")
        print(f"  [추출] 첫 100자: {raw[:100]}...")
    else:
        print("  ⚠️ 상세 설명 추출 실패")

    return raw


# ──────────────────────────────────────────────
# 메인 실행
# ──────────────────────────────────────────────
async def main():
    ensure_dirs()

    # 사용자로부터 URL 입력받기
    print(f"\n{'=' * 60}")
    print("  무신사 상품 상세 크롤러")
    print(f"{'=' * 60}")
    TARGET_URL = input("  크롤링할 무신사 상품 URL을 입력하세요: ").strip()

    if not TARGET_URL:
        print("  ⚠️ URL이 입력되지 않았습니다.")
        return
    if "/products/" not in TARGET_URL:
        print("  ⚠️ 올바른 무신사 상품 URL이 아닙니다. (예: https://www.musinsa.com/products/1420730)")
        return

    product_id = extract_product_id(TARGET_URL)

    print(f"\n{'=' * 60}")
    print("  무신사 상품 상세 크롤러")
    print(f"  URL: {TARGET_URL}")
    print(f"  상품 ID: {product_id}")
    print(f"  시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 60}")

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
            print("[설정] playwright_stealth 적용")
        except ImportError:
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                window.chrome = {runtime: {}};
            """)
            print("[설정] 수동 stealth 적용")

        page = await context.new_page()
        page.set_default_timeout(15000)

        result = {"product_detail": {}}

        try:
            print("\n[진입] 페이지 로딩...")
            await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2500)

            try:
                await page.wait_for_load_state("networkidle", timeout=7000)
            except Exception:
                pass

            await page.wait_for_timeout(1500)

            await dismiss_popups(page)
            await page.wait_for_timeout(500)

            await screenshot(page, "01_initial")

            schema = await extract_product_schema(page)

            print("\n[수집] 기본 정보 추출")
            product_name = await get_product_name(page, schema)
            print(f"  상품명: {product_name[:50] if product_name else 'N/A'}")

            brand_name = await get_brand_name(page, schema)
            print(f"  브랜드: {brand_name}")

            price = await get_price(page, schema)
            print(f"  가격: {price}")

            rating, review_count = await get_rating_and_reviews(page, schema)
            print(f"  평점: {rating}")
            print(f"  리뷰수: {review_count}")

            main_image_url = await get_main_image(page, schema)
            print(f"  이미지: {main_image_url[:60] if main_image_url else 'N/A'}")

            description_raw = await click_show_more_and_extract(page)
            description_summary = summarize_description(description_raw)

            result = {
                "product_detail": {
                    "product_id": product_id,
                    "url": TARGET_URL,
                    "product_name": product_name,
                    "brand_name": brand_name,
                    "price": price,
                    "rating": rating,
                    "review_count": review_count,
                    "Description": description_summary,
                    "description_raw": description_raw,
                    "main_image_url": main_image_url,
                }
            }

        except Exception as e:
            print(f"\n[오류] {e}")
            traceback.print_exc()
            await screenshot(page, "99_error")
            await save_debug_html(page, "99_error")

        finally:
            await browser.close()

    output_path = os.path.join(OUTPUT_DIR, f"{product_id}_product_details.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    pd = result.get("product_detail", {})
    print(f"\n{'=' * 60}")
    print("  수집 완료")
    print(f"{'=' * 60}")
    print(f"  상품 ID: {pd.get('product_id')}")
    print(f"  상품명: {pd.get('product_name', '')[:50]}")
    print(f"  브랜드: {pd.get('brand_name')}")
    print(f"  가격: {pd.get('price')}")
    print(f"  평점: {pd.get('rating')}")
    print(f"  리뷰수: {pd.get('review_count')}")
    print(f"  설명 원문: {len(pd.get('description_raw', ''))}자")
    print(f"  설명 요약: {pd.get('Description', '')[:80]}...")
    print(f"  이미지: {pd.get('main_image_url', '')[:60]}")
    print(f"\n  📁 {os.path.abspath(output_path)}")
    print(f"  📁 {os.path.abspath(DEBUG_DIR)}/")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    asyncio.run(main())