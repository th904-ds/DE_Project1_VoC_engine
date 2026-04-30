"""
크롤러 유틸리티 — Streamlit 통합용 (headless, in-memory)
"""
from __future__ import annotations

import asyncio
import json
import re
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple

from playwright.async_api import Page, Response, async_playwright


# ── 공통 유틸 ─────────────────────────────────────────────────────────────────

def _clean(t: Any) -> str:
    if not t:
        return ""
    return re.sub(r"\s+", " ", str(t).strip())


def _safe_int(v: Any) -> Optional[int]:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _safe_float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _norm_date(value: Any) -> Optional[str]:
    if not value:
        return None
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", _clean(value))
    return m.group(1) if m else None


def _extract_product_id(url: str) -> str:
    m = re.search(r"/(?:goods|products)/(\d+)", url)
    return m.group(1) if m else "unknown"


def _parse_price(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", str(text))
    return int(digits) if digits else None


def _parse_rating_val(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    text = str(text).strip().replace(",", ".")
    m = re.search(r"(?<!\d)([0-5](?:\.\d+)?)", text)
    if not m:
        return None
    v = float(m.group(1))
    return v if 0 < v <= 5 else None


def _parse_review_count(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    m = re.search(r"([\d,]+)", str(text))
    return int(m.group(1).replace(",", "")) if m else None


def _norm_image_url(url: Optional[str]) -> str:
    if not url:
        return ""
    url = url.strip()
    if url.startswith("//"):
        url = "https:" + url
    elif url.startswith("/"):
        url = "https://www.musinsa.com" + url
    return re.sub(r"\?.*$", "", url)


def _clean_product_name(text: str) -> str:
    text = _clean(text)
    text = re.sub(r"\s*-\s*사이즈\s*&\s*후기\s*\|\s*무신사$", "", text)
    text = re.sub(r"\s*\|\s*무신사$", "", text)
    return text.strip()


# ── 리뷰 파서 ─────────────────────────────────────────────────────────────────

def _parse_profile(raw: Dict[str, Any]) -> Tuple[Optional[str], Optional[int], Optional[int]]:
    if not raw.get("showUserProfile", True):
        return None, None, None

    info = raw.get("userProfileInfo")
    if isinstance(info, dict):
        gender = _clean(info.get("reviewSex") or info.get("userSex") or "")
        if gender not in {"남성", "여성"}:
            gender = None
        height = _safe_int(info.get("userHeight"))
        if height is not None and not (100 <= height <= 250):
            height = None
        weight = _safe_int(info.get("userWeight"))
        if weight is not None and not (20 <= weight <= 200):
            weight = None
        if any(x is not None for x in (gender, height, weight)):
            return gender, height, weight

    up = raw.get("userProfile")
    if isinstance(up, str) and _clean(up):
        text = _clean(up)
        gender = None
        g = re.search(r"\b(남성|여성)\b", text)
        if g:
            gender = g.group(1)
        height = None
        h = re.search(r"(?<!\d)(\d{2,3})\s*cm\b", text, re.I)
        if h:
            v = int(h.group(1))
            if 100 <= v <= 250:
                height = v
        weight = None
        w = re.search(r"(?<!\d)(\d{2,3})\s*kg\b", text, re.I)
        if w:
            v = int(w.group(1))
            if 20 <= v <= 200:
                weight = v
        return gender, height, weight

    return None, None, None


def _parse_review(raw: Dict[str, Any], fallback_pid: str) -> Dict[str, Any]:
    gender, height, weight = _parse_profile(raw)
    goods = raw.get("goods") if isinstance(raw.get("goods"), dict) else {}
    product_id = str(goods.get("goodsNo") or fallback_pid)

    images = []
    for img in (raw.get("images") or []):
        if isinstance(img, dict):
            path = img.get("image") or img.get("imageUrl") or ""
            if path:
                url = path if str(path).startswith("http") else f"https://image.musinsa.com{path}"
                images.append(url)

    nick = _clean(raw.get("userProfileName")) or None
    if not nick:
        info = raw.get("userProfileInfo")
        if isinstance(info, dict):
            nick = _clean(info.get("userNickName")) or None

    option = None
    for k in ["goodsOption", "optionName", "option", "goodsOptionText"]:
        v = raw.get(k)
        if isinstance(v, str) and _clean(v):
            option = _clean(v)
            break

    return {
        "product_id": product_id,
        "review_id": str(raw.get("no", "")),
        "encrypted_user_id": _clean(raw.get("encryptedUserId")) or None,
        "reviewer_nickname": nick,
        "date": _norm_date(raw.get("pastDate")) or _norm_date(raw.get("createDate")),
        "rating": _safe_float(raw.get("grade")),
        "option": option,
        "reviewer_level": _safe_int((raw.get("userProfileInfo") or {}).get("userLevel"))
            if isinstance(raw.get("userProfileInfo"), dict) else None,
        "reviewer_gender": gender,
        "reviewer_height_cm": height,
        "reviewer_weight_kg": weight,
        "review_type": _clean(raw.get("type")) or None,
        "review_text": _clean(raw.get("content")) or None,
        "photo_urls": images,
        "like_count": _safe_int(raw.get("likeCount")) or 0,
    }


def _extract_review_list_and_total(body: Any) -> Tuple[Optional[List], Optional[int]]:
    total = None
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
            c = data.get(key)
            if isinstance(c, list) and c and isinstance(c[0], dict):
                review_list = c
                break
        if total is None:
            for pk in ["page", "pageInfo", "pagination"]:
                pinfo = data.get(pk)
                if isinstance(pinfo, dict):
                    for tk in ["totalElements", "totalCount", "total"]:
                        tv = pinfo.get(tk)
                        if tv is not None:
                            try:
                                total = int(tv)
                            except Exception:
                                pass
                            break
                if total is not None:
                    break
        if review_list is not None:
            return review_list, total
    return None, total


def _make_dedup_key(raw: Dict[str, Any]) -> str:
    no = raw.get("no")
    if no is not None:
        return f"no:{no}"
    content = _clean(raw.get("content", ""))[:80]
    date = _norm_date(raw.get("pastDate")) or _norm_date(raw.get("createDate")) or ""
    return f"{content}|{date}"


# ── JSON-LD 파서 ──────────────────────────────────────────────────────────────

def _walk_jsonld(obj: Any):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk_jsonld(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from _walk_jsonld(item)


async def _get_product_schema(page: Page) -> Dict[str, Any]:
    scripts = await page.query_selector_all('script[type="application/ld+json"]')
    for sc in scripts:
        try:
            raw = await sc.inner_text()
            if not raw:
                continue
            data = json.loads(raw)
            for node in _walk_jsonld(data):
                ntype = node.get("@type")
                if ntype == "Product" or (isinstance(ntype, list) and "Product" in ntype):
                    agg = node.get("aggregateRating") or {}
                    offers = node.get("offers") or {}
                    image = node.get("image")
                    if isinstance(image, list):
                        image = image[0] if image else ""
                    return {
                        "name": node.get("name"),
                        "brand": node.get("brand", {}).get("name")
                            if isinstance(node.get("brand"), dict) else node.get("brand"),
                        "rating": _parse_rating_val(agg.get("ratingValue")),
                        "review_count": _parse_review_count(
                            agg.get("reviewCount") or agg.get("ratingCount")
                        ),
                        "image": _norm_image_url(image),
                        "price": _parse_price(offers.get("price")),
                    }
        except Exception:
            continue
    return {}


# ── 상품 상세 설명 추출 ───────────────────────────────────────────────────────

def _clean_description_text(raw_text: str) -> str:
    if not raw_text:
        return ""
    lines = raw_text.splitlines()
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
    text = "\n".join(result).strip()

    blocklist = [
        r"^판매자가 카카오톡, SMS 등으로 무신사 외 사이트 구매 유도 시, 무신사 안전거래센터에 신고 해주세요\.?$",
        r"^판매자가 카카오톡,\s*SMS 등으로.*안전거래센터에 신고.*$",
        r"^무신사 안전거래센터에 신고.*$",
    ]
    cleaned: List[str] = []
    for line in text.splitlines():
        if not line:
            cleaned.append("")
            continue
        if any(re.search(pat, line) for pat in blocklist):
            continue
        cleaned.append(line)
    text = "\n".join(cleaned)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def _summarize_description(raw_text: str) -> str:
    if not raw_text:
        return ""
    text = re.sub(r"\n{3,}", "\n\n", raw_text.strip())
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


async def _click_and_extract_description(page: Page) -> str:
    """'상품 정보 더보기' 클릭 후 펼쳐진 상세 설명 텍스트를 반환."""
    button_found = False
    button_el = None

    for text in ["상품 정보 더보기", "상품정보 더보기"]:
        try:
            locator = page.get_by_text(text, exact=False)
            if await locator.count() > 0:
                for i in range(await locator.count()):
                    el = locator.nth(i)
                    if await el.is_visible():
                        button_el = el
                        button_found = True
                        break
            if button_found:
                break
        except Exception:
            continue

    if not button_found:
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
                    break
            except Exception:
                continue

    if not button_found:
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
                        button_el = btn
                        button_found = True
                        break
        except Exception:
            pass

    if not button_found:
        return ""

    try:
        await button_el.evaluate("el => el.scrollIntoView({behavior:'smooth', block:'center'})")
        await page.wait_for_timeout(800)
    except Exception:
        try:
            await button_el.scroll_into_view_if_needed()
            await page.wait_for_timeout(800)
        except Exception:
            pass

    await _dismiss_popups(page)
    await page.wait_for_timeout(300)

    try:
        await button_el.click(timeout=5000)
    except Exception:
        try:
            handle = await button_el.element_handle()
            if handle:
                await handle.evaluate("el => el.click()")
        except Exception:
            return ""

    await page.wait_for_timeout(2000)

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

    return _clean_description_text(description_raw)


# ── 브라우저 컨텍스트 ─────────────────────────────────────────────────────────

async def _make_browser_context(playwright):
    browser = await playwright.chromium.launch(
        headless=True,
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
    except ImportError:
        await context.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
            "window.chrome={runtime:{}};"
        )
    return browser, context


async def _dismiss_popups(page: Page) -> None:
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
                    await page.wait_for_timeout(200)
        except Exception:
            pass


# ── 상품 크롤러 ───────────────────────────────────────────────────────────────

async def _crawl_product_async(url: str) -> Dict[str, Any]:
    product_id = _extract_product_id(url)

    async with async_playwright() as p:
        browser, context = await _make_browser_context(p)
        try:
            page = await context.new_page()
            page.set_default_timeout(15000)
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2500)
            try:
                await page.wait_for_load_state("networkidle", timeout=7000)
            except Exception:
                pass
            await _dismiss_popups(page)

            schema = await _get_product_schema(page)

            # 상품명
            name = _clean_product_name(schema.get("name") or "")
            if not name:
                el = await page.query_selector('meta[property="og:title"]')
                if el:
                    name = _clean_product_name(await el.get_attribute("content") or "")
            if not name:
                name = _clean_product_name(await page.title())

            # 브랜드
            brand = _clean(schema.get("brand") or "")
            if not brand:
                for sel in ['[class*="brand"] a', 'a[href*="/brands/"]']:
                    try:
                        el = await page.query_selector(sel)
                        if el and await el.is_visible():
                            brand = _clean(await el.inner_text())
                            if brand:
                                break
                    except Exception:
                        pass

            # 대표 이미지 (og:image 우선)
            main_image = ""
            try:
                el = await page.query_selector('meta[property="og:image"]')
                if el:
                    main_image = _norm_image_url(await el.get_attribute("content") or "")
            except Exception:
                pass
            if not main_image and schema.get("image"):
                main_image = schema["image"]

            description_raw = await _click_and_extract_description(page)
            description_summary = _summarize_description(description_raw)

            return {
                "product_id": product_id,
                "url": url,
                "product_name": name,
                "brand_name": brand,
                "price": schema.get("price"),
                "rating": schema.get("rating"),
                "review_count": schema.get("review_count"),
                "Description": description_summary,
                "description_raw": description_raw,
                "main_image_url": main_image,
            }
        finally:
            await browser.close()


# ── 리뷰 크롤러 ───────────────────────────────────────────────────────────────

async def _crawl_reviews_async(
    url: str,
    on_progress: Optional[Callable[[int, Optional[int]], None]] = None,
    max_reviews: Optional[int] = None,
) -> List[Dict[str, Any]]:
    fallback_pid = _extract_product_id(url)
    captured: List[dict] = []
    seen_keys: set = set()
    total_expected: List[Optional[int]] = [None]

    async with async_playwright() as p:
        browser, context = await _make_browser_context(p)
        try:
            page = await context.new_page()
            page.set_default_timeout(15000)

            async def on_response(response: Response):
                try:
                    if max_reviews is not None and len(captured) >= max_reviews:
                        return
                    resp_url = response.url
                    if response.status != 200 or "review" not in resp_url:
                        return
                    if "json" not in response.headers.get("content-type", "").lower():
                        return
                    if not any(kw in resp_url for kw in ["list", "api", "view"]):
                        return
                    try:
                        body = await response.json()
                    except Exception:
                        return
                    review_list, total = _extract_review_list_and_total(body)
                    if review_list is None:
                        return
                    if total_expected[0] is None and total is not None:
                        total_expected[0] = total
                    for raw in review_list:
                        if max_reviews is not None and len(captured) >= max_reviews:
                            break
                        if not isinstance(raw, dict):
                            continue
                        key = _make_dedup_key(raw)
                        if key in seen_keys:
                            continue
                        seen_keys.add(key)
                        captured.append(raw)
                    if on_progress:
                        try:
                            on_progress(len(captured), total_expected[0])
                        except Exception:
                            pass
                except Exception:
                    pass

            page.on("response", on_response)
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(5000)
            await _dismiss_popups(page)

            no_change = 0
            scroll_num = 0
            prev_count = 0

            while True:
                if max_reviews is not None and len(captured) >= max_reviews:
                    break
                if total_expected[0] and len(captured) >= total_expected[0]:
                    break
                if no_change >= 15:
                    break

                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(1.2)
                scroll_num += 1

                current = len(captured)
                if current == prev_count:
                    no_change += 1
                else:
                    no_change = 0
                    prev_count = current

                if scroll_num % 25 == 0:
                    await _dismiss_popups(page)

        finally:
            await browser.close()

    # 파싱 + 중복 제거
    parsed = []
    seen_ids: set = set()
    for raw in captured:
        r = _parse_review(raw, fallback_pid)
        if not (r["review_text"] or r["rating"] is not None):
            continue
        rid = r.get("review_id")
        if rid:
            if rid in seen_ids:
                continue
            seen_ids.add(rid)
        parsed.append(r)

    return parsed


# ── Streamlit-safe 동기 래퍼 ──────────────────────────────────────────────────

def _run_async(coro):
    """Streamlit 스크립트 컨텍스트를 배경 스레드에 전파하여 async 코루틴을 실행."""
    streamlit_ctx = None
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        streamlit_ctx = get_script_run_ctx()
    except Exception:
        pass

    result: List[Any] = [None]
    error: List[Optional[Exception]] = [None]

    def target():
        if streamlit_ctx is not None:
            try:
                from streamlit.runtime.scriptrunner import add_script_run_ctx
                add_script_run_ctx(threading.current_thread(), streamlit_ctx)
            except Exception:
                pass
        try:
            result[0] = asyncio.run(coro)
        except Exception as e:
            error[0] = e

    t = threading.Thread(target=target, daemon=True)
    t.start()
    t.join()

    if error[0]:
        raise error[0]
    return result[0]


def crawl_product(url: str) -> Dict[str, Any]:
    return _run_async(_crawl_product_async(url))


def crawl_reviews(
    url: str,
    on_progress: Optional[Callable[[int, Optional[int]], None]] = None,
    max_reviews: Optional[int] = None,
) -> List[Dict[str, Any]]:
    return _run_async(_crawl_reviews_async(url, on_progress, max_reviews))
