"""무신사 리뷰 분석기 — VOC Engine."""
from __future__ import annotations

import os
import re
import random
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pymongo.operations import UpdateOne

from aspect_mapping import extract_aspects
from crawl_utils import crawl_product, crawl_reviews
from preprocessing import clean_review_text, compute_persona
from task6_absa import SentimentRunner

load_dotenv()

# ── Playwright 브라우저 설치 + 가상 디스플레이 (클라우드 배포 환경 대응) ────────

@st.cache_resource(show_spinner=False)
def _install_playwright_browsers():
    import subprocess, sys, platform, os, time
    subprocess.run(
        [sys.executable, "-m", "playwright", "install", "chromium"],
        check=False,
    )
    if platform.system() == "Linux":
        try:
            subprocess.Popen(
                ["Xvfb", ":99", "-screen", "0", "1280x720x24", "-ac"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            time.sleep(1)
            os.environ["DISPLAY"] = ":99"
        except Exception:
            pass

_install_playwright_browsers()

# ── 페이지 설정 ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="VOC Engine",
    page_icon="🧭",
    layout="wide",
)

st.markdown("""
<style>
/* form_submit_button */
[data-testid="stFormSubmitButton"] > button {
    background-color: #7b2d8b !important;
    border-color: #7b2d8b !important;
    color: white !important;
}
[data-testid="stFormSubmitButton"] > button:hover,
[data-testid="stFormSubmitButton"] > button:focus {
    background-color: #6a1f78 !important;
    border-color: #6a1f78 !important;
}
/* 일반 primary 버튼 */
button[data-testid="baseButton-primary"] {
    background-color: #7b2d8b !important;
    border-color: #7b2d8b !important;
    color: white !important;
}
button[data-testid="baseButton-primary"]:hover,
button[data-testid="baseButton-primary"]:focus {
    background-color: #6a1f78 !important;
    border-color: #6a1f78 !important;
}
</style>
""", unsafe_allow_html=True)

# ── 상수 ─────────────────────────────────────────────────────────────────────

ASPECTS = ["소재", "핏", "사이즈", "색상", "가격", "배송"]
POSITIVE_LABEL = "긍정"

# 히트맵 colorscale: -1=N/A(회색), 0-25=빨강, 25-50=연핑크, 50-75=하늘, 75-100=남색
_eps = 1e-4
_HEATMAP_COLORSCALE = [
    [0.0,          "#cccccc"],   # N/A 회색
    [1/101 - _eps, "#cccccc"],
    [1/101,        "#ef5350"],   # 0 %  빨강
    [26/101,       "#f8bbd0"],   # 25 % 연핑크
    [51/101,       "#81d4fa"],   # 50 % 하늘
    [76/101,       "#1565c0"],   # 75 % 남색
    [1.0,          "#0d47a1"],   # 100% 짙은 남색
]
PAGE_SIZE = 20
SAMPLE_SIZE = 4000

NAV_INPUT      = "📝 0. URL 입력"
NAV_PRODUCT    = "🛍️ 1. 분석 상품 개요"
NAV_ANALYSIS   = "📊 2. Aspect × Persona 분석"
NAV_COMPETITOR = "🏆 3. 경쟁사 분석"
NAV_REVIEWS    = "🔍 4. 리뷰 브라우저"


# ── 세션 상태 초기화 ──────────────────────────────────────────────────────────

def _init_state() -> None:
    defaults: Dict[str, Any] = {
        "nav_page":     NAV_INPUT,
        "product_data": None,
        "reviews":      [],
        "summary":      None,
        "result_id":    None,
        "page_num":     0,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

_init_state()


# ── 캐시 리소스 ───────────────────────────────────────────────────────────────

@st.cache_resource
def get_db():
    uri = os.environ.get("MONGO_URI")
    if not uri:
        return None
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        return client["musinsa_db"]
    except (ConnectionFailure, Exception):
        return None


@st.cache_resource(show_spinner="감성 분석 모델 로드 중...")
def get_absa_runner() -> SentimentRunner:
    return SentimentRunner("matthewburke/korean_sentiment", batch_size=16)


# ── 설명 파싱 헬퍼 ────────────────────────────────────────────────────────────

def parse_description_fields(desc_raw: str) -> Dict[str, Optional[str]]:
    """description_raw 텍스트에서 성별/조회수/누적판매 파싱 (레이블 다음 줄이 값)."""
    result: Dict[str, Optional[str]] = {
        "성별": None, "조회수": None, "누적판매": None,
    }
    if not desc_raw:
        return result
    lines = [ln.strip() for ln in desc_raw.splitlines() if ln.strip()]
    for i, line in enumerate(lines):
        if i + 1 >= len(lines):
            break
        if line == "성별":
            result["성별"] = lines[i + 1]
        elif line == "조회수":
            result["조회수"] = lines[i + 1]
        elif line == "누적판매":
            result["누적판매"] = lines[i + 1]
    return result


def parse_sales_count(s: Optional[str]) -> Optional[int]:
    """한국어 숫자 표현 포함 → 정수 변환. 예) '25만 개 이상' → 250000."""
    if not s:
        return None
    s = str(s).strip()
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

    digits = re.sub(r'[^\d]', '', s)
    return int(digits) if digits else None


def normalize_gender(g: Optional[str]) -> Optional[str]:
    """성별 문자열 → '남성'/'여성'/'남녀공용' 정규화."""
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


# ── 전처리 파이프라인 ─────────────────────────────────────────────────────────

def preprocess_reviews(reviews_raw: List[Dict]) -> List[Dict]:
    result = []
    for r in reviews_raw:
        text_clean = clean_review_text(r.get("review_text") or "")
        option = r.get("option") or ""
        color, size = None, None
        if "·" in option:
            parts = option.split("·", 1)
            color, size = parts[0].strip(), parts[1].strip()
        persona = compute_persona(
            gender=r.get("reviewer_gender"),
            height_cm=r.get("reviewer_height_cm"),
            weight_kg=r.get("reviewer_weight_kg"),
            size_raw=size,
        )
        result.append({
            **r,
            "text_clean": text_clean,
            "persona": persona,
            "color": color,
            "size": size,
            "aspects_sentences": {},
            "absa_result": {},
        })
    return result


def map_aspects_inplace(reviews: List[Dict]) -> None:
    for r in reviews:
        r["aspects_sentences"] = extract_aspects(r.get("text_clean") or "")


def run_absa(
    reviews: List[Dict],
    runner: SentimentRunner,
    on_progress: Optional[Any] = None,
) -> None:
    tasks = [
        (i, asp, " | ".join(sents))
        for i, r in enumerate(reviews)
        for asp, sents in r.get("aspects_sentences", {}).items()
        if sents
    ]
    if not tasks:
        return
    total = len(tasks)
    preds: list = []
    CHUNK = 32
    for start in range(0, total, CHUNK):
        preds.extend(runner.predict([t[2] for t in tasks[start:start + CHUNK]]))
        if on_progress:
            try:
                on_progress(min(start + CHUNK, total), total)
            except Exception:
                pass
    for (i, asp, _), (label, score) in zip(tasks, preds):
        reviews[i]["absa_result"][asp] = {"label": label, "score": round(score, 4)}


# ── 집계 ─────────────────────────────────────────────────────────────────────

def build_summary(
    product_data: Dict,
    all_reviews: List[Dict],
    absa_reviews: List[Dict],
    sampled: bool,
    sample_size: int,
) -> Dict:
    ratings = [r["rating"] for r in all_reviews if r.get("rating") is not None]
    avg_rating = round(sum(ratings) / len(ratings), 2) if ratings else None

    rating_dist = {str(i): 0 for i in range(1, 6)}
    for rt in ratings:
        key = str(int(round(rt)))
        if key in rating_dist:
            rating_dist[key] += 1

    persona_dist: Dict[str, int] = {}
    for r in all_reviews:
        p = r.get("persona") or "unknown"
        persona_dist[p] = persona_dist.get(p, 0) + 1

    aspect_sentiment: Dict[str, Dict] = {}
    for asp in ASPECTS:
        pos = sum(1 for r in absa_reviews
                  if r.get("absa_result", {}).get(asp, {}).get("label") == POSITIVE_LABEL)
        neg = sum(1 for r in absa_reviews
                  if r.get("absa_result", {}).get(asp)
                  and r["absa_result"][asp].get("label") != POSITIVE_LABEL)
        mentioned = pos + neg
        aspect_sentiment[asp] = {
            "긍정": pos, "부정": neg, "언급수": mentioned,
            "긍정률": round(pos / mentioned * 100, 1) if mentioned > 0 else None,
        }

    personas = sorted(p for p in persona_dist if p != "unknown")
    heatmap: Dict[str, Dict] = {}
    for persona in personas:
        p_revs = [r for r in absa_reviews if r.get("persona") == persona]
        heatmap[persona] = {}
        for asp in ASPECTS:
            pos = sum(1 for r in p_revs
                      if r.get("absa_result", {}).get(asp, {}).get("label") == POSITIVE_LABEL)
            neg = sum(1 for r in p_revs
                      if r.get("absa_result", {}).get(asp)
                      and r["absa_result"][asp].get("label") != POSITIVE_LABEL)
            total_asp = pos + neg
            heatmap[persona][asp] = round(pos / total_asp * 100, 1) if total_asp > 0 else None

    return {
        "total": len(all_reviews),
        "absa_count": len(absa_reviews),
        "avg_rating": avg_rating,
        "sampled": sampled,
        "sample_size": sample_size,
        "rating_distribution": rating_dist,
        "persona_distribution": persona_dist,
        "aspect_sentiment": aspect_sentiment,
        "persona_aspect_heatmap": heatmap,
    }


# ── MongoDB 저장 ──────────────────────────────────────────────────────────────

def save_persona_aspect_summary(
    db, product_id: str, absa_reviews: List[Dict]
) -> None:
    """가중치 반영 방식 — score 값을 합산해 positive_rate/negative_rate 계산."""
    if db is None:
        return
    try:
        # {persona: {aspect: {weighted_pos, weighted_neg, total, rating_sum, rating_count}}}
        agg: Dict[str, Dict[str, Dict]] = defaultdict(
            lambda: defaultdict(lambda: {
                "weighted_pos": 0.0, "weighted_neg": 0.0,
                "total": 0, "rating_sum": 0.0, "rating_count": 0,
            })
        )
        for r in absa_reviews:
            persona = r.get("persona") or "unknown"
            rating = r.get("rating")
            for asp, res in r.get("absa_result", {}).items():
                score = float(res.get("score") or 0)
                label = res.get("label", "")
                d = agg[persona][asp]
                if label == POSITIVE_LABEL:
                    d["weighted_pos"] += score
                else:
                    d["weighted_neg"] += score
                d["total"] += 1
                if rating is not None:
                    d["rating_sum"] += rating
                    d["rating_count"] += 1

        ops = []
        for persona, asp_data in agg.items():
            for asp, d in asp_data.items():
                total = d["total"]
                pos_rate = round(d["weighted_pos"] / total * 100, 1) if total > 0 else None
                neg_rate = round(d["weighted_neg"] / total * 100, 1) if total > 0 else None
                avg_r = (
                    round(d["rating_sum"] / d["rating_count"], 1)
                    if d["rating_count"] > 0 else None
                )
                ops.append(UpdateOne(
                    {"product_id": product_id, "persona": persona, "aspect": asp},
                    {"$set": {
                        "product_id": product_id,
                        "persona": persona,
                        "aspect": asp,
                        "positive_rate": pos_rate,
                        "negative_rate": neg_rate,
                        "avg_rating": avg_r,
                        "sample_size": total,
                    }},
                    upsert=True,
                ))
        if ops:
            db.persona_aspect_summary.bulk_write(ops, ordered=False)
    except Exception as e:
        st.warning(f"persona_aspect_summary 저장 오류: {e}")


def save_to_mongo(
    db,
    product_data: Dict,
    reviews: List[Dict],
    summary: Dict,
    absa_reviews: Optional[List[Dict]] = None,
) -> Optional[Any]:
    if db is None:
        return None
    try:
        fields = parse_description_fields(product_data.get("description_raw") or "")
        gender_norm = normalize_gender(fields.get("성별"))
        sales_count = parse_sales_count(fields.get("누적판매"))
        product_doc = {
            **product_data,
            "parsed_성별": gender_norm,
            "parsed_누적판매": sales_count,
        }
        db.products.update_one(
            {"product_id": product_data["product_id"]},
            {"$set": product_doc},
            upsert=True,
        )

        result_doc = {
            "product_id": product_data["product_id"],
            "analyzed_at": datetime.utcnow(),
            "product_data": product_data,
            "crawl_info": {
                "total_crawled": summary["total"],
                "absa_count": summary["absa_count"],
                "sampled": summary["sampled"],
                "sample_size": summary.get("sample_size"),
            },
            "summary": {
                "avg_rating": summary["avg_rating"],
                "rating_distribution": summary["rating_distribution"],
                "persona_distribution": summary["persona_distribution"],
                "aspect_sentiment": summary["aspect_sentiment"],
            },
        }
        # 같은 product_id면 덮어쓰기 (upsert)
        replace_result = db.streamlit_analyses.replace_one(
            {"product_id": product_data["product_id"]},
            result_doc,
            upsert=True,
        )
        if replace_result.upserted_id:
            result_id = replace_result.upserted_id
        else:
            doc = db.streamlit_analyses.find_one(
                {"product_id": product_data["product_id"]}, {"_id": 1}
            )
            result_id = doc["_id"] if doc else None

        if reviews:
            # 기존 리뷰 삭제 후 새로 적재 (중복 방지)
            db.streamlit_reviews.delete_many({"product_id": product_data["product_id"]})
            db.streamlit_reviews.insert_many([
                {
                    "result_id": result_id,
                    "product_id": r["product_id"],
                    "review_id": r.get("review_id"),
                    "date": r.get("date"),
                    "rating": r.get("rating"),
                    "review_text": r.get("review_text"),
                    "text_clean": r.get("text_clean"),
                    "persona": r.get("persona"),
                    "color": r.get("color"),
                    "size": r.get("size"),
                    "reviewer_gender": r.get("reviewer_gender"),
                    "reviewer_height_cm": r.get("reviewer_height_cm"),
                    "reviewer_weight_kg": r.get("reviewer_weight_kg"),
                    "like_count": r.get("like_count", 0),
                    "has_photo": len(r.get("photo_urls") or []) > 0,
                    "aspects_mentioned": list(r.get("absa_result", {}).keys()),
                    "absa_result": r.get("absa_result", {}),
                }
                for r in reviews
            ], ordered=False)

        if absa_reviews:
            save_persona_aspect_summary(
                db, product_data["product_id"], absa_reviews
            )

        return result_id
    except Exception as e:
        st.warning(f"MongoDB 저장 오류: {e}")
        return None


# ── 경쟁사 조회 헬퍼 ─────────────────────────────────────────────────────────

def get_competitors(
    db,
    product_id: str,
    gender_norm: Optional[str],
    top_n: int = 3,
) -> List[Dict]:
    """같은 성별 중 rating >= 4.8인 상품을 누적판매 많은 순으로 top_n개 반환."""
    if db is None or not gender_norm:
        return []
    try:
        query = {
            "parsed_성별": gender_norm,
            "rating": {"$gte": 4.7},
            "product_id": {"$ne": product_id},
        }
        cursor = (
            db.products.find(query)
            .sort("parsed_누적판매", -1)
            .limit(top_n)
        )
        return list(cursor)
    except Exception:
        return []


def get_persona_aspect_data(db, product_id: str) -> Dict[str, Dict[str, Optional[float]]]:
    """persona_aspect_summary에서 {persona: {aspect: pos_rate}} 반환."""
    if db is None:
        return {}
    try:
        docs = list(db.persona_aspect_summary.find({"product_id": product_id}))
        result: Dict[str, Dict[str, Optional[float]]] = {}
        for doc in docs:
            persona = doc.get("persona") or "unknown"
            aspect = doc.get("aspect")
            pos_rate = doc.get("positive_rate")
            if aspect:
                result.setdefault(persona, {})[aspect] = pos_rate
        return result
    except Exception:
        return {}


def get_persona_insights(db, product_id: str) -> Dict:
    """
    persona_aspect_summary → 메인/충성/취약 타겟층 계산.
    충성·취약 타겟층은 전체 리뷰의 10% 이상 페르소나에서만 선정.
    """
    if db is None:
        return {}
    try:
        docs = list(db.persona_aspect_summary.find(
            {"product_id": product_id},
            {"_id": 0, "persona": 1, "positive_rate": 1, "negative_rate": 1, "sample_size": 1},
        ))
        if not docs:
            return {}

        agg: Dict[str, Dict] = {}
        for doc in docs:
            p = doc.get("persona") or "unknown"
            if p not in agg:
                agg[p] = {"samples": [], "pos": [], "neg": []}
            agg[p]["samples"].append(doc.get("sample_size") or 0)
            if doc.get("positive_rate") is not None:
                agg[p]["pos"].append(doc["positive_rate"])
            if doc.get("negative_rate") is not None:
                agg[p]["neg"].append(doc["negative_rate"])

        stats = {
            p: {
                "max_sample": max(d["samples"]) if d["samples"] else 0,
                "avg_pos": round(sum(d["pos"]) / len(d["pos"]), 1) if d["pos"] else 0.0,
                "avg_neg": round(sum(d["neg"]) / len(d["neg"]), 1) if d["neg"] else 0.0,
            }
            for p, d in agg.items()
        }

        total = sum(s["max_sample"] for s in stats.values())
        threshold = total * 0.1

        main_p = max(stats, key=lambda p: stats[p]["max_sample"])
        qualified = {p: s for p, s in stats.items() if s["max_sample"] >= threshold}

        loyal_p = max(qualified, key=lambda p: qualified[p]["avg_pos"]) if qualified else None
        weak_p  = max(qualified, key=lambda p: qualified[p]["avg_neg"]) if qualified else None

        return {
            "main":      main_p,
            "main_n":    stats[main_p]["max_sample"],
            "loyal":     loyal_p,
            "loyal_pos": stats[loyal_p]["avg_pos"] if loyal_p else None,
            "weak":      weak_p,
            "weak_neg":  stats[weak_p]["avg_neg"]  if weak_p  else None,
        }
    except Exception:
        return {}


def get_aspect_positive_rates(db, product_id: str) -> Dict[str, Optional[float]]:
    """persona_aspect_summary를 집계하여 aspect별 전체 긍정률 반환 (가중치 방식)."""
    if db is None:
        return {}
    try:
        docs = list(db.persona_aspect_summary.find({"product_id": product_id}))
        # positive_rate * sample_size / 100 → weighted_pos_score 역산 후 재집계
        agg: Dict[str, Dict[str, float]] = {}
        for doc in docs:
            asp = doc.get("aspect")
            pos_rate = doc.get("positive_rate") or 0
            sample = doc.get("sample_size", 0) or 0
            if asp and sample > 0:
                if asp not in agg:
                    agg[asp] = {"weighted_pos_sum": 0.0, "total": 0}
                agg[asp]["weighted_pos_sum"] += pos_rate * sample / 100
                agg[asp]["total"] += sample
        return {
            asp: round(d["weighted_pos_sum"] / d["total"] * 100, 1) if d["total"] > 0 else None
            for asp, d in agg.items()
        }
    except Exception:
        return {}


def get_persona_aspect_table_df(db, product_id: str) -> pd.DataFrame:
    """persona_aspect_summary → 긍정/부정/중립/평균평점 포함 DataFrame."""
    if db is None:
        return pd.DataFrame()
    try:
        docs = list(db.persona_aspect_summary.find(
            {"product_id": product_id},
            {"_id": 0, "persona": 1, "aspect": 1,
             "positive_rate": 1, "negative_rate": 1, "avg_rating": 1, "sample_size": 1},
        ))
        if not docs:
            return pd.DataFrame()
        rows = []
        for doc in docs:
            pos = doc.get("positive_rate") or 0.0
            neg = doc.get("negative_rate") or 0.0
            neutral = round(max(0.0, 100.0 - pos - neg), 1)
            rows.append({
                "페르소나": doc.get("persona", "unknown"),
                "Aspect": doc.get("aspect", ""),
                "긍정(%)": round(pos, 1),
                "부정(%)": round(neg, 1),
                "중립(%)": neutral,
                "평균 평점": doc.get("avg_rating"),
                "sample_size": doc.get("sample_size") or 0,
            })
        df = pd.DataFrame(rows)
        asp_order = {a: i for i, a in enumerate(ASPECTS)}
        df["_asp_order"] = df["Aspect"].map(asp_order).fillna(99)
        df = df.sort_values(["페르소나", "_asp_order"]).drop(columns=["_asp_order"])
        return df.reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


# ── 사이드바 네비게이션 ───────────────────────────────────────────────────────

def render_sidebar() -> str:
    with st.sidebar:
        st.markdown(
            "<div style='font-size:1.6rem;font-weight:800;letter-spacing:-0.5px'>"
            "🧭 VoC Engine</div>",
            unsafe_allow_html=True,
        )
        st.divider()

        pages = [NAV_INPUT]
        if st.session_state.product_data:
            pages.append(NAV_PRODUCT)
        if st.session_state.summary:
            pages += [NAV_ANALYSIS, NAV_COMPETITOR, NAV_REVIEWS]

        current = st.session_state.nav_page
        if current not in pages:
            current = pages[0]

        selected = st.radio(
            "navigation",
            pages,
            index=pages.index(current),
            label_visibility="collapsed",
        )
        st.session_state.nav_page = selected

        if st.session_state.product_data:
            st.divider()
            if st.button("🗑️ 초기화", use_container_width=True):
                for k in ["product_data", "reviews", "summary", "result_id"]:
                    st.session_state[k] = None if k != "reviews" else []
                st.session_state.nav_page = NAV_INPUT
                st.session_state.page_num = 0
                st.rerun()

        st.divider()
        db = get_db()
        if db is not None:
            st.caption("🟢 MongoDB 연결됨")
        else:
            st.caption("🔴 MongoDB 연결 없음")

    return selected


# ── 페이지: URL 입력 ──────────────────────────────────────────────────────────

def render_input_page() -> None:
    st.title("VoC Engine: We Turn Reviews into Action!")
    st.caption("상품 URL과 리뷰 URL을 입력하면 end-to-end로 리뷰를 분석합니다.")

    with st.form("url_form"):
        col1, col2 = st.columns(2)
        with col1:
            product_url = st.text_input(
                "상품 URL",
                placeholder="https://www.musinsa.com/products/1420730",
            )
        with col2:
            review_url = st.text_input(
                "리뷰 URL",
                placeholder="https://www.musinsa.com/review/goods/1420730?...",
            )
        absa_mode = st.radio(
            "ABSA 분석 범위",
            ["전체 리뷰", f"랜덤 샘플 {SAMPLE_SIZE:,}개"],
            horizontal=True,
            help="전체 분석은 리뷰 수에 따라 최대 30분 이상 소요될 수 있습니다.",
        )
        submitted = st.form_submit_button(
            "🚀 분석 시작", type="primary", use_container_width=True
        )

    if submitted and product_url and review_url:
        st.session_state.page_num = 0
        success = run_pipeline(product_url.strip(), review_url.strip(), absa_mode)
        if success:
            st.session_state.nav_page = NAV_PRODUCT
            st.rerun()


# ── 페이지: 분석 상품 개요 ────────────────────────────────────────────────────

def render_product_page() -> None:
    product_data = st.session_state.product_data
    if not product_data:
        st.info("분석된 상품이 없습니다.")
        return

    st.title("1. 분석 상품 개요")

    desc_raw = product_data.get("description_raw") or ""
    fields = parse_description_fields(desc_raw)

    # ── 상품 기본 정보 카드 ─────────────────────────────────────────────────
    img_col, info_col = st.columns([1, 2])
    with img_col:
        img_url = product_data.get("main_image_url")
        if img_url:
            st.image(img_url, use_container_width=True)
    with info_col:
        st.markdown(f"## {product_data.get('product_name') or '상품명 없음'}")
        brand = product_data.get("brand_name")
        if brand:
            st.markdown(f"**브랜드**: {brand}")
        price = product_data.get("price")
        st.markdown(f"**가격**: {price:,}원" if price else "**가격**: -")
        rating = product_data.get("rating")
        rc = product_data.get("review_count")
        rating_str = f"★ {rating}" if rating else "-"
        rc_str = f"({rc:,}개 리뷰)" if rc else ""
        st.markdown(f"**평점**: {rating_str} {rc_str}")
        st.markdown(f"**상품 ID**: `{product_data.get('product_id')}`")

        upper_labels = ["성별", "조회수"]
        valid_upper = [lbl for lbl in upper_labels if fields.get(lbl)]
        if valid_upper:
            st.markdown("")
            ratios = [1 if lbl == "성별" else 3 for lbl in valid_upper]
            dcols = st.columns(ratios)
            for dc, label in zip(dcols, valid_upper):
                dc.markdown(
                    f"<div style='font-size:1.6rem;color:#888;margin-bottom:4px'>{label}</div>"
                    f"<div style='font-size:2.4rem;font-weight:600;line-height:1.3'>{fields[label]}</div>",
                    unsafe_allow_html=True,
                )

    if not st.session_state.summary:
        return

    summary = st.session_state.summary
    st.divider()

    # ── KPI 행 ──────────────────────────────────────────────────────────────
    def _kpi_html(label: str, value: str) -> str:
        return (
            f"<div style='font-size:1.56rem;color:#888;margin-bottom:4px'>{label}</div>"
            f"<div style='font-size:2.6rem;font-weight:600;line-height:1.3'>{value}</div>"
        )

    k1, k2, k3, k4 = st.columns([2, 1.5, 1.5, 1.5])
    sales_int = parse_sales_count(fields.get("누적판매"))
    sales_disp = f"{sales_int:,}개" if sales_int else "-"
    k1.markdown(_kpi_html("누적판매", sales_disp), unsafe_allow_html=True)
    avg = summary.get("avg_rating")
    k2.markdown(_kpi_html("평균 평점", f"{avg:.2f} ★" if avg else "-"), unsafe_allow_html=True)
    k3.markdown(_kpi_html("총 리뷰", f"{summary['total']:,}개"), unsafe_allow_html=True)
    k4.markdown(
        _kpi_html("감성 분석", f"{summary['absa_count']:,}개"
                  + (" (샘플)" if summary["sampled"] else "")),
        unsafe_allow_html=True,
    )

    st.divider()

    # ── 평점 분포 + 페르소나 분포 ────────────────────────────────────────────
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**1) 평점 분포**")
        rd = summary["rating_distribution"]
        color_map = {"1": "#ef5350", "2": "#ff7043", "3": "#ffa726",
                     "4": "#66bb6a", "5": "#42a5f5"}
        fig = px.bar(x=list(rd.keys()), y=list(rd.values()),
                     labels={"x": "평점", "y": "리뷰 수"},
                     color=list(rd.keys()), color_discrete_map=color_map,
                     text=list(rd.values()))
        fig.update_traces(texttemplate="%{text:,}개", textposition="outside")
        fig.update_layout(showlegend=False, height=300, margin=dict(t=30, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.markdown("**2) Aspect 언급 빈도**")
        asp_rows = [{"aspect": asp, "언급수": d["언급수"]}
                    for asp, d in summary["aspect_sentiment"].items() if d["언급수"] > 0]
        if asp_rows:
            asp_df = pd.DataFrame(asp_rows).sort_values("언급수", ascending=False)
            ASPECT_COLORS = ["#ef5350", "#ff7043", "#ffa726", "#66bb6a", "#42a5f5", "#7e57c2"]
            fig_asp = px.pie(
                asp_df, names="aspect", values="언급수",
                hole=0.45,
                color_discrete_sequence=ASPECT_COLORS,
            )
            fig_asp.update_traces(textposition="inside", textinfo="label+percent")
            fig_asp.update_layout(
                showlegend=False, height=300,
                margin=dict(t=10, b=10, l=10, r=10),
            )
            st.plotly_chart(fig_asp, use_container_width=True)

    # ── 페르소나 분포 (가로 누적 막대) ─────────────────────────────────────
    st.markdown("**3) 페르소나 분포**")
    pd_all = {k: v for k, v in summary["persona_distribution"].items() if v > 0}
    if pd_all:
        BODY_RANK = {"풍만": 0, "대형": 0, "통통": 1, "중형": 1,
                     "보통": 2, "마름": 3, "소형": 3}
        GENDER_RANK = {"여성": 0, "남성": 1}  # unknown → 2

        bar_rows = []
        for persona, cnt in pd_all.items():
            parts = persona.split("_", 1)
            gender = parts[0]
            body   = parts[1] if len(parts) > 1 else "기타"
            b_rank = next((r for kw, r in BODY_RANK.items() if kw in body), 99)
            g_rank = GENDER_RANK.get(gender, 2)
            bar_rows.append({
                "성별": gender,
                "체형": body,
                "인원": cnt,
                "persona": persona,
                "_gr": g_rank,
                "_br": b_rank,
            })

        bar_df = pd.DataFrame(bar_rows).sort_values(["_gr", "_br"])
        color_map_pd = {r["persona"]: _persona_color(r["persona"])
                        for _, r in bar_df.iterrows()}
        # 성별 Y축 순서: 여성 위 → 남성 → unknown 아래
        existing_genders = bar_df["성별"].unique().tolist()
        y_order = [g for g in ["여성", "남성", "unknown"] if g in existing_genders]

        fig_pd = px.bar(
            bar_df,
            x="인원", y="성별",
            color="persona",
            orientation="h",
            barmode="stack",
            color_discrete_map=color_map_pd,
            text="체형",
            category_orders={"성별": y_order},
            hover_data={"인원": True, "체형": True, "성별": False, "persona": False,
                        "_gr": False, "_br": False},
        )
        fig_pd.update_traces(
            textposition="inside",
            insidetextanchor="middle",
            textfont=dict(size=12, color="black"),
        )
        fig_pd.update_layout(
            height=320,
            xaxis_title="인원 수",
            yaxis_title="",
            showlegend=False,
            margin=dict(t=10, b=10, l=10, r=10),
        )
        st.plotly_chart(fig_pd, use_container_width=True)
    else:
        st.info("페르소나 데이터 없음")


# ── 페이지: Aspect × Persona 분석 ────────────────────────────────────────────

def _persona_color(persona: str) -> str:
    """성별별 계열색, 체형 클수록 진하게."""
    MALE = [
        ("풍만", "#0d47a1"), ("대형", "#0d47a1"),
        ("통통", "#1976d2"), ("중형", "#64b5f6"),
        ("보통", "#90caf9"),
        ("마름", "#bbdefb"), ("소형", "#e3f2fd"),
    ]
    FEMALE = [
        ("풍만", "#880e4f"), ("대형", "#880e4f"),
        ("통통", "#e91e63"), ("중형", "#f48fb1"),
        ("보통", "#f8bbd0"),
        ("마름", "#fce4ec"), ("소형", "#fff0f5"),
    ]
    UNK = [
        ("대형", "#f9d71c"), ("풍만", "#f9d71c"),
        ("통통", "#ffee58"), ("중형", "#fff176"),
        ("보통", "#fff9c4"),
        ("마름", "#fffde7"), ("소형", "#fffff8"),
    ]
    parts = persona.split("_", 1)
    gender, body = parts[0], (parts[1] if len(parts) > 1 else "")
    palette = MALE if gender == "남성" else FEMALE if gender == "여성" else UNK
    for kw, color in palette:
        if kw in body:
            return color
    return "#bdbdbd"


def _judgment(pos_rate: Optional[float]) -> str:
    if pos_rate is None:
        return "-"
    if pos_rate >= 70:
        return "강점"
    if pos_rate >= 50:
        return "주의"
    return "개선 필요"


def _style_judgment(val: str) -> str:
    colors = {"강점": "#c8f7c5", "주의": "#fff3cd", "개선 필요": "#ffd6d6"}
    return f"background-color: {colors.get(val, '')}; font-weight: bold"


def _style_pos(val: float) -> str:
    if val is None:
        return ""
    if val >= 70:
        return "color: #2e7d32; font-weight: bold"
    if val >= 50:
        return "color: #388e3c"
    return ""


def _style_neg(val: float) -> str:
    if val is None:
        return ""
    if val >= 40:
        return "color: #c62828; font-weight: bold"
    if val >= 25:
        return "color: #d32f2f"
    return ""


def _persona_sort_key(persona: str) -> tuple:
    """여성>남성>unknown, 풍만/대형>통통/중형>보통>마름/소형 순 정렬 키."""
    gender_rank = {"여성": 0, "남성": 1}
    body_rank = {"풍만": 0, "대형": 0, "통통": 1, "중형": 1, "보통": 2, "마름": 3, "소형": 3}
    parts = persona.split("_", 1)
    gender = parts[0] if parts else ""
    body = parts[1] if len(parts) > 1 else ""
    g = gender_rank.get(gender, 2)
    b = next((r for kw, r in body_rank.items() if kw in body), 99)
    return (g, b, persona)


def _build_heatmap(
    data: Dict[str, Dict[str, Optional[float]]],
    height_per_row: int = 50,
) -> Optional[go.Figure]:
    """
    {persona: {aspect: pos_rate|None}} 입력.
    - 모든 값이 None인 페르소나 행 제거
    - None 셀 → 회색
    - 축 레이블 → 검정
    """
    personas = sorted(
        (p for p in data if any(data[p].get(asp) is not None for asp in ASPECTS)),
        key=_persona_sort_key,
    )
    if not personas:
        return None

    z_filled = [
        [-1 if data[p].get(asp) is None else data[p].get(asp)
         for asp in ASPECTS]
        for p in personas
    ]
    text_vals = [
        ["N/A" if v == -1 else f"{v:.1f}%" for v in row]
        for row in z_filled
    ]

    fig = go.Figure(data=go.Heatmap(
        z=z_filled, x=ASPECTS, y=personas,
        colorscale=_HEATMAP_COLORSCALE, zmin=-1, zmax=100,
        text=text_vals, texttemplate="%{text}",
        colorbar=dict(
            title="긍정률(%)",
            tickvals=[0, 25, 50, 75, 100],
            ticktext=["0%", "25%", "50%", "75%", "100%"],
        ),
    ))
    fig.update_layout(
        xaxis=dict(tickfont=dict(color="black", size=12)),
        yaxis=dict(tickfont=dict(color="black", size=12), autorange="reversed"),
        height=max(250, len(personas) * height_per_row + 100),
        margin=dict(t=20, b=20),
    )
    return fig


def render_analysis_page() -> None:
    summary = st.session_state.summary
    if not summary:
        st.info("분석 결과가 없습니다.")
        return

    st.title("2. Aspect × Persona 분석")

    # ── 핵심 인사이트 카드 ────────────────────────────────────────────────────
    _product_id = (st.session_state.product_data or {}).get("product_id")
    _db = get_db()
    _insights = get_persona_insights(_db, _product_id) if _product_id else {}

    if _insights:
        ic1, ic2, ic3 = st.columns(3)
        with ic1:
            st.markdown(
                "<div style='background:#e3f2fd;border-radius:10px;padding:14px 16px'>"
                "<div style='font-size:0.78rem;color:#555;margin-bottom:4px'>📊 메인 타겟층</div>"
                f"<div style='font-size:1.05rem;font-weight:700'>{_insights['main']}</div>"
                f"<div style='font-size:0.82rem;color:#777'>샘플 {_insights['main_n']}명</div>"
                "</div>",
                unsafe_allow_html=True,
            )
        with ic2:
            _loyal = _insights.get("loyal") or "-"
            _loyal_pos = _insights.get("loyal_pos")
            _loyal_pos_str = f"평균 긍정률 {_loyal_pos}%" if _loyal_pos is not None else ""
            st.markdown(
                "<div style='background:#e8f5e9;border-radius:10px;padding:14px 16px'>"
                "<div style='font-size:0.78rem;color:#555;margin-bottom:4px'>💚 충성 타겟층</div>"
                f"<div style='font-size:1.05rem;font-weight:700'>{_loyal}</div>"
                f"<div style='font-size:0.82rem;color:#2e7d32'>{_loyal_pos_str}</div>"
                "</div>",
                unsafe_allow_html=True,
            )
        with ic3:
            _weak = _insights.get("weak") or "-"
            _weak_neg = _insights.get("weak_neg")
            _weak_neg_str = f"평균 부정률 {_weak_neg}%" if _weak_neg is not None else ""
            st.markdown(
                "<div style='background:#fce4ec;border-radius:10px;padding:14px 16px'>"
                "<div style='font-size:0.78rem;color:#555;margin-bottom:4px'>⚠️ 취약 타겟층</div>"
                f"<div style='font-size:1.05rem;font-weight:700'>{_weak}</div>"
                f"<div style='font-size:0.82rem;color:#c62828'>{_weak_neg_str}</div>"
                "</div>",
                unsafe_allow_html=True,
            )
        st.markdown("")

    # ── Section 1: Aspect 전체 감성 ─────────────────────────────────────────
    st.subheader("1) Aspect 전체 감성")
    valid = {k: v for k, v in summary["aspect_sentiment"].items() if v["언급수"] > 0}
    if not valid:
        st.info("분석된 aspect가 없습니다.")
        return

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**긍정률 레이더 차트**")
        labels = list(valid.keys())
        values = [v["긍정률"] or 0 for v in valid.values()]
        fig_radar = go.Figure(data=go.Scatterpolar(
            r=values + [values[0]], theta=labels + [labels[0]],
            fill="toself", line_color="#42a5f5",
            fillcolor="rgba(66,165,245,0.2)",
        ))
        fig_radar.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
            height=350, margin=dict(t=30, b=30),
        )
        st.plotly_chart(fig_radar, use_container_width=True)

    with col2:
        st.markdown("**긍정 / 부정 비율 (수평 스택 바)**")
        bar_rows = []
        for asp, d in valid.items():
            total = d["언급수"]
            if total > 0:
                bar_rows.append({"Aspect": asp, "구분": "긍정",
                                  "비율": round(d["긍정"] / total * 100, 1)})
                bar_rows.append({"Aspect": asp, "구분": "부정",
                                  "비율": round(d["부정"] / total * 100, 1)})
        if bar_rows:
            fig_bar = px.bar(
                pd.DataFrame(bar_rows), x="비율", y="Aspect", color="구분",
                orientation="h",
                color_discrete_map={"긍정": "#42a5f5", "부정": "#ef5350"},
                barmode="stack",
            )
            fig_bar.update_layout(height=350, margin=dict(t=10, b=10),
                                   xaxis=dict(range=[0, 100], ticksuffix="%"))
            st.plotly_chart(fig_bar, use_container_width=True)

    # ── Section 2: 페르소나 × Aspect 집계 테이블 (판단 포함) ─────────────────
    st.divider()
    st.subheader("2) 페르소나 × Aspect 집계")
    product_id = (st.session_state.product_data or {}).get("product_id")
    db = get_db()
    tdf = get_persona_aspect_table_df(db, product_id) if product_id else pd.DataFrame()

    heatmap = summary.get("persona_aspect_heatmap", {})

    if not tdf.empty:
        tdf["판단"] = tdf["긍정(%)"].apply(_judgment)
        # 커스텀 페르소나 순서 + Aspect 순서로 정렬
        asp_rank = {a: i for i, a in enumerate(ASPECTS)}
        tdf = tdf.copy()
        tdf["_pk"] = tdf["페르소나"].apply(_persona_sort_key)
        tdf["_ar"] = tdf["Aspect"].map(asp_rank).fillna(99)
        tdf = tdf.sort_values(["_pk", "_ar"]).drop(columns=["_pk", "_ar"]).reset_index(drop=True)
        # 페르소나별 샘플 수 (각 페르소나의 aspect별 sample_size 중 최대값)
        persona_sample = tdf.groupby("페르소나")["sample_size"].max().to_dict()
        # 같은 페르소나 두 번째 행부터 페르소나 칸 공백, 첫 행엔 (X명) 추가
        display_persona = []
        prev = None
        for p in tdf["페르소나"]:
            if p != prev:
                n = persona_sample.get(p, 0)
                display_persona.append(f"{p}({n}명)" if n else p)
                prev = p
            else:
                display_persona.append("")
        tdf = tdf.drop(columns=["sample_size"]).copy()
        tdf["페르소나"] = display_persona
        fmt = {
            "긍정(%)": "{:.1f}%",
            "부정(%)": "{:.1f}%",
            "중립(%)": "{:.1f}%",
            "평균 평점": lambda v: f"{v:.1f}" if v is not None else "-",
        }
        styled = (
            tdf.style
            .applymap(_style_pos, subset=["긍정(%)"])
            .applymap(_style_neg, subset=["부정(%)"])
            .applymap(_style_judgment, subset=["판단"])
            .format(fmt)
        )
        st.dataframe(styled, use_container_width=True, hide_index=True)
    else:
        st.info("집계 데이터가 없습니다. (분석 후 DB 저장 완료 시 표시됩니다)")

    # ── Section 3: 페르소나별 히트맵 ────────────────────────────────────────
    st.divider()
    st.subheader("3) 페르소나별 Aspect 긍정률 히트맵")
    if heatmap:
        fig_heat = _build_heatmap(heatmap)
        if fig_heat:
            st.plotly_chart(fig_heat, use_container_width=True)
        else:
            st.info("유효한 페르소나 데이터가 없습니다.")
    else:
        st.info("페르소나 데이터가 없습니다.")


# ── 페이지: 리뷰 브라우저 ─────────────────────────────────────────────────────

def render_reviews_page() -> None:
    reviews = st.session_state.reviews
    if not reviews:
        st.info("리뷰 데이터가 없습니다.")
        return

    st.title("4. 리뷰 브라우저")
    all_personas = ["전체"] + sorted({r.get("persona") or "unknown" for r in reviews})

    with st.expander("필터", expanded=True):
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            rating_range = st.slider("평점", 1, 5, (1, 5), key="f_rating",
                                     on_change=lambda: st.session_state.update({"page_num": 0}))
        with c2:
            persona_sel = st.selectbox("페르소나", all_personas, key="f_persona",
                                       on_change=lambda: st.session_state.update({"page_num": 0}))
        with c3:
            aspect_sel = st.selectbox("Aspect", ["전체"] + ASPECTS, key="f_aspect",
                                      on_change=lambda: st.session_state.update({"page_num": 0}))
        with c4:
            search_q = st.text_input("텍스트 검색", key="f_search",
                                     on_change=lambda: st.session_state.update({"page_num": 0}))

    filtered = [r for r in reviews
                if rating_range[0] <= (r.get("rating") or 0) <= rating_range[1]]
    if persona_sel != "전체":
        filtered = [r for r in filtered if r.get("persona") == persona_sel]
    if aspect_sel != "전체":
        filtered = [r for r in filtered if aspect_sel in r.get("absa_result", {})]
    if search_q:
        q = search_q.lower()
        filtered = [r for r in filtered if q in (r.get("review_text") or "").lower()]

    total_filtered = len(filtered)
    total_pages = max(1, (total_filtered + PAGE_SIZE - 1) // PAGE_SIZE)
    page_num = min(st.session_state.page_num, total_pages - 1)

    st.caption(f"총 {total_filtered:,}개 (전체 {len(reviews):,}개 중)")
    _render_pagination(page_num, total_pages, "top")

    page_reviews = filtered[page_num * PAGE_SIZE:(page_num + 1) * PAGE_SIZE]
    for r in page_reviews:
        with st.container(border=True):
            ci, ca = st.columns([3, 1])
            with ci:
                rating = r.get("rating")
                stars = ("★" * int(rating) + "☆" * (5 - int(rating))) if rating else ""
                st.markdown(
                    f"**{stars}** {rating or '-'}점 &nbsp;·&nbsp; "
                    f"{r.get('date') or '-'} &nbsp;·&nbsp; "
                    f"`{r.get('persona') or 'unknown'}`"
                )
                st.write(r.get("review_text") or "")
                meta = []
                if r.get("color"):
                    meta.append(f"색상: {r['color']}")
                if r.get("size"):
                    meta.append(f"사이즈: {r['size']}")
                if r.get("reviewer_gender"):
                    meta.append(r["reviewer_gender"])
                if r.get("reviewer_height_cm"):
                    meta.append(f"{r['reviewer_height_cm']}cm")
                if r.get("reviewer_weight_kg"):
                    meta.append(f"{r['reviewer_weight_kg']}kg")
                if meta:
                    st.caption("  ·  ".join(meta))
            with ca:
                absa = r.get("absa_result", {})
                if absa:
                    for asp, res in absa.items():
                        label = res.get("label", "")
                        icon = "🟦" if label == POSITIVE_LABEL else "🟥"
                        st.caption(f"{icon} {asp}: {label} ({res.get('score', 0):.2f})")
                else:
                    st.caption("aspect 없음")

    _render_pagination(page_num, total_pages, "bot")


def _render_pagination(page_num: int, total_pages: int, key_suffix: str) -> None:
    c1, c2, c3 = st.columns([1, 3, 1])
    with c1:
        if st.button("◀ 이전", key=f"prev_{key_suffix}", disabled=(page_num == 0)):
            st.session_state.page_num = page_num - 1
            st.rerun()
    with c2:
        st.markdown(
            f"<p style='text-align:center;padding-top:6px'>"
            f"{page_num + 1} / {total_pages} 페이지</p>",
            unsafe_allow_html=True,
        )
    with c3:
        if st.button("다음 ▶", key=f"next_{key_suffix}",
                     disabled=(page_num >= total_pages - 1)):
            st.session_state.page_num = page_num + 1
            st.rerun()


# ── 페이지: 경쟁사 분석 ───────────────────────────────────────────────────────

def render_competitor_page() -> None:
    product_data = st.session_state.product_data
    summary = st.session_state.summary
    if not product_data or not summary:
        st.info("분석 결과가 없습니다.")
        return

    st.title("3. 경쟁사 분석")

    db = get_db()
    desc_raw = product_data.get("description_raw") or ""
    fields = parse_description_fields(desc_raw)
    gender_norm = normalize_gender(fields.get("성별"))

    if not gender_norm:
        st.info("현재 상품의 성별 정보가 없어 경쟁사를 찾을 수 없습니다.")
        return

    st.caption(f"기준: 성별 **{gender_norm}** · 평점 4.7 이상 · 누적판매 많은 순")

    competitors = get_competitors(db, product_data["product_id"], gender_norm)

    if not competitors:
        st.info(f"동일 성별({gender_norm}) 중 평점 4.7 이상인 다른 상품이 없습니다.")
        return

    # 상품 카드 — 현재 상품 + 경쟁사
    all_products = [("📌 현재 상품", product_data)] + [
        (f"🏆 경쟁사 {i + 1}", c) for i, c in enumerate(competitors)
    ]
    cols = st.columns(len(all_products))
    for col, (label, prod) in zip(cols, all_products):
        with col:
            st.markdown(f"**{label}**")
            img = prod.get("main_image_url") or ""
            if img:
                st.image(img, use_container_width=True)
            name = (prod.get("product_name") or "-")[:25]
            st.markdown(f"**{name}**")
            brand = prod.get("brand_name") or "-"
            st.caption(f"브랜드: {brand}")
            rating = prod.get("rating")
            rc = prod.get("review_count")
            rc_str = f"({rc:,}개)" if rc else ""
            st.markdown(
                f"<div style='font-size:1rem;font-weight:700;color:#111;margin:2px 0'>"
                f"★ {rating or '-'}&nbsp;&nbsp;{rc_str}</div>",
                unsafe_allow_html=True,
            )
            # 누적판매
            sales_int = prod.get("parsed_누적판매")
            if not sales_int:
                dr2 = prod.get("description_raw") or ""
                sales_int = parse_sales_count(parse_description_fields(dr2).get("누적판매"))
            if sales_int:
                st.markdown(
                    f"<div style='font-size:1rem;font-weight:700;color:#111;margin:2px 0'>"
                    f"누적판매 {sales_int:,}개</div>",
                    unsafe_allow_html=True,
                )

    st.divider()

    # 그룹 바 차트 — Aspect별 긍정률 비교
    st.subheader("1) Aspect 긍정률 비교")

    current_asp = summary.get("aspect_sentiment", {})
    bar_rows = []
    current_label = "현재 상품"
    for asp in ASPECTS:
        val = (current_asp.get(asp) or {}).get("긍정률") or 0
        bar_rows.append({"Aspect": asp, "상품": current_label, "긍정률(%)": val})

    comp_labels = [f"경쟁사{i+1}" for i in range(len(competitors))]
    comp_rates_list = []
    for idx, comp in enumerate(competitors):
        comp_rates = get_aspect_positive_rates(db, comp["product_id"])
        comp_rates_list.append(comp_rates)
        for asp in ASPECTS:
            val = comp_rates.get(asp) or 0
            bar_rows.append({"Aspect": asp, "상품": comp_labels[idx], "긍정률(%)": val})

    bar_df = pd.DataFrame(bar_rows)
    NAVY = "#283593"
    color_map = {current_label: "#42a5f5"}
    for lbl in comp_labels:
        color_map[lbl] = NAVY

    bar_fig = px.bar(
        bar_df, x="Aspect", y="긍정률(%)", color="상품",
        barmode="group",
        color_discrete_map=color_map,
        text="긍정률(%)",
        category_orders={"상품": [current_label] + comp_labels},
    )
    _y_min = max(0, bar_df["긍정률(%)"].min() - 10)
    bar_fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    bar_fig.update_layout(
        height=450,
        yaxis=dict(range=[_y_min, 108], ticksuffix="%"),
        legend=dict(orientation="h", yanchor="bottom", y=-0.3),
        margin=dict(t=20, b=80),
    )
    st.plotly_chart(bar_fig, use_container_width=True)

    st.divider()

    # 타겟 페르소나별 경쟁사 비교
    st.subheader("2) 타겟 페르소나별 경쟁사 비교")
    st.caption("현재 상품의 메인·충성·취약 타겟 페르소나에서, 각 상품이 Aspect별로 어떤 반응을 얻는지 비교합니다.")

    current_insights = get_persona_insights(db, product_data["product_id"])
    if not current_insights:
        st.info("현재 상품의 페르소나 인사이트 데이터가 없습니다. 분석 완료 후 다시 확인해 주세요.")
    else:
        prod_labels = ["현재 상품"] + [f"경쟁사{i+1}" for i in range(len(competitors))]
        all_asp_data: Dict[str, Dict] = {
            "현재 상품": get_persona_aspect_data(db, product_data["product_id"])
        }
        for i, comp in enumerate(competitors):
            all_asp_data[f"경쟁사{i+1}"] = get_persona_aspect_data(db, comp["product_id"])

        persona_cards = [
            ("📊 메인 타겟", current_insights.get("main")),
            ("💚 충성 타겟", current_insights.get("loyal")),
            ("⚠️ 취약 타겟", current_insights.get("weak")),
        ]
        # 경쟁사 대비 차이 colorscale: 파랑=현재 상품 우위, 흰색=동일, 빨강=경쟁사 우위
        _delta_scale = [
            [0.0, "#1565c0"],
            [0.5, "#f5f5f5"],
            [1.0, "#ef5350"],
        ]
        st.caption("🔵 파랑: 현재 상품 우위  ·  ⚪ 흰색: 동일  ·  🔴 빨강: 경쟁사 우위")

        for pi, (card_label, persona) in enumerate(persona_cards):
            if not persona:
                continue
            st.markdown(f"**{card_label}: `{persona}`**")

            # 현재 상품 절대값을 기준(baseline)으로 delta 계산
            cur_asp = (all_asp_data.get("현재 상품", {}).get(persona) or {})

            z_rows, text_rows = [], []
            for asp in ASPECTS:
                z_row, t_row = [], []
                cur_val = cur_asp.get(asp)
                for pl in prod_labels:
                    rate = (all_asp_data.get(pl, {}).get(persona) or {}).get(asp)
                    if pl == "현재 상품":
                        z_row.append(0)
                        t_row.append(f"{cur_val:.1f}% (기준)" if cur_val is not None else "N/A")
                    elif cur_val is None or rate is None:
                        z_row.append(0)
                        t_row.append("N/A")
                    else:
                        delta = rate - cur_val
                        z_row.append(delta)
                        sign = "+" if delta >= 0 else ""
                        t_row.append(f"{sign}{delta:.1f}%p")
                z_rows.append(z_row)
                text_rows.append(t_row)

            fig = go.Figure(data=go.Heatmap(
                z=z_rows,
                x=prod_labels,
                y=ASPECTS,
                colorscale=_delta_scale,
                zmin=-30, zmax=30,
                text=text_rows,
                texttemplate="%{text}",
                colorbar=dict(
                    title="경쟁사−현재(%p)",
                    tickvals=[-30, -15, 0, 15, 30],
                    ticktext=["-30", "-15", "0", "+15", "+30"],
                ),
            ))
            fig.update_layout(
                xaxis=dict(tickfont=dict(color="black", size=12)),
                yaxis=dict(tickfont=dict(color="black", size=12), autorange="reversed"),
                height=300,
                margin=dict(t=10, b=20, l=10, r=10),
            )
            st.plotly_chart(fig, use_container_width=True, key=f"comp_persona_hm_{pi}")


# ── 파이프라인 실행 ───────────────────────────────────────────────────────────

def run_pipeline(product_url: str, review_url: str, absa_mode: str) -> bool:
    use_sample = "샘플" in absa_mode

    with st.status("Step 1: 상품 정보 수집 중...", expanded=True) as s1:
        try:
            product_data = crawl_product(product_url)
            st.session_state.product_data = product_data
            s1.update(label=f"Step 1 완료 — {product_data.get('product_name', '')[:40]}")
        except Exception as e:
            s1.update(label=f"Step 1 실패: {e}", state="error")
            st.error(str(e))
            return False

    with st.container(border=True):
        img_col, info_col = st.columns([1, 3])
        with img_col:
            if product_data.get("main_image_url"):
                st.image(product_data["main_image_url"], width=120)
        with info_col:
            st.markdown(f"**{product_data.get('product_name') or ''}**")
            fields = parse_description_fields(product_data.get("description_raw") or "")
            detail_parts = [
                f"{lbl}: {fields[lbl]}"
                for lbl in ["성별", "조회수", "누적판매"]
                if fields.get(lbl)
            ]
            if detail_parts:
                st.caption("  ·  ".join(detail_parts))

    with st.status("Step 2: 리뷰 크롤링 중...", expanded=True) as s2:
        pb = st.progress(0.0)
        tx = st.empty()

        def on_review_progress(cur: int, tot: Optional[int]) -> None:
            if tot:
                pb.progress(min(cur / tot, 1.0))
                tx.text(f"{cur:,} / {tot:,}개 수집됨")
            else:
                tx.text(f"{cur:,}개 수집됨")

        try:
            reviews_raw = crawl_reviews(review_url, on_progress=on_review_progress)
            s2.update(label=f"Step 2 완료 — {len(reviews_raw):,}개 리뷰 수집")
        except Exception as e:
            s2.update(label=f"Step 2 실패: {e}", state="error")
            st.error(str(e))
            return False

    with st.status("Step 3: 전처리 및 Aspect 매핑 중...", expanded=False) as s3:
        reviews = preprocess_reviews(reviews_raw)
        map_aspects_inplace(reviews)
        s3.update(label=f"Step 3 완료 — {len(reviews):,}개 전처리")

    sampled = use_sample and len(reviews) > SAMPLE_SIZE
    absa_reviews = random.sample(reviews, SAMPLE_SIZE) if sampled else reviews
    with st.status(f"Step 4: 감성 분석 중 ({len(absa_reviews):,}개)...",
                   expanded=True) as s4:
        ap = st.progress(0.0)
        at = st.empty()

        def on_absa_progress(cur: int, tot: int) -> None:
            ap.progress(min(cur / tot, 1.0))
            at.text(f"aspect 쌍 {cur:,} / {tot:,} 분류 중...")

        try:
            runner = get_absa_runner()
            run_absa(absa_reviews, runner, on_progress=on_absa_progress)
            s4.update(label=f"Step 4 완료 — {len(absa_reviews):,}개 감성 분석")
        except Exception as e:
            s4.update(label=f"Step 4 실패: {e}", state="error")
            st.error(str(e))
            return False

    with st.status("Step 5: 집계 및 MongoDB 저장 중...", expanded=False) as s5:
        summary = build_summary(
            product_data, reviews, absa_reviews,
            sampled=sampled,
            sample_size=SAMPLE_SIZE if sampled else len(reviews),
        )
        st.session_state.reviews = reviews
        st.session_state.summary = summary

        db = get_db()
        result_id = save_to_mongo(
            db, product_data, reviews, summary, absa_reviews=absa_reviews
        )
        st.session_state.result_id = result_id

        mongo_msg = (
            f" · MongoDB 저장 완료 ({result_id})" if result_id else " · MongoDB 저장 생략"
        )
        s5.update(label=f"Step 5 완료{mongo_msg}")

    return True


# ── 메인 ─────────────────────────────────────────────────────────────────────

def main() -> None:
    nav = render_sidebar()

    if nav == NAV_INPUT:
        render_input_page()
    elif nav == NAV_PRODUCT:
        render_product_page()
    elif nav == NAV_ANALYSIS:
        render_analysis_page()
    elif nav == NAV_COMPETITOR:
        render_competitor_page()
    elif nav == NAV_REVIEWS:
        render_reviews_page()


main()
