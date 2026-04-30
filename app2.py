"""무신사 리뷰 분석기 — VOC Engine (UI Redesign)."""
from __future__ import annotations

import os
import random
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

from aspect_mapping import extract_aspects
from crawl_utils import crawl_product, crawl_reviews
from preprocessing import clean_review_text, compute_persona
from task6_absa import SentimentRunner

load_dotenv()

# ── 페이지 설정 ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="VOC Engine — Review to Action",
    page_icon="🔍",
    layout="wide",
)

# ── 상수 ─────────────────────────────────────────────────────────────────────

ASPECTS = ["소재", "핏", "사이즈", "색상", "가격", "배송"]
POSITIVE_LABEL = "긍정"
PAGE_SIZE = 20
SAMPLE_SIZE = 4000

NAV_INPUT    = "상품 URL"
NAV_OVERVIEW = "분석 결과"
NAV_PERSONA  = "페르소나 상세"
NAV_ASPECT   = "Aspect 상세"
NAV_REVIEWS  = "원본 리뷰 검색"

# 파이프라인 단계
PIPELINE_STEPS = ["크롤링", "DB 적재", "전처리", "분석", "시각화", "완료"]

# 색상 팔레트 (이미지 참고)
COLOR_GREEN      = "#4CAF50"
COLOR_GREEN_DARK = "#388E3C"
COLOR_GREEN_LIGHT = "#E8F5E9"
COLOR_CORAL      = "#E57373"
COLOR_CORAL_DARK = "#D32F2F"
COLOR_YELLOW     = "#FDD835"
COLOR_GRAY       = "#BDBDBD"
COLOR_GRAY_LIGHT = "#F5F5F5"
COLOR_TEXT        = "#333333"
COLOR_TEXT_LIGHT  = "#757575"


# ── 커스텀 CSS ─────────────────────────────────────────────────────────────────

def inject_css() -> None:
    st.markdown("""
    <style>
    /* ── 전체 폰트 & 배경 ── */
    .main .block-container {
        padding-top: 2rem;
        max-width: 1100px;
    }

    /* ── 사이드바 ── */
    section[data-testid="stSidebar"] {
        background-color: #FAFAFA;
        border-right: 1px solid #E0E0E0;
    }
    section[data-testid="stSidebar"] .stRadio > label {
        display: none;
    }
    section[data-testid="stSidebar"] .stRadio > div {
        gap: 0;
    }
    section[data-testid="stSidebar"] .stRadio > div > label {
        padding: 10px 16px;
        border-radius: 8px;
        margin: 2px 0;
        font-size: 0.95rem;
        transition: background 0.15s;
    }
    section[data-testid="stSidebar"] .stRadio > div > label:hover {
        background: #E8F5E9;
    }
    section[data-testid="stSidebar"] .stRadio > div > label[data-checked="true"] {
        background: #E8F5E9;
        font-weight: 600;
    }

    /* ── 메인 제목 ── */
    .main-title {
        font-size: 2rem;
        font-weight: 700;
        color: #212121;
        margin-bottom: 0;
        line-height: 1.2;
    }
    .main-subtitle {
        font-size: 1rem;
        color: #757575;
        margin-bottom: 1.5rem;
    }

    /* ── 타임라인 ── */
    .timeline-container {
        display: flex;
        align-items: center;
        justify-content: center;
        padding: 24px 0;
        margin: 16px 0 32px 0;
        background: #FAFAFA;
        border-radius: 12px;
        border: 1px solid #E0E0E0;
    }
    .timeline-step {
        display: flex;
        flex-direction: column;
        align-items: center;
        position: relative;
        min-width: 80px;
    }
    .timeline-circle {
        width: 40px;
        height: 40px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        font-weight: 700;
        font-size: 1rem;
        color: white;
        z-index: 2;
    }
    .timeline-circle.active {
        background-color: #4CAF50;
        box-shadow: 0 2px 8px rgba(76, 175, 80, 0.35);
    }
    .timeline-circle.inactive {
        background-color: #BDBDBD;
    }
    .timeline-label {
        margin-top: 8px;
        font-size: 0.82rem;
        color: #555;
        font-weight: 500;
    }
    .timeline-line {
        flex: 1;
        height: 3px;
        min-width: 40px;
        max-width: 80px;
        margin: 0 -4px;
        align-self: center;
        margin-bottom: 26px;
    }
    .timeline-line.active {
        background-color: #4CAF50;
    }
    .timeline-line.inactive {
        background-color: #BDBDBD;
    }

    /* ── 섹션 헤더 ── */
    .section-header {
        font-size: 1.3rem;
        font-weight: 700;
        color: #212121;
        margin: 28px 0 16px 0;
        padding-bottom: 8px;
        border-bottom: 2px solid #4CAF50;
        display: inline-block;
    }

    /* ── 배지 ── */
    .badge {
        display: inline-block;
        padding: 4px 14px;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 600;
        text-align: center;
    }
    .badge-green {
        background-color: #E8F5E9;
        color: #2E7D32;
        border: 1px solid #A5D6A7;
    }
    .badge-red {
        background-color: #FFEBEE;
        color: #C62828;
        border: 1px solid #EF9A9A;
    }
    .badge-yellow {
        background-color: #FFF8E1;
        color: #F57F17;
        border: 1px solid #FFE082;
    }

    /* ── 분석 테이블 ── */
    .analysis-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 0.9rem;
        margin: 12px 0;
    }
    .analysis-table thead th {
        background-color: #F5F5F5;
        padding: 12px 16px;
        text-align: center;
        font-weight: 600;
        color: #424242;
        border-bottom: 2px solid #E0E0E0;
    }
    .analysis-table tbody td {
        padding: 12px 16px;
        text-align: center;
        border-bottom: 1px solid #EEEEEE;
        color: #333;
    }
    .analysis-table tbody tr:hover {
        background-color: #FAFAFA;
    }
    .highlight-green {
        color: #2E7D32;
        font-weight: 700;
    }
    .highlight-red {
        color: #C62828;
        font-weight: 700;
    }

    /* ── Pain Point 카드 ── */
    .painpoint-card {
        background: #FAFAFA;
        border: 1px solid #E0E0E0;
        border-radius: 10px;
        padding: 20px;
    }
    .painpoint-title {
        font-weight: 700;
        font-size: 1rem;
        margin-bottom: 16px;
        color: #333;
    }
    .painpoint-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 8px 0;
        border-bottom: 1px solid #EEEEEE;
    }
    .painpoint-row:last-child {
        border-bottom: none;
    }
    .painpoint-persona {
        font-weight: 600;
        color: #424242;
        font-size: 0.88rem;
    }
    .painpoint-badge {
        background: #FFEBEE;
        color: #C62828;
        padding: 4px 12px;
        border-radius: 16px;
        font-size: 0.8rem;
        font-weight: 600;
    }

    /* ── 상품 정보 카드 ── */
    .product-card {
        background: white;
        border: 1px solid #E0E0E0;
        border-radius: 12px;
        padding: 20px;
        margin: 12px 0;
    }

    /* ── 사이드바 상품 정보 ── */
    .sidebar-product-info {
        background: #F5F5F5;
        border-radius: 8px;
        padding: 12px;
        margin: 8px 0;
        font-size: 0.85rem;
    }
    .sidebar-product-name {
        font-weight: 700;
        font-size: 0.95rem;
        color: #212121;
        margin-bottom: 4px;
    }
    .sidebar-product-meta {
        color: #757575;
        font-size: 0.82rem;
    }

    /* ── 분석 시작 버튼 ── */
    .stFormSubmitButton > button,
    div[data-testid="stFormSubmitButton"] > button {
        background-color: #4CAF50 !important;
        color: white !important;
        border: none !important;
        font-weight: 700 !important;
        font-size: 1.05rem !important;
        border-radius: 8px !important;
        padding: 12px !important;
    }
    .stFormSubmitButton > button:hover,
    div[data-testid="stFormSubmitButton"] > button:hover {
        background-color: #388E3C !important;
    }

    /* ── 메트릭 카드 ── */
    div[data-testid="stMetric"] {
        background: #FAFAFA;
        border: 1px solid #E0E0E0;
        border-radius: 10px;
        padding: 12px 16px;
    }
    div[data-testid="stMetric"] label {
        color: #757575 !important;
        font-size: 0.82rem !important;
    }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        color: #212121 !important;
        font-weight: 700 !important;
    }

    /* ── 리뷰 카드 ── */
    div[data-testid="stExpander"] {
        border-radius: 10px !important;
        border-color: #E0E0E0 !important;
    }

    /* 스크롤바 */
    ::-webkit-scrollbar { width: 6px; }
    ::-webkit-scrollbar-thumb { background: #BDBDBD; border-radius: 3px; }
    </style>
    """, unsafe_allow_html=True)


# ── 세션 상태 초기화 ──────────────────────────────────────────────────────────

def _init_state() -> None:
    defaults: Dict[str, Any] = {
        "nav_page":       NAV_INPUT,
        "product_data":   None,
        "reviews":        [],
        "summary":        None,
        "result_id":      None,
        "page_num":       0,
        "pipeline_step":  0,
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


# ── 타임라인 컴포넌트 ─────────────────────────────────────────────────────────

def render_timeline(current_step: int = 0) -> None:
    """현재 파이프라인 단계까지 활성화된 타임라인을 렌더링합니다."""
    steps_html = []
    for i, label in enumerate(PIPELINE_STEPS):
        num = i + 1
        is_active = i < current_step
        circle_cls = "active" if is_active else "inactive"
        steps_html.append(f"""
            <div class="timeline-step">
                <div class="timeline-circle {circle_cls}">{num}</div>
                <div class="timeline-label">{label}</div>
            </div>
        """)
        if i < len(PIPELINE_STEPS) - 1:
            line_cls = "active" if i < current_step - 1 else "inactive"
            steps_html.append(f'<div class="timeline-line {line_cls}"></div>')

    st.markdown(
        f'<div class="timeline-container">{"".join(steps_html)}</div>',
        unsafe_allow_html=True,
    )


# ── 판단 배지 ─────────────────────────────────────────────────────────────────

def get_judgment(pos_rate: Optional[float]) -> tuple[str, str]:
    """긍정률에 따라 (텍스트, badge 클래스)를 반환합니다."""
    if pos_rate is None:
        return ("-", "badge-yellow")
    if pos_rate >= 70:
        return ("강점", "badge-green")
    elif pos_rate >= 45:
        return ("주의", "badge-yellow")
    else:
        return ("개선 필요", "badge-red")


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
        result.append({**r, "text_clean": text_clean, "persona": persona,
                       "color": color, "size": size,
                       "aspects_sentences": {}, "absa_result": {}})
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

    # 페르소나별 aspect 상세 (히트맵 + 판단 테이블)
    personas = sorted(p for p in persona_dist if p != "unknown")
    heatmap: Dict[str, Dict] = {}
    persona_aspect_detail: List[Dict] = []
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
            pos_rate = round(pos / total_asp * 100, 1) if total_asp > 0 else None
            neg_rate = round(neg / total_asp * 100, 1) if total_asp > 0 else None
            neutral_rate = round(100 - (pos_rate or 0) - (neg_rate or 0), 1) if total_asp > 0 else None
            heatmap[persona][asp] = pos_rate

            # 평균 평점 계산 (해당 페르소나 리뷰의 평균)
            persona_ratings = [r["rating"] for r in p_revs if r.get("rating") is not None]
            avg_p_rating = round(sum(persona_ratings) / len(persona_ratings), 1) if persona_ratings else None

            if total_asp > 0:
                persona_aspect_detail.append({
                    "페르소나": persona,
                    "Aspect": asp,
                    "긍정": f"{pos_rate}%",
                    "부정": f"{neg_rate}%",
                    "중립": f"{neutral_rate}%",
                    "평균 평점": avg_p_rating,
                    "긍정률_raw": pos_rate,
                    "부정률_raw": neg_rate,
                })

    # 페르소나별 주요 pain point
    pain_points: List[Dict] = []
    for persona in personas:
        worst_asp = None
        worst_neg = 0
        for asp in ASPECTS:
            neg = sum(1 for r in [rv for rv in absa_reviews if rv.get("persona") == persona]
                      if r.get("absa_result", {}).get(asp)
                      and r["absa_result"][asp].get("label") != POSITIVE_LABEL)
            total_m = sum(1 for r in [rv for rv in absa_reviews if rv.get("persona") == persona]
                         if r.get("absa_result", {}).get(asp))
            neg_rate = round(neg / total_m * 100) if total_m > 0 else 0
            if neg_rate > worst_neg:
                worst_neg = neg_rate
                worst_asp = asp
        if worst_asp and worst_neg > 0:
            pain_points.append({
                "persona": persona,
                "aspect": worst_asp,
                "neg_rate": worst_neg,
            })
    # 상위 5개만
    pain_points.sort(key=lambda x: x["neg_rate"], reverse=True)
    pain_points = pain_points[:5]

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
        "persona_aspect_detail": persona_aspect_detail,
        "pain_points": pain_points,
    }


# ── MongoDB 저장 ──────────────────────────────────────────────────────────────

def save_to_mongo(db, product_data: Dict, reviews: List[Dict], summary: Dict) -> Optional[Any]:
    if db is None:
        return None
    try:
        db.products.update_one(
            {"product_id": product_data["product_id"]},
            {"$set": product_data},
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
        inserted = db.streamlit_analyses.insert_one(result_doc)
        result_id = inserted.inserted_id

        if reviews:
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

        return result_id
    except Exception as e:
        st.warning(f"MongoDB 저장 오류: {e}")
        return None


# ── 사이드바 네비게이션 ───────────────────────────────────────────────────────

def render_sidebar() -> str:
    with st.sidebar:
        st.markdown("## 🔍 VOC Engine")
        st.divider()

        # 네비게이션 구성
        pages = [NAV_INPUT]
        if st.session_state.summary:
            pages += [NAV_OVERVIEW, NAV_PERSONA, NAV_ASPECT, NAV_REVIEWS]

        current = st.session_state.nav_page
        if current not in pages:
            current = pages[0]

        # 페이지 라벨 (아이콘 제거, 깔끔하게)
        selected = st.radio(
            "페이지",
            pages,
            index=pages.index(current),
            label_visibility="collapsed",
        )
        st.session_state.nav_page = selected

        # 상품 정보 표시
        if st.session_state.product_data:
            st.divider()
            pd_ = st.session_state.product_data
            st.markdown("**분석 상품**")
            st.markdown(f"""
            <div class="sidebar-product-info">
                <div class="sidebar-product-name">{pd_.get('product_name', '')[:30]}</div>
                <div class="sidebar-product-meta">
                    {pd_.get('brand_name', '')} {'(' + pd_.get('brand_name', '') + ')' if pd_.get('brand_name') else ''}<br>
                    {st.session_state.summary['total']:,}건 리뷰
                </div>
            </div>
            """, unsafe_allow_html=True) if st.session_state.summary else None

        # 초기화 버튼
        if st.session_state.product_data:
            st.divider()
            if st.button("🗑️ 초기화", use_container_width=True):
                for k in ["product_data", "reviews", "summary", "result_id"]:
                    st.session_state[k] = None if k != "reviews" else []
                st.session_state.nav_page = NAV_INPUT
                st.session_state.page_num = 0
                st.session_state.pipeline_step = 0
                st.rerun()

        # MongoDB 연결 상태
        st.divider()
        db = get_db()
        if db is not None:
            st.caption("🟢 MongoDB 연결됨")
        else:
            st.caption("🔴 MongoDB 연결 없음")

    return selected


# ── 페이지: URL 입력 ──────────────────────────────────────────────────────────

def render_input_page() -> None:
    st.markdown('<div class="main-title">Review to Action</div>', unsafe_allow_html=True)
    st.markdown('<div class="main-subtitle">Seller Insight VOC Engine</div>', unsafe_allow_html=True)

    render_timeline(st.session_state.pipeline_step)

    with st.form("url_form"):
        product_url = st.text_input(
            "상품 URL",
            placeholder="https://www.musinsa.com/products/1420730",
        )
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
            st.session_state.nav_page = NAV_OVERVIEW
            st.rerun()


# ── 페이지: 분석 결과 (Overview) ───────────────────────────────────────────────

def render_overview_page() -> None:
    summary = st.session_state.summary
    product_data = st.session_state.product_data
    if not summary:
        st.info("분석 결과가 없습니다.")
        return

    st.markdown('<div class="main-title">Review to Action</div>', unsafe_allow_html=True)
    st.markdown('<div class="main-subtitle">Seller Insight VOC Engine</div>', unsafe_allow_html=True)

    render_timeline(6)  # 완료 상태

    # ── 페르소나 x Aspect 감성 집계 테이블 ──
    st.markdown('<div class="section-header">페르소나 × Aspect 감성 집계</div>',
                unsafe_allow_html=True)

    detail = summary.get("persona_aspect_detail", [])
    if detail:
        # 상위 N개 (긍정률이 높거나 낮은 순으로 정렬)
        sorted_detail = sorted(detail, key=lambda x: x.get("긍정률_raw") or 0, reverse=True)

        table_rows = ""
        for row in sorted_detail:
            pos_rate = row.get("긍정률_raw")
            neg_rate = row.get("부정률_raw")
            judgment_text, badge_cls = get_judgment(pos_rate)

            # 하이라이트: 긍정률이 70 이상이면 green, 부정률이 40 이상이면 red
            pos_cls = "highlight-green" if (pos_rate or 0) >= 70 else ""
            neg_cls = "highlight-red" if (neg_rate or 0) >= 40 else ""

            table_rows += f"""
            <tr>
                <td style="text-align:left;font-weight:500">{row['페르소나']}</td>
                <td>{row['Aspect']}</td>
                <td class="{pos_cls}">{row['긍정']}</td>
                <td class="{neg_cls}">{row['부정']}</td>
                <td>{row['중립']}</td>
                <td>{row['평균 평점'] or '-'}</td>
                <td><span class="badge {badge_cls}">{judgment_text}</span></td>
            </tr>
            """

        st.markdown(f"""
        <table class="analysis-table">
            <thead>
                <tr>
                    <th>페르소나</th>
                    <th>Aspect</th>
                    <th>긍정</th>
                    <th>부정</th>
                    <th>중립</th>
                    <th>평균 평점</th>
                    <th>판단</th>
                </tr>
            </thead>
            <tbody>
                {table_rows}
            </tbody>
        </table>
        """, unsafe_allow_html=True)
    else:
        st.info("페르소나별 상세 데이터가 없습니다.")

    # ── Aspect별 감성 분포 ──
    st.markdown('<div class="section-header">Aspect별 감성 분포</div>',
                unsafe_allow_html=True)

    col_chart, col_pain = st.columns([3, 2])

    with col_chart:
        st.markdown("**전체 Aspect 감성 비율**")
        asp_data = summary["aspect_sentiment"]
        valid_aspects = {k: v for k, v in asp_data.items() if v["언급수"] > 0}

        if valid_aspects:
            rows = []
            for asp, d in valid_aspects.items():
                total_m = d["언급수"]
                pos_pct = round(d["긍정"] / total_m * 100) if total_m else 0
                neg_pct = round(d["부정"] / total_m * 100) if total_m else 0
                neu_pct = 100 - pos_pct - neg_pct
                rows.append({"aspect": asp, "label": "긍정", "pct": pos_pct})
                rows.append({"aspect": asp, "label": "부정", "pct": neg_pct})
                rows.append({"aspect": asp, "label": "중립", "pct": neu_pct})

            df = pd.DataFrame(rows)
            fig = px.bar(
                df, y="aspect", x="pct", color="label",
                orientation="h",
                color_discrete_map={"긍정": "#4CAF50", "부정": "#E57373", "중립": "#BDBDBD"},
                category_orders={"aspect": list(valid_aspects.keys())},
                labels={"pct": "%", "aspect": ""},
            )
            fig.update_layout(
                barmode="stack",
                height=280,
                margin=dict(t=10, b=10, l=10, r=10),
                legend=dict(
                    orientation="h", yanchor="bottom", y=-0.25,
                    xanchor="center", x=0.5, title=None,
                ),
                xaxis=dict(showgrid=False, showticklabels=False),
                yaxis=dict(autorange="reversed"),
                plot_bgcolor="white",
            )
            # 퍼센트 텍스트 추가
            for trace in fig.data:
                trace.text = [f"{v}% +" if trace.name == "긍정" else "" for v in trace.x]
                trace.textposition = "auto"
                trace.textfont = dict(size=11)

            st.plotly_chart(fig, use_container_width=True)

    with col_pain:
        pain_points = summary.get("pain_points", [])
        pain_html = ""
        for pp in pain_points:
            pain_html += f"""
            <div class="painpoint-row">
                <span class="painpoint-persona">{pp['persona']}</span>
                <span class="painpoint-badge">{pp['aspect']} 부정 {pp['neg_rate']}%</span>
            </div>
            """
        st.markdown(f"""
        <div class="painpoint-card">
            <div class="painpoint-title">페르소나별 주요 pain point</div>
            {pain_html if pain_html else '<div style="color:#999">데이터 없음</div>'}
        </div>
        """, unsafe_allow_html=True)


# ── 페이지: 페르소나 상세 ─────────────────────────────────────────────────────

def render_persona_page() -> None:
    summary = st.session_state.summary
    if not summary:
        st.info("분석 결과가 없습니다.")
        return

    st.markdown('<div class="main-title">페르소나 상세</div>', unsafe_allow_html=True)
    st.markdown('<div class="main-subtitle">페르소나별 Aspect 감성 히트맵</div>',
                unsafe_allow_html=True)

    heatmap = summary.get("persona_aspect_heatmap", {})
    if not heatmap:
        st.info("페르소나 데이터가 없습니다.")
        return

    personas = list(heatmap.keys())
    z_vals = [[heatmap[p].get(asp) for asp in ASPECTS] for p in personas]
    text_vals = [[f"{v}%" if v is not None else "N/A" for v in row] for row in z_vals]

    fig = go.Figure(data=go.Heatmap(
        z=z_vals, x=ASPECTS, y=personas,
        colorscale=[[0, "#E57373"], [0.5, "#FFF9C4"], [1, "#4CAF50"]],
        zmin=0, zmax=100,
        text=text_vals, texttemplate="%{text}",
        textfont=dict(size=12, color="#333"),
        colorbar=dict(title="긍정률(%)", ticksuffix="%"),
    ))
    fig.update_layout(
        xaxis_title="Aspect", yaxis_title="페르소나",
        height=max(350, len(personas) * 45 + 120),
        margin=dict(t=20, b=20),
        plot_bgcolor="white",
    )
    st.plotly_chart(fig, use_container_width=True)

    # 페르소나 분포 파이 차트
    st.markdown('<div class="section-header">페르소나 분포</div>', unsafe_allow_html=True)
    pd_data = {k: v for k, v in summary["persona_distribution"].items() if k != "unknown"}
    if pd_data:
        fig2 = px.pie(
            names=list(pd_data.keys()), values=list(pd_data.values()),
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Set3,
        )
        fig2.update_layout(height=350, margin=dict(t=10, b=10))
        st.plotly_chart(fig2, use_container_width=True)


# ── 페이지: Aspect 상세 ───────────────────────────────────────────────────────

def render_aspect_page() -> None:
    summary = st.session_state.summary
    if not summary:
        st.info("분석 결과가 없습니다.")
        return

    st.markdown('<div class="main-title">Aspect 상세</div>', unsafe_allow_html=True)
    st.markdown('<div class="main-subtitle">Aspect별 감성 분석 상세</div>',
                unsafe_allow_html=True)

    valid = {k: v for k, v in summary["aspect_sentiment"].items() if v["언급수"] > 0}
    if not valid:
        st.info("분석된 aspect가 없습니다.")
        return

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="section-header">긍정률 레이더 차트</div>',
                    unsafe_allow_html=True)
        labels = list(valid.keys())
        values = [v["긍정률"] or 0 for v in valid.values()]
        fig = go.Figure(data=go.Scatterpolar(
            r=values + [values[0]], theta=labels + [labels[0]],
            fill="toself", line_color="#4CAF50",
            fillcolor="rgba(76,175,80,0.15)",
        ))
        fig.update_layout(
            polar=dict(
                radialaxis=dict(visible=True, range=[0, 100]),
                bgcolor="white",
            ),
            height=380, margin=dict(t=40, b=40),
            plot_bgcolor="white",
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown('<div class="section-header">긍정 / 부정 건수</div>',
                    unsafe_allow_html=True)
        rows = []
        for asp, d in valid.items():
            rows.append({"aspect": asp, "label": "긍정", "count": d["긍정"]})
            rows.append({"aspect": asp, "label": "부정", "count": d["부정"]})
        fig2 = px.bar(
            pd.DataFrame(rows), x="aspect", y="count", color="label",
            color_discrete_map={"긍정": "#4CAF50", "부정": "#E57373"},
            barmode="stack",
        )
        fig2.update_layout(
            height=380, margin=dict(t=40, b=10),
            plot_bgcolor="white",
            legend=dict(orientation="h", yanchor="bottom", y=-0.2,
                        xanchor="center", x=0.5, title=None),
        )
        st.plotly_chart(fig2, use_container_width=True)

    # 상세 수치 테이블
    st.markdown('<div class="section-header">Aspect별 상세 수치</div>',
                unsafe_allow_html=True)
    table_rows = ""
    for asp, d in valid.items():
        pos_rate = d["긍정률"]
        judgment_text, badge_cls = get_judgment(pos_rate)
        table_rows += f"""
        <tr>
            <td style="font-weight:600">{asp}</td>
            <td>{d['긍정']}</td>
            <td>{d['부정']}</td>
            <td>{d['언급수']}</td>
            <td>{f"{pos_rate}%" if pos_rate is not None else '-'}</td>
            <td><span class="badge {badge_cls}">{judgment_text}</span></td>
        </tr>
        """
    st.markdown(f"""
    <table class="analysis-table">
        <thead>
            <tr><th>Aspect</th><th>긍정</th><th>부정</th><th>언급수</th><th>긍정률</th><th>판단</th></tr>
        </thead>
        <tbody>{table_rows}</tbody>
    </table>
    """, unsafe_allow_html=True)

    # 평점 분포
    st.markdown('<div class="section-header">평점 분포</div>', unsafe_allow_html=True)
    rd = summary["rating_distribution"]
    color_map = {"1": "#E57373", "2": "#FF8A65", "3": "#FFD54F",
                 "4": "#81C784", "5": "#4CAF50"}
    fig3 = px.bar(x=list(rd.keys()), y=list(rd.values()),
                  labels={"x": "평점", "y": "리뷰 수"},
                  color=list(rd.keys()), color_discrete_map=color_map)
    fig3.update_layout(showlegend=False, height=280, margin=dict(t=10, b=10),
                       plot_bgcolor="white")
    st.plotly_chart(fig3, use_container_width=True)


# ── 페이지: 리뷰 브라우저 ─────────────────────────────────────────────────────

def render_reviews_page() -> None:
    reviews = st.session_state.reviews
    if not reviews:
        st.info("리뷰 데이터가 없습니다.")
        return

    st.markdown('<div class="main-title">원본 리뷰 검색</div>', unsafe_allow_html=True)
    st.markdown('<div class="main-subtitle">필터를 사용하여 리뷰를 탐색하세요</div>',
                unsafe_allow_html=True)

    all_personas = ["전체"] + sorted({r.get("persona") or "unknown" for r in reviews})

    with st.expander("🔎 필터", expanded=True):
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
                persona_badge = r.get('persona') or 'unknown'
                st.markdown(
                    f"**{stars}** {rating or '-'}점 &nbsp;·&nbsp; "
                    f"{r.get('date') or '-'} &nbsp;·&nbsp; "
                    f"<span class='badge badge-green' style='font-size:0.75rem;padding:2px 8px'>"
                    f"{persona_badge}</span>",
                    unsafe_allow_html=True,
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
                        if label == POSITIVE_LABEL:
                            icon_html = f'<span style="color:#4CAF50;font-weight:700">● {asp}: {label}</span>'
                        else:
                            icon_html = f'<span style="color:#E57373;font-weight:700">● {asp}: {label}</span>'
                        st.markdown(icon_html, unsafe_allow_html=True)
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
            f"<p style='text-align:center;padding-top:6px;color:#757575'>"
            f"{page_num + 1} / {total_pages} 페이지</p>",
            unsafe_allow_html=True,
        )
    with c3:
        if st.button("다음 ▶", key=f"next_{key_suffix}",
                     disabled=(page_num >= total_pages - 1)):
            st.session_state.page_num = page_num + 1
            st.rerun()


# ── 파이프라인 실행 ───────────────────────────────────────────────────────────

def run_pipeline(product_url: str, review_url: str, absa_mode: str) -> bool:
    use_sample = "샘플" in absa_mode

    # Step 1: 크롤링
    st.session_state.pipeline_step = 1
    with st.status("Step 1: 상품 정보 수집 중...", expanded=True) as s1:
        try:
            product_data = crawl_product(product_url)
            st.session_state.product_data = product_data
            s1.update(label=f"✅ Step 1 완료 — {product_data.get('product_name', '')[:40]}")
        except Exception as e:
            s1.update(label=f"❌ Step 1 실패: {e}", state="error")
            st.error(str(e))
            return False

    # 상품 카드 간략 표시
    with st.container(border=True):
        img_col, info_col = st.columns([1, 3])
        with img_col:
            if product_data.get("main_image_url"):
                st.image(product_data["main_image_url"], width=120)
        with info_col:
            st.markdown(f"**{product_data.get('product_name') or ''}**")
            info = product_data.get("product_info") or {}
            detail_str = "  ·  ".join(
                f"{lbl}: {info.get(lbl) or '-'}"
                for lbl in ["성별", "조회수", "누적판매", "시즌"]
            )
            st.caption(detail_str)

    # Step 2: DB 적재 (리뷰 크롤링)
    st.session_state.pipeline_step = 2
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
            s2.update(label=f"✅ Step 2 완료 — {len(reviews_raw):,}개 리뷰 수집")
        except Exception as e:
            s2.update(label=f"❌ Step 2 실패: {e}", state="error")
            st.error(str(e))
            return False

    # Step 3: 전처리
    st.session_state.pipeline_step = 3
    with st.status("Step 3: 전처리 및 Aspect 매핑 중...", expanded=False) as s3:
        reviews = preprocess_reviews(reviews_raw)
        map_aspects_inplace(reviews)
        s3.update(label=f"✅ Step 3 완료 — {len(reviews):,}개 전처리")

    # Step 4: 분석 (ABSA)
    st.session_state.pipeline_step = 4
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
            s4.update(label=f"✅ Step 4 완료 — {len(absa_reviews):,}개 감성 분석")
        except Exception as e:
            s4.update(label=f"❌ Step 4 실패: {e}", state="error")
            st.error(str(e))
            return False

    # Step 5: 시각화 준비 + MongoDB 저장
    st.session_state.pipeline_step = 5
    with st.status("Step 5: 집계 및 저장 중...", expanded=False) as s5:
        summary = build_summary(
            product_data, reviews, absa_reviews,
            sampled=sampled,
            sample_size=SAMPLE_SIZE if sampled else len(reviews),
        )
        st.session_state.reviews = reviews
        st.session_state.summary = summary

        db = get_db()
        result_id = save_to_mongo(db, product_data, reviews, summary)
        st.session_state.result_id = result_id

        mongo_msg = f" · MongoDB 저장 완료" if result_id else " · MongoDB 저장 생략"
        s5.update(label=f"✅ Step 5 완료{mongo_msg}")

    # Step 6: 완료
    st.session_state.pipeline_step = 6
    return True


# ── 메인 ─────────────────────────────────────────────────────────────────────

def main() -> None:
    inject_css()
    nav = render_sidebar()

    if nav == NAV_INPUT:
        render_input_page()
    elif nav == NAV_OVERVIEW:
        render_overview_page()
    elif nav == NAV_PERSONA:
        render_persona_page()
    elif nav == NAV_ASPECT:
        render_aspect_page()
    elif nav == NAV_REVIEWS:
        render_reviews_page()


main()