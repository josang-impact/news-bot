import os
import re
import html
import time
import logging
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from collections import defaultdict

import pandas as pd
import requests


# ──────────────────────────────────────────────
# 로깅 설정
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# 환경변수
# ──────────────────────────────────────────────
KST = timezone(timedelta(hours=9))

KAKAOWORK_WEBHOOK_URL = os.getenv("KAKAOWORK_WEBHOOK_URL", "").strip()
SHEET_ID = os.getenv("SHEET_ID", "").strip()
SHEET_GID = os.getenv("SHEET_GID", "0").strip()

NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID", "").strip()
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET", "").strip()

MAX_ARTICLES_PER_ORG = int(os.getenv("MAX_ARTICLES_PER_ORG", "3"))
NAVER_DISPLAY = int(os.getenv("NAVER_DISPLAY", "20"))
NAVER_PAGES = int(os.getenv("NAVER_PAGES", "5"))


# ──────────────────────────────────────────────
# 유틸리티
# ──────────────────────────────────────────────
def clean_text(text):
    """HTML 엔티티 디코딩 및 태그 제거."""
    text = html.unescape(str(text or ""))
    text = re.sub(r"<[^>]+>", "", text)
    return text.strip()


def parse_pubdate(value):
    """RSS pubDate 문자열을 KST datetime으로 변환."""
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(KST)
    except Exception:
        return None


def safe_str(value):
    """시트 셀 값을 안전하게 문자열로 변환 (NaN 처리)."""
    if pd.isna(value):
        return ""
    return str(value).strip()


def parse_csv_list(value):
    """쉼표로 구분된 문자열을 리스트로 변환. 빈 문자열이면 빈 리스트 반환."""
    s = safe_str(value)
    if not s:
        return []
    return [item.strip() for item in s.split(",") if item.strip()]


def parse_keywords(query_str):
    """
    검색어 필드에서 or로 구분된 개별 키워드를 추출.
    예: '"브라이언임팩트" or "브라이언 임팩트"' → ['브라이언임팩트', '브라이언 임팩트']
    """
    parts = re.split(r"\bor\b", query_str, flags=re.IGNORECASE)
    keywords = []
    for p in parts:
        cleaned = p.strip().strip('"').strip()
        if cleaned:
            keywords.append(cleaned)
    return keywords


# ──────────────────────────────────────────────
# 필터링
# ──────────────────────────────────────────────
def relevance_pass(title, summary, must_all, must_any, block):
    """
    시트의 MUST_ALL, MUST_ANY, BLOCK 컬럼을 활용한 필터링.

    - BLOCK:    하나라도 포함되면 제외
    - MUST_ALL: 모든 키워드가 포함되어야 통과
    - MUST_ANY: 하나 이상 포함되어야 통과
    - 세 컬럼 모두 비어 있으면 기본 통과
    """
    text = (title + " " + summary).lower()

    # BLOCK 체크
    for b in block:
        if b.lower() in text:
            return False

    # MUST_ALL 체크
    if must_all:
        for m in must_all:
            if m.lower() not in text:
                return False

    # MUST_ANY 체크
    if must_any:
        if not any(m.lower() in text for m in must_any):
            return False

    return True


# ──────────────────────────────────────────────
# 시트 로딩
# ──────────────────────────────────────────────
def load_sheet():
    """구글 시트를 CSV로 읽어 DataFrame 반환."""
    url = (
        f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
        f"/export?format=csv&gid={SHEET_GID}"
    )
    try:
        df = pd.read_csv(url)
        logger.info(f"시트 로딩 완료: {len(df)}행")
        return df
    except Exception as e:
        logger.error(f"시트 로딩 실패: {e}")
        raise


# ──────────────────────────────────────────────
# 네이버 검색
# ──────────────────────────────────────────────
def search_naver(query):
    """네이버 뉴스 검색 API를 호출하여 결과 리스트 반환."""
    headers = {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    }

    results = []

    for page in range(NAVER_PAGES):
        start = 1 + page * NAVER_DISPLAY
        params = {
            "query": query,
            "display": NAVER_DISPLAY,
            "start": start,
            "sort": "date",
        }

        try:
            r = requests.get(
                "https://openapi.naver.com/v1/search/news.json",
                headers=headers,
                params=params,
                timeout=20,
            )
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"네이버 검색 실패 (query={query}, page={page}): {e}")
            break

        items = r.json().get("items", [])
        if not items:
            break

        for it in items:
            title = clean_text(it.get("title"))
            summary = clean_text(it.get("description"))
            link = it.get("originallink") or it.get("link")
            pub_date = parse_pubdate(it.get("pubDate"))

            if not title or not link:
                continue

            results.append(
                {
                    "title": title,
                    "summary": summary,
                    "link": link,
                    "published_at": pub_date,
                }
            )

        time.sleep(0.2)

    return results


def search_all_keywords(keywords):
    """
    여러 키워드(or로 분리된)를 각각 검색하고 결과를 합침.
    링크 기준으로 중복 제거.
    """
    seen_links = set()
    results = []

    for kw in keywords:
        for item in search_naver(kw):
            if item["link"] not in seen_links:
                seen_links.add(item["link"])
                results.append(item)

    return results


# ──────────────────────────────────────────────
# 발송 시간 윈도우
# ──────────────────────────────────────────────
def get_delivery_window():
    """발송 대상 기사의 시간 범위를 반환. 주말이면 (None, None)."""
    now = datetime.now(KST)
    weekday = now.weekday()

    if weekday >= 5:
        return None, None

    end_dt = now.replace(hour=7, minute=59, second=59, microsecond=0)

    if weekday == 0:
        start_base = now - timedelta(days=3)
    else:
        start_base = now - timedelta(days=1)

    start_dt = start_base.replace(hour=8, minute=0, second=0, microsecond=0)

    return start_dt, end_dt


# ──────────────────────────────────────────────
# 메시지 작성 & 발송
# ──────────────────────────────────────────────
def build_message(org, items):
    """카카오워크로 보낼 텍스트 메시지 생성."""
    lines = [f"📰 {org}", ""]

    for it in items:
        t = it["title"]
        l = it["link"]
        dt = it["published_at"]
        time_str = dt.strftime("%m-%d %H:%M") if dt else ""

        lines.append(f"- {t}")
        lines.append(f"  ({time_str})")
        lines.append(f"  {l}")
        lines.append("")

    lines.append(f"총 {len(items)}건")
    return "\n".join(lines)


def send_kakaowork(text):
    """카카오워크 웹훅으로 메시지 전송."""
    try:
        r = requests.post(
            KAKAOWORK_WEBHOOK_URL,
            json={"text": text},
            timeout=20,
        )
        r.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"카카오워크 전송 실패: {e}")


# ──────────────────────────────────────────────
# 메인
# ──────────────────────────────────────────────
def main():
    start_dt, end_dt = get_delivery_window()
    if not start_dt:
        logger.info("주말이므로 발송하지 않습니다.")
        return

    logger.info(f"발송 윈도우: {start_dt} ~ {end_dt}")

    df = load_sheet()

    # ── 1단계: 조직별로 행을 묶어서 검색 & 필터링 ──
    org_results = defaultdict(list)

    for _, row in df.iterrows():
        org = safe_str(row.get("조직명"))
        query = safe_str(row.get("검색어"))

        if not org or not query:
            continue

        # 시트 필터링 컬럼 파싱
        must_all = parse_csv_list(row.get("MUST_ALL"))
        must_any = parse_csv_list(row.get("MUST_ANY"))
        block = parse_csv_list(row.get("BLOCK"))

        # or로 구분된 키워드 전체 검색
        keywords = parse_keywords(query)
        if not keywords:
            continue

        news = search_all_keywords(keywords)

        for item in news:
            title = item["title"]
            summary = item["summary"]
            pub = item["published_at"]

            # 시간 범위 필터
            if not pub:
                continue
            if not (start_dt <= pub <= end_dt):
                continue

            # MUST_ALL / MUST_ANY / BLOCK 필터
            if not relevance_pass(title, summary, must_all, must_any, block):
                continue

            org_results[org].append(item)

    # ── 2단계: 조직별 중복 제거, 정렬, 발송 ──
    sent_count = 0

    for org, items in org_results.items():
        # 링크 기준 중복 제거
        deduped = {}
        for it in items:
            if it["link"] not in deduped:
                deduped[it["link"]] = it

        # 최신순 정렬
        sorted_items = sorted(
            deduped.values(),
            key=lambda x: x["published_at"] or datetime.min.replace(tzinfo=KST),
            reverse=True,
        )

        final = sorted_items[:MAX_ARTICLES_PER_ORG]

        if not final:
            continue

        msg = build_message(org, final)
        send_kakaowork(msg)
        sent_count += 1
        logger.info(f"✅ {org}: {len(final)}건 전송 완료")

    logger.info(f"총 {sent_count}개 조직 발송 완료")


if __name__ == "__main__":
    main()
