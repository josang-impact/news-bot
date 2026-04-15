import os
import re
import html
import time
import logging
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from collections import defaultdict
from urllib.parse import urlparse

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

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()
SHEET_ID = os.getenv("SHEET_ID", "").strip()
SHEET_GID = os.getenv("SHEET_GID", "0").strip()

NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID", "").strip()
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET", "").strip()

NEWSAPI_KEY = os.getenv("NEWSAPI_KEY", "").strip()

# 빈 문자열이 들어와도 기본값으로 동작하도록 `or` 처리
MAX_ARTICLES_PER_ORG = int(os.getenv("MAX_ARTICLES_PER_ORG") or 3)
NAVER_DISPLAY = int(os.getenv("NAVER_DISPLAY") or 20)
NAVER_PAGES = int(os.getenv("NAVER_PAGES") or 5)

# Slack 전송 간 간격 (초). Incoming Webhook 권고치(~1 msg/sec) 준수
SLACK_SEND_INTERVAL = float(os.getenv("SLACK_SEND_INTERVAL") or 1.0)

# 제목에 반드시 포함되어야 하는 키워드
TITLE_ONLY_KEYWORDS = {"카카오", "김범수"}

# 기사 수 제한 없는 조직
UNCAPPED_ORGS = {"카카오", "김범수", "브라이언임팩트", "카카오임팩트"}

# 짧은 키워드 기준
SHORT_KW_KR = 3
SHORT_KW_EN = 5


# ──────────────────────────────────────────────
# 출처(언론사) 매핑
# ──────────────────────────────────────────────
SOURCE_NAME_MAP = {
    "chosun.com": "조선일보",
    "donga.com": "동아일보",
    "hani.co.kr": "한겨레",
    "joongang.co.kr": "중앙일보",
    "mk.co.kr": "매일경제",
    "hankyung.com": "한국경제",
    "news.naver.com": "네이버뉴스",
    "yna.co.kr": "연합뉴스",
    "yonhapnews.co.kr": "연합뉴스",
    "zdnet.co.kr": "ZDNet Korea",
    "etnews.com": "전자신문",
    "bloter.net": "블로터",
    "mt.co.kr": "머니투데이",
    "edaily.co.kr": "이데일리",
    "sedaily.com": "서울경제",
    "kmib.co.kr": "국민일보",
    "khan.co.kr": "경향신문",
    "mbc.co.kr": "MBC",
    "kbs.co.kr": "KBS",
    "sbs.co.kr": "SBS",
    "ytn.co.kr": "YTN",
    "jtbc.co.kr": "JTBC",
    "news1.kr": "뉴스1",
    "newsis.com": "뉴시스",
    "hankookilbo.com": "한국일보",
    "fnnews.com": "파이낸셜뉴스",
    "mbn.co.kr": "MBN",
    "businesspost.co.kr": "비즈니스포스트",
    "techm.kr": "테크M",
    "ddaily.co.kr": "디지털데일리",
    "dt.co.kr": "디지털타임스",
    "inews24.com": "아이뉴스24",
}


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


def parse_iso_datetime(value):
    """ISO 8601 형식 문자열을 KST datetime으로 변환 (NewsAPI용)."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
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
    """쉼표로 구분된 문자열을 리스트로 변환."""
    s = safe_str(value)
    if not s:
        return []
    return [item.strip() for item in s.split(",") if item.strip()]


def parse_keywords(query_str):
    """검색어 필드에서 or로 구분된 개별 키워드를 추출."""
    parts = re.split(r"\bor\b", query_str, flags=re.IGNORECASE)
    keywords = []
    for p in parts:
        cleaned = p.strip().strip('"').strip()
        if cleaned:
            keywords.append(cleaned)
    return keywords


def extract_source(link: str) -> str:
    """URL에서 출처(언론사)명 추출. 매핑에 없으면 도메인 그대로."""
    if not link:
        return ""
    try:
        host = urlparse(link).netloc.lower()
        for prefix in ("www.", "m.", "news."):
            if host.startswith(prefix):
                host = host[len(prefix):]
                break
        for domain, name in SOURCE_NAME_MAP.items():
            if host == domain or host.endswith("." + domain):
                return name
        parts = host.split(".")
        if len(parts) >= 2:
            return ".".join(parts[-2:])
        return host
    except Exception:
        return ""


# ──────────────────────────────────────────────
# 필터링
# ──────────────────────────────────────────────
def is_short_keyword(kw):
    """짧은 키워드인지 판단. 한글 3글자 이하, 영문 5자 이하."""
    kw = kw.strip()
    if re.search(r"[가-힣]", kw):
        return len(kw) <= SHORT_KW_KR
    return len(kw) <= SHORT_KW_EN


def keyword_match(keywords, title, summary, has_must_filter):
    """
    매칭 규칙:
    1) TITLE_ONLY_KEYWORDS → 항상 제목 필수
    2) 짧은 키워드 + MUST 필터 있음 → 제목 또는 요약
       짧은 키워드 + MUST 필터 없음 → 제목 필수
    3) 긴 키워드 → 제목 또는 요약
    """
    title_lower = title.lower()
    summary_lower = summary.lower()

    for kw in keywords:
        kw_lower = kw.lower()

        if kw in TITLE_ONLY_KEYWORDS:
            if kw_lower in title_lower:
                return True
            continue

        if is_short_keyword(kw):
            if kw_lower in title_lower:
                return True
            if has_must_filter and kw_lower in summary_lower:
                return True
            continue

        if kw_lower in title_lower or kw_lower in summary_lower:
            return True

    return False


def relevance_pass(title, summary, keywords, must_all, must_any, block):
    """키워드 매칭 → BLOCK → MUST_ALL → MUST_ANY 순으로 필터링."""
    has_must_filter = bool(must_all or must_any)

    if not keyword_match(keywords, title, summary, has_must_filter):
        return False

    text = (title + " " + summary).lower()

    for b in block:
        if b.lower() in text:
            return False

    if must_all:
        for m in must_all:
            if m.lower() not in text:
                return False

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
    """네이버 뉴스 검색 API 호출. 결과 리스트 반환."""
    if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
        logger.warning("네이버 API 키가 설정되지 않았습니다.")
        return []

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


# ──────────────────────────────────────────────
# NewsAPI.org 검색
# ──────────────────────────────────────────────
def search_newsapi(query, start_dt, end_dt):
    """NewsAPI.org /v2/everything 엔드포인트로 검색."""
    if not NEWSAPI_KEY:
        return []

    from_utc = start_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    to_utc = end_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        "q": query,
        "from": from_utc,
        "to": to_utc,
        "sortBy": "publishedAt",
        "pageSize": 50,
        "language": "ko",
        "apiKey": NEWSAPI_KEY,
    }

    try:
        r = requests.get(
            "https://newsapi.org/v2/everything",
            params=params,
            timeout=20,
        )
        r.raise_for_status()
    except requests.RequestException as e:
        logger.warning(f"NewsAPI 검색 실패 (query={query}): {e}")
        return []

    articles = r.json().get("articles", [])
    results = []

    for a in articles:
        title = clean_text(a.get("title"))
        link = a.get("url")
        pub_date = parse_iso_datetime(a.get("publishedAt"))
        summary = clean_text(a.get("description") or a.get("content") or "")

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

    logger.info(f"NewsAPI 결과: {len(results)}건 (query={query})")
    return results


# ──────────────────────────────────────────────
# 통합 검색
# ──────────────────────────────────────────────
def search_all_keywords(keywords, start_dt, end_dt):
    """네이버 + NewsAPI 통합 검색, 링크 기준 중복 제거."""
    seen_links = set()
    results = []

    def _add(item):
        if item["link"] not in seen_links:
            seen_links.add(item["link"])
            results.append(item)

    for kw in keywords:
        for item in search_naver(kw):
            _add(item)

    if NEWSAPI_KEY and keywords:
        newsapi_query = " OR ".join(f'"{kw}"' for kw in keywords)
        for item in search_newsapi(newsapi_query, start_dt, end_dt):
            _add(item)

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
# Slack 메시지 작성 & 발송
# ──────────────────────────────────────────────
def build_slack_payload(org: str, item: dict) -> dict:
    """기사 1건을 Slack Block Kit 메시지로 변환."""
    title = item["title"]
    link = item["link"]
    summary = item.get("summary", "") or ""
    dt = item.get("published_at")
    time_str = dt.strftime("%m-%d %H:%M") if dt else ""
    source = extract_source(link)

    meta_parts = []
    if time_str:
        meta_parts.append(f"({time_str})")
    if source:
        meta_parts.append(f"({source})")
    meta = " ".join(meta_parts)

    # Slack mrkdwn: 링크 텍스트/URL 모두 꺾쇠·파이프 금지
    safe_title = title.replace("<", "‹").replace(">", "›").replace("|", "｜")
    safe_link = link.replace(">", "%3E").replace("|", "%7C")

    header = f"*<{safe_link}|[{org}] {safe_title}>*"
    if meta:
        header = f"{header} {meta}"

    clean_summary = re.sub(r"\s+", " ", summary).strip()
    if len(clean_summary) > 240:
        clean_summary = clean_summary[:237] + "…"

    body = header
    if clean_summary:
        body += f"\n{clean_summary}"

    return {
        "blocks": [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": body},
            }
        ],
        "text": f"[{org}] {title}",
        "unfurl_links": False,
        "unfurl_media": False,
    }


def send_slack(payload: dict) -> bool:
    """Slack Incoming Webhook 전송. 성공 여부 반환."""
    if not SLACK_WEBHOOK_URL:
        logger.error("SLACK_WEBHOOK_URL이 설정되지 않았습니다.")
        return False
    try:
        r = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=20)
        r.raise_for_status()
        return True
    except requests.RequestException as e:
        logger.error(f"Slack 전송 실패: {e}")
        return False


# ──────────────────────────────────────────────
# 메인
# ──────────────────────────────────────────────
def main():
    start_dt, end_dt = get_delivery_window()
    if not start_dt:
        logger.info("주말이므로 발송하지 않습니다.")
        return

    logger.info(f"발송 윈도우: {start_dt} ~ {end_dt}")

    if NEWSAPI_KEY:
        logger.info("NewsAPI.org 활성화됨")
    else:
        logger.info("NewsAPI.org 비활성화 (NEWSAPI_KEY 미설정)")

    df = load_sheet()

    # ── 1단계: 조직별 검색 & 필터링 ──
    org_results = defaultdict(list)

    for _, row in df.iterrows():
        org = safe_str(row.get("조직명"))
        query = safe_str(row.get("검색어"))

        if not org or not query:
            continue

        must_all = parse_csv_list(row.get("MUST_ALL"))
        must_any = parse_csv_list(row.get("MUST_ANY"))
        block = parse_csv_list(row.get("BLOCK"))

        keywords = parse_keywords(query)
        if not keywords:
            continue

        news = search_all_keywords(keywords, start_dt, end_dt)

        for item in news:
            title = item["title"]
            summary = item["summary"]
            pub = item["published_at"]

            if not pub:
                continue
            if not (start_dt <= pub <= end_dt):
                continue

            if not relevance_pass(title, summary, keywords, must_all, must_any, block):
                continue

            org_results[org].append(item)

    # ── 2단계: 조직별 중복 제거, 정렬, 기사 1개씩 발송 ──
    total_sent = 0

    for org, items in org_results.items():
        deduped = {}
        for it in items:
            if it["link"] not in deduped:
                deduped[it["link"]] = it

        sorted_items = sorted(
            deduped.values(),
            key=lambda x: x["published_at"] or datetime.min.replace(tzinfo=KST),
            reverse=True,
        )

        if org in UNCAPPED_ORGS:
            final = sorted_items
        else:
            final = sorted_items[:MAX_ARTICLES_PER_ORG]

        if not final:
            continue

        org_sent = 0
        for it in final:
            payload = build_slack_payload(org, it)
            if send_slack(payload):
                org_sent += 1
                total_sent += 1
            time.sleep(SLACK_SEND_INTERVAL)

        logger.info(f"✅ {org}: {org_sent}/{len(final)}건 전송 완료")

    logger.info(f"총 {total_sent}건 발송 완료")


if __name__ == "__main__":
    main()
