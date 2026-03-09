import os
import re
import html
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import pandas as pd
import requests


KST = timezone(timedelta(hours=9))

KAKAOWORK_WEBHOOK_URL = os.getenv("KAKAOWORK_WEBHOOK_URL", "").strip()
SHEET_ID = os.getenv("SHEET_ID", "").strip()
SHEET_GID = os.getenv("SHEET_GID", "0").strip()

NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID", "").strip()
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET", "").strip()
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY", "").strip()

MAX_ARTICLES_PER_ORG = int(os.getenv("MAX_ARTICLES_PER_ORG", "3"))
MAX_TOTAL_ARTICLES = int(os.getenv("MAX_TOTAL_ARTICLES", "50"))

NAVER_DISPLAY = int(os.getenv("NAVER_DISPLAY", "20"))
NAVER_PAGES = int(os.getenv("NAVER_PAGES", "5"))

# 쉼표로 넣으면 예외 조직은 조직별 기사 수 상한을 적용하지 않음
UNCAPPED_ORGS = {
    x.strip()
    for x in os.getenv("UNCAPPED_ORGS", "카카오,브라이언임팩트,김범수").split(",")
    if x.strip()
}

NAVER_NEWS_API_URL = "https://openapi.naver.com/v1/search/news.json"
NEWSAPI_URL = "https://newsapi.org/v2/everything"


def build_google_sheet_csv_url(sheet_id: str, gid: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"


def load_sheet() -> pd.DataFrame:
    if not SHEET_ID:
        raise ValueError("SHEET_ID 환경변수가 비어 있습니다.")
    url = build_google_sheet_csv_url(SHEET_ID, SHEET_GID)
    return pd.read_csv(url)


def clean_text(text: str) -> str:
    text = html.unescape(str(text or ""))
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def split_keywords(value: str) -> list[str]:
    if pd.isna(value) or str(value).strip() == "":
        return []
    return [x.strip() for x in str(value).split(",") if x.strip()]


def parse_naver_pubdate(value: str):
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(KST)
    except Exception:
        return None


def parse_iso_datetime(value: str):
    if not value:
        return None
    try:
        value = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(KST)
    except Exception:
        return None


def get_delivery_window(now_kst: datetime | None = None) -> tuple[datetime | None, datetime | None]:
    """
    발송 시점 기준 기사 수집 구간

    - 월요일: 직전 금요일 08:00:00 ~ 월요일 07:59:59
    - 화~금: 전날 08:00:00 ~ 당일 07:59:59
    - 토/일: 발송 안 함
    """
    if now_kst is None:
        now_kst = datetime.now(KST)

    weekday = now_kst.weekday()  # 월=0, 화=1, ..., 토=5, 일=6

    if weekday >= 5:
        return None, None

    end_dt = now_kst.replace(hour=7, minute=59, second=59, microsecond=0)

    if weekday == 0:
        start_base = now_kst - timedelta(days=3)
    else:
        start_base = now_kst - timedelta(days=1)

    start_dt = start_base.replace(hour=8, minute=0, second=0, microsecond=0)
    return start_dt, end_dt


def normalize_query(raw_query: str, org_name: str) -> str:
    q = str(raw_query or "").strip()
    if not q:
        return f'"{org_name}"'
    return q


def split_query_tokens(query: str) -> list[str]:
    """
    예:
    "브라이언임팩트" or "브라이언 임팩트" or "Brianimpact"
    -> ["브라이언임팩트", "브라이언 임팩트", "Brianimpact"]
    """
    if not query:
        return []

    parts = re.split(r"\bor\b", query, flags=re.IGNORECASE)
    tokens = []

    for part in parts:
        token = part.strip()
        token = token.strip('"').strip("'").strip()
        if token:
            tokens.append(token)

    return tokens


def get_naver_query(query: str, org_name: str) -> str:
    """
    네이버 API용 검색어:
    OR 전체를 보내지 않고 첫 번째 토큰만 사용
    """
    tokens = split_query_tokens(query)
    if tokens:
        return tokens[0]
    return org_name


def contains_all(text: str, keywords: list[str]) -> bool:
    lowered = text.lower()
    return all(k.lower() in lowered for k in keywords)


def contains_any(text: str, keywords: list[str]) -> bool:
    if not keywords:
        return True
    lowered = text.lower()
    return any(k.lower() in lowered for k in keywords)


def contains_block(text: str, keywords: list[str]) -> bool:
    lowered = text.lower()
    return any(k.lower() in lowered for k in keywords)


def strip_source_from_title(title: str, source: str) -> str:
    if not title:
        return ""

    title_clean = title.strip()
    source_clean = (source or "").strip()

    if not source_clean:
        return title_clean

    patterns = [
        f" - {source_clean}",
        f" | {source_clean}",
        f" / {source_clean}",
    ]

    for pattern in patterns:
        if title_clean.endswith(pattern):
            return title_clean[:-len(pattern)].strip()

    title_clean = re.sub(
        rf"(\s*[-|/]\s*)?{re.escape(source_clean)}$",
        "",
        title_clean,
        flags=re.IGNORECASE,
    ).strip()

    return title_clean


def normalize_title_for_dedup(title: str) -> str:
    title = clean_text(title).lower()
    title = re.sub(r"\[[^\]]+\]", "", title)
    title = re.sub(r"[\"'“”‘’]", "", title)
    title = re.sub(r"[^0-9a-zA-Z가-힣\s]", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title


def get_org_emoji(org_type: str) -> str:
    org_type = clean_text(org_type)

    if "재단" in org_type:
        return "🌱"
    if "협동조합" in org_type:
        return "🤝"
    if "병원" in org_type:
        return "🏥"
    if "학교" in org_type:
        return "🎓"
    if "기업" in org_type or "org" in org_type.lower():
        return "🏢"
    if "person" in org_type.lower() or "pers" in org_type.lower() or "인물" in org_type:
        return "👤"

    return "📰"


def extract_source_name(url: str) -> str:
    if not url:
        return ""
    m = re.search(r"https?://(?:www\.)?([^/]+)", url)
    if not m:
        return ""
    return m.group(1).lower().replace("www.", "")


def row_to_config(row: pd.Series) -> dict:
    org_name = clean_text(row.get("조직명", ""))
    full_query = normalize_query(row.get("검색어", ""), org_name)

    return {
        "org_name": org_name,
        "query": full_query,
        "naver_query": get_naver_query(full_query, org_name),
        "type": clean_text(row.get("유형", "")),
        "must_all": split_keywords(row.get("MUST_ALL", "")),
        "must_any": split_keywords(row.get("MUST_ANY", "")),
        "block": split_keywords(row.get("BLOCK", "")),
        "seq": clean_text(row.get("연번", "")),
        "query_tokens": [token.lower() for token in split_query_tokens(full_query)],
    }


def search_naver_news(query: str, display: int = 20, pages: int = 5) -> list[dict]:
    if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
        raise ValueError("NAVER_CLIENT_ID 또는 NAVER_CLIENT_SECRET 환경변수가 비어 있습니다.")

    headers = {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    }

    results = []

    for page in range(pages):
        start = 1 + page * display
        params = {
            "query": query,
            "display": display,
            "start": start,
            "sort": "date",
        }

        try:
            response = requests.get(
                NAVER_NEWS_API_URL,
                headers=headers,
                params=params,
                timeout=20,
            )
            response.raise_for_status()
            items = response.json().get("items", [])

            if not items:
                break

            for item in items:
                title = clean_text(item.get("title", ""))
                summary = clean_text(item.get("description", ""))
                pub_date = parse_naver_pubdate(item.get("pubDate", ""))

                originallink = clean_text(item.get("originallink", ""))
                link = originallink or clean_text(item.get("link", ""))

                if not title or not link:
                    continue

                results.append(
                    {
                        "title": title,
                        "summary": summary,
                        "link": link,
                        "published_at": pub_date,
                        "source": extract_source_name(link),
                        "origin": "naver",
                    }
                )

        except Exception as e:
            print(f"[WARN][NAVER] query={query} page={page + 1}: {e}")

        time.sleep(0.2)

    return results


def search_newsapi(query: str, start_dt: datetime, end_dt: datetime) -> list[dict]:
    if not NEWSAPI_KEY:
        return []

    params = {
        "q": query,
        "from": start_dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "to": end_dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "sortBy": "publishedAt",
        "pageSize": 50,
        "language": "ko",
        "apiKey": NEWSAPI_KEY,
    }

    try:
        response = requests.get(NEWSAPI_URL, params=params, timeout=20)
        response.raise_for_status()
        items = response.json().get("articles", [])

        results = []
        for item in items:
            title = clean_text(item.get("title", ""))
            summary = clean_text(item.get("description", "") or item.get("content", ""))
            link = clean_text(item.get("url", ""))
            pub_date = parse_iso_datetime(item.get("publishedAt", ""))

            if not title or not link:
                continue

            results.append(
                {
                    "title": title,
                    "summary": summary,
                    "link": link,
                    "published_at": pub_date,
                    "source": clean_text((item.get("source") or {}).get("name", "")) or extract_source_name(link),
                    "origin": "newsapi",
                }
            )
        return results

    except Exception as e:
        print(f"[WARN][NEWSAPI] query={query}: {e}")
        return []


def is_relevant_by_rule(config: dict, title: str, summary: str, source_name: str) -> bool:
    full_text = f"{title} {summary} {source_name}".lower()

    query_tokens = config.get("query_tokens", [])
    if query_tokens and not contains_any(full_text, query_tokens):
        return False

    if config["must_all"] and not contains_all(full_text, config["must_all"]):
        return False

    if config["must_any"] and not contains_any(full_text, config["must_any"]):
        return False

    if config["block"] and contains_block(full_text, config["block"]):
        return False

    return True


def fetch_news_for_config(config: dict, start_dt: datetime, end_dt: datetime) -> list[dict]:
    naver_items = search_naver_news(
        query=config["naver_query"],
        display=NAVER_DISPLAY,
        pages=NAVER_PAGES,
    )

    newsapi_items = search_newsapi(
        query=config["query"],
        start_dt=start_dt,
        end_dt=end_dt,
    )

    raw_items = naver_items + newsapi_items
    results = []

    for item in raw_items:
        title = clean_text(item.get("title", ""))
        summary = clean_text(item.get("summary", ""))
        link = clean_text(item.get("link", ""))
        pub_date = item.get("published_at")
        source_name = clean_text(item.get("source", "")) or extract_source_name(link)

        if not title or not link or not pub_date:
            continue

        if not (start_dt <= pub_date <= end_dt):
            continue

        title = strip_source_from_title(title, source_name)

        if not is_relevant_by_rule(config, title, summary, source_name):
            continue

        results.append(
            {
                "org_name": config["org_name"],
                "type": config["type"],
                "title": title,
                "summary": summary,
                "link": link,
                "source": source_name,
                "published_at": pub_date,
                "query": config["query"],
                "naver_query": config["naver_query"],
                "seq": config["seq"],
                "origin": item.get("origin", ""),
            }
        )

    return results


def deduplicate_items(items: list[dict]) -> list[dict]:
    seen_links = set()
    seen_titles = set()
    deduped = []

    for item in items:
        org_name = item["org_name"]
        link_key = (org_name, item["link"].strip())
        title_key = (org_name, normalize_title_for_dedup(item["title"]))

        if link_key in seen_links:
            continue
        if title_key in seen_titles:
            continue

        seen_links.add(link_key)
        seen_titles.add(title_key)
        deduped.append(item)

    return deduped


def apply_per_org_limit(grouped: dict[str, list[dict]]) -> dict[str, list[dict]]:
    limited = {}

    for org_name, org_items in grouped.items():
        if org_name in UNCAPPED_ORGS:
            limited[org_name] = org_items
        else:
            limited[org_name] = org_items[:MAX_ARTICLES_PER_ORG]

    return limited


def build_message_for_org(org_name: str, org_items: list[dict]) -> str:
    first = org_items[0]
    type_text = f" ({first['type']})" if first["type"] else ""
    emoji = get_org_emoji(first["type"])

    lines = [
        f"{emoji} {org_name}{type_text}",
        "",
    ]

    count = 0

    for item in org_items:
        meta_parts = []

        if item["source"]:
            meta_parts.append(item["source"])

        if item["published_at"]:
            meta_parts.append(item["published_at"].strftime("%m-%d %H:%M"))

        meta_text = " / ".join(meta_parts)

        lines.append(f"- {item['title']}")

        if meta_text:
            lines.append(f"  ({meta_text})")

        lines.append(f"  {item['link']}")
        lines.append("")

        count += 1

    lines.append(f"총 {count}건")
    return "\n".join(lines)


def send_to_kakaowork(text: str) -> None:
    if not KAKAOWORK_WEBHOOK_URL:
        raise ValueError("KAKAOWORK_WEBHOOK_URL 환경변수가 비어 있습니다.")

    payload = {"text": text}

    response = requests.post(
        KAKAOWORK_WEBHOOK_URL,
        json=payload,
        timeout=20,
    )
    response.raise_for_status()


def main():
    required_envs = {
        "KAKAOWORK_WEBHOOK_URL": KAKAOWORK_WEBHOOK_URL,
        "SHEET_ID": SHEET_ID,
        "NAVER_CLIENT_ID": NAVER_CLIENT_ID,
        "NAVER_CLIENT_SECRET": NAVER_CLIENT_SECRET,
    }
    missing = [k for k, v in required_envs.items() if not v]
    if missing:
        raise ValueError(f"필수 환경변수가 비어 있습니다: {', '.join(missing)}")

    now_kst = datetime.now(KST)
    weekday = now_kst.weekday()

    if weekday >= 5:
        print("주말이므로 발송하지 않습니다.")
        return

    start_dt, end_dt = get_delivery_window(now_kst)
    if start_dt is None or end_dt is None:
        print("발송 구간을 계산할 수 없습니다.")
        return

    df = load_sheet()

    required_columns = ["조직명", "검색어"]
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"시트에 '{col}' 컬럼이 없습니다.")

    configs = []
    for _, row in df.iterrows():
        config = row_to_config(row)
        if config["org_name"]:
            configs.append(config)

    all_items = []

    for config in configs:
        try:
            items = fetch_news_for_config(config, start_dt, end_dt)
            all_items.extend(items)
            print(
                f"[INFO] {config['org_name']} | "
                f"naver_query={config['naver_query']} | "
                f"full_query={config['query']} | "
                f"collected={len(items)}"
            )
        except Exception as e:
            print(f"[WARN] {config['org_name']}: {e}")

    all_items = deduplicate_items(all_items)
    all_items.sort(
        key=lambda x: x["published_at"] or datetime(1970, 1, 1, tzinfo=KST),
        reverse=True,
    )

    grouped = {}
    for item in all_items:
        grouped.setdefault(item["org_name"], []).append(item)

    grouped = apply_per_org_limit(grouped)

    flattened = []
    for _, items in grouped.items():
        flattened.extend(items)

    flattened.sort(
        key=lambda x: x["published_at"] or datetime(1970, 1, 1, tzinfo=KST),
        reverse=True,
    )

    flattened = flattened[:MAX_TOTAL_ARTICLES]

    final_grouped = {}
    for item in flattened:
        final_grouped.setdefault(item["org_name"], []).append(item)

    if not final_grouped:
        print("조건에 맞는 뉴스가 없습니다.")
        return

    for org_name, org_items in final_grouped.items():
        message = build_message_for_org(org_name, org_items)
        send_to_kakaowork(message)
        print(f"{org_name} 전송 완료")


if __name__ == "__main__":
    main()
