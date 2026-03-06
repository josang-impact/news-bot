import os
import re
import html
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

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
MAX_ARTICLES_PER_ORG = int(os.getenv("MAX_ARTICLES_PER_ORG", "2"))
MAX_TOTAL_ARTICLES = int(os.getenv("MAX_TOTAL_ARTICLES", "20"))

NAVER_NEWS_API_URL = "https://openapi.naver.com/v1/search/news.json"


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


def normalize_query(raw_query: str, org_name: str) -> str:
    q = str(raw_query or "").strip()
    if not q:
        return f'"{org_name}"'
    return q


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


def row_to_config(row: pd.Series) -> dict:
    return {
        "org_name": clean_text(row.get("조직명", "")),
        "query": normalize_query(row.get("검색어", ""), row.get("조직명", "")),
        "type": clean_text(row.get("유형", "")),
        "must_all": split_keywords(row.get("MUST_ALL", "")),
        "must_any": split_keywords(row.get("MUST_ANY", "")),
        "block": split_keywords(row.get("BLOCK", "")),
        "seq": clean_text(row.get("연번", "")),
    }


def search_naver_news(query: str, display: int = 10, start: int = 1, sort: str = "date") -> dict:
    if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
        raise ValueError("NAVER_CLIENT_ID 또는 NAVER_CLIENT_SECRET 환경변수가 비어 있습니다.")

    headers = {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    }
    params = {
        "query": query,
        "display": display,
        "start": start,
        "sort": sort,
    }

    response = requests.get(
        NAVER_NEWS_API_URL,
        headers=headers,
        params=params,
        timeout=20,
    )
    response.raise_for_status()
    return response.json()


def fetch_news_for_config(config: dict) -> list[dict]:
    # 조직별 최대 기사 수보다 좀 넉넉하게 받아서 필터링
    raw = search_naver_news(
        query=config["query"],
        display=min(20, max(10, MAX_ARTICLES_PER_ORG * 5)),
        start=1,
        sort="date",
    )

    cutoff = datetime.now(KST) - timedelta(days=LOOKBACK_DAYS)
    results = []

    for item in raw.get("items", []):
        title = clean_text(item.get("title", ""))
        summary = clean_text(item.get("description", ""))
        source = clean_text(item.get("originallink", ""))  # source 아님, 일단 비워두고 아래에서 재지정
        pub_date = parse_naver_pubdate(item.get("pubDate", ""))

        if pub_date and pub_date < cutoff:
            continue

        originallink = clean_text(item.get("originallink", ""))
        link = originallink or clean_text(item.get("link", ""))

        if not title or not link:
            continue

        # title에 매체명이 붙는 경우가 있어 description 기반 필터 전에 정리
        # 네이버 API 자체에 source 필드가 없어서 link 도메인으로 매체명 비슷하게 표시
        source_name = extract_source_name(link)
        title = strip_source_from_title(title, source_name)

        full_text = f"{title} {summary} {source_name}"

        if config["must_all"] and not contains_all(full_text, config["must_all"]):
            continue

        if config["must_any"] and not contains_any(full_text, config["must_any"]):
            continue

        if config["block"] and contains_block(full_text, config["block"]):
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
                "seq": config["seq"],
            }
        )

        if len(results) >= MAX_ARTICLES_PER_ORG:
            break

    return results


def extract_source_name(url: str) -> str:
    if not url:
        return ""
    m = re.search(r"https?://(?:www\.)?([^/]+)", url)
    if not m:
        return ""
    domain = m.group(1).lower()
    domain = domain.replace("www.", "")
    return domain


def deduplicate_items(items: list[dict]) -> list[dict]:
    seen = set()
    deduped = []

    for item in items:
        key = (item["org_name"], item["link"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    return deduped


def build_message(items: list[dict]) -> str:
    today = datetime.now(KST).strftime("%Y-%m-%d")
    lines = [
        "[일일 조직 뉴스 알림]",
        f"기준일: {today}",
        "",
    ]

    if not items:
        lines.append("조건에 맞는 뉴스가 없습니다.")
        return "\n".join(lines)

    grouped = {}
    for item in items[:MAX_TOTAL_ARTICLES]:
        grouped.setdefault(item["org_name"], []).append(item)

    count = 0

    for org_name, org_items in grouped.items():
        first = org_items[0]
        type_text = f" ({first['type']})" if first["type"] else ""
        lines.append(f"■ {org_name}{type_text}")

        for item in org_items:
            meta_parts = []
            if item["source"]:
                meta_parts.append(item["source"])
            if item["published_at"]:
                meta_parts.append(item["published_at"].strftime("%m-%d %H:%M"))
            meta_text = " / ".join(meta_parts)

            if meta_text:
                lines.append(f"- {item['title']} ({meta_text})")
            else:
                lines.append(f"- {item['title']}")

            lines.append(f"  {item['link']}")
            count += 1

        lines.append("")

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
            items = fetch_news_for_config(config)
            all_items.extend(items)
        except Exception as e:
            print(f"[WARN] {config['org_name']}: {e}")

    all_items = deduplicate_items(all_items)
    all_items.sort(
        key=lambda x: x["published_at"] or datetime(1970, 1, 1, tzinfo=KST),
        reverse=True,
    )

    message = build_message(all_items)
    send_to_kakaowork(message)
    print("카카오워크 전송 완료")


if __name__ == "__main__":
    main()
