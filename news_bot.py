import os
import re
import html
from datetime import datetime, timedelta, timezone
from collections import OrderedDict

import feedparser
import pandas as pd
import requests
from dateutil import parser as date_parser


KST = timezone(timedelta(hours=9))

KAKAOWORK_WEBHOOK_URL = os.getenv("KAKAOWORK_WEBHOOK_URL", "").strip()
SHEET_ID = os.getenv("SHEET_ID", "").strip()
SHEET_GID = os.getenv("SHEET_GID", "0").strip()

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
MAX_ARTICLES_PER_ORG = int(os.getenv("MAX_ARTICLES_PER_ORG", "2"))
MAX_TOTAL_ARTICLES = int(os.getenv("MAX_TOTAL_ARTICLES", "20"))


def build_google_sheet_csv_url(sheet_id: str, gid: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"


def load_sheet() -> pd.DataFrame:
    if not SHEET_ID:
        raise ValueError("SHEET_ID 환경변수가 비어 있습니다.")
    url = build_google_sheet_csv_url(SHEET_ID, SHEET_GID)
    return pd.read_csv(url)


def clean_text(text: str) -> str:
    text = html.unescape(str(text or ""))
    text = re.sub(r"\s+", " ", text).strip()
    return text


def split_keywords(value: str) -> list[str]:
    if pd.isna(value) or str(value).strip() == "":
        return []
    return [x.strip() for x in str(value).split(",") if x.strip()]


def parse_datetime(entry) -> datetime | None:
    for key in ["published", "updated"]:
        value = entry.get(key)
        if not value:
            continue
        try:
            dt = date_parser.parse(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(KST)
        except Exception:
            continue
    return None


def make_google_news_rss_url(query: str) -> str:
    encoded = requests.utils.quote(query)
    return f"https://news.google.com/rss/search?q={encoded}&hl=ko&gl=KR&ceid=KR:ko"


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

    # 제목 안에 source가 마지막 토큰처럼 반복되는 경우도 정리
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


def fetch_news_for_config(config: dict) -> list[dict]:
    feed = feedparser.parse(make_google_news_rss_url(config["query"]))
    cutoff = datetime.now(KST) - timedelta(days=LOOKBACK_DAYS)

    results = []

    for entry in feed.entries:
        raw_title = clean_text(entry.get("title", ""))
        summary = clean_text(entry.get("summary", ""))
        link = clean_text(entry.get("link", ""))
        published_at = parse_datetime(entry)

        if not raw_title or not link:
            continue

        if published_at and published_at < cutoff:
            continue

        source = ""
        source_obj = entry.get("source")
        if isinstance(source_obj, dict):
            source = clean_text(source_obj.get("title", ""))

        title = strip_source_from_title(raw_title, source)
        full_text = f"{title} {summary} {source}"

        if config["must_all"] and not contains_all(full_text, config["must_all"]):
            continue

        if config["must_any"] and not contains_any(full_text, config["must_any"]):
            continue

        if config["block"] and contains_block(full_text, config["block"]):
            continue

        results.append({
            "org_name": config["org_name"],
            "type": config["type"],
            "title": title,
            "summary": summary,
            "link": link,
            "source": source,
            "published_at": published_at,
            "query": config["query"],
            "seq": config["seq"],
        })

        if len(results) >= MAX_ARTICLES_PER_ORG:
            break

    return results


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


def build_fallback_text(items: list[dict]) -> str:
    today = datetime.now(KST).strftime("%Y-%m-%d")
    if not items:
        return f"[일일 조직 뉴스 알림] {today} / 조건에 맞는 뉴스가 없습니다."

    parts = [f"[일일 조직 뉴스 알림] {today}"]
    for item in items[: min(len(items), 5)]:
        parts.append(f"[{item['org_name']}] {item['title']}")
    return " / ".join(parts)


def make_header_block(text: str, style: str = "blue") -> dict:
    return {
        "type": "header",
        "text": text[:20],  # 카카오워크 헤더 최대 20자
        "style": style,
    }


def make_text_block(text: str, inlines: list[dict] | None = None) -> dict:
    block = {
        "type": "text",
        "text": text,
    }
    if inlines:
        block["inlines"] = inlines
    return block


def build_blocks(items: list[dict]) -> list[dict]:
    blocks = []

    today = datetime.now(KST).strftime("%Y-%m-%d")
    blocks.append(make_header_block("일일 조직 뉴스", "blue"))
    blocks.append(make_text_block(f"기준일: {today}"))

    if not items:
        blocks.append(make_text_block("조건에 맞는 뉴스가 없습니다."))
        return blocks

    grouped = OrderedDict()
    for item in items[:MAX_TOTAL_ARTICLES]:
        grouped.setdefault(item["org_name"], []).append(item)

    for org_name, org_items in grouped.items():
        first = org_items[0]
        type_text = f" ({first['type']})" if first["type"] else ""
        header_text = f"{org_name}{type_text}"
        blocks.append(make_header_block(header_text, "white"))

        for item in org_items:
            meta_parts = []
            if item["source"]:
                meta_parts.append(item["source"])
            if item["published_at"]:
                meta_parts.append(item["published_at"].strftime("%m-%d %H:%M"))
            meta_text = " / ".join(meta_parts)

            article_text = item["title"]
            inlines = [
                {
                    "type": "link",
                    "text": item["title"],
                    "url": item["link"],
                }
            ]

            if meta_text:
                article_text += f"\n{meta_text}"

            blocks.append(make_text_block(article_text, inlines))

        # 조직 간 빈 줄
        blocks.append(make_text_block(" "))

    total_count = min(len(items), MAX_TOTAL_ARTICLES)
    blocks.append(make_text_block(f"총 {total_count}건"))

    return blocks


def send_to_kakaowork(items: list[dict]) -> None:
    if not KAKAOWORK_WEBHOOK_URL:
        raise ValueError("KAKAOWORK_WEBHOOK_URL 환경변수가 비어 있습니다.")

    payload = {
        "text": build_fallback_text(items),
        "blocks": build_blocks(items),
    }

    response = requests.post(
        KAKAOWORK_WEBHOOK_URL,
        json=payload,
        timeout=20,
    )
    response.raise_for_status()


def main():
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

    send_to_kakaowork(all_items)
    print("카카오워크 전송 완료")


if __name__ == "__main__":
    main()
