import os
import re
import html
from datetime import datetime, timedelta, timezone

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
        title = clean_text(entry.get("title", ""))
        summary = clean_text(entry.get("summary", ""))
        link = clean_text(entry.get("link", ""))
        published_at = parse_datetime(entry)

        if not title or not link:
            continue

        if published_at and published_at < cutoff:
            continue

        source = ""
        source_obj = entry.get("source")
        if isinstance(source_obj, dict):
            source = clean_text(source_obj.get("title", ""))

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


def build_message(items: list[dict]) -> str:
    today = datetime.now(KST).strftime("%Y-%m-%d")
    lines = [
        "[일일 조직 뉴스 알림]",
        f"기준일: {today}",
        ""
    ]

    if not items:
        lines.append("조건에 맞는 뉴스가 없습니다.")
        return "\n".join(lines)

    current_org = None
    count = 0

    for item in items[:MAX_TOTAL_ARTICLES]:
        if item["org_name"] != current_org:
            current_org = item["org_name"]
            type_text = f" ({item['type']})" if item["type"] else ""
            lines.append(f"■ {current_org}{type_text}")

        source_text = f" / {item['source']}" if item["source"] else ""
        time_text = f" / {item['published_at'].strftime('%m-%d %H:%M')}" if item["published_at"] else ""
        lines.append(f"- {item['title']}{source_text}{time_text}")
        lines.append(f"  {item['link']}")
        count += 1

    lines.append("")
    lines.append(f"총 {count}건")
    return "\n".join(lines)


def send_to_kakaowork(text: str) -> None:
    if not KAKAOWORK_WEBHOOK_URL:
        raise ValueError("KAKAOWORK_WEBHOOK_URL 환경변수가 비어 있습니다.")

    payload = {"text": text}
    response = requests.post(KAKAOWORK_WEBHOOK_URL, json=payload, timeout=20)
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

    all_items.sort(
        key=lambda x: x["published_at"] or datetime(1970, 1, 1, tzinfo=KST),
        reverse=True,
    )

    message = build_message(all_items)
    send_to_kakaowork(message)
    print("카카오워크 전송 완료")


if __name__ == "__main__":
    main()
