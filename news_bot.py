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

MAX_ARTICLES_PER_ORG = int(os.getenv("MAX_ARTICLES_PER_ORG", "3"))
NAVER_DISPLAY = int(os.getenv("NAVER_DISPLAY", "20"))
NAVER_PAGES = int(os.getenv("NAVER_PAGES", "5"))


# Broad keyword 목록
BROAD_KEYWORDS = {
    "카카오",
    "삼성",
    "네이버",
    "현대",
    "LG",
}


def is_broad_keyword(keyword: str) -> bool:
    keyword = keyword.strip()
    if keyword in BROAD_KEYWORDS:
        return True
    if len(keyword) <= 3:
        return True
    return False


def keyword_score(keyword: str, title: str, summary: str) -> int:
    score = 0

    if keyword.lower() in title.lower():
        score += 3

    if keyword.lower() in summary.lower():
        score += 1

    return score


def relevance_pass(keyword: str, title: str, summary: str) -> bool:
    score = keyword_score(keyword, title, summary)

    if is_broad_keyword(keyword):
        return score >= 4
    else:
        return score >= 3


def clean_text(text):
    text = html.unescape(str(text or ""))
    text = re.sub(r"<[^>]+>", "", text)
    return text.strip()


def parse_pubdate(value):
    if not value:
        return None

    try:
        dt = parsedate_to_datetime(value)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(KST)

    except:
        return None


def load_sheet():

    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={SHEET_GID}"
    return pd.read_csv(url)


def search_naver(query):

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

        r = requests.get(
            "https://openapi.naver.com/v1/search/news.json",
            headers=headers,
            params=params,
            timeout=20,
        )

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


def get_delivery_window():

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


def build_message(org, items):

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

    requests.post(
        KAKAOWORK_WEBHOOK_URL,
        json={"text": text},
        timeout=20,
    )


def main():

    start_dt, end_dt = get_delivery_window()

    if not start_dt:
        return

    df = load_sheet()

    for _, row in df.iterrows():

        org = str(row.get("조직명", "")).strip()
        query = str(row.get("검색어", "")).strip()

        if not org:
            continue

        naver_query = query.split("or")[0].strip().replace('"', "")

        news = search_naver(naver_query)

        filtered = []

        for item in news:

            title = item["title"]
            summary = item["summary"]

            pub = item["published_at"]

            if not pub:
                continue

            if not (start_dt <= pub <= end_dt):
                continue

            if not relevance_pass(naver_query, title, summary):
                continue

            filtered.append(item)

        filtered = filtered[:MAX_ARTICLES_PER_ORG]

        if not filtered:
            continue

        msg = build_message(org, filtered)

        send_kakaowork(msg)

        print(f"{org} 전송 완료")


if __name__ == "__main__":
    main()
