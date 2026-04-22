"""
Slack 채널의 뉴스봇 메시지에서 ❌ 반응이 달린 기사를 수집해
Apps Script 웹훅을 거쳐 구글 시트 `피드백` / `BLOCK후보` 탭에 기록하는 스크립트.

실행 흐름:
  1) Slack conversations.history로 최근 N일치 메시지 조회
  2) 뉴스봇 포맷(`*<link|[조직] 제목>*`)에 맞는 메시지만 필터
  3) ❌ 계열 반응 개수 집계 → Apps Script 웹훅에 upsert 요청
  4) ❌가 달린 기사들에서 조직별 빈출 단어 추출 → `BLOCK후보` 탭 전체 갱신 요청
"""

import logging
import os
import re
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

import requests


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


KST = timezone(timedelta(hours=9))

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "").strip()
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID", "").strip()

SHEET_WEBHOOK_URL = os.getenv("SHEET_WEBHOOK_URL", "").strip()
SHEET_WEBHOOK_TOKEN = os.getenv("SHEET_WEBHOOK_TOKEN", "").strip()

LOOKBACK_DAYS = int(os.getenv("FEEDBACK_LOOKBACK_DAYS") or 3)
BLOCK_MIN_COUNT = int(os.getenv("BLOCK_MIN_COUNT") or 2)
BLOCK_TOP_N = int(os.getenv("BLOCK_TOP_N") or 15)

# ❌로 인정할 Slack 이모지 이름들
NEGATIVE_REACTIONS = {"x", "no_entry", "no_entry_sign", "thumbsdown", "-1"}

FEEDBACK_TAB = "피드백"
BLOCK_CANDIDATE_TAB = "BLOCK후보"

FEEDBACK_HEADERS = [
    "날짜", "조직", "제목", "요약", "링크", "출처", "❌개수", "메시지TS", "최근업데이트",
]
BLOCK_CANDIDATE_HEADERS = [
    "조직", "후보키워드", "빈도", "샘플제목", "최근업데이트",
]

# 뉴스봇이 발송한 메시지 블록 포맷: *<LINK|[ORG] TITLE>* (MM-DD HH:MM) (SOURCE)
BOT_MSG_PATTERN = re.compile(r"\*<([^|>]+)\|\[([^\]]+)\]\s*([^>]+)>\*(.*)")

STOPWORDS = {
    "이", "그", "저", "것", "등", "및", "도", "는", "은", "을", "를", "에", "의",
    "가", "와", "과", "로", "으로", "에서", "부터", "까지", "에게", "한테", "보다",
    "처럼", "같이", "또는", "혹은", "그리고", "하지만", "그러나", "뿐", "만",
    "만큼", "정도", "위해", "통해", "있다", "없다", "되다", "하다", "했다", "한다",
    "이다", "aka", "the", "and", "for", "with", "from", "com", "kr",
    "co", "기자", "뉴스", "기사", "오전", "오후", "오늘", "어제", "내일",
    "https", "http", "www",
}


# ──────────────────────────────────────────────
# Slack 메시지 조회
# ──────────────────────────────────────────────
def fetch_messages():
    if not SLACK_BOT_TOKEN or not SLACK_CHANNEL_ID:
        raise RuntimeError("SLACK_BOT_TOKEN / SLACK_CHANNEL_ID가 설정되지 않았습니다.")

    oldest_ts = (datetime.now(KST) - timedelta(days=LOOKBACK_DAYS)).timestamp()
    url = "https://slack.com/api/conversations.history"
    headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}

    messages = []
    cursor = None
    while True:
        params = {
            "channel": SLACK_CHANNEL_ID,
            "oldest": f"{oldest_ts:.6f}",
            "limit": 200,
        }
        if cursor:
            params["cursor"] = cursor

        r = requests.get(url, headers=headers, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        if not data.get("ok"):
            raise RuntimeError(f"Slack API error: {data.get('error')}")

        messages.extend(data.get("messages", []))
        cursor = data.get("response_metadata", {}).get("next_cursor") or None
        if not cursor:
            break
        time.sleep(0.5)

    logger.info(f"Slack 메시지 {len(messages)}건 수집 (최근 {LOOKBACK_DAYS}일)")
    return messages


# ──────────────────────────────────────────────
# 메시지 파싱
# ──────────────────────────────────────────────
def extract_block_text(msg):
    blocks = msg.get("blocks") or []
    for b in blocks:
        if b.get("type") == "section":
            txt = (b.get("text") or {}).get("text") or ""
            if txt:
                return txt
    return msg.get("text") or ""


def parse_bot_message(msg):
    """봇 메시지 형식이면 dict 반환, 아니면 None."""
    text = extract_block_text(msg)
    if not text:
        return None

    m = BOT_MSG_PATTERN.search(text)
    if not m:
        return None

    link = m.group(1).strip()
    org = m.group(2).strip()
    title_raw = m.group(3).strip()
    trailing = m.group(4) or ""

    title = title_raw.replace("‹", "<").replace("›", ">").replace("｜", "|")

    source = ""
    meta_matches = re.findall(r"\(([^)]+)\)", trailing)
    if meta_matches:
        source = meta_matches[-1].strip()

    summary = ""
    parts = text.split("\n", 1)
    if len(parts) > 1:
        summary = parts[1].strip()

    return {
        "org": org,
        "title": title,
        "link": link,
        "summary": summary,
        "source": source,
    }


def count_negative_reactions(msg):
    total = 0
    for r in msg.get("reactions") or []:
        if r.get("name") in NEGATIVE_REACTIONS:
            total += int(r.get("count") or 0)
    return total


def msg_datetime(msg):
    ts = msg.get("ts")
    if not ts:
        return None
    try:
        return datetime.fromtimestamp(float(ts), tz=KST)
    except (TypeError, ValueError):
        return None


# ──────────────────────────────────────────────
# Apps Script 웹훅 호출
# ──────────────────────────────────────────────
def post_to_sheet(payload):
    if not SHEET_WEBHOOK_URL or not SHEET_WEBHOOK_TOKEN:
        raise RuntimeError("SHEET_WEBHOOK_URL / SHEET_WEBHOOK_TOKEN가 설정되지 않았습니다.")

    body = {"token": SHEET_WEBHOOK_TOKEN, **payload}
    r = requests.post(SHEET_WEBHOOK_URL, json=body, timeout=60)
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"웹훅 에러: {data.get('error')}")
    return data


def upsert_feedback(records):
    post_to_sheet({
        "op": "upsert_feedback",
        "tabName": FEEDBACK_TAB,
        "headers": FEEDBACK_HEADERS,
        "keyColumn": "링크",
        "records": records,
    })
    logger.info(f"피드백 탭 upsert 완료: {len(records)}건")


def write_block_candidates(candidates):
    rows = [[c.get(h, "") for h in BLOCK_CANDIDATE_HEADERS] for c in candidates]
    post_to_sheet({
        "op": "replace_tab",
        "tabName": BLOCK_CANDIDATE_TAB,
        "headers": BLOCK_CANDIDATE_HEADERS,
        "rows": rows,
    })
    logger.info(f"BLOCK후보 탭 갱신 완료: {len(candidates)}건")


# ──────────────────────────────────────────────
# BLOCK 후보 추출
# ──────────────────────────────────────────────
def extract_block_candidates(records):
    by_org = defaultdict(list)
    for rec in records:
        if rec["❌개수"] >= 1:
            text = f"{rec['제목']} {rec.get('요약', '')}"
            by_org[rec["조직"]].append({"text": text, "title": rec["제목"]})

    now_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    candidates = []

    for org, items in by_org.items():
        # 조직명·대표자명에 포함된 단어는 제외
        org_tokens = set(re.findall(r"[가-힣A-Za-z0-9]{2,}", org))

        words = []
        for it in items:
            tokens = re.findall(r"[가-힣A-Za-z0-9]{2,}", it["text"])
            for t in tokens:
                if t in STOPWORDS or t in org_tokens:
                    continue
                if t.isdigit():
                    continue
                words.append(t)

        counter = Counter(words)
        for word, count in counter.most_common(BLOCK_TOP_N):
            if count < BLOCK_MIN_COUNT:
                break
            sample = items[0]["title"][:80]
            candidates.append({
                "조직": org,
                "후보키워드": word,
                "빈도": count,
                "샘플제목": sample,
                "최근업데이트": now_str,
            })

    return candidates


# ──────────────────────────────────────────────
# 메인
# ──────────────────────────────────────────────
def main():
    messages = fetch_messages()

    now_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    records = []
    for msg in messages:
        parsed = parse_bot_message(msg)
        if not parsed:
            continue
        neg = count_negative_reactions(msg)
        dt = msg_datetime(msg)
        date_str = dt.strftime("%Y-%m-%d") if dt else ""

        records.append({
            "날짜": date_str,
            "조직": parsed["org"],
            "제목": parsed["title"],
            "요약": parsed["summary"],
            "링크": parsed["link"],
            "출처": parsed["source"],
            "❌개수": neg,
            "메시지TS": msg.get("ts", ""),
            "최근업데이트": now_str,
        })

    logger.info(f"봇 메시지 파싱: {len(records)}건")
    neg_count = sum(1 for r in records if r["❌개수"] > 0)
    logger.info(f"❌ 반응 있는 기사: {neg_count}건")

    if not records:
        logger.info("기록할 봇 메시지가 없습니다. 종료.")
        return

    upsert_feedback(records)

    candidates = extract_block_candidates(records)
    write_block_candidates(candidates)

    logger.info("완료")


if __name__ == "__main__":
    main()
