#!/bin/bash
# 뉴스봇 아침 실행 — launchd(com.sophie.newsbot-trigger)가 평일 08:00에 호출.
# GitHub 예약 실행(cron)은 몇 시간씩 밀리므로, 맥이 직접 "지금 실행"을 요청한다.
# 오늘 이미 보냈는지는 워크플로 첫 단계가 확인하므로 여기선 요청만 한다.
# DRY_RUN=1 이면 요청 없이 인증·접근만 확인한다.

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
REPO="josang-impact/news-bot"
LOG="/Users/sophie.impact/Documents/_Workspace/92_인프라도구/newsbot/logs/trigger.log"
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOG"; }

dow=$(date +%u)  # 1=월 … 7=일
if [ "$dow" -ge 6 ]; then log "SKIP 주말"; exit 0; fi
if [ "$(date +%-H)" -lt 8 ]; then log "SKIP 08시 이전(기사 수집 범위가 07:59까지)"; exit 0; fi

# 맥이 막 깨어났으면 네트워크가 붙을 때까지 최대 5분 기다린다
for _ in $(seq 30); do curl -s -o /dev/null -m 5 https://api.github.com && break; sleep 10; done

if [ "${DRY_RUN:-0}" = "1" ]; then
  gh api "repos/$REPO" --jq .full_name >> "$LOG" 2>&1 && log "DRY_RUN OK" || log "DRY_RUN FAIL"
  exit 0
fi

for attempt in 1 2 3; do
  if gh workflow run daily_news.yml -R "$REPO" >> "$LOG" 2>&1; then
    log "TRIGGERED (시도 $attempt)"; exit 0
  fi
  sleep 60
done
log "FAIL — 3회 실패. GitHub 예약 실행이 대신 보낸다(늦을 수 있음)"
/usr/bin/osascript -e 'display notification "뉴스봇 실행 요청 실패. 예약 실행으로 늦게 올 수 있어요." with title "늬우스배달부"' 2>/dev/null
exit 1
