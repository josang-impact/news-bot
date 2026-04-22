# org-news-slack-bot

Google Sheet에 정의된 조직별 키워드로 네이버 뉴스 API(옵션: NewsAPI.org)를 검색해,
매 평일 아침 Slack 채널에 기사 1건당 메시지 1개씩 전송하는 봇입니다.

메시지 포맷:
- 제목: `[조직명] 기사 제목 (MM-DD HH:MM) (출처)` — 제목 전체가 기사 링크
- 본문: 기사 요약 1~2줄

## Required GitHub Secrets

- `SLACK_WEBHOOK_URL` — Slack 채널의 Incoming Webhook URL
- `SHEET_ID` — 구글 시트 ID
- `SHEET_GID` — 시트 내 탭 gid (기본 `0`)
- `NAVER_CLIENT_ID`
- `NAVER_CLIENT_SECRET`

## Optional GitHub Secrets

- `NEWSAPI_KEY` — 설정하면 NewsAPI.org 검색을 추가로 사용
- `MAX_ARTICLES_PER_ORG` — 조직당 최대 기사 수 (기본 `3`)
- `NAVER_DISPLAY` — 네이버 1페이지 결과 수 (기본 `20`, 최대 `100`)
- `NAVER_PAGES` — 네이버 페이지네이션 횟수 (기본 `5`)
- `SLACK_SEND_INTERVAL` — Slack 전송 간격(초) (기본 `1.0`)

## Google Sheet 컬럼

| 컬럼명 | 설명 |
|---|---|
| `조직명` | Slack 메시지의 `[조직명]` 머리말에 사용 |
| `검색어` | `or`로 구분된 키워드 (예: `"브라이언임팩트" or "브라이언 임팩트"`) |
| `MUST_ALL` | 쉼표 구분. 제목+요약에 **모두** 포함되어야 통과 (옵션) |
| `MUST_ANY` | 쉼표 구분. 제목+요약에 **하나 이상** 포함되어야 통과 (옵션) |
| `BLOCK` | 쉼표 구분. 하나라도 포함되면 제외 (옵션) |

## 스케줄

`.github/workflows/daily_news.yml` 기준 평일 08:00 KST (`0 23 * * 0-4` UTC)에 실행되며,
수동 실행은 GitHub Actions의 **Run workflow** 버튼으로 가능합니다.

---

# 피드백 수집 (collect_feedback.py)

Slack 채널에 올라간 뉴스봇 메시지에 ❌ 반응이 달리면, 해당 기사를 수집해
구글 시트의 `피드백` / `BLOCK후보` 탭에 누적 기록하는 보조 스크립트입니다.

- `피드백` 탭: 봇이 보낸 모든 기사의 ❌ 개수, 조직, 제목, 링크 등을 링크 기준 upsert
- `BLOCK후보` 탭: ❌가 1개 이상 달린 기사들에서 조직별 빈출 단어를 자동 추출 (검토 후 수동으로 `BLOCK` 컬럼에 옮기세요. **자동 반영 아님**)

## 스케줄

`.github/workflows/collect_feedback.yml` 기준 매일 20:00 KST (`0 11 * * *` UTC)에 실행됩니다.
최근 3일치 메시지를 재집계하므로 뒤늦게 ❌를 달아도 반영됩니다.

## 추가 GitHub Secrets

- `SLACK_BOT_TOKEN` — `xoxb-`로 시작하는 Bot User OAuth Token
- `SLACK_CHANNEL_ID` — 뉴스봇이 발송하는 채널 ID (예: `C01234ABCDE`)
- `SHEET_WEBHOOK_URL` — Apps Script 웹 앱 배포 URL
- `SHEET_WEBHOOK_TOKEN` — Apps Script 코드에 하드코딩한 공유 시크릿과 동일한 값
- `FEEDBACK_LOOKBACK_DAYS` (옵션, 기본 `3`) — 재집계할 최근 일수

## Slack App 설정 (1회)

1. https://api.slack.com/apps → **Create New App** → From scratch
2. App Name: `newsbot-feedback` (임의), 워크스페이스 선택
3. 좌측 **OAuth & Permissions** → **Bot Token Scopes** 에 아래 3개 추가:
   - `channels:history` (public 채널 메시지 읽기)
   - `groups:history` (private 채널 쓰는 경우)
   - `reactions:read`
4. 페이지 상단 **Install to Workspace** → 승인
5. 설치 후 표시되는 **Bot User OAuth Token** (`xoxb-...`) 을 복사 → GitHub Secret `SLACK_BOT_TOKEN` 에 저장
6. Slack에서 해당 채널로 가서 `/invite @newsbot-feedback` 으로 봇을 채널에 초대

## Apps Script 웹훅 배포 (1회)

조직 정책으로 서비스 계정 JSON 키 발급이 막혀 있어, 구글 시트 쓰기는 Apps Script 웹훅을 통해 처리합니다. 이 파일의 [`apps_script.gs`](apps_script.gs) 참고.

1. 뉴스봇이 쓰는 구글 시트 열기 → **확장 프로그램** → **Apps Script**
2. 기본 코드를 지우고 `apps_script.gs` 내용을 전부 붙여넣기
3. 파일 내 `SHARED_TOKEN` 을 충분히 긴 랜덤 문자열로 교체
   - 생성 예: `python -c "import secrets; print(secrets.token_urlsafe(32))"`
4. **저장** → 상단 우측 **배포** → **새 배포**
5. 유형 톱니 아이콘 → **웹 앱** 선택, 다음과 같이 설정:
   - **설명**: `newsbot-feedback` (임의)
   - **다음 사용자로 실행**: 나 (본인 계정)
   - **액세스 권한이 있는 사용자**: 모든 사용자
6. **배포** → 권한 승인 프롬프트 → 계정 선택 → "고급" → "안전하지 않은 페이지로 이동" → 허용
7. 표시된 **웹 앱 URL** (`https://script.google.com/macros/s/.../exec`) 을 GitHub Secret `SHEET_WEBHOOK_URL` 에 저장
8. 3번에서 쓴 랜덤 문자열을 GitHub Secret `SHEET_WEBHOOK_TOKEN` 에 저장 (Apps Script 쪽과 동일해야 함)

> **스크립트 수정 후 재배포:** 배포 → 배포 관리 → 연필 아이콘 → 버전 "새 버전" → 배포. 기존 URL이 그대로 유지됩니다.

## 동작 방식

1. Slack `conversations.history` API로 최근 N일치 메시지 조회
2. `*<link|[조직] 제목>*` 포맷의 봇 메시지만 필터
3. 메시지의 reactions 중 `:x:`, `:no_entry:`, `:no_entry_sign:`, `:-1:`, `:thumbsdown:` 개수 합산
4. Apps Script 웹훅으로 `피드백` 탭에 링크 기준 upsert (❌ 개수 최신화)
5. ❌가 달린 기사들에서 조직별 빈출 단어를 뽑아 `BLOCK후보` 탭 전체 갱신 (조직명·대표자명은 자동 제외)
