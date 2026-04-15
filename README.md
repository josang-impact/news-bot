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
