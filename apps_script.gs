// ============================================================
// newsbot-feedback: Apps Script 웹훅
// ------------------------------------------------------------
// 배포 방법 (최초 1회):
//   1) 뉴스봇 구글 시트 열기 → 확장 프로그램 → Apps Script
//   2) 기본 코드 전부 지우고 이 파일 내용 붙여넣기
//   3) 아래 SHARED_TOKEN 을 충분히 긴 랜덤 문자열로 교체
//      (같은 값을 GitHub Secret `SHEET_WEBHOOK_TOKEN` 에도 저장)
//   4) 저장 → 배포 → 새 배포 → 유형 "웹 앱" 선택
//      - 실행 계정: 나
//      - 액세스 권한: 모든 사용자 (익명)
//   5) "액세스 승인" 프롬프트 → 본인 계정으로 승인
//   6) 배포 후 표시되는 웹 앱 URL 을 GitHub Secret `SHEET_WEBHOOK_URL` 에 저장
//
// 스크립트 수정 후 재배포할 때: 배포 → 배포 관리 → 편집(연필) →
//   "버전" 드롭다운에서 "새 버전" 선택 → 배포. URL 은 유지됩니다.
// ============================================================

// !!! 교체 필수 !!! 아래 문자열을 긴 랜덤 값으로 바꾸세요 (예: Python `secrets.token_urlsafe(32)`)
const SHARED_TOKEN = "REPLACE_WITH_LONG_RANDOM_TOKEN";


function doPost(e) {
  try {
    const body = JSON.parse(e.postData.contents);
    if (body.token !== SHARED_TOKEN) {
      return jsonResponse({ ok: false, error: "unauthorized" });
    }

    const ss = SpreadsheetApp.getActive();
    const op = body.op;

    if (op === "upsert_feedback") {
      upsertByKey(ss, body.tabName, body.headers, body.records, body.keyColumn);
    } else if (op === "replace_tab") {
      replaceTab(ss, body.tabName, body.headers, body.rows);
    } else if (op === "ping") {
      // 배포 확인용
    } else {
      return jsonResponse({ ok: false, error: "unknown op: " + op });
    }

    return jsonResponse({ ok: true });
  } catch (err) {
    return jsonResponse({ ok: false, error: String(err) });
  }
}


function jsonResponse(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}


function getOrCreateTab(ss, name, headers) {
  let sh = ss.getSheetByName(name);
  if (!sh) {
    sh = ss.insertSheet(name);
    sh.getRange(1, 1, 1, headers.length).setValues([headers]);
    sh.setFrozenRows(1);
  } else if (sh.getLastRow() === 0) {
    sh.getRange(1, 1, 1, headers.length).setValues([headers]);
    sh.setFrozenRows(1);
  }
  return sh;
}


function upsertByKey(ss, tabName, headers, records, keyColumn) {
  const sh = getOrCreateTab(ss, tabName, headers);
  const lastRow = sh.getLastRow();
  const headerRow = sh.getRange(1, 1, 1, Math.max(sh.getLastColumn(), headers.length))
                      .getValues()[0];
  // 시트에 이미 있는 헤더를 사용 (사용자가 컬럼 순서를 바꿨을 수도 있으므로)
  const useHeaders = headerRow.filter(String);
  const actualHeaders = useHeaders.length ? useHeaders : headers;
  const keyIdx = actualHeaders.indexOf(keyColumn);
  if (keyIdx < 0) throw new Error("keyColumn not found in sheet: " + keyColumn);

  const rowsByKey = {};
  if (lastRow >= 2) {
    const data = sh.getRange(2, 1, lastRow - 1, actualHeaders.length).getValues();
    for (let i = 0; i < data.length; i++) {
      const key = data[i][keyIdx];
      if (key) rowsByKey[key] = i + 2;
    }
  }

  const appends = [];
  for (const rec of records) {
    const row = actualHeaders.map(function (h) {
      const v = rec[h];
      return (v === undefined || v === null) ? "" : v;
    });
    const existing = rowsByKey[rec[keyColumn]];
    if (existing) {
      sh.getRange(existing, 1, 1, actualHeaders.length).setValues([row]);
    } else {
      appends.push(row);
    }
  }
  if (appends.length > 0) {
    sh.getRange(sh.getLastRow() + 1, 1, appends.length, actualHeaders.length)
      .setValues(appends);
  }
}


function replaceTab(ss, tabName, headers, rows) {
  const sh = getOrCreateTab(ss, tabName, headers);
  sh.clear();
  const all = [headers];
  for (const r of rows) all.push(r);
  sh.getRange(1, 1, all.length, headers.length).setValues(all);
  sh.setFrozenRows(1);
}
