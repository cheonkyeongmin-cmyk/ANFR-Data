#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import html
import hashlib
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

# ==========================
# 설정
# ==========================
DATASET = "das-telephonie-mobile"
PREVIOUS_FILE = "previous_data.csv"
REPORT_FILE = "report.html"

NTFY_TOPIC = os.getenv("NTFY_TOPIC", "peter-anfr-data-daily-noti")
NTFY_URL = f"https://ntfy.sh/{NTFY_TOPIC}"

KST = timezone(timedelta(hours=9))
today = datetime.now(KST).date()
now_text = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST")

BRANDS = {
    "samsung": "Samsung",
    "apple": "Apple",
    "xiaomi": "Xiaomi",
    "oppo": "Oppo",
    "huawei": "Huawei",
    "hauwei": "Huawei",
    "honor": "Honor",
    "google": "Google",
    "sony": "Sony",
    "motorola": "Motorola",
    "oneplus": "OnePlus",
    "realme": "Realme",
    "vivo": "Vivo",
    "nokia": "Nokia",
}

# ==========================
# 데이터 가져오기
# ==========================
def fetch_data():
    all_rows = []

    # 1차: 구버전 D4C API 사용
    rows = 100
    start = 0

    while True:
        url = (
            f"https://data.anfr.fr/d4c/api/records/1.0/search/"
            f"?dataset={DATASET}&rows={rows}&start={start}"
        )

        print("시도:", url)
        r = requests.get(url, timeout=30)
        print("status:", r.status_code)

        if r.status_code != 200:
            raise Exception(f"ANFR D4C API 실패: {r.status_code}")

        data = r.json()
        records = data.get("records", [])

        if not records:
            break

        for rec in records:
            fields = rec.get("fields", {})
            if fields:
                all_rows.append(fields)

        if len(records) < rows:
            break

        start += rows

    if not all_rows:
        raise Exception("ANFR 데이터가 비어 있습니다.")

    return pd.DataFrame(all_rows)


# ==========================
# 컬럼 탐색
# ==========================
def find_col(df, keywords):
    for col in df.columns:
        for k in keywords:
            if k.lower() in col.lower():
                return col
    return None


# ==========================
# 브랜드 / 상태 감지
# ==========================
def detect_brand(row):
    text = " ".join([str(x).lower() for x in row.values])
    for k, v in BRANDS.items():
        if k in text:
            return v
    return None


def detect_status(row):
    text = " ".join([str(x).lower() for x in row.values])

    if "non conforme" in text or "non-conforme" in text:
        return "부적합"

    if "conforme" in text:
        return "적합"

    return "확인필요"


def make_hash(row):
    text = "|".join([str(x) for x in row.values])
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ==========================
# HTML 생성
# ==========================
def make_rows(df):
    rows = ""

    if df.empty:
        return """
        <tr>
            <td colspan="5" style="text-align:center;">표시할 업데이트가 없습니다.</td>
        </tr>
        """

    for _, r in df.iterrows():
        rows += f"""
        <tr>
            <td>{html.escape(str(r.get("date", "")))}</td>
            <td>{html.escape(str(r.get("brand", "")))}</td>
            <td>{html.escape(str(r.get("model", "")))}</td>
            <td>{html.escape(str(r.get("status", "")))}</td>
            <td>{html.escape(str(r.get("raw_key", "")))[:16]}</td>
        </tr>
        """

    return rows


def save_report(current_df, updated_df, first_run):
    if first_run:
        title = "ANFR 기준 데이터 생성"
        message = "첫 실행이므로 기존 비교 데이터가 없습니다. 오늘 조회한 데이터를 기준 데이터로 저장했습니다."
    elif updated_df.empty:
        title = "ANFR 업데이트 없음"
        message = "기존 저장 데이터와 비교한 결과 신규/변경 항목이 없습니다."
    else:
        title = "ANFR 업데이트 있음"
        message = f"신규/변경 항목 {len(updated_df)}건이 확인되었습니다."

    brand_summary = (
        current_df.groupby(["brand", "status"])
        .size()
        .reset_index(name="count")
        .sort_values(["brand", "status"])
    )

    summary_rows = ""
    for _, r in brand_summary.iterrows():
        summary_rows += f"""
        <tr>
            <td>{html.escape(str(r["brand"]))}</td>
            <td>{html.escape(str(r["status"]))}</td>
            <td>{html.escape(str(r["count"]))}</td>
        </tr>
        """

    report = f"""
    <!DOCTYPE html>
    <html lang="ko">
    <head>
        <meta charset="utf-8">
        <title>ANFR Daily Report</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 24px;
            }}
            table {{
                border-collapse: collapse;
                width: 100%;
                margin-top: 12px;
            }}
            th, td {{
                border: 1px solid #ccc;
                padding: 8px;
                font-size: 14px;
            }}
            th {{
                background: #f2f2f2;
            }}
            .box {{
                padding: 12px;
                background: #f9f9f9;
                border: 1px solid #ddd;
                margin-bottom: 20px;
            }}
            .updated {{
                background: #fff2a8;
            }}
        </style>
    </head>
    <body>
        <h2>{title}</h2>

        <div class="box">
            <p><b>실행 시각:</b> {now_text}</p>
            <p><b>전체 확인 건수:</b> {len(current_df)}</p>
            <p><b>신규/변경 건수:</b> {len(updated_df)}</p>
            <p>{message}</p>
        </div>

        <h3>신규/변경 항목</h3>
        <table>
            <tr>
                <th>Date</th>
                <th>Brand</th>
                <th>Model</th>
                <th>Status</th>
                <th>Key</th>
            </tr>
            {make_rows(updated_df)}
        </table>

        <h3>현재 데이터 요약</h3>
        <table>
            <tr>
                <th>Brand</th>
                <th>Status</th>
                <th>Count</th>
            </tr>
            {summary_rows}
        </table>
    </body>
    </html>
    """

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(report)


# ==========================
# ntfy 알림
# ==========================
def send_ntfy(current_df, updated_df, first_run):
    if first_run:
        msg = f"""
[ANFR] 기준 데이터 생성

첫 실행으로 비교 대상이 없습니다.
현재 데이터: {len(current_df)}건
실행시각: {now_text}
"""
    elif updated_df.empty:
        msg = f"""
[ANFR] 업데이트 없음

현재 데이터: {len(current_df)}건
실행시각: {now_text}
"""
    else:
        top_items = ""
        for _, r in updated_df.head(10).iterrows():
            top_items += f"- {r.get('brand')} / {r.get('model')} / {r.get('status')} / {r.get('date')}\n"

        msg = f"""
[ANFR] 업데이트 있음

신규/변경: {len(updated_df)}건
현재 데이터: {len(current_df)}건

{top_items}
실행시각: {now_text}
"""

try:
    r = requests.post(
        "https://ntfy.sh/peter-anfr-data-daily-noti",
        data=msg.encode("utf-8"),
        headers={
            "Title": "ANFR Daily Monitor",
            "Content-Type": "text/plain; charset=utf-8"
        },
        timeout=30
    )
    print("ntfy status:", r.status_code)
    print("ntfy response:", r.text)
except Exception as e:
    print("ntfy 전송 실패:", e)


# ==========================
# 메인 실행
# ==========================
def main():
    print("오늘:", today)

    df = fetch_data()
    print("원본 데이터:", len(df))
    print("원본 컬럼:", list(df.columns))

    brand_col = find_col(df, ["marque", "brand"])
    model_col = find_col(df, ["modele", "model", "nom"])
    date_col = find_col(df, ["date"])
    status_col = find_col(df, ["conform"])

    print("탐지 컬럼:", brand_col, model_col, date_col, status_col)

    df["brand"] = df.apply(detect_brand, axis=1)
    df["status"] = df.apply(detect_status, axis=1)

    if date_col:
        df["date"] = pd.to_datetime(df[date_col], errors="coerce").dt.date.astype(str)
    else:
        df["date"] = ""

    if model_col:
        df["model"] = df[model_col].astype(str)
    else:
        df["model"] = "unknown"

    df["raw_key"] = df.apply(make_hash, axis=1)

    current_df = df[df["brand"].notna()].copy()
    current_df = current_df[["raw_key", "date", "brand", "model", "status"]].drop_duplicates()

    print("브랜드 필터 후:", len(current_df))

    first_run = not os.path.exists(PREVIOUS_FILE)

    if first_run:
        print("previous_data.csv 없음. 첫 실행으로 판단합니다.")
        updated_df = pd.DataFrame(columns=current_df.columns)
    else:
        old_df = pd.read_csv(PREVIOUS_FILE, dtype=str)

        if "raw_key" not in old_df.columns:
            print("기존 파일에 raw_key 없음. 전체를 신규 기준으로 재생성합니다.")
            updated_df = pd.DataFrame(columns=current_df.columns)
            first_run = True
        else:
            old_keys = set(old_df["raw_key"].astype(str))
            updated_df = current_df[~current_df["raw_key"].astype(str).isin(old_keys)].copy()

    print("업데이트 건수:", len(updated_df))

    save_report(current_df, updated_df, first_run)
    send_ntfy(current_df, updated_df, first_run)

    current_df.to_csv(PREVIOUS_FILE, index=False, encoding="utf-8-sig")

    print("완료")


if __name__ == "__main__":
    main()
