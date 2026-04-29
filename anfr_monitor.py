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

NTFY_TOPIC = "peter-anfr-data-daily-noti"
NTFY_URL = f"https://ntfy.sh/{NTFY_TOPIC}"

KST = timezone(timedelta(hours=9))
today = datetime.now(KST).date()
now_text = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST")

TARGET_BRANDS = ["Samsung", "Apple", "Xiaomi", "Oppo", "Huawei"]

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
            raise Exception(f"ANFR API 실패: {r.status_code}")

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
# 공통 함수
# ==========================
def find_col(df, keywords):
    for col in df.columns:
        for k in keywords:
            if k.lower() in col.lower():
                return col
    return None


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


def signed_num(n):
    if n > 0:
        return f"+{n}"
    return str(n)


# ==========================
# 요약 계산
# ==========================
def calc_counts(df):
    total = len(df)
    ok = len(df[df["status"] == "적합"])
    nok = len(df[df["status"] == "부적합"])
    unknown = len(df[df["status"] == "확인필요"])
    return {
        "total": total,
        "ok": ok,
        "nok": nok,
        "unknown": unknown,
    }


def build_summary(current_df, old_df=None):
    rows = []

    groups = [("전체", current_df)]

    for brand in TARGET_BRANDS:
        groups.append((brand, current_df[current_df["brand"] == brand]))

    for name, sub_df in groups:
        cur = calc_counts(sub_df)

        if old_df is not None and not old_df.empty:
            if name == "전체":
                old_sub = old_df
            else:
                old_sub = old_df[old_df["brand"] == name]

            old = calc_counts(old_sub)
        else:
            old = {"total": 0, "ok": 0, "nok": 0, "unknown": 0}

        rows.append({
            "name": name,
            "total": cur["total"],
            "ok": cur["ok"],
            "nok": cur["nok"],
            "unknown": cur["unknown"],
            "delta_total": cur["total"] - old["total"],
            "delta_ok": cur["ok"] - old["ok"],
            "delta_nok": cur["nok"] - old["nok"],
            "delta_unknown": cur["unknown"] - old["unknown"],
        })

    return rows


def make_summary_html(summary_rows):
    html_rows = ""

    for r in summary_rows:
        hot = " class='hot'" if r["delta_total"] != 0 or r["delta_nok"] != 0 else ""

        html_rows += f"""
        <tr{hot}>
            <td><b>{html.escape(str(r["name"]))}</b></td>
            <td>{r["total"]} <span class="delta">({signed_num(r["delta_total"])})</span></td>
            <td>{r["ok"]} <span class="delta">({signed_num(r["delta_ok"])})</span></td>
            <td>{r["nok"]} <span class="delta danger">({signed_num(r["delta_nok"])})</span></td>
            <td>{r["unknown"]} <span class="delta">({signed_num(r["delta_unknown"])})</span></td>
        </tr>
        """

    return html_rows


# ==========================
# 업데이트 테이블
# ==========================
def make_update_rows(df):
    if df.empty:
        return """
        <tr>
            <td colspan="5" style="text-align:center;">신규/변경 업데이트가 없습니다.</td>
        </tr>
        """

    rows = ""
    for _, r in df.iterrows():
        rows += f"""
        <tr class="updated">
            <td>🔥</td>
            <td>{html.escape(str(r.get("date", "")))}</td>
            <td>{html.escape(str(r.get("brand", "")))}</td>
            <td>{html.escape(str(r.get("model", "")))}</td>
            <td>{html.escape(str(r.get("status", "")))}</td>
        </tr>
        """
    return rows


# ==========================
# HTML 리포트 저장
# ==========================
def save_report(current_df, old_df, updated_df, first_run):
    summary_rows = build_summary(current_df, old_df)
    summary_html = make_summary_html(summary_rows)

    if first_run:
        title = "ANFR 기준 데이터 생성"
        message = "첫 실행이므로 기존 비교 데이터가 없습니다. 오늘 조회한 데이터를 기준 데이터로 저장했습니다."
    elif updated_df.empty:
        title = "ANFR 업데이트 없음"
        message = "기존 저장 데이터와 비교한 결과 신규/변경 항목이 없습니다."
    else:
        title = "🔥 ANFR 업데이트 있음"
        message = f"신규/변경 항목 {len(updated_df)}건이 확인되었습니다."

    report = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="utf-8">
    <title>ANFR Daily Report</title>
    <style>
        body {{
            font-family: Arial, "Malgun Gothic", sans-serif;
            margin: 24px;
            line-height: 1.5;
            color: #222;
        }}
        h2 {{
            margin-bottom: 12px;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            margin-top: 12px;
            margin-bottom: 28px;
        }}
        th, td {{
            border: 1px solid #ccc;
            padding: 8px;
            font-size: 14px;
            text-align: center;
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
            font-weight: bold;
        }}
        .hot {{
            background: #fff7d6;
            font-weight: bold;
        }}
        .delta {{
            color: #555;
            font-size: 12px;
        }}
        .danger {{
            color: #d00000;
            font-weight: bold;
        }}
        .note {{
            color: #666;
            font-size: 13px;
        }}
    </style>
</head>
<body>
    <h2>{title}</h2>

    <div class="box">
        <p><b>실행 시각:</b> {now_text}</p>
        <p><b>현재 전체 데이터:</b> {len(current_df)}건</p>
        <p><b>신규/변경 건수:</b> {len(updated_df)}건</p>
        <p>{message}</p>
        <p class="note">괄호 안 숫자는 previous_data.csv 기준 어제 대비 증감입니다.</p>
    </div>

    <h3>📈 요약 현황</h3>
    <table>
        <tr>
            <th>구분</th>
            <th>전체 건수</th>
            <th>적합</th>
            <th>부적합</th>
            <th>확인필요</th>
        </tr>
        {summary_html}
    </table>

    <h3>🔥 신규/변경 업데이트 항목</h3>
    <table>
        <tr>
            <th>표시</th>
            <th>Date</th>
            <th>Brand</th>
            <th>Model</th>
            <th>Status</th>
        </tr>
        {make_update_rows(updated_df)}
    </table>
</body>
</html>
"""

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(report)


# ==========================
# ntfy 알림
# ==========================
def send_ntfy(current_df, old_df, updated_df, first_run):
    try:
        print("===== NTFY DEBUG START =====")

        summary_rows = build_summary(current_df, old_df)

        summary_text = ""
        for r in summary_rows:
            summary_text += (
                f"{r['name']}: 총 {r['total']}({signed_num(r['delta_total'])}), "
                f"적합 {r['ok']}({signed_num(r['delta_ok'])}), "
                f"부적합 {r['nok']}({signed_num(r['delta_nok'])})\n"
            )

        print("summary_text OK")

        if first_run:
            msg = f"[ANFR] 첫 실행\n\n{summary_text}"
        elif updated_df.empty:
            msg = f"[ANFR] 업데이트 없음\n\n{summary_text}"
        else:
            msg = f"[ANFR] 업데이트 있음 ({len(updated_df)}건)\n\n{summary_text}"

        print("message 생성 OK")

        r = requests.post(
            NTFY_URL,
            data=msg.encode("utf-8"),
            headers={
                "Title": "ANFR Monitor",
                "Content-Type": "text/plain; charset=utf-8"
            },
            timeout=30,
        )

        print("ntfy status:", r.status_code)
        print("ntfy response:", r.text)
        print("===== NTFY DEBUG END =====")

    except Exception as e:
        print("🔥🔥🔥 NTFY 전체 실패:", str(e))
        raise   # ← 이거 중요 (실패하면 workflow도 실패하게)


# ==========================
# 메인
# ==========================
def main():
    print("오늘:", today)

    df = fetch_data()

    print("원본 데이터:", len(df))
    print("원본 컬럼:", list(df.columns))

    model_col = find_col(df, ["modele", "model", "nom"])
    date_col = find_col(df, ["date"])

    print("탐지 컬럼:", model_col, date_col)

    df["brand"] = df.apply(detect_brand, axis=1)
    df["brand"] = df["brand"].fillna("Unknown")

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

    current_df = df[["raw_key", "date", "brand", "model", "status"]].copy()
    current_df = current_df.drop_duplicates()

    print("현재 데이터:", len(current_df))

    first_run = not os.path.exists(PREVIOUS_FILE)

    if first_run:
        print("previous_data.csv 없음. 첫 실행으로 판단합니다.")
        old_df = pd.DataFrame(columns=current_df.columns)
        updated_df = pd.DataFrame(columns=current_df.columns)
    else:
        old_df = pd.read_csv(PREVIOUS_FILE, dtype=str)

        if "raw_key" not in old_df.columns:
            print("기존 previous_data.csv에 raw_key 없음. 기준 데이터 재생성.")
            old_df = pd.DataFrame(columns=current_df.columns)
            updated_df = pd.DataFrame(columns=current_df.columns)
            first_run = True
        else:
            old_keys = set(old_df["raw_key"].astype(str))
            updated_df = current_df[
                ~current_df["raw_key"].astype(str).isin(old_keys)
            ].copy()

    print("업데이트 건수:", len(updated_df))

    save_report(current_df, old_df, updated_df, first_run)
    send_ntfy(current_df, old_df, updated_df, first_run)

    current_df.to_csv(PREVIOUS_FILE, index=False, encoding="utf-8-sig")

    print("완료")


if __name__ == "__main__":
    main()
