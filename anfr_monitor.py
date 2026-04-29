#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import pandas as pd
import re
import os
from datetime import datetime, timedelta, timezone

# ==========================
# 설정
# ==========================
DATASET = "das-telephonie-mobile"
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "peter-anfr-data-daily-noti")
NTFY_URL = f"https://ntfy.sh/{NTFY_TOPIC}"

KST = timezone(timedelta(hours=9))

BRANDS = {
    "samsung": "Samsung",
    "apple": "Apple",
    "xiaomi": "Xiaomi",
    "oppo": "Oppo",
    "huawei": "Huawei",
    "hauwei": "Huawei",
}

# ==========================
# 날짜
# ==========================
today = datetime.now(KST).date()
week_ago = today - timedelta(days=7)

print("오늘:", today)
print("최근 1주일 시작일:", week_ago)

# ==========================
# 데이터 가져오기
# ==========================
def fetch_data():
    urls = [
        f"https://data.anfr.fr/api/explore/v2.1/catalog/datasets/{DATASET}/records?limit=100&offset=0",
        f"https://data.anfr.fr/d4c/api/records/1.0/search/?dataset={DATASET}&rows=100"
    ]

    for url in urls:
        try:
            r = requests.get(url, timeout=30)
            print("시도:", url, "status:", r.status_code)

            if r.status_code != 200:
                continue

            data = r.json()

            if "results" in data:
                return pd.DataFrame(data["results"])
            if "records" in data:
                return pd.DataFrame([rec["fields"] for rec in data["records"]])

        except Exception as e:
            print("실패:", e)

    raise Exception("데이터 가져오기 실패")

df = fetch_data()

print("총 데이터:", len(df))
print(df.head())

# ==========================
# 컬럼 탐색
# ==========================
def find_col(df, keywords):
    for col in df.columns:
        for k in keywords:
            if k.lower() in col.lower():
                return col
    return None

brand_col = find_col(df, ["marque", "brand"])
model_col = find_col(df, ["modele", "model", "nom"])
date_col = find_col(df, ["date"])
status_col = find_col(df, ["conform"])

print("컬럼:", brand_col, model_col, date_col, status_col)

# ==========================
# 데이터 가공
# ==========================
def detect_brand(row):
    text = " ".join([str(x).lower() for x in row.values])
    for k, v in BRANDS.items():
        if k in text:
            return v
    return None

def detect_status(row):
    text = " ".join([str(x).lower() for x in row.values])
    if "non" in text:
        return "부적합"
    if "conforme" in text:
        return "적합"
    return "확인필요"

df["brand"] = df.apply(detect_brand, axis=1)
df["status"] = df.apply(detect_status, axis=1)

if date_col:
    df["date"] = pd.to_datetime(df[date_col], errors="coerce").dt.date
else:
    df["date"] = None

df["model"] = df[model_col] if model_col else "unknown"

# ==========================
# 필터
# ==========================
comp_df = df[df["brand"].notna()]

recent_df = comp_df[
    comp_df["date"].apply(lambda x: x is not None and week_ago <= x <= today)
] if date_col else comp_df

recent_df["is_today"] = recent_df["date"] == today

# ==========================
# 요약
# ==========================
summary = recent_df.groupby(["brand", "status"]).size().reset_index(name="count")

pivot = summary.pivot(index="brand", columns="status", values="count").fillna(0)

print(pivot)

# ==========================
# HTML 생성
# ==========================
rows = ""
for _, r in recent_df.iterrows():
    color = "#fff2a8" if r["is_today"] else ""
    rows += f"""
    <tr style="background:{color}">
        <td>{r.get("date")}</td>
        <td>{r.get("brand")}</td>
        <td>{r.get("model")}</td>
        <td>{r.get("status")}</td>
    </tr>
    """

html = f"""
<html>
<body>
<h2>ANFR 리포트</h2>

<p>
기간: {week_ago} ~ {today}<br>
총 건수: {len(recent_df)}
</p>

<table border="1">
<tr>
<th>Date</th><th>Brand</th><th>Model</th><th>Status</th>
</tr>
{rows}
</table>

</body>
</html>
"""

with open("report.html", "w") as f:
    f.write(html)

# ==========================
# ntfy 알림
# ==========================
msg = f"""
[ANFR] {today}

최근 1주일: {len(recent_df)}건
오늘 변경: {recent_df['is_today'].sum()}건
"""

requests.post(NTFY_URL, data=msg.encode("utf-8"))

print("완료")
