#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
history.json 백필 스크립트
네이버 차트 API를 이용해 3/3 이후 일별 종가로 수익 계산 후 저장
"""
import json, os, requests, warnings
from datetime import date, timedelta
from xml.etree import ElementTree as ET

warnings.filterwarnings("ignore")

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE  = os.path.join(BASE_DIR, "config.json")
HISTORY_FILE = os.path.join(BASE_DIR, "history.json")

HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://finance.naver.com/",
}

def get_history(code, count=30):
    """Naver 차트 API → {YYYY-MM-DD: 종가} 딕셔너리"""
    url = (f"https://fchart.stock.naver.com/sise.nhn"
           f"?symbol={code}&timeframe=day&count={count}&requestType=0")
    try:
        resp = requests.get(url, headers=HDR, timeout=10, verify=False)
        resp.encoding = "euc-kr"
        root = ET.fromstring(resp.text)
        prices = {}
        for item in root.iter("item"):
            raw = item.get("data", "")
            parts = raw.split("|")
            if len(parts) >= 5 and parts[0] and parts[4]:
                d = parts[0]  # YYYYMMDD
                close = int(parts[4])
                key = f"{d[:4]}-{d[4:6]}-{d[6:]}"
                prices[key] = close
        return prices
    except Exception as e:
        print(f"  [오류] {code}: {e}")
        return {}


def backfill():
    with open(CONFIG_FILE, encoding="utf-8") as f:
        config = json.load(f)
    portfolio = config.get("portfolio", [])

    # 역사 가격 조회
    print("종목별 역사 종가 조회 중...")
    stock_prices = {}
    for item in portfolio:
        code  = item["code"]
        name  = item.get("name", code)
        prices = get_history(code, count=60)
        stock_prices[code] = prices
        print(f"  {name} ({code}): {len(prices)}일치 데이터")

    # 날짜 범위: 2026-03-03 ~ 오늘 (평일만)
    start = date(2026, 3, 3)
    end   = date.today()

    records = []
    d = start
    while d <= end:
        if d.weekday() < 5:          # 월~금
            date_str   = d.strftime("%Y-%m-%d")
            total_eval = 0
            total_buy  = 0
            missing    = []

            for item in portfolio:
                code  = item["code"]
                qty   = int(item["quantity"])
                avg_p = float(item["avg_price"])
                close = stock_prices.get(code, {}).get(date_str)
                if close is None:
                    missing.append(code)
                    continue
                total_eval += close * qty
                total_buy  += avg_p * qty

            if missing:
                print(f"  {date_str}: 종가 없음 ({', '.join(missing)}) — 건너뜀")
            else:
                total_profit = total_eval - total_buy
                profit_rate  = round(total_profit / total_buy * 100, 2) if total_buy else 0.0
                records.append({
                    "date":              date_str,
                    "total_buy":         round(total_buy),
                    "total_eval":        round(total_eval),
                    "total_profit":      round(total_profit),
                    "total_profit_rate": profit_rate,
                    "daily_change":      0,   # 아래서 재계산
                })
        d += timedelta(days=1)

    # daily_change: 전일 대비 평가금액 변화
    for i in range(len(records)):
        if i == 0:
            records[i]["daily_change"] = 0
        else:
            records[i]["daily_change"] = records[i]["total_eval"] - records[i - 1]["total_eval"]

    # 최신순 정렬
    records.reverse()

    # history.json 저장
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump({"records": records}, f, ensure_ascii=False, indent=2)

    print(f"\n총 {len(records)}건 저장 완료 → {HISTORY_FILE}")
    print("-" * 60)
    for r in records:
        sign = "+" if r["daily_change"] >= 0 else ""
        print(f"  {r['date']}  평가 {r['total_eval']:>15,}원  "
              f"손익 {r['total_profit']:>+15,}원  "
              f"({r['total_profit_rate']:>+6.2f}%)  "
              f"전일대비 {sign}{r['daily_change']:,}원")


if __name__ == "__main__":
    backfill()
