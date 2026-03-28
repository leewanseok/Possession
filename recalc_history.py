#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
3월 20일부터 history.json 재계산 배치
KIS API로 각 종목 KRX 일별 종가 조회 → total_eval 재산정
"""
import json, os, requests, warnings
warnings.filterwarnings("ignore")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE  = os.path.join(BASE_DIR, "config.json")
HISTORY_FILE = os.path.join(BASE_DIR, "history.json")

# ── 설정 로드 ──────────────────────────────────────────────
with open(CONFIG_FILE, encoding="utf-8") as f:
    cfg = json.load(f)

kis = cfg["kis"]
APP_KEY    = kis["app_key"]
APP_SECRET = kis["app_secret"]
BASE_URL   = "https://openapi.koreainvestment.com:9443"

PORTFOLIO = cfg["portfolio"]   # [{code, quantity, avg_price, ...}, ...]
TOTAL_BUY = 446972809          # 고정 매입금액

# ── KIS 토큰 발급 ───────────────────────────────────────────
def get_token():
    r = requests.post(
        f"{BASE_URL}/oauth2/tokenP",
        json={"grant_type": "client_credentials",
              "appkey": APP_KEY, "appsecret": APP_SECRET},
        timeout=10, verify=False
    )
    return r.json()["access_token"]

# ── KRX 일봉 조회 ───────────────────────────────────────────
def fetch_daily_prices(token, code, from_date, to_date):
    """code의 from_date~to_date KRX 일봉 종가 반환 → {date: close_price}"""
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": "FHKST03010100",
    }
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD":         code,
        "FID_INPUT_DATE_1":       from_date.replace("-",""),
        "FID_INPUT_DATE_2":       to_date.replace("-",""),
        "FID_PERIOD_DIV_CODE":    "D",
        "FID_ORG_ADJ_PRC":        "0",
    }
    r = requests.get(
        f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
        headers=headers, params=params, timeout=10, verify=False
    )
    d = r.json()
    result = {}
    if d.get("rt_cd") != "0":
        print(f"  [오류] {code}: {d.get('msg1','')}")
        return result
    for o in d.get("output2", []):
        dt = o.get("stck_bsop_date","")
        c  = int(o.get("stck_clpr", 0) or 0)
        if dt and c > 0:
            key = f"{dt[:4]}-{dt[4:6]}-{dt[6:]}"
            result[key] = c
    return result

# ── 메인 ────────────────────────────────────────────────────
print("토큰 발급 중...")
token = get_token()
print("토큰 발급 완료")

FROM_DATE = "2026-03-19"   # daily_change 계산을 위해 하루 전부터
TO_DATE   = "2026-03-24"

# 종목별 일별 종가 수집
price_by_date = {}   # {date: {code: close_price}}
for it in PORTFOLIO:
    code = str(it["code"]).zfill(6)
    name = it["name"]
    print(f"  {name}({code}) 조회 중...")
    daily = fetch_daily_prices(token, code, FROM_DATE, TO_DATE)
    print(f"    → {daily}")
    for date, price in daily.items():
        price_by_date.setdefault(date, {})[code] = price

print("\n수집된 날짜별 종가:")
for d in sorted(price_by_date.keys()):
    print(f"  {d}: {price_by_date[d]}")

# 각 날짜별 total_eval 계산
def calc_eval(date):
    prices = price_by_date.get(date, {})
    total = 0
    for it in PORTFOLIO:
        code = str(it["code"]).zfill(6)
        qty  = it["quantity"]
        p    = prices.get(code, 0)
        if p == 0:
            print(f"  [경고] {date} {it['name']} 종가 없음!")
            return None
        total += p * qty
    return total

# 재계산 대상 날짜 (3/20부터, 거래일만)
TARGET_DATES = ["2026-03-20", "2026-03-21", "2026-03-23", "2026-03-24"]
# 3/21은 토요일이므로 실제 거래일: 3/20(금), 3/23(월), 3/24(화)
TRADING_DATES = [d for d in TARGET_DATES if d in price_by_date]
print(f"\n재계산 거래일: {TRADING_DATES}")

# 기존 history 로드
with open(HISTORY_FILE, encoding="utf-8") as f:
    hist = json.load(f)

records = hist.get("records", [])
rec_map = {r["date"]: r for r in records}

# 각 날짜 재계산
new_records = []
for date in TRADING_DATES:
    total_eval = calc_eval(date)
    if total_eval is None:
        print(f"  [스킵] {date} - 종가 데이터 불완전")
        continue

    total_profit = total_eval - TOTAL_BUY
    total_profit_rate = round(total_profit / TOTAL_BUY * 100, 2)

    # 전일 total_eval 계산 (daily_change용)
    all_dates_sorted = sorted(price_by_date.keys())
    idx = all_dates_sorted.index(date)
    prev_eval = 0
    if idx > 0:
        prev_date = all_dates_sorted[idx - 1]
        prev_eval = calc_eval(prev_date) or 0
        if prev_eval == 0 and prev_date in rec_map:
            prev_eval = rec_map[prev_date]["total_eval"]
    daily_change = total_eval - prev_eval if prev_eval else 0

    new_rec = {
        "date": date,
        "total_buy": TOTAL_BUY,
        "total_eval": total_eval,
        "total_profit": total_profit,
        "total_profit_rate": total_profit_rate,
        "daily_change": daily_change,
    }
    old = rec_map.get(date, {})
    print(f"\n[{date}]")
    print(f"  이전 total_eval: {old.get('total_eval', 'N/A'):,}원" if old else f"  이전 기록 없음")
    print(f"  새  total_eval: {total_eval:,}원")
    print(f"  전일대비: {'+' if daily_change>=0 else ''}{daily_change:,}원")
    new_records.append(new_rec)

# history에 반영 (재계산 날짜 교체, 나머지 유지)
updated_map = {r["date"]: r for r in new_records}
final_records = []
for r in records:
    if r["date"] in updated_map:
        final_records.append(updated_map.pop(r["date"]))
    else:
        final_records.append(r)
# 새로 추가된 날짜 (기존에 없던 날짜)
for r in new_records:
    if r["date"] in updated_map:
        final_records.append(updated_map[r["date"]])
        del updated_map[r["date"]]

# 날짜 내림차순 정렬
final_records.sort(key=lambda x: x["date"], reverse=True)

hist["records"] = final_records
with open(HISTORY_FILE, "w", encoding="utf-8") as f:
    json.dump(hist, f, ensure_ascii=False, indent=2)

print("\n\nhistory.json 업데이트 완료!")
print("\n최종 결과:")
for r in final_records[:6]:
    print(f"  {r['date']}: eval={r['total_eval']:,}  daily={'+' if r['daily_change']>=0 else ''}{r['daily_change']:,}")
