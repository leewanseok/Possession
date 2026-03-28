#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
"""
미국 주식 포트폴리오 누락 데이터 배치 복구
==========================================
KIS API 해외 일봉 + 날짜별 USD/KRW 환율을 조회하여
us_history.json 을 채웁니다.

사용법:
  python batch_catchup_us.py              # 마지막 기록 다음 날부터 오늘까지
  python batch_catchup_us.py --from 2026-03-01
  python batch_catchup_us.py --from 2026-03-01 --to 2026-03-20
  python batch_catchup_us.py --recalc    # 2026-03-01 부터 전체 재계산
"""

import json, os, argparse, time, warnings, requests
from datetime import datetime, timedelta, time as dtime

warnings.filterwarnings("ignore")

BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE     = os.path.join(BASE_DIR, "config.json")
US_HISTORY_FILE = os.path.join(BASE_DIR, "us_history.json")

BASE_URL = "https://openapi.koreainvestment.com:9443"

US_HOLIDAYS = {
    "2025-01-01","2025-01-20","2025-02-17","2025-04-18",
    "2025-05-26","2025-06-19","2025-07-04","2025-09-01",
    "2025-11-27","2025-12-25",
    "2026-01-01","2026-01-19","2026-02-16","2026-04-03",
    "2026-05-25","2026-07-03","2026-09-07","2026-11-26",
    "2026-12-25",
}

# ── 유틸 ──────────────────────────────────────────────────────
def is_us_trading_day(d: str) -> bool:
    dt = datetime.strptime(d, "%Y-%m-%d")
    return dt.weekday() < 5 and d not in US_HOLIDAYS

def trading_days_between(from_d: str, to_d: str) -> list:
    result, cur = [], datetime.strptime(from_d, "%Y-%m-%d")
    end = datetime.strptime(to_d, "%Y-%m-%d")
    while cur <= end:
        s = cur.strftime("%Y-%m-%d")
        if is_us_trading_day(s):
            result.append(s)
        cur += timedelta(days=1)
    return result

def last_us_closed_day() -> str:
    """마감 완료된 직전 미국 거래일 (DST 인식)
    미국 정규장 마감: 서머타임 KST 05:00 / 일반 KST 06:00
    마감 시간 이후 → 어제(KST) = 직전 US 거래일
    마감 시간 이전 → 그제(KST) 이전 거래일"""
    now = datetime.now()
    m = now.month
    if m < 3 or m > 11:
        is_dst = False
    elif 3 < m < 11:
        is_dst = True
    elif m == 3:
        sun2 = 8 + (6 - datetime(now.year, 3, 1).weekday()) % 7
        is_dst = now.day >= sun2
    else:
        sun1 = 1 + (6 - datetime(now.year, 11, 1).weekday()) % 7
        is_dst = now.day < sun1
    close_kst = dtime(5, 0) if is_dst else dtime(6, 0)
    offset = 1 if now.time() >= close_kst else 2
    dt = now - timedelta(days=offset)
    while True:
        s = dt.strftime("%Y-%m-%d")
        if is_us_trading_day(s):
            return s
        dt -= timedelta(days=1)

# ── KIS API ───────────────────────────────────────────────────
def get_token(cfg: dict) -> str:
    kis = cfg["kis"]
    r = requests.post(
        f"{BASE_URL}/oauth2/tokenP",
        json={"grant_type": "client_credentials",
              "appkey": kis["app_key"], "appsecret": kis["app_secret"]},
        timeout=10, verify=False,
    )
    d = r.json()
    if "access_token" not in d:
        raise RuntimeError(f"토큰 발급 실패: {d}")
    return d["access_token"]

def fetch_us_ohlcv(token: str, cfg: dict, ticker: str, exchange: str, to_date: str) -> dict:
    """HHDFS76240000 해외 일봉 → {date: close_price}"""
    kis = cfg["kis"]
    r = requests.get(
        f"{BASE_URL}/uapi/overseas-price/v1/quotations/dailyprice",
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": kis["app_key"],
            "appsecret": kis["app_secret"],
            "tr_id": "HHDFS76240000",
        },
        params={
            "AUTH": "", "EXCD": exchange, "SYMB": ticker.upper(),
            "GUBN": "0", "BYMD": to_date.replace("-", ""), "MODP": "1",
        },
        timeout=10, verify=False,
    )
    d = r.json()
    if d.get("rt_cd") != "0":
        print(f"    [경고] {ticker} API 오류: {d.get('msg1','')}")
        return {}
    result = {}
    for o in d.get("output2", []):
        date = o.get("xymd", "")
        c = float(o.get("clos", 0) or 0)
        if date and c > 0:
            fmt = f"{date[:4]}-{date[4:6]}-{date[6:]}"
            result[fmt] = c
    return result

def get_historical_exrate(date: str) -> float:
    """fawazahmed0 API — 해당 날짜 USD/KRW 환율"""
    try:
        url = (f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api"
               f"@{date}/v1/currencies/usd.min.json")
        r = requests.get(url, timeout=8)
        rate = float(r.json().get("usd", {}).get("krw", 0))
        if rate > 0:
            return rate
    except Exception as e:
        print(f"    [환율] {date} 조회 실패: {e}")
    # fallback: 최신 환율
    try:
        r = requests.get(
            "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api"
            "@latest/v1/currencies/usd.min.json", timeout=8)
        rate = float(r.json().get("usd", {}).get("krw", 0))
        if rate > 0:
            return rate
    except Exception:
        pass
    return 1380.0

# ── 히스토리 I/O ──────────────────────────────────────────────
def load_us_history() -> dict:
    if os.path.exists(US_HISTORY_FILE):
        try:
            with open(US_HISTORY_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"records": []}

# ── 메인 ──────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="미국 주식 포트폴리오 누락 데이터 복구")
    parser.add_argument("--from",  dest="from_date", default=None, help="시작 날짜 (YYYY-MM-DD)")
    parser.add_argument("--to",    dest="to_date",   default=None, help="종료 날짜 (YYYY-MM-DD)")
    parser.add_argument("--recalc", action="store_true",            help="2026-03-01부터 전체 재계산")
    args = parser.parse_args()

    print("=" * 56)
    print("  미국 주식 포트폴리오 배치 복구")
    print("=" * 56)

    with open(CONFIG_FILE, encoding="utf-8") as f:
        cfg = json.load(f)
    us_portfolio = cfg.get("us_portfolio", [])
    if not us_portfolio:
        print("  ❌ config.json 에 us_portfolio 설정이 없습니다.")
        return

    TOTAL_BUY_USD = round(sum(it["quantity"] * it["avg_price"] for it in us_portfolio), 2)
    print(f"  종목 수  : {len(us_portfolio)}개")
    for it in us_portfolio:
        print(f"    - {it['ticker']:6s} {it.get('name',''):20s} {it['quantity']}주 @ ${it['avg_price']:.4f}")
    print(f"  총매입($): ${TOTAL_BUY_USD:,.2f}\n")

    hist    = load_us_history()
    records = hist.get("records", [])
    rec_map = {r["date"]: r for r in records}
    last_saved = records[0]["date"] if records else "2026-02-28"

    to_date = args.to_date or last_us_closed_day()

    if args.recalc:
        from_date = "2026-03-01"
        print(f"  모드     : 전체 재계산 (2026-03-01 ~ {to_date})")
    elif args.from_date:
        from_date = args.from_date
        print(f"  모드     : 지정 기간 ({from_date} ~ {to_date})")
    else:
        nxt_list = trading_days_between(
            (datetime.strptime(last_saved, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d"),
            to_date,
        )
        from_date = nxt_list[0] if nxt_list else None
        print(f"  모드     : 자동 (마지막 기록 다음부터)")

    if not from_date or from_date > to_date:
        print(f"\n✅ 이미 최신 상태입니다. (마지막 기록: {last_saved})")
        return

    target_days = trading_days_between(from_date, to_date)
    if not target_days:
        print(f"\n✅ 해당 기간에 거래일 없음 ({from_date} ~ {to_date})")
        return

    print(f"  기간     : {from_date} ~ {to_date}")
    print(f"  처리 거래일: {len(target_days)}일\n")

    # ── 토큰 ────────────────────────────────────────────────────
    print("[ 1 ] KIS API 토큰 발급 중...")
    token = get_token(cfg)
    print("      ✅ 완료\n")

    # ── OHLCV 조회 ──────────────────────────────────────────────
    print(f"[ 2 ] 종목별 일봉 조회 (기준일: {to_date})")
    price_by_date = {}  # {date: {ticker: close}}
    for it in us_portfolio:
        ticker   = it["ticker"]
        exchange = it.get("exchange", "NAS")
        print(f"      {ticker} ({exchange})...", end=" ", flush=True)
        daily = fetch_us_ohlcv(token, cfg, ticker, exchange, to_date)
        for dt, close in daily.items():
            price_by_date.setdefault(dt, {})[ticker] = close
        hits = {dt: round(v, 2) for dt, v in daily.items() if dt in target_days}
        print(f"OK  {hits}")
        time.sleep(0.35)
    print()

    # ── 환율 조회 ────────────────────────────────────────────────
    print("[ 3 ] 일별 USD/KRW 환율 조회")
    exrate_map = {}
    for d in target_days:
        rate = get_historical_exrate(d)
        exrate_map[d] = rate
        print(f"      {d}: {rate:,.2f}원")
        time.sleep(0.15)
    print()

    # ── 일자별 포트폴리오 계산 ───────────────────────────────────
    print("[ 4 ] 일자별 포트폴리오 계산")
    new_records = []
    sorted_days = sorted(price_by_date.keys())

    for target_date in target_days:
        day_prices = price_by_date.get(target_date)
        if not day_prices:
            print(f"      ⚠️  {target_date}: 종가 데이터 없음 → 스킵 (휴장 가능성)")
            continue

        missing = [it["ticker"] for it in us_portfolio if it["ticker"] not in day_prices]
        if missing:
            print(f"      ⚠️  {target_date}: 누락 종목 {missing} → 스킵")
            continue

        ex_rate           = exrate_map.get(target_date, 1380.0)
        total_eval_usd    = round(sum(day_prices[it["ticker"]] * it["quantity"] for it in us_portfolio), 2)
        total_profit_usd  = round(total_eval_usd - TOTAL_BUY_USD, 2)
        total_profit_rate = round(total_profit_usd / TOTAL_BUY_USD * 100, 2) if TOTAL_BUY_USD else 0.0
        total_eval_krw    = round(total_eval_usd * ex_rate)
        total_buy_krw     = round(TOTAL_BUY_USD  * ex_rate)
        total_profit_krw  = round(total_profit_usd * ex_rate)

        # 전일 평가금액 (USD)
        idx = sorted_days.index(target_date) if target_date in sorted_days else -1
        prev_eval_usd = 0.0
        if idx > 0:
            prev_date = sorted_days[idx - 1]
            pd = price_by_date.get(prev_date, {})
            if all(it["ticker"] in pd for it in us_portfolio):
                prev_eval_usd = round(sum(pd[it["ticker"]] * it["quantity"] for it in us_portfolio), 2)
        if prev_eval_usd == 0:
            for r in sorted(records + new_records, key=lambda x: x["date"], reverse=True):
                if r["date"] < target_date:
                    prev_eval_usd = r.get("total_eval_usd", 0)
                    break

        daily_chg_usd = round(total_eval_usd - prev_eval_usd, 2) if prev_eval_usd else 0.0
        daily_chg_krw = round(daily_chg_usd * ex_rate)

        tag  = "수정" if target_date in rec_map else "신규"
        sign = "+" if daily_chg_krw >= 0 else ""
        print(f"      [{tag}] {target_date}  "
              f"eval=${total_eval_usd:>10,.2f}  ₩{total_eval_krw:>13,}  "
              f"daily={sign}₩{daily_chg_krw:>10,}  (환율:{ex_rate:,.0f})")

        new_records.append({
            "date":               target_date,
            "total_buy_usd":      TOTAL_BUY_USD,
            "total_eval_usd":     total_eval_usd,
            "total_profit_usd":   total_profit_usd,
            "total_profit_rate":  total_profit_rate,
            "daily_change_usd":   daily_chg_usd,
            "exchange_rate":      ex_rate,
            "total_buy_krw":      total_buy_krw,
            "total_eval_krw":     total_eval_krw,
            "total_profit_krw":   total_profit_krw,
            "daily_change_krw":   daily_chg_krw,
        })

    if not new_records:
        print("\n  처리할 데이터가 없습니다.")
        return

    # ── 저장 ────────────────────────────────────────────────────
    print(f"\n[ 5 ] us_history.json 업데이트 ({len(new_records)}건)")
    new_map = {r["date"]: r for r in new_records}
    final   = [new_map.get(r["date"], r) for r in records]
    for r in new_records:
        if r["date"] not in rec_map:
            final.append(r)
    final.sort(key=lambda x: x["date"], reverse=True)
    hist["records"] = final
    with open(US_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(hist, f, ensure_ascii=False, indent=2)
    print("      ✅ 완료")

    print("\n" + "=" * 56)
    print("  배치 완료! 최근 기록:")
    print("=" * 56)
    for r in final[:min(7, len(final))]:
        sign = "+" if r.get("daily_change_krw", 0) >= 0 else ""
        print(f"  {r['date']}  평가 ₩{r.get('total_eval_krw',0):>13,}  "
              f"전일대비 {sign}₩{r.get('daily_change_krw',0):>10,}")


if __name__ == "__main__":
    main()
