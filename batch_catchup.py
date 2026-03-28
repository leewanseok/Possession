#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
"""
포트폴리오 누락 데이터 배치 복구 스크립트
=========================================
프로그램이 꺼져 있던 기간의 일별 손익 데이터를 KIS API로 자동 복구합니다.

사용법:
  python batch_catchup.py              # 마지막 기록 다음 날부터 오늘까지 자동 복구
  python batch_catchup.py --from 2026-03-01          # 특정 날짜부터 복구
  python batch_catchup.py --from 2026-03-01 --to 2026-03-20   # 특정 기간 복구
  python batch_catchup.py --recalc     # 전체 기간 재계산
"""

import json, os, sys, requests, warnings, argparse
from datetime import datetime, timedelta, time as dtime

warnings.filterwarnings("ignore")

BASE_DIR         = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE      = os.path.join(BASE_DIR, "config.json")
HISTORY_FILE     = os.path.join(BASE_DIR, "history.json")
PRICE_CACHE_FILE = os.path.join(BASE_DIR, "price_cache.json")
KRX_CLOSE_FILE   = os.path.join(BASE_DIR, "krx_close.json")

KR_HOLIDAYS = {
    "2025-01-01", "2025-01-28", "2025-01-29", "2025-01-30",
    "2025-05-05", "2025-05-06",
    "2025-06-06", "2025-08-15",
    "2025-10-03", "2025-10-06", "2025-10-07", "2025-10-08", "2025-10-09",
    "2025-12-25", "2025-12-31",
    "2026-01-01", "2026-01-28", "2026-01-29", "2026-01-30",
    "2026-03-02",
    "2026-05-05", "2026-05-25",
    "2026-06-06",
    "2026-08-17",
    "2026-09-24", "2026-09-25",
    "2026-10-03", "2026-10-09",
    "2026-12-25", "2026-12-31",
}

# ── 유틸 ──────────────────────────────────────────────────────────
def is_trading_day(d: str) -> bool:
    dt = datetime.strptime(d, "%Y-%m-%d")
    return dt.weekday() < 5 and d not in KR_HOLIDAYS

def prev_trading_day(d: str) -> str:
    dt = datetime.strptime(d, "%Y-%m-%d") - timedelta(days=1)
    while True:
        s = dt.strftime("%Y-%m-%d")
        if is_trading_day(s):
            return s
        dt -= timedelta(days=1)

def trading_days_between(from_d: str, to_d: str) -> list:
    result, cur = [], datetime.strptime(from_d, "%Y-%m-%d")
    end = datetime.strptime(to_d, "%Y-%m-%d")
    while cur <= end:
        s = cur.strftime("%Y-%m-%d")
        if is_trading_day(s):
            result.append(s)
        cur += timedelta(days=1)
    return result

def last_closed_trading_day() -> str:
    """오늘 장이 마감됐으면 오늘, 아니면 전 거래일 반환"""
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    closed = is_trading_day(today) and now.time() >= dtime(15, 35)
    return today if closed else prev_trading_day(today)

# ── KIS API ───────────────────────────────────────────────────────
BASE_URL = "https://openapi.koreainvestment.com:9443"

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

def fetch_ohlcv(token: str, cfg: dict, code: str, from_d: str, to_d: str) -> dict:
    """KRX 일봉 조회 → {date: {close, change, change_rate, volume}}"""
    kis = cfg["kis"]
    r = requests.get(
        f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": kis["app_key"],
            "appsecret": kis["app_secret"],
            "tr_id": "FHKST03010100",
        },
        params={
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD":         code,
            "FID_INPUT_DATE_1":       from_d.replace("-", ""),
            "FID_INPUT_DATE_2":       to_d.replace("-", ""),
            "FID_PERIOD_DIV_CODE":    "D",
            "FID_ORG_ADJ_PRC":        "0",
        },
        timeout=10, verify=False,
    )
    d = r.json()
    if d.get("rt_cd") != "0":
        print(f"    [경고] {code} API 오류: {d.get('msg1','')}")
        return {}

    rows = []
    for o in d.get("output2", []):
        dt  = o.get("stck_bsop_date", "")
        c   = int(o.get("stck_clpr", 0) or 0)
        vol = int(o.get("acml_vol",  0) or 0)
        if dt and c > 0:
            rows.append((f"{dt[:4]}-{dt[4:6]}-{dt[6:]}", c, vol))
    rows.sort(key=lambda x: x[0])   # 오래된 날짜 순

    result = {}
    for i, (date, close, volume) in enumerate(rows):
        prev_close = rows[i-1][1] if i > 0 else 0
        change      = close - prev_close if prev_close else 0
        change_rate = round(change / prev_close * 100, 2) if prev_close else 0.0
        result[date] = {"close": close, "change": change,
                        "change_rate": change_rate, "volume": volume}
    return result


# ── 메인 ──────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="포트폴리오 누락 데이터 복구")
    parser.add_argument("--from",  dest="from_date", default=None, help="시작 날짜 (YYYY-MM-DD)")
    parser.add_argument("--to",    dest="to_date",   default=None, help="종료 날짜 (YYYY-MM-DD)")
    parser.add_argument("--recalc",action="store_true",            help="전체 기간 재계산")
    args = parser.parse_args()

    print("=" * 54)
    print("  포트폴리오 누락 데이터 배치 복구")
    print("=" * 54)

    # ── 설정 로드 ────────────────────────────────────────────────
    with open(CONFIG_FILE, encoding="utf-8") as f:
        cfg = json.load(f)
    portfolio = cfg["portfolio"]
    TOTAL_BUY = round(sum(it["quantity"] * it["avg_price"] for it in portfolio))
    print(f"  종목 수  : {len(portfolio)}개")
    print(f"  총매입금 : {TOTAL_BUY:,}원\n")

    # ── 기존 history 로드 ────────────────────────────────────────
    with open(HISTORY_FILE, encoding="utf-8") as f:
        hist = json.load(f)
    records  = hist.get("records", [])
    rec_map  = {r["date"]: r for r in records}
    last_saved = records[0]["date"] if records else "2026-01-01"

    # ── 날짜 범위 결정 ───────────────────────────────────────────
    to_date = args.to_date or last_closed_trading_day()

    if args.recalc:
        from_date = records[-1]["date"] if records else last_saved
        print(f"  모드     : 전체 재계산")
    elif args.from_date:
        from_date = args.from_date
        print(f"  모드     : 지정 기간")
    else:
        # 자동: 마지막 기록 다음 거래일부터
        from_date = trading_days_between(
            (datetime.strptime(last_saved, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d"),
            to_date
        )
        from_date = from_date[0] if from_date else None
        print(f"  모드     : 자동 (마지막 기록 다음부터)")

    if not from_date or from_date > to_date:
        print(f"\n✅ 이미 최신 상태입니다. (마지막 기록: {last_saved})")
        return

    target_days = trading_days_between(from_date, to_date)
    if not target_days:
        print(f"\n✅ 해당 기간에 거래일 없음 ({from_date} ~ {to_date})")
        return

    print(f"  기간     : {from_date} ~ {to_date}")
    print(f"  처리 거래일: {len(target_days)}일")
    print(f"  {target_days}\n")

    # ── 토큰 발급 ────────────────────────────────────────────────
    print("[ 1 ] KIS API 토큰 발급 중...")
    token = get_token(cfg)
    print("      ✅ 완료\n")

    # ── OHLCV 조회 (전일 change 계산을 위해 7일 전부터) ──────────
    fetch_from = (datetime.strptime(from_date, "%Y-%m-%d") - timedelta(days=10)).strftime("%Y-%m-%d")
    print(f"[ 2 ] 종목별 일봉 조회 ({fetch_from} ~ {to_date})")
    price_by_date = {}   # {date: {code: {close, change, change_rate}}}
    for it in portfolio:
        code = str(it["code"]).zfill(6)
        print(f"      {it['name']}({code})...", end=" ", flush=True)
        daily = fetch_ohlcv(token, cfg, code, fetch_from, to_date)
        for dt, info in daily.items():
            price_by_date.setdefault(dt, {})[code] = info
        hits = {dt: info["close"] for dt, info in daily.items() if dt in target_days}
        print(f"OK  {hits}")
    print()

    # ── 일자별 총평가금액 계산 ────────────────────────────────────
    print("[ 3 ] 일자별 포트폴리오 계산")
    new_records = []
    all_sorted  = sorted(price_by_date.keys())

    for target_date in target_days:
        day_prices = price_by_date.get(target_date)
        if not day_prices:
            print(f"      ⚠️  {target_date}: 종가 데이터 없음 → 스킵 (휴장 가능성)")
            continue

        missing = [str(it["code"]).zfill(6) for it in portfolio
                   if str(it["code"]).zfill(6) not in day_prices]
        if missing:
            print(f"      ⚠️  {target_date}: 누락 종목 {missing} → 스킵")
            continue

        total_eval = round(sum(
            day_prices[str(it["code"]).zfill(6)]["close"] * it["quantity"]
            for it in portfolio
        ))
        total_profit      = total_eval - TOTAL_BUY
        total_profit_rate = round(total_profit / TOTAL_BUY * 100, 2)

        # 전일 평가금액 (price_by_date 우선, 없으면 history에서 찾기)
        idx = all_sorted.index(target_date) if target_date in all_sorted else -1
        prev_eval = 0
        if idx > 0:
            prev_date = all_sorted[idx - 1]
            pd = price_by_date.get(prev_date, {})
            if all(str(it["code"]).zfill(6) in pd for it in portfolio):
                prev_eval = round(sum(
                    pd[str(it["code"]).zfill(6)]["close"] * it["quantity"]
                    for it in portfolio
                ))
        if prev_eval == 0:
            for r in sorted(records, key=lambda x: x["date"], reverse=True):
                if r["date"] < target_date:
                    prev_eval = r["total_eval"]
                    break

        daily_change = round(total_eval - prev_eval) if prev_eval else 0
        tag = "수정" if target_date in rec_map else "신규"
        sign = "+" if daily_change >= 0 else ""
        print(f"      [{tag}] {target_date}  eval={total_eval:,}원  "
              f"daily={sign}{daily_change:,}원")

        new_records.append({
            "date":              target_date,
            "total_buy":         TOTAL_BUY,
            "total_eval":        total_eval,
            "total_profit":      total_profit,
            "total_profit_rate": total_profit_rate,
            "daily_change":      daily_change,
        })

    if not new_records:
        print("\n  처리할 데이터가 없습니다.")
        return

    # ── history.json 업데이트 ────────────────────────────────────
    print(f"\n[ 4 ] history.json 업데이트 ({len(new_records)}건)")
    new_map = {r["date"]: r for r in new_records}
    final   = [new_map.get(r["date"], r) for r in records]    # 기존 기록 교체
    for r in new_records:                                       # 신규 추가
        if r["date"] not in rec_map:
            final.append(r)
    final.sort(key=lambda x: x["date"], reverse=True)
    hist["records"] = final
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(hist, f, ensure_ascii=False, indent=2)
    print("      ✅ 완료")

    # ── price_cache.json 최신 가격으로 업데이트 ──────────────────
    latest_date  = max(d for d in price_by_date if d in target_days or d == to_date)
    latest_prices = price_by_date.get(latest_date, {})
    if latest_prices and all(str(it["code"]).zfill(6) in latest_prices for it in portfolio):
        print(f"\n[ 5 ] price_cache.json 업데이트 ({latest_date} 기준)")
        cache = {}
        for it in portfolio:
            code = str(it["code"]).zfill(6)
            info = latest_prices[code]
            avg, qty = it["avg_price"], it["quantity"]
            cache[code] = {
                "code":         code,
                "name":         it["name"],
                "current_price":info["close"],
                "change":       info["change"],
                "change_rate":  info["change_rate"],
                "profit_rate":  round((info["close"] - avg) / avg * 100, 2) if avg else 0.0,
                "profit_loss":  round((info["close"] - avg) * qty, 2),
            }
            print(f"      {it['name']:12s}: {info['close']:,}원  "
                  f"{'+' if info['change']>=0 else ''}{info['change']:,}  "
                  f"({'+' if info['change_rate']>=0 else ''}{info['change_rate']:.2f}%)")
        with open(PRICE_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
        print("      ✅ 완료")

        # ── krx_close.json 업데이트 ──────────────────────────────
        print(f"\n[ 6 ] krx_close.json 업데이트 ({latest_date} 기준)")
        latest_rec = next((r for r in final if r["date"] == latest_date), None)
        if latest_rec:
            holdings = []
            for it in portfolio:
                code = str(it["code"]).zfill(6)
                info = latest_prices[code]
                avg, qty = it["avg_price"], it["quantity"]
                holdings.append({
                    "code":         code,
                    "name":         it["name"],
                    "current_price":info["close"],
                    "change":       info["change"],
                    "change_rate":  info["change_rate"],
                    "profit_rate":  round((info["close"] - avg) / avg * 100, 2) if avg else 0.0,
                    "profit_loss":  round((info["close"] - avg) * qty, 2),
                })
            with open(KRX_CLOSE_FILE, "w", encoding="utf-8") as f:
                json.dump({
                    "date":              latest_rec["date"],
                    "total_eval":        latest_rec["total_eval"],
                    "total_profit":      latest_rec["total_profit"],
                    "total_profit_rate": latest_rec["total_profit_rate"],
                    "daily_change":      latest_rec["daily_change"],
                    "holdings":          holdings,
                }, f, ensure_ascii=False, indent=2)
            print("      ✅ 완료")

    # ── 결과 요약 ─────────────────────────────────────────────────
    print("\n" + "=" * 54)
    print("  배치 완료! 최근 기록:")
    print("=" * 54)
    for r in final[:min(7, len(final))]:
        sign = "+" if r["daily_change"] >= 0 else ""
        print(f"  {r['date']}  평가 {r['total_eval']:>13,}원  "
              f"전일대비 {sign}{r['daily_change']:>11,}원")


if __name__ == "__main__":
    main()
