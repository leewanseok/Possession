"""
코인 일별손익 백필 스크립트
- 레코드 날짜 D = D+1일 09:00 KST 시가 기준 (D 기간: D 09:00 ~ D+1 09:00)
- 2026-03-01(기간) ~ 어제(기간) 생성
"""
import json, os, sys, time, warnings
import requests
from datetime import date, timedelta

warnings.filterwarnings("ignore")

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "config.json")
HIST_FILE   = os.path.join(BASE_DIR, "coin_history.json")
UPBIT_BASE  = "https://api.upbit.com/v1"
START_DATE  = date(2026, 3, 1)

# ── config 로드 ──────────────────────────────────────
with open(CONFIG_FILE, encoding="utf-8") as f:
    config = json.load(f)
holdings = config.get("coin_portfolio", [])
if not holdings:
    print("[오류] config.json에 coin_portfolio가 없습니다.")
    sys.exit(1)

# ── 업비트 일봉 캔들 조회 ────────────────────────────
def get_daily_candles(ticker: str, count: int = 100) -> dict:
    """{ '2026-03-01': opening_price, ... } 형태로 반환"""
    market = f"KRW-{ticker}"
    prices = {}
    fetched = 0
    to_param = None
    while fetched < count:
        params = {"market": market, "count": min(200, count - fetched)}
        if to_param:
            params["to"] = to_param
        try:
            r = requests.get(f"{UPBIT_BASE}/candles/days",
                             params=params,
                             headers={"Accept": "application/json"},
                             timeout=10, verify=False)
            data = r.json()
            if not data:
                break
            for c in data:
                dt = c["candle_date_time_kst"][:10]   # "2026-03-27"
                prices[dt] = c["opening_price"]         # 09:00 KST 시가
            # 다음 페이지: 마지막 캔들 날짜 이전
            last_dt = data[-1]["candle_date_time_utc"]  # UTC 기준
            to_param = last_dt
            fetched += len(data)
            time.sleep(0.1)
        except Exception as e:
            print(f"  [경고] {ticker} 캔들 조회 오류: {e}")
            break
    return prices

# ── 각 코인 캔들 수집 ────────────────────────────────
today     = date.today()
yesterday = today - timedelta(days=1)
# 레코드 D는 D+1일 시가 필요 → 오늘 시가까지 수집
days_needed = (today - START_DATE).days + 10  # 여유분 포함

print(f"업비트 일봉 데이터 수집 중 (기간: {START_DATE} ~ {yesterday}, 시가기준: {START_DATE + timedelta(days=1)} ~ {today})...")
all_prices = {}  # { 'BTC': { '2026-03-01': price, ... }, ... }
for h in holdings:
    ticker = h["ticker"].upper()
    print(f"  {ticker} 캔들 조회 중...", end=" ")
    prices = get_daily_candles(ticker, days_needed)
    all_prices[ticker] = prices
    print(f"{len(prices)}일 수집 완료")

# ── 날짜별 포트폴리오 가치 계산 ──────────────────────
# 레코드 날짜 D → 평가금액은 D+1일 09:00 시가 사용 (D 기간의 종료 시점)
def calc_snapshot(period_date: date) -> dict | None:
    next_date_str = (period_date + timedelta(days=1)).strftime("%Y-%m-%d")
    total_buy  = 0.0
    total_eval = 0.0
    for h in holdings:
        ticker = h["ticker"].upper()
        qty    = float(h["quantity"])
        avg    = float(h["avg_price"])
        price  = all_prices.get(ticker, {}).get(next_date_str, 0)
        if price <= 0:
            return None  # 해당일 데이터 없음
        total_buy  += qty * avg
        total_eval += qty * price
    total_profit = total_eval - total_buy
    rate = (total_profit / total_buy * 100) if total_buy > 0 else 0
    return {
        "date":              period_date.strftime("%Y-%m-%d"),
        "total_buy":         total_buy,
        "total_eval":        total_eval,
        "total_profit":      total_profit,
        "total_profit_rate": rate,
        "daily_change":      0.0,  # 아래에서 계산
    }

# 2026-03-01(기간) ~ 어제(기간)까지 생성
print("\n일별 스냅샷 생성 중...")
records = []
d = START_DATE
while d <= yesterday:
    snap = calc_snapshot(d)
    if snap:
        records.append(snap)
    d += timedelta(days=1)

# daily_change 계산 (전일 대비)
for i, rec in enumerate(records):
    if i == 0:
        rec["daily_change"] = rec["total_eval"]
    else:
        rec["daily_change"] = rec["total_eval"] - records[i - 1]["total_eval"]

# ── 기존 히스토리와 병합 (최신순 정렬) ───────────────
if os.path.exists(HIST_FILE):
    with open(HIST_FILE, encoding="utf-8") as f:
        existing = json.load(f)
    existing_recs = existing.get("records", [])
else:
    existing_recs = []

# 백필 날짜 우선 → 기존 기록 중 백필에 없는 날짜만 추가
backfill_dates = {r["date"] for r in records}
merged = records + [r for r in existing_recs if r["date"] not in backfill_dates]
merged.sort(key=lambda r: r["date"])  # 오름차순으로 daily_change 재계산

# 병합 후 daily_change 전체 재계산 (날짜순)
for i, rec in enumerate(merged):
    if i == 0:
        rec["daily_change"] = rec["total_eval"]
    else:
        rec["daily_change"] = rec["total_eval"] - merged[i - 1]["total_eval"]

merged.sort(key=lambda r: r["date"], reverse=True)  # 최신순으로 저장

with open(HIST_FILE, "w", encoding="utf-8") as f:
    json.dump({"records": merged}, f, ensure_ascii=False, indent=2)

print(f"\n완료: {len(records)}개 레코드 생성, 총 {len(merged)}개 저장")
print(f"파일: {HIST_FILE}")
for r in records[:5]:
    print(f"  {r['date']}: 평가 {r['total_eval']:,.0f}원  손익 {r['total_profit']:+,.0f}원")
if len(records) > 5:
    print(f"  ... (총 {len(records)}일)")
