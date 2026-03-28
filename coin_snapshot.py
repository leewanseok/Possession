"""
코인 일별손익 스냅샷 스크립트 (매일 09:00 실행)
- 업비트 09:00 KST 시가 기준으로 오늘 날짜 스냅샷 저장
- Windows 작업 스케줄러에서 매일 09:00에 실행
"""
import json, os, sys, warnings
import requests
from datetime import datetime, timedelta

warnings.filterwarnings("ignore")

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "config.json")
HIST_FILE   = os.path.join(BASE_DIR, "coin_history.json")
LOG_FILE    = os.path.join(BASE_DIR, "coin_snapshot.log")
UPBIT_BASE  = "https://api.upbit.com/v1"

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def main():
    now       = datetime.now()
    today     = now.strftime("%Y-%m-%d")
    # 매일 09:00에 실행 - 어제 기간(어제 09:00 ~ 오늘 09:00)을 어제 날짜로 저장
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    log(f"=== 코인 스냅샷 시작 ({yesterday} 기간 종료, 오늘 {today} 09:00 시가 기준) ===")

    # ── config 로드 ───────────────────────────────────
    try:
        with open(CONFIG_FILE, encoding="utf-8") as f:
            config = json.load(f)
    except Exception as e:
        log(f"[오류] config.json 로드 실패: {e}")
        sys.exit(1)

    holdings = config.get("coin_portfolio", [])
    if not holdings:
        log("[경고] coin_portfolio가 비어있습니다. 종료.")
        return

    # ── 기존 히스토리 로드 ────────────────────────────
    if os.path.exists(HIST_FILE):
        with open(HIST_FILE, encoding="utf-8") as f:
            hist = json.load(f)
        records = hist.get("records", [])
    else:
        records = []

    # 기간 시작 기준가: yesterday 저장 레코드의 09:00 시가
    period_ref = next((r for r in records if r["date"] == yesterday), None)

    # 재실행 허용: yesterday 레코드 제거 후 재저장
    records = [r for r in records if r["date"] != yesterday]

    # ── 업비트 09:00 시가 조회 (일봉 count=1) ─────────
    prices = {}
    for h in holdings:
        ticker = h["ticker"].upper()
        market = f"KRW-{ticker}"
        try:
            r = requests.get(f"{UPBIT_BASE}/candles/days",
                             params={"market": market, "count": 1},
                             headers={"Accept": "application/json"},
                             timeout=10, verify=False)
            data = r.json()
            if data:
                # 오늘 캔들이 아직 없을 수 있으므로 날짜 확인
                c = data[0]
                candle_date = c["candle_date_time_kst"][:10]
                if candle_date == today:
                    prices[ticker] = c["opening_price"]
                    log(f"  {ticker}: {c['opening_price']:,.0f}원 (09:00 시가)")
                else:
                    # 오늘 09:00 이전에 실행된 경우 — 현재가 사용
                    r2 = requests.get(f"{UPBIT_BASE}/ticker",
                                      params={"markets": market},
                                      headers={"Accept": "application/json"},
                                      timeout=10, verify=False)
                    tp = float(r2.json()[0].get("trade_price", 0))
                    prices[ticker] = tp
                    log(f"  {ticker}: {tp:,.0f}원 (현재가 사용 - 오늘 캔들 미개설)")
        except Exception as e:
            log(f"  [오류] {ticker} 가격 조회 실패: {e}")

    if len(prices) != len(holdings):
        log(f"[경고] 일부 코인 가격 조회 실패. 저장 건너뜀.")
        return

    # ── 포트폴리오 계산 ───────────────────────────────
    total_buy  = 0.0
    total_eval = 0.0
    for h in holdings:
        ticker = h["ticker"].upper()
        qty    = float(h["quantity"])
        avg    = float(h["avg_price"])
        price  = prices.get(ticker, 0)
        total_buy  += qty * avg
        total_eval += qty * price

    total_profit = total_eval - total_buy
    rate = (total_profit / total_buy * 100) if total_buy > 0 else 0

    # 기간 변화: 오늘 09:00 시가 - 어제 09:00 시가 (period_ref = 어제 저장 레코드)
    if period_ref:
        prev_eval = period_ref["total_eval"]
    else:
        prev_eval = records[0]["total_eval"] if records else 0
    daily_change = total_eval - prev_eval

    new_rec = {
        "date":              yesterday,
        "total_buy":         total_buy,
        "total_eval":        total_eval,
        "total_profit":      total_profit,
        "total_profit_rate": rate,
        "daily_change":      daily_change,
    }
    records.insert(0, new_rec)

    # ── 저장 ─────────────────────────────────────────
    with open(HIST_FILE, "w", encoding="utf-8") as f:
        json.dump({"records": records}, f, ensure_ascii=False, indent=2)

    log(f"저장 완료: [{yesterday}] 평가 {total_eval:,.0f}원, 손익 {total_profit:+,.0f}원 ({rate:+.2f}%), 기간변화 {daily_change:+,.0f}원")

    # ── 텔레그램 알림 ─────────────────────────────
    try:
        tg_cfg = config.get("telegram", {})
        token  = tg_cfg.get("bot_token", "").strip()
        chat   = tg_cfg.get("chat_id",   "").strip()
        if token and chat:
            sign_dc = "+" if daily_change >= 0 else ""
            sign_p  = "+" if total_profit >= 0 else ""
            lines   = [f"<b>[코인 일별 스냅샷] {yesterday} 기간 완료</b>",
                       "─" * 20]
            for h in holdings:
                t  = h["ticker"].upper()
                p  = prices.get(t, 0)
                lines.append(f"  {t}: {p:,.0f}원")
            lines += [
                "─" * 20,
                f"평가금액  <b>{total_eval:,.0f}원</b>",
                f"평가손익  <b>{sign_p}{total_profit:,.0f}원</b> ({rate:+.2f}%)",
                f"기간변화  {sign_dc}{daily_change:,.0f}원",
                datetime.now().strftime("%Y-%m-%d %H:%M"),
            ]
            import urllib.request
            req = urllib.request.Request(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data=__import__("json").dumps({
                    "chat_id": chat, "text": "\n".join(lines), "parse_mode": "HTML"
                }).encode(),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=5)
            log("텔레그램 알림 발송 완료")
    except Exception as e:
        log(f"[경고] 텔레그램 발송 실패: {e}")

    log(f"=== 완료 ===")

if __name__ == "__main__":
    main()
