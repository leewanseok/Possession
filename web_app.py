

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
주식 포트폴리오 웹 대시보드
Flask 기반 실시간 주식 현황 조회
"""

import eventlet
eventlet.monkey_patch()

import json
import os
import sys
import threading
import time
import requests
import warnings
from datetime import datetime, time as dtime, timedelta
from queue import Queue, Empty
from flask import Flask, jsonify, render_template, request, Response, stream_with_context
from flask_socketio import SocketIO, emit as socketio_emit

warnings.filterwarnings("ignore")

_BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE      = os.path.join(_BASE_DIR, "config.json")
HISTORY_FILE     = os.path.join(_BASE_DIR, "history.json")
PRICE_CACHE_FILE = os.path.join(_BASE_DIR, "price_cache.json")
CHART_CACHE_DIR      = os.path.join(_BASE_DIR, "chart_cache")
US_HISTORY_FILE      = os.path.join(_BASE_DIR, "us_history.json")
US_PRICE_CACHE_FILE  = os.path.join(_BASE_DIR, "us_price_cache.json")
US_OPEN_ALERT_FILE   = os.path.join(_BASE_DIR, "us_open_alert.json")
US_CLOSE_ALERT_FILE  = os.path.join(_BASE_DIR, "us_close_alert.json")
COIN_HISTORY_FILE     = os.path.join(_BASE_DIR, "coin_history.json")
COMBINED_HISTORY_FILE = os.path.join(_BASE_DIR, "combined_history.json")
os.makedirs(CHART_CACHE_DIR, exist_ok=True)

# NYSE/NASDAQ 주요 휴장일
US_HOLIDAYS = {
    "2025-01-01", "2025-01-20", "2025-02-17", "2025-04-18",
    "2025-05-26", "2025-06-19", "2025-07-04", "2025-09-01",
    "2025-11-27", "2025-12-25",
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03",
    "2026-05-25", "2026-06-19", "2026-07-03", "2026-09-07",
    "2026-11-26", "2026-12-25",
}

# KRX 공휴일 (주말 제외, 평일 휴장일만)
KR_HOLIDAYS = {
    # 2025
    "2025-01-01", "2025-01-28", "2025-01-29", "2025-01-30",
    "2025-05-05", "2025-05-06",
    "2025-06-06", "2025-08-15",
    "2025-10-03", "2025-10-06", "2025-10-07", "2025-10-08", "2025-10-09",
    "2025-12-25", "2025-12-31",
    # 2026
    "2026-01-01", "2026-01-28", "2026-01-29", "2026-01-30",
    "2026-03-02",
    "2026-05-05", "2026-05-25",
    "2026-06-06",
    "2026-08-17",
    "2026-09-24", "2026-09-25",
    "2026-10-03", "2026-10-09",
    "2026-12-25", "2026-12-31",
}

DEFAULT_CONFIG = {
    "api_provider": "naver",
    "refresh_interval": 10,
    "telegram": {
        "bot_token": "",
        "chat_id":   ""
    },
    "anthropic_api_key": "",
    "kis": {
        "app_key":    "YOUR_APP_KEY",
        "app_secret": "YOUR_APP_SECRET",
        "account_no": "YOUR_ACCOUNT_NO",
        "is_paper":   False
    },
    "portfolio": [
        {"code": "005930", "quantity": 10, "avg_price": 72000},
        {"code": "000660", "quantity": 5,  "avg_price": 140000},
        {"code": "035420", "quantity": 8,  "avg_price": 195000}
    ],
    "coin_portfolio": []
}

app = Flask(__name__)
socketio = SocketIO(app, async_mode='eventlet', cors_allowed_origins="*", logger=False, engineio_logger=False)

# ============================================================
#  KIS API
# ============================================================
class KISApi:
    REAL_URL  = "https://openapi.koreainvestment.com:9443"
    PAPER_URL = "https://openapivts.koreainvestment.com:29443"

    def __init__(self, app_key, app_secret, account_no, is_paper=False):
        self.app_key   = app_key
        self.app_secret = app_secret
        raw = account_no.replace("-", "")
        self.cano         = raw[:8]
        self.acnt_prdt_cd = raw[8:] if len(raw) > 8 else "01"
        self.base_url     = self.PAPER_URL if is_paper else self.REAL_URL
        self.is_paper     = is_paper
        self.access_token = None
        # 연결 재사용을 위한 Session
        self._session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20)
        self._session.mount("https://", adapter)

    TOKEN_CACHE = "kis_token.json"

    def authenticate(self):
        # 캐시된 토큰이 유효하면 재사용
        if self._load_cached_token():
            return True
        try:
            resp = requests.post(
                f"{self.base_url}/oauth2/tokenP",
                json={"grant_type": "client_credentials",
                      "appkey": self.app_key, "appsecret": self.app_secret},
                timeout=5
            )
            print(f"[KIS] HTTP {resp.status_code}")
            data = resp.json()
            if resp.status_code != 200:
                msg = data.get("error_description") or data.get("msg1") or str(data)
                print(f"[KIS] 인증 실패 원인: {msg}")
                return False
            self.access_token = data.get("access_token", "")
            if not self.access_token:
                print(f"[KIS] access_token 없음: {data}")
                return False
            # 토큰 캐시 저장 (22시간 후 만료 → 1시간 여유로 자동 갱신)
            expires_at = time.time() + 22 * 3600
            with open(self.TOKEN_CACHE, "w") as f:
                json.dump({"token": self.access_token, "expires_at": expires_at}, f)
            print("[KIS] 인증 성공!")
            return True
        except Exception as e:
            print(f"[KIS] 연결 오류: {e}")
            return False

    def _load_cached_token(self):
        try:
            if not os.path.exists(self.TOKEN_CACHE):
                return False
            with open(self.TOKEN_CACHE) as f:
                d = json.load(f)
            if time.time() < d.get("expires_at", 0):
                self.access_token = d["token"]
                print("[KIS] 캐시 토큰 사용")
                return True
        except Exception:
            pass
        return False

    def _ensure_token(self):
        """토큰이 만료됐으면 재발급. fetch_price_raw 호출 전 사용."""
        try:
            if not os.path.exists(self.TOKEN_CACHE):
                return self.authenticate()
            with open(self.TOKEN_CACHE) as f:
                d = json.load(f)
            if time.time() < d.get("expires_at", 0):
                if not self.access_token:
                    self.access_token = d["token"]
                return True
        except Exception:
            pass
        print("[KIS] 토큰 만료 → 재발급 시도")
        return self.authenticate()

    def _h(self, tr_id):
        return {
            "content-type":  "application/json; charset=utf-8",
            "authorization": f"Bearer {self.access_token}",
            "appkey":        self.app_key,
            "appsecret":     self.app_secret,
            "tr_id":         tr_id,
            "custtype":      "P",
        }

    def _parse_kr_price(self, code, o):
        sign        = o.get("prdy_vrss_sign", "3")
        minus       = sign in ("4", "5")
        change      = int(o.get("prdy_vrss", 0))
        change_rate = float(o.get("prdy_ctrt", 0))
        if minus:
            change      = -abs(change)
            change_rate = -abs(change_rate)
        else:
            change      = abs(change)
            change_rate = abs(change_rate)
        return {
            "code": code, "name": o.get("hts_kor_isnm", ""),
            "current_price": int(o.get("stck_prpr", 0)),
            "change":        change,
            "change_rate":   change_rate,
            "volume":        int(o.get("acml_vol", 0)),
            "open":  int(o.get("stck_oprc", 0)),
            "high":  int(o.get("stck_hgpr", 0)),
            "low":   int(o.get("stck_lwpr", 0)),
        }

    def fetch_price_raw(self, code, mrkt):
        """단일 (code, market) 요청 → 유효한 가격 dict 또는 None (Session 재사용)"""
        if not self._ensure_token():
            return None
        try:
            resp = self._session.get(
                f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-price",
                headers=self._h("FHKST01010100"),
                params={"FID_COND_MRKT_DIV_CODE": mrkt, "FID_INPUT_ISCD": code},
                timeout=3,
                verify=False,
            )
            d = resp.json()
            if d.get("rt_cd") == "0":
                o = d["output"]
                if int(o.get("stck_prpr", 0)) > 0:
                    return self._parse_kr_price(code, o)
        except Exception as e:
            print(f"[KIS] 현재가 오류 ({code}, {mrkt}): {e}")
        return None

    def get_price(self, code):
        return self.fetch_price_raw(code, "NX") or self.fetch_price_raw(code, "J")

    def _fetch_ohlcv_once(self, code, from_date, to_date, period):
        """기간별 OHLCV 단일 요청 (FHKST03010100) — period: D/W/M"""
        try:
            resp = self._session.get(
                f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
                headers=self._h("FHKST03010100"),
                params={
                    "FID_COND_MRKT_DIV_CODE": "J",
                    "FID_INPUT_ISCD":         code,
                    "FID_INPUT_DATE_1":       from_date,
                    "FID_INPUT_DATE_2":       to_date,
                    "FID_PERIOD_DIV_CODE":    period,
                    "FID_ORG_ADJ_PRC":        "0",
                },
                timeout=10, verify=False,
            )
            d = resp.json()
            if d.get("rt_cd") != "0":
                return []
            rows = []
            for o in d.get("output2", []):
                date = o.get("stck_bsop_date", "")
                c    = int(o.get("stck_clpr", 0))
                if not date or c <= 0:
                    continue
                rows.append({
                    "date":   f"{date[:4]}-{date[4:6]}-{date[6:]}",
                    "open":   int(o.get("stck_oprc", c)),
                    "high":   int(o.get("stck_hgpr", c)),
                    "low":    int(o.get("stck_lwpr", c)),
                    "close":  c,
                    "volume": int(o.get("acml_vol", 0)),
                })
            return list(reversed(rows))   # 오래된 날짜 → 최신 순
        except Exception as e:
            print(f"[KIS] OHLCV 오류 ({code}/{period}): {e}")
            return []

    def get_index_daily(self, iscd, from_date, to_date):
        """업종지수 일봉 조회 (FHKUP03500100) — KOSPI: "0001", KOSDAQ: "1001" """
        try:
            resp = self._session.get(
                f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice",
                headers=self._h("FHKUP03500100"),
                params={
                    "FID_COND_MRKT_DIV_CODE": "U",
                    "FID_INPUT_ISCD":         iscd,
                    "FID_INPUT_DATE_1":       from_date,
                    "FID_INPUT_DATE_2":       to_date,
                    "FID_PERIOD_DIV_CODE":    "D",
                },
                timeout=10, verify=False,
            )
            d = resp.json()
            if d.get("rt_cd") != "0":
                return []
            rows = []
            for o in d.get("output2", []):
                date = o.get("stck_bsop_date", "")
                # KIS API 응답 필드: bstp_nmix_prpr(현재가/종가) 우선, 없으면 clpr
                c = float(o.get("bstp_nmix_prpr") or o.get("bstp_nmix_clpr") or 0)
                if not date or c <= 0:
                    continue
                rows.append({
                    "date":  f"{date[:4]}-{date[4:6]}-{date[6:]}",
                    "close": c,
                })
            print(f"[KIS] KOSPI {iscd}: {len(rows)}개 수집")
            return sorted(rows, key=lambda r: r["date"])
        except Exception as e:
            print(f"[KIS] 지수 조회 오류 ({iscd}): {e}")
            return []

    def get_ohlcv(self, code, to_date, period="D"):
        """기간별 OHLCV 병렬 페이징 조회
        D: 20청크 × 145일 → 최대 2000개 / W: 5청크 × 700일 → 최대 500개 / M: 3청크 × 1200일 → 최대 150개
        """
        from concurrent.futures import ThreadPoolExecutor

        # (chunk_calendar_days, n_chunks, max_records)
        cfg = {"D": (145, 20, 2000), "W": (700, 5, 500), "M": (1200, 3, 150)}
        chunk_cal, n_chunks, max_rec = cfg[period]
        end_dt = datetime.strptime(to_date, "%Y%m%d")

        ranges = []
        for i in range(n_chunks):
            t = end_dt - timedelta(days=i * chunk_cal)
            f = t - timedelta(days=chunk_cal - 1)
            ranges.append((f.strftime("%Y%m%d"), t.strftime("%Y%m%d")))

        seen = {}
        with ThreadPoolExecutor(max_workers=min(10, n_chunks)) as ex:
            futs = [ex.submit(self._fetch_ohlcv_once, code, fr, to, period) for fr, to in ranges]
            for fut in futs:
                try:
                    for r in fut.result():
                        seen[r["date"]] = r
                except Exception:
                    pass

        rows = sorted(seen.values(), key=lambda r: r["date"])
        print(f"[KIS] {period}봉 {code}: {len(rows)}개 수집")
        return rows[-max_rec:]

    def fetch_us_price(self, ticker: str, exchange: str = None):
        """해외주식 현재가 조회 (HHDFS00000300) — NAS/NYS/AMS 순 탐색"""
        if not self._ensure_token():
            return None
        exchanges = [exchange] if exchange else ["NAS", "NYS", "AMS"]
        for excd in exchanges:
            try:
                resp = self._session.get(
                    f"{self.base_url}/uapi/overseas-price/v1/quotations/price",
                    headers=self._h("HHDFS00000300"),
                    params={"AUTH": "", "EXCD": excd, "SYMB": ticker.upper()},
                    timeout=3, verify=False,
                )
                d = resp.json()
                if d.get("rt_cd") == "0":
                    o = d.get("output", {})
                    last = float(o.get("last", 0) or 0)
                    if last > 0:
                        rate_raw = float(o.get("rate", 0) or 0)
                        diff_raw = float(o.get("diff", 0) or 0)
                        # diff는 항상 양수 — rate 부호로 방향 결정
                        diff_signed = -abs(diff_raw) if rate_raw < 0 else abs(diff_raw)
                        return {
                            "ticker":        ticker.upper(),
                            "exchange":      excd,
                            "current_price": last,
                            "prev_close":    float(o.get("base", 0) or 0),
                            "change":        diff_signed,
                            "change_rate":   rate_raw,
                            "open":          float(o.get("open", 0) or 0),
                            "high":          float(o.get("high", 0) or 0),
                            "low":           float(o.get("low",  0) or 0),
                            "volume":        int(float(o.get("tvol", 0) or 0)),
                        }
            except Exception as e:
                print(f"[KIS US] {ticker}/{excd} 오류: {e}")
        return None

    def _fetch_us_ohlcv_once(self, ticker: str, exchange: str, to_date: str, gubn: str = "0"):
        """해외주식 기간별시세 단일 호출 (HHDFS76240000) — 최대 ~100개"""
        if not self._ensure_token():
            return []
        try:
            resp = self._session.get(
                f"{self.base_url}/uapi/overseas-price/v1/quotations/dailyprice",
                headers=self._h("HHDFS76240000"),
                params={
                    "AUTH": "", "EXCD": exchange, "SYMB": ticker.upper(),
                    "GUBN": gubn, "BYMD": to_date, "MODP": "1",
                },
                timeout=10, verify=False,
            )
            d = resp.json()
            if d.get("rt_cd") != "0":
                return []
            rows = []
            for o in d.get("output2", []):
                date = o.get("xymd", "")
                c = float(o.get("clos", 0) or 0)
                if not date or c <= 0:
                    continue
                rows.append({
                    "date":   f"{date[:4]}-{date[4:6]}-{date[6:]}",
                    "open":   float(o.get("open", c) or c),
                    "high":   float(o.get("high", c) or c),
                    "low":    float(o.get("lowe", c) or c),
                    "close":  c,
                    "volume": int(float(o.get("tvol", 0) or 0)),
                })
            return list(reversed(rows))
        except Exception as e:
            print(f"[KIS US OHLCV] {ticker} 오류: {e}")
            return []

    def fetch_us_ohlcv(self, ticker: str, exchange: str, to_date: str):
        """하위 호환용 — 단일 호출"""
        return self._fetch_us_ohlcv_once(ticker, exchange, to_date)

    def get_us_ohlcv(self, ticker: str, exchange: str, to_date: str, period: str = "D"):
        """해외주식 기간별시세 병렬 페이징 (한국 get_ohlcv와 동일 구조)
        D: 10청크 × 100일 → 최대 1000개 / W: 5청크 × 700일 / M: 3청크 × 1200일
        GUBN: 0=일, 1=주, 2=월
        """
        from concurrent.futures import ThreadPoolExecutor
        gubn_map = {"D": "0", "W": "1", "M": "2"}
        gubn = gubn_map.get(period, "0")
        # (chunk_calendar_days, n_chunks, max_records)
        cfg = {"D": (100, 10, 1000), "W": (700, 5, 500), "M": (1200, 3, 150)}
        chunk_cal, n_chunks, max_rec = cfg[period]
        end_dt = datetime.strptime(to_date, "%Y%m%d")

        ranges = []
        for i in range(n_chunks):
            t = end_dt - timedelta(days=i * chunk_cal)
            ranges.append(t.strftime("%Y%m%d"))

        seen = {}
        with ThreadPoolExecutor(max_workers=min(10, n_chunks)) as ex:
            futs = [ex.submit(self._fetch_us_ohlcv_once, ticker, exchange, bymd, gubn)
                    for bymd in ranges]
            for fut in futs:
                try:
                    for r in fut.result():
                        seen[r["date"]] = r
                except Exception:
                    pass

        rows = sorted(seen.values(), key=lambda r: r["date"])
        print(f"[KIS US] {period}봉 {ticker}/{exchange}: {len(rows)}개 수집")
        return rows[-max_rec:]

    def get_balance(self):
        try:
            tr_id = "VTTC8434R" if self.is_paper else "TTTC8434R"
            resp = requests.get(
                f"{self.base_url}/uapi/domestic-stock/v1/trading/inquire-balance",
                headers=self._h(tr_id),
                params={
                    "CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd,
                    "AFHR_FLPR_YN": "N", "OFL_YN": "", "INQR_DVSN": "02",
                    "UNPR_DVSN": "01", "FUND_STTL_ICLD_YN": "N",
                    "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "01",
                    "CTX_AREA_FK100": "", "CTX_AREA_NK100": "",
                }, timeout=5,
            )
            d = resp.json()
            if d.get("rt_cd") == "0":
                holdings = []
                for item in d.get("output1", []):
                    qty = int(item.get("hldg_qty", 0))
                    if qty > 0:
                        holdings.append({
                            "code":          item.get("pdno", ""),
                            "name":          item.get("prdt_name", ""),
                            "quantity":      qty,
                            "avg_price":     float(item.get("pchs_avg_pric", 0)),
                            "current_price": int(item.get("prpr", 0)),
                            "eval_amount":   int(item.get("evlu_amt", 0)),
                            "buy_amount":    int(item.get("pchs_amt", 0)),
                            "profit_loss":   int(item.get("evlu_pfls_amt", 0)),
                            "profit_rate":   float(item.get("evlu_pfls_rt", 0)),
                            "change": 0, "change_rate": 0.0,
                        })
                s = (d.get("output2") or [{}])[0]
                return {
                    "holdings": holdings,
                    "total_eval":        int(s.get("scts_evlu_amt", 0)),
                    "total_buy":         int(s.get("pchs_amt_smtl_amt", 0)),
                    "total_profit":      int(s.get("evlu_pfls_smtl_amt", 0)),
                    "total_profit_rate": float(s.get("evlu_erng_rt", 0)),
                    "deposit":           int(s.get("dnca_tot_amt", 0)),
                    "total_asset":       int(s.get("tot_evlu_amt", 0)),
                }
        except Exception as e:
            print(f"[KIS] 잔고 오류: {e}")
        return None


# ============================================================
#  네이버 금융 API
# ============================================================
class NaverStockProvider:
    BASE = "https://m.stock.naver.com/api/stock"
    SRCH = "https://ac.stock.naver.com/ac"
    HDR  = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://m.stock.naver.com/",
    }

    def get_price(self, code):
        try:
            resp = requests.get(f"{self.BASE}/{code}/basic",
                                headers=self.HDR, timeout=5, verify=False)
            if resp.status_code != 200:
                return None
            d = resp.json()

            def to_int(v):
                return int(str(v).replace(",", "") or 0) if v else 0
            def to_float(v):
                try: return float(str(v).replace(",", "").replace("%", "") or 0)
                except: return 0.0

            current = to_int(d.get("closePrice"))
            change  = to_int(d.get("compareToPreviousClosePrice"))
            rate    = to_float(d.get("fluctuationsRatio"))

            sign = d.get("compareToPreviousPrice", {})
            if isinstance(sign, dict) and sign.get("code") in ("2", "4", "하락"):
                change = -abs(change)
                rate   = -abs(rate)

            return {
                "code":          code,
                "name":          d.get("stockName", code),
                "current_price": current,
                "change":        change,
                "change_rate":   rate,
                "volume":        to_int(d.get("accumulatedTradingVolume")),
                "open":          to_int(d.get("openingPrice")),
                "high":          to_int(d.get("highPrice")),
                "low":           to_int(d.get("lowPrice")),
            }
        except Exception as e:
            print(f"[Naver] {code} 오류: {e}")
        return None

    def search(self, keyword):
        try:
            resp = requests.get(
                self.SRCH,
                params={"q": keyword, "target": "index,stock,marketindicator"},
                headers=self.HDR, timeout=5, verify=False
            )
            items = resp.json().get("items", [])
            results = []
            for item in items:
                code = item.get("code", "")
                name = item.get("name", "")
                # 주식 종목만 (KOSPI/KOSDAQ)
                type_code = item.get("typeCode", "")
                if code and name and type_code in ("KOSPI", "KOSDAQ", "KOSPI200"):
                    results.append({"code": code, "name": name})
            return results[:10]
        except Exception as e:
            print(f"[Naver] 검색 오류: {e}")
        return []


# ============================================================
#  앱 초기화
# ============================================================
def load_config():
    if not os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CONFIG, f, ensure_ascii=False, indent=2)
    with open(CONFIG_FILE, encoding="utf-8") as f:
        return json.load(f)

_config   = load_config()
_provider = _config.get("api_provider", "naver")
_kis_api  = None
_naver    = NaverStockProvider()

if _provider == "kis":
    kc = _config.get("kis", {})
    if not kc.get("app_key", "").startswith("YOUR"):
        _kis_api = KISApi(kc["app_key"], kc["app_secret"],
                          kc["account_no"], kc.get("is_paper", False))
        if not _kis_api.authenticate():
            print("[KIS] 인증 실패 → 네이버 API 사용")
            _kis_api = None

def get_price(code):
    if _kis_api:
        return _kis_api.get_price(code)
    return _naver.get_price(code)


# ============================================================
#  환율 (USD/KRW)
# ============================================================
_exrate_cache = {"rate": 1380.0, "ts": 0.0, "source": "기준", "updated_at": ""}
_EXRATE_TTL   = 60  # 60초 (실시간)

def get_usd_krw_rate() -> float:
    """USD/KRW 환율 조회 (60초 캐시)
    1순위: Yahoo Finance 실시간 / 2순위: fawazahmed0 기준환율"""
    now = time.time()
    if now - _exrate_cache["ts"] < _EXRATE_TTL:
        return _exrate_cache["rate"]
    # 1순위: Yahoo Finance 실시간
    try:
        r = requests.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/USDKRW=X?interval=1m&range=1d",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=5, verify=False,
        )
        meta = r.json().get("chart", {}).get("result", [{}])[0].get("meta", {})
        rate = float(meta.get("regularMarketPrice", 0) or 0)
        if rate > 0:
            _exrate_cache["rate"]       = rate
            _exrate_cache["ts"]         = now
            _exrate_cache["source"]     = "실시간"
            _exrate_cache["updated_at"] = datetime.now().strftime("%H:%M")
            print(f"[환율] USD/KRW = {rate:,.2f} (실시간)")
            return rate
    except Exception as e:
        print(f"[환율] Yahoo 조회 오류: {e}")
    # 2순위: fawazahmed0 기준환율
    try:
        r = requests.get(
            "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.min.json",
            timeout=5, verify=False,
        )
        rate = float(r.json().get("usd", {}).get("krw", 0))
        if rate > 0:
            _exrate_cache["rate"]       = rate
            _exrate_cache["ts"]         = now
            _exrate_cache["source"]     = "기준"
            _exrate_cache["updated_at"] = datetime.now().strftime("%H:%M")
            print(f"[환율] USD/KRW = {rate:,.2f} (기준)")
            return rate
    except Exception as e:
        print(f"[환율] 기준환율 조회 오류: {e}")
    return _exrate_cache["rate"]


# ============================================================
#  미국 주식 포트폴리오 빌드
# ============================================================
def build_us_portfolio():
    """미국 주식 포트폴리오 데이터 계산"""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    items   = _config.get("us_portfolio", [])
    ex_rate = get_usd_krw_rate()
    empty   = {
        "holdings": [], "exchange_rate": ex_rate,
        "total_eval_usd": 0, "total_buy_usd": 0, "total_profit_usd": 0,
        "total_eval_krw": 0, "total_buy_krw": 0, "total_profit_krw": 0,
        "total_profit_rate": 0.0,
    }
    if not items:
        return {"us": empty}

    # 캐시 로드
    us_cached = {}
    if os.path.exists(US_PRICE_CACHE_FILE):
        try:
            with open(US_PRICE_CACHE_FILE, encoding="utf-8") as f:
                us_cached = json.load(f)
        except Exception:
            pass

    price_map = {}

    if _kis_api:
        # 미국 거래 시간대 구분 (KST 기준, EDT 써머타임)
        # 프리마켓:   EDT 17:00~22:30 / EST 18:00~23:30
        # 정규장:     EDT 22:30~05:00 / EST 23:30~06:00
        # 애프터마켓: EDT 05:00~09:00 / EST 06:00~10:00
        # 마감:       EDT 09:00~17:00 / EST 10:00~18:00
        # → 모든 시간대 KIS API 실시간 조회 (실패 시 캐시 fallback)
        now_dt    = datetime.now()
        now_t     = now_dt.time()
        dow       = now_dt.weekday()
        today_str = now_dt.strftime("%Y-%m-%d")
        is_dst    = _is_us_dst(now_dt)
        # 월요일 00:00~09:00 KST는 사실상 일요일 연장 (미국장 미개장) → 캐시 사용
        is_weekend_holiday = (dow >= 5) or today_str in US_HOLIDAYS or (dow == 0 and now_t < dtime(9, 0))
        # 프리마켓 구간
        premarket_start = dtime(17, 0)  if is_dst else dtime(18, 0)
        market_open     = dtime(22, 30) if is_dst else dtime(23, 30)
        is_us_premarket = not is_weekend_holiday and (premarket_start <= now_t < market_open)
        # 애프터마켓 구간
        market_close    = dtime(5, 0)  if is_dst else dtime(6, 0)
        aftermarket_end = dtime(9, 0)  if is_dst else dtime(10, 0)
        is_us_aftermarket = not is_weekend_holiday and (market_close <= now_t < aftermarket_end)

        if is_weekend_holiday:
            # 주말/휴장일: 캐시만 사용
            for it in items:
                t = it.get("ticker", "").upper()
                if t in us_cached:
                    price_map[t] = us_cached[t]
        else:
            # 평일: 모든 시간대 KIS API 실시간 조회, 실패 시 캐시 fallback
            pairs = [(it.get("ticker", "").upper(), it.get("exchange") or None) for it in items]
            with ThreadPoolExecutor(max_workers=max(len(pairs), 1)) as ex:
                futs = {ex.submit(_kis_api.fetch_us_price, t, e): t for t, e in pairs}
                for fut in as_completed(futs):
                    ticker = futs[fut]
                    try:
                        r = fut.result()
                        if r:
                            price_map[ticker] = r
                            for it in items:
                                if it.get("ticker", "").upper() == ticker and not it.get("exchange"):
                                    it["exchange"] = r["exchange"]
                    except Exception:
                        pass

    # 캐시 fallback
    for it in items:
        t = it.get("ticker", "").upper()
        if t not in price_map and t in us_cached:
            price_map[t] = us_cached[t]

    holdings = []
    total_eval_usd = total_buy_usd = 0.0
    cfg_updated = False

    for item in items:
        ticker = item.get("ticker", "").upper()
        qty    = float(item.get("quantity", 0))
        avg_p  = float(item.get("avg_price", 0))
        pi     = price_map.get(ticker)
        if pi is None or qty <= 0:
            continue

        if not item.get("name") and pi.get("name"):
            item["name"] = pi["name"]
            cfg_updated = True

        name     = item.get("name") or ticker
        cur      = float(pi.get("current_price", 0))
        eval_usd = round(cur * qty, 4)
        buy_usd  = round(avg_p * qty, 4)
        pl_usd   = round(eval_usd - buy_usd, 4)
        pr       = round(pl_usd / buy_usd * 100, 2) if buy_usd else 0.0

        holdings.append({
            "ticker":        ticker,
            "name":          name,
            "exchange":      pi.get("exchange", item.get("exchange", "")),
            "quantity":      qty,
            "avg_price":     avg_p,
            "current_price": cur,
            "change":        float(pi.get("change", 0)),
            "change_rate":   float(pi.get("change_rate", 0)),
            "open":          float(pi.get("open", 0)),
            "high":          float(pi.get("high", 0)),
            "low":           float(pi.get("low",  0)),
            "volume":        int(pi.get("volume", 0)),
            "eval_usd":      eval_usd,
            "buy_usd":       buy_usd,
            "profit_usd":    pl_usd,
            "profit_rate":   pr,
            "eval_krw":      round(eval_usd  * ex_rate),
            "buy_krw":       round(buy_usd   * ex_rate),
            "profit_krw":    round(pl_usd    * ex_rate),
        })
        total_eval_usd += eval_usd
        total_buy_usd  += buy_usd

    if cfg_updated:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(_config, f, ensure_ascii=False, indent=2)

    # 유효 가격 캐시 저장
    if any(h["current_price"] > 0 for h in holdings):
        try:
            cache = {
                h["ticker"]: {k: h[k] for k in
                    ("ticker","exchange","current_price","change","change_rate","open","high","low","volume")}
                for h in holdings
            }
            with open(US_PRICE_CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(cache, f, ensure_ascii=False)
        except Exception:
            pass

    total_profit_usd  = round(total_eval_usd - total_buy_usd, 4)
    total_profit_rate = round(total_profit_usd / total_buy_usd * 100, 2) if total_buy_usd else 0.0

    return {
        "us": {
            "holdings":          holdings,
            "exchange_rate":     ex_rate,
            "total_eval_usd":    round(total_eval_usd, 2),
            "total_buy_usd":     round(total_buy_usd,  2),
            "total_profit_usd":  round(total_profit_usd, 2),
            "total_eval_krw":    round(total_eval_usd  * ex_rate),
            "total_buy_krw":     round(total_buy_usd   * ex_rate),
            "total_profit_krw":  round(total_profit_usd * ex_rate),
            "total_profit_rate":   total_profit_rate,
            "exchange_rate_source":     _exrate_cache.get("source", "기준"),
            "exchange_rate_updated_at": _exrate_cache.get("updated_at", ""),
            "is_premarket":      is_us_premarket   if _kis_api else False,
            "is_aftermarket":    is_us_aftermarket if _kis_api else False,
            "is_weekend_holiday": is_weekend_holiday,
            "is_regular_market": (not is_weekend_holiday and not is_us_premarket and not is_us_aftermarket
                                  and (now_t >= market_open or now_t < market_close)),
            "is_daytime":        (not is_weekend_holiday and not is_us_premarket and not is_us_aftermarket
                                  and aftermarket_end <= now_t < premarket_start),
        }
    }


def build_portfolio(force_market=None):
    """포트폴리오 데이터 계산 — 모든 (종목×시장) 요청을 단일 flat 풀로 병렬 처리"""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    items = _config.get("portfolio", [])
    # 결과 맵: code → price_info
    price_map = {}

    # 이전 캐시 가격 (장 마감 후 API 500 시 fallback) — 메모리 우선, 없으면 파일
    cached_prices = {}
    with _data_lock:
        for h in _latest_data.get("kr", {}).get("holdings", []):
            cached_prices[h["code"]] = h
    if not cached_prices and os.path.exists(PRICE_CACHE_FILE):
        try:
            with open(PRICE_CACHE_FILE, encoding="utf-8") as f:
                cached_prices = json.load(f)
        except Exception:
            pass

    if _kis_api:
        # 시간대별 시장 선택:
        # 08:00~08:50 : NXT 시세
        # 08:50~09:00 : KRX 동시호가 전용 (NXT 혼입 방지)
        # 09:00~15:30 : KRX 정규장 우선, NXT 병렬
        # 15:30~15:40 : KRX 종가 확정 대기 (NXT 변동 제외)
        # 15:40~20:00 : NXT 시간외 거래
        # 그 외       : KRX 마지막 종가 (캐시)
        now_t = datetime.now().time()
        if force_market:
            markets = (force_market,)   # 강제 시장 지정 (장마감 알림 등)
        else:
            if dtime(8, 0) <= now_t < dtime(8, 50):
                markets = ("NX",)       # NXT 프리마켓
            elif dtime(8, 50) <= now_t < dtime(9, 0):
                markets = ("J",)        # KRX 동시호가 전용 (NXT는 동시호가 미참여)
            elif dtime(9, 0) <= now_t < dtime(15, 30):
                markets = ("J", "NX")   # KRX 정규장 우선, NXT 병렬
            elif dtime(15, 30) <= now_t < dtime(15, 40):
                markets = ("J",)        # KRX 종가 확정 대기 (NXT 변동 제외)
            elif dtime(15, 40) <= now_t < dtime(20, 0):
                markets = ("NX",)       # NXT 시간외 거래
            else:
                markets = ("J",)        # 장 외: KRX 마지막 종가
        # KRX 전용 종목 (NXT 미거래): 시간대 무관하게 항상 "J" 조회
        krx_only_codes = {str(it.get("code","")).zfill(6) for it in items if it.get("krx_only")}
        # 모든 종목 × 시장 요청을 한 번에 제출 (KRX 전용 종목은 항상 "J")
        pairs = []
        for it in items:
            code = str(it.get("code","")).zfill(6)
            if code in krx_only_codes:
                pairs.append((code, "J"))
            else:
                for m in markets:
                    pairs.append((code, m))
        pending = {}   # future → (code, market)
        first_done = {}  # code → result (먼저 도착한 유효값)

        nxt_premarket = dtime(8, 0) <= now_t < dtime(8, 50)  # NXT 프리마켓 구간만
        nxt_success = set()  # NXT 응답 성공한 종목
        # 장 외 시간 여부 (주말 또는 08:00 이전 / 20:00 이후)
        now_dt = datetime.now()
        is_market_closed = (now_dt.weekday() >= 5) or (now_dt.strftime("%Y-%m-%d") in KR_HOLIDAYS) or not (dtime(8, 0) <= now_t < dtime(20, 0))

        # 장이 열리지 않는 날: API 호출 없이 전일 NXT 종가 스냅샷 직접 사용
        nxt_close_map = {}
        if is_market_closed and os.path.exists(NXT_CLOSE_FILE):
            try:
                with open(NXT_CLOSE_FILE, encoding="utf-8") as f:
                    nxt_obj = json.load(f)
                for h in nxt_obj.get("holdings", []):
                    nxt_close_map[h["code"]] = h
            except Exception:
                pass

        if is_market_closed:
            # 장 외: API 호출 생략, 고정 가격 사용
            # price_cache.json 을 파일에서 직접 로드 (메모리 캐시 제외 — API 오염 방지)
            file_cache = {}
            if os.path.exists(PRICE_CACHE_FILE):
                try:
                    with open(PRICE_CACHE_FILE, encoding="utf-8") as f:
                        file_cache = json.load(f)
                except Exception:
                    pass

            for it in items:
                code = str(it.get("code","")).zfill(6)
                if code in nxt_close_map:           # NXT 종가 우선
                    n = nxt_close_map[code]
                    price_map[code] = {
                        "code": code, "name": n.get("name", code),
                        "current_price": n.get("current_price", 0),
                        "change": n.get("change", 0),
                        "change_rate": n.get("change_rate", 0.0),
                    }
                elif code in file_cache:            # price_cache.json fallback
                    c = file_cache[code]
                    price_map[code] = {
                        "code": code, "name": c.get("name",""),
                        "current_price": c.get("current_price", 0),
                        "change": c.get("change", 0),
                        "change_rate": c.get("change_rate", 0.0),
                    }
                else:
                    price_map[code] = None
        else:
            krx_regular = dtime(9, 0) <= now_t < dtime(15, 30)  # KRX 정규장: J 결과 우선

            # ── KIS WebSocket 캐시 우선 사용 (장 중 + WS 활성 시) ──
            if _kr_ws_active and krx_regular:
                ws_miss = []   # WS 캐시 없는 종목
                with _kr_ws_lock:
                    ws_snap = dict(_kr_ws_prices)
                for it in items:
                    code = str(it.get("code", "")).zfill(6)
                    if code in ws_snap:
                        p = ws_snap[code]
                        first_done[code] = {
                            "code":         code,
                            "name":         it.get("name", code),
                            "current_price": p["current_price"],
                            "change":        p["change"],
                            "change_rate":   p["change_rate"],
                        }
                    else:
                        ws_miss.append((code, "J"))  # 미수신 종목만 API로 보완
                if ws_miss:
                    with ThreadPoolExecutor(max_workers=len(ws_miss)) as ex:
                        miss_futs = {ex.submit(_kis_api.fetch_price_raw, c, m): c for c, m in ws_miss}
                        for fut in as_completed(miss_futs):
                            c = miss_futs[fut]
                            try:
                                r = fut.result()
                                if r:
                                    first_done[c] = r
                            except Exception:
                                pass
            else:
                # REST API 폴링 (WS 비활성 또는 비정규장 구간)
                all_results = {}   # code → {"J": result, "NX": result}
                with ThreadPoolExecutor(max_workers=max(len(pairs), 1)) as ex:
                    for code, mrkt in pairs:
                        pending[ex.submit(_kis_api.fetch_price_raw, code, mrkt)] = (code, mrkt)
                    for fut in as_completed(pending):
                        code, mrkt = pending[fut]
                        try:
                            result = fut.result()
                            if result:
                                all_results.setdefault(code, {})[mrkt] = result
                                if mrkt == "NX":
                                    nxt_success.add(code)
                        except Exception:
                            pass
                # KRX 정규장: change/change_rate는 KRX("J") 기준 우선
                for code, res in all_results.items():
                    if krx_regular and "J" in res:
                        first_done[code] = res["J"]
                    else:
                        first_done[code] = res.get("J") or res.get("NX")

            # NXT 전용 구간(프리마켓 08:00~08:50 또는 시간외 15:40~20:00)에서 KRX 종가 맵 로드 — NXT 미거래 종목 fallback용
            nxt_afterhours = dtime(15, 40) <= now_t < dtime(20, 0)
            nxt_only_period = nxt_premarket or nxt_afterhours  # NXT만 조회하는 구간
            krx_close_map = {}
            if nxt_only_period:
                krx_close_file = os.path.join(_BASE_DIR, "krx_close.json")
                if os.path.exists(krx_close_file):
                    try:
                        with open(krx_close_file, encoding="utf-8") as _f:
                            _krx_obj = json.load(_f)
                        for _h in _krx_obj.get("holdings", []):
                            krx_close_map[_h["code"]] = _h
                    except Exception:
                        pass

            for it in items:
                code = str(it.get("code","")).zfill(6)
                pi = first_done.get(code)
                # NXT 전용 구간(프리마켓/시간외): NXT 미거래 종목은 krx_close.json 우선 (in-memory 캐시보다 정확)
                if pi is None and nxt_only_period and code in krx_close_map:
                    _h = krx_close_map[code]
                    pi = {
                        "code": code, "name": _h.get("name", ""),
                        "current_price": _h.get("current_price", 0),
                        "change": _h.get("change", 0),
                        "change_rate": _h.get("change_rate", 0.0),
                    }
                # KIS 실패 시 Naver로 실시간 시세 fallback (장 중 오래된 캐시 방지)
                if pi is None and not is_market_closed:
                    try:
                        npi = _naver.get_price(code)
                        if npi and npi.get("current_price", 0) > 0:
                            pi = npi
                    except Exception:
                        pass
                if pi is None and code in cached_prices:
                    # 장 외 API 실패 시 캐시된 마지막 가격 사용
                    c = cached_prices[code]
                    pi = {
                        "code": code, "name": c.get("name",""),
                        "current_price": c.get("current_price", 0),
                        "change": c.get("change", 0),
                        "change_rate": c.get("change_rate", 0.0),
                    }
                # pi 여전히 None → nxt_close.json 최후 fallback
                if pi is None and os.path.exists(NXT_CLOSE_FILE):
                    try:
                        with open(NXT_CLOSE_FILE, encoding="utf-8") as _f:
                            _nxt_obj = json.load(_f)
                        for _h in _nxt_obj.get("holdings", []):
                            if _h["code"] == code:
                                pi = {
                                    "code": code, "name": _h.get("name", ""),
                                    "current_price": _h.get("current_price", 0),
                                    "change": 0, "change_rate": 0.0,
                                }
                                break
                    except Exception:
                        pass
                # NXT 프리마켓 구간에서 NXT 미거래 종목 → 전일 종가 유지, 등락 0으로 처리
                if pi and nxt_premarket and code not in nxt_success:
                    pi = dict(pi)
                    pi["change"] = 0
                    pi["change_rate"] = 0.0
                price_map[code] = pi
    else:
        # Naver fallback
        for it in items:
            code = str(it.get("code","")).zfill(6)
            price_map[code] = _naver.get_price(code)

    holdings = []
    total_eval = total_buy = 0
    cfg_updated = False

    for item in items:
        code  = str(item.get("code","")).zfill(6)
        qty   = int(item.get("quantity", 0))
        avg_p = float(item.get("avg_price", 0))
        pi    = price_map.get(code)
        if pi is None:
            continue
        if qty <= 0:
            continue
        existing_name = item.get("name", "")
        api_name      = (pi.get("name") or "") if pi else ""
        # name이 비어있거나 코드 자체인 경우 갱신
        need_update = not existing_name or existing_name == code
        if need_update and api_name:
            item["name"] = api_name
            cfg_updated = True
            existing_name = api_name
        name     = existing_name or api_name or code
        cur      = pi.get("current_price", 0)
        eval_amt = round(cur * qty, 4)
        buy_amt  = round(avg_p * qty, 4)
        pl       = round(eval_amt - buy_amt, 4)
        pr       = round(pl / buy_amt * 100, 2) if buy_amt else 0.0
        holdings.append({
            "code": code, "name": name, "market": "KR",
            "quantity": qty, "avg_price": avg_p, "current_price": cur,
            "change":      pi.get("change", 0),
            "change_rate": pi.get("change_rate", 0.0),
            "open":        pi.get("open", 0),
            "high":        pi.get("high", 0),
            "low":         pi.get("low", 0),
            "volume":      pi.get("volume", 0),
            "eval_amount": eval_amt, "buy_amount": buy_amt,
            "profit_loss": pl, "profit_rate": pr,
        })
        total_eval += eval_amt
        total_buy  += buy_amt

    if cfg_updated:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(_config, f, ensure_ascii=False, indent=2)

    # 유효 가격이 있으면 파일에 저장 (재시작 후 장 마감 fallback 용)
    if any(h["current_price"] > 0 for h in holdings):
        try:
            cache = {h["code"]: h for h in holdings}
            with open(PRICE_CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(cache, f, ensure_ascii=False)
        except Exception:
            pass

    total_profit = round(total_eval - total_buy, 4)
    return {
        "kr": {
            "holdings": holdings,
            "total_eval":        round(total_eval, 4),
            "total_buy":         round(total_buy, 4),
            "total_profit":      total_profit,
            "total_profit_rate": round(total_profit / total_buy * 100, 2) if total_buy else 0.0,
        }
    }


# ============================================================
#  미국 주식 히스토리
# ============================================================
def _load_us_history():
    if os.path.exists(US_HISTORY_FILE):
        try:
            with open(US_HISTORY_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"records": []}

def _save_us_daily_snapshot(portfolio_data: dict):
    """미국 정규장 마감 후(KST 05:05~10:00) 직전 미국 거래일 기준으로 저장
    - 날짜: KST 당일이 아닌 직전 미국 거래일 (배치와 동일 기준)
    - 서머타임 05:00 KST / 일반 06:00 KST 마감 → 05:05부터 커버
    - 미국 금요일 마감은 KST 토요일 → weekday 제한 없음"""
    now = datetime.now()
    if not (dtime(5, 5) <= now.time() < dtime(10, 0)):
        return

    us = portfolio_data.get("us", {})
    if not us.get("holdings"):
        return

    # 직전 미국 거래일 = 배치와 동일 기준
    dt = now - timedelta(days=1)
    while True:
        s = dt.strftime("%Y-%m-%d")
        if dt.weekday() < 5 and s not in US_HOLIDAYS:
            us_date = s
            break
        dt -= timedelta(days=1)

    history = _load_us_history()
    if any(r["date"] == us_date for r in history.get("records", [])):
        return

    total_eval_usd   = us.get("total_eval_usd", 0)
    ex_rate          = us.get("exchange_rate", _exrate_cache["rate"])
    prev_rec         = history["records"][0] if history["records"] else {}
    prev_eval_usd    = prev_rec.get("total_eval_usd", 0)
    prev_eval_krw    = prev_rec.get("total_eval_krw", 0)
    daily_chg_usd    = round(total_eval_usd - prev_eval_usd, 2) if prev_eval_usd else 0.0
    total_eval_krw   = round(total_eval_usd * ex_rate)
    total_buy_krw    = round(us.get("total_buy_usd", 0) * ex_rate)
    total_profit_krw = round(us.get("total_profit_usd", 0) * ex_rate)
    daily_chg_krw    = round(total_eval_krw - prev_eval_krw) if prev_eval_krw else 0

    record = {
        "date":               us_date,
        "total_buy_usd":      round(us.get("total_buy_usd",    0), 2),
        "total_eval_usd":     round(total_eval_usd, 2),
        "total_profit_usd":   round(us.get("total_profit_usd", 0), 2),
        "total_profit_rate":  round(us.get("total_profit_rate", 0), 2),
        "daily_change_usd":   daily_chg_usd,
        "exchange_rate":      ex_rate,
        "total_buy_krw":      total_buy_krw,
        "total_eval_krw":     total_eval_krw,
        "total_profit_krw":   total_profit_krw,
        "daily_change_krw":   daily_chg_krw,
    }
    history["records"].insert(0, record)
    with open(US_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    print(f"[US 히스토리] {us_date} 저장 완료")
    return True


# ============================================================
#  텔레그램 알림
# ============================================================
_tg_session = requests.Session()

def _tg_history_save(text: str, msg_type: str):
    """텔레그램 발송 내역 파일 저장 (최대 200건)"""
    entry = {
        "ts":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "type": msg_type,
        "text": text,
    }
    with _tg_history_lock:
        try:
            if os.path.exists(TG_HISTORY_FILE):
                with open(TG_HISTORY_FILE, encoding="utf-8") as f:
                    hist = json.load(f)
            else:
                hist = {"messages": []}
            hist["messages"].insert(0, entry)
            hist["messages"] = hist["messages"][:_TG_HISTORY_MAX]
            with open(TG_HISTORY_FILE, "w", encoding="utf-8") as f:
                json.dump(hist, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[TG 히스토리] 저장 실패: {e}")


def _tg_send(text: str, msg_type: str = ""):
    """텔레그램 메시지 전송 + 히스토리 저장"""
    _tg_history_save(text, msg_type)
    cfg = _config.get("telegram", {})
    token = cfg.get("bot_token", "").strip()
    chat  = cfg.get("chat_id",   "").strip()
    if not token or not chat:
        return
    try:
        _tg_session.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat, "text": text, "parse_mode": "HTML"},
            timeout=5, verify=False,
        )
    except Exception as e:
        print(f"[텔레그램] 전송 실패: {e}")


def _tg_send_photo(image_bytes: bytes, caption: str = ""):
    """텔레그램 이미지 전송 (sendPhoto)"""
    cfg = _config.get("telegram", {})
    token = cfg.get("bot_token", "").strip()
    chat  = cfg.get("chat_id",   "").strip()
    if not token or not chat:
        return
    try:
        _tg_session.post(
            f"https://api.telegram.org/bot{token}/sendPhoto",
            data={"chat_id": chat, "caption": caption, "parse_mode": "HTML"},
            files={"photo": ("chart.png", image_bytes, "image/png")},
            timeout=15, verify=False,
        )
    except Exception as e:
        print(f"[텔레그램] 이미지 전송 실패: {e}")


def _ai_analyze_stock(code: str, name: str) -> str:
    """Claude API로 종목 분석 요청 → 분석 텍스트 반환"""
    # config.json에서 직접 읽어 서버 재시작 없이도 반영
    try:
        with open(CONFIG_FILE, encoding="utf-8") as _cf:
            api_key = json.load(_cf).get("anthropic_api_key", "").strip()
    except Exception:
        api_key = _config.get("anthropic_api_key", "").strip()
    if not api_key:
        return "anthropic_api_key가 config.json에 설정되지 않았습니다.\nconsole.anthropic.com 에서 API 키를 발급받아 입력해주세요."
    try:
        import anthropic as _anthropic

        # 현재가 + OHLCV 조회
        pi = _kis_api.fetch_price_raw(code, "J") or _kis_api.fetch_price_raw(code, "NX")
        price_info = ""
        if pi:
            chg   = pi.get("change", 0)
            rate  = pi.get("change_rate", 0.0)
            arrow = "▲" if chg >= 0 else "▼"
            price_info = (
                f"현재가: {pi.get('current_price',0):,}원  {arrow}{abs(chg):,} ({rate:+.2f}%)\n"
                f"시가: {pi.get('open',0):,}  고가: {pi.get('high',0):,}  "
                f"저가: {pi.get('low',0):,}  거래량: {pi.get('volume',0):,}"
            )

        # 최근 20일 OHLCV
        to_dt   = datetime.now()
        from_dt = to_dt - timedelta(days=40)
        rows = _kis_api._fetch_ohlcv_once(code, from_dt.strftime("%Y%m%d"), to_dt.strftime("%Y%m%d"), "D")
        ohlcv_text = ""
        if rows:
            lines = ["날짜        시가      고가      저가      종가      거래량"]
            for r in rows[-20:]:
                lines.append(
                    f"{r['date']}  {r['open']:>8,}  {r['high']:>8,}  "
                    f"{r['low']:>8,}  {r['close']:>8,}  {r['volume']:>12,}"
                )
            ohlcv_text = "\n".join(lines)

        # 최근 뉴스 5건
        news_items = _fetch_stock_news(code)
        news_text = ""
        if news_items:
            news_text = "\n".join(
                f"- [{n['date']}] {n['title']} ({n['source']})"
                for n in news_items[:5]
            )

        # config.json의 사용자 정의 프롬프트 템플릿 사용
        try:
            with open(CONFIG_FILE, encoding="utf-8") as _cf:
                prompt_template = json.load(_cf).get("analysis_prompt", "")
        except Exception:
            prompt_template = _config.get("analysis_prompt", "")

        if not prompt_template:
            prompt_template = (
                "다음은 {name}({code}) 종목의 최신 데이터입니다.\n\n"
                "[현재 시세]\n{price_info}\n\n"
                "[최근 20일 OHLCV]\n{ohlcv_text}\n\n"
                "[최근 뉴스]\n{news_text}\n\n"
                "위 데이터를 바탕으로 주가 흐름, 기술적 분석, 뉴스 심리, 주의사항을 간결하게 분석해주세요."
            )

        prompt = prompt_template.format(
            name=name, code=code,
            price_info=price_info,
            ohlcv_text=ohlcv_text,
            news_text=news_text,
        )

        client = _anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text

    except Exception as e:
        print(f"[AI 분석] 오류: {e}")
        return f"AI 분석 중 오류가 발생했습니다: {e}"


def _build_chart_image(code: str, name: str, days: int = 60) -> bytes | None:
    """OHLCV 데이터로 캔들차트 이미지 생성 → PNG bytes 반환 (웹 차트 스타일)"""
    try:
        import io as _io
        import pandas as pd
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.gridspec as gridspec
        import matplotlib.patches as mpatches
        import matplotlib.ticker as mticker
        import matplotlib.font_manager as _fm

        # ── 한글 폰트 ──
        _font_file = None
        for _fp in [r"C:\Windows\Fonts\malgun.ttf", "/System/Library/Fonts/AppleGothic.ttf"]:
            if os.path.exists(_fp):
                _fm.fontManager.addfont(_fp)
                _font_file = _fp
                break
        if not _font_file:
            for _f in _fm.fontManager.ttflist:
                if any(k in _f.name for k in ("Malgun", "Noto Sans KR", "NanumGothic", "Gulim")):
                    _font_file = _f.fname
                    break
        def fp(size):
            return _fm.FontProperties(fname=_font_file, size=size) if _font_file else \
                   _fm.FontProperties(size=size)

        # ── OHLCV 데이터 ──
        to_dt   = datetime.now()
        from_dt = to_dt - timedelta(days=days * 2)
        rows = _kis_api._fetch_ohlcv_once(
            code, from_dt.strftime("%Y%m%d"), to_dt.strftime("%Y%m%d"), "D")
        if not rows:
            return None
        rows = rows[-days:]
        df = pd.DataFrame(rows)
        df["Date"] = pd.to_datetime(df["date"])
        df = df.set_index("Date")[["open","high","low","close","volume"]].astype(float)
        n = len(df)

        # ── 색상 상수 ──
        BG, BG2        = "#131620", "#1a1d2b"
        GRID, BORDER   = "#2e3250", "#2e3250"
        TEXT, TEXT2    = "#e0e4f0", "#8892a4"
        UP, DN         = "#f04e5e", "#3b9eff"

        today = df.iloc[-1]
        prev  = df.iloc[-2] if n >= 2 else today
        chg   = today["close"] - prev["close"]
        rate  = chg / prev["close"] * 100 if prev["close"] else 0
        arrow = "▲" if chg >= 0 else "▼"
        v_col = UP if chg >= 0 else DN

        # ── Figure 레이아웃 ──
        # 행: [info bar] [캔들] [거래량]
        fig = plt.figure(figsize=(10, 7.5), facecolor=BG)
        gs  = gridspec.GridSpec(
            3, 1, height_ratios=[0.09, 0.62, 0.22],
            hspace=0.0,
            left=0.03, right=0.88, top=0.93, bottom=0.07,
        )
        ax_info = fig.add_subplot(gs[0])
        ax_c    = fig.add_subplot(gs[1])
        ax_v    = fig.add_subplot(gs[2], sharex=ax_c)

        # 공통 축 스타일
        for ax in [ax_c, ax_v]:
            ax.set_facecolor(BG)
            ax.tick_params(colors=TEXT2, labelsize=8, length=3)
            for sp in ax.spines.values():
                sp.set_color(BORDER)
            ax.grid(True, color=GRID, linestyle=":", linewidth=0.5, alpha=0.8)
            ax.yaxis.set_label_position("right")
            ax.yaxis.tick_right()

        # info bar 스타일
        ax_info.set_facecolor(BG2)
        ax_info.axis("off")
        for sp in ax_info.spines.values():
            sp.set_visible(False)

        # ── 캔들스틱 그리기 ──
        w = 0.5
        price_range = df["high"].max() - df["low"].min()
        min_body    = price_range * 0.003  # 도지 최소 높이
        for i in range(n):
            r     = df.iloc[i]
            is_up = r["close"] >= r["open"]
            col   = UP if is_up else DN
            # 심지
            ax_c.plot([i, i], [r["low"], r["high"]], color=col, linewidth=0.8, zorder=2)
            # 몸통
            b_lo = min(r["open"], r["close"])
            b_h  = max(abs(r["close"] - r["open"]), min_body)
            ax_c.add_patch(mpatches.Rectangle(
                (i - w / 2, b_lo), w, b_h,
                facecolor=col, edgecolor=col, linewidth=0, zorder=3,
            ))
            # 거래량 바
            ax_v.bar(i, r["volume"], width=w, color=col, alpha=0.85, zorder=2)

        # ── 이동평균선 ──
        xi = list(range(n))
        ma5  = df["close"].rolling(5).mean()
        ma20 = df["close"].rolling(20).mean()
        if n >= 5:
            ax_c.plot(xi, ma5,  color="#f5a623", linewidth=1.4, label="MA5",  zorder=4)
        if n >= 20:
            ax_c.plot(xi, ma20, color="#a29bfe", linewidth=1.4, label="MA20", zorder=4)

        # ── 범례 ──
        leg_handles = []
        if n >= 5:
            leg_handles.append(plt.Line2D([0],[0], color="#f5a623", lw=1.4, label="MA5"))
        if n >= 20:
            leg_handles.append(plt.Line2D([0],[0], color="#a29bfe", lw=1.4, label="MA20"))
        if leg_handles:
            ax_c.legend(handles=leg_handles, loc="upper left", prop=fp(8),
                        facecolor=BG2, edgecolor=BORDER, labelcolor=TEXT, framealpha=0.85)

        # ── X축 날짜 ──
        tick_step  = max(1, n // 7)
        tick_pos   = list(range(0, n, tick_step))
        tick_labels = [df.index[i].strftime("%m-%d") for i in tick_pos]
        ax_v.set_xticks(tick_pos)
        ax_v.set_xticklabels(tick_labels, fontproperties=fp(8), color=TEXT2)
        plt.setp(ax_c.get_xticklabels(), visible=False)
        ax_c.set_xlim(-0.8, n - 0.2)

        # ── Y축 포맷 ──
        ax_c.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
        ax_v.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda v, _: f"{v/1e6:.0f}M" if v >= 1e6 else f"{int(v):,}"))
        for ax in [ax_c, ax_v]:
            ax.tick_params(axis="y", colors=TEXT2, labelsize=8)

        # 거래량 y축 레이블 — info bar에 이미 표시되므로 생략

        # ── 타이틀 ──
        fig.text(0.455, 0.97, f"{name}({code})  최근 {n}일",
                 fontproperties=fp(13), color=TEXT,
                 ha="center", va="top", transform=fig.transFigure)

        # ── Info bar: 현재가 | 전일대비 | 시 고 저 종 | 거래량 ──
        # ax_info 내부 좌표(0~1)로 배치
        def itext(x, s, color, size=9, ha="left"):
            ax_info.text(x, 0.5, s, fontproperties=fp(size),
                         color=color, ha=ha, va="center",
                         transform=ax_info.transAxes)

        # 현재가 (크게)
        itext(0.01, f"{int(today['close']):,}", v_col, size=13)
        # 전일대비
        itext(0.13, "전일대비", TEXT2, size=8)
        itext(0.22, f"{arrow}{abs(chg):,.0f} ({rate:+.2f}%)", v_col, size=9)
        # 구분
        itext(0.40, "|", BORDER, size=9)
        # 시 고 저 종
        ohlc = [("시", today["open"]), ("고", today["high"]),
                ("저", today["low"]),  ("종", today["close"])]
        ox = 0.43
        for lbl, val in ohlc:
            itext(ox,        lbl,           TEXT2, size=8)
            itext(ox + 0.03, f"{int(val):,}", v_col, size=9)
            ox += 0.115
        # 구분 + 거래량 (우측 정렬 단일 텍스트로 겹침 방지)
        itext(0.905, "|", BORDER, size=9)
        itext(0.99, f"거래량  {int(today['volume']):,}", TEXT2, size=8.5, ha="right")

        # info bar 하단 구분선
        ax_info.axhline(y=0, color=BORDER, linewidth=0.8, xmin=0, xmax=1)

        buf = _io.BytesIO()
        fig.savefig(buf, dpi=130, bbox_inches="tight", facecolor=BG)
        plt.close(fig)
        buf.seek(0)
        return buf.read()
    except Exception as e:
        print(f"[차트] 이미지 생성 오류: {e}")
        import traceback; traceback.print_exc()
        return None


# ── 알림 상태 추적 ──
# { code: { "pct_last_sent": {5: timestamp, -5: timestamp, ...},
#            "high_sent": date_str, "low_sent": date_str } }
_ALERT_THRESHOLDS = [5, 10, 15, 20, 25, 30]   # % 단위
_ALERT_COOLDOWN   = 3600                        # 같은 임계값 재발송 최소 간격(초)
CLOSE_ALERT_FILE     = os.path.join(_BASE_DIR, "close_alert.json")
OPEN_ALERT_FILE      = os.path.join(_BASE_DIR, "open_alert.json")
ALERT_STATE_FILE     = os.path.join(_BASE_DIR, "alert_state.json")
NXT_CLOSE_ALERT_FILE = os.path.join(_BASE_DIR, "nxt_close_alert.json")
TG_HISTORY_FILE      = os.path.join(_BASE_DIR, "tg_history.json")
_TG_HISTORY_MAX      = 200
_tg_history_lock     = threading.Lock()
NXT_CLOSE_FILE       = os.path.join(_BASE_DIR, "nxt_close.json")

def _load_alert_state() -> dict:
    try:
        with open(ALERT_STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_alert_state():
    try:
        with open(ALERT_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(_alert_state, f, ensure_ascii=False)
    except Exception as e:
        print(f"[알림] alert_state 저장 실패: {e}")

_alert_state: dict = _load_alert_state()
_alert_state_lock = threading.Lock()  # _alert_state 멀티스레드 동시 접근 방지

def _load_open_alert_date() -> str:
    try:
        with open(OPEN_ALERT_FILE, encoding="utf-8") as f:
            return json.load(f).get("sent_date", "")
    except Exception:
        return ""

def _save_open_alert_date(date: str):
    try:
        with open(OPEN_ALERT_FILE, "w", encoding="utf-8") as f:
            json.dump({"sent_date": date}, f)
    except Exception as e:
        print(f"[알림] open_alert 저장 실패: {e}")



def _load_close_alert_date() -> str:
    try:
        with open(CLOSE_ALERT_FILE, encoding="utf-8") as f:
            return json.load(f).get("sent_date", "")
    except Exception:
        return ""

def _save_close_alert_date(date: str):
    try:
        with open(CLOSE_ALERT_FILE, "w", encoding="utf-8") as f:
            json.dump({"sent_date": date}, f)
    except Exception as e:
        print(f"[알림] close_alert 저장 실패: {e}")


def _load_nxt_close_alert_date() -> str:
    try:
        with open(NXT_CLOSE_ALERT_FILE, encoding="utf-8") as f:
            return json.load(f).get("sent_date", "")
    except Exception:
        return ""

def _save_nxt_close_alert_date(date: str):
    try:
        with open(NXT_CLOSE_ALERT_FILE, "w", encoding="utf-8") as f:
            json.dump({"sent_date": date}, f)
    except Exception as e:
        print(f"[알림] nxt_close_alert 저장 실패: {e}")



def _check_rate_alerts(holdings: list):
    """전일대비 등락률 5%·10%·15%… 임계값 최초 도달 시 1회 알림.
    임계값 아래로 내려갔다가 재돌파 시 직전 발송으로부터 1시간 경과 후에만 재발송."""
    now_ts = time.time()
    # 발송할 메시지 목록을 먼저 수집 (락 외부에서 발송하여 락 보유 최소화)
    to_send = []

    with _alert_state_lock:
        for h in holdings:
            code   = h.get("code", "")
            name   = h.get("name", code)
            rate   = h.get("change_rate", 0.0)
            price  = h.get("current_price", 0)
            change = h.get("change", 0)
            buy_price = h.get("avg_price", 0)
            eval_pnl  = h.get("profit_loss", 0)
            eval_rate = h.get("profit_rate", 0.0)

            if code not in _alert_state:
                _alert_state[code] = {"pct_last_sent": {}, "pct_triggered": {}}
            state = _alert_state[code]
            if "pct_triggered" not in state:
                state["pct_triggered"] = {}

            for thr in _ALERT_THRESHOLDS:
                for sign, label in [(1, "상승"), (-1, "하락")]:
                    key = str(sign * thr)
                    above = (sign == 1 and rate >= thr) or (sign == -1 and rate <= -thr)

                    if not above:
                        state["pct_triggered"].pop(key, None)
                        continue

                    if state["pct_triggered"].get(key, False):
                        continue

                    last_sent = state["pct_last_sent"].get(key, 0)
                    if now_ts - last_sent < _ALERT_COOLDOWN:
                        state["pct_triggered"][key] = True
                        continue

                    # 발송 결정 — 락 내에서 플래그 선점 후 메시지 수집
                    state["pct_last_sent"][key] = now_ts
                    state["pct_triggered"][key] = True

                    pnl_line = ""
                    if buy_price:
                        pnl_line = f"\n평가손익  <b>{'+' if eval_pnl>=0 else ''}{round(eval_pnl):,}원</b> ({eval_rate:+.2f}%)"
                    msg = (
                        f"<b>[{name}] 전일대비 {thr}% {label}</b>\n"
                        f"{'─' * 20}\n"
                        f"현재가   <b>{price:,}원</b>\n"
                        f"전일대비  {'+' if change>=0 else ''}{change:,}원 ({rate:+.2f}%)"
                        f"{pnl_line}\n"
                        f"{datetime.now().strftime('%Y-%m-%d %H:%M')}"
                    )
                    to_send.append((name, thr, label, msg))

        if to_send:
            _save_alert_state()

    # 락 해제 후 텔레그램 발송 (네트워크 I/O는 락 밖에서)
    for name, thr, label, msg in to_send:
        _tg_send(msg, msg_type="rate_alert")
        print(f"[알림] {name} {thr}% {label} → 텔레그램 발송")


def _check_high_low_alerts(holdings: list):
    """3개월 신고가·신저가 알림"""
    if not _kis_api:
        return
    today   = datetime.now().strftime("%Y-%m-%d")
    now_str = datetime.now().strftime("%H:%M")

    # 1단계: 오늘 OHLCV 조회가 필요한 종목 선별 (락 내에서 hl_checked 선점)
    to_check = []
    with _alert_state_lock:
        for h in holdings:
            code  = h.get("code", "")
            price = h.get("current_price", 0)
            if not price:
                continue
            if code not in _alert_state:
                _alert_state[code] = {"pct_last_sent": {}}
            state = _alert_state[code]
            if state.get("hl_checked") == today:
                continue
            state["hl_checked"] = today   # 선점: 다른 스레드가 중복 API 호출 못 함
            to_check.append((code, h))

    # 2단계: API 호출 (락 밖에서, 느린 작업)
    for code, h in to_check:
        name  = h.get("name", code)
        price = h.get("current_price", 0)
        rate  = h.get("change_rate", 0.0)
        change = h.get("change", 0)

        to_dt = datetime.now().strftime("%Y%m%d")
        rows  = _kis_api._fetch_ohlcv_once(code,
                    (datetime.now() - timedelta(days=95)).strftime("%Y%m%d"),
                    to_dt, "D")
        if len(rows) < 5:
            continue

        prev     = rows[:-1]
        max_high = max(r["high"] for r in prev)
        min_low  = min(r["low"]  for r in prev)

        # 3단계: 발송 결정 (락 내에서 플래그 선점, 메시지 수집)
        to_send = []
        with _alert_state_lock:
            state = _alert_state.get(code, {})
            if price > max_high and state.get("high_sent") != today:
                state["high_sent"] = today
                _alert_state[code] = state
                diff = price - max_high
                to_send.append(("high", f"<b>[{name}] 3개월 신고가 돌파</b>\n"
                    f"{'─' * 20}\n"
                    f"현재가   <b>{price:,}원</b>\n"
                    f"직전 고가  {max_high:,}원  (+{diff:,}원)\n"
                    f"전일대비  {'+' if change>=0 else ''}{change:,}원 ({rate:+.2f}%)\n"
                    f"{now_str}"))
            if price < min_low and state.get("low_sent") != today:
                state["low_sent"] = today
                _alert_state[code] = state
                diff = min_low - price
                to_send.append(("low", f"<b>[{name}] 3개월 신저가 하락</b>\n"
                    f"{'─' * 20}\n"
                    f"현재가   <b>{price:,}원</b>\n"
                    f"직전 저가  {min_low:,}원  (-{diff:,}원)\n"
                    f"전일대비  {'+' if change>=0 else ''}{change:,}원 ({rate:+.2f}%)\n"
                    f"{now_str}"))
            if to_send:
                _save_alert_state()

        # 4단계: 텔레그램 발송 (락 밖에서)
        for kind, msg in to_send:
            _tg_send(msg, msg_type="hl_alert")
            label = "3개월 신고가" if kind == "high" else "3개월 신저가"
            print(f"[알림] {name} {label}")


def _check_close_alert():
    """장 마감 후 보유종목 종합 알림 - KRX 종가 강제 조회 (하루 1번)"""
    global _close_alert_sent_date
    now = datetime.now()
    if now.weekday() >= 5:
        return
    if now.strftime("%Y-%m-%d") in KR_HOLIDAYS:
        return
    if now.time() < dtime(15, 36):  # 종가 스냅샷(15:35) 저장 후 발송
        return
    today = now.strftime("%Y-%m-%d")
    # 인메모리 1차 확인 (락 없이 빠르게)
    if _close_alert_sent_date == today:
        return

    # 오늘 스냅샷이 저장된 후에만 발송
    history = _load_history()
    if not any(r.get("date") == today for r in history.get("records", [])):
        return

    # krx_close.json 에서 KRX 종가 데이터 로드 (API 재조회 없이 정확한 종가 사용)
    krx_close_file = os.path.join(_BASE_DIR, "krx_close.json")
    try:
        with open(krx_close_file, encoding="utf-8") as f:
            krx = json.load(f)
        if krx.get("date") != today:
            return  # 오늘 KRX 종가 데이터 없음 (아직 15:35 미도달)
    except Exception:
        return

    # 모든 데이터 준비 완료 → 락 획득 후 최종 중복 체크 및 플래그 선점
    with _close_alert_lock:
        if _close_alert_sent_date == today:
            return
        if _load_close_alert_date() == today:
            _close_alert_sent_date = today
            return
        _close_alert_sent_date = today

    holdings     = krx.get("holdings", [])
    if not holdings:
        return

    total_eval   = krx.get("total_eval", 0)
    total_profit = krx.get("total_profit", 0)
    total_rate   = krx.get("total_profit_rate", 0.0)
    daily_chg    = krx.get("daily_change", 0)

    SEP = "─" * 20
    sorted_h = sorted(holdings, key=lambda x: x.get("change_rate", 0), reverse=True)

    lines = [
        f"<b>장 마감 포트폴리오 리포트 ({today})</b>",
        SEP,
    ]

    for h in sorted_h:
        name   = h.get("name", h.get("code", ""))
        code   = h.get("code", "")
        price  = h.get("current_price", 0)
        rate   = h.get("change_rate", 0.0)
        chg    = h.get("change", 0)
        prate  = h.get("profit_rate", 0.0)
        pnl    = h.get("profit_loss", 0)
        arrow  = "▲" if chg >= 0 else "▼"
        lines.append(
            f"<b>{name}({code})</b>\n"
            f"- 현재가 : {price:,}원\n"
            f"- 전일대비 : {arrow}{abs(chg):,}원 ({rate:+.2f}%)\n"
            f"- 수 익 률 : {'+' if prate>=0 else ''}{prate:.2f}%\n"
            f"- 평가손익 : {'+' if pnl>=0 else ''}{round(pnl):,}원"
        )

    sign_profit = "+" if total_profit >= 0 else ""
    sign_daily  = "+" if daily_chg >= 0 else ""
    bullet = "\u25aa\ufe0f"
    won = "\uc6d0"
    lines += [
        SEP,
        f"{bullet} \ucd1d \ud3c9\uac00\uae08\uc561: <b>{round(total_eval):,}{won}</b>",
        f"{bullet} \ucd1d \ud3c9\uac00\uc190\uc775: <b>{sign_profit}{round(total_profit):,}{won}</b> ({total_rate:+.2f}%)",
        f"{bullet} \uc804\uc77c \ub300\ube44:   <b>{sign_daily}{daily_chg:,}{won}</b>",
    ]

    _save_close_alert_date(today)  # 전송 전에 저장 — 실패해도 재발송 방지
    _tg_send("\n".join(lines), msg_type="close_report")
    print("[알림] 장 마감 텔레그램 발송")


def _check_nxt_close_alert():
    """NXT 장 마감(20:00) 후 20:05에 보유종목 종합 알림 (하루 1번)"""
    global _nxt_close_alert_sent_date
    now = datetime.now()
    if now.weekday() >= 5:
        return
    if now.strftime("%Y-%m-%d") in KR_HOLIDAYS:
        return
    if not (dtime(20, 5) <= now.time() < dtime(23, 59)):
        return
    today = now.strftime("%Y-%m-%d")
    # 인메모리 1차 확인 (락 없이 빠르게)
    if _nxt_close_alert_sent_date == today:
        return

    # nxt_close.json 에서 NXT 종가 스냅샷 로드 (19:55~20:00 저장)
    try:
        with open(NXT_CLOSE_FILE, encoding="utf-8") as f:
            nxt = json.load(f)
        if nxt.get("date") != today:
            return  # 오늘 NXT 종가 데이터 없음
    except Exception:
        return

    holdings     = nxt.get("holdings", [])
    if not holdings:
        return

    # 모든 데이터 준비 완료 → 락 획득 후 최종 중복 체크 및 플래그 선점
    with _nxt_close_alert_lock:
        if _nxt_close_alert_sent_date == today:
            return
        if _load_nxt_close_alert_date() == today:
            _nxt_close_alert_sent_date = today
            return
        _nxt_close_alert_sent_date = today

    total_eval   = nxt.get("total_eval", 0)
    total_profit = nxt.get("total_profit", 0)
    total_rate   = nxt.get("total_profit_rate", 0.0)
    daily_chg    = nxt.get("daily_change", 0)

    SEP = "─" * 20
    sorted_h = sorted(holdings, key=lambda x: x.get("change_rate", 0), reverse=True)

    lines = [
        f"<b>NXT 장마감 포트폴리오 리포트 ({today})</b>",
        SEP,
    ]

    for h in sorted_h:
        name  = h.get("name", h.get("code", ""))
        code  = h.get("code", "")
        price = h.get("current_price", 0)
        rate  = h.get("change_rate", 0.0)
        chg   = h.get("change", 0)
        prate = h.get("profit_rate", 0.0)
        pnl   = h.get("profit_loss", 0)
        arrow = "▲" if chg >= 0 else "▼"
        lines.append(
            f"<b>{name}({code})</b>\n"
            f"- 종가 : {price:,}원\n"
            f"- 전일대비 : {arrow}{abs(chg):,}원 ({rate:+.2f}%)\n"
            f"- 수 익 률 : {'+' if prate>=0 else ''}{prate:.2f}%\n"
            f"- 평가손익 : {'+' if pnl>=0 else ''}{round(pnl):,}원"
        )

    sign_profit = "+" if total_profit >= 0 else ""
    sign_daily  = "+" if daily_chg >= 0 else ""
    bullet = "▪️"
    won = "원"
    lines += [
        SEP,
        f"{bullet} 총 평가금액: <b>{round(total_eval):,}{won}</b>",
        f"{bullet} 총 평가손익: <b>{sign_profit}{round(total_profit):,}{won}</b> ({total_rate:+.2f}%)",
        f"{bullet} 전일 대비:   <b>{sign_daily}{daily_chg:,}{won}</b>",
    ]

    _save_nxt_close_alert_date(today)
    _tg_send("\n".join(lines), msg_type="nxt_report")
    print("[알림] NXT 장마감 텔레그램 발송")


# ─────────────────────────────────────────────
#  미국장 알림
# ─────────────────────────────────────────────
def _load_us_open_alert_date():
    try:
        with open(US_OPEN_ALERT_FILE, encoding="utf-8") as f:
            return json.load(f).get("sent_date", "")
    except Exception:
        return ""

def _save_us_open_alert_date(date: str):
    try:
        with open(US_OPEN_ALERT_FILE, "w", encoding="utf-8") as f:
            json.dump({"sent_date": date}, f)
    except Exception as e:
        print(f"[US 알림] open 저장 실패: {e}")

def _load_us_close_alert_date():
    try:
        with open(US_CLOSE_ALERT_FILE, encoding="utf-8") as f:
            return json.load(f).get("sent_date", "")
    except Exception:
        return ""

def _save_us_close_alert_date(date: str):
    try:
        with open(US_CLOSE_ALERT_FILE, "w", encoding="utf-8") as f:
            json.dump({"sent_date": date}, f)
    except Exception as e:
        print(f"[US 알림] close 저장 실패: {e}")


def _is_us_dst(dt=None) -> bool:
    """미국 DST(EDT) 적용 여부: 3월 둘째 일요일 ~ 11월 첫째 일요일"""
    if dt is None:
        dt = datetime.now()
    y, mo = dt.year, dt.month
    if mo < 3 or mo > 11:  return False
    if 3 < mo < 11:         return True
    if mo == 3:
        sun2 = 8 + (6 - datetime(y, 3, 1).weekday()) % 7   # 둘째 일요일
        return dt.day >= sun2
    sun1 = 1 + (6 - datetime(y, 11, 1).weekday()) % 7      # 첫째 일요일
    return dt.day < sun1


def _check_us_open_alert(portfolio_data: dict):
    """미국 정규장 시작 보유종목 알림 (하루 1번)
    EDT(써머타임): 22:30 KST / EST: 23:30 KST"""
    global _us_open_alert_sent_date
    now = datetime.now()
    if now.weekday() >= 5:
        return
    if now.strftime("%Y-%m-%d") in US_HOLIDAYS:
        return
    open_h = dtime(22, 30) if _is_us_dst(now) else dtime(23, 30)
    open_end = dtime(open_h.hour, open_h.minute + 7)
    if not (open_h <= now.time() < open_end):
        return
    today = now.strftime("%Y-%m-%d")
    # 인메모리 1차 확인 (락 없이 빠르게)
    if _us_open_alert_sent_date == today:
        return

    us       = portfolio_data.get("us", {})
    holdings = us.get("holdings", [])
    if not holdings:
        return

    # 모든 데이터 준비 완료 → 락 획득 후 최종 중복 체크 및 플래그 선점
    with _us_open_alert_lock:
        if _us_open_alert_sent_date == today:
            return
        if _load_us_open_alert_date() == today:
            _us_open_alert_sent_date = today
            return
        _us_open_alert_sent_date = today

    ex_rate          = us.get("exchange_rate", 1380)
    total_eval_usd   = us.get("total_eval_usd", 0)
    total_profit_usd = us.get("total_profit_usd", 0)
    total_rate       = us.get("total_profit_rate", 0.0)
    SEP = "─" * 20
    sorted_h = sorted(holdings, key=lambda x: x.get("change_rate", 0), reverse=True)
    lines = [f"<b>\U0001f1fa\U0001f1f8 미국장 시작 포트폴리오 ({today})</b>", SEP]
    for h in sorted_h:
        name  = h.get("name", h.get("ticker", ""))
        tick  = h.get("ticker", "")
        cur   = h.get("current_price", 0)
        rate  = h.get("change_rate", 0.0)
        chg   = h.get("change", 0.0)
        prate = h.get("profit_rate", 0.0)
        pnl   = h.get("profit_usd", 0.0)
        pnl_krw = round(pnl * ex_rate)
        arrow = "\u25b2" if chg >= 0 else "\u25bc"
        lines.append(
            f"<b>{name}({tick})</b>\n"
            f"- \ud604\uc7ac\uac00: ${cur:,.2f}\n"
            f"- \uc804\uc77c\ub300\ube44: {arrow}${abs(chg):.2f} ({rate:+.2f}%)\n"
            f"- \uc218\uc775\ub960: {'+' if prate>=0 else ''}{prate:.2f}%\n"
            f"- \ud3c9\uac00\uc190\uc775: {'+' if pnl_krw>=0 else ''}{pnl_krw:,}\uc6d0"
        )
    total_eval_krw   = round(total_eval_usd * ex_rate)
    total_profit_krw = round(total_profit_usd * ex_rate)
    sign = "+" if total_profit_usd >= 0 else ""
    lines += [
        SEP,
        f"\u25aa\ufe0f \ucd1d \ud3c9\uac00\uae08\uc561: <b>{total_eval_krw:,}\uc6d0</b>",
        f"\u25aa\ufe0f \ucd1d \ud3c9\uac00\uc190\uc775: <b>{sign}{total_profit_krw:,}\uc6d0</b> ({total_rate:+.2f}%)",
        f"\u25aa\ufe0f \ud658\uc728: {ex_rate:,.0f}\uc6d0/USD",
    ]
    _save_us_open_alert_date(today)
    _tg_send("\n".join(lines), msg_type="us_open_report")
    print("[US 알림] 미국장 시작 텔레그램 발송")


def _check_us_close_alert(portfolio_data: dict):
    """미국 정규장 마감 보유종목 알림 (하루 1번)
    EDT(써머타임): 05:00 KST / EST: 06:00 KST
    마감 시각 05:00 KST = 전날 16:00 EDT → 미국 실제 거래일은 KST 기준 전일"""
    global _us_close_alert_sent_date
    now = datetime.now()
    close_h = dtime(5, 0) if _is_us_dst(now) else dtime(6, 0)
    close_end = dtime(close_h.hour, close_h.minute + 10)
    if not (close_h <= now.time() < close_end):
        return
    # 미국 실제 거래일 = KST 기준 전일 (05:00 KST = 전날 16:00 EDT)
    us_trading_dt  = now - timedelta(days=1)
    us_trading_dow = us_trading_dt.weekday()
    us_date_str    = us_trading_dt.strftime("%Y-%m-%d")
    # 주말(토=5, 일=6) 또는 미국 휴장일이면 skip
    if us_trading_dow >= 5:
        return
    if us_date_str in US_HOLIDAYS:
        return
    today = now.strftime("%Y-%m-%d")
    # 인메모리 1차 확인 (락 없이 빠르게)
    if _us_close_alert_sent_date == today:
        return

    us       = portfolio_data.get("us", {})
    holdings = us.get("holdings", [])
    if not holdings:
        return

    # 모든 데이터 준비 완료 → 락 획득 후 최종 중복 체크 및 플래그 선점
    with _us_close_alert_lock:
        if _us_close_alert_sent_date == today:
            return
        if _load_us_close_alert_date() == today:
            _us_close_alert_sent_date = today
            return
        _us_close_alert_sent_date = today

    ex_rate          = us.get("exchange_rate", 1380)
    total_eval_usd   = us.get("total_eval_usd", 0)
    total_profit_usd = us.get("total_profit_usd", 0)
    total_rate       = us.get("total_profit_rate", 0.0)
    SEP = "─" * 20
    sorted_h = sorted(holdings, key=lambda x: x.get("change_rate", 0), reverse=True)
    lines = [f"<b>\U0001f1fa\U0001f1f8 미국장 마감 포트폴리오 ({us_date_str})</b>", SEP]
    for h in sorted_h:
        name  = h.get("name", h.get("ticker", ""))
        tick  = h.get("ticker", "")
        cur   = h.get("current_price", 0)
        rate  = h.get("change_rate", 0.0)
        chg   = h.get("change", 0.0)
        prate = h.get("profit_rate", 0.0)
        pnl   = h.get("profit_usd", 0.0)
        pnl_krw = round(pnl * ex_rate)
        arrow = "\u25b2" if chg >= 0 else "\u25bc"
        lines.append(
            f"<b>{name}({tick})</b>\n"
            f"- \ud604\uc7ac\uac00: ${cur:,.2f}\n"
            f"- \uc804\uc77c\ub300\ube44: {arrow}${abs(chg):.2f} ({rate:+.2f}%)\n"
            f"- \uc218\uc775\ub960: {'+' if prate>=0 else ''}{prate:.2f}%\n"
            f"- \ud3c9\uac00\uc190\uc775: {'+' if pnl_krw>=0 else ''}{pnl_krw:,}\uc6d0"
        )
    total_eval_krw   = round(total_eval_usd * ex_rate)
    total_profit_krw = round(total_profit_usd * ex_rate)
    sign = "+" if total_profit_usd >= 0 else ""
    lines += [
        SEP,
        f"\u25aa\ufe0f \ucd1d \ud3c9\uac00\uae08\uc561: <b>{total_eval_krw:,}\uc6d0</b>",
        f"\u25aa\ufe0f \ucd1d \ud3c9\uac00\uc190\uc775: <b>{sign}{total_profit_krw:,}\uc6d0</b> ({total_rate:+.2f}%)",
        f"\u25aa\ufe0f \ud658\uc728: {ex_rate:,.0f}\uc6d0/USD",
    ]
    _save_us_close_alert_date(today)
    _tg_send("\n".join(lines), msg_type="us_close_report")
    print("[US 알림] 미국장 마감 텔레그램 발송")


def _check_open_alert(portfolio_data: dict):
    """KRX 장 시작 후(09:02~09:07) 보유종목 시가 기준 알림 (하루 1번)"""
    global _open_alert_sent_date
    now = datetime.now()
    if now.weekday() >= 5:
        return
    if now.strftime("%Y-%m-%d") in KR_HOLIDAYS:
        return
    if not (dtime(9, 2) <= now.time() < dtime(9, 7)):
        return
    today = now.strftime("%Y-%m-%d")
    # 인메모리 1차 확인 (락 없이 빠르게) — 이미 발송한 날이면 즉시 종료
    if _open_alert_sent_date == today:
        return

    kr       = portfolio_data.get("kr", {})
    holdings = kr.get("holdings", [])
    if not holdings:
        return

    # KRX(J) 시가 직접 조회
    cfg_portfolio = _config.get("portfolio", [])
    krx_price_map = {}
    for p in cfg_portfolio:
        code = str(p.get("code", "")).zfill(6)
        item = _kis_api.fetch_price_raw(code, "J") if _kis_api else None
        if item:
            krx_price_map[code] = item

    # 시가가 0인 종목이 과반이면 아직 미체결 — 다음 루프에서 재시도
    if sum(1 for v in krx_price_map.values() if v.get("open", 0) > 0) < len(holdings) // 2 + 1:
        return

    # 모든 데이터 준비 완료 → 락 획득 후 최종 중복 체크 및 플래그 선점
    # (여기까지 통과한 여러 스레드 중 오직 한 스레드만 발송)
    with _open_alert_lock:
        if _open_alert_sent_date == today:
            return
        if _load_open_alert_date() == today:
            _open_alert_sent_date = today
            return
        _open_alert_sent_date = today   # 선점: 락 해제 후 다른 스레드는 인메모리 플래그로 차단

    # 시가 기준 총 평가금액 / 손익 계산
    total_buy  = sum(h.get("buy_amount", 0) for h in holdings)
    total_eval = sum(
        (krx_price_map.get(h.get("code", ""), {}).get("open") or h.get("current_price", 0))
        * h.get("quantity", 0) for h in holdings
    )
    total_profit = total_eval - total_buy
    total_rate   = round(total_profit / total_buy * 100, 2) if total_buy else 0.0

    history   = _load_history()
    records   = history.get("records", [])
    prev_eval = records[0].get("total_eval", 0) if records else 0
    daily_chg = round(total_eval - prev_eval) if prev_eval else 0

    SEP = "─" * 20
    sorted_h = sorted(holdings, key=lambda x: x.get("change_rate", 0), reverse=True)

    lines = [
        f"<b>장 시작 포트폴리오 리포트 ({today})</b>",
        SEP,
    ]

    for h in sorted_h:
        name  = h.get("name", h.get("code", ""))
        code  = h.get("code", "")
        krx   = krx_price_map.get(code, {})
        opn   = krx.get("open", 0) or h.get("current_price", 0)
        avg   = h.get("avg_price", 0)
        qty   = h.get("quantity", 0)
        chg   = krx.get("change", h.get("change", 0))
        rate  = krx.get("change_rate", h.get("change_rate", 0.0))
        pnl   = round((opn - avg) * qty) if avg else 0
        prate = round((opn - avg) / avg * 100, 2) if avg else 0.0
        arrow = "▲" if chg >= 0 else "▼"
        lines.append(
            f"<b>{name}({code})</b>\n"
            f"- 현재가 : {opn:,}원\n"
            f"- 전일대비 : {arrow}{abs(chg):,}원 ({rate:+.2f}%)\n"
            f"- 수 익 률 : {'+' if prate>=0 else ''}{prate:.2f}%\n"
            f"- 평가손익 : {'+' if pnl>=0 else ''}{pnl:,}원"
        )

    sign_profit = "+" if total_profit >= 0 else ""
    sign_daily  = "+" if daily_chg >= 0 else ""
    bullet = "\u25aa\ufe0f"
    won = "\uc6d0"
    lines += [
        SEP,
        f"{bullet} 총 평가금액: <b>{round(total_eval):,}{won}</b>",
        f"{bullet} 총 평가손익: <b>{sign_profit}{round(total_profit):,}{won}</b> ({total_rate:+.2f}%)",
        f"{bullet} 전일 대비:   <b>{sign_daily}{daily_chg:,}{won}</b>",
    ]

    _save_open_alert_date(today)
    _tg_send("\n".join(lines), msg_type="open_report")
    print("[알림] 장 시작 텔레그램 발송")



# ============================================================
#  실시간 가격 업데이터 (백그라운드 스레드)
# ============================================================
_latest_data   = {}          # 최신 포트폴리오 스냅샷
_sse_listeners = []          # 연결된 SSE 클라이언트 큐 목록
_data_lock     = threading.Lock()
_INTERVAL      = max(1, int(_config.get("refresh_interval", 1)))

# ── KIS WebSocket 실시간 체결가 ──
_kr_ws_prices  = {}          # code → {current_price, change, change_rate}
_kr_ws_active  = False       # WebSocket 활성 여부
_kr_ws_lock    = threading.Lock()

# ── 알림 중복 방지 (레이스 컨디션 방어) ──
# 파일 I/O만으로는 동시 실행 스레드가 모두 "미발송"으로 판단할 수 있으므로
# 인메모리 락 + 발송일 변수로 1차 차단 후 파일로 영구 저장
_open_alert_lock      = threading.Lock()
_open_alert_sent_date = _load_open_alert_date()       # 재시작 후 당일 중복 발송 방지
_close_alert_lock      = threading.Lock()
_close_alert_sent_date = _load_close_alert_date()
_nxt_close_alert_lock      = threading.Lock()
_nxt_close_alert_sent_date = _load_nxt_close_alert_date()
_us_open_alert_lock      = threading.Lock()
_us_open_alert_sent_date = _load_us_open_alert_date()
_us_close_alert_lock      = threading.Lock()
_us_close_alert_sent_date = _load_us_close_alert_date()


def _get_kis_ws_approval_key():
    """KIS WebSocket 접속 승인키 발급"""
    kc = _config.get("kis", {})
    is_paper = kc.get("is_paper", False)
    base = ("https://openapivts.koreainvestment.com:29443" if is_paper
            else "https://openapi.koreainvestment.com:9443")
    try:
        resp = requests.post(
            f"{base}/oauth2/Approval",
            json={"grant_type": "client_credentials",
                  "appkey":    kc.get("app_key", ""),
                  "secretkey": kc.get("app_secret", "")},
            timeout=10, verify=False,
        )
        key = resp.json().get("approval_key", "")
        if key:
            print("[KIS WS] 승인키 발급 성공")
        return key
    except Exception as e:
        print(f"[KIS WS] 승인키 발급 실패: {e}")
        return ""


def _is_kr_trading_now():
    """현재 KRX 정규장 시간 여부 (09:00~15:35, 평일, 비공휴일)"""
    now = datetime.now()
    return (now.weekday() < 5
            and now.strftime("%Y-%m-%d") not in KR_HOLIDAYS
            and dtime(9, 0) <= now.time() < dtime(15, 36))


def _run_kr_websocket():
    """KIS H0STCNT0 실시간 체결가 WebSocket 루프
    - 장 중(09:00~15:35)에만 연결, 그 외에는 30초 간격으로 재확인
    - 체결가 수신 시 즉시 포트폴리오 재계산 & socketio emit
    """
    import websocket as ws_lib
    import ssl

    global _kr_ws_active

    while True:
        if not _is_kr_trading_now():
            _kr_ws_active = False
            time.sleep(30)
            continue

        if not _kis_api:
            time.sleep(60)
            continue

        approval_key = _get_kis_ws_approval_key()
        if not approval_key:
            time.sleep(60)
            continue

        kc = _config.get("kis", {})
        ws_url = ("ws://ops.koreainvestment.com:31000" if kc.get("is_paper")
                  else "ws://ops.koreainvestment.com:21000")
        codes = [str(it.get("code", "")).zfill(6)
                 for it in _config.get("portfolio", []) if it.get("code")]

        def _make_sub_msg(code, tr_type="1"):
            return json.dumps({
                "header": {
                    "approval_key": approval_key,
                    "custtype":     "P",
                    "tr_type":      tr_type,
                    "content-type": "utf-8",
                },
                "body": {"input": {"tr_id": "H0STCNT0", "tr_key": code}},
            })

        def on_open(ws):
            global _kr_ws_active
            _kr_ws_active = True
            print(f"[KIS WS] 연결됨 → {len(codes)}종목 구독")
            for code in codes:
                ws.send(_make_sub_msg(code))

        def on_message(ws, message):
            # PINGPONG 처리
            if '"tr_id":"PINGPONG"' in message:
                ws.send(message)
                return
            # 실시간 체결가: "0|H0STCNT0|NNN|code^price^sign^chg^rate^..."
            if not message.startswith("0|H0STCNT0|"):
                return
            parts = message.split("|", 3)
            if len(parts) < 4:
                return
            fields = parts[3].split("^")
            if len(fields) < 6:
                return
            try:
                code      = fields[0]
                cur       = int(fields[2])
                sign_flag = fields[3]          # "2"=상승, "3"=보합, "4"=하한, "5"=하락
                minus     = sign_flag in ("4", "5")
                # abs() 후 부호 적용: signed/unsigned 입력 모두 올바르게 처리
                chg      = -abs(int(fields[4]))   if minus else abs(int(fields[4]))
                chg_rate = -abs(float(fields[5])) if minus else abs(float(fields[5]))
                if cur <= 0:
                    return
                with _kr_ws_lock:
                    _kr_ws_prices[code] = {
                        "current_price": cur,
                        "change":        chg,
                        "change_rate":   chg_rate,
                    }
                # 단일 push 스레드에 신호만 전달 (스레드 생성 없음)
                _ws_push_event.set()
            except Exception as e:
                print(f"[KIS WS] 파싱 오류: {e}")

        def on_error(ws, error):
            global _kr_ws_active
            _kr_ws_active = False
            print(f"[KIS WS] 오류: {error}")

        def on_close(ws, code, msg):
            global _kr_ws_active
            _kr_ws_active = False
            print(f"[KIS WS] 연결 종료")

        ws_app = ws_lib.WebSocketApp(
            ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws_app.run_forever(ping_interval=30, ping_timeout=10)
        _kr_ws_active = False
        time.sleep(5)   # 재연결 전 대기


# KIS WS push 전담 스레드용 Event
_ws_push_event = threading.Event()

def _ws_push_worker():
    """KIS WS 체결가 push 전담 스레드
    - 틱 수신 시 Event.set()으로 신호만 받음 → 스레드 생성 0
    - 작업 중 추가 틱이 와도 완료 후 즉시 재처리 → 데이터 손실 없음
    - race condition 없음 (단일 스레드가 순서대로 처리)
    """
    while True:
        _ws_push_event.wait()      # 틱 신호 대기
        _ws_push_event.clear()     # 신호 소비 (이후 추가 틱은 다시 set됨)
        try:
            data = build_portfolio()
            with _data_lock:
                us_snap = dict(_latest_data.get("us", {}))
            if us_snap:
                data["us"] = us_snap   # US는 REST 주기에 맡김
            data["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with _data_lock:
                _latest_data.update(data)
            _push_to_all(data)
        except Exception as e:
            print(f"[KIS WS] push 오류: {e}")

def _push_to_all(data):
    # WebSocket 푸시 (연결된 모든 클라이언트)
    try:
        socketio.emit('portfolio', data)
    except Exception as e:
        print(f"[WS] emit 오류: {e}")
    # SSE 폴백 (하위 호환)
    dead = []
    for q in _sse_listeners:
        try:
            q.put_nowait(data)
        except Exception:
            dead.append(q)
    for q in dead:
        if q in _sse_listeners:
            _sse_listeners.remove(q)

# ============================================================
#  일별 히스토리 저장
# ============================================================
def _load_history():
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"records": []}

def _save_daily_snapshot(portfolio_data):
    """평일 15:30 이후 오늘 기록이 없으면 저장"""
    now = datetime.now()
    if now.weekday() >= 5:          # 주말 제외
        return
    if now.strftime("%Y-%m-%d") in KR_HOLIDAYS:  # 공휴일 제외
        return
    if now.time() < dtime(15, 35):  # KRX 종가 확정 후 저장 (NXT 전환 전)
        return

    today   = now.strftime("%Y-%m-%d")
    history = _load_history()

    if any(r["date"] == today for r in history["records"]):
        return  # 이미 저장됨

    kr = portfolio_data.get("kr", {})
    total_eval = kr.get("total_eval", 0)

    # 전일대비 손익 = 오늘 평가금액 - 전일 평가금액
    prev_eval    = history["records"][0].get("total_eval", 0) if history["records"] else 0
    daily_change = round(total_eval - prev_eval) if prev_eval else 0

    record = {
        "date":              today,
        "total_buy":         round(kr.get("total_buy", 0)),
        "total_eval":        round(total_eval),
        "total_profit":      round(kr.get("total_profit", 0)),
        "total_profit_rate": round(kr.get("total_profit_rate", 0), 2),
        "daily_change":      daily_change,
    }
    history["records"].insert(0, record)   # 최신순
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    print(f"[히스토리] {today} 저장 완료")

    # KRX 종가 시점의 종목별 데이터를 별도 저장 (장마감 알림용)
    krx_close = {
        "date": today,
        "total_eval":        round(total_eval),
        "total_profit":      round(kr.get("total_profit", 0)),
        "total_profit_rate": round(kr.get("total_profit_rate", 0), 2),
        "daily_change":      daily_change,
        "holdings": [
            {k: h[k] for k in ("code","name","current_price","change","change_rate",
                                "profit_rate","profit_loss") if k in h}
            for h in kr.get("holdings", [])
        ]
    }
    krx_close_file = os.path.join(_BASE_DIR, "krx_close.json")
    with open(krx_close_file, "w", encoding="utf-8") as f:
        json.dump(krx_close, f, ensure_ascii=False, indent=2)
    print(f"[KRX 종가] {today} 종목별 종가 저장 완료")
    return True


def _save_nxt_snapshot():
    """평일 20:00~20:05 NXT 종가 스냅샷 저장 (하루 1번)
    - 현재가: NXT 우선, 없으면 KRX 폴백
    - 전일대비: 전일 KRX 종가 기준 (KIS API prdy_vrss)
    """
    now = datetime.now()
    if now.weekday() >= 5:
        return
    if now.strftime("%Y-%m-%d") in KR_HOLIDAYS:
        return
    if not (dtime(20, 0) <= now.time() < dtime(20, 5)):
        return
    if not _kis_api:
        return

    today = now.strftime("%Y-%m-%d")
    try:
        with open(NXT_CLOSE_FILE, encoding="utf-8") as f:
            if json.load(f).get("date") == today:
                return  # 이미 저장됨
    except Exception:
        pass

    cfg_portfolio = _config.get("portfolio", [])
    holdings = []
    for p in cfg_portfolio:
        code     = p["code"]
        avg      = p.get("avg_price", 0)
        qty      = p.get("quantity", 0)
        name     = p.get("name", code)
        # NXT 우선, 없으면 KRX — change/change_rate 는 API가 전일 KRX 종가 기준으로 반환
        item = _kis_api.fetch_price_raw(code, "NX") or _kis_api.fetch_price_raw(code, "J")
        if not item:
            continue
        price = item["current_price"]
        pnl   = (price - avg) * qty
        prate = (price - avg) / avg * 100 if avg else 0
        holdings.append({
            "code":          code,
            "name":          name,
            "current_price": price,
            "change":        item["change"],
            "change_rate":   item["change_rate"],
            "profit_rate":   round(prate, 2),
            "profit_loss":   round(pnl),
            "eval_amount":   price * qty,
            "buy_amount":    avg * qty,
        })

    if not holdings:
        return

    total_eval   = sum(h["eval_amount"] for h in holdings)
    total_buy    = sum(h["buy_amount"]  for h in holdings)
    total_profit = total_eval - total_buy
    total_rate   = round(total_profit / total_buy * 100, 2) if total_buy else 0
    # 전일 대비: history.json 의 전일 KRX 평가금액 기준
    history   = _load_history()
    records   = history.get("records", [])
    prev_eval = records[0].get("total_eval", 0) if records else 0
    daily_chg = round(total_eval - prev_eval) if prev_eval else 0

    nxt_close = {
        "date":              today,
        "total_eval":        round(total_eval),
        "total_profit":      round(total_profit),
        "total_profit_rate": total_rate,
        "daily_change":      daily_chg,
        "holdings": [
            {k: h[k] for k in ("code","name","current_price","change","change_rate",
                                "profit_rate","profit_loss") if k in h}
            for h in holdings
        ]
    }
    with open(NXT_CLOSE_FILE, "w", encoding="utf-8") as f:
        json.dump(nxt_close, f, ensure_ascii=False, indent=2)
    print(f"[NXT 종가] {today} NXT 종가 스냅샷 저장 완료")


_auto_batch_done_date = None   # 당일 자동 배치 실행 여부 추적

def _auto_batch_if_needed():
    """이전 거래일 데이터가 history.json에 없으면 batch_catchup.py 자동 실행"""
    global _auto_batch_done_date
    import subprocess

    now = datetime.now()
    today = now.strftime("%Y-%m-%d")

    # 하루에 한 번만 실행
    if _auto_batch_done_date == today:
        return
    _auto_batch_done_date = today

    # 전 거래일 계산
    def prev_trading_day(d):
        from datetime import timedelta
        dt = datetime.strptime(d, "%Y-%m-%d") - timedelta(days=1)
        while True:
            s = dt.strftime("%Y-%m-%d")
            if dt.weekday() < 5 and s not in KR_HOLIDAYS:
                return s
            dt -= timedelta(days=1)

    prev_day = prev_trading_day(today)

    # history.json에 전 거래일 기록이 있는지 확인
    history = _load_history()
    already = any(r["date"] == prev_day for r in history.get("records", []))
    if already:
        return

    print(f"[자동배치] {prev_day} 데이터 없음 → batch_catchup.py 실행")
    _tg_send(f"<b>자동 배치</b> 실행 중... ({prev_day} 누락 데이터 복구)", msg_type="system")
    try:
        result = subprocess.run(
            [sys.executable, os.path.join(_BASE_DIR, "batch_catchup.py")],
            capture_output=True, timeout=180,
            env={**os.environ, "PYTHONIOENCODING": "utf-8"},
        )
        out = (result.stdout or b"").decode("utf-8", errors="replace")
        lines = [l for l in out.splitlines() if l.strip() and not l.startswith("=")]
        summary = "\n".join(lines[-8:]) if lines else "완료 (변경 없음)"
        print(f"[자동배치] 완료\n{summary}")
        _tg_send(f"<b>자동 배치 완료</b>\n<code>{summary}</code>", msg_type="system")
    except subprocess.TimeoutExpired:
        print("[자동배치] 시간 초과")
    except Exception as e:
        print(f"[자동배치] 오류: {e}")


def _run_background_tasks(data):
    """알림·스냅샷 등 부가 작업 — _price_loop 블로킹 방지용 백그라운드 실행"""
    try:
        if _save_daily_snapshot(data):
            _push_to_all({**data, "history_updated": True})
        if _save_us_daily_snapshot(data):
            _push_to_all({**data, "us_history_updated": True})
        holdings = data.get("kr", {}).get("holdings", [])
        if holdings:
            _check_rate_alerts(holdings)
            _check_high_low_alerts(holdings)
        _check_open_alert(data)
        _check_close_alert()
        _save_nxt_snapshot()
        _check_nxt_close_alert()
        if data.get("us", {}).get("holdings"):
            _check_us_open_alert(data)
            _check_us_close_alert(data)
        _save_combined_daily_snapshot()
        _auto_batch_if_needed()
    except Exception as e:
        print(f"[백그라운드] 오류: {e}")


def _price_loop():
    while True:
        t0 = time.time()
        try:
            from concurrent.futures import ThreadPoolExecutor
            has_us = bool(_config.get("us_portfolio"))

            # KIS WebSocket 활성 중: KR은 WS가 담당하므로 US만 갱신
            if _kr_ws_active and _is_kr_trading_now():
                if has_us:
                    us_data = build_us_portfolio()
                    with _data_lock:
                        _latest_data.update(us_data)
                        data = dict(_latest_data)
                    _push_to_all(data)
                else:
                    with _data_lock:
                        data = dict(_latest_data)
            else:
                # WS 비활성 시: KR + US 병렬 REST 폴링
                with ThreadPoolExecutor(max_workers=2) as _ex:
                    kr_fut = _ex.submit(build_portfolio)
                    us_fut = _ex.submit(build_us_portfolio) if has_us else None
                    data = kr_fut.result()
                    if us_fut:
                        try:
                            data.update(us_fut.result(timeout=15))
                        except Exception as _e:
                            print(f"[US 업데이터] 오류: {_e}")
                data["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                with _data_lock:
                    _latest_data.clear()
                    _latest_data.update(data)
                _push_to_all(data)

            # 알림·스냅샷은 블로킹하지 않고 백그라운드에서 실행
            threading.Thread(target=_run_background_tasks, args=(dict(data),), daemon=True).start()

        except Exception as e:
            print(f"[업데이터] 오류: {e}")
        elapsed = time.time() - t0
        sleep_time = max(0, _INTERVAL - elapsed)
        if elapsed > _INTERVAL:
            print(f"[업데이터] 조회 {elapsed:.1f}s (인터벌 {_INTERVAL}s 초과)")
        time.sleep(sleep_time)

_updater = threading.Thread(target=_price_loop, daemon=True)
_updater.start()

# KIS WebSocket 실시간 체결가 스레드 시작
_kr_ws_thread = threading.Thread(target=_run_kr_websocket, daemon=True)
_kr_ws_thread.start()

# WS push 전담 스레드 시작
_ws_push_worker_thread = threading.Thread(target=_ws_push_worker, daemon=True)
_ws_push_worker_thread.start()

# ============================================================
#  텔레그램 명령 수신 (Long Polling)
# ============================================================
_tg_poll_offset = 0   # 마지막 처리한 update_id + 1

def _tg_handle_command(text: str, chat_id: str):
    """수신 명령 처리 → 응답 메시지 반환"""
    cmd = text.strip().split()[0].lower().lstrip("/")

    if cmd in ("도움", "help", "명령"):
        return (
            "<b>사용 가능한 명령어</b>\n"
            "────────────────────\n"
            "/현황  현재 포트폴리오 전체 현황\n"
            "/가격  보유종목 현재가\n"
            "/종목명  종목 현재가·시고저종·거래량 (전체 검색)\n"
            "/차트 종목명  종목 캔들차트 이미지\n"
            "/분석 종목명  AI 종목 분석 리포트\n"
            "/뉴스 종목명  종목 최근 뉴스 5건\n"
            "/배치  누락 데이터 자동 복구\n"
            "/도움  이 도움말"
        )

    if cmd in ("차트", "chart"):
        parts = text.strip().split(maxsplit=1)
        query = parts[1].strip() if len(parts) > 1 else ""
        if not query:
            return "/차트 사용법: /차트 삼성전자"
        # 종목 코드/이름 찾기
        found_code, found_name = None, None
        for it in _config.get("portfolio", []):
            c = str(it.get("code", "")).zfill(6)
            n = it.get("name", "")
            if query == c or query in n:
                found_code, found_name = c, n
                break
        if not found_code:
            matches = _naver.search(query)
            if matches:
                found_code = str(matches[0]["code"]).zfill(6)
                found_name = matches[0]["name"]
        if not found_code:
            return f"'{query}' 종목을 찾을 수 없습니다."
        # 백그라운드로 차트 생성 후 전송
        def _send_chart():
            _tg_send(f"<b>{found_name}</b> 차트 생성 중...", msg_type="cmd_response")
            img = _build_chart_image(found_code, found_name)
            if img:
                caption = f"<b>{found_name}({found_code})</b>  일봉 최근 60일"
                _tg_send_photo(img, caption=caption)
            else:
                _tg_send(f"[{found_name}] 차트 데이터를 불러오지 못했습니다.", msg_type="cmd_response")
        threading.Thread(target=_send_chart, daemon=True).start()
        return None  # 응답은 백그라운드에서 전송

    if cmd in ("분석", "analyze", "ai"):
        parts = text.strip().split(maxsplit=1)
        query = parts[1].strip() if len(parts) > 1 else ""
        if not query:
            return "/분석 사용법: /분석 삼성전자"
        # 종목 찾기 (포트폴리오 → 네이버 검색)
        found_code, found_name = None, None
        for it in _config.get("portfolio", []):
            c = str(it.get("code", "")).zfill(6)
            n = it.get("name", "")
            if query == c or query in n:
                found_code, found_name = c, n
                break
        if not found_code:
            matches = _naver.search(query)
            if matches:
                found_code = str(matches[0]["code"]).zfill(6)
                found_name = matches[0]["name"]
        if not found_code:
            return f"'{query}' 종목을 찾을 수 없습니다."
        # 백그라운드로 AI 분석 후 전송
        def _send_analysis():
            _tg_send(f"<b>{found_name}</b> AI 분석 중... ⏳", msg_type="cmd_response")
            result = _ai_analyze_stock(found_code, found_name)
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
            msg = f"<b>🤖 {found_name}({found_code}) AI 분석</b>  {now_str}\n{'─'*20}\n{result}"
            _tg_send(msg, msg_type="cmd_response")
        threading.Thread(target=_send_analysis, daemon=True).start()
        return None

    if cmd in ("현황", "status"):
        with _data_lock:
            data = dict(_latest_data)
        if not data.get("kr"):
            return "데이터 준비 중입니다. 잠시 후 다시 시도해주세요."
        kr = data["kr"]
        holdings = kr.get("holdings", [])
        now_str  = datetime.now().strftime("%Y-%m-%d %H:%M")
        lines = [f"<b>포트폴리오 현황 ({now_str})</b>", "─" * 20]
        sorted_h = sorted(holdings, key=lambda x: x.get("change_rate", 0), reverse=True)
        for h in sorted_h:
            chg   = h.get("change", 0)
            rate  = h.get("change_rate", 0.0)
            pnl   = h.get("profit_loss", 0)
            pr    = h.get("profit_rate", 0.0)
            arrow = "▲" if chg >= 0 else "▼"
            lines.append(
                f"<b>{h['name']}({h['code']})</b>\n"
                f"- 현재가: {h['current_price']:,}원\n"
                f"- 전일대비: {arrow}{abs(chg):,}원 ({rate:+.2f}%)\n"
                f"- 평가손익: {'+' if pnl>=0 else ''}{round(pnl):,}원 ({pr:+.2f}%)"
            )
        te = kr.get("total_eval", 0)
        tp = kr.get("total_profit", 0)
        tr = kr.get("total_profit_rate", 0.0)
        lines += [
            "─" * 20,
            f"■ 총 평가금액: <b>{round(te):,}원</b>",
            f"■ 총 평가손익: <b>{'+' if tp>=0 else ''}{round(tp):,}원</b> ({tr:+.2f}%)",
        ]
        return "\n".join(lines)

    if cmd in ("가격", "price"):
        with _data_lock:
            data = dict(_latest_data)
        if not data.get("kr"):
            return "데이터 준비 중입니다."
        holdings = data["kr"].get("holdings", [])
        now_str  = datetime.now().strftime("%H:%M")
        lines = [f"<b>현재가 ({now_str})</b>", "─" * 20]
        for h in holdings:
            chg  = h.get("change", 0)
            rate = h.get("change_rate", 0.0)
            arrow = "▲" if chg >= 0 else "▼"
            lines.append(
                f"{h['name']}: <b>{h['current_price']:,}원</b>  "
                f"{arrow}{abs(chg):,} ({rate:+.2f}%)"
            )
        return "\n".join(lines)

    if cmd in ("뉴스", "news"):
        parts = text.strip().split(maxsplit=1)
        query = parts[1].strip() if len(parts) > 1 else ""
        # 종목명 또는 코드로 찾기
        portfolio = _config.get("portfolio", [])
        target = None
        for it in portfolio:
            c = str(it.get("code","")).zfill(6)
            if query == c or query in it.get("name",""):
                target = it; break
        if not target:
            names = ", ".join(it.get("name","") for it in portfolio)
            return f"종목을 찾을 수 없습니다.\n사용법: /뉴스 삼성전자\n보유종목: {names}"
        code = str(target["code"]).zfill(6)
        name = target["name"]
        items = _fetch_stock_news(code)
        if not items:
            return f"[{name}] 뉴스를 불러오지 못했습니다."
        lines = [f"<b>[{name}] 최근 뉴스</b>", "─" * 20]
        for i, n in enumerate(items[:5], 1):
            lines.append(f"{i}. <a href=\"{n['url']}\">{n['title']}</a>\n   {n['source']}  {n['date']}")
        return "\n".join(lines)

    if cmd in ("배치", "batch"):
        import subprocess
        _tg_send("배치 복구를 시작합니다...", msg_type="cmd_response")
        try:
            result = subprocess.run(
                [sys.executable, os.path.join(_BASE_DIR, "batch_catchup.py")],
                capture_output=True, timeout=120,
                env={**os.environ, "PYTHONIOENCODING": "utf-8"},
            )
            out = (result.stdout or b"").decode("utf-8", errors="replace")
            lines = [l for l in out.splitlines() if l.strip() and not l.startswith("=")]
            summary = "\n".join(lines[-10:]) if lines else "완료 (변경 없음)"
            return f"<b>배치 복구 완료</b>\n<code>{summary}</code>"
        except subprocess.TimeoutExpired:
            return "배치 실행 시간 초과"
        except Exception as e:
            return f"배치 실행 오류: {e}"

    # /종목명 또는 /종목코드 → 검색 결과 전체 시세 (종목검색과 동일)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    if cmd.isdigit():
        # 코드 직접 입력
        code = cmd.zfill(6)
        pi = get_price(code)
        if pi:
            if not pi.get("name"):
                npi = _naver.get_price(code)
                pi["name"] = (npi.get("name") if npi else "") or code
            candidates = [pi]
        else:
            candidates = []
    else:
        matches = _naver.search(cmd)
        candidates = []
        for m in matches[:6]:
            pi = get_price(m["code"])
            if not pi:
                pi = _naver.get_price(m["code"])  # 장 전/후 KIS 0원 응답 시 네이버 폴백
            if pi:
                if not pi.get("name"):
                    pi["name"] = m.get("name", m["code"])
                candidates.append(pi)

    # 국내 검색 결과 없으면 미국 종목 시도
    us_result = None
    if not candidates and _kis_api:
        ticker = cmd.upper()
        us_result = _kis_api.fetch_us_price(ticker)

    if not candidates and not us_result:
        return f"'{cmd}' 검색 결과가 없습니다."

    sep = "─" * 20
    lines = [f"<b>🔍 {cmd.upper()} 검색결과  {now_str}</b>", sep]

    # 미국 종목 결과
    if us_result:
        ex_rate   = get_usd_krw_rate()
        ticker    = us_result.get("ticker", cmd.upper())
        excd      = us_result.get("exchange", "")
        cur       = us_result.get("current_price", 0.0)
        chg       = us_result.get("change", 0.0)
        rate      = us_result.get("change_rate", 0.0)
        opn       = us_result.get("open", 0.0)
        high      = us_result.get("high", 0.0)
        low       = us_result.get("low", 0.0)
        vol       = us_result.get("volume", 0)
        prev_close = us_result.get("prev_close", cur - chg)
        arrow     = "▲" if chg >= 0 else "▼"
        def _ur(p): return (p - prev_close) / prev_close * 100 if prev_close else 0.0
        def _ua(p): return "▲" if p >= prev_close else "▼"
        def _fmt_us(label, p):
            if p <= 0:
                return f"{label} : -"
            return f"{label} : ${p:,.2f}  {_ua(p)}${abs(p-prev_close):.2f} ({_ur(p):+.2f}%)"
        lines.append(
            f"<b>{ticker} ({excd})</b>\n"
            f"현재가 : <b>${cur:,.2f}</b>  {arrow}${abs(chg):.2f} ({rate:+.2f}%)\n"
            f"{_fmt_us('시가', opn)}\n"
            f"{_fmt_us('고가', high)}\n"
            f"{_fmt_us('저가', low)}\n"
            f"거래량 : {vol:,}\n"
            f"환율 : {ex_rate:,.0f}원/USD"
        )
        lines.append(sep)
        return "\n".join(lines)

    # 국내 종목 결과
    for pi in candidates:
        code  = str(pi.get("code", "")).zfill(6)
        name  = pi.get("name", code)
        cur   = pi.get("current_price", 0)
        chg   = pi.get("change", 0)
        rate  = pi.get("change_rate", 0.0)
        opn   = pi.get("open", 0)
        high  = pi.get("high", 0)
        low   = pi.get("low", 0)
        vol   = pi.get("volume", 0)
        arrow = "▲" if chg >= 0 else "▼"
        prev_close = cur - chg if chg else cur
        def _r(p): return (p - prev_close) / prev_close * 100 if prev_close else 0.0
        def _a(p): return "▲" if p >= prev_close else "▼"
        lines.append(
            f"<b>{name}({code})</b>\n"
            f"현재가 : <b>{cur:,}원</b>  {arrow}{abs(chg):,} ({rate:+.2f}%)\n"
            f"시가 : {opn:,}원  {_a(opn)}{abs(opn-prev_close):,} ({_r(opn):+.2f}%)\n"
            f"고가 : {high:,}원  {_a(high)}{abs(high-prev_close):,} ({_r(high):+.2f}%)\n"
            f"저가 : {low:,}원  {_a(low)}{abs(low-prev_close):,} ({_r(low):+.2f}%)\n"
            f"거래량 : {vol:,}"
        )
        lines.append(sep)
    return "\n".join(lines)

    return f"알 수 없는 명령입니다: /{cmd}\n/도움 으로 사용법을 확인하세요."


def _tg_poll_loop():
    """텔레그램 Long Polling 수신 루프"""
    global _tg_poll_offset
    cfg_tg = _config.get("telegram", {})
    token  = cfg_tg.get("bot_token", "").strip()
    my_chat = cfg_tg.get("chat_id", "").strip()
    if not token or not my_chat:
        return   # 텔레그램 미설정

    print("[TG 수신] 텔레그램 명령 수신 대기 시작")
    while True:
        try:
            resp = _tg_session.get(
                f"https://api.telegram.org/bot{token}/getUpdates",
                params={"offset": _tg_poll_offset, "timeout": 30,
                        "allowed_updates": ["message"]},
                timeout=35, verify=False,
            )
            updates = resp.json().get("result", [])
            for upd in updates:
                _tg_poll_offset = upd["update_id"] + 1
                msg = upd.get("message", {})
                from_chat = str(msg.get("chat", {}).get("id", ""))
                text = msg.get("text", "").strip()

                if not text or not text.startswith("/"):
                    continue
                if from_chat != my_chat:
                    print(f"[TG 수신] 허가되지 않은 chat_id({from_chat}) 무시")
                    continue

                print(f"[TG 수신] 명령: {text}")
                reply = _tg_handle_command(text, from_chat)
                if reply is not None:
                    _tg_send(reply, msg_type="cmd_response")
        except Exception as e:
            if "timed out" not in str(e).lower():
                print(f"[TG 수신] 오류: {e}")
            time.sleep(5)

threading.Thread(target=_tg_poll_loop, daemon=True).start()

# ============================================================
#  브라우저 종료 감지 (핑 워치독)
# ============================================================
_last_ping    = time.time()
_PING_TIMEOUT = 300  # 초: 5분간 핑 없으면 종료 (잠금/최소화 대응)
_PING_GRACE   = 60   # 초: 최초 브라우저 접속 대기 시간
_NO_BROWSER   = False  # --no-browser 모드 플래그 (main에서 설정)

def _watchdog():
    time.sleep(_PING_GRACE)     # 최초 접속 전 여유 시간 (이 사이 main에서 _NO_BROWSER 설정됨)
    if _NO_BROWSER:
        return  # 백그라운드 모드에서는 워치독 비활성화
    while True:
        time.sleep(10)
        if time.time() - _last_ping > _PING_TIMEOUT:
            print("[종료] 브라우저 연결 끊김 → 서버 종료")
            os._exit(0)

threading.Thread(target=_watchdog, daemon=True).start()

# ============================================================
#  라우트
# ============================================================
@app.route("/")
def index():
    resp = render_template("index.html")
    from flask import make_response
    r = make_response(resp)
    r.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    r.headers["Pragma"] = "no-cache"
    return r

@app.route("/api/portfolio")
def api_portfolio():
    with _data_lock:
        data = dict(_latest_data)
    if not data:
        data = build_portfolio()
        data["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data["refresh_interval"] = _INTERVAL
    return jsonify(data)

@app.route("/api/history")
def api_history():
    return jsonify(_load_history().get("records", []))

_news_cache = {}   # {code: {"ts": float, "items": list}}
_NEWS_TTL   = 300  # 5분 캐시

def _fetch_stock_news(code: str) -> list:
    """네이버 금융 종목 뉴스 스크래핑 (캐시 5분)"""
    import re, html as html_module
    now = time.time()
    if code in _news_cache and now - _news_cache[code]["ts"] < _NEWS_TTL:
        return _news_cache[code]["items"]
    try:
        url = f"https://finance.naver.com/item/news_news.naver?code={code}&page=1"
        hdrs = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            "Referer": f"https://finance.naver.com/item/main.naver?code={code}",
            "Accept-Language": "ko-KR,ko;q=0.9",
        }
        r = requests.get(url, headers=hdrs, timeout=8, verify=False)
        r.encoding = "euc-kr"
        text = r.text
        items = []
        seen = set()
        # 각 뉴스 행 파싱 (중첩 table 포함)
        for row in re.findall(r'<tr[^>]*>(.*?)</tr>', text, re.DOTALL):
            # 제목+링크 (class="tit" 앵커)
            m_title = re.search(r'<td class="title">\s*<a[^>]+href="([^"]+)"[^>]*class="tit"[^>]*>(.*?)</a>', row, re.DOTALL)
            if not m_title:
                m_title = re.search(r'<td class="title">\s*<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', row, re.DOTALL)
            # 출처
            m_src  = re.search(r'<td class="info">(.*?)</td>', row, re.DOTALL)
            # 날짜
            m_date = re.search(r'<td class="date">(.*?)</td>', row, re.DOTALL)
            if not (m_title and m_src and m_date):
                continue
            href   = m_title.group(1).strip()
            title  = html_module.unescape(re.sub(r'<[^>]+>', '', m_title.group(2))).strip()
            source = re.sub(r'<[^>]+>', '', m_src.group(1)).strip()
            date   = re.sub(r'<[^>]+>', '', m_date.group(1)).strip()
            if not title or href in seen:
                continue
            seen.add(href)
            if href.startswith("/"):
                href = "https://finance.naver.com" + href
            items.append({"title": title, "source": source, "date": date, "url": href})
        _news_cache[code] = {"ts": now, "items": items[:20]}
        return _news_cache[code]["items"]
    except Exception as e:
        print(f"[뉴스] {code} 조회 오류: {e}")
        return []

@app.route("/api/news/<code>")
def api_news(code):
    code = code.zfill(6)
    return jsonify(_fetch_stock_news(code))

@app.route("/api/analysis_prompt", methods=["GET", "POST"])
def api_analysis_prompt():
    """분석 프롬프트 조회/저장"""
    if request.method == "GET":
        try:
            with open(CONFIG_FILE, encoding="utf-8") as f:
                return jsonify({"prompt": json.load(f).get("analysis_prompt", "")})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    # POST: 저장
    data = request.get_json(silent=True) or {}
    new_prompt = data.get("prompt", "").strip()
    if not new_prompt:
        return jsonify({"error": "프롬프트가 비어 있습니다."}), 400
    try:
        with open(CONFIG_FILE, encoding="utf-8") as f:
            cfg = json.load(f)
        cfg["analysis_prompt"] = new_prompt
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
        _config["analysis_prompt"] = new_prompt
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/tg_history")
def api_tg_history():
    try:
        if os.path.exists(TG_HISTORY_FILE):
            with open(TG_HISTORY_FILE, encoding="utf-8") as f:
                return jsonify(json.load(f).get("messages", []))
        return jsonify([])
    except Exception:
        return jsonify([])

@app.route("/api/ping", methods=["POST"])
def api_ping():
    global _last_ping
    _last_ping = time.time()
    return "", 204

# ── WebSocket 이벤트 핸들러 ──
@socketio.on('connect')
def handle_ws_connect():
    """클라이언트 접속 시 현재 캐시 데이터 즉시 전송"""
    with _data_lock:
        if _latest_data:
            socketio_emit('portfolio', dict(_latest_data))

@socketio.on('disconnect')
def handle_ws_disconnect():
    pass

@app.route("/api/stream")
def api_stream():
    """SSE 실시간 스트림"""
    q = Queue(maxsize=10)
    _sse_listeners.append(q)

    # 현재 캐시 즉시 전송
    with _data_lock:
        if _latest_data:
            q.put_nowait(dict(_latest_data))

    def generate():
        try:
            while True:
                try:
                    data = q.get(timeout=25)
                    yield f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
                except Empty:
                    yield ": heartbeat\n\n"   # 연결 유지용 ping
        except GeneratorExit:
            pass
        finally:
            if q in _sse_listeners:
                _sse_listeners.remove(q)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":    "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":       "keep-alive",
        },
    )

@app.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])
    results = []
    if q.isdigit():
        code = q.zfill(6)
        pi = get_price(code)
        if pi:
            if not pi.get("name"):
                npi = _naver.get_price(code)
                pi["name"] = (npi.get("name") if npi else "") or code
            results.append(pi)
    else:
        matches = _naver.search(q)
        for m in matches[:6]:
            pi = get_price(m["code"])
            if pi:
                if not pi.get("name"):
                    pi["name"] = m.get("name", m["code"])
                results.append(pi)
    return jsonify(results)

def _is_korean(text: str) -> bool:
    return any('\uAC00' <= c <= '\uD7A3' or '\u3131' <= c <= '\u314E' for c in text)

def _translate_ko_to_en(text: str) -> str:
    """Google Translate 비공식 API로 한국어 → 영어 번역"""
    cache_key = f"ko2en:{text}"
    if cache_key in _translate_cache:
        return _translate_cache[cache_key]
    try:
        r = requests.get(
            "https://translate.googleapis.com/translate_a/single",
            params={"client": "gtx", "sl": "ko", "tl": "en", "dt": "t", "q": text},
            timeout=5, verify=False,
        )
        parts = r.json()[0]
        translated = "".join(p[0] for p in parts if p and p[0])
        if translated:
            _translate_cache[cache_key] = translated
            return translated
    except Exception:
        pass
    return text

# Yahoo Finance exchange code → KIS exchange code 매핑
_YAHOO_EXCH_MAP = {
    "NMS": "NAS", "NGM": "NAS", "NCM": "NAS",  # NASDAQ
    "NYQ": "NYS",                                # NYSE
    "PCX": "AMS",                                # AMEX
    "NAS": "NAS", "NYS": "NYS", "AMS": "AMS",   # 이미 KIS 형식
}

@app.route("/api/us/search")
def api_us_search():
    """미국 주식 종목 검색 (Yahoo Finance, 한국어 지원)"""
    q_raw = request.args.get("q", "").strip()
    if not q_raw:
        return jsonify([])

    # 한국어 쿼리면 영어로 번역 후 검색
    if _is_korean(q_raw):
        q_en = _translate_ko_to_en(q_raw)
    else:
        q_en = q_raw.upper()

    # config의 us_portfolio에서 한국어 이름 직접 매칭
    cfg = load_config()
    portfolio_matches = []
    for item in cfg.get("us_portfolio", []):
        name_ko = item.get("name", "")
        if q_raw in name_ko or name_ko in q_raw:
            portfolio_matches.append(item.get("ticker", ""))

    try:
        r = requests.get(
            "https://query1.finance.yahoo.com/v1/finance/search",
            params={"q": q_en, "lang": "en-US", "region": "US", "quotesCount": 8, "newsCount": 0},
            headers={"User-Agent": "Mozilla/5.0"}, timeout=6, verify=False,
        )
        quotes = r.json().get("quotes", [])

        # portfolio 직접 매칭 티커를 맨 앞으로, 비주요 거래소 후순위
        _MAJOR = {"NMS", "NGM", "NCM", "NYQ", "PCX", "NAS", "NYS", "AMS"}
        def _sort_key(qt):
            is_match  = 0 if qt.get("symbol", "") in portfolio_matches else 1
            is_major  = 0 if qt.get("exchange", "") in _MAJOR else 1
            return (is_match, is_major)
        quotes.sort(key=_sort_key)

        # 주요 거래소 종목만 필터 (최대 6개)
        candidates = []
        for qt in quotes:
            if qt.get("quoteType") not in ("EQUITY", "ETF"):
                continue
            exch_yahoo = qt.get("exchange", "")
            exch_kis   = _YAHOO_EXCH_MAP.get(exch_yahoo)
            if not exch_kis:
                continue  # 주요 거래소(NAS/NYS/AMS)가 아니면 제외
            candidates.append((qt.get("symbol", ""), qt.get("longname") or qt.get("shortname") or qt.get("symbol",""), exch_kis))
            if len(candidates) >= 6:
                break

        # 가격 병렬 조회
        from concurrent.futures import ThreadPoolExecutor, as_completed
        price_map = {}
        def _fetch(ticker, exch_kis):
            return ticker, _kis_api.fetch_us_price(ticker, exch_kis) or {}
        with ThreadPoolExecutor(max_workers=len(candidates) or 1) as ex:
            for ticker, h in ex.map(lambda c: _fetch(c[0], c[2]), candidates):
                price_map[ticker] = h

        results = []
        for ticker, name, exch_kis in candidates:
            h = price_map.get(ticker, {})
            results.append({
                "ticker":        ticker,
                "name":          name,
                "exchange":      exch_kis,
                "current_price": h.get("current_price", 0),
                "change":        h.get("change", 0),
                "change_rate":   h.get("change_rate", 0),
                "volume":        h.get("volume", 0),
            })
        return jsonify(results)
    except Exception as e:
        print(f"[US 검색] 오류: {e}")
        return jsonify([])


@app.route("/api/price/<code>")
def api_price(code):
    code = code.zfill(6)
    pi   = get_price(code)
    if not pi:
        return jsonify({"error": "조회 실패"}), 404
    if not pi.get("name"):
        npi = _naver.get_price(code)
        pi["name"] = (npi.get("name") if npi else "") or code
    return jsonify(pi)

def _force_refresh():
    """포트폴리오 즉시 재빌드 후 SSE 푸시 (백그라운드)"""
    def _do():
        try:
            data = build_portfolio()
            data["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with _data_lock:
                _latest_data.clear()
                _latest_data.update(data)
            _push_to_all(data)
        except Exception as e:
            print(f"[강제갱신] 오류: {e}")
    threading.Thread(target=_do, daemon=True).start()

@app.route("/api/holdings", methods=["GET"])
def api_holdings_get():
    return jsonify(_config.get("portfolio", []))

@app.route("/api/holdings", methods=["POST"])
def api_holdings_add():
    body  = request.get_json()
    code  = str(body.get("code", "")).strip().zfill(6)
    qty   = int(body.get("quantity", 0))
    avg_p = float(body.get("avg_price", 0))
    if not code or qty <= 0 or avg_p <= 0:
        return jsonify({"error": "입력값 오류"}), 400

    portfolio = _config.get("portfolio", [])
    for item in portfolio:
        if str(item["code"]) == code:
            return jsonify({"error": f"{code} 이미 등록된 종목"}), 400

    pi          = get_price(code)
    passed_name = str(body.get("name", "")).strip()
    api_name    = (pi.get("name") if pi else "") or ""
    if not api_name and not passed_name:
        npi      = _naver.get_price(code)
        api_name = (npi.get("name") if npi else "") or ""
    name = api_name or passed_name or code

    portfolio.append({"code": code, "name": name, "market": "KR", "quantity": qty, "avg_price": avg_p})
    _config["portfolio"] = portfolio
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(_config, f, ensure_ascii=False, indent=2)
    _force_refresh()
    return jsonify({"ok": True, "name": name})

@app.route("/api/holdings/<code>", methods=["DELETE"])
def api_holdings_delete(code):
    code      = code.zfill(6)
    portfolio = _config.get("portfolio", [])
    new_list  = [p for p in portfolio if str(p["code"]) != code]
    if len(new_list) == len(portfolio):
        return jsonify({"error": "종목 없음"}), 404
    _config["portfolio"] = new_list
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(_config, f, ensure_ascii=False, indent=2)
    _force_refresh()
    return jsonify({"ok": True})

@app.route("/api/holdings/<code>", methods=["PUT"])
def api_holdings_update(code):
    code  = code.zfill(6)
    body  = request.get_json()
    qty   = int(body.get("quantity", 0))
    avg_p = float(body.get("avg_price", 0))
    if qty <= 0 or avg_p <= 0:
        return jsonify({"error": "입력값 오류"}), 400
    portfolio = _config.get("portfolio", [])
    for item in portfolio:
        if str(item["code"]) == code:
            item["quantity"]  = qty
            item["avg_price"] = avg_p
            _config["portfolio"] = portfolio
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(_config, f, ensure_ascii=False, indent=2)
            _force_refresh()
            return jsonify({"ok": True})
    return jsonify({"error": "종목 없음"}), 404


@app.route("/api/chart/<code>")
def api_chart(code):
    code   = code.zfill(6)
    period = request.args.get("period", "D").upper()   # D / W / M
    if period not in ("D", "W", "M"):
        period = "D"

    today      = datetime.now().strftime("%Y-%m-%d")
    cache_file = os.path.join(CHART_CACHE_DIR, f"{code}_{period}.json")

    # 캐시가 오늘 업데이트되어 있으면 즉시 반환 (일봉은 500개 미만 구 캐시 재조회)
    if os.path.exists(cache_file):
        try:
            with open(cache_file, encoding="utf-8") as f:
                cached = json.load(f)
            min_rec = {"D": 500, "W": 50, "M": 20}[period]
            enough = len(cached.get("data", [])) >= min_rec
            if cached.get("updated") == today and enough:
                return jsonify(cached)
        except Exception:
            pass

    if not _kis_api:
        return jsonify({"error": "KIS API 미설정"}), 503

    # 기간별 조회 범위 (일봉은 내부 페이징으로 2000개)
    to_dt = datetime.now().strftime("%Y%m%d")
    rows  = _kis_api.get_ohlcv(code, to_dt, period)

    # 종목명
    name = code
    for item in _config.get("portfolio", []):
        if str(item.get("code","")).zfill(6) == code:
            name = item.get("name", code)
            break

    result = {"code": code, "name": name, "period": period, "updated": today, "data": rows}
    try:
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False)
    except Exception:
        pass

    return jsonify(result)


@app.route("/api/kospi")
def api_kospi():
    """히스토리 날짜 범위의 KOSPI 일봉 종가 반환"""
    records = _load_history().get("records", [])
    if not records:
        return jsonify([])
    dates = sorted(r["date"] for r in records if r.get("date"))
    if not dates:
        return jsonify([])

    from_date = dates[0].replace("-", "")
    to_date   = datetime.now().strftime("%Y%m%d")

    if not _kis_api:
        return jsonify({"error": "KIS API 미설정"}), 503

    rows = _kis_api.get_index_daily("0001", from_date, to_date)
    print(f"[API] /api/kospi → {len(rows)}개 ({from_date}~{to_date})")
    return jsonify(rows)


@app.route("/api/kospi/now")
def api_kospi_now():
    """KOSPI 지수 현재값 반환"""
    if not _kis_api:
        return jsonify({"error": "KIS API 미설정"}), 503
    try:
        resp = _kis_api._session.get(
            f"{_kis_api.base_url}/uapi/domestic-stock/v1/quotations/inquire-index-price",
            headers=_kis_api._h("FHPUP02100000"),
            params={"FID_COND_MRKT_DIV_CODE": "U", "FID_INPUT_ISCD": "0001"},
            timeout=5, verify=False,
        )
        d = resp.json()
        if d.get("rt_cd") == "0":
            o = d.get("output", {})
            val = float(o.get("bstp_nmix_prpr") or o.get("bstp_nmix_clpr") or 0)
            if val > 0:
                return jsonify({"value": val})
    except Exception as e:
        print(f"[KIS] KOSPI 현재값 오류: {e}")
    return jsonify({"error": "조회 실패"}), 503


def _fetch_nasdaq_history(from_date: str) -> list:
    """Yahoo Finance v8 API로 NASDAQ Composite(^IXIC) 일봉 조회"""
    import calendar
    try:
        dt_from = datetime.strptime(from_date, "%Y-%m-%d")
        dt_to   = datetime.now()
        p1 = int(calendar.timegm(dt_from.timetuple()))
        p2 = int(calendar.timegm(dt_to.timetuple()))
        url = (f"https://query1.finance.yahoo.com/v8/finance/chart/%5EIXIC"
               f"?period1={p1}&period2={p2}&interval=1d")
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        data = r.json()
        result_data = data["chart"]["result"][0]
        timestamps  = result_data["timestamp"]
        closes      = result_data["indicators"]["quote"][0]["close"]
        rows = []
        for ts, c in zip(timestamps, closes):
            if c is None:
                continue
            d = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
            rows.append({"date": d, "close": round(c, 2)})
        rows.sort(key=lambda x: x["date"])
        print(f"[NASDAQ] Yahoo Finance → {len(rows)}개 ({from_date}~)")
        return rows
    except Exception as e:
        print(f"[NASDAQ] 조회 오류: {e}")
        return []


@app.route("/api/nasdaq")
def api_nasdaq():
    """US 히스토리 날짜 범위의 NASDAQ Composite 일봉 종가 (Yahoo Finance)"""
    records = _load_us_history().get("records", [])
    dates = sorted(r["date"] for r in records if r.get("date") and r.get("total_eval_krw"))
    if not dates:
        return jsonify([])
    rows = _fetch_nasdaq_history(dates[0])
    return jsonify(rows)


@app.route("/api/nasdaq/now")
def api_nasdaq_now():
    """NASDAQ 지수 현재값 (Yahoo Finance)"""
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/%5EIXIC?interval=1m&range=1d"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
        data = r.json()
        meta = data["chart"]["result"][0]["meta"]
        val  = float(meta.get("regularMarketPrice") or meta.get("previousClose") or 0)
        if val > 0:
            return jsonify({"value": val})
    except Exception as e:
        print(f"[NASDAQ now] 오류: {e}")
    return jsonify({"error": "조회 실패"}), 503


# ============================================================
#  미국 주식 라우트
# ============================================================
_us_news_cache: dict = {}
_translate_cache: dict = {}

def _translate_to_ko(text: str) -> str:
    """Google Translate 비공식 API로 영문 → 한국어 번역 (캐시)"""
    if not text or text in _translate_cache:
        return _translate_cache.get(text, text)
    try:
        r = requests.get(
            "https://translate.googleapis.com/translate_a/single",
            params={"client": "gtx", "sl": "en", "tl": "ko", "dt": "t", "q": text},
            timeout=5, verify=False,
        )
        parts = r.json()[0]
        translated = "".join(p[0] for p in parts if p and p[0])
        if translated:
            _translate_cache[text] = translated
            return translated
    except Exception:
        pass
    return text

def _fetch_us_news(ticker: str) -> list:
    """Yahoo Finance RSS 뉴스 스크래핑 (5분 캐시)"""
    import xml.etree.ElementTree as ET
    now = time.time()
    t = ticker.upper()
    if t in _us_news_cache and now - _us_news_cache[t]["ts"] < 300:
        return _us_news_cache[t]["items"]
    try:
        url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={t}&region=US&lang=en-US"
        r = requests.get(url, timeout=8,
                         headers={"User-Agent": "Mozilla/5.0"}, verify=False)
        root = ET.fromstring(r.content)
        items = []
        for item in root.findall(".//item"):
            title = (item.findtext("title") or "").strip()
            link  = (item.findtext("link")  or "").strip()
            pub   = (item.findtext("pubDate") or "").strip()
            try:
                from email.utils import parsedate_to_datetime
                date_str = parsedate_to_datetime(pub).strftime("%Y.%m.%d %H:%M")
            except Exception:
                date_str = pub[:16] if pub else ""
            src = ""
            for el in item:
                if el.tag.endswith("creator"):
                    src = el.text or ""
                    break
            if not src and link:
                import re as _re
                m = _re.search(r"https?://(?:www\.)?([^/]+)", link)
                src = m.group(1) if m else ""
            if title and link:
                items.append({"title": title, "url": link, "source": src, "date": date_str})
        # 제목 한국어 번역 (병렬)
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=5) as ex:
            titles_ko = list(ex.map(lambda it: _translate_to_ko(it["title"]), items[:20]))
        for i, ko in enumerate(titles_ko):
            items[i]["title_ko"] = ko
        _us_news_cache[t] = {"ts": now, "items": items[:20]}
        return items[:20]
    except Exception as e:
        print(f"[US 뉴스] {ticker} 오류: {e}")
        return []


@app.route("/api/us/history")
def api_us_history():
    return jsonify(_load_us_history().get("records", []))


@app.route("/api/us/holdings", methods=["GET"])
def api_us_holdings_get():
    return jsonify(_config.get("us_portfolio", []))


@app.route("/api/us/holdings", methods=["POST"])
def api_us_holdings_add():
    body   = request.get_json()
    ticker = str(body.get("ticker", "")).strip().upper()
    qty    = float(body.get("quantity", 0))
    avg_p  = float(body.get("avg_price", 0))
    if not ticker or qty <= 0 or avg_p <= 0:
        return jsonify({"error": "입력값 오류"}), 400

    us_portfolio = _config.get("us_portfolio", [])
    for item in us_portfolio:
        if item.get("ticker", "").upper() == ticker:
            return jsonify({"error": f"{ticker} 이미 등록된 종목"}), 400

    # 종목명 / 거래소 자동 조회
    name     = str(body.get("name", "")).strip() or ticker
    exchange = str(body.get("exchange", "")).strip().upper() or None
    if _kis_api:
        pi = _kis_api.fetch_us_price(ticker, exchange)
        if pi:
            exchange = pi.get("exchange", exchange or "")
            if not body.get("name"):
                name = ticker  # KIS API는 영문 이름 미제공

    us_portfolio.append({
        "ticker":    ticker,
        "name":      name,
        "exchange":  exchange or "",
        "quantity":  qty,
        "avg_price": avg_p,
    })
    _config["us_portfolio"] = us_portfolio
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(_config, f, ensure_ascii=False, indent=2)
    _force_refresh()
    return jsonify({"ok": True, "name": name, "exchange": exchange or ""})


@app.route("/api/us/holdings/<ticker>", methods=["DELETE"])
def api_us_holdings_delete(ticker):
    ticker = ticker.upper()
    us_portfolio = _config.get("us_portfolio", [])
    new_list = [p for p in us_portfolio if p.get("ticker", "").upper() != ticker]
    if len(new_list) == len(us_portfolio):
        return jsonify({"error": "종목 없음"}), 404
    _config["us_portfolio"] = new_list
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(_config, f, ensure_ascii=False, indent=2)
    _force_refresh()
    return jsonify({"ok": True})


@app.route("/api/us/holdings/<ticker>", methods=["PUT"])
def api_us_holdings_update(ticker):
    ticker = ticker.upper()
    body   = request.get_json()
    qty    = float(body.get("quantity", 0))
    avg_p  = float(body.get("avg_price", 0))
    if qty <= 0 or avg_p <= 0:
        return jsonify({"error": "입력값 오류"}), 400
    us_portfolio = _config.get("us_portfolio", [])
    for item in us_portfolio:
        if item.get("ticker", "").upper() == ticker:
            item["quantity"]  = qty
            item["avg_price"] = avg_p
            if body.get("name"):
                item["name"] = body["name"]
            _config["us_portfolio"] = us_portfolio
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(_config, f, ensure_ascii=False, indent=2)
            _force_refresh()
            return jsonify({"ok": True})
    return jsonify({"error": "종목 없음"}), 404


@app.route("/api/us/news/<ticker>")
def api_us_news(ticker):
    return jsonify(_fetch_us_news(ticker.upper()))


@app.route("/api/us/chart/<ticker>")
def api_us_chart(ticker):
    ticker = ticker.upper()
    period = request.args.get("period", "D").upper()
    if period not in ("D", "W", "M"):
        period = "D"
    if not _kis_api:
        return jsonify({"error": "KIS API 미설정"}), 503

    today      = datetime.now().strftime("%Y-%m-%d")
    cache_file = os.path.join(CHART_CACHE_DIR, f"US_{ticker}_{period}.json")

    # 오늘 캐시가 충분하면 즉시 반환
    if os.path.exists(cache_file):
        try:
            with open(cache_file, encoding="utf-8") as f:
                cached = json.load(f)
            min_rec = {"D": 200, "W": 50, "M": 20}[period]
            if cached.get("updated") == today and len(cached.get("data", [])) >= min_rec:
                return jsonify(cached)
        except Exception:
            pass

    # 거래소 찾기
    exchange = ""
    for it in _config.get("us_portfolio", []):
        if it.get("ticker", "").upper() == ticker:
            exchange = it.get("exchange", "")
            break
    if not exchange:
        pi = _kis_api.fetch_us_price(ticker)
        exchange = (pi or {}).get("exchange", "NAS")

    to_date = datetime.now().strftime("%Y%m%d")
    rows    = _kis_api.get_us_ohlcv(ticker, exchange, to_date, period)

    name = ticker
    for it in _config.get("us_portfolio", []):
        if it.get("ticker", "").upper() == ticker:
            name = it.get("name", ticker)
            break

    result = {"ticker": ticker, "name": name, "exchange": exchange,
              "period": period, "updated": today, "data": rows}
    try:
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False)
    except Exception:
        pass

    return jsonify(result)


@app.route("/api/exchange_rate")
def api_exchange_rate():
    return jsonify({"usd_krw": get_usd_krw_rate()})


def _run_catchup_batch_if_needed():
    """서버 시작 시 누락된 US 히스토리가 있으면 배치 자동 실행 (백그라운드)"""
    import subprocess

    def _last_us_trading_day():
        """마감 완료된 직전 미국 거래일
        미국 정규장 마감: 서머타임 KST 05:00 / 일반 KST 06:00
        KST 마감 시간이 지났으면 어제(KST) = 직전 미국 거래일 세션
        마감 전이면 그보다 하루 더 이전 세션"""
        now = datetime.now()
        m = now.month
        if m < 3 or m > 11:
            is_dst = False
        elif 3 < m < 11:
            is_dst = True
        elif m == 3:
            sun2 = 8 + (6 - datetime(now.year, 3, 1).weekday()) % 7
            is_dst = now.day >= sun2
        else:  # m == 11
            sun1 = 1 + (6 - datetime(now.year, 11, 1).weekday()) % 7
            is_dst = now.day < sun1
        close_kst = dtime(5, 0) if is_dst else dtime(6, 0)
        # 마감 시간 이후면 어제가 US 거래일, 이전이면 그제부터
        offset = 1 if now.time() >= close_kst else 2
        dt = now - timedelta(days=offset)
        while True:
            s = dt.strftime("%Y-%m-%d")
            if dt.weekday() < 5 and s not in US_HOLIDAYS:
                return s
            dt -= timedelta(days=1)

    def run():
        try:
            history = _load_us_history()
            records = history.get("records", [])
            last_saved = records[0]["date"] if records else None
            latest_us  = _last_us_trading_day()

            if last_saved and last_saved >= latest_us:
                print(f"[US 배치] 최신 상태 ({last_saved}), 스킵")
                return

            from_date = (datetime.strptime(last_saved, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d") \
                        if last_saved else "2026-03-01"
            print(f"[US 배치] 누락 감지 ({last_saved} → {latest_us}), 자동 실행: --from {from_date}")

            batch_script = os.path.join(_BASE_DIR, "batch_catchup_us.py")
            python_exe   = sys.executable
            result = subprocess.run(
                [python_exe, batch_script, "--from", from_date],
                capture_output=True, text=True, encoding="utf-8", errors="replace",
                timeout=300,
            )
            if result.returncode == 0:
                print(f"[US 배치] 완료\n{result.stdout[-800:]}")
            else:
                print(f"[US 배치] 오류 (code={result.returncode})\n{result.stderr[-400:]}")
        except Exception as e:
            print(f"[US 배치] 예외: {e}")

    threading.Thread(target=run, daemon=True).start()


# ============================================================
#  코인 (업비트)
# ============================================================

_UPBIT_BASE = "https://api.upbit.com/v1"
_upbit_market_cache: dict = {}   # {"BTC": "KRW-BTC", ...}
_coin_price_cache: dict = {}     # {market: {data, ts}}
_COIN_PRICE_TTL = 1              # 1초 캐시

def _upbit_headers():
    return {"Accept": "application/json"}

def _load_upbit_markets():
    """업비트 KRW 마켓 목록 로드 (캐시)"""
    global _upbit_market_cache
    if _upbit_market_cache:
        return _upbit_market_cache
    try:
        r = requests.get(f"{_UPBIT_BASE}/market/all", headers=_upbit_headers(), timeout=5, verify=False)
        for m in r.json():
            code = m.get("market", "")
            if code.startswith("KRW-"):
                ticker = code[4:]
                _upbit_market_cache[ticker] = {"market": code, "korean_name": m.get("korean_name", ticker), "english_name": m.get("english_name", ticker)}
    except Exception as e:
        print(f"[Upbit] 마켓 로드 오류: {e}")
    return _upbit_market_cache

def _upbit_get_prices(tickers: list) -> dict:
    """업비트 현재가 조회 (캐시 5초)"""
    markets_info = _load_upbit_markets()
    markets = [markets_info[t]["market"] for t in tickers if t in markets_info]
    if not markets:
        return {}
    now = time.time()
    result = {}
    missing = []
    for m in markets:
        c = _coin_price_cache.get(m)
        if c and now - c["ts"] < _COIN_PRICE_TTL:
            result[m] = c["data"]
        else:
            missing.append(m)
    if missing:
        try:
            r = requests.get(f"{_UPBIT_BASE}/ticker",
                             params={"markets": ",".join(missing)},
                             headers=_upbit_headers(), timeout=5, verify=False)
            for item in r.json():
                m = item.get("market", "")
                _coin_price_cache[m] = {"data": item, "ts": now}
                result[m] = item
        except Exception as e:
            print(f"[Upbit] 가격 조회 오류: {e}")
    return result

def _load_coin_history():
    if os.path.exists(COIN_HISTORY_FILE):
        try:
            with open(COIN_HISTORY_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"records": []}

def _save_coin_history(data: dict):
    with open(COIN_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def build_coin_portfolio():
    holdings_cfg = _config.get("coin_portfolio", [])
    markets_info = _load_upbit_markets()
    tickers = [h["ticker"] for h in holdings_cfg if h.get("ticker")]
    prices  = _upbit_get_prices(tickers)

    total_buy = 0
    total_eval = 0
    holdings_out = []
    for h in holdings_cfg:
        ticker = h.get("ticker", "").upper()
        qty    = float(h.get("quantity", 0))
        avg    = float(h.get("avg_price", 0))
        minfo  = markets_info.get(ticker, {})
        market = minfo.get("market", f"KRW-{ticker}")
        pdata  = prices.get(market, {})
        cur    = float(pdata.get("trade_price", 0))
        chg    = float(pdata.get("signed_change_price", 0))
        rate   = float(pdata.get("signed_change_rate", 0)) * 100
        buy_amt  = qty * avg
        eval_amt = qty * cur if cur > 0 else buy_amt
        pnl      = eval_amt - buy_amt
        pr       = (pnl / buy_amt * 100) if buy_amt > 0 else 0
        total_buy  += buy_amt
        total_eval += eval_amt
        holdings_out.append({
            "ticker":        ticker,
            "name":          minfo.get("korean_name", ticker),
            "english_name":  minfo.get("english_name", ticker),
            "quantity":      qty,
            "avg_price":     avg,
            "current_price": cur,
            "change":        chg,
            "change_rate":   rate,
            "buy_amount":    buy_amt,
            "eval_amount":   eval_amt,
            "profit":        pnl,
            "profit_rate":   pr,
        })
    total_profit = total_eval - total_buy
    total_rate   = (total_profit / total_buy * 100) if total_buy > 0 else 0
    return {
        "holdings":      holdings_out,
        "total_buy":     total_buy,
        "total_eval":    total_eval,
        "total_profit":  total_profit,
        "total_rate":    total_rate,
        "count":         len(holdings_out),
        "updated_at":    datetime.now().strftime("%H:%M:%S"),
    }

def _save_coin_daily_snapshot():
    """오늘 코인 스냅샷 저장 (중복 방지)"""
    today = datetime.now().strftime("%Y-%m-%d")
    data  = _load_coin_history()
    recs  = data.get("records", [])
    if recs and recs[0]["date"] == today:
        return
    try:
        port = build_coin_portfolio()
        if port["total_eval"] <= 0:
            return
        prev_eval = recs[0]["total_eval"] if recs else 0
        recs.insert(0, {
            "date":         today,
            "total_buy":    port["total_buy"],
            "total_eval":   port["total_eval"],
            "total_profit": port["total_profit"],
            "total_profit_rate": port["total_rate"],
            "daily_change": port["total_eval"] - prev_eval,
        })
        _save_coin_history({"records": recs})
    except Exception as e:
        print(f"[코인 스냅샷] 오류: {e}")

def _load_combined_history():
    if os.path.exists(COMBINED_HISTORY_FILE):
        try:
            with open(COMBINED_HISTORY_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"records": []}

def _save_combined_history(data: dict):
    with open(COMBINED_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

_combined_snapshot_done_date = None  # 당일 종합 배치 실행 여부

def _save_combined_daily_snapshot():
    """매일 09:00~09:10 KST 종합 스냅샷 저장 (주말·휴장일 포함).
    코인 배치(익일 09:00)와 동일 시간대에 실행하여 전날(yesterday) 데이터를 저장.
    - 국내/미국: 전날 이하의 가장 최근 레코드 사용 (휴장일이면 마지막 거래일 값)
    - 코인: coin_history.json의 전날 레코드 사용 (코인 배치가 저장한 값)
    """
    global _combined_snapshot_done_date
    now       = datetime.now()
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")

    # 하루 1회 (09:00~09:10 사이)
    if not (dtime(9, 0) <= now.time() < dtime(9, 10)):
        return
    if _combined_snapshot_done_date == yesterday:
        return

    # KR: 전날 이하 가장 최근 non-realtime 레코드
    kr_recs = _load_history().get("records", [])
    kr = next(
        (r for r in kr_recs if not r.get("realtime") and r.get("date", "") <= yesterday),
        kr_recs[0] if kr_recs else {}
    )
    kr_buy    = kr.get("total_buy", 0)
    kr_eval   = kr.get("total_eval", 0)
    kr_profit = kr.get("total_profit", 0)
    kr_date   = kr.get("date", "")

    # US: 전날 이하 가장 최근 레코드
    us_recs = _load_us_history().get("records", [])
    us = next(
        (r for r in us_recs if r.get("date", "") <= yesterday),
        us_recs[0] if us_recs else {}
    )
    us_buy    = us.get("total_buy_krw", 0)
    us_eval   = us.get("total_eval_krw", 0)
    us_profit = us.get("total_profit_krw", 0)
    us_date   = us.get("date", "")

    # Coin: coin_history.json의 전날 레코드 (코인 배치 09:00 결과)
    coin_recs = _load_coin_history().get("records", [])
    coin = next((r for r in coin_recs if r.get("date") == yesterday), None)
    if not coin:
        print(f"[종합 히스토리] {yesterday} 코인 레코드 없음 — 스킵 (코인 배치 완료 후 재시도 필요)")
        return
    coin_buy    = coin.get("total_buy", 0)
    coin_eval   = coin.get("total_eval", 0)
    coin_profit = coin.get("total_profit", 0)

    total_buy    = kr_buy + us_buy + coin_buy
    total_eval   = kr_eval + us_eval + coin_eval
    total_profit = kr_profit + us_profit + coin_profit
    total_rate   = round(total_profit / total_buy * 100, 2) if total_buy else 0.0

    combined_data = _load_combined_history()
    recs = combined_data.get("records", [])

    # 전날 레코드 이미 존재하면 덮어씀
    recs = [r for r in recs if r.get("date") != yesterday]

    prev_rec     = next((r for r in recs if r.get("date", "") < yesterday), None)
    prev_eval    = prev_rec.get("total_eval", 0) if prev_rec else 0
    daily_change = round(total_eval - prev_eval) if prev_eval else 0

    record = {
        "date":         yesterday,
        "total_buy":    round(total_buy),
        "total_eval":   round(total_eval),
        "total_profit": round(total_profit),
        "total_rate":   total_rate,
        "daily_change": daily_change,
        "kr_eval":      round(kr_eval),
        "us_eval":      round(us_eval),
        "coin_eval":    round(coin_eval),
        "kr_date":      kr_date,
        "us_date":      us_date,
    }
    recs.insert(0, record)
    recs.sort(key=lambda r: r["date"], reverse=True)
    _save_combined_history({"records": recs})
    _combined_snapshot_done_date = yesterday
    print(f"[종합 히스토리] {yesterday} 저장 완료 "
          f"(KR={kr_date}, US={us_date}, 합계={round(total_eval):,}원)")
    return True


def _check_coin_rate_alerts(holdings: list):
    """코인 전일대비 등락률 5%·10%·15%… 임계값 최초 도달 시 텔레그램 알림"""
    now_ts = time.time()
    for h in holdings:
        ticker    = h.get("ticker", "")
        name      = h.get("name", ticker)
        rate      = h.get("change_rate", 0.0)   # 업비트 signed_change_rate * 100
        price     = h.get("current_price", 0)
        change    = h.get("change", 0)
        eval_pnl  = h.get("profit", 0)
        eval_rate = h.get("profit_rate", 0.0)

        key_prefix = f"COIN_{ticker}"
        if key_prefix not in _alert_state:
            _alert_state[key_prefix] = {"pct_last_sent": {}, "pct_triggered": {}}
        state = _alert_state[key_prefix]
        if "pct_triggered" not in state:
            state["pct_triggered"] = {}

        for thr in _ALERT_THRESHOLDS:
            for sign, label in [(1, "상승"), (-1, "하락")]:
                skey  = str(sign * thr)
                above = (sign == 1 and rate >= thr) or (sign == -1 and rate <= -thr)

                if not above:
                    state["pct_triggered"].pop(skey, None)
                    continue

                if state["pct_triggered"].get(skey, False):
                    continue

                last_sent = state["pct_last_sent"].get(skey, 0)
                if now_ts - last_sent < _ALERT_COOLDOWN:
                    state["pct_triggered"][skey] = True
                    continue

                state["pct_last_sent"][skey] = now_ts
                state["pct_triggered"][skey] = True
                _save_alert_state()

                msg = (
                    f"<b>[코인 {name}({ticker})] 전일대비 {thr}% {label}</b>\n"
                    f"{'─' * 20}\n"
                    f"현재가   <b>{price:,.0f}원</b>\n"
                    f"전일대비  {'+' if change>=0 else ''}{change:,.0f}원 ({rate:+.2f}%)\n"
                    f"평가손익  <b>{'+' if eval_pnl>=0 else ''}{eval_pnl:,.0f}원</b> ({eval_rate:+.2f}%)\n"
                    f"{datetime.now().strftime('%Y-%m-%d %H:%M')}"
                )
                _tg_send(msg, msg_type="rate_alert")
                print(f"[코인 알림] {name}({ticker}) {thr}% {label} → 텔레그램 발송")


@app.route("/api/coin/portfolio")
def api_coin_portfolio():
    try:
        port = build_coin_portfolio()
        _check_coin_rate_alerts(port.get("holdings", []))
        return jsonify(port)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

_UBMI_CACHE: dict = {"data": [], "ts": 0.0}
_UBMI_TTL = 300  # 5분 캐시

def _fetch_ubmi_data(from_date: str, count: int) -> list:
    """UBMI 일봉 조회 — 여러 엔드포인트 순서대로 시도"""
    headers = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}

    # 1순위: 공식 UBMI 인덱스 엔드포인트
    for base in ["https://crix-api-cdn.upbit.com", "https://crix-api-endpoint.upbit.com"]:
        try:
            r = requests.get(f"{base}/v1/crix/candles/days",
                             params={"code": "IDX.UPBIT.UBMI", "count": min(count, 200)},
                             headers=headers, timeout=8, verify=False)
            if r.status_code == 200:
                raw = r.json()
                if raw and isinstance(raw, list):
                    rows = []
                    for c in raw:
                        dt = c.get("candleDateTimeKst", "")[:10]
                        if dt >= from_date:
                            rows.append({"date": dt, "close": c.get("tradePrice", 0)})
                    rows.sort(key=lambda x: x["date"])
                    print(f"[UBMI] {base} → {len(rows)}일")
                    return rows
        except Exception as e:
            print(f"[UBMI] {base} 실패: {e}")

    # 2순위: BTC 일봉 가격으로 대체 (UBMI 대체 지표)
    print("[UBMI] 공식 API 불가 → BTC 가격으로 대체")
    try:
        r = requests.get("https://crix-api-cdn.upbit.com/v1/crix/candles/days",
                         params={"code": "CRIX.UPBIT.KRW-BTC", "count": min(count, 200)},
                         headers=headers, timeout=8, verify=False)
        if r.status_code == 200:
            raw = r.json()
            rows = []
            for c in raw:
                dt = c.get("candleDateTimeKst", "")[:10]
                if dt >= from_date:
                    rows.append({"date": dt, "close": c.get("tradePrice", 0)})
            rows.sort(key=lambda x: x["date"])
            print(f"[UBMI-BTC] {len(rows)}일")
            return rows
    except Exception as e:
        print(f"[UBMI-BTC] 실패: {e}")

    return []

@app.route("/api/ubmi")
def api_ubmi():
    """UBMI 일봉 데이터 (코인 히스토리 기간 기준)"""
    import time as _time
    now_ts = _time.time()
    if now_ts - _UBMI_CACHE["ts"] < _UBMI_TTL and _UBMI_CACHE["data"] and isinstance(_UBMI_CACHE["data"], dict):
        return jsonify(_UBMI_CACHE["data"])
    try:
        records = _load_coin_history().get("records", [])
        dates = sorted(r["date"] for r in records if r.get("total_eval"))
        if not dates:
            return jsonify([])
        count = (datetime.now().date() - datetime.strptime(dates[0], "%Y-%m-%d").date()).days + 10
        # UBMI 먼저 시도
        ubmi_rows = _fetch_ubmi_data(dates[0], count)
        is_btc_fallback = False
        if not ubmi_rows:
            return jsonify([])
        # BTC 대체 여부 판별: tradePrice 크기로 구분 (BTC는 수천만 단위)
        if ubmi_rows and ubmi_rows[0]["close"] > 1_000_000:
            is_btc_fallback = True
        result = {"rows": ubmi_rows, "source": "BTC" if is_btc_fallback else "UBMI"}
        _UBMI_CACHE["data"] = result
        _UBMI_CACHE["ts"] = now_ts
        return jsonify(result)
    except Exception as e:
        print(f"[UBMI] 오류: {e}")
        return jsonify([])


@app.route("/api/coin/history")
def api_coin_history():
    from datetime import timedelta
    data  = _load_coin_history()
    recs  = list(data.get("records", []))
    now   = datetime.now()

    # 기간 시작일: 09:00 이전 = 어제(현재 기간은 어제 09:00 시작), 이후 = 오늘
    rt_date = (now - timedelta(days=1)).strftime("%Y-%m-%d") if now.hour < 9 else now.strftime("%Y-%m-%d")

    try:
        port = build_coin_portfolio()
        if port["total_eval"] > 0:
            # 기간 시작 기준가: rt_date 저장 레코드의 09:00 시가
            period_ref = next((r for r in recs if r["date"] == rt_date), None)
            if period_ref:
                prev_eval = period_ref["total_eval"]
            else:
                older = [r for r in recs if r["date"] < rt_date]
                prev_eval = older[0]["total_eval"] if older else 0
            rt_rec = {
                "date":              rt_date,
                "total_buy":         port["total_buy"],
                "total_eval":        port["total_eval"],
                "total_profit":      port["total_profit"],
                "total_profit_rate": port["total_rate"],
                "daily_change":      port["total_eval"] - prev_eval,
                "realtime":          True,
            }
            recs = [r for r in recs if r["date"] != rt_date]
            recs.insert(0, rt_rec)
    except Exception:
        pass

    return jsonify(recs)

@app.route("/api/coin/search")
def api_coin_search():
    q = request.args.get("q", "").strip().upper()
    if not q:
        return jsonify([])
    markets_info = _load_upbit_markets()
    matched = []
    for ticker, info in markets_info.items():
        if (q in ticker or
            q in info.get("korean_name", "").upper() or
            q in info.get("english_name", "").upper()):
            matched.append(ticker)
        if len(matched) >= 10:
            break
    if not matched:
        return jsonify([])
    prices = _upbit_get_prices(matched)
    results = []
    for ticker in matched:
        info   = markets_info[ticker]
        market = info["market"]
        pdata  = prices.get(market, {})
        results.append({
            "ticker":        ticker,
            "name":          info.get("korean_name", ticker),
            "english_name":  info.get("english_name", ticker),
            "current_price": float(pdata.get("trade_price", 0)),
            "change":        float(pdata.get("signed_change_price", 0)),
            "change_rate":   float(pdata.get("signed_change_rate", 0)) * 100,
            "volume":        float(pdata.get("acc_trade_volume_24h", 0)),
        })
    return jsonify(results)

@app.route("/api/coin/chart/<ticker>")
def api_coin_chart(ticker):
    ticker = ticker.upper()
    period = request.args.get("period", "D").upper()
    if period not in ("D", "W", "M"):
        period = "D"
    market = f"KRW-{ticker}"
    today  = datetime.now().strftime("%Y-%m-%d")
    cache_file = os.path.join(CHART_CACHE_DIR, f"COIN_{ticker}_{period}.json")
    if os.path.exists(cache_file):
        try:
            with open(cache_file, encoding="utf-8") as f:
                cached = json.load(f)
            min_rec = {"D": 100, "W": 30, "M": 12}[period]
            if cached.get("updated") == today and len(cached.get("data", [])) >= min_rec:
                return jsonify(cached)
        except Exception:
            pass
    ep_map = {"D": "days", "W": "weeks", "M": "months"}
    endpoint = ep_map[period]
    count = {"D": 200, "W": 100, "M": 60}[period]
    try:
        r = requests.get(
            f"{_UPBIT_BASE}/candles/{endpoint}",
            params={"market": market, "count": count},
            headers=_upbit_headers(), timeout=10, verify=False,
        )
        candles = r.json()
        rows = []
        for c in reversed(candles):
            dt = c.get("candle_date_time_kst", "")[:10]
            rows.append({
                "date":   dt,
                "open":   float(c.get("opening_price", 0)),
                "high":   float(c.get("high_price", 0)),
                "low":    float(c.get("low_price", 0)),
                "close":  float(c.get("trade_price", 0)),
                "volume": float(c.get("candle_acc_trade_volume", 0)),
            })
        result = {"ticker": ticker, "name": ticker, "period": period, "updated": today, "data": rows}
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False)
        except Exception:
            pass
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/coin/add", methods=["POST"])
def api_coin_add():
    body   = request.get_json(force=True)
    ticker = body.get("ticker", "").strip().upper()
    qty    = float(body.get("quantity", 0))
    price  = float(body.get("avg_price", 0))
    if not ticker or qty <= 0 or price <= 0:
        return jsonify({"error": "입력값 오류"}), 400
    portfolio = _config.setdefault("coin_portfolio", [])
    for item in portfolio:
        if item.get("ticker", "").upper() == ticker:
            return jsonify({"error": "이미 존재하는 코인"}), 400
    # 업비트 마켓 확인
    markets_info = _load_upbit_markets()
    if ticker not in markets_info:
        return jsonify({"error": f"{ticker} — 업비트에서 지원하지 않는 코인"}), 400
    name = markets_info[ticker].get("korean_name", ticker)
    portfolio.append({"ticker": ticker, "name": name, "quantity": qty, "avg_price": price})
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(_config, f, ensure_ascii=False, indent=2)
    return jsonify({"ok": True, "name": name})

@app.route("/api/coin/edit", methods=["POST"])
def api_coin_edit():
    body   = request.get_json(force=True)
    ticker = body.get("ticker", "").strip().upper()
    qty    = float(body.get("quantity", 0))
    price  = float(body.get("avg_price", 0))
    if qty <= 0 or price <= 0:
        return jsonify({"error": "입력값 오류"}), 400
    for item in _config.get("coin_portfolio", []):
        if item.get("ticker", "").upper() == ticker:
            item["quantity"]  = qty
            item["avg_price"] = price
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(_config, f, ensure_ascii=False, indent=2)
            return jsonify({"ok": True})
    return jsonify({"error": "종목 없음"}), 404

@app.route("/api/coin/delete", methods=["POST"])
def api_coin_delete():
    ticker = request.get_json(force=True).get("ticker", "").strip().upper()
    before = len(_config.get("coin_portfolio", []))
    _config["coin_portfolio"] = [i for i in _config.get("coin_portfolio", []) if i.get("ticker","").upper() != ticker]
    if len(_config["coin_portfolio"]) < before:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(_config, f, ensure_ascii=False, indent=2)
        return jsonify({"ok": True})
    return jsonify({"error": "종목 없음"}), 404

@app.route("/api/coin/snapshot", methods=["POST"])
def api_coin_snapshot():
    _save_coin_daily_snapshot()
    return jsonify({"ok": True})

@app.route("/api/combined/history")
def api_combined_history():
    return jsonify(_load_combined_history().get("records", []))

@app.route("/api/combined/snapshot", methods=["POST"])
def api_combined_snapshot():
    """종합 스냅샷 수동 저장 (시간 제한 없이 즉시 저장 — 전날 날짜 기준).
    날짜 파라미터: ?date=YYYY-MM-DD (생략 시 어제)
    """
    global _combined_snapshot_done_date
    now = datetime.now()
    target_date = request.args.get("date") or (now - timedelta(days=1)).strftime("%Y-%m-%d")
    _combined_snapshot_done_date = None   # 플래그 초기화 → 재저장 허용

    # KR: target_date 이하 가장 최근 non-realtime 레코드
    kr_recs = _load_history().get("records", [])
    kr = next(
        (r for r in kr_recs if not r.get("realtime") and r.get("date", "") <= target_date),
        kr_recs[0] if kr_recs else {}
    )
    # US: target_date 이하 가장 최근 레코드
    us_recs = _load_us_history().get("records", [])
    us = next(
        (r for r in us_recs if r.get("date", "") <= target_date),
        us_recs[0] if us_recs else {}
    )
    # Coin: coin_history.json의 target_date 레코드
    coin_recs = _load_coin_history().get("records", [])
    coin = next((r for r in coin_recs if r.get("date") == target_date), None)
    if not coin:
        return jsonify({"error": f"{target_date} 코인 레코드 없음"}), 400

    kr_buy    = kr.get("total_buy", 0);    us_buy    = us.get("total_buy_krw", 0)
    kr_eval   = kr.get("total_eval", 0);   us_eval   = us.get("total_eval_krw", 0)
    kr_profit = kr.get("total_profit", 0); us_profit = us.get("total_profit_krw", 0)
    coin_buy  = coin.get("total_buy", 0);  coin_eval = coin.get("total_eval", 0)
    coin_profit = coin.get("total_profit", 0)

    total_buy    = kr_buy + us_buy + coin_buy
    total_eval   = kr_eval + us_eval + coin_eval
    total_profit = kr_profit + us_profit + coin_profit
    total_rate   = round(total_profit / total_buy * 100, 2) if total_buy else 0.0

    combined_data = _load_combined_history()
    recs = [r for r in combined_data.get("records", []) if r.get("date") != target_date]
    prev_rec     = next((r for r in recs if r.get("date", "") < target_date), None)
    prev_eval    = prev_rec.get("total_eval", 0) if prev_rec else 0
    daily_change = round(total_eval - prev_eval) if prev_eval else 0

    record = {
        "date": target_date, "total_buy": round(total_buy), "total_eval": round(total_eval),
        "total_profit": round(total_profit), "total_rate": total_rate,
        "daily_change": daily_change,
        "kr_eval": round(kr_eval), "us_eval": round(us_eval), "coin_eval": round(coin_eval),
        "kr_date": kr.get("date", ""), "us_date": us.get("date", ""),
    }
    recs.insert(0, record)
    recs.sort(key=lambda r: r["date"], reverse=True)
    _save_combined_history({"records": recs})
    _combined_snapshot_done_date = target_date
    return jsonify({"ok": True, "date": target_date, "total_eval": round(total_eval)})


if __name__ == "__main__":
    import sys
    import webbrowser

    no_browser = "--no-browser" in sys.argv

    # 워치독 플래그 설정 (모듈 스코프에서 직접 수정)
    _NO_BROWSER = no_browser

    # 백그라운드 모드: stdout/stderr를 로그 파일로 리디렉션
    if no_browser:
        import sys
        _log_path = os.path.join(_BASE_DIR, "server.log")
        _log_file = open(_log_path, "a", encoding="utf-8", buffering=1)
        sys.stdout = _log_file
        sys.stderr = _log_file

    print("=" * 50)
    print(f"  서버 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("  주식 포트폴리오 웹 대시보드")
    print("  http://localhost:5000")
    if no_browser:
        print("  [백그라운드 모드 - 브라우저 미실행]")
    print("=" * 50)

    # US 히스토리 누락 시 배치 자동 실행 (백그라운드)
    _run_catchup_batch_if_needed()

    if not no_browser:
        def open_browser():
            import time
            time.sleep(1.2)
            webbrowser.open_new("http://localhost:5000")
        threading.Thread(target=open_browser, daemon=True).start()

    socketio.run(app, host="0.0.0.0", port=5000, debug=False)
