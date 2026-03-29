#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
종합 히스토리 백필 스크립트
- 지정 기간의 KR/US/Coin 히스토리를 날짜별로 합산하여 combined_history.json 생성
- 주식 휴장일: 해당 날짜 이전 가장 최근 거래일 레코드 사용
"""

import json
import os
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def load(path):
    if os.path.exists(path):
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    return {"records": []}

def save(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def find_latest_on_or_before(records, date_str):
    """records는 최신순 정렬. date_str 이하인 가장 최신 레코드 반환"""
    for r in records:
        if r.get('date', '') <= date_str:
            return r
    return {}

def date_range(start, end):
    cur = datetime.strptime(start, '%Y-%m-%d')
    end_dt = datetime.strptime(end, '%Y-%m-%d')
    while cur <= end_dt:
        yield cur.strftime('%Y-%m-%d')
        cur += timedelta(days=1)

# ── 데이터 로드 ──
kr_recs   = load(os.path.join(BASE_DIR, 'history.json')).get('records', [])
us_recs   = load(os.path.join(BASE_DIR, 'us_history.json')).get('records', [])
coin_recs = load(os.path.join(BASE_DIR, 'coin_history.json')).get('records', [])

# 날짜 오름차순 정렬 (백필 전용)
kr_sorted   = sorted(kr_recs,   key=lambda r: r['date'])
us_sorted   = sorted(us_recs,   key=lambda r: r['date'])
coin_sorted = sorted(coin_recs, key=lambda r: r['date'])

# ── 백필 범위 ──
START_DATE = '2026-03-01'
END_DATE   = '2026-03-28'

# 기존 combined_history.json 로드 (있으면 덮어쓰지 않을 날짜 확인용)
combined_file = os.path.join(BASE_DIR, 'combined_history.json')
combined_data = load(combined_file)
existing_recs = combined_data.get('records', [])
existing_dates = {r['date'] for r in existing_recs}

new_records = []

# kr/us records를 날짜 내림차순 유지 (find_latest_on_or_before용)
kr_desc   = sorted(kr_recs,   key=lambda r: r['date'], reverse=True)
us_desc   = sorted(us_recs,   key=lambda r: r['date'], reverse=True)
coin_dict = {r['date']: r for r in coin_recs}

print(f'백필 범위: {START_DATE} ~ {END_DATE}')
print(f'KR 레코드: {len(kr_recs)}개  ({kr_recs[-1]["date"] if kr_recs else "?"} ~ {kr_recs[0]["date"] if kr_recs else "?"})')
print(f'US 레코드: {len(us_recs)}개  ({us_recs[-1]["date"] if us_recs else "?"} ~ {us_recs[0]["date"] if us_recs else "?"})')
print(f'Coin 레코드: {len(coin_recs)}개  ({coin_recs[-1]["date"] if coin_recs else "?"} ~ {coin_recs[0]["date"] if coin_recs else "?"})')
print()

dates = list(date_range(START_DATE, END_DATE))

for i, date in enumerate(dates):
    # 코인: 해당 날짜 레코드 없으면 스킵
    coin = coin_dict.get(date)
    if not coin:
        print(f'  {date} → 코인 데이터 없음, 스킵')
        continue

    # KR: 해당 날짜 이하 가장 최근 레코드
    kr = find_latest_on_or_before(kr_desc, date)
    # US: 해당 날짜 이하 가장 최근 레코드
    us = find_latest_on_or_before(us_desc, date)

    kr_buy    = kr.get('total_buy', 0)
    kr_eval   = kr.get('total_eval', 0)
    kr_profit = kr.get('total_profit', 0)
    kr_date   = kr.get('date', '')

    us_buy    = us.get('total_buy_krw', 0)
    us_eval   = us.get('total_eval_krw', 0)
    us_profit = us.get('total_profit_krw', 0)
    us_date   = us.get('date', '')

    coin_buy    = coin.get('total_buy', 0)
    coin_eval   = coin.get('total_eval', 0)
    coin_profit = coin.get('total_profit', 0)

    total_buy    = kr_buy + us_buy + coin_buy
    total_eval   = kr_eval + us_eval + coin_eval
    total_profit = kr_profit + us_profit + coin_profit
    total_rate   = round(total_profit / total_buy * 100, 2) if total_buy else 0.0

    # 전일대비: 직전 날짜의 combined eval
    if new_records:
        prev_eval    = new_records[-1]['total_eval']
        daily_change = round(total_eval - prev_eval)
    else:
        daily_change = 0

    record = {
        'date':         date,
        'total_buy':    round(total_buy),
        'total_eval':   round(total_eval),
        'total_profit': round(total_profit),
        'total_rate':   total_rate,
        'daily_change': daily_change,
        'kr_eval':      round(kr_eval),
        'us_eval':      round(us_eval),
        'coin_eval':    round(coin_eval),
        'kr_date':      kr_date,
        'us_date':      us_date,
    }
    new_records.append(record)

    kr_note = f' ← {kr_date}' if kr_date != date else ''
    us_note = f' ← {us_date}' if us_date != date else ''
    print(f'  {date}  합계={round(total_eval):>15,}원  KR={round(kr_eval):>13,}원{kr_note}  US={round(us_eval):>13,}원{us_note}  Coin={round(coin_eval):>12,}원  전일대비={daily_change:+,}원')

# ── 기존 레코드와 병합 (백필 범위 외 레코드 유지) ──
out_of_range = [r for r in existing_recs
                if r['date'] < START_DATE or r['date'] > END_DATE]

# new_records(오름차순) + out_of_range → 최신순 정렬
all_records = new_records + out_of_range
all_records.sort(key=lambda r: r['date'], reverse=True)

save(combined_file, {'records': all_records})
print(f'\n총 {len(new_records)}개 레코드 저장 완료 → combined_history.json ({len(all_records)}개)')
