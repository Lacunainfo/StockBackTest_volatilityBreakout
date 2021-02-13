import sqlite3
import pandas as pd
import numpy as np
import datetime
import time

import os

"""
코스피 & 코스닥 전종목 180830~210205 일별 시가/고가/저가/종가 데이터를 활용한
래리 윌리엄스 변동성 돌파 백테스트,
가장 간단한 형태의 단기 모멘텀 돌파 전략을 사용.

Range = 전일 고가 - 전일 저가
진입 = 오늘 시가 + Range * K
K = 0.5
청산 = 다음날 시가

분석 소요시간 대략 2시간
"""


def test_momentum(code):
    # 일봉 데이터 로딩
    con2= sqlite3.connect("./DailyPrice.db")
    df_original = pd.read_sql("SELECT * FROM '{}'".format(code), con2, index_col='index')
    con2.close()

    # 위아래 반전 (키움 일봉 데이터는 1행이 가장 최근 데이터)
    df_price = df_original[::-1].copy()
    del df_original
    # 일봉 다운로드에서 append로 했더니 같은 날짜의 인덱스가 2개 행씩 존재 -> 중복제거
    # 000040 KR모터스 20년2월 11일 거래중지 -> 1일로 중복행 제거하는 긍정적 효과도 있는듯.
    df_price.drop_duplicates(inplace=True)

    # 열 생성
    df_price['K3Range'] = 0
    df_price['K3BOPrice'] = 0
    df_price['K3BuyFlag'] = 0
    df_price['K3tmrOpenWin'] = 0
    df_price['K3tmrOpenPct'] = 0
    df_price['K3tmrHighWin'] = 0
    df_price['K3tmrHighPct'] = 0

    df_price['K5Range'] = 0
    df_price['K5BOPrice'] = 0
    df_price['K5BuyFlag'] = 0
    df_price['K5tmrOpenWin'] = 0
    df_price['K5tmrOpenPct'] = 0
    df_price['K5tmrHighWin'] = 0
    df_price['K5tmrHighPct'] = 0


    full_index = df_price.index.astype('str')

    for i in range(len(full_index[:-2])):
        yesterday = full_index[i]
        today = full_index[i+1]
        tomorrow = full_index[i+2]

        yst_high = df_price.loc[yesterday, 'high']
        yst_low = df_price.loc[yesterday, 'low']
        tdy_open = df_price.loc[today, 'open']
        tdy_high = df_price.loc[today, 'high']
        tmr_open = df_price.loc[tomorrow, 'open']
        tmr_high = df_price.loc[tomorrow, 'high']

        k3buy_flag = 0
        k3tmrOpenWin = 0
        k3tmrHighWin = 0

        k3bo_range = (yst_high - yst_low) * K3
        k3bo_price = tdy_open + k3bo_range

        if k3bo_price <= tdy_high:
            k3buy_flag = 1
            if k3bo_price <= tmr_open:
                k3tmrOpenWin = 1
            if k3bo_price <= tmr_high:
                k3tmrHighWin = 1

        df_price.loc[today, 'K3Range'] = int(k3bo_range)
        df_price.loc[today, 'K3BOPrice'] = int(k3bo_price)
        df_price.loc[today, 'K3BuyFlag'] = int(k3buy_flag)
        df_price.loc[today, 'K3tmrOpenWin'] = int(k3tmrOpenWin)
        df_price.loc[today, 'K3tmrHighWin'] = int(k3tmrHighWin)
        if k3buy_flag == 1:
            df_price.loc[today, 'K3tmrOpenPct'] = round((tmr_open / k3bo_price - 1) * 100, 2)
            df_price.loc[today, 'K3tmrHighPct'] = round((tmr_high / k3bo_price - 1) * 100, 2)


        k5buy_flag = 0
        k5tmrOpenWin = 0
        k5tmrHighWin = 0

        k5bo_range = (yst_high - yst_low) * K5
        k5bo_price = tdy_open + k5bo_range

        if k5bo_price <= tdy_high:
            k5buy_flag = 1
            if k5bo_price <= tmr_open:
                k5tmrOpenWin = 1
            if k5bo_price <= tmr_high:
                k5tmrHighWin = 1

        df_price.loc[today, 'K5Range'] = int(k5bo_range)
        df_price.loc[today, 'K5BOPrice'] = int(k5bo_price)
        df_price.loc[today, 'K5BuyFlag'] = int(k5buy_flag)
        df_price.loc[today, 'K5tmrOpenWin'] = int(k5tmrOpenWin)
        df_price.loc[today, 'K5tmrHighWin'] = int(k5tmrHighWin)
        if k5buy_flag == 1:
            df_price.loc[today, 'K5tmrOpenPct'] = round((tmr_open / k5bo_price - 1) * 100, 4)
            df_price.loc[today, 'K5tmrHighPct'] = round((tmr_high / k5bo_price - 1) * 100, 4)

    # 일봉 데이터 로딩
    con = sqlite3.connect("./tested_momentum_breakout.db")
    df_price.to_sql(code, con, if_exists='replace')
    con.close()

    df_code.loc[code, 'total'] = len(df_price.index)

    sum_k3buy = df_price['K3BuyFlag'].sum()
    sum_k3openwin = df_price['K3tmrOpenWin'].sum()
    sum_k3highwin = df_price['K3tmrHighWin'].sum()

    aa = df_price[df_price['K3tmrOpenPct']!=0]
    bb = df_price[df_price['K3tmrHighPct']!=0]

    df_code.loc[code, 'K3BuyFlag'] = sum_k3buy
    df_code.loc[code, 'K3tmrOpenWin'] = sum_k3openwin
    if sum_k3buy != 0:
        df_code.loc[code, 'K3tmrOpenWinRatio'] = round((sum_k3openwin / sum_k3buy) * 100)
        df_code.loc[code, 'K3tmrHighWinRatio'] = round((sum_k3highwin / sum_k3buy) * 100)
    else:
        df_code.loc[code, 'K3tmrOpenWinRatio'] = 0
        df_code.loc[code, 'K3tmrHighWinRatio'] = 0
    df_code.loc[code, 'K3tmrOpenPct'] = round(aa['K3tmrOpenPct'].mean(), 2)
    df_code.loc[code, 'K3tmrHighWin'] = sum_k3highwin
    df_code.loc[code, 'K3tmrHighPct'] = round(bb['K3tmrHighPct'].mean(), 2)

    sum_k5buy = df_price['K5BuyFlag'].sum()
    sum_k5openwin = df_price['K5tmrOpenWin'].sum()
    sum_k5highwin = df_price['K5tmrHighWin'].sum()

    cc = df_price[df_price['K5tmrOpenPct'] != 0]
    dd = df_price[df_price['K5tmrHighPct'] != 0]

    df_code.loc[code, 'K5BuyFlag'] = sum_k5buy
    df_code.loc[code, 'K5tmrOpenWin'] = sum_k5openwin
    if sum_k5buy != 0:
        df_code.loc[code, 'K5tmrOpenWinRatio'] = round((sum_k5openwin / sum_k5buy) * 100)
        df_code.loc[code, 'K5tmrHighWinRatio'] = round((sum_k5highwin / sum_k5buy) * 100)
    else:
        df_code.loc[code, 'K5tmrOpenWinRatio'] = 0
        df_code.loc[code, 'K5tmrHighWinRatio'] = 0
    df_code.loc[code, 'K5tmrOpenPct'] = round(cc['K5tmrOpenPct'].mean(), 2)
    df_code.loc[code, 'K5tmrHighWin'] = sum_k5highwin
    df_code.loc[code, 'K5tmrHighPct'] = round(dd['K5tmrHighPct'].mean(), 2)

    del df_price
    del aa
    del bb
    del cc
    del dd


# 변동성 계수 설정
K3 = 0.3
K5 = 0.5

# 코드 목록 로딩
con = sqlite3.connect("./CodeList.db")
df_code = pd.read_sql("SELECT * FROM CodeList", con, index_col='index')
con.close()



# 코드 변수에 열 생성
df_code['total'] = 0
df_code['K3BuyFlag'] = 0
df_code['K3tmrOpenWin'] = 0
df_code['K3tmrOpenWinRatio'] = 0
df_code['K3tmrOpenPct'] = 0
df_code['K3tmrHighWin'] = 0
df_code['K3tmrHighWinRatio'] = 0
df_code['K3tmrHighPct'] = 0

df_code['K5BuyFlag'] = 0
df_code['K5tmrOpenWin'] = 0
df_code['K5tmrOpenWinRatio'] = 0
df_code['K5tmrOpenPct'] = 0
df_code['K5tmrHighWin'] = 0
df_code['K5tmrHighWinRatio'] = 0
df_code['K5tmrHighPct'] = 0


print("start time: {}".format(datetime.datetime.now()))
total_num = len(df_code.index)
print("number of target codes: {}".format(total_num))
num = 1

# DB 파일에서 테이블 이름 목록 가져오기
con = sqlite3.connect('./tested_momentum_breakout.db')
cursor = con.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
table_list = cursor.fetchall()
done_list = []
for target in table_list:
    done_list.append(target[0])

#for code in ['000020', '000040']:
for code in df_code.index:
    print("current stage: {} / {}".format(num, total_num))
    num += 1
    if code not in done_list:
        test_momentum(code)
print("end time: {}".format(datetime.datetime.now()))

con = sqlite3.connect("./tested_result.db")
df_code.to_sql("MomentumTest", con, if_exists='replace')
con.close()



