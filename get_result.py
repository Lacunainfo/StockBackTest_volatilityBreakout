import pandas as pd
import sqlite3

# DB 파일에서 테이블 이름 목록 가져오기
con = sqlite3.connect('./tested_momentum_breakout.db')
cursor = con.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
table_list = cursor.fetchall()
done_list = []
for target in table_list:
    done_list.append(target[0])


# 코드 목록 로딩
con3 = sqlite3.connect("./CodeList.db")
df_result = pd.read_sql("SELECT * FROM CodeList", con3, index_col='index')
con3.close()

# 코드 변수에 열 생성
df_result['total'] = 0
df_result['K3BuyFlag'] = 0
df_result['K3tmrOpenWin'] = 0
df_result['K3tmrOpenWinRatio'] = 0
df_result['K3tmrOpenPct'] = 0
df_result['K3tmrHighWin'] = 0
df_result['K3tmrHighWinRatio'] = 0
df_result['K3tmrHighPct'] = 0

df_result['K5BuyFlag'] = 0
df_result['K5tmrOpenWin'] = 0
df_result['K5tmrOpenWinRatio'] = 0
df_result['K5tmrOpenPct'] = 0
df_result['K5tmrHighWin'] = 0
df_result['K5tmrHighWinRatio'] = 0
df_result['K5tmrHighPct'] = 0


tot = len(done_list)
num = 1
for code in done_list:
    print("working {} / {}".format(num, tot))
    num += 1

    df_price = pd.read_sql("SELECT * FROM '{}'".format(code), con, index_col='index')

    df_result.loc[code, 'total'] = len(df_price.index)

    sum_k3buy = df_price['K3BuyFlag'].sum()
    sum_k3openwin = df_price['K3tmrOpenWin'].sum()
    sum_k3highwin = df_price['K3tmrHighWin'].sum()

    aa = df_price[df_price['K3tmrOpenPct'] != 0]
    bb = df_price[df_price['K3tmrHighPct'] != 0]

    df_result.loc[code, 'K3BuyFlag'] = sum_k3buy
    df_result.loc[code, 'K3tmrOpenWin'] = sum_k3openwin
    if sum_k3buy != 0:
        df_result.loc[code, 'K3tmrOpenWinRatio'] = round((sum_k3openwin / sum_k3buy) * 100)
        df_result.loc[code, 'K3tmrHighWinRatio'] = round((sum_k3highwin / sum_k3buy) * 100)
    else:
        df_result.loc[code, 'K3tmrOpenWinRatio'] = 0
        df_result.loc[code, 'K3tmrHighWinRatio'] = 0
    df_result.loc[code, 'K3tmrOpenPct'] = round(aa['K3tmrOpenPct'].mean(), 2)
    df_result.loc[code, 'K3tmrHighWin'] = sum_k3highwin
    df_result.loc[code, 'K3tmrHighPct'] = round(bb['K3tmrHighPct'].mean(), 2)

    sum_k5buy = df_price['K5BuyFlag'].sum()
    sum_k5openwin = df_price['K5tmrOpenWin'].sum()
    sum_k5highwin = df_price['K5tmrHighWin'].sum()

    cc = df_price[df_price['K5tmrOpenPct'] != 0]
    dd = df_price[df_price['K5tmrHighPct'] != 0]

    df_result.loc[code, 'K5BuyFlag'] = sum_k5buy
    df_result.loc[code, 'K5tmrOpenWin'] = sum_k5openwin
    if sum_k5buy != 0:
        df_result.loc[code, 'K5tmrOpenWinRatio'] = round((sum_k5openwin / sum_k5buy) * 100)
        df_result.loc[code, 'K5tmrHighWinRatio'] = round((sum_k5highwin / sum_k5buy) * 100)
    else:
        df_result.loc[code, 'K5tmrOpenWinRatio'] = 0
        df_result.loc[code, 'K5tmrHighWinRatio'] = 0
    df_result.loc[code, 'K5tmrOpenPct'] = round(cc['K5tmrOpenPct'].mean(), 2)
    df_result.loc[code, 'K5tmrHighWin'] = sum_k5highwin
    df_result.loc[code, 'K5tmrHighPct'] = round(dd['K5tmrHighPct'].mean(), 2)

    del df_price
    del aa
    del bb
    del cc
    del dd

con.close()


con4 = sqlite3.connect("./tested_result.db")
df_result.to_sql("MomentumTest", con4, if_exists='replace')
con4.close()

