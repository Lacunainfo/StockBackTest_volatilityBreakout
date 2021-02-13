# 래리윌리엄스 변동성 돌파 전략 백테스트

가장 간단한 형태의 변동성 돌파가격 전략을 모사함



Range = (전날 고가 - 전날 저가) x K

(K는 통상 0.5)

변동성 돌파가격 = 당일 시가 + Range

현재가가 변동성 가격을 돌파시 -> 매수

다음 날 시초가 청산



### [Input DB 데이터의 형태]

(키움 API 에서 OPT100081로 받을 수 있는 데이터의 형태와 같음)

테이블명 : 종목코드

  index     open high   low   close  volume
  
YYYYMMDD    시가  고가  저가  종가  거래량



### [Output 열의 의미]

    """ K3 은 K = 0.3 으로 분석을 의미 """
    df_price['K3Range'] = 0       # 변동성 가격 범위
    df_price['K3BOPrice'] = 0     # 변동성 돌파 가격
    df_price['K3BuyFlag'] = 0     # 변동성 돌파 여부 (당일 시가에서 변동성 가격범위를 더한 가격을 당일 고가가 뛰어넘는지)
    df_price['K3tmrOpenWin'] = 0  # 명일 시초가가 변동성 돌파가격보다 높은지
    df_price['K3tmrOpenPct'] = 0  # 명일 시초가의 변동성 돌파가격대비 %
    df_price['K3tmrHighWin'] = 0  # 명일 고가가 변동성 돌파가격보다 높은지
    df_price['K3tmrHighPct'] = 0  # 명일 고가의 변동성 돌파가격대비 %






