#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# TA-Lib テクニカルデータ集計プログラム
# Copyright (c) 2022, Des Securities all rights reserved.
#
# Filename : des_securities.py
# Date     : June 8, 2022
# Author   : Des Securities
#

#
# 定数ファイルのインポート
#
# Python Library Pathの設定
import sys
sys.path.append("../const")
from DesSecuritiesConst import DES_SECURITIES_TABLE_NAME
from DesSecuritiesConst import DES_SECURITIES_DB_FILE
from DesSecuritiesConst import MY_SLACK_WEB_HOOK_GENERAL

#
# です證券 テクニカル分析処理プログラム
#
table_name = DES_SECURITIES_TABLE_NAME
database_file_name = DES_SECURITIES_DB_FILE

# Python Import Libraries
import slackweb
import datetime

import pyodbc
import pandas as pd
import talib as ta
from talib import MA_Type
import time
from sqlalchemy import create_engine
from urllib.parse import quote_plus

#
# ACCESS DBにアクセスするDBエンジンの設定
#
def alchemy_engine(db_path):
    con_str = "DRIVER=" + \
              "{Microsoft Access Driver (*.mdb, *.accdb)};" + \
               f"DBQ={db_path};"
    con_str = quote_plus(con_str)
    engine = create_engine(
        f"access+pyodbc:///?odbc_connect={con_str}",
        echo=True)
 
    return engine

#
# Slack Log Send
#
slack = slackweb.Slack(url=MY_SLACK_WEB_HOOK_GENERAL)
dt_now = datetime.datetime.now()
slack.notify(text = dt_now.strftime('%Y/%m/%d %H:%M:%S') + " : TA-Lib Processing Start")

con_str = 'Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};Dbq={0};'.format(database_file_name)
conn = pyodbc.connect(con_str)
cur = conn.cursor()

cur.execute(f'DELETE FROM {table_name}')
conn.commit()

# 結果を出力するデータフレーム
cols = ['銘柄コード', '銘柄名称', '日付', \
    'BBanTdy', 'BBanYdy', \
    'macd_flag', 'rsi_flag', 'stocha_flag',\
    'macd_delta', 'rsi_delta', 'stocha_delta',\
    'MacdTdy', 'MacdYdy', 'MacdSigTdy', 'MacdSigYdy', \
    'Rsi9Tdy', 'Rsi9Ydy', 'Rsi14Tdy', 'Rsi14Ydy',\
    'StochKTdy', 'StochKYdy','StochDTdy', 'StochDYdy',\
    'TwoCrows','ThreeBlackCrows','ThreeInside','ThreeLineStrike',\
    'ThreeOutside','ThreeStarsInSouth','ThreeWhiteSoldiers',\
    'AbandonedBaby','AdvancedBlock','BeltHold',\
    'BreakAway','ClosingMarubouzu','ConcealBabysWall',\
    'CounterAttack','DarkCloudCover','Doji',\
    'DojiStar','DragonflyDoji','Engulfing',\
    'EveningDojiStar','EveningStar','GapSideSideWhite',\
    'GraveStoneDoji','Hammer','HangingMan',\
    'Harami','HaramiCross','HighWave',\
    'Hikkake','HikkakeMod','HomingPigeon','IdenticalThreeCrows','Inneck',\
    'InvertedHammer','Kicking','KickingByLength',\
    'LadderBottom','LongLeggedDoji','LongLine',\
    'Marubozu','MatchingLow','MatHold','MorningDojiStar','MorningStar',\
    'OnNeck','Piercing','RickShawman','RiseAllThreeMethods','SeparatingLines',\
    'ShootingStar','ShortLine','SpinningTop','StalledPatter','StickSandwitch',\
    'Takuri','TakuriGap','Thrusting','TriStar',\
    'UniqueThreeRiver','UpsideGapTwoCrows','CrossSideGapThreeMethods'\
    ]
df_result = pd.DataFrame(index=['銘柄コード'], columns=cols)

query = ('select DISTINCT 銘柄コード from 日足クエリ;')
df = pd.read_sql(query, conn)

# 開始
start_time = time.perf_counter()

#lastindex=60
lastindex=75

for code in df.銘柄コード:
    query = ('select 銘柄コード,銘柄名称,日付,連番,始値,高値,安値,終値 from 日足クエリ where 銘柄コード={} and 連番<={} ORDER BY 連番 DESC;'.format(code, lastindex))

    df_stock = pd.read_sql(query, conn)

    record = pd.Series([code], index=df.columns)
    df_result = df_result.append(record, ignore_index=True)

    open = df_stock['始値']
    high = df_stock['高値']
    low = df_stock['安値']
    close = df_stock['終値']

    if len(df_stock) == lastindex:

        # ボリンジャーバンドの計算
        upper1, middle,lower1 = ta.BBANDS(close, timeperiod=25, nbdevup=1, nbdevdn=1, matype=0)
        upper2, middle, lower2 = ta.BBANDS(close, timeperiod=25, nbdevup=2, nbdevdn=2, matype=0)
        upper3, middle, lower3 = ta.BBANDS(close, timeperiod=25, nbdevup=3, nbdevdn=3, matype=0)

        #MACD
        macd, macdsignal, macdhist = ta.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
        macd_flag = False
        macd_delta=0
        if (macdsignal[lastindex-2] > macd[lastindex-2])  and (macdsignal[lastindex-1] < macd[lastindex-1]):
            macd_delta = (macdsignal[lastindex-2] - macd[lastindex-2]) + (macd[lastindex-1] - macdsignal[lastindex-1])
            macd_flag=True
        #RSI
        rsi9 = ta.RSI(close, timeperiod=9)
        rsi14 = ta.RSI(close, timeperiod=14)
        rsi_flag = False
        rsi_delta = 0
        if (rsi14[lastindex-2] > rsi9[lastindex-2])  and (rsi14[lastindex-1] < rsi9[lastindex-1]):
            rsi_delta = (rsi14[lastindex-2] - rsi9[lastindex-2])  + (rsi9[lastindex-1] - rsi14[lastindex-1])
            rsi_flag=True

        #stochastic
        slowk, slowd = ta.STOCH(high, low, close, fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)
        stocha_flag = False
        stocha_delta = 0
        if (slowd[lastindex-2] > slowk[lastindex-2])  and (slowd[lastindex-1] < slowk[lastindex-1]):
            stocha_delta = (slowd[lastindex-2] - slowk[lastindex-2]) + (slowk[lastindex-1] - slowd[lastindex-1])
            stocha_flag=True

        #Parabolic SAR
        #sar = ta.SAR(high, low, 0.020)
        #parabolic=sar[lastindex-2]
        
        #pattern recognition
        CDL2CROWS = ta.CDL2CROWS(open, high, low, close)                                    # 売り：二羽烏
        CDL3BLACKCROWS = ta.CDL3BLACKCROWS(open, high, low, close)                          # 売り：黒三兵(三羽鳥) 
        CDL3INSIDE = ta.CDL3INSIDE(open, high, low, close)                                  # はらみ上げ：陽線上げなら買い　はらみ下げ：陰線下げなら売り
        CDL3LINESTRIKE = ta.CDL3LINESTRIKE(open, high, low, close)                          # 強気の三手打ち：陰線なら買い　	弱気の三手打ち：陽線なら売り
        CDL3OUTSIDE = ta.CDL3OUTSIDE(open, high, low, close)                                # 包み上げ：陽線上げなら買い　包み下げ：陰線下げなら売り
        CDL3STARSINSOUTH = ta.CDL3STARSINSOUTH(open, high, low, close)                      # 南の三ツ星：買い
        CDL3WHITESOLDIERS = ta.CDL3WHITESOLDIERS(open, high, low, close)                    # 赤三兵：買い
        CDLABANDONEDBABY = ta.CDLABANDONEDBABY(open, high, low, close, penetration=0)       # 強気の捨て子線：陽線なら買い　弱気の捨て子線：陰線なら売り
        CDLADVANCEBLOCK = ta.CDLADVANCEBLOCK(open, high, low, close)                        # 赤三兵先詰まり：売り
        CDLBELTHOLD = ta.CDLBELTHOLD(open, high, low, close)                                # 寄付坊主：陽線なら買い　大引坊主：陰線なら売り        
        CDLBREAKAWAY = ta.CDLBREAKAWAY(open, high, low, close)                              # 強気の放れ三手：陽線なら買い　弱気の放れ三手：陰線なら売り
        CDLCLOSINGMARUBOZU = ta.CDLCLOSINGMARUBOZU(open, high, low, close)                  # 終値丸坊主　陽線の場合は強気、陰線の場合は弱気
        CDLCONCEALBABYSWALL = ta.CDLCONCEALBABYSWALL(open, high, low, close)                # 小燕包み：買い
        CDLCOUNTERATTACK = ta.CDLCOUNTERATTACK(open, high, low, close)                      # 強気の出会い線：陽線なら買い　弱気の出会い線：陰線なら売り
        CDLDARKCLOUDCOVER = ta.CDLDARKCLOUDCOVER(open, high, low, close, penetration=0)     # 被せ線：売り
        CDLDOJI = ta.CDLDOJI(open, high, low, close)                                        # 同時線：トレンド転換
        CDLDOJISTAR = ta.CDLDOJISTAR(open, high, low, close)                                # 強気の寄り引き同事線：下降中なら買い　弱気の寄り引き同事線：上昇中なら売り
        CDLDRAGONFLYDOJI = ta.CDLDRAGONFLYDOJI(open, high, low, close)                      # トンボ：トレンド転換
        CDLENGULFING = ta.CDLENGULFING(open, high, low, close)                              # 強気の抱き線(包み線)：陽線なら買い　弱気の抱き線(包み線)：陰線なら売り
        CDLEVENINGDOJISTAR = ta.CDLEVENINGDOJISTAR(open, high, low, close, penetration=0)   # 三川明けの十字星：売り
        CDLEVENINGSTAR = ta.CDLEVENINGSTAR(open, high, low, close, penetration=0)           # 三川宵の明星：売り
        CDLGAPSIDESIDEWHITE = ta.CDLGAPSIDESIDEWHITE(open, high, low, close)                # 下放れ並び：買い  上放れ並び赤:売り
        CDLGRAVESTONEDOJI = ta.CDLGRAVESTONEDOJI(open, high, low, close)                    # 塔婆(トウバ)：トレンド転換
        CDLHAMMER = ta.CDLHAMMER(open, high, low, close)                                    # カラカサ線(たくり線)：買い
        CDLHANGINGMAN = ta.CDLHANGINGMAN(open, high, low, close)                            # 首吊り線：売り
        CDLHARAMI = ta.CDLHARAMI(open, high, low, close)                                    # 強気のはらみ線：買い　弱気のはらみ線：売り
        CDLHARAMICROSS = ta.CDLHARAMICROSS(open, high, low, close)                          # 強気のはらみ寄せ線：買い　弱気のはらみ寄せ線：売り
        CDLHIGHWAVE = ta.CDLHIGHWAVE(open, high, low, close)                                # 高波：強気相場や弱気相場の迷い
        CDLHIKKAKE = ta.CDLHIKKAKE(open, high, low, close)                                  # 強気の引掛けパターン：買い　弱気の引掛けパターン	：売り
        CDLHIKKAKEMOD = ta.CDLHIKKAKEMOD(open, high, low, close)                            # 改善版引っ掛け
        CDLHOMINGPIGEON = ta.CDLHOMINGPIGEON(open, high, low, close)                        # 小鳩返し：買い
        CDLIDENTICAL3CROWS = ta.CDLIDENTICAL3CROWS(open, high, low, close)                  # 雪崩三羽烏(同時三羽)
        CDLINNECK = ta.CDLINNECK(open, high, low, close)                                    # 入り首線：売り
        CDLINVERTEDHAMMER = ta.CDLINVERTEDHAMMER(open, high, low, close)                    # 金槌(カナヅチ/トンカチ)：買い
        CDLKICKING = ta.CDLKICKING(open, high, low, close)                                  # 強気の行き違い線：買い　弱気の行き違い線：売り
        CDLKICKINGBYLENGTH = ta.CDLKICKINGBYLENGTH(open, high, low, close)                  # 蹴り上げ
        CDLLADDERBOTTOM = ta.CDLLADDERBOTTOM(open, high, low, close)                        # はしご底：買い
        CDLLONGLEGGEDDOJI = ta.CDLLONGLEGGEDDOJI(open, high, low, close)                    # 長い足の同時線：トレンド転換
        CDLLONGLINE = ta.CDLLONGLINE(open, high, low, close)                                # 長いローソク足　大陽線：買い　大陰線：売り
        CDLMARUBOZU = ta.CDLMARUBOZU(open, high, low, close)                                # 丸坊主　大陽線：買い　大陰線：売り
        CDLMATCHINGLOW = ta.CDLMATCHINGLOW(open, high, low, close)                          # 二点底：買い
        CDLMATHOLD = ta.CDLMATHOLD(open, high, low, close, penetration=0)                   # 強気の押え込み線:買い	弱気の押え込み線：売り
        CDLMORNINGDOJISTAR = ta.CDLMORNINGDOJISTAR(open, high, low, close, penetration=0)   # 三川明けの十字星：買い
        CDLMORNINGSTAR = ta.CDLMORNINGSTAR(open, high, low, close, penetration=0)           # 三川明けの明星：買い
        CDLONNECK = ta.CDLONNECK(open, high, low, close)                                    # あて首線：売り
        CDLPIERCING = ta.CDLPIERCING(open, high, low, close)                                # 切り込み線(切り返し線)：買い
        CDLRICKSHAWMAN = ta.CDLRICKSHAWMAN(open, high, low, close)                          # 人力車の人
        CDLRISEFALL3METHODS = ta.CDLRISEFALL3METHODS(open, high, low, close)                # 上げ三法：買い　下げ三法：売り
        CDLSEPARATINGLINES = ta.CDLSEPARATINGLINES(open, high, low, close)                  # 強気の振分け線：買い　弱気の振分け線：売り
        CDLSHOOTINGSTAR = ta.CDLSHOOTINGSTAR(open, high, low, close)                        # 流れ星：買い
        CDLSHORTLINE = ta.CDLSHORTLINE(open, high, low, close)                              # 短いローソク足
        CDLSPINNINGTOP = ta.CDLSPINNINGTOP(open, high, low, close)                          # コマ
        CDLSTALLEDPATTERN = ta.CDLSTALLEDPATTERN(open, high, low, close)                    # 赤三兵思案星：売り
        CDLSTICKSANDWICH = ta.CDLSTICKSANDWICH(open, high, low, close)                      # 逆差し二点底20
        CDLTAKURI = ta.CDLTAKURI(open, high, low, close)                                    # たくり線：買い
        CDLTASUKIGAP = ta.CDLTASUKIGAP(open, high, low, close)                              # 上放れたすき線：買い　下放れたすき線：売り
        CDLTHRUSTING = ta.CDLTHRUSTING(open, high, low, close)                              # 差込み線：売り
        CDLTRISTAR = ta.CDLTRISTAR(open, high, low, close)                                  # 強気の三ツ星：買い　弱気の三ツ星：売り
        CDLUNIQUE3RIVER = ta.CDLUNIQUE3RIVER(open, high, low, close)                        # 変形三川底：買い
        CDLUPSIDEGAP2CROWS = ta.CDLUPSIDEGAP2CROWS(open, high, low, close)                  # 三川上放れ二羽烏：売り
        CDLXSIDEGAP3METHODS = ta.CDLXSIDEGAP3METHODS(open, high, low, close)                # 上放れ三法：買い　下放れ三法：売り

        # TotalScore
        AbsTotalScore = abs(CDL2CROWS[lastindex-1]) + abs(CDL3BLACKCROWS[lastindex-1]) + abs(CDL3INSIDE[lastindex-1]) + abs(CDL3LINESTRIKE[lastindex-1])+ \
                abs(CDL3OUTSIDE[lastindex-1]) + abs(CDL3STARSINSOUTH[lastindex-1]) + abs(CDL3WHITESOLDIERS[lastindex-1]) + \
                abs(CDLABANDONEDBABY[lastindex-1]) + abs(CDLADVANCEBLOCK[lastindex-1]) + abs(CDLBELTHOLD[lastindex-1]) + \
                abs(CDLBREAKAWAY[lastindex-1]) + abs(CDLCLOSINGMARUBOZU[lastindex-1]) + abs(CDLCONCEALBABYSWALL[lastindex-1]) + \
                abs(CDLCOUNTERATTACK[lastindex-1]) + abs(CDLDARKCLOUDCOVER[lastindex-1]) + abs(CDLDOJI[lastindex-1]) + \
                abs(CDLDOJISTAR[lastindex-1]) + abs(CDLDRAGONFLYDOJI[lastindex-1]) + abs(CDLENGULFING[lastindex-1]) + \
                abs(CDLEVENINGDOJISTAR[lastindex-1]) + abs(CDLEVENINGSTAR[lastindex-1]) + abs(CDLGAPSIDESIDEWHITE[lastindex-1]) + \
                abs(CDLGRAVESTONEDOJI[lastindex-1]) + abs(CDLHAMMER[lastindex-1]) + abs(CDLHANGINGMAN[lastindex-1]) + \
                abs(CDLHARAMI[lastindex-1]) + abs(CDLHARAMICROSS[lastindex-1]) + abs(CDLHIGHWAVE[lastindex-1]) + \
                abs(CDLHIKKAKE[lastindex-1]) + abs(CDLHIKKAKEMOD[lastindex-1]) + abs(CDLHOMINGPIGEON[lastindex-1]) + abs(CDLIDENTICAL3CROWS[lastindex-1]) + abs(CDLINNECK[lastindex-1]) + \
                abs(CDLINVERTEDHAMMER[lastindex-1]) + abs(CDLKICKING[lastindex-1]) + abs(CDLKICKINGBYLENGTH[lastindex-1]) + \
                abs(CDLLADDERBOTTOM[lastindex-1]) + abs(CDLLONGLEGGEDDOJI[lastindex-1]) + abs(CDLLONGLINE[lastindex-1]) + \
                abs(CDLMARUBOZU[lastindex-1]) + abs(CDLMATCHINGLOW[lastindex-1]) + abs(CDLMATHOLD[lastindex-1]) + abs(CDLMORNINGDOJISTAR[lastindex-1]) + abs(CDLMORNINGSTAR[lastindex-1]) + \
                abs(CDLONNECK[lastindex-1]) + abs(CDLPIERCING[lastindex-1]) + abs(CDLRICKSHAWMAN[lastindex-1]) + abs(CDLRISEFALL3METHODS[lastindex-1]) + abs(CDLSEPARATINGLINES[lastindex-1]) + \
                abs(CDLSHOOTINGSTAR[lastindex-1]) + abs(CDLSHORTLINE[lastindex-1]) + abs(CDLSPINNINGTOP[lastindex-1]) + abs(CDLSTALLEDPATTERN[lastindex-1]) + abs(CDLSTICKSANDWICH[lastindex-1]) + \
                abs(CDLTAKURI[lastindex-1]) + abs(CDLTASUKIGAP[lastindex-1]) + abs(CDLTHRUSTING[lastindex-1]) + abs(CDLTRISTAR[lastindex-1]) + \
                abs(CDLUNIQUE3RIVER[lastindex-1]) + abs(CDLUPSIDEGAP2CROWS[lastindex-1]) + abs(CDLXSIDEGAP3METHODS[lastindex-1])

        TotalScore = CDL2CROWS[lastindex-1] + CDL3BLACKCROWS[lastindex-1] + CDL3INSIDE[lastindex-1] + CDL3LINESTRIKE[lastindex-1]+ \
                CDL3OUTSIDE[lastindex-1] + CDL3STARSINSOUTH[lastindex-1] + CDL3WHITESOLDIERS[lastindex-1] + \
                CDLABANDONEDBABY[lastindex-1] + CDLADVANCEBLOCK[lastindex-1] + CDLBELTHOLD[lastindex-1] + \
                CDLBREAKAWAY[lastindex-1] + CDLCLOSINGMARUBOZU[lastindex-1] + CDLCONCEALBABYSWALL[lastindex-1] + \
                CDLCOUNTERATTACK[lastindex-1] + CDLDARKCLOUDCOVER[lastindex-1] + CDLDOJI[lastindex-1] + \
                CDLDOJISTAR[lastindex-1] + CDLDRAGONFLYDOJI[lastindex-1] + CDLENGULFING[lastindex-1] + \
                CDLEVENINGDOJISTAR[lastindex-1] + CDLEVENINGSTAR[lastindex-1] + CDLGAPSIDESIDEWHITE[lastindex-1] + \
                CDLGRAVESTONEDOJI[lastindex-1] + CDLHAMMER[lastindex-1] + CDLHANGINGMAN[lastindex-1] + \
                CDLHARAMI[lastindex-1] + CDLHARAMICROSS[lastindex-1] + CDLHIGHWAVE[lastindex-1] + \
                CDLHIKKAKE[lastindex-1] + CDLHIKKAKEMOD[lastindex-1] + CDLHOMINGPIGEON[lastindex-1] + CDLIDENTICAL3CROWS[lastindex-1] + CDLINNECK[lastindex-1] + \
                CDLINVERTEDHAMMER[lastindex-1] + CDLKICKING[lastindex-1] + CDLKICKINGBYLENGTH[lastindex-1] + \
                CDLLADDERBOTTOM[lastindex-1] + CDLLONGLEGGEDDOJI[lastindex-1] + CDLLONGLINE[lastindex-1] + \
                CDLMARUBOZU[lastindex-1] + CDLMATCHINGLOW[lastindex-1] + CDLMATHOLD[lastindex-1] + CDLMORNINGDOJISTAR[lastindex-1] + CDLMORNINGSTAR[lastindex-1] + \
                CDLONNECK[lastindex-1] + CDLPIERCING[lastindex-1] + CDLRICKSHAWMAN[lastindex-1] + CDLRISEFALL3METHODS[lastindex-1] + CDLSEPARATINGLINES[lastindex-1] + \
                CDLSHOOTINGSTAR[lastindex-1] + CDLSHORTLINE[lastindex-1] + CDLSPINNINGTOP[lastindex-1] + CDLSTALLEDPATTERN[lastindex-1] + CDLSTICKSANDWICH[lastindex-1] + \
                CDLTAKURI[lastindex-1] + CDLTASUKIGAP[lastindex-1] + CDLTHRUSTING[lastindex-1] + CDLTRISTAR[lastindex-1] + \
                CDLUNIQUE3RIVER[lastindex-1] + CDLUPSIDEGAP2CROWS[lastindex-1] + CDLXSIDEGAP3METHODS[lastindex-1]


        df_result.loc[df_result['銘柄コード'] == code, ['銘柄名称','日付',\
                'BBanTdy', 'BBanYdy', \
                'macd_flag', 'rsi_flag', 'stocha_flag',\
                'macd_delta', 'rsi_delta', 'stocha_delta',\
                'MacdTdy', 'MacdYdy', 'MacdSigTdy', 'MacdSigYdy',\
                'Rsi9Tdy', 'Rsi9Ydy', 'Rsi14Tdy', 'Rsi14Ydy',\
                'StochKTdy', 'StochKYdy','StochDTdy', 'StochDYdy',\
                'TwoCrows','ThreeBlackCrows','ThreeInside','ThreeLineStrike',\
                'ThreeOutside','ThreeStarsInSouth','ThreeWhiteSoldiers',\
                'AbandonedBaby','AdvancedBlock','BeltHold',\
                'BreakAway','ClosingMarubouzu','ConcealBabysWall',\
                'CounterAttack','DarkCloudCover','Doji',\
                'DojiStar','DragonflyDoji','Engulfing',\
                'EveningDojiStar','EveningStar','GapSideSideWhite',\
                'GraveStoneDoji','Hammer','HangingMan',\
                'Harami','HaramiCross','HighWave',\
                'Hikkake','HikkakeMod','HomingPigeon','IdenticalThreeCrows','Inneck',\
                'InvertedHammer','Kicking','KickingByLength',\
                'LadderBottom','LongLeggedDoji','LongLine',\
                'Marubozu','MatchingLow','MatHold','MorningDojiStar','MorningStar',\
                'OnNeck','Piercing','RickShawman','RiseAllThreeMethods','SeparatingLines',\
                'ShootingStar','ShortLine','SpinningTop','StalledPatter','StickSandwitch',\
                'Takuri','TakuriGap','Thrusting','TriStar',\
                'UniqueThreeRiver','UpsideGapTwoCrows','CrossSideGapThreeMethods',"AbsTotalScore","TotalScore"
                ]] = \
            [df_stock['銘柄名称'][lastindex-1], df_stock['日付'][lastindex-1], \
               (close[lastindex-1]-middle[lastindex-1])/(upper1[lastindex-1]-middle[lastindex-1]), (close[lastindex-2]-middle[lastindex-2])/(upper1[lastindex-2]-middle[lastindex-2]),\
                macd_flag, rsi_flag, stocha_flag, \
                macd_delta, rsi_delta, stocha_delta, \
                macd[lastindex-1], macd[lastindex-2], macdsignal[lastindex-1], macdsignal[lastindex-2], \
                rsi9[lastindex-1], rsi9[lastindex-2], rsi14[lastindex-1], rsi14[lastindex-2],\
                slowk[lastindex-1], slowk[lastindex-2], slowd[lastindex-1], slowd[lastindex-2],\
                CDL2CROWS[lastindex-1],CDL3BLACKCROWS[lastindex-1],CDL3INSIDE[lastindex-1],CDL3LINESTRIKE[lastindex-1],\
                CDL3OUTSIDE[lastindex-1],CDL3STARSINSOUTH[lastindex-1],CDL3WHITESOLDIERS[lastindex-1],\
                CDLABANDONEDBABY[lastindex-1],CDLADVANCEBLOCK[lastindex-1],CDLBELTHOLD[lastindex-1],\
                CDLBREAKAWAY[lastindex-1],CDLCLOSINGMARUBOZU[lastindex-1],CDLCONCEALBABYSWALL[lastindex-1],\
                CDLCOUNTERATTACK[lastindex-1],CDLDARKCLOUDCOVER[lastindex-1],CDLDOJI[lastindex-1],\
                CDLDOJISTAR[lastindex-1],CDLDRAGONFLYDOJI[lastindex-1],CDLENGULFING[lastindex-1],\
                CDLEVENINGDOJISTAR[lastindex-1],CDLEVENINGSTAR[lastindex-1],CDLGAPSIDESIDEWHITE[lastindex-1],\
                CDLGRAVESTONEDOJI[lastindex-1],CDLHAMMER[lastindex-1],CDLHANGINGMAN[lastindex-1],\
                CDLHARAMI[lastindex-1],CDLHARAMICROSS[lastindex-1],CDLHIGHWAVE[lastindex-1],\
                CDLHIKKAKE[lastindex-1],CDLHIKKAKEMOD[lastindex-1],CDLHOMINGPIGEON[lastindex-1],CDLIDENTICAL3CROWS[lastindex-1],CDLINNECK[lastindex-1],\
                CDLINVERTEDHAMMER[lastindex-1],CDLKICKING[lastindex-1],CDLKICKINGBYLENGTH[lastindex-1],\
                CDLLADDERBOTTOM[lastindex-1],CDLLONGLEGGEDDOJI[lastindex-1],CDLLONGLINE[lastindex-1],\
                CDLMARUBOZU[lastindex-1],CDLMATCHINGLOW[lastindex-1],CDLMATHOLD[lastindex-1],CDLMORNINGDOJISTAR[lastindex-1],CDLMORNINGSTAR[lastindex-1],\
                CDLONNECK[lastindex-1],CDLPIERCING[lastindex-1],CDLRICKSHAWMAN[lastindex-1],CDLRISEFALL3METHODS[lastindex-1],CDLSEPARATINGLINES[lastindex-1],\
                CDLSHOOTINGSTAR[lastindex-1],CDLSHORTLINE[lastindex-1],CDLSPINNINGTOP[lastindex-1],CDLSTALLEDPATTERN[lastindex-1],CDLSTICKSANDWICH[lastindex-1],\
                CDLTAKURI[lastindex-1],CDLTASUKIGAP[lastindex-1],CDLTHRUSTING[lastindex-1],CDLTRISTAR[lastindex-1],\
                CDLUNIQUE3RIVER[lastindex-1],CDLUPSIDEGAP2CROWS[lastindex-1],CDLXSIDEGAP3METHODS[lastindex-1],AbsTotalScore, TotalScore\
            ]

# 経過時間を出力(秒)
end_time = time.perf_counter()
elapsed_time = end_time - start_time
print("データ作成時間 = ", elapsed_time)

print(df_result)
engine = alchemy_engine(database_file_name)
df_result.to_sql(table_name, engine, if_exists='append', index=False)

# 経過時間を出力(秒)
end_time = time.perf_counter()
elapsed_time = end_time - start_time
print("データ出力込みの時間 = ", elapsed_time)

#
# Slack Log Send
#
dt_now = datetime.datetime.now()
slack.notify(text = dt_now.strftime('%Y/%m/%d %H:%M:%S') + " : TA-Lib Processing Finish")
