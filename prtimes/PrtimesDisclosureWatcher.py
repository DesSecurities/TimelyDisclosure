#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# PRTIMESプレスリリースチェッカー
# Copyright (c) 2022, Des Securities all rights reserved.
#
# Filename : PrtimesDisclosureWatcher.py
# Date     : June 22, 2022
# Author   : Des Securities referenced カビー山田さん
#

#
# 動かし方
# 

# In[ ]:

#
# import libraries（下記3つのパラメータは各自でDesSecuritiesConst.pyに設定してください）
#
#
# Python Library Pathの設定
#
import sys

sys.path.append("const")
sys.path.append("libraries")

from DesSecuritiesConst import MY_SLACK_WEB_HOOK_DISCLOSURE    # WebHookのURLをコミットしないようにするため別ファイルからインポート

from DesSecuritiesConst import PRTIMES_LOGIN_ID     # PRTIMEのログインIDをコミットしないようにするため別ファイルからインポート
from DesSecuritiesConst import PRTIMES_LOGIN_PW     # PRTIMEのログインPWをコミットしないようにするため別ファイルからインポート
from DesSecuritiesConst import WEB_DRIVER_PATH      # WEB DRIVER PATH

import os
import sys
import time
import numpy as np
import calendar
import csv

from datetime import date, timedelta
from datetime import datetime

import pandas as pd
import requests

import mplfinance as mpf
import talib as ta
import japanize_matplotlib 

import openpyxl
from openpyxl import load_workbook
from openpyxl.styles.fonts import Font
from openpyxl.drawing.image import Image
from openpyxl.styles import Alignment
from openpyxl.styles.borders import Border, Side

from bs4 import BeautifulSoup
from selenium import webdriver

import urllib
from urllib.parse import urljoin
from urllib.request import urlopen
import urllib.request as req

import slackweb
#
# 楽天RSS（Ⅱではない）
# これを使用できるようにするためには
# １．"pip install rakuten_rss"を実行、"ddeclient.py"をこのプログラムと同じフォルダに配置
# ２．楽天マーケットスピードをインストール
# ３．デスクトップにインストールされた"Realtime SpreadSheet.exe"を起動（マーケットスピード２と同時使用は未確認）
#
from ddeclient import DDEClient
def rss(code, item):
	""" 楽天RSSから情報を取得
	Parameters
	----------
	code : str
		株価や先物のコード　例：東京電力の場合'9501.T'
	item :
	Returns
	-------
	str

	Examples
	----------

	>>>rss('9501.T' , '始値')
	'668.00'

	>>>rss('9501.T' , '現在値')
	'669.00'

	>>>rss('9501.T' , '銘柄名称')
	'東京電力ＨＤ'

	>>>rss('9501.T' , '現在値詳細時刻')
	'15:00:00'

	"""

	dde = DDEClient("rss", str(code))
	try:
		res = dde.request(item).decode('sjis').strip()
	except:
		print('fail: code@', code)
		res = 0
	finally:
		dde.__del__()
	return res


def rss_dict(code, *args):
	"""
	楽天RSSから辞書形式で情報を取り出す(複数の詳細情報問い合わせ可）

	Parameters
	----------
	code : str
	args : *str

	Returns
	-------
	dict

	Examples
	----------
	>>>rss_dict('9502.T', '始値','銘柄名称','現在値')
	{'始値': '1739.50', '現在値': '1661.50', '銘柄名称': '中部電力'}


	"""

	dde = DDEClient("rss", str(code))
	res = {}
	try:
		for item in args:
			res[item] = dde.request(item).decode('sjis').strip()
	except:
		print('fail: code@', code)
		res = {}
	finally:
		dde.__del__()
	return res


def fetch_open(code):
	""" 始値を返す（SQ計算用に関数切り出し,入力int）

	Parameters
	----------
	code : int
	Examples
	---------
	>>> fetch_open(9551)
	50050
	"""

	return float(rss(str(code) + '.T', '始値'))

#
# Slack Web Hook URL
#
# 自分のSLACKチャンネルを取得して、そのWebHook URLを代入してください
#
# Des Securities #disclosure 
slack = slackweb.Slack(url = MY_SLACK_WEB_HOOK_DISCLOSURE)     # MY_SLACK_WEB_HOOK_DISCLOSUREは別ファイルからインポートするか、直値を代入してください

#
# Master disclosure DB
# このDBに起動後からの開示情報を蓄積する
#
DisclosureDF = pd.DataFrame(columns=['日付', '時刻', 'コード', '会社名', '表題', 'url', 'XBRL'])

#
# ザラバチェック
#
def IsMarketOpened():
    #
    # タイムスタンプの時刻を取得
    #
    dt_now = datetime.now()
    dt_market_open = datetime(dt_now.year, dt_now.month, dt_now.day, 7, 59, 59)
    dt_market_close = datetime(dt_now.year, dt_now.month, dt_now.day, 16, 29, 59)

    if (dt_now > dt_market_open) and (dt_now < dt_market_close):
        return True
    else:
        return False

#
# 楽天証券で取り扱う銘柄かどうかをチェックする関数
#
def IsRakutenAvailable(code):
    exclusion_code = [1412, 1432, 1440, 1445, 1452, 2149, 2452, 2977, 2985, 2990, 2992,     # 東証ProMarket
                      2994, 3448, 3450, 3456, 3483, 3827, 4197, 4257, 4426, 4589, 5072, 
                      5073, 5075, 5077, 5858, 6168, 6174, 6228, 6576, 6596, 6695, 7056, 
                      7075, 7098, 7125, 7132, 7136, 7137, 7170, 7176, 7355, 7364, 7680, 
                      7690, 7691, 7693, 7790, 9146, 9243, 9276, 9388, 9465, 
                      4875, 1557, 2070, 1385, 1672,                                         # 東証外国株やPTSで取り扱わないETF銘柄など
                      8554, 8559, 8560,                                                     # 福岡証券取引所
                      3808]                                                                 # 名証セントレックス
    
    return(code in exclusion_code)

def sendSlackDM(code, message):
    #
    # 東証以外の地方証券を除外する、ただし東証ProMarketや東証REITは削除できないのでコード判別
    #
    if IsRakutenAvailable(code):
        return

    #
    # タイムスタンプの時刻を取得
    #
    dt_now = datetime.now()

    #
    # 楽天RSSを使用して株価情報を取得します
    # https://www.rakuten-sec.co.jp/MarketSpeed/onLineHelp/msman1_11_6a.html
    #
    open = 0
    high = 0
    low = 0
    close = 0
    volume = 0
    vwap = 0
    opentime = ""
    hightime = ""
    lowtime = ""
    closetime = ""
    touraku = 0
    tourakuratio = 0
    per = 0
    pbr = 0
    latest_close = 0
    latest_date = ""
    latest_tick = ""
    latest_flag = ""
    market_type = ""
    meigaraname = ""
    separator = "==============================\r\n"

    #
    # 出来高が無いと、４本値取得がエラーになるので回避
    #
    stock_code = str(code) + '.T'

    #
    # ザラバもしくは出来高が無かった場合は、最後の出来高があった日を取得します
    #
    volume = float(rss(stock_code, '出来高'))

    if IsMarketOpened():    # ザラバ
        latest_close = float(rss(stock_code, '前日終値'))
        latest_date = rss(stock_code, '前日日付')
        latest_flag = rss(stock_code, '前日終値フラグ')
        market_type = rss(stock_code, "市場部略称")
    else:                   # PTS
        market_type = rss(stock_code, "市場部略称")
        if volume > 0:
            latest_close = float(rss(stock_code, '現在値'))
            latest_date = rss(stock_code, '現在値詳細時刻')
            latest_tick = rss(stock_code, '現在値ティック')
            latest_flag = rss(stock_code, '現在値フラグ')
        else:
            latest_close = float(rss(stock_code, '前日終値'))
            latest_date = rss(stock_code, '前日日付')
            latest_flag = rss(stock_code, '前日終値フラグ')
        stock_code = str(code) + '.JNX'

    #
    # 楽天証券に存在しない銘柄は通知しない
    #
    if market_type == 0:
        return
    else:
        meigaraname = rss(stock_code, '銘柄名称')

    #
    # ザラバもしくはPTSの出来高を取得します
    #
    volume = float(rss(stock_code, '出来高'))

    if volume > 0:
        open = float(rss(stock_code, '始値'))
        opentime = rss(stock_code, '始値詳細時刻')
        high = float(rss(stock_code, '高値'))
        hightime = rss(stock_code, '高値詳細時刻')
        low  = float(rss(stock_code, '安値'))
        lowtime = rss(stock_code, '安値詳細時刻')
        close = float(rss(stock_code, '現在値'))
        closetime = rss(stock_code, '現在値詳細時刻')
        touraku = float(rss(stock_code, '前日比'))
        tourakuratio = float(rss(stock_code, '前日比率'))
        vwap = float(rss(stock_code, '出来高加重平均'))
        per = float(rss(stock_code, 'ＰＥＲ'))
        pbr = float(rss(stock_code, 'ＰＢＲ'))

    #
    # タイムスタンプの時刻を取得
    #
    dt_now = datetime.now()

    #
    # SLACK送信部分（TwitterAPI・LINE APIに置き換えることによって、プラットフォームを変更できます）
    #

    #
    # ザラバ中ならば
    #
    itadict = ['売成行数量', '買成行数量', 'OVER気配数量', 'UNDER気配数量',
               '最良売気配値１０', '最良売気配値９', '最良売気配値８', '最良売気配値７', '最良売気配値６',
               '最良売気配値５', '最良売気配値４', '最良売気配値３', '最良売気配値２', '最良売気配値１',
               '最良買気配値１', '最良買気配値２', '最良買気配値３', '最良買気配値４', '最良買気配値５',
               '最良買気配値６', '最良買気配値７', '最良買気配値８', '最良買気配値９', '最良買気配値１０',
               '最良売気配数量１０', '最良売気配数量９', '最良売気配数量８', '最良売気配数量７', '最良売気配数量６',
               '最良売気配数量５', '最良売気配数量４', '最良売気配数量３', '最良売気配数量２', '最良売気配数量１',
               '最良買気配数量１', '最良買気配数量２', '最良買気配数量３', '最良買気配数量４', '最良買気配数量５',
               '最良買気配数量６', '最良買気配数量７', '最良買気配数量８', '最良買気配数量９', '最良買気配数量１０']

    itaprice = []

    if IsMarketOpened():   # 10本板表示しないバージョン
        for index, name in enumerate(itadict):
            price = rss(stock_code, name)
            if price != "":
                itaprice.append(float(price))
            else:
                itaprice.append(0.0)
        
        meigara_info = "[" + str(code) + ":" + market_type + "]" + \
                    meigaraname + " [" + \
                    "<https://finance.yahoo.co.jp/search/?query=" + str(code) + " | ヤ> " + \
                    "<https://kabutan.jp/stock/?code=" + str(code) + " | 株> " + \
                    "<https://shikiho.toyokeizai.net/stocks/" + str(code) + " | 四> " + \
                    "<https://www.rakuten-sec.co.jp/web/search/?page=1&d=&doctype=all&q=" + str(code) + \
                    "&sort=0&pagemax=10&imgsize=0/"  + " | 楽天>]" + "\r\n"
        itatext = meigara_info + "成売 = {:,.0f} 成買 = {:,.0f}\r\n".format(itaprice[0], itaprice[1]) + \
                  "Over = {:,.0f}\r\n".format(itaprice[2])
        for num in range(4, 4 + 20):
            if num < 4 + 10:
                itatext = itatext + "{:8,.0f}    {:8,.0f}\r\n".format(itaprice[num + 20], itaprice[num])
            else:
                itatext = itatext + "            {:8,.0f}    {:8>,.0f}\r\n".format(itaprice[num], itaprice[num + 20])

        itatext += "               UNDER {:,.0f}\r\n".format(itaprice[3])

        #
        # SLACK送信部分（TwitterAPI・LINE APIに置き換えることによって、プラットフォームを変更できます）
        #
        slack.notify(text = message + "\r\n\r\n" + itatext)
    else:
        meigara_info = "[" + str(code) + ":" + market_type + "]" + \
                    meigaraname + " [" + \
                    "<https://finance.yahoo.co.jp/search/?query=" + str(code) + " | ヤ> " + \
                    "<https://kabutan.jp/stock/?code=" + str(code) + " | 株> " + \
                    "<https://shikiho.toyokeizai.net/stocks/" + str(code) + " | 四> " + \
                    "<https://www.rakuten-sec.co.jp/web/search/?page=1&d=&doctype=all&q=" + str(code) + \
                    "&sort=0&pagemax=10&imgsize=0/"  + " | 楽天>]" + "\r\n"
        slack.notify(text = message + "\r\n\r\n" + meigara_info + \
                    "当日終値 = " + "{:,.0f}".format(latest_close) + " (" + str(latest_date) + ") "  + str(latest_tick) + " " + str(latest_flag) +"\r\n" + \
                    "PTS 始値 = " + "{:,.0f}".format(open) + " (" + str(opentime) + ")\r\n" + \
                    "PTS 高値 = " + "{:,.0f}".format(high) + " (" + str(hightime) + ")\r\n" + \
                    "PTS 安値 = " + "{:,.0f}".format(low) + " (" + str(lowtime) + ")\r\n" + \
                    "PTS 現在値 = " + "{:,.0f}".format(close) + " (" + str(closetime) + ") " + "{:,.0f}".format(touraku) + "(" + "{:,.2f}".format(tourakuratio) + "%)\r\n" + \
                    "PTS 出来高 = " + "{:,.0f}".format(volume) + "  PTS VWAP = " + "{:,.2f}".format(vwap) + "\r\n" + \
                    "PER = " + "{:,.2f}".format(per) + " PBR = " + "{:,.2f}".format(pbr))

#
# メインプログラム
#
def main():
    browser = webdriver.Chrome(executable_path=WEB_DRIVER_PATH)
    browser.implicitly_wait(3)

    #
    # PRTIMESアカウント
    #                                
    url_login = "https://prtimes.jp/"
    browser.get(url_login)
    time.sleep(3)
    print("Login to PRTIMES")
    
    browser.find_element_by_xpath('/html/body/header/div/div[1]/ul/li[3]/a').click()
    time.sleep(3)
    
    browser.find_element_by_xpath('/html/body/div[1]/section/div[1]/div/div[2]/a[2]').click()
    time.sleep(3)
    
    #
    # PRTIMESへのログインするためのアカウント・パスワード
    #
    elem = browser.find_element_by_name('mail')
    elem.clear()
    elem.send_keys(PRTIMES_LOGIN_ID)
    elem = browser.find_element_by_name('pass')
    elem.clear()
    elem.send_keys(PRTIMES_LOGIN_PW)
    elem.submit()
    
    time.sleep(3)
    
    no_code_company_list = []
    
    url0 = 'https://prtimes.jp/main/html/rd/p/'
    
    #
    # 銘柄コード, キーワード(銘柄名称)の辞書 CSVデータの読み込み
    #
    df_meigara = pd.read_csv("data/CorporateCodeIndex.csv", names=('銘柄コード', '銘柄名称'))

    count = 0
#    for i in range(1000):

    while(True):
        #
        # もし現在時刻をHH:MM:10まで待つ処理
        #
        wait_second = 10    # 1～10あたりにして、更新直後を狙って読みに行く
        prev_second = datetime.now().second

        while(True):
            dt_now = datetime.now()
            if (dt_now.second >= wait_second) and (prev_second < wait_second):
                break
            prev_second = dt_now.second
            time.sleep(1.0)

        #
        #ページを更新(ブラウザのリフレッシュ)
        #
        browser.refresh()
        dt_now = datetime.now()
        print(dt_now.strftime('Refresh PRTimes Web page : %Y/%m/%d %H:%M:%S'))

        html = browser.page_source.encode('utf-8')
        parse_html = BeautifulSoup(html,'html.parser')

        section_all = parse_html.findAll("section")

        a = [] #リストを６つ用意
        
        dt_now = datetime.now()
        dt_now_str = str(dt_now)[:16] 

        for section in section_all:
            if len(a) > 15: #開示件数。
                break

            if section.a.get("href") == "":
                print("リンク無いです")
                continue

            kaiji_text = section.h2.a.text
            a += [kaiji_text]

            url_temp = str(section.h2.a.get("href"))        # a href="action.php?run=mypage&amp;page=detail&amp;company_id=51247&amp;release_id=75"
            url_temp = url_temp.split("id=")                # id= で分けて、company_id　release_id　を取り出す
            company_id = url_temp[1].split("&")[0]
            release_id = url_temp[2]
            kaiji_url = url1 = url0  +  '{}.{}.html'.format(release_id.zfill(9),company_id.zfill(9))

            ps = section.findAll('p') 
            #count = 0
            for p in ps:
                #count += 1
                if p.get("class")[0] == "company-name": 
                    company_name = p.text.replace('株式会社', '')

            #
            # から約〇〇分前という文字列以外は1時間以上経過しているので処理しない
            #
            kaiji_time = dt_now_str + " から約" + section.time.text
            print(kaiji_text, kaiji_time)
#            if not '分前' in kaiji_time:
#                #print("ここまでが1時間前の開示") ############## 1時間以内の告知
#                break
            
            code = -1
            for index, row in df_meigara.iterrows():
                if row['銘柄名称'] in company_name:
                    code = int(row['銘柄コード'])
                    break
            #
            # 上場企業に該当していない場合
            #
            if code is -1:
                continue

            #
            # お好みで、取得したい開示キーワードをここにセットしま
            # 
            keyword = ['経済産業省','厚生労働省','臨床','治験','新薬','療法','抗ウィルス','承認','ワクチン',
                                 '業務提携','補助金','助成金','配当金受領','出資',
                                 '選定','認証','指数','オフィシャルパートナー','認定','採用','FDA',
                                 '最新作','世界累計','事前登録','累計出荷数','契約数','世界初','日本初','初','特許','提供開始','世界で初めて',
                                 'インド','中国','アメリカ','欧州','米国',
                                 'メタバース','WEB3','NFT','ＮＦＴ','半導体','発電','電力','自動運転',
                                 'Google','シャープ','トヨタ','マツダ','りそな銀行','基礎科学']

            isFoundIndex = 0
            for index, word in enumerate(keyword):
                if word in kaiji_text:
                    isFoundIndex = index
                    break

            if isFoundIndex is not -1:
                '''
                slack_message = dt_now.strftime('%Y/%m/%d %H:%M:%S') + \
                                " | [PRTIMES] Press Release\r\n◆" + keyword[isFoundIndex] + "◆" + \
                                str(code) + "◆" + \
                                company_name + "◆" + \
                                kaiji_time + "\n<" +  \
                                kaiji_url + "| " + \
                                kaiji_text + ">"
                '''
                slack_message = "==============================\r\n" + \
                                dt_now.strftime('%Y/%m/%d %H:%M:%S') + " | [PRTIMES] 開示\r\n" + \
                                "==============================\r\n" + \
                                company_name + " : " + \
                                kaiji_time + "\r\n<" +  \
                                kaiji_url + "| " + \
                                kaiji_text + ">"
                sendSlackDM(code, slack_message)

#
# メインプログラム
#
if __name__ == '__main__':
    main()



# %%
