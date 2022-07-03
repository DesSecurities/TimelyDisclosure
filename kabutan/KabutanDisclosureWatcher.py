#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# 株探適時開示チェッカー
# Copyright (c) 2022, Des Securities all rights reserved.
#
# Filename : KabutanDisclosureWatcher.py
# Date     : June 8, 2022
# Author   : Des Securities
#

#
# Python Library Pathの設定
#
import sys
sys.path.append("const")
sys.path.append("libraries")

from DesSecuritiesConst import MY_SLACK_WEB_HOOK_DISCLOSURE    # WebHookのURLをコミットしないようにするため別ファイルからインポート
from ExcludeKeyword import EXCLUDE_KEYWORD

import slackweb
import pandas as pd
import requests
import requests_cache
from datetime import datetime
import time
from bs4 import BeautifulSoup
from collections import OrderedDict

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
DisclosureDF = pd.DataFrame(columns=['Code', 'Name', 'Market', 'Type', 'Title', 'DisclosureDateTime', 'kaiji_url', "sentflag"])

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
                      4875, 1557, 2070,1385, 1672,                                          # 東証外国株やPTSで取り扱わないETF銘柄など
                      8554, 8559, 8560,                                                     # 福岡証券取引所
                      3808]                                                                 # 名証セントレックス
    
    return(code in exclusion_code)

def setSlackDefaultFont():
    slack.notify(text = "/slackfont Comic Sans MS")

def sendSlackDM(kaiji):
    if(IsRakutenAvailable(int(kaiji['Code'][:4])) or
       (("東証" in (kaiji['Market'][:2])) == False)):   # 東証以外の地方証券を除外する、ただし東証ProMarketや東証REITは削除できないのでコード判別
        return

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
    separator = "==============================\r\n"

    #
    # 出来高が無いと、４本値取得がエラーになるので回避
    #
    stock_code = kaiji['Code'][:4] + '.T'

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
        stock_code = kaiji['Code'][:4] + '.JNX'

    #
    # 楽天証券に存在しない銘柄は通知しない
    #
    if market_type == 0:
        return

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
    itadict = [ '売成行数量', '買成行数量', 'OVER気配数量', 'UNDER気配数量',
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
        
        itatext = "成売 = {:,.0f} 成買 = {:,.0f}\r\n".format(itaprice[0], itaprice[1]) + \
                  "Over = {:,.0f}\r\n".format(itaprice[2])
        for num in range(4, 4 + 20):
            if num < 4 + 10:
                if itaprice[num] > 0:       # ストップ高の場合
                    itatext = itatext + "{:8,.0f}    {:8,.0f}\r\n".format(itaprice[num + 20], itaprice[num])
            else:
                if itaprice[num + 20] > 0:  # ストップ安の場合
                    itatext = itatext + "            {:8,.0f}    {:8>,.0f}\r\n".format(itaprice[num], itaprice[num + 20])

        itatext += "               UNDER {:,.0f}\r\n".format(itaprice[3])

        try:
            slack.notify(text = separator + dt_now.strftime('%Y/%m/%d %H:%M:%S') + \
                " | [株探]適時開示\r\n" + separator + "[" + kaiji['Code'][:4] + ":" + market_type + "]" + \
                kaiji['Name'] + " [" + \
                "<https://finance.yahoo.co.jp/search/?query=" + kaiji['Code'][:4] + " | ヤ> " + \
                "<https://kabutan.jp/stock/?code=" + kaiji['Code'][:4] + " | 株> " + \
                "<https://shikiho.toyokeizai.net/stocks/" + kaiji['Code'][:4] + " | 四> " + \
                "<https://www.rakuten-sec.co.jp/web/search/?page=1&d=&doctype=all&q=" + kaiji['Code'][:4] + \
                "&sort=0&pagemax=10&imgsize=0/"  + " | 楽天>]" + \
                "\r\n20" + kaiji['DisclosureDateTime'] + ":00"\
                " | <" + kaiji['kaiji_url'] + "| " + \
                kaiji['Title'] + ">" + "\r\n" + \
                "前日終値 = " + "{:,.0f}".format(latest_close) + " (" + str(latest_date) + ") "  + str(latest_tick) + " " + str(latest_flag) +"\r\n" + \
                "始値 = " + "{:,.0f}".format(open) + " (" + str(opentime) + ")\r\n" + \
                "高値 = " + "{:,.0f}".format(high) + " (" + str(hightime) + ")\r\n" + \
                "安値 = " + "{:,.0f}".format(low) + " (" + str(lowtime) + ")\r\n" + \
                "現在値 = " + "{:,.0f}".format(close) + " (" + str(closetime) + ") " + "{:,.0f}".format(touraku) + "(" + "{:,.2f}".format(tourakuratio) + "%)\r\n" + \
                "出来高 = " + "{:,.0f}".format(volume) + " VWAP = " + "{:,.2f}".format(vwap) + "\r\n" + \
                "PER = " + "{:,.2f}".format(per) + " PBR = " + "{:,.2f}".format(pbr) + "\r\n\r\n" + \
                itatext)

        except TypeError as e:
            print(kaiji)
            slack.notify(text = str(e) + "\r\n" + kaiji)
    #
    # 時間外取引中ならば
    #
    else:      # 10本板表示するバージョン
        try:
            slack.notify(text =  separator + dt_now.strftime('%Y/%m/%d %H:%M:%S') + \
                " | [株探]適時開示\r\n" + separator + "[" + kaiji['Code'][:4] + ":" + market_type + "]" + \
                kaiji['Name'] + " [" + \
                "<https://finance.yahoo.co.jp/search/?query=" + kaiji['Code'][:4] + " | ヤ> " + \
                "<https://kabutan.jp/stock/?code=" + kaiji['Code'][:4] + " | 株> " + \
                "<https://shikiho.toyokeizai.net/stocks/" + kaiji['Code'][:4] + " | 四> " + \
                "<https://www.rakuten-sec.co.jp/web/search/?page=1&d=&doctype=all&q=" + kaiji['Code'][:4] + \
                "&sort=0&pagemax=10&imgsize=0/"  + " | 楽天>]" + \
                "\r\n20" + kaiji['DisclosureDateTime'] + ":00"\
                " | <" + kaiji['kaiji_url'] + "| " + \
                kaiji['Title'] + ">" + "\r\n" + \
                "当日終値 = " + "{:,.0f}".format(latest_close) + " (" + str(latest_date) + ") "  + str(latest_tick) + " " + str(latest_flag) +"\r\n" + \
                "PTS 始値 = " + "{:,.0f}".format(open) + " (" + str(opentime) + ")\r\n" + \
                "PTS 高値 = " + "{:,.0f}".format(high) + " (" + str(hightime) + ")\r\n" + \
                "PTS 安値 = " + "{:,.0f}".format(low) + " (" + str(lowtime) + ")\r\n" + \
                "PTS 現在値 = " + "{:,.0f}".format(close) + " (" + str(closetime) + ") " + "{:,.0f}".format(touraku) + "(" + "{:,.2f}".format(tourakuratio) + "%)\r\n" + \
                "PTS 出来高 = " + "{:,.0f}".format(volume) + "  PTS VWAP = " + "{:,.2f}".format(vwap) + "\r\n" + \
                "PER = " + "{:,.2f}".format(per) + " PBR = " + "{:,.2f}".format(pbr))

        except TypeError as e:
            print(kaiji)
            slack.notify(text = str(e) + "\r\n" + kaiji)

#
# 30秒毎に株探開示情報をチェックする無限ループ
#
while(True):
    #
    # もし現在時刻をHH:MM:30まで待つ処理
    #
    wait_second = 40    # 1～10あたりにして、更新直後を狙って読みに行く
    prev_second = datetime.now().second

    while(True):
        dt_now = datetime.now()
        if (dt_now.second >= wait_second) and (prev_second < wait_second):
            break
        prev_second = dt_now.second
        time.sleep(1.0)

    #
    # 処理時刻表示
    #
    dt_now = datetime.now()
    print(dt_now.strftime('%Y/%m/%d %H:%M:%S'))

    #
    # 各ページから取得した適時開示情報を格納するリスト
    #
    DisclosureList = list()

    # 一度に取得する株探ページ数（多く取りすぎても無駄なので2ページ）
    max_pages=2

    for page in range(1, max_pages + 1):
        with requests_cache.disabled():
            response = requests.get(f'https://kabutan.jp/disclosures/?kubun=&page={page}')
            soup = BeautifulSoup(response.text, "lxml")

        table = soup.find("table", {"class":"stock_table"})
        if not table == None:
            # <tr>で囲まれた中に、開示リストが含まれているので全て取得する。row - 1個分の開示リストが格納されている
            rows = table.findAll("tr")

            # 毎回、格納する開示リストはクリアする
            DisclosureList.clear

            for i, row in enumerate(rows):
                #
                # 一番最初に見つかった部分は項目なので、何もしないで次の部分を取得します
                #
                if i == 0:  # header skip
                    continue

                # <td>と<th>で挟まれた部分を全て探す
                csvRow = [cell.get_text() for cell in row.findAll(['td', 'th'])]

                # ハイパーリンク部分<a href>の部分を探す
                a_tag = row.findAll("a")

                # 開示ハイパーリンクURLを初期化して、抜き出します
                kaiji_url = ""
                if a_tag:
                    #
                    # a_tag[0] : 株探の銘柄情報ページへのハイパーリンク
                    # a_tag[1] : 適時開示記事内容PDFファイルへのハイパーリンク
                    #
                    kaiji_url = a_tag[1].attrs['href']

                    #
                    # この時点ではcsvRow = ["銘柄コード", ”銘柄名称", "市場名称", "情報種別", "開示内容", "開示時刻"] なので
                    # この後ろに開示記事内容のハイパーリンクを付加します
                    # csvRow = ["銘柄コード", ”銘柄名称", "市場名称", "情報種別", "開示内容", "開示時刻", "開示ハイパーリンク"]
                    # 
                    csvRow.append(kaiji_url)
            
                if DisclosureDF.empty:
                    DisclosureList.append(csvRow)
                else:
                    flag = DisclosureDF['kaiji_url'].isin([kaiji_url])
                    if flag.sum() == 0:
                        print(csvRow)
                        DisclosureList.append(csvRow)
    
    datanum = len(DisclosureList)
    if datanum > 0:
        tmpdf = pd.DataFrame(DisclosureList, columns=["Code", "Name", "Market", "Type", "Title", "DisclosureDateTime", "kaiji_url"])
        tmpdf['sentflag'] = False
        tmpdf = tmpdf.dropna(subset=['Code'])
        DisclosureDF = DisclosureDF.append(tmpdf, ignore_index = True)

        #
        # Slack Log Send
        #
        dt_now = datetime.now()
        for index, kaiji in DisclosureDF.iterrows():
            if kaiji['sentflag'] == True:
                continue
            else:
                #print("Target : ")
                #print(kaiji)
                #print("DB     : ")
                #print(DisclosureDF.loc[index])
                DisclosureDF.loc[index]['sentflag'] = True

            dt_now = datetime.now()
            
            #
            # キーワード指定でフィルタリング通知を行う場合
            #
            if True:
                keyword_found = False

                #
                # 最初の20文字がASCII文字ならば、外国人向け適時開示と判断して送信しない
                #
                if kaiji['Title'][:20].isascii():
                    keyword_found = True
                else:
                    #
                    # 除外キーワードが含まれる適時開示は送信しない
                    #
                    for keyword in EXCLUDE_KEYWORD:
                        if (keyword in kaiji['Title']):
                            #
                            # SLACK送信しない
                            #
                            keyword_found = True
                            break

                #
                # SLACK送信部分（TwitterAPI・LINE APIに置き換えることによって、プラットフォームを変更できます）
                #
                if keyword_found == False:
                    sendSlackDM(kaiji)
            
            #
            # キーワード指定でフィルタリング通知を行わない場合
            #
            else:
                #
                # SLACK送信部分（TwitterAPI・LINE APIに置き換えることによって、プラットフォームを変更できます）
                #
                sendSlackDM(kaiji)

#
# メインプログラム
#
#if __name__ == '__main__':
#    main()
