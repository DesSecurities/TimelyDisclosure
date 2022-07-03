# disclosure
Timely Disclosure will be notified you ASAP.

[このアプリケーションについて]
このアプリケーションは、東京証券取引所から開示される「適時開示」をTDNETもしくは株探からピックアップし高速にSLACKに通知するアプリケーションです。

[起動方法]
・楽天RSSをしようするため pip install rakuten_rssを実行して、このプログラムをCloneした場所に置きます
・このアプリは楽天RSS1(注意：2ではない）を使用していますので、楽天マーケットスピード（注意：楽天マーケットスピード2ではない）を起動します。
・次に、デスクトップにある「RealtimeSpreadsheet.exe」を起動しRSSを使用可能にします。
・slack = slackweb.Slack()で指定しているURLを自分のSLACKの通知先に変更します（そうしないと、届きません、絶対に書き換えてください）
　自分のSLACKチャンネルの使い方は https://api.slack.com/ を参照してください

ここまで出来上がれば、あとはプログラムを起動するだけです。

1.TDNET版
　python TdnetDisclosureWatcher.py

2.株探版
　python KabutanDisclosureWatcher.py
