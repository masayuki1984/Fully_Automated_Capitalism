#coding:utf-8

import crawler_config
import logging
import requests
import csv
import datetime
import sys
import slackweb
import configparser


BASE_DIR = '/usr/local/script/'
account = BASE_DIR + 'config/account.ini'
account_config = configparser.ConfigParser()
account_config.read(account, 'UTF-8')

path = BASE_DIR + 'config/path.ini'
path_config = configparser.ConfigParser()
path_config.read(path, 'UTF-8')

KABU_PLUS_ID = account_config.get('kabu_plus', 'KABU_PLUS_ID')
KABU_PLUS_PW = account_config.get('kabu_plus', 'KABU_PLUS_PW')
DOWNLOAD_SAVE_DIR = path_config.get('csv_path', 'data_base') + path_config.get('csv_path', 'japan_all_stock_prices_new_format')

args = sys.argv

class CSVFile:

    KABU_PLUS_ID = ""
    KABU_PLUS_PW = ""

    def __init__(self, KABU_PLUS_ID, KABU_PLUS_PW):
        self.KABU_PLUS_ID = KABU_PLUS_ID
        self.KABU_PLUS_PW = KABU_PLUS_PW

    def retrieve_csv_file(self, url):
        res = requests.get(url, auth=(KABU_PLUS_ID, KABU_PLUS_PW))
        if res.status_code != 200:
            log.error('ステータスコードが200以外')
            slack.notify(text="日本株全銘柄（新フォーマット）取得処理：異常終了")
            sys.exit(1)
        data = res.content.decode('shift-jis')
        return data

    def to_csv(self, data, file=None):
        if file:
            with open(file, 'w', encoding='utf-8') as f:
                writer = csv.writer(f)
                reader = csv.reader(data.splitlines())
                for row in reader:
                    writer.writerow(row)


if __name__ == '__main__':
    # モジュール名でロガーを生成する(メインモジュールは 名前が '__main__' になる)
    log = logging.getLogger(__name__)

    # Slack Incoming webhook設定
    slack_log_url = account_config.get('slack', 'slack_log_url')
    slack = slackweb.Slack(url=slack_log_url)

    # 対象の日付を設定（引数でYYYYMMDD形式で日付を入れるとその日付のファイルを対象とする）
    if len(args) < 2:
        TODAY = datetime.date.today()
    else:
        TARGET_DAY = args[1]
        TODAY = datetime.datetime(int(TARGET_DAY[:4]), int(TARGET_DAY[4:6]), int(TARGET_DAY[-2:]))
    file_name_date_part = str(TODAY.year) + '{:0=2}'.format(TODAY.month) + '{:0=2}'.format(TODAY.day)

    log.info('日本株全銘柄(新フォーマット)取得処理 : 開始')
    # CSVファイルを取り扱うクラスの作成
    csv_file = CSVFile(KABU_PLUS_ID, KABU_PLUS_PW)

    file_name = 'japan-all-stock-prices-2_' + file_name_date_part + '.csv'
    URL = 'https://csvex.com/kabu.plus/csv/japan-all-stock-prices-2/daily/' + file_name
    log.info('CSVファイル取得処理（日本株全銘柄）ファイル名:' + file_name)
    data = csv_file.retrieve_csv_file(URL)
    csv_file.to_csv(data, DOWNLOAD_SAVE_DIR + file_name)
    log.info('日本株全銘柄(新フォーマット)取得処理 : 終了')
    slack.notify(text="日本株全銘柄（新フォーマット）取得処理：正常終了")
    sys.exit(0)