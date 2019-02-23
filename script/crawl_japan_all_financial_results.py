#coding:utf-8

import logging_config
import logging
import requests
import csv
import datetime
import sys
import slackweb
import configparser


BASE_DIR = '/usr/local/script/'
config_path = BASE_DIR + 'config/account.ini'
config = configparser.ConfigParser()
config.read(config_path, 'UTF-8')
KABU_PLUS_ID = config.get('kabu_plus', 'KABU_PLUS_ID')
KABU_PLUS_PW = config.get('kabu_plus', 'KABU_PLUS_PW')
DOWNLOAD_SAVE_DIR = BASE_DIR + "../data/japan_all_financial_results/"

def retrieve_csv_file(url):
    res = requests.get(url, auth=(KABU_PLUS_ID, KABU_PLUS_PW))
    if res.status_code == 404:
        log.info('ステータスコードが404')
        slack.notify(text="決算・財務・業績データ 取得処理：正常終了（取得不可）")
        sys.exit(0)
    elif res.status_code != 200:
        log.error('ステータスコードが200 or 404以外')
        slack.notify(text="決算・財務・業績データ 取得処理：異常終了")
        sys.exit(1)
    data = res.content.decode('shift-jis')
    return data

def to_csv(data, file=None):
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
    slack_log_url = config.get('slack', 'slack_log_url')
    slack = slackweb.Slack(url=slack_log_url)

    log.info('決算・財務・業績データ取得処理 : 開始')
    dt = datetime.date.today().strftime("%Y%m%d")
    file_name = 'japan-all-stock-financial-results_' + dt + '.csv'
    URL = 'https://csvex.com/kabu.plus/csv/japan-all-stock-financial-results/monthly/' + file_name
    log.info('CSVファイル取得処理（決算・財務・業績データ）ファイル名:' + file_name)
    data = retrieve_csv_file(URL)
    to_csv(data, DOWNLOAD_SAVE_DIR + file_name)
    log.info('決算・財務・業績データ取得処理 : 終了')
    slack.notify(text="決算・財務・業績データ取得処理：正常終了")
    sys.exit(0)