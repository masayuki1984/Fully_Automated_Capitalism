#coding:utf-8

import statistics_config
import logging
import slackweb
import configparser
import mysql.connector
import sys
import datetime
from dateutil.relativedelta import relativedelta


BASE_DIR = '/usr/local/script/'
account = BASE_DIR + 'config/account.ini'
account_config = configparser.ConfigParser()
account_config.read(account, 'UTF-8')

# Constants
DB_USER = account_config.get('db', 'DB_USER')
DB_PASSWORD = account_config.get('db', 'DB_PASSWORD')
DB_HOST = account_config.get('db', 'DB_HOST')
DB_DATABASE = account_config.get('db', 'DB_DATABASE')
INPUT_TABLE_NIKKEI225 = 'nikkei225_topix_stock_prices'
INPUT_TABLE_ALL_STOCK_PRICES = 'japan_all_stock_data'
OUTPUT_TABLE = 'monthly_average_per_industry'

args = sys.argv

if __name__ == '__main__':
    # モジュール名でロガーを生成する(メインモジュールは 名前が '__main__' になる)
    log = logging.getLogger(__name__)
    # Slack Incoming webhook設定
    slack_log_url = account_config.get('slack', 'slack_log_url')
    slack = slackweb.Slack(url=slack_log_url)

    log.info('統計処理 全市場平均株価収益率(PER) 算出 : 開始')

    # 対象の日付を設定（引数でYYYYMM形式で年月を入れるとその年月の統計処理を対象とする）
    if len(args) < 2:
        # 引数を指定しない場合は前月を対象とする
        TODAY = datetime.date.today()
        temp_target_month = TODAY - relativedelta(months=1)
        TARGET_MONTH = datetime.datetime.strftime(temp_target_month, "%Y%m")
    else:
        TARGET_MONTH = args[1]


    # MariaDB connect
    try:
        conn = mysql.connector.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, database=DB_DATABASE)
        cursor = conn.cursor()

        # 「japan_all_stock_data」テーブルから対象年月の業種別平均PERを算出する
        cursor.execute('''
                SELECT MIN(dt), MAX(dt)
                FROM {DB_NAME}.{TABLE_NAME}
                WHERE dt LIKE \'{start_date}-{end_date}-%\'
            '''.format(
            DB_NAME=DB_DATABASE,
            TABLE_NAME=INPUT_TABLE_NIKKEI225,
            start_date=TARGET_MONTH[:4],
            end_date=TARGET_MONTH[4:])
        )
        opening_and_end_day = cursor.fetchall()
        opening_day = opening_and_end_day[0][0]
        end_day = opening_and_end_day[0][1]