#coding:utf-8

import statistics_config
import logging
import slackweb
import configparser
import mysql.connector
import sys
import datetime
from itertools import chain
import numpy as np
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
INPUT_TABLE_ALL_STOCK_PRICES = 'japan_all_stock_prices'
OUTPUT_TABLE = 'correlation_coefficient_with_nikkei'

args = sys.argv

if __name__ == '__main__':
    # モジュール名でロガーを生成する(メインモジュールは 名前が '__main__' になる)
    log = logging.getLogger(__name__)
    # Slack Incoming webhook設定
    slack_log_url = account_config.get('slack', 'slack_log_url')
    slack = slackweb.Slack(url=slack_log_url)

    log.info('統計処理 日経平均株価との相関係数 算出 : 開始')

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

        cursor.execute('''
                SELECT
                    day_before_ratio_percentage
                FROM
                    {DB_NAME}.{TABLE_NAME}
                WHERE
                    dt LIKE \'{target_year}-{target_month}-%\'
                AND
                    security_code = 1
                ORDER BY dt;
        '''.format(
            DB_NAME=DB_DATABASE,
            TABLE_NAME=INPUT_TABLE_NIKKEI225,
            target_year=TARGET_MONTH[:4],
            target_month=TARGET_MONTH[4:])
        )
        nikkei_225_day_before_ratio_list = list(chain.from_iterable(cursor.fetchall()))
        #nikkei_225_day_before_ratio_list = cursor.fetchall()
        print(nikkei_225_day_before_ratio_list)
        #opening_day = opening_and_end_day[0][0]
        #end_day = opening_and_end_day[0][1]

    except mysql.connector.Error as e:
        log.error(e)
        slack.notify(text="【ERROR】統計処理 日経平均株価との相関係数 算出：異常終了")
        conn.close()
        sys.exit(1)