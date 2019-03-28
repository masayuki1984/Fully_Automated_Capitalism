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
INPUT_TABLE_ALL_STOCK_PRICES = 'japan_all_stock_prices'
OUTPUT_TABLE = 'monthly_average_rate_of_return'

args = sys.argv

if __name__ == '__main__':
    # モジュール名でロガーを生成する(メインモジュールは 名前が '__main__' になる)
    log = logging.getLogger(__name__)
    # Slack Incoming webhook設定
    slack_log_url = account_config.get('slack', 'slack_log_url')
    slack = slackweb.Slack(url=slack_log_url)

    log.info('統計処理 月次平均収益率 算出 : 開始')

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

        # 「nikkei225_topix_stock_prices」テーブルから対象年月の期首日と期末日を取得する
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

        # 「japan_all_stock_prices」テーブルから各株式の期首日と期末日から月次平均収益率を算出する
        # 終値がNULLのものは出来高0のため。出来高が少ないものは投資対象とはならないためここでは単純に除外している。
        cursor.execute('''
                SELECT
                    a.security_code AS security_code,
                    a.company_name AS company_name,
                    '{month}' AS target_month,
                    a.closing_price AS start_closing_price,
                    b.closing_price AS end_closing_price,
                    ROUND((b.closing_price - a.closing_price) / a.closing_price, 4) AS monthly_average_rate_of_return
                FROM
                    {DB_NAME}.{TABLE_NAME} a
                INNER JOIN
                    {DB_NAME}.{TABLE_NAME} b
                ON
                    a.security_code = b.security_code
                AND
                    a.company_name = b.company_name
                WHERE
                    a.dt = '{start_date}'
                AND
                    b.dt = '{end_date}'
                AND
                    (a.closing_price IS NOT NULL AND b.closing_price IS NOT NULL)
        '''.format(
            month=TARGET_MONTH,
            DB_NAME=DB_DATABASE,
            TABLE_NAME=INPUT_TABLE_ALL_STOCK_PRICES,
            start_date=opening_day,
            end_date=end_day)
        )
        monthly_average_rate_of_return_result =  cursor.fetchall()
        print(monthly_average_rate_of_return_result)
    except mysql.connector.Error as e:
        log.error(e)
        conn.close()
        sys.exit(1)

    conn.close()


