#coding:utf-8

import statistics_config
import logging
import slackweb
import configparser
import mysql.connector
import sys
import datetime
from itertools import chain
import pandas as pd
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

        # 日経225の前日比(終値)のパーセンテージを取得
        cursor.execute('''
                SELECT
                    dt,
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
        nikkei_225_day_before_ratio_list = list(cursor.fetchall())

        # 各タプルから日付部分を抽出しリストに格納、同時に値部分のみ抽出してSeriesにする
        nikkei_225_days = [days_data[0] for days_data in nikkei_225_day_before_ratio_list]
        tmp_nikkei_value = [days_data[1] for days_data in nikkei_225_day_before_ratio_list]
        nikkei_value = pd.Series(tmp_nikkei_value)

        # 各株式の前日比(終値)のパーセンテージを取得
        cursor.execute('''
                SELECT
                    security_code
                FROM
                    {DB_NAME}.{TABLE_NAME}
                WHERE
                    dt LIKE \'{target_year}-{target_month}-%\'
                GROUP BY
                    security_code;
        '''.format(
            DB_NAME=DB_DATABASE,
            TABLE_NAME=INPUT_TABLE_ALL_STOCK_PRICES,
            target_year=TARGET_MONTH[:4],
            target_month=TARGET_MONTH[4:])
        )
        target_sc = list(chain.from_iterable(cursor.fetchall()))

        target_sc_dict = dict()
        for sc in target_sc:
            cursor.execute('''
                    SELECT
                        dt,
                        day_before_ratio_percentage
                    FROM
                        {DB_NAME}.{TABLE_NAME}
                    WHERE
                        dt LIKE \'{target_year}-{target_month}-%\'
                    AND
                        security_code = {SC}
                    ORDER BY dt;
            '''.format(
                DB_NAME=DB_DATABASE,
                TABLE_NAME=INPUT_TABLE_ALL_STOCK_PRICES,
                target_year=TARGET_MONTH[:4],
                target_month=TARGET_MONTH[4:],
                SC=sc)
            )
            target_sc_dict[sc] = cursor.fetchall()

        # INSERT用のクエリ文
        insert_query = '''INSERT INTO {DB_NAME}.{TABLE_NAME} 
                (security_code, target_month, corr_coef_day_before_ratio_percentage)
                VALUES '''.format(
                DB_NAME=DB_DATABASE,
                TABLE_NAME=OUTPUT_TABLE
        )

        for sc_code in target_sc_dict.keys():
            sc_days = [sc[0] for sc in target_sc_dict[sc_code]]
            if nikkei_225_days == sc_days:
                tmp_sc_value = [sc[1] for sc in target_sc_dict[sc_code]]
                sc_value = pd.Series(tmp_sc_value)

                corr_coef = nikkei_value.corr(sc_value)
                # 相関係数を計算した結果NaNだった場合にはスキップする
                if pd.isnull(corr_coef):
                    continue
                insert_block = "({SC}, '{target_month}', {corr_coef_day_before_ratio_percentage}),".format(
                    SC=sc_code,
                    target_month=TARGET_MONTH,
                    corr_coef_day_before_ratio_percentage=corr_coef
                )
                insert_query += insert_block
            else:
                log.warn("日付欠損あり：" + str(sc_code))
        insert_query = insert_query[:-1] + ';'
        cursor.execute(insert_query)
        conn.commit()
    except mysql.connector.Error as e:
        log.error(e)
        slack.notify(text="【ERROR】統計処理 日経平均株価との相関係数 算出：異常終了")
        conn.close()
        sys.exit(1)

    log.info('統計処理 日経平均株価との相関係数 算出 : 終了')
    slack.notify(text="日経平均株価との相関係数 計算処理正常終了")
    conn.close()