#coding:utf-8

import db_config
from functions import exchange_code
import logging
import datetime
import csv
import mysql.connector
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

# Constants
DB_USER = account_config.get('db', 'DB_USER')
DB_PASSWORD = account_config.get('db', 'DB_PASSWORD')
DB_HOST = account_config.get('db', 'DB_HOST')
DB_DATABASE = account_config.get('db', 'DB_DATABASE')
TABLE = 'japan_all_stock_prices'
CSV_FILE_DIR = path_config.get('csv_path', 'data_base') + path_config.get('csv_path', 'japan_all_stock_prices')

args = sys.argv

if __name__ == '__main__':
    # モジュール名でロガーを生成する(メインモジュールは 名前が '__main__' になる)
    log = logging.getLogger(__name__)
    # Slack Incoming webhook設定
    slack_log_url = account_config.get('slack', 'slack_log_url')
    slack = slackweb.Slack(url=slack_log_url)

    log.info('日本株全銘柄テーブルインポート処理 : 開始')

    # 対象の日付を設定（引数でYYYYMMDD形式で日付を入れるとその日付のファイルを対象とする）
    if len(args) < 2:
        TODAY = datetime.date.today()
    else:
        TARGET_DAY = args[1]
        TODAY = datetime.datetime(int(TARGET_DAY[:4]), int(TARGET_DAY[4:6]), int(TARGET_DAY[-2:]))

    # Target File Name
    file_name_date_part = str(TODAY.year) + '{:0=2}'.format(TODAY.month) + '{:0=2}'.format(TODAY.day)
    file_name = 'japan-all-stock-prices_' + file_name_date_part + '.csv'

    log.info('テーブル名:%s 対象ファイル:%s' % (TABLE, file_name))

    with open (CSV_FILE_DIR + file_name) as csvfile:
        reader = csv.reader(csvfile)
        # headerと日経225、TOPIXをスキップする
        for i in range(3):
            next(reader, None)

        # MariaDB connect
        try:
            conn = mysql.connector.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, database=DB_DATABASE)
            cursor = conn.cursor()

            for row in reader:
                security_code = row[0] if row[0] != '-' else 'null'
                dt = file_name_date_part
                company_name = row[1] if row[1] != '-' else 'null'
                stock_exchange_code = exchange_code.get_stock_exchange_code(row[2])
                industry_type = exchange_code.get_industry_type(row[3])
                opening_price = row[9] if row[9] != '-' else 'null'
                closing_price = row[5] if row[5] != '-' else 'null'
                high_price = row[10] if row[10] != '-' else 'null'
                low_price = row[11] if row[11] != '-' else 'null'
                day_before_ratio = row[6] if row[6] != '-' else 'null'
                day_before_ratio_percentage = row[7] if row[7] != '-' else 'null'
                last_day_closing_price = row[8] if row[8] != '-' else 'null'
                volume = row[12] if row[12] != '-' else 'null'
                trading_value = row[13] if row[13] != '-' else 'null'
                market_capitalization = row[14] if row[14] != '-' else 'null'
                price_range_lower_limit = row[15] if row[15] != '-' else'null'
                price_range_upper_limit = row[16] if row[16] != '-' else 'null'
                cursor.execute('''INSERT INTO %s.%s (security_code, dt, company_name,
                               stock_exchange_code, industry_type, opening_price, closing_price, high_price, low_price,
                               day_before_ratio, day_before_ratio_percentage, last_day_closing_price, volume,
                               trading_value, market_capitalization, price_range_lower_limit, price_range_upper_limit)
                               VALUES(%s, '%s', "%s", %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                               ''' % (DB_DATABASE, TABLE, security_code, dt, company_name, stock_exchange_code, industry_type, opening_price,
                                      closing_price, high_price, low_price, day_before_ratio, day_before_ratio_percentage,
                                      last_day_closing_price, volume, trading_value, market_capitalization,
                                      price_range_lower_limit, price_range_upper_limit))
        except mysql.connector.Error as e:
            log.error(e)
            conn.close()

        conn.commit()
        conn.close()

        # テーブルINSERT件数確認
        try:
            conn = mysql.connector.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, database=DB_DATABASE)
            cursor = conn.cursor()
            count_query = """SELECT dt, COUNT(*) FROM %s.%s WHERE dt = '%s' GROUP BY dt""" % (DB_DATABASE, TABLE, TODAY)
            cursor.execute(count_query)
            result = cursor.fetchone()
            result_word = "処理日時：%s 取得件数：%s" % (result[0], result[1])
            slack.notify(text="日本株全銘柄テーブルインポート\n" + result_word)
        except mysql.connector.Error as e:
            log.error(e)
            conn.close()

        log.info('日本株全銘柄テーブルインポート処理 : 終了')
        conn.close()
