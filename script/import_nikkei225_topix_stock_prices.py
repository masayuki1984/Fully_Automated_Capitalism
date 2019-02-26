#coding:utf-8

import logging_config
import logging
import datetime
import csv
import mysql.connector
import sys
import slackweb
import configparser


BASE_DIR = '/usr/local/script/'
config_path = BASE_DIR + 'config/account.ini'
config = configparser.ConfigParser()
config.read(config_path, 'UTF-8')

# Constants
DB_USER = config.get('db', 'DB_USER')
DB_PASSWORD = config.get('db', 'DB_PASSWORD')
DB_HOST = config.get('db', 'DB_HOST')
DB_DATABASE = config.get('db', 'DB_DATABASE')
TABLE = 'nikkei225_topix_stock_prices'
CSV_FILE_DIR = BASE_DIR + "/../data/japan_all_stock_prices/"

args = sys.argv


if __name__ == '__main__':
    # モジュール名でロガーを生成する(メインモジュールは 名前が '__main__' になる)
    log = logging.getLogger(__name__)
    # Slack Incoming webhook設定
    slack_log_url = config.get('slack', 'slack_log_url')
    slack = slackweb.Slack(url=slack_log_url)

    log.info('日経225&TOPIXテーブルインポート処理 : 開始')
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

        # MariaDB connect
        try:
            conn = mysql.connector.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, database=DB_DATABASE)
            cursor = conn.cursor()

            for row in reader:
                # 日経225とTOPIX以外は対象外
                if row[0] not in ['0001', '0002']:
                    continue

                security_code = row[0] if row[0] != '-' else 'null'
                dt = file_name_date_part
                name = row[1] if row[1] != '-' else 'null'
                opening_price = row[9] if row[9] != '-' else 'null'
                closing_price = row[5] if row[5] != '-' else 'null'
                high_price = row[10] if row[10] != '-' else 'null'
                low_price = row[11] if row[11] != '-' else 'null'
                day_before_ratio = row[6] if row[6] != '-' else 'null'
                day_before_ratio_percentage = row[7] if row[7] != '-' else 'null'
                last_day_closing_price = row[8] if row[8] != '-' else 'null'
                cursor.execute('''INSERT INTO %s.%s (security_code, dt, name,
                                                       opening_price, closing_price, high_price, low_price,
                                                       day_before_ratio, day_before_ratio_percentage, last_day_closing_price)
                                                       VALUES(%s, '%s', '%s', %s, %s, %s, %s, %s, %s, %s);
                                                       ''' % (
                    DB_DATABASE, TABLE, security_code, dt, name, opening_price, closing_price,
                    high_price, low_price, day_before_ratio, day_before_ratio_percentage,
                    last_day_closing_price))
        except mysql.connector.Error as e:
            print(e)
            conn.close()

        conn.commit()
        conn.close()

        log.info('日経225&TOPIXテーブルインポート処理 : 終了')