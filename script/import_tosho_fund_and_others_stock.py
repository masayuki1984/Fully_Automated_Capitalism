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
TABLE = 'tosho_fund_and_others_stock_prices'
CSV_FILE_DIR = BASE_DIR + "/../data/tosho_fund_and_others_stock_prices/"

args = sys.argv

def get_stock_exchange_code(stock_exchange_name):
    if stock_exchange_name == '東証一部':
        return 1
    elif stock_exchange_name == '東証':
        return 13
    else:
        return 'null'


def get_industry_type(industry_name):
    if industry_name == '優先株':
        return 37
    elif industry_name == 'カントリーファンド':
        return 38
    elif industry_name == 'インフラファンド':
        return 39
    elif industry_name == 'ベンチャーファンド':
        return 40
    else:
        return 'null'


if __name__ == '__main__':
    # モジュール名でロガーを生成する(メインモジュールは 名前が '__main__' になる)
    log = logging.getLogger(__name__)
    # Slack Incoming webhook設定
    slack_log_url = config.get('slack', 'slack_log_url')
    slack = slackweb.Slack(url=slack_log_url)

    log.info('Fund and Othersテーブルインポート処理 : 開始')
    # 対象の日付を設定（引数でYYYYMMDD形式で日付を入れるとその日付のファイルを対象とする）
    if len(args) < 2:
        TODAY = datetime.date.today()
    else:
        TARGET_DAY = args[1]
        TODAY = datetime.datetime(int(TARGET_DAY[:4]), int(TARGET_DAY[4:6]), int(TARGET_DAY[-2:]))

    # Target File Name
    file_name_date_part = str(TODAY.year) + '{:0=2}'.format(TODAY.month) + '{:0=2}'.format(TODAY.day)
    file_name = 'tosho-fund-and-others-stock-prices_' + file_name_date_part + '.csv'

    log.info('テーブル名:%s 対象ファイル:%s' % (TABLE, file_name))

    with open (CSV_FILE_DIR + file_name) as csvfile:
        reader = csv.reader(csvfile)

        # headerをスキップする
        next(reader, None)

        # MariaDB connect
        try:
            conn = mysql.connector.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, database=DB_DATABASE)
            cursor = conn.cursor()

            for row in reader:
                security_code = row[0] if row[0] != '-' else 'null'
                dt = file_name_date_part
                fund_name = row[1] if row[1] != '-' else 'null'
                stock_exchange_code = get_stock_exchange_code(row[2])
                industry_type = get_industry_type(row[3])
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
                price_range_lower_limit = row[15] if row[15] != '-' else 'null'
                price_range_upper_limit = row[16] if row[16] != '-' else 'null'
                query = '''INSERT INTO %s.%s (security_code, dt, fund_name, 
                                stock_exchange_code, industry_type, opening_price, closing_price, high_price, low_price, 
                                day_before_ratio, day_before_ratio_percentage, last_day_closing_price, volume, 
                                trading_value, market_capitalization, price_range_lower_limit, price_range_upper_limit)
                                VALUES(%s, '%s', '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                ''' % (
                    DB_DATABASE, TABLE, security_code, dt, fund_name, stock_exchange_code, industry_type,
                    opening_price, closing_price, high_price, low_price, day_before_ratio, day_before_ratio_percentage,
                    last_day_closing_price, volume, trading_value, market_capitalization, price_range_lower_limit,
                    price_range_upper_limit)
                cursor.execute(query)
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
            result_word = "処理日時：%s 取得件数：%s" %(result[0], result[1])
            slack.notify(text="Fund and Othersテーブルインポート\n" + result_word)
        except mysql.connector.Error as e:
            log.error(e)
            conn.close()

        log.info('Fund and Othersテーブルインポート処理 : 終了')
        conn.close()
