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
TABLE = 'japan_all_stock_data'
CSV_FILE_DIR = path_config.get('csv_path', 'data_base') + path_config.get('csv_path', 'japan_all_stock_data')

args = sys.argv

if __name__ == '__main__':
    # モジュール名でロガーを生成する(メインモジュールは 名前が '__main__' になる)
    log = logging.getLogger(__name__)
    # Slack Incoming webhook設定
    slack_log_url = account_config.get('slack', 'slack_log_url')
    slack = slackweb.Slack(url=slack_log_url)

    log.info('投資指標データ テーブルインポート処理 : 開始')

    # 対象の日付を設定（引数でYYYYMMDD形式で日付を入れるとその日付のファイルを対象とする）
    if len(args) < 2:
        TODAY = datetime.date.today()
    else:
        TARGET_DAY = args[1]
        TODAY = datetime.datetime(int(TARGET_DAY[:4]), int(TARGET_DAY[4:6]), int(TARGET_DAY[-2:]))

    # Target File Name
    file_name_date_part = str(TODAY.year) + '{:0=2}'.format(TODAY.month) + '{:0=2}'.format(TODAY.day)
    file_name = 'japan-all-stock-data_' + file_name_date_part + '.csv'

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
                market_capitalization = row[4] if row[4] != '-' else 'null'
                outstanding_shares = row[5] if row[5] != '-' else 'null'
                dividend_yield = row[6] if row[6] != '-' else 'null'
                dividends_per_share = row[7] if row[7] != '-' else 'null'
                per = row[8] if row[8] != '-' else 'null'
                pbr = row[9] if row[9] != '-' else 'null'
                eps = row[10] if row[10] != '-' else 'null'
                bps = row[11] if row[11] != '-' else 'null'
                minimum_purchase_amount = row[12] if row[12] != '-' else 'null'
                share_unit = row[13] if row[13] != '-' else 'null'
                high_price_date = row[14] if row[14] != '-' else 'null'
                yearly_high_price = row[15] if row[15] != '-' else 'null'
                low_price_date = row[16] if row[16] != '-' else 'null'
                yearly_low_price = row[17] if row[17] != '-' else 'null'
                cursor.execute('''
                        INSERT INTO {DB}.{TABLE} (security_code, dt, company_name, stock_exchange_code, industry_type,
                            market_capitalization, outstanding_shares, dividend_yield, dividends_per_share,
                            per, pbr, eps, bps, minimum_purchase_amount, share_unit, high_price_date, yearly_high_price,
                            low_price_date, yearly_low_price)
                        VALUES({sc}, '{dt}', "{company_name}", {stock_exchange_code}, {industry_type}, {mc}, 
                            {out_shares}, {dividend_yield}, {dividends_per_share}, {per}, {pbr}, {eps}, {bps}, 
                            {minimum_purchase_amount}, {share_unit}, '{high_price_date}', {yearly_high_price}, 
                            '{low_price_date}', {yearly_low_price});
                '''.format(DB=DB_DATABASE, TABLE=TABLE, sc=security_code, dt=dt, company_name=company_name,
                           stock_exchange_code=stock_exchange_code, industry_type=industry_type, mc=market_capitalization,
                           out_shares=outstanding_shares, dividend_yield=dividend_yield, dividends_per_share=dividends_per_share,
                           per=per, pbr=pbr, eps=eps, bps=bps, minimum_purchase_amount=minimum_purchase_amount,
                           share_unit=share_unit, high_price_date=high_price_date, yearly_high_price=yearly_high_price,
                           low_price_date=low_price_date, yearly_low_price=yearly_low_price)
                )
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
            slack.notify(text="投資指標データ テーブルインポート\n" + result_word)
        except mysql.connector.Error as e:
            log.error(e)
            conn.close()

        log.info('投資指標データ テーブルインポート処理 : 終了')
        conn.close()
