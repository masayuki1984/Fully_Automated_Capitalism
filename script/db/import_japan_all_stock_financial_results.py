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
TABLE = 'japan_all_stock_financial_results'
CSV_FILE_DIR = path_config.get('csv_path', 'data_base') + path_config.get('csv_path', 'japan_all_stock_financial_results')

args = sys.argv

if __name__ == '__main__':
    # モジュール名でロガーを生成する(メインモジュールは 名前が '__main__' になる)
    log = logging.getLogger(__name__)
    # Slack Incoming webhook設定
    slack_log_url = account_config.get('slack', 'slack_log_url')
    slack = slackweb.Slack(url=slack_log_url)

    log.info('決算・財務・業績テーブルインポート処理 : 開始')

    # 対象の日付を設定（引数でYYYYMMDD形式で日付を入れるとその日付のファイルを対象とする）
    if len(args) < 2:
        TODAY = datetime.date.today()
    else:
        TARGET_DAY = args[1]
        TODAY = datetime.datetime(int(TARGET_DAY[:4]), int(TARGET_DAY[4:6]), int(TARGET_DAY[-2:]))

    # Target File Name
    file_name_date_part = str(TODAY.year) + '{:0=2}'.format(TODAY.month) + '{:0=2}'.format(TODAY.day)
    file_name = 'japan-all-stock-financial-results_' + file_name_date_part + '.csv'

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
                target_month = file_name_date_part[:6]
                company_name = row[1] if row[1] != '-' else 'null'
                settlement_period = row[2].replace('/', '') if row[2] != '-' else 'null'
                main_earnings_announcement_day = row[3].replace('/', '-') if row[3] != '-' else 'null'
                sales_amount = row[4] if row[4] != '-' else 'null'
                operating_income = row[5] if row[5] != '-' else 'null'
                ordinary_profit = row[6] if row[6] != '-' else 'null'
                net_income = row[7] if row[7] != '-' else 'null'
                total_asset = row[8] if row[8] != '-' else 'null'
                net_worth = row[9] if row[9] != '-' else 'null'
                capital = row[10] if row[10] != '-' else 'null'
                interest_bearing_debt = row[11] if row[11] != '-' else 0
                capital_adequacy_ratio = row[12] if row[12] != '-' else 'null'
                ROE = row[13] if row[13] != '-' else 'null'
                ROA = row[14] if row[14] != '-' else'null'
                issued_shares = row[15] if row[15] != '-' else 'null'
                cursor.execute('''
                        INSERT INTO {DB}.{TABLE} (security_code, target_month, company_name, settlement_period,
                            main_earnings_announcement_day, sales_amount, operating_income, ordinary_profit,
                            net_income, total_asset, net_worth, capital, interest_bearing_debt, capital_adequacy_ratio,
                            ROE, ROA, issued_shares)
                        VALUES({sc}, '{target_month}', "{company_name}", '{settlement_period}', '{main_earnings_announcement_day}',
                            {sales_amount}, {operating_income}, {ordinary_profit}, {net_income}, {total_asset}, {net_worth},
                            {capital}, {interest_bearing_debt}, {capital_adequacy_ratio}, {ROE}, {ROA}, {issued_shares});
                               '''.format(DB=DB_DATABASE, TABLE=TABLE, sc=security_code, target_month=target_month,
                                          company_name=company_name, settlement_period=settlement_period,
                                          main_earnings_announcement_day=main_earnings_announcement_day, sales_amount=sales_amount,
                                          operating_income=operating_income, ordinary_profit=ordinary_profit,
                                          net_income=net_income, total_asset=total_asset, net_worth=net_worth, capital=capital,
                                          interest_bearing_debt=interest_bearing_debt, capital_adequacy_ratio=capital_adequacy_ratio,
                                          ROE=ROE, ROA=ROA, issued_shares=issued_shares))
        except mysql.connector.Error as e:
            log.error(e)
            conn.close()

        conn.commit()
        conn.close()

        # テーブルINSERT件数確認
        try:
            conn = mysql.connector.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, database=DB_DATABASE)
            cursor = conn.cursor()
            count_query = """
                          SELECT target_month, COUNT(*) FROM {DB}.{TABLE} WHERE target_month = '{target_month}' GROUP BY target_month
                          """.format(DB=DB_DATABASE, TABLE=TABLE, target_month=target_month)
            cursor.execute(count_query)
            result = cursor.fetchone()
            result_word = "処理日時：%s 取得件数：%s" % (result[0], result[1])
            slack.notify(text="決算・財務・業績テーブルインポート\n" + result_word)
        except mysql.connector.Error as e:
            log.error(e)
            conn.close()

        log.info('決算・財務・業績テーブルインポート処理 : 終了')
        conn.close()
