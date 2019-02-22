#coding:utf-8

import sys
import os
import glob
import shutil
import datetime
from dateutil.relativedelta import relativedelta
import slackweb
import configparser


# constants
args = sys.argv
BASE_DIR = '/usr/local/script/compression_file/'
CONFIG_DIR = BASE_DIR + "../config"
config_path = CONFIG_DIR + '/account.ini'
config = configparser.ConfigParser()
config.read(config_path, 'UTF-8')
TARGET_DIR = BASE_DIR + "../../data/japan_all_stock_prices_new_format/"
FILE_NAME_PREFIX = 'japan-all-stock-prices-2_'

def move_glob(dst_path, pathname, recursive=True):
    for p in glob.glob(pathname, recursive=recursive):
        shutil.move(p, dst_path)


if __name__ == '__main__':
    # Slack Incoming webhook設定
    slack_log_url = config.get('slack', 'slack_log_url')
    slack = slackweb.Slack(url=slack_log_url)

    # 対象の年月を設定
    if len(args) < 2:
        TODAY = datetime.date.today()
        one_month_ago = TODAY - relativedelta(months=1)
        target_month = str(one_month_ago.year) + str(one_month_ago.month)
    else:
        target_month = args[1]

    target_file_list = list()
    try:
        # YYYYmm形式のディレクトリ作成
        target_month_dir = TARGET_DIR + target_month
        os.makedirs(target_month_dir, exist_ok=True)
        # 対象ファイルを作成したディレクトリに移動
        move_from = TARGET_DIR + FILE_NAME_PREFIX + target_month + '*.csv'
        move_to = target_month_dir
        move_glob(move_to, move_from)
        # 対象ファイルを格納したディレクトリをzip圧縮する
        archived_file = TARGET_DIR + FILE_NAME_PREFIX + target_month
        archive_format = 'zip'
        shutil.make_archive(archived_file, archive_format, TARGET_DIR[:-1], target_month)
        # 圧縮が成功したら圧縮前のディレクトリに格納されているファイルをディレクトリごと削除する
        if os.path.isfile(archived_file + '.zip'):
            shutil.rmtree(move_to)

    except shutil.Error as e:
        print(e)
        sys.exit(1)

    slack.notify(text="日本株全銘柄(新フォーマット) CSVファイル圧縮月次処理(" + target_month + ")：正常終了")
    sys.exit(0)