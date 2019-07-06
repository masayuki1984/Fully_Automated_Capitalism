#coding:utf-8

import conoha_config
import logging
import slackweb
import configparser
from functions import api_common
import requests
import sys


BASE_DIR = '/usr/local/script/'
account = BASE_DIR + 'config/account.ini'
account_config = configparser.ConfigParser()
account_config.read(account, 'UTF-8')

conoha = BASE_DIR + 'config/conoha.ini'
conoha_config = configparser.ConfigParser()
conoha_config.read(conoha, 'UTF-8')

# Constants
TENANT_ID = conoha_config.get('common', 'TENANT_ID')
USER_NAME = conoha_config.get('common', 'USER_NAME')
PASSWARD = conoha_config.get('common', 'PASSWARD')
ACCEPT_JSON = conoha_config.get('header', 'ACCEPT_JSON')
IDENTITY_ENDPOINT = conoha_config.get('base_endpoint', 'ENDPOINT_IDENTITY_SERVICE')
ACCOUNT_ENDPOINT = conoha_config.get('base_endpoint', 'ENDPOINT_ACCOUNT_SERVICE')
TOKEN_PATH = conoha_config.get('endpoint', 'TOKENS')
NOTIFICATIONS = conoha_config.get('endpoint', 'NOTIFICATIONS')


if __name__ == "__main__":
    # モジュール名でロガーを生成する(メインモジュールは 名前が '__main__' になる)
    log = logging.getLogger(__name__)
    # Slack Incoming webhook設定
    slack_log_url = account_config.get('slack', 'slack_log_url')
    slack = slackweb.Slack(url=slack_log_url)

    log.info('ConoHa告知一覧取得 : 開始')

    # APIトークン取得
    try:
        token = api_common.get_token(IDENTITY_ENDPOINT, TOKEN_PATH, TENANT_ID, USER_NAME, PASSWARD, ACCEPT_JSON)
    except requests.RequestException as e:
        log.error("APIトークン取得 失敗 : " + str(e))
        slack.notify(text="【ERROR】ConoHa 告知情報取得処理失敗しました。ログを確認してください。")
        sys.exit(1)

    # 告知一覧取得処理
    try:
        notice_list = api_common.get_notification_code_list(ACCOUNT_ENDPOINT, NOTIFICATIONS, TENANT_ID, ACCEPT_JSON, token)
    except requests.RequestException as e:
        log.error("告知一覧取得処理 失敗 : " + str(e))
        slack.notify(text="【ERROR】ConoHa 告知情報取得処理失敗しました。ログを確認してください。")
        sys.exit(1)

    title_list = list()
    # 各告知をslack通知
    for notice in notice_list:
        if notice['read_status'] == 'Read':
            continue
        title_list.append(notice['title'])
        # 告知ステータスを既読に変更
        try:
            notice_code = notice['notification_code']
            read_status = 'Read'
            chenge_status = api_common.set_read_status(
                ACCOUNT_ENDPOINT,
                NOTIFICATIONS,
                TENANT_ID,
                ACCEPT_JSON,
                token,
                notice_code,
                read_status
            )
            if chenge_status != read_status:
                log.warn("既読ステータス変更失敗 : " + notice['title'])
        except requests.RequestException as e:
            log.error("告知ステータス変更 失敗 : " + str(e))
            slack.notify(text="【ERROR】ConoHa 告知情報取得処理失敗しました。ログを確認してください。")
            sys.exit(1)

    slack.notify(text='\n'.join(title_list))
    log.info('ConoHa告知一覧取得 : 終了')
