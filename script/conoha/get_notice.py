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
BASE_ENDPOINT = conoha_config.get('base_endpoint', 'ENDPOINT_IDENTITY_SERVICE')
TOKEN_PATH = conoha_config.get('endpoint', 'TOKENS')


if __name__ == "__main__":
    # モジュール名でロガーを生成する(メインモジュールは 名前が '__main__' になる)
    log = logging.getLogger(__name__)
    # Slack Incoming webhook設定
    slack_log_url = account_config.get('slack', 'slack_log_url')
    slack = slackweb.Slack(url=slack_log_url)

    log.info('ConoHa告知一覧取得 : 開始')

    # APIトークン取得
    try:
        token = api_common.get_token(BASE_ENDPOINT, TOKEN_PATH, TENANT_ID, USER_NAME, PASSWARD, ACCEPT_JSON)
    except requests.RequestException as e:
        log.error(e)
        sys.exit(1)
    print(token.text)




