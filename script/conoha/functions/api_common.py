#coding:utf-8

import requests
import json


def get_token(BASE_ENDPOINT, TOKEN_PATH, TENANT_ID, USER_NAME, PASSWARD, ACCEPT_JSON):
    url = BASE_ENDPOINT + TOKEN_PATH
    body = {
        "auth":{
            "passwordCredentials":{
                "username": USER_NAME,
                "password": PASSWARD
            },
        "tenantId": TENANT_ID
        }
    }

    respons = requests.post(
        url,
        json.dumps(body),
        headers={'Accept': ACCEPT_JSON}
    )

    # HTTPステータスコードが「200:OK」
    if respons.status_code == requests.codes.ok:
        return respons
    else:
        return respons.raise_for_status()

