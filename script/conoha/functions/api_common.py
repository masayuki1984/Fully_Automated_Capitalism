#coding:utf-8

import requests
import json

# Constants
SLASH = "/"


def get_token(BASE_ENDPOINT, SUB_PATH, TENANT_ID, USER_NAME, PASSWARD, ACCEPT_JSON):
    url = BASE_ENDPOINT + SUB_PATH
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
        respons_json = json.loads(respons.text)
        return respons_json['access']['token']['id']
    else:
        return respons.raise_for_status()

def get_notification_code_list(BASE_ENDPOINT, SUB_PATH, TENANT_ID, ACCEPT_JSON, token):
    url = BASE_ENDPOINT + SLASH + TENANT_ID + SUB_PATH
    respons = requests.get(
        url,
        params={'limit': 20},
        headers={'Accept': ACCEPT_JSON, 'X-Auth-Token': token}
    )
    if respons.status_code == requests.codes.ok:
        respons_json = json.loads(respons.text)
        return respons_json['notifications']
    else:
        return respons.raise_for_status()

def set_read_status(BASE_ENDPOINT, SUB_PATH, TENANT_ID, ACCEPT_JSON, token, notice_code, read_status):
    url = BASE_ENDPOINT + SLASH + TENANT_ID + SUB_PATH + SLASH + str(notice_code)
    body = {
        "notification":{
            "read_status": read_status
        }
    }
    response = requests.put(
        url,
        json.dumps(body),
        headers={'Accept': ACCEPT_JSON, 'X-Auth-Token': token}
    )
    if response.status_code == requests.codes.ok:
        response_json = json.loads(response.text)
        return response_json['notification']['read_status']
    else:
        return response.raise_for_status()