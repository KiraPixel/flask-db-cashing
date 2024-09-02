import json
import requests
import config

token = config.WIALON_TOKEN
api_url = config.WIALON_HOST


def get_wialon_sid():
    params = {
        'token': token
    }
    response = requests.get(api_url, params={
        'svc': 'token/login',
        'params': json.dumps(params)
    }, verify=False)

    if response.status_code == 200:
        result = response.json()
        if 'eid' in result:
            return result['eid']
        else:
            print(f"Error: {result}")
            return None
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None


def search_all_items():
    params = {
        'spec': {
            'itemsType': 'avl_unit',
            'propName': 'sys_name',
            'propValueMask': '*',
            'sortType': 'sys_name'
        },
        'force': 1,
        'flags': 1 | 256 | 1024,
        'from': 0,
        'to': 0,
    }
    response = requests.get(api_url, params={
        'svc': 'core/search_items',
        'params': json.dumps(params),
        'sid': get_wialon_sid()
    }, verify=False)

    if response.status_code == 200:
        final_response = response.json()
        final_response = final_response['items']
        return final_response
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None