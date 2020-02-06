import json

import requests


def get_token(username: str, password: str):
    """
    Get access token for Escavador.
    :param username: The user's username.
    :param password: The user's password.
    :return: dict with token info.
    """
    url = "https://api.escavador.com/api/v1/request-token"
    r = requests.post(url=url, data={
        'username': username,
        'password': password
    })
    return json.loads(r.text)


def get_process(token: str, state: str, number: str):
    """
    Get process.

    :param token: The Bearer Auth token.
    :return:
    """
    url = "https://api.escavador.com/api/v1/oab/{state}/{number}/processos".format(state=state, number=number)

    headers = {
        'Authorization': 'Bearer {access_token}'.format(access_token=token),
        'X-Requested-With': 'XMLHttpRequest'
    }
    r = requests.request('GET', url, headers=headers)
    return json.loads(r.text)


def search_person(token: str, name: str) -> dict:
    """
    Search for a person.

    :param token: Bearer token.
    :param name: Name of the person
    :return: dict with person data.
    """
    url = 'https://api.escavador.com/api/v1/busca'

    params = {
        'q': f'"{name}"',
        'qo': 'p'
    }

    headers = {
        'Authorization': 'Bearer {access_token}'.format(access_token=token),
        'X-Requested-With': 'XMLHttpRequest'
    }

    response = requests.request('GET', url, headers=headers, params=params)
    return json.loads(response.text, encoding='utf-8')
