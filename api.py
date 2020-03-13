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
    r = requests.post(
        url=url,
        data={
            'username': username,
            'password': password
        })
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
        'Authorization': f'Bearer {token}',
        'X-Requested-With': 'XMLHttpRequest'
    }

    response = requests.request('GET', url, headers=headers, params=params)
    return json.loads(response.text, encoding='utf-8')


def get_person_profile(token: str, person_id: int):
    """
    Get a person's profile.

    api/v1/pessoas/{pessoaId}

    :param token: Bearer token.
    :param person_id: Id of the person
    :return: dict with person data.
    """
    url = f'https://api.escavador.com/api/v1/pessoas/{person_id}'

    headers = {
        'Authorization': f'Bearer {token}',
        'X-Requested-With': 'XMLHttpRequest'
    }

    response = requests.request('GET', url, headers=headers)
    return json.loads(response.text)


def get_process_by_person(token: str, person_id: int):
    """
    Get a person's processes.

    :param token: Bearer token.
    :param person_id: Id of the person
    :return: dict with person's process data.
    """
    url = f'https://api.escavador.com/api/v1/pessoas/{person_id}/processos/'

    headers = {
        'Authorization': f'Bearer {token}',
        'X-Requested-With': 'XMLHttpRequest'
    }

    response = requests.request('GET', url, headers=headers)
    return json.loads(response.text)
