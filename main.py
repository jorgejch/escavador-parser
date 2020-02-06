import json
import logging
import os
from json import JSONDecodeError

from google.cloud import error_reporting
from google.cloud import firestore, pubsub_v1 as pubsub
from google.cloud import storage
from google.cloud.firestore_v1 import CollectionReference, DocumentReference
from json2html import json2html

import api

_token = None
_logger = None
_error_reporting_client = None


def _get_logger():
    global _logger
    if _logger is not None:
        return _logger

    loglevel = os.getenv('LOG_LEVEL')

    if loglevel is None:
        raise Exception("Missing environment variable 'LOG_LEVEL'.")

    logging.basicConfig(level=logging._nameToLevel[loglevel])
    _logger = logging.getLogger()
    return _get_logger()


def _get_credentials():
    """
    Get/set sensitive info.

    >>> import main
    >>> main._get_credentials() # doctest: +ELLIPSIS
    {'username': ..., 'password': ...}

    :return: dict with sensitive function data.
    """

    st_client = storage.Client()
    bucket = st_client.get_bucket(os.getenv('VARS_BUCKET'))
    blob = bucket.get_blob(os.getenv("VARS_BLOB"))
    return json.loads(blob.download_as_string())


def _get_error_reporting_client() -> error_reporting.Client:
    """
    Get/set GC error reporting client.
    :return: error reporting client.
    """
    global _error_reporting_client
    if _error_reporting_client is not None:
        return _error_reporting_client
    _error_reporting_client = error_reporting.Client()
    return _error_reporting_client


def _get_token():
    """
    Get/set api auth token.

    >>> import main
    >>> main._get_token() # doctest: +ELLIPSIS
    '...'

    :return: the auth token.
    """
    global _token
    if _token is not None:
        return _token

    vars_dict = _get_credentials()
    r_dict = api.get_token(username=vars_dict['username'], password=vars_dict['password'])
    _token = r_dict['access_token']
    return _get_token()


def _notify_email(subject: str, body: str, pub_sub_client: pubsub.PublisherClient):
    """
    Trigger email notification function with payload.

    >>> import main
    >>> from google.cloud import pubsub_v1 as pubsub
    >>> existing_profile = _get_searched_person_record({'items': [{
    ...        'id': 43213656,
    ...        'nome': 'Jorge Jafet da Cruz Haddad',
    ...        'resumo': 'blablabla',
    ...        'atuacao_formacao': None,
    ...        'nome_em_citacoes': None,
    ...        'quantidade_processos': 1,
    ...        'tem_patente': 0,
    ...        'tem_processo': 1,
    ...        'updated_at': '2017-09-08 01:26:56',
    ...        'link': 'https://www.escavador.com/sobre/321736294/jorge-jafet-da-cruz-haddad'
    ...     }]}, monitor_processes=True)
    >>> current_profile = _get_searched_person_record({'items': [{
    ...        'id': 43213656,
    ...        'nome': 'Jorge Jafet da Cruz Haddad',
    ...        'resumo': 'blubsblubs',
    ...        'atuacao_formacao': None,
    ...        'nome_em_citacoes': None,
    ...        'quantidade_processos': 2,
    ...        'tem_patente': 0,
    ...        'tem_processo': 2,
    ...        'updated_at': '2019-04-02 05:26:56',
    ...        'link': 'https://www.escavador.com/sobre/321736294/jorge-jafet-da-cruz-haddad'
    ...     }]}, monitor_processes=True)
    >>> body = main._get_person_search_event_email_body(
    ...     existing_profile=existing_profile,
    ...     new_profile=current_profile
    ... )
    >>> subject = "Escavador person search results have differed."
    >>> ps_client = pubsub.PublisherClient()
    >>> main._notify_email(subject, body, ps_client)
    0

    """
    to_emails = os.getenv('TO_EMAILS')
    pub_sub_topic = os.getenv('EMAIL_NOTIFY_PUBSUB_TOPIC')

    try:
        if to_emails is None:
            raise Exception("Missing TO_EMAILS env var.")

        data = json.dumps({
            'email': {
                'message': body,
                'to_emails': to_emails,
                'subject': f"[EscavadorParser]{subject}"
            }
        })
        if pub_sub_topic is None:
            raise Exception("Missing EMAIL_NOTIFY_PUBSUB_TOPIC env var.")

        pub_sub_client.publish(pub_sub_topic, bytes(data, 'utf-8'))
    except Exception as e:
        _get_logger().warning("Unable to send email notification with subject '{}' due to: {}".format(subject, e))
        _get_error_reporting_client().report_exception()
        return 1
    return 0


def _get_person_search_event_email_body(existing_profile: dict = None, new_profile: dict = None) -> str:
    """
    Get email body for an email notifying that a person's search resolution.

    :param existing_profile: The existing person search record.
    :param new_profile:  The newly obtained person search record.
    :return: html string of the email body.
    """
    body = None

    if existing_profile and new_profile:
        body = f"""
                <h2> Escavador person search result record has differed from existing version. </h2>
                    <h3> New profile: </h3>
                        <div>{json2html.convert(json=existing_profile, table_attributes='border="1"')}</div>
                    <h3> Existing profile record: </h3>
                        <div>{json2html.convert(json=new_profile, table_attributes='border="1"')}</div>
                """
    elif not existing_profile:
        body = f"""
                <h2> New escavador person search result record. </h2>
                    <h3> New profile: </h3>
                        <div>{json2html.convert(json=new_profile, table_attributes='border="1"')}</div>
                """
    return body


def _get_searched_person_record(raw_search_person_result: dict, monitor_processes=False) -> dict:
    """
    Get a dict with the extracted person info from a person search result.

    :param raw_search_person_result: The json loaded result of the call to api/v1/busca for a person.
    :param monitor_processes: Whether any process associated to the person should be monitored.
    :return: dict with the person's returned data plus monitor_processes property.
    """
    person_dict = raw_search_person_result['items'][0]
    person_dict['monitor_processes'] = monitor_processes
    return person_dict


def _handle_person_new_search_result(
        person_new_search_record: dict,
        person_name: str,
        ppl_col_ref: firestore.CollectionReference
) -> dict or None:
    """
    Compare person new search result record to possible existing record. If no prior search record is found for the
    person, persist the new record and return it. If there is a prior record, but it differs from the new, substitute
    the old by the new and return the old. If new and existing search result records are the same, just return None.

    :param person_new_search_record: Record obtained in the latest search for the person.
    :param person_name: The searched person's name.
    :param ppl_col_ref: A CollectionReference object to store the person's record in.
    :return: None in case existing records don't differ; new record in case they differ; existing record in case there
    is no persisted search record for the person.
    """


def search_people_on_escavador(event, context) -> int:
    """
    Search for people in escavador and notify if any changes are detected in their profile compared to the last search.
    Related env vars:
        - PEOPLE: JSON string containing array of people objects to search for.
            EX: [{"name":"\"Jorge Jafet da Cruz Haddad\"", "monitor_processes": 1}]

    >>> import main
    >>> main.search_people_on_escavador({}, {})
    0

    :return: 0|1 success/failure.
    """
    try:
        people_list = json.loads(os.getenv("PEOPLE"))
    except JSONDecodeError as e:
        _get_logger().warning("No people to search for or bad people to search json string. Error: {}".format(e))
        _get_error_reporting_client().report_exception()
        return 1

    # Firestore db client.
    db = firestore.Client()
    escavador_col_ref: CollectionReference = db.collection("escavador")
    people_doc_ref: DocumentReference = escavador_col_ref.document("people")
    records_col_ref: CollectionReference = people_doc_ref.collection("search_records")
    pub_sub_client = pubsub.PublisherClient()

    for person_dict in people_list:
        name = person_dict['name']
        monitor_processes = person_dict['monitor_processes']

        try:
            _get_logger().info(f"Searching for '{name}'.")
            person_new_search_result = api.search_person(_get_token(), name=name)
            _get_logger().debug(f"Result of the search for {name}: {person_new_search_result}")

            if len(person_new_search_result['items']) > 0:
                person_new_search_record = _get_searched_person_record(person_new_search_result, monitor_processes)
                _get_logger().debug(
                    f"Search record found for '{name}':" + '\n' + json.dumps(
                        person_new_search_record,
                        indent=4
                    )
                )
                person_doc_ref = records_col_ref.document(name)
                person_doc = person_doc_ref.get()

                if not person_doc.exists:
                    _get_logger().info(f"Search record for person '{name}' found. Updating.")
                    person_doc_ref.set(person_new_search_record)
                    email_subject = f"New Escavador person search record found for {name}."
                    email_body = _get_person_search_event_email_body(new_profile=person_new_search_record)
                    _notify_email(email_subject, email_body, pub_sub_client=pub_sub_client)
                    continue

                person_existing_search_record: dict = person_doc.to_dict()

                if person_existing_search_record != person_new_search_record:
                    _get_logger().info(f"Person {name}'s search record has changed. Updating.")
                    person_doc_ref.set(person_new_search_record)
                    email_subject = f"Escavador person search record for '{name}' updated."
                    email_body = _get_person_search_event_email_body(
                        existing_profile=person_existing_search_record,
                        new_profile=person_new_search_record
                    )
                    _notify_email(email_subject, email_body, pub_sub_client=pub_sub_client)
                    continue
            else:
                _get_logger().info(f"Escavador did not return a record for '{name}'.")
        except Exception as e:
            _get_logger().warning(
                f"Unable to search for person '{name}' or otherwise process it's information due to: {e}."
            )
            _get_error_reporting_client().report_exception()
            continue

    return 0
