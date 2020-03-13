import json
import logging
import os
from json import JSONDecodeError

from google.cloud import error_reporting
from google.cloud import firestore, pubsub_v1 as pubsub
from google.cloud import storage
from google.cloud.firestore_v1 import CollectionReference, DocumentReference, DocumentSnapshot
from json2html import json2html

import api

_ESCAVADOR_COL_KEY = "escavador"
_PEOPLE_DOC_KEY = "people"
_PROFILES_COL_KEY = "profiles"
_MONITOR_PERSON_PROCESSES_FIELD_KEY = 'monitor_processes'
_PERSON_NAME_FIELD_KEY = 'name'
_PERSON_ID_FIELD_KEY = 'id'
_TOKEN = None
_LOGGER = None
_ERROR_REPORTING_CLIENT = None


def _get_logger():
    global _LOGGER
    if _LOGGER is not None:
        return _LOGGER

    loglevel = os.getenv('LOG_LEVEL') or "INFO"

    if loglevel is None:
        raise Exception("Missing environment variable 'LOG_LEVEL'.")

    logging.basicConfig(level=logging._nameToLevel[loglevel])
    _LOGGER = logging.getLogger()
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
    global _ERROR_REPORTING_CLIENT
    if _ERROR_REPORTING_CLIENT is not None:
        return _ERROR_REPORTING_CLIENT
    _ERROR_REPORTING_CLIENT = error_reporting.Client()
    return _ERROR_REPORTING_CLIENT


def _get_token():
    """
    Get/set api auth token.

    >>> import main
    >>> main._get_token() # doctest: +ELLIPSIS
    '...'

    :return: the auth token.
    """
    global _TOKEN
    if _TOKEN is not None:
        return _TOKEN

    vars_dict = _get_credentials()
    r_dict = api.get_token(username=vars_dict['username'], password=vars_dict['password'])
    _TOKEN = r_dict['access_token']
    return _get_token()


def _notify_email(subject: str, body: str, pub_sub_client: pubsub.PublisherClient):
    """
    Trigger email notification function with payload.

    >>> import main
    >>> from google.cloud import pubsub_v1 as pubsub
    >>> existing_profile = _get_person_profile_record({'items': [{
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
    >>> current_profile = _get_person_profile_record({'items': [{
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
    >>> body = main._get_person_profile_event_email_body(
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

        if pub_sub_topic is None:
            raise Exception("Missing EMAIL_NOTIFY_PUBSUB_TOPIC env var.")


        data = json.dumps({
            'email': {
                'message': body,
                'to_emails': to_emails,
                'subject': f"[EscavadorParser] {subject}"
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


def _get_person_profile_event_email_body(existing_profile: dict = None, new_profile: dict = None) -> str:
    """
    Get email body for an email notifying a person's profile record resolution.

    :param existing_profile: The existing person profile record.
    :param new_profile:  The newly obtained person profile record.
    :return: html string of the email body.
    """
    body = None

    if existing_profile and new_profile:
        body = f"""
                <h2> Escavador person profile record has differed from existing version. </h2>
                    <h3> New profile: </h3>
                        <div>{json2html.convert(json=existing_profile, table_attributes='border="1"')}</div>
                    <h3> Existing profile record: </h3>
                        <div>{json2html.convert(json=new_profile, table_attributes='border="1"')}</div>
                """
    elif not existing_profile:
        body = f"""
                <h2> New escavador person profile record. </h2>
                    <h3> New profile: </h3>
                        <div>{json2html.convert(json=new_profile, table_attributes='border="1"')}</div>
                """
    return body


def _get_person_profile_record(raw_person_profile: dict, monitor_processes=False) -> dict:
    """
    Get a dict with the incremented person profile.

    :param raw_person_profile: The json loaded result of the call api/v1/pessoas/{pessoaId} for a person.
    :param monitor_processes: Whether any process associated to the person should be monitored.
    :return: dict with the person's returned data plus monitor_processes property.
    """
    raw_person_profile['monitor_processes'] = monitor_processes
    return raw_person_profile


def process_profiles_escavador(event, context) -> int:
    """
    Search for people in escavador and notify if any changes are detected in their profile compared to the last search.
    Related env vars:
        - PEOPLE: JSON string containing array of people objects to search for.
            EX: [{"id":321736294, "name":"Jorge Jafet da Cruz Haddad", "monitor_processes": 1}]

    >>> import main
    >>> main.process_profiles_escavador({}, {})
    0

    :return: 0|1 success/failure.
    """
    try:
        people_list = json.loads(os.getenv("PEOPLE"))
    except JSONDecodeError as e:
        _get_logger().warning(f"No people to fetch profile for or bad people json string. Error: {e}")
        _get_error_reporting_client().report_exception()
        return 1

    # Firestore db client.
    db = firestore.Client()
    escavador_col_ref: CollectionReference = db.collection(_ESCAVADOR_COL_KEY)
    people_doc_ref: DocumentReference = escavador_col_ref.document(_PEOPLE_DOC_KEY)
    records_col_ref: CollectionReference = people_doc_ref.collection(_PROFILES_COL_KEY)
    pub_sub_client = pubsub.PublisherClient()

    for person_dict in people_list:
        name = person_dict[_PERSON_NAME_FIELD_KEY]
        monitor_processes = person_dict[_MONITOR_PERSON_PROCESSES_FIELD_KEY]

        try:
            _get_logger().info(f"Searching for profile of {name}.")
            searched_profile: dict = api.search_person(_get_token(), name)
            person_id = str(searched_profile['items'][0]['id'])
            _get_logger().info(f"Getting profile for '{name}' with id '{person_id}'.")
            person_profile: dict = api.get_person_profile(_get_token(), person_id)
            _get_logger().debug(f"Profile of {name}: \n{person_profile}")

            if not person_profile.keys().__contains__("error"):
                person_new_profile_record = _get_person_profile_record(person_profile, monitor_processes)
                person_doc_ref = records_col_ref.document(person_id)
                person_doc = person_doc_ref.get()

                if not person_doc.exists:
                    _get_logger().info(f"Profile record for person '{name}' found. Updating.")
                    person_doc_ref.set(person_new_profile_record)
                    email_subject = f"New Escavador person profile record found for {name}."
                    email_body = _get_person_profile_event_email_body(new_profile=person_new_profile_record)
                    _notify_email(email_subject, email_body, pub_sub_client=pub_sub_client)
                    continue

                person_existing_profile_record: dict = person_doc.to_dict()

                if person_existing_profile_record != person_new_profile_record:
                    _get_logger().info(f"Person {name}'s profile record has changed. Updating.")
                    person_doc_ref.set(person_new_profile_record)
                    email_subject = f"Escavador person profile record for '{name}' updated."
                    email_body = _get_person_profile_event_email_body(
                        existing_profile=person_existing_profile_record,
                        new_profile=person_new_profile_record
                    )
                    _notify_email(email_subject, email_body, pub_sub_client=pub_sub_client)
                    continue
            else:
                raise Exception(f"Escavador returned an error: {person_profile['error']}")
        except Exception as e:
            _get_logger().warning(
                f"Unable to process profile for person '{name}' due to: {e}."
            )
            _get_error_reporting_client().report_exception()
            continue

    return 0