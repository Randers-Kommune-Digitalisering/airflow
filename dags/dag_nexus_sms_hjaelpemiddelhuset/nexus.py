import logging
import random
import pendulum

from airflow.hooks.base import BaseHook
from airflow.hooks.http_hook import HttpHook
from rkdigi.token_session import ManagedOAuth2Session

from dag_nexus_sms_hjaelpemiddelhuset.sms_sender import send_sms

logger = logging.getLogger(__name__)

MSG_PREFIX = "\n***Besked fra SMS service "
MSG_SUFFIX = "***\n"

message_template = """Hej {navn}
Dine hjælpemidler er klar til afhentning på Hjælpemiddelhuset Kronjylland, Randers kommune.
Agerskellet 22, 8920 Randers NV

Når du henter dine hjælpemidler, skal du anvende følgende ordre-id. Dit ordre ID: {ordreid}

Hvis du henter udenfor åbningstiden, skal du anvende nedenstående kode.
Du bedes afhente dine hjælpemidler indenfor 3 dage.

Din kode til hoveddøren er: {doerkode}

Hvis du vil vide mere:
https://www.randers.dk/borger/socialt/hjaelpemidler-og-hjaelp/hjaelpemiddelhuset/

Du er altid velkommen til at kontakte os på telefon 89158600

Tak for din hjælp og god dag.

Venlig hilsen
Hjælpemiddelhuset Kronjylland

Denne sms kan ikke besvares.
"""


def send_sms_for_hjaelpemiddelhuset_orders(nexus_hook: BaseHook, sms_hook: HttpHook, door_codes: list[str]) -> bool:
    nexus_conn = nexus_hook.get_connection(nexus_hook.http_conn_id)
    nexus_session = ManagedOAuth2Session(
        token_url=nexus_conn.extra_dejson.get("token_url"),
        client_id=nexus_conn.login,
        client_secret=nexus_conn.password,
    )
    res = nexus_session.get(f"{nexus_conn.host.strip('/')}/api/core/mobile/randers/v2/")
    res.raise_for_status()
    home = res.json()

    if not home:
        logger.error("Nexus failed")
        return False

    orders = _get_orders(session=nexus_session, base_url=nexus_conn.host, home=home)

    for item in orders:
        res = nexus_session.get(item['_links']['self']['href'])
        res.raise_for_status()
        order = res.json()

        if not all(k in order for k in ['deliveryNote', 'requestedDeliveryDate', 'phones']):
            logger.warning(f"Order {order.get('uid', 'unknown id')} is missing required fields. Skipping.")
            if 'deliveryNote' not in order:
                delivery_note = order.get('deliveryNote', ' ')
                delivery_date = order.get('requestedDeliveryDate', None)
                res = nexus_session.put(order['_links']['update']['href'], json={"phones": order['phones'], "requestedDeliveryDate": delivery_date, "deliveryNote": delivery_note})
                res.raise_for_status()
            continue

        delivery_note = order.get('deliveryNote', '')
        order_number = order.get('orderNumber', None)
        delivery_date = order.get('requestedDeliveryDate', None)

        if MSG_PREFIX in delivery_note:
            # has already been handled. Skipping
            continue

        if not order_number:
            logger.warning(f"Order {order.get('uid', 'unknown id')} has no order number. Skipping.")
            continue

        if not delivery_date:
            logger.warning(f"Order {order.get('uid', 'unknown id')} has no delivery date. Skipping.")
            continue

        phone_numbers = []
        for _, value in order.get("phones", {}).items():
            phone_numbers.append(value)

        message = MSG_PREFIX + f"{pendulum.now('Europe/Copenhagen').strftime('%d/%m/%Y %H:%M:%S')}: "

        if not phone_numbers:
            logger.info(f"Order {order.get('uid', 'unknown id')} has no phone numbers")
            message = MSG_PREFIX + "Ingen telefonnumre tilknyttet ordren" + MSG_SUFFIX
        else:
            name = _get_patient_name(session=nexus_session, home=home, id=order['patientId'])
            if name:
                # Updating order with the same info to ensure it can be updated later
                res = nexus_session.put(order['_links']['update']['href'], json={"phones": order['phones'], "requestedDeliveryDate": delivery_date, "deliveryNote": delivery_note})
                res.raise_for_status()
                text_message = _construct_message(door_codes=door_codes, name=name, order_number=order_number)
                if text_message:
                    for phone_number in phone_numbers:
                        message += send_sms(http_hook=sms_hook, phone_number=phone_number, text_message=text_message)
                        message += ", "
                    message = message.rstrip(', ') + MSG_SUFFIX
                else:
                    logger.error("Malformed text message!")
                    return False
            else:
                logger.warning(f"Order {order.get('uid', 'unknown id')} has no name")
                message = MSG_PREFIX + "Intet navn tilknyttet ordren" + MSG_SUFFIX
        # Updating the order with a message in the delivery note
        res = nexus_session.put(order['_links']['update']['href'], json={"phones": order['phones'], "requestedDeliveryDate": delivery_date, "deliveryNote": delivery_note + message})
    nexus_session.get(nexus_conn.extra_dejson.get("logout_url"))
    return True


# helper functions
def _get_orders(session: ManagedOAuth2Session, base_url: str, home: dict) -> list:
    """Fetches orders from Nexus, filters them based on handover type and status, and returns the filtered list."""
    res = session.get(f"{home['_links']['preferences']['href']}")
    res.raise_for_status()
    available_lists = res.json()

    selvafhentning_filter_id = None

    for list in available_lists.get('HCL_ORDER', []):
        if list.get('name', '') == 'Selvafhentning':
            selvafhentning_filter_id = list.get('id', None)

    if selvafhentning_filter_id is None:
        raise ValueError("Selvafhentning filter not found")

    res = session.get(f"{base_url}{home['_links']['hclRegisterOrderFilterConfiguration']['href']}", params={'nexusPreferenceId': selvafhentning_filter_id})
    res.raise_for_status()
    filtered_views = res.json()

    res = session.get(f"{filtered_views[0]['_links']['orders']['href']}")
    res.raise_for_status()
    orders = res.json()

    filtered_orders = []
    for order in orders:
        requests = order.get('requests', [])
        if all(req.get('handoverType') == 'SELF_COLLECT' and req.get('status') == 'READY_FOR_DELIVERY' for req in requests) and order.get('handoverType') == 'SELF_COLLECT':
            filtered_orders.append(order)
        else:
            logger.warning(f"Something went wrong with order: {order.get('uid', 'unknown id')} - skipping")
            continue

    return filtered_orders


def _get_patient_name(session: ManagedOAuth2Session, home: dict, id: str) -> str | None:
    """Fetches patient details from Nexus and returns the patient's first name."""
    res = session.get(f"{home['_links']['patients']['href']}", params={'id': id})
    res.raise_for_status()
    patient = res.json()
    return patient.get('firstName', None)


def _construct_message(door_codes: list[str], name: str, order_number: str) -> str | None:
    """Constructs a message for the patient, including a random door code if available."""
    if len(door_codes) > 0:
        door_code = random.choice(door_codes)
        return message_template.format(navn=name, ordreid=order_number, doerkode=door_code)
