import logging
import time
from airflow.hooks.http_hook import HttpHook

from xml.etree import ElementTree as ET

logger = logging.getLogger(__name__)

# Constants
first_number_for_mobile_phones = [2, 30, 31, 40, 41, 42, 50, 51, 52, 53, 60, 61, 71, 81, 91, 92, 93]
xml_template = '<?xml version="1.0" encoding="UTF-8"?><sms><countrycode>45</countrycode><number>{phone_number}</number><message>{message}</message></sms>'

sms_sent = {}


def send_sms(http_hook: HttpHook, phone_number: str, text_message: str) -> str:
    """Validates the phone number and message, checks rate limits, and sends an SMS via the provided HttpHook. Returns a status message indicating success or failure."""
    try:
        cleaned_phone_number = _check_if_mobile_number_and_clean(phone_number)
        if not cleaned_phone_number:
            raise ValueError("Ikke et gyldigt mobilnummer")
        if any(char in text_message for char in "&<>"):
            raise ValueError("Ulovlig charakter i besked.")
        try:
            xml_payload = xml_template.format(phone_number=cleaned_phone_number, message=text_message)

            if _get_sms_sent(cleaned_phone_number) >= 10:
                last_sent = _get_last_sms_time(cleaned_phone_number)
                if last_sent and (time.time() - last_sent) < 86400:
                    logger.warning(f"SMS to {cleaned_phone_number} was sent less than a day ago.")
                    return f"10 SMSer allerede sendt til {cleaned_phone_number} indenfor det sidste døgn."

            response = http_hook.run(
                endpoint="/api/sms/send",
                data=xml_payload.encode('utf-8'),
                headers={"Content-Type": "application/xml; charset=utf-8"}
            )

            response_xml = response.text

            root = ET.fromstring(response_xml)
            description = root.find(".//description").text

            if description.lower() == "message handled successfully.":
                _add_to_sms_sent(cleaned_phone_number)
                return f"SMS sendt til {phone_number}"
            else:
                logger.error(f"Error in SMS response: {description}")
                raise Exception("Fejl")
        except Exception as e:
            logger.error(f"Error sending SMS: {e}")
            raise Exception("Fejl")
    except Exception as e:
        logger.warning(f"Error sending SMS: {e}")
        return str(e) + f" - kunne ikke sende SMS til {phone_number}"


# Helper functions
def _check_if_mobile_number_and_clean(number: str) -> str | bool:
    """Cleans the phone number by removing country codes and leading zeros, then checks if it's a valid mobile number based on predefined prefixes. Returns the cleaned number if valid, or False if invalid."""
    if number.startswith("+45"):
        number = number[3:]
    elif number.startswith("45") and len(number) > 8:
        number = number[2:]
    elif number.startswith("0"):
        number = number[1:]

    if len(number) == 8 and (int(number[0]) in first_number_for_mobile_phones or int(number[:2]) in first_number_for_mobile_phones):
        return number
    else:
        return False


def _add_to_sms_sent(phone_number: str) -> None:
    """Adds a record of a sent SMS for the given phone number, updating the count and last sent time."""
    now = time.time()
    if phone_number in sms_sent:
        sms_sent[phone_number]['count'] += 1
        sms_sent[phone_number]['last_sent'] = now
    else:
        sms_sent[phone_number] = {'count': 1, 'last_sent': now}


def _get_sms_sent(phone_number: str) -> int:
    """Retrieves the count of sent SMS for the given phone number. Returns 0 if no record exists."""
    entry = sms_sent.get(phone_number)
    if entry:
        return entry['count']
    return 0


def _get_last_sms_time(phone_number: str) -> float | None:
    """Retrieves the last sent time for the given phone number. Returns None if no record exists."""
    entry = sms_sent.get(phone_number)
    if entry:
        return entry['last_sent']
