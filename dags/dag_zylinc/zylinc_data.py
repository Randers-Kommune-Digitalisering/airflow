import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


def query_queue_elasticsearch(es_client: Any, scroll_size: int, body: dict) -> Optional[List[Dict[str, Any]]]:
    """
    Fetch queue data from Elasticsearch.

    :param es_client: Elasticsearch client instance.
    :param scroll_size: Number of results per scroll.
    :param body: Query body for Elasticsearch.
    :return: List of dicts with queue data or None if error.
    """
    try:
        all_hits = scroll_all_hits(es_client, index="conversation-events-1", body=body, scroll='2m', size=scroll_size)
        data_to_insert = []
        for hit in all_hits:
            source = hit['_source']
            formatted_start_time = hit['fields']['FormattedStartTimeUtc'][0] if 'fields' in hit and 'FormattedStartTimeUtc' in hit['fields'] else None
            data_to_insert.append({
                "QueueName": source.get("QueueName"),
                "Result": source.get("Result"),
                "AgentDisplayName": source.get("AgentDisplayName"),
                "ConversationEventType": source.get("ConversationEventType"),
                "StartTimeUtc": formatted_start_time,
                "TotalDurationInMilliseconds": source.get("TotalDurationInMilliseconds"),
                "EventDurationInMilliseconds": source.get("EventDurationInMilliseconds")
            })
        return data_to_insert
    except Exception as e:
        logger.error(f"Error querying Elasticsearch: {e}")
        return None


def scroll_all_hits(es_client: Any, index: str, body: dict, scroll: str = '2m', size: int = 1000):
    """
    Generator that yields all hits from an Elasticsearch index using scroll.

    :param es_client: Elasticsearch client instance.
    :param index: Index name.
    :param body: Query body.
    :param scroll: Scroll duration.
    :param size: Number of results per scroll.
    """
    page = es_client.search(
        index=index,
        body=body,
        scroll=scroll,
        size=size
    )
    sid = page['_scroll_id']
    hits = page['hits']['hits']
    while hits:
        for hit in hits:
            yield hit
        page = es_client.scroll(scroll_id=sid, scroll=scroll)
        sid = page['_scroll_id']
        hits = page['hits']['hits']


def fetch_queue_data_from_elasticsearch(queue_name: str, es_client: Any, scroll_size: int = 1000) -> Optional[List[Dict[str, Any]]]:
    """
    Fetch queue data from Elasticsearch for a specific queue.

    :param queue_name: Name of the queue.
    :param es_client: Elasticsearch client instance.
    :param scroll_size: Number of results per scroll.
    :return: List of dicts with queue data or None if error.
    """
    try:
        logger.info(f"Fetching data from Elasticsearch for queue: {queue_name}")
        body = {
            "_source": ["QueueName", "Result", "AgentDisplayName", "ConversationEventType", "TotalDurationInMilliseconds", "EventDurationInMilliseconds"],
            "query": {
                "match": {
                    "QueueName": queue_name
                }
            },
            "script_fields": {
                "FormattedStartTimeUtc": {
                    "script": {
                        "source": "SimpleDateFormat format = new SimpleDateFormat('yyyy-MM-dd HH:mm:ss'); return format.format(new Date(doc['StartTimeUtc'].value.toInstant().toEpochMilli()));"
                    }
                }
            }
        }
        data_to_insert = query_queue_elasticsearch(es_client, scroll_size, body)
        return data_to_insert
    except Exception as e:
        logger.error(f"Error fetching data from Elasticsearch for queue {queue_name}: {e}")
        return None


def query_activity_data(es_client: Any, scroll_size: int, body: dict) -> Optional[List[Dict[str, Any]]]:
    """
    Fetch activity data from Elasticsearch.

    :param es_client: Elasticsearch client instance.
    :param scroll_size: Number of results per scroll.
    :param body: Query body for Elasticsearch.
    :return: List of dicts with activity data or None if error.
    """
    try:
        all_hits = scroll_all_hits(es_client, index="clientprod-t19n-activity-data-6", body=body, scroll='2m', size=scroll_size)
        data_to_insert = []
        for hit in all_hits:
            source = hit['_source']
            formatted_first_answer_time = hit['fields']['FormattedFirstAnswerTimeUtc'][0] if 'fields' in hit and 'FormattedFirstAnswerTimeUtc' in hit['fields'] else None
            formatted_start_time = hit['fields']['FormattedStartTimeUtc'][0] if 'fields' in hit and 'FormattedStartTimeUtc' in hit['fields'] else None
            data_to_insert.append({
                "FirstQueueDisplayName": source.get("FirstQueueDisplayName"),
                "FirstAnswerAgentDisplayName": source.get("FirstAnswerAgentDisplayName"),
                "LastQueueDisplayName": source.get("LastQueueDisplayName"),
                "FirstAnswerTimeUtc": formatted_first_answer_time,
                "StartTimeUtc": formatted_start_time,
                "TransferToName": source.get("TransferToName"),
                "Result": source.get("Result")
            })
        return data_to_insert
    except Exception as e:
        logger.error(f"Error querying clientprod-t19n-activity-data-6: {e}")
        return None


EXCLUDED_QUEUE_NAMES: List[str] = [
    "Omstillingen",
    "Jobcenter Randers",
    "UURanders_4747",
    "Ydelseskontor_Team HTF_7194"
]


def _build_must_not_clause(excluded_names: List[str]) -> List[Dict[str, Dict[str, str]]]:
    """
    Build must_not clause for Elasticsearch query.

    :param excluded_names: List of queue names to exclude.
    :return: List of must_not clauses.
    """
    return [{"match": {"LastQueueDisplayName": name}} for name in excluded_names]


def fetch_activity_data_from_elasticsearch(es_client: Any, queue_name: str = "Jobcenter Randers", excluded_queues: List[str] = EXCLUDED_QUEUE_NAMES, scroll_size: int = 1000) -> Optional[List[Dict[str, Any]]]:
    """
    Fetch activity data from Elasticsearch for a specific queue, excluding certain queues.

    :param es_client: Elasticsearch client instance.
    :param queue_name: Name of the queue.
    :param excluded_queues: List of queue names to exclude.
    :param scroll_size: Number of results per scroll.
    :return: List of dicts with activity data or None if error.
    """
    try:
        logger.info("Fetching data from Elasticsearch index: clientprod-t19n-activity-data-6")
        body = {
            "_source": [
                "FirstQueueDisplayName",
                "FirstAnswerAgentDisplayName",
                "LastQueueDisplayName",
                "FirstAnswerTimeUtc",
                "StartTimeUtc",
                "TransferToName",
                "Result"
            ],
            "query": {
                "bool": {
                    "must": [
                        {"match": {"FirstQueueDisplayName": queue_name}}
                    ],
                    "must_not": _build_must_not_clause(excluded_queues)
                }
            },
            "script_fields": {
                "FormattedFirstAnswerTimeUtc": {
                    "script": {
                        "source": """
                            if (doc.containsKey('FirstAnswerTimeUtc') && !doc['FirstAnswerTimeUtc'].empty) {
                                SimpleDateFormat format = new SimpleDateFormat('yyyy-MM-dd HH:mm:ss');
                                return format.format(new Date(doc['FirstAnswerTimeUtc'].value.toInstant().toEpochMilli()));
                            } else {
                                return null;
                            }
                        """
                    }
                },
                "FormattedStartTimeUtc": {
                    "script": {
                        "source": """
                            if (doc.containsKey('StartTimeUtc') && !doc['StartTimeUtc'].empty) {
                                SimpleDateFormat format = new SimpleDateFormat('yyyy-MM-dd HH:mm:ss');
                                return format.format(new Date(doc['StartTimeUtc'].value.toInstant().toEpochMilli()));
                            } else {
                                return null;
                            }
                        """
                    }
                }
            }
        }
        data_to_insert = query_activity_data(es_client, scroll_size, body)
        return data_to_insert
    except Exception as e:
        logger.error(f"Error fetching data from clientprod-t19n-activity-data-6: {e}")
        return None


def get_queue_names() -> List[str]:
    """
    Returns a list of queue names.

    :return: List of queue names.
    """
    queue_names = [
        "IT_Digitalisering_1818",
        "Jobcenter Randers",
        "Jobcenter_Fleksgruppen_7734",
        "Jobcenter_Jobservice_7733",
        "Jobcenter_JobogTilknytning_7732",
        "Jobcenter_Udviklingshuset_7735",
        "Jobcenter_Sygedagpenge_7732",
        "Jobcenter_Team Integration_7738",
        "Hovednummer_89151515",
        "TM_Byggesag_5100",
        "Borgerservice_Boliglån_1984",
        "Borgerservice_Folkeregister_1978",
        "Borgerservice_Pas_Korekort_89159000",
        "Borgerservice_Pension_89151986",
        "Borgerservice_Team Information_89159001",
        "Omstillingen"
    ]
    return queue_names
