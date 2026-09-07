# Tables are created and owned outside this repo (https://github.com/Randers-Kommune-Digitalisering/external-api), so they are reflected at runtime instead of mapped here.
from enum import Enum

SCHEMA = "xflow_nexus"


class RowStatus(str, Enum):
    RECEIVED = "RECEIVED"
    PROCESSING = "PROCESSING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
