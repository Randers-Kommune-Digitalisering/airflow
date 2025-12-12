from datetime import datetime
from dag_novax_district_control.novax_utils import parse_address
from dag_novax_district_control.clients.district_map_client import DataforsyningClient, DistrictMapClient
# from dag_novax_district_control.clients.cpr_client import CPRClient

dataforsyning_client = DataforsyningClient()
map_client = DistrictMapClient()
# cpr_client = CPRClient()


class Address:
    def __init__(self, street: str = None, number: str = None, postal_code: str = None, city: str = None, full_address: str = None):
        # Initialize from address components
        if street and number and postal_code:
            self.street = street
            self.number = number
            self.postal_code = postal_code
            self.city = city  # optional
            self.full_address = f"{street} {number}, {postal_code} {city}" if city else f"{street} {number}, {postal_code}"

        # Initialize from full_address, parsing it into components
        elif full_address:
            parsed = parse_address(full_address)  # Parse address into components
            if parsed:
                self = parsed
            else:
                self.full_address = full_address
                raise ValueError("Invalid full_address format.")

        else:
            raise ValueError("Either full_address or all address components must be provided.")

    def to_dict(self):
        obj = {
            'street': self.street,
            'number': self.number,
            'postal_code': self.postal_code,
            'city': self.city,  # optional
            'full_address': self.full_address
        }
        if hasattr(self, 'x'):
            obj['x'] = self.x
        if hasattr(self, 'y'):
            obj['y'] = self.y
        return obj


class UserData:
    def __init__(self, cpr: str, navnid: int, address: Address, district: str, tlf_nr: str, timestamp: datetime, journal: str = None):
        self.cpr: str = cpr
        self.navnid: int = navnid
        self.current_address: Address = address
        self.current_district: str = district
        self.current_tlf_nr: str = tlf_nr
        self.timestamp: datetime = timestamp
        self.journal: str = journal

        self.new_address: Address = None
        self.new_district: str = None
        self.new_tlf_nr: str = None
        self.parsed_journal: dict = None

    def to_dict(self):
        return {
            'cpr': self.cpr,
            'navnid': self.navnid,
            'current_address': self.current_address.to_dict() if hasattr(self.current_address, 'to_dict') else None,
            'current_district': self.current_district,
            'current_tlf_nr': self.current_tlf_nr,
            'timestamp': self.timestamp.strftime('%Y-%m-%d %H:%M:%S') if self.timestamp else None,
            'new_address': self.new_address.to_dict() if hasattr(self.new_address, 'to_dict') else None,
            'new_district': self.new_district,
            'new_tlf_nr': self.new_tlf_nr,
            'parsed_journal': self.parsed_journal
        }
