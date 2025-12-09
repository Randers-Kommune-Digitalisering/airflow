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
        self.cpr = cpr
        self.navnid = navnid
        self.current_address = address
        self.current_district = district
        self.current_tlf_nr = tlf_nr
        self.timestamp = timestamp
        self.journal = journal

        # # Get new address from CPR
        # cpr_address = cpr_client.lookup_address([self.cpr]) if self.cpr else None
        # if cpr_address:
        #     address_str = cpr_address.get('aktuelAdresse', {}).get('standardadresse', '') + ' ' + cpr_address.get('aktuelAdresse', {}).get('postnummer', '')
        #     self.new_address = parse_address(address_str)  # Parse new address
        # else:
        #     self.new_address = self.current_address  # Default to current address if no new address found

        # # Lookup address details from Dataforsyningen (get coordinates)
        # address_details = dataforsyning_client.lookup_address(self.new_address.full_address if self.new_address else self.current_address.full_address)
        # if address_details:
        #     self.new_address.x = address_details.get('adgangsadresse', {}).get('x')
        #     self.new_address.y = address_details.get('adgangsadresse', {}).get('y')
        #     if self.new_address.city is None:
        #         self.new_address.city = address_details.get('adgangsadresse', {}).get('postnrnavn')

        # self.new_district = self.get_new_district(self.new_address if self.new_address else self.current_address)

    # def get_new_district(self, address: Address):
    #     # address_info = dataforsyning_client.lookup_address(address.full_address)
    #     if not address or not address.x or not address.y:
    #         raise ValueError("Address must have valid coordinates (x, y) to determine district.")

    #     if address.x and address.y:
    #         self.new_district = map_client.get_district(address.x, address.y)
    #     else:
    #         self.new_district = None
    #         raise ValueError("Could not find new district for the given address.")

    #     return self.new_district
        
    def to_dict(self):
        return {
            'cpr': self.cpr,
            'navnid': self.navnid,
            'current_address': self.current_address.to_dict() if self.current_address and hasattr(self.current_address, 'to_dict') else None,
            'current_district': self.current_district,
            'current_tlf_nr': self.current_tlf_nr,
            'timestamp': self.timestamp.strftime('%Y-%m-%d %H:%M:%S') if self.timestamp else None,
            'journal': self.journal,
            'new_address': self.new_address.to_dict() if self.new_address and hasattr(self.new_address, 'to_dict') else None,
            'new_district': self.new_district if self.new_district else None,
            # 'address_info': self.address_info
        }
