from datetime import datetime
from dag_novax_district_control.novax_utils import parse_address


class Address:
    def __init__(self, street: str | None = None, number: str | None = None, postal_code: str | None = None, city: str | None = None, full_address: str | None = None):
        """
        Initialize Address either from components or from full address string.
        Either provide all components (street, number, postal_code, city (optional)) or full_address.
        """
        # Initialize from address components
        if street and number and postal_code:
            self.street: str = street
            self.number: str = number
            self.postal_code: str = postal_code
            self.city: str | None = city  # optional
            self.full_address: str = f"{street} {number}, {postal_code} {city}" if city else f"{street} {number}, {postal_code}"

        # Initialize from full_address, parsing it into components
        elif full_address:
            parsed = parse_address(full_address)  # Parse address into components
            if parsed:
                # Copy attributes from the parsed object onto this instance
                for attr in ("street", "number", "postal_code", "city", "full_address", "x", "y"):
                    if hasattr(parsed, attr):
                        setattr(self, attr, getattr(parsed, attr))
            else:
                self.full_address = full_address
                raise ValueError("Invalid full_address format.")

        else:
            raise ValueError("Either full_address or all address components must be provided.")


class UserData:
    def __init__(self, cpr: str, navnid: int, address: Address | None, district: str, tlf_nr: str | None, timestamp: datetime, journal: str | None = None):
        self.cpr: str = cpr
        self.navnid: int = navnid
        self.current_address: Address | None = address
        self.current_district: str = district
        self.current_tlf_nr: str | None = tlf_nr
        self.timestamp: datetime = timestamp
        self.journal: str | None = journal

        self.new_address: Address | None = None
        self.new_district: str | None = None
        self.new_tlf_nr: str | None = None
        self.parsed_journal: dict | None = None

    def to_dict(self):
        return {
            'cpr': self.cpr,
            'navnid': self.navnid,
            'current_address': self.current_address.__dict__ if self.current_address else None,
            'current_district': self.current_district,
            'current_tlf_nr': self.current_tlf_nr,
            'timestamp': self.timestamp.strftime('%Y-%m-%d %H:%M:%S') if self.timestamp else None,
            'new_address': self.new_address.__dict__ if self.new_address else None,
            'new_district': self.new_district,
            'new_tlf_nr': self.new_tlf_nr,
            'parsed_journal': self.parsed_journal
        }
