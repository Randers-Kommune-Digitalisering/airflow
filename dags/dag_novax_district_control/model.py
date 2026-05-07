from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import TEXT, Column, Integer, CHAR, DATETIME, SmallInteger

Base = declarative_base()


class Godkommu(Base):
    __tablename__ = 'Godkommu'
    __table_args__ = {'schema': 'dbo'}
    RECNUM = Column(Integer, nullable=False, primary_key=True)
    NAVNID = Column(CHAR(36), nullable=False, default='')
    JOURNALDATO = Column(DATETIME, nullable=False, default='1753-01-01')
    JOURNALTID = Column(CHAR(5), nullable=False, default='')
    EMNEBREV = Column(CHAR(200), nullable=False, default='')


class Name(Base):
    __tablename__ = 'navn'
    __table_args__ = {'schema': 'dbo'}
    RECNUM = Column(Integer, nullable=False, primary_key=True)
    CPR = Column(CHAR(10), nullable=False, default='')
    AKTIV = Column(CHAR(1), nullable=False, default='')
    DISTRIKT = Column(CHAR(4), nullable=False, default='')
    ADRESSE = Column(CHAR(100), nullable=False, default='')
    AnsvarsShpl = Column(CHAR(8), nullable=False, default='')
    TS_KOMID = Column(CHAR(3), nullable=False, default='')
    ID = Column(CHAR(36), nullable=False, default='')
    TS_UPDD = Column(DATETIME, nullable=False, default='1753-01-01')
    TS_UPDT = Column(CHAR(5), nullable=False, default='')

    details = relationship(
        "NameDetails",
        primaryjoin="Name.ID == foreign(NameDetails.NAVNID)",
        viewonly=False,
        lazy="joined",
        uselist=False
    )

    addresses = relationship(
        "Address",
        primaryjoin="Name.ID == foreign(Address.NAVNID)",
        viewonly=False,
        lazy="joined",
    )

    person_districts = relationship(
        "PersonDistrict",
        primaryjoin="Name.ID == foreign(PersonDistrict.NAVNID)",
        viewonly=False,
        lazy="joined"
    )

    phones = relationship(
        "Phone",
        primaryjoin="Name.ID == foreign(Phone.NAVNID)",
        viewonly=False,
        lazy="joined"
    )


class NameDetails(Base):
    __tablename__ = 'NAVNDETALJER'
    __table_args__ = {'schema': 'dbo'}
    RECNUM = Column(Integer, nullable=False, primary_key=True)
    NAVNID = Column(CHAR(36), nullable=False, default='')
    TERMIN = Column(DATETIME, nullable=False, default='1753-01-01')
    BESKYTTETADRESSE = Column(SmallInteger, nullable=False, default=0)
    KOMMUNE_OPR = Column(CHAR(3))  # Same as TS_KOMID
    TS_KOMID = Column(CHAR(3), nullable=False, default='')
    TS_UPDD = Column(DATETIME, nullable=False, default='1753-01-01')
    TS_UPDT = Column(CHAR(5), nullable=False, default='')


class Address(Base):
    __tablename__ = 'adrs'
    __table_args__ = {'schema': 'dbo'}
    RECNUM = Column(Integer, nullable=False, primary_key=True)
    NAVNID = Column(CHAR(36), nullable=False, default='')
    VEJKODE = Column(CHAR(10), nullable=False, default='')
    KOMMUNEKODE = Column(CHAR(3), nullable=False, default='')
    POSTNR = Column(CHAR(4), nullable=False, default='')
    STEDNAVN = Column(CHAR(50), nullable=False, default='')
    NR_LT_ETAGE = Column(CHAR(20), nullable=False, default='')
    DATO_FRA = Column(DATETIME, nullable=False, default='1753-01-01')
    DATO_TIL = Column(DATETIME, nullable=False, default='1753-01-01')
    TS_DATE = Column(DATETIME, nullable=False, default='1753-01-01')
    TS_TIME = Column(CHAR(5), nullable=False, default='')
    TS_UPDD = Column(DATETIME, nullable=False, default='1753-01-01')
    TS_UPDT = Column(CHAR(5), nullable=False, default='')


class PersonDistrict(Base):
    __tablename__ = 'PERSONDISTRICT'
    __table_args__ = {'schema': 'dbo'}
    RECNUM = Column(Integer, nullable=False, primary_key=True)
    NAVNID = Column(CHAR(36), nullable=False, default='')
    DISTRICT = Column(CHAR(4), nullable=False, default='')
    DATEFROM = Column(DATETIME, nullable=False, default='1753-01-01')
    DATETO = Column(DATETIME, nullable=False, default='1753-01-01')
    TS_DATE = Column(DATETIME, nullable=False, default='1753-01-01')
    TS_TIME = Column(CHAR(5), nullable=False, default='')
    TS_UPDD = Column(DATETIME, nullable=False, default='1753-01-01')
    TS_UPDT = Column(CHAR(5), nullable=False, default='')


class Phone(Base):
    __tablename__ = 'TELEFON'
    __table_args__ = {'schema': 'dbo'}
    RECNUM = Column(Integer, nullable=False, primary_key=True)
    NAVNID = Column(CHAR(36), nullable=False, default='')
    TELEFONNUMMER = Column(CHAR(20), nullable=False, default='')
    PRIMAER = Column(SmallInteger, nullable=False, default=0)
    TS_DATE = Column(DATETIME, nullable=False, default='1753-01-01')
    TS_TIME = Column(CHAR(5), nullable=False, default='')
    TS_UPDD = Column(DATETIME, nullable=False, default='1753-01-01')
    TS_UPDT = Column(CHAR(5), nullable=False, default='')


class Note(Base):
    __tablename__ = 'Note'
    __table_args__ = {'schema': 'dbo'}
    RECNUM = Column(Integer, nullable=False, primary_key=True)
    NAVNID = Column(CHAR(36), nullable=False, default='')
    DATO = Column(DATETIME, nullable=False, default='1753-01-01')
    TIDSPUNKT = Column(CHAR(5), nullable=False, default='')
    NOTE = Column(CHAR(2000), nullable=False, default='')
    TS_DATE = Column(DATETIME, nullable=False, default='1753-01-01')


class Remind(Base):
    __tablename__ = 'Remind'
    __table_args__ = {'schema': 'dbo'}
    RECNUM = Column(Integer, nullable=False, primary_key=True)
    KODE = Column(CHAR(10), nullable=False, default='')
    BEMAERK = Column(TEXT(16), nullable=False, default='')
    BRUGER = Column(CHAR(8), nullable=False, default='')
    TS_DATE = Column(DATETIME, nullable=False, default='1753-01-01')
    TS_TIME = Column(CHAR(5), nullable=False, default='')
    TS_UPDD = Column(DATETIME, nullable=False, default='1753-01-01')
    TS_UPDT = Column(CHAR(5), nullable=False, default='')
    OPRETTET = Column(DATETIME, nullable=False, default='1753-01-01')
    NAVNID = Column(CHAR(36), nullable=False, default='')
