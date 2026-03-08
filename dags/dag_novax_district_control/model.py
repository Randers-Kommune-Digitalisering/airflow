from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, Integer, CHAR, DATETIME, SmallInteger

Base = declarative_base()


class Godkommu(Base):
    __tablename__ = 'Godkommu'
    __table_args__ = {'schema': 'dbo'}
    RECNUM = Column(Integer, primary_key=True)
    NAVNID = Column(CHAR(36))
    JOURNALDATO = Column(DATETIME)
    JOURNALTID = Column(CHAR(5))
    EMNEBREV = Column(CHAR(200))


class Name(Base):
    __tablename__ = 'navn'
    __table_args__ = {'schema': 'dbo'}
    RECNUM = Column(Integer, primary_key=True)
    CPR = Column(CHAR(10), nullable=False, default='')
    AKTIV = Column(CHAR(1), nullable=False, default='')
    DISTRIKT = Column(CHAR(4), nullable=False, default='')
    ADRESSE = Column(CHAR(100), nullable=False, default='')
    # AnsvarsShpl = Column(CHAR(8), nullable=False, default='')  # Must be 'FIKTIV' ?!
    TS_KOMID = Column(CHAR(3), nullable=False, default='')
    ID = Column(CHAR(36), nullable=False, default='')
    TS_UPDD = Column(DATETIME)
    TS_UPDT = Column(CHAR(5))

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
    RECNUM = Column(Integer, primary_key=True)
    NAVNID = Column(CHAR(36))
    TERMIN = Column(DATETIME)
    BESKYTTETADRESSE = Column(SmallInteger)
    # KOMMUNE_OPR = Column(CHAR(3))  # Is this the same as TS_KOMID?
    TS_KOMID = Column(CHAR(3))
    TS_UPDD = Column(DATETIME)
    TS_UPDT = Column(CHAR(5))


class Address(Base):
    __tablename__ = 'adrs'
    __table_args__ = {'schema': 'dbo'}
    RECNUM = Column(Integer, primary_key=True)
    NAVNID = Column(CHAR(36))
    VEJKODE = Column(CHAR(10))
    KOMMUNEKODE = Column(CHAR(3))
    POSTNR = Column(CHAR(4))
    STEDNAVN = Column(CHAR(50))
    NR_LT_ETAGE = Column(CHAR(20))
    DATO_FRA = Column(DATETIME)
    DATO_TIL = Column(DATETIME)
    TS_DATE = Column(DATETIME)
    TS_TIME = Column(CHAR(5))
    TS_UPDD = Column(DATETIME)
    TS_UPDT = Column(CHAR(5))


class PersonDistrict(Base):
    __tablename__ = 'PERSONDISTRICT'
    __table_args__ = {'schema': 'dbo'}
    RECNUM = Column(Integer, primary_key=True)
    NAVNID = Column(CHAR(36))
    DISTRICT = Column(CHAR(4))
    DATEFROM = Column(DATETIME)
    DATETO = Column(DATETIME)
    TS_DATE = Column(DATETIME)
    TS_TIME = Column(CHAR(5))
    TS_UPDD = Column(DATETIME)
    TS_UPDT = Column(CHAR(5))


class Phone(Base):
    __tablename__ = 'TELEFON'
    __table_args__ = {'schema': 'dbo'}
    RECNUM = Column(Integer, primary_key=True)
    NAVNID = Column(CHAR(36))
    TELEFONNUMMER = Column(CHAR(20))
    PRIMAER = Column(SmallInteger)
    TS_DATE = Column(DATETIME)
    TS_TIME = Column(CHAR(5))
    TS_UPDD = Column(DATETIME)
    TS_UPDT = Column(CHAR(5))


class Note(Base):
    __tablename__ = 'Note'
    __table_args__ = {'schema': 'dbo'}
    RECNUM = Column(Integer, primary_key=True)
    NAVNID = Column(CHAR(36))
    DATO = Column(DATETIME)
    TIDSPUNKT = Column(CHAR(5))
    NOTE = Column(CHAR(2000))
    TS_DATE = Column(DATETIME)
