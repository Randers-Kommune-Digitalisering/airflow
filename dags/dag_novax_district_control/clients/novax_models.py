from __future__ import annotations

from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Navn(Base):
    __tablename__ = "navn"

    RECNUM = Column(Integer, primary_key=True)
    CPR = Column(String(10), nullable=False, default="")
    DISTRIKT = Column(String(8), nullable=False, default="")
    ADRESSE = Column(String(255), nullable=False, default="")
    ID = Column(String(36), nullable=False, default="")


class NavnDetaljer(Base):
    __tablename__ = "NAVNDETALJER"

    RECNUM = Column(Integer, primary_key=True)
    NAVNID = Column(String(36), nullable=False, default="")
    TERMIN = Column(DateTime, nullable=True)
    TS_KOMID = Column(Integer, nullable=True)
    KOMMUNE_OPR = Column(Integer, nullable=True)


class PersonDistrict(Base):
    __tablename__ = "PERSONDISTRICT"

    RECNUM = Column(Integer, primary_key=True)
    NAVNID = Column(String(36), nullable=False, default="")
    DISTRICT = Column(String(8), nullable=False, default="")
    DATEFROM = Column(DateTime, nullable=True)
    DATETO = Column(DateTime, nullable=True)


class Telefon(Base):
    __tablename__ = "TELEFON"

    RECNUM = Column(Integer, primary_key=True)
    NAVNID = Column(String(36), nullable=False, default="")
    TELEFONNUMMER = Column(String(13), nullable=False, default="")
    PRIMAER = Column(Integer, nullable=False, default=0)
    TS_UPDD = Column(DateTime, nullable=True)
