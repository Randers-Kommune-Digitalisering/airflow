# This file declares partial classes for "SbsysNetDrift" and "SbsysNetDrift_Byggesag" databases
import datetime

from sqlalchemy import Column, DateTime, ForeignKeyConstraint, Identity, Index, Integer, PrimaryKeyConstraint, Unicode
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


# Models for SbsysNetDrift
class Sag(Base):
    __tablename__ = 'Sag'
    __table_args__ = (
        ForeignKeyConstraint(['BeslutningsTypeID'], ['SbsysNetDrift.dbo.BeslutningsType.ID'], name='Sag_BeslutningsType'),
        ForeignKeyConstraint(['SkabelonID'], ['SbsysNetDrift.dbo.SagSkabelon.ID'], name='Sag_SagSkabelon'),
        PrimaryKeyConstraint('ID', name='PK_Sag'),
        {"schema": "SbsysNetDrift.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    BeslutningsTypeID = Column(Integer)
    LastStatusChange = Column(DateTime)
    Created = Column(DateTime, nullable=False)
    SkabelonID = Column(Integer)
    SagsStatusID = Column(Integer)

    BeslutningsType = relationship("BeslutningsType", back_populates="Sag")
    ByggeSager = relationship('ByggeSag', back_populates='Sag')
    SagSkabelon = relationship('SagSkabelon', back_populates='Sag')


class SagSkabelon(Base):
    __tablename__ = 'SagSkabelon'
    __table_args__ = (
        PrimaryKeyConstraint('ID', name='PK_SagSkabelon'),
        {"schema": "SbsysNetDrift.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    Navn = Column(Unicode(100, collation='SQL_Danish_Pref_CP1_CI_AS'), nullable=False)

    Sag = relationship('Sag', back_populates='SagSkabelon')


class BeslutningsType(Base):
    __tablename__ = 'BeslutningsType'
    __table_args__ = (
        PrimaryKeyConstraint('ID', name='PK_AfgoeringType'),
        {"schema": "SbsysNetDrift.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    Navn = Column(Unicode(50, collation='SQL_Danish_Pref_CP1_CI_AS'), nullable=False)

    Sag = relationship('Sag', back_populates='BeslutningsType')


# Models for SbsysNetDrift_Byggesag database
class ByggeSag(Base):
    __tablename__ = 'ByggeSag'
    __table_args__ = (
        ForeignKeyConstraint(['ByggeSagKodeID'], ['SbsysNetDrift_Byggesag.dbo.ByggeSagKode.ID'], name='FK_ByggeSag_ByggeSagKode'),
        ForeignKeyConstraint(['SagID'], ['SbsysNetDrift.dbo.Sag.ID'], name='FK_ByggeSag_Sag'),
        PrimaryKeyConstraint('ID', name='PK_ByggeSag'),
        Index('IX_ByggeSag_SagId', 'SagID'),
        {"schema": "SbsysNetDrift_Byggesag.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    SagID = Column(Integer, nullable=False)
    Modtaget = Column(DateTime, nullable=False)
    ByggeSagKodeID = Column(Integer)
    Registreret = Column(DateTime)
    FaerdigMeldt = Column(DateTime)
    Byggetilladelse = Column(DateTime)

    ByggeSagKode = relationship('ByggeSagKode', back_populates='ByggeSag')
    Sag = relationship('Sag', back_populates='ByggeSager')


class ByggeSagKode(Base):
    __tablename__ = 'ByggeSagKode'
    __table_args__ = (
        PrimaryKeyConstraint('ID', name='PK_Byggekode'),
        {"schema": "SbsysNetDrift_Byggesag.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    Kode = Column(Unicode(100, collation='SQL_Danish_Pref_CP1_CI_AS'), nullable=False)

    ByggeSag = relationship('ByggeSag', back_populates='ByggeSagKode')
