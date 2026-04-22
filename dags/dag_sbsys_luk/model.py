from sqlalchemy import Column, DateTime, ForeignKeyConstraint, Identity, Integer, PrimaryKeyConstraint, Unicode
from sqlalchemy.dialects.mssql import BIT, TINYINT
from sqlalchemy.orm import declarative_base, relationship


Base = declarative_base()


class Sag(Base):
    __tablename__ = 'Sag'
    __table_args__ = (
        ForeignKeyConstraint(['SagsStatusID'], ['SbsysNetDrift.dbo.Sagsstatus.ID'], name='Sag_Sagsstatus'),
        ForeignKeyConstraint(['SagsPartID'], ['SbsysNetDrift.dbo.Sagspart.ID'], name='Sag_Sagspart'),
        PrimaryKeyConstraint('ID', name='PK_Sag'),
        {"schema": "SbsysNetDrift.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    SkabelonID = Column(Integer)
    SagsStatusID = Column(Integer)
    SagsPartID = Column(Integer)
    LastStatusChange = Column(DateTime)
    LastStatusChangeComment = Column(Unicode(500, collation='SQL_Danish_Pref_CP1_CI_AS'))

    SagsStatus = relationship('Sagsstatus')
    SagsPart = relationship('Sagspart')
    Erindring = relationship('Erindring', back_populates='Sag')
    Kladde = relationship('KladdeRegistrering', back_populates='Sag')


class Sagsstatus(Base):
    __tablename__ = 'Sagsstatus'
    __table_args__ = (
        PrimaryKeyConstraint('ID', name='PK_Sagsstatus'),
        {"schema": "SbsysNetDrift.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    Navn = Column(Unicode(100, collation='SQL_Danish_Pref_CP1_CI_AS'), nullable=False)


class Sagspart(Base):
    __tablename__ = 'Sagspart'
    __table_args__ = (
        ForeignKeyConstraint(['PartID'], ['SbsysNetDrift.dbo.Person.ID'], name='Sagspart_Person'),
        PrimaryKeyConstraint('ID', name='PK_Sagspart'),
        {"schema": "SbsysNetDrift.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    PartID = Column(Integer)
    PartType = Column(Integer)

    Person = relationship('Person', back_populates='Sagspart')


class Person(Base):
    __tablename__ = 'Person'
    __table_args__ = (
        ForeignKeyConstraint(['CivilstandID'], ['SbsysNetDrift.dbo.CivilstandOpslag.ID'], name='Person_CivilstandOpslag'),
        PrimaryKeyConstraint('ID', name='PK_Person'),
        {"schema": "SbsysNetDrift.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    CivilstandID = Column(Integer)

    Civilstand = relationship('CivilstandOpslag', back_populates='Person')
    Sagspart = relationship('Sagspart', back_populates='Person')


class CivilstandOpslag(Base):
    __tablename__ = 'CivilstandOpslag'
    __table_args__ = (
        PrimaryKeyConstraint('ID', name='PK_CivilstandOpslag'),
        {"schema": "SbsysNetDrift.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    Navn = Column(Unicode(100, collation='SQL_Danish_Pref_CP1_CI_AS'), nullable=False)

    Person = relationship('Person', back_populates='Civilstand')


class Erindring(Base):
    __tablename__ = 'Erindring'
    __table_args__ = (
        ForeignKeyConstraint(['SagID'], ['SbsysNetDrift.dbo.Sag.ID'], name='Erindring_Sag'),
        PrimaryKeyConstraint('ID', name='PK_Erindring'),
        {"schema": "SbsysNetDrift.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    SagID = Column(Integer)

    ErAfsluttet = Column(BIT, nullable=False)
    AfsluttetAfID = Column(Integer)
    Afsluttet = Column(DateTime)
    AfsluttetNotat = Column(Unicode(500, collation='SQL_Danish_Pref_CP1_CI_AS'))

    Sag = relationship('Sag', back_populates='Erindring')


class KladdeRegistrering(Base):
    __tablename__ = 'Kladde'
    __table_args__ = (
        ForeignKeyConstraint(['SagID'], ['SbsysNetDrift.dbo.Sag.ID'], name='Kladde_Sag'),
        PrimaryKeyConstraint('ID', name='PK_KladdeRegistrering'),
        {"schema": "SbsysNetDrift.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    SagID = Column(Integer)

    DeletedState = Column(TINYINT)
    DeletedDate = Column(DateTime)
    DeletedByID = Column(Integer)
    DeletedReason = Column(Unicode(500, collation='SQL_Danish_Pref_CP1_CI_AS'))
    DeleteConfirmed = Column(DateTime)
    DeleteConfirmedByID = Column(Integer)

    Sag = relationship('Sag', back_populates='Kladde')
