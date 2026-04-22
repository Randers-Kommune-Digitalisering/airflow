from typing import Optional

from sqlalchemy import ForeignKeyConstraint, Identity, Integer, PrimaryKeyConstraint, Unicode
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Sag(Base):
    __tablename__ = 'Sag'
    __table_args__ = (
        ForeignKeyConstraint(['SagsStatusID'], ['SbsysNetDrift.dbo.Sagsstatus.ID'], name='Sag_Sagsstatus'),
        ForeignKeyConstraint(['SagsPartID'], ['SbsysNetDrift.dbo.Sagspart.ID'], name='Sag_Sagspart'),
        PrimaryKeyConstraint('ID', name='PK_Sag'),
        {"schema": "SbsysNetDrift.dbo"}
    )

    ID: Mapped[int] = mapped_column(Integer, Identity(start=1, increment=1), primary_key=True)
    SkabelonID: Mapped[Optional[int]] = mapped_column(Integer)
    SagsStatusID: Mapped[Optional[int]] = mapped_column(Integer)
    SagsPartID: Mapped[Optional[int]] = mapped_column(Integer)

    SagsStatus: Mapped[Optional['Sagsstatus']] = relationship('Sagsstatus')
    SagsPart: Mapped[Optional['Sagspart']] = relationship('Sagspart')


class Sagsstatus(Base):
    __tablename__ = 'Sagsstatus'
    __table_args__ = (
        PrimaryKeyConstraint('ID', name='PK_Sagsstatus'),
        {"schema": "SbsysNetDrift.dbo"}
    )

    ID: Mapped[int] = mapped_column(Integer, Identity(start=1, increment=1), primary_key=True)
    Navn: Mapped[str] = mapped_column(Unicode(100, 'SQL_Danish_Pref_CP1_CI_AS'), nullable=False)


class Sagspart(Base):
    __tablename__ = 'Sagspart'
    __table_args__ = (
        ForeignKeyConstraint(['PartID'], ['SbsysNetDrift.dbo.Person.ID'], name='Sagspart_Person'),
        PrimaryKeyConstraint('ID', name='PK_Sagspart'),
        {"schema": "SbsysNetDrift.dbo"}
    )

    ID: Mapped[int] = mapped_column(Integer, Identity(start=1, increment=1), primary_key=True)
    PartID: Mapped[Optional[int]] = mapped_column(Integer)
    PartType: Mapped[Optional[int]] = mapped_column(Integer)

    Person: Mapped[Optional['Person']] = relationship('Person', back_populates='Sagspart')


class Person(Base):
    __tablename__ = 'Person'
    __table_args__ = (
        ForeignKeyConstraint(['CivilstandID'], ['SbsysNetDrift.dbo.CivilstandOpslag.ID'], name='Person_CivilstandOpslag'),
        PrimaryKeyConstraint('ID', name='PK_Person'),
        {"schema": "SbsysNetDrift.dbo"}
    )

    ID: Mapped[int] = mapped_column(Integer, Identity(start=1, increment=1), primary_key=True)
    CivilstandID: Mapped[Optional[int]] = mapped_column(Integer)

    Civilstand: Mapped[Optional['CivilstandOpslag']] = relationship('CivilstandOpslag', back_populates='Person')
    Sagspart: Mapped[list['Sagspart']] = relationship('Sagspart', back_populates='Person')


class CivilstandOpslag(Base):
    __tablename__ = 'CivilstandOpslag'
    __table_args__ = (
        PrimaryKeyConstraint('ID', name='PK_CivilstandOpslag'),
        {"schema": "SbsysNetDrift.dbo"}
    )

    ID: Mapped[int] = mapped_column(Integer, Identity(start=1, increment=1), primary_key=True)
    Navn: Mapped[str] = mapped_column(Unicode(100, 'SQL_Danish_Pref_CP1_CI_AS'), nullable=False)

    Person: Mapped[list['Person']] = relationship('Person', back_populates='Civilstand')
