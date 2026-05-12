from sqlalchemy import Column, DateTime, ForeignKeyConstraint, Identity, Integer, LargeBinary, PrimaryKeyConstraint, Unicode
from sqlalchemy.dialects.mssql import BIT
from sqlalchemy.orm import declarative_base, relationship
from airflow.models import Variable

Base = declarative_base()

# Placeholder "schema" key that we will translate at execution time.
DOKUMENTDATA_SHARD_SCHEMA = "DOKUMENTDATA_SHARD"
KLADDEDATA_SHARD_SCHEMA = "KLADDEDATA_SHARD"
ENV = "Test" if Variable.get("SBSYS_LUK_TEST_ENV", default_var="False").lower() == "true" else "Drift"


class Sag(Base):
    __tablename__ = 'Sag'
    __table_args__ = (
        ForeignKeyConstraint(['SagsStatusID'], [f'SbsysNet{ENV}.dbo.Sagsstatus.ID'], name='Sag_Sagsstatus'),
        ForeignKeyConstraint(['SagsPartID'], [f'SbsysNet{ENV}.dbo.Sagspart.ID'], name='Sag_Sagspart'),
        PrimaryKeyConstraint('ID', name='PK_Sag'),
        {"schema": f"SbsysNet{ENV}.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    Nummer = Column(Unicode(50, collation='SQL_Danish_Pref_CP1_CI_AS'), nullable=False)
    SkabelonID = Column(Integer)
    SagsStatusID = Column(Integer)
    SagsPartID = Column(Integer)
    LastStatusChange = Column(DateTime)
    LastStatusChangeComments = Column(Unicode(400, collation='SQL_Danish_Pref_CP1_CI_AS'))

    SagsStatus = relationship('Sagsstatus')
    SagsPart = relationship('Sagspart')
    Erindring = relationship('Erindring')
    KladdeRegistrering = relationship('KladdeRegistrering')


class Sagsstatus(Base):
    __tablename__ = 'Sagsstatus'
    __table_args__ = (
        PrimaryKeyConstraint('ID', name='PK_Sagsstatus'),
        {"schema": f"SbsysNet{ENV}.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    Navn = Column(Unicode(100, collation='SQL_Danish_Pref_CP1_CI_AS'), nullable=False)


class Sagspart(Base):
    __tablename__ = 'Sagspart'
    __table_args__ = (
        ForeignKeyConstraint(['PartID'], [f'SbsysNet{ENV}.dbo.Person.ID'], name='Sagspart_Person'),
        PrimaryKeyConstraint('ID', name='PK_Sagspart'),
        {"schema": f"SbsysNet{ENV}.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    PartID = Column(Integer)
    PartType = Column(Integer)

    Person = relationship('Person')


class Person(Base):
    __tablename__ = 'Person'
    __table_args__ = (
        ForeignKeyConstraint(['CivilstandID'], [f'SbsysNet{ENV}.dbo.CivilstandOpslag.ID'], name='Person_CivilstandOpslag'),
        PrimaryKeyConstraint('ID', name='PK_Person'),
        {"schema": f"SbsysNet{ENV}.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    CivilstandID = Column(Integer)

    Civilstand = relationship('CivilstandOpslag')


class CivilstandOpslag(Base):
    __tablename__ = 'CivilstandOpslag'
    __table_args__ = (
        PrimaryKeyConstraint('ID', name='PK_CivilstandOpslag'),
        {"schema": f"SbsysNet{ENV}.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    Navn = Column(Unicode(100, collation='SQL_Danish_Pref_CP1_CI_AS'), nullable=False)


class Erindring(Base):
    __tablename__ = 'Erindring'
    __table_args__ = (
        ForeignKeyConstraint(['SagID'], [f'SbsysNet{ENV}.dbo.Sag.ID'], name='Erindring_Sag'),
        PrimaryKeyConstraint('ID', name='PK_Erindring'),
        {"schema": f"SbsysNet{ENV}.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    SagID = Column(Integer)

    ErAfsluttet = Column(BIT, nullable=False)
    AfsluttetAfID = Column(Integer)
    Afsluttet = Column(DateTime)
    AfsluttetNotat = Column(Unicode(500, collation='SQL_Danish_Pref_CP1_CI_AS'))


class KladdeRegistrering(Base):
    __tablename__ = 'KladdeRegistrering'
    __table_args__ = (
        ForeignKeyConstraint(['SagID'], [f'SbsysNet{ENV}.dbo.Sag.ID'], name='Kladde_Sag'),
        ForeignKeyConstraint(['KladdeID'], [f'SbsysNet{ENV}.dbo.Kladde.ID'], name='KladdeRegistrering_Kladde'),
        PrimaryKeyConstraint('ID', name='PK_KladdeRegistrering'),
        {"schema": f"SbsysNet{ENV}.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    SagID = Column(Integer)
    KladdeID = Column(Integer)
    Navn = Column(Unicode(200, collation='SQL_Danish_Pref_CP1_CI_AS'), nullable=True)
    Beskrivelse = Column(Unicode(255, collation='SQL_Danish_Pref_CP1_CI_AS'), nullable=True)

    Kladde = relationship('Kladde', uselist=False, back_populates='KladdeRegistrering')
    DelforloebKladdeRegistrering = relationship('DelforloebKladdeRegistrering', back_populates='KladdeRegistrering')
    # Bilag = relationship('Bilag')


class DelforloebKladdeRegistrering(Base):
    __tablename__ = 'DelforloebKladdeRegistrering'
    __table_args__ = (
        ForeignKeyConstraint(['DelforloebID'], [f'SbsysNet{ENV}.dbo.Delforloeb.ID'], name='DelforloebKladdeRegistrering_Delforloeb'),
        ForeignKeyConstraint(['KladdeRegistreringID'], [f'SbsysNet{ENV}.dbo.KladdeRegistrering.ID'], name='DelforloebKladdeRegistrering_KladdeRegistrering'),
        PrimaryKeyConstraint('ID', name='PK_DelforloebKladdeRegistrering'),
        {"schema": f"SbsysNet{ENV}.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    DelforloebID = Column(Integer, nullable=False)
    KladdeRegistreringID = Column(Integer, nullable=False)

    KladdeRegistrering = relationship('KladdeRegistrering', back_populates='DelforloebKladdeRegistrering')


class Kladde(Base):
    __tablename__ = 'Kladde'
    __table_args__ = (
        PrimaryKeyConstraint('ID', name='PK_Kladde'),
        {"schema": f"SbsysNet{ENV}.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    Navn = Column(Unicode(200, collation='SQL_Danish_Pref_CP1_CI_AS'), nullable=False)
    Beskrivelse = Column(Unicode(255, collation='SQL_Danish_Pref_CP1_CI_AS'), nullable=True)
    IsArchived = Column(BIT, nullable=False)
    FileName = Column(Unicode(255, collation='SQL_Danish_Pref_CP1_CI_AS'), nullable=False)
    FileExtension = Column(Unicode(50, collation='SQL_Danish_Pref_CP1_CI_AS'), nullable=False)

    KladdeRegistrering = relationship('KladdeRegistrering', uselist=False, back_populates='Kladde')
    KladdeData = relationship('KladdeData', uselist=False, back_populates='Kladde')


class KladdeData(Base):
    __tablename__ = 'KladdeData'
    __table_args__ = (
        ForeignKeyConstraint(['KladdeID'], [f'SbsysNet{ENV}.dbo.Kladde.ID'], name='KladdeData_Kladde'),
        PrimaryKeyConstraint('ID', name='PK_KladdeData'),
        {"schema": KLADDEDATA_SHARD_SCHEMA}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    KladdeID = Column(Integer)
    Data = Column(LargeBinary, nullable=True)

    Kladde = relationship('Kladde', uselist=False, back_populates='KladdeData')


class DokumentRegistrering(Base):
    __tablename__ = 'DokumentRegistrering'
    __table_args__ = (
        ForeignKeyConstraint(['SagID'], [f'SbsysNet{ENV}.dbo.Sag.ID'], name='DokumentRegistrering_Sag'),
        ForeignKeyConstraint(['DokumentID'], [f'SbsysNet{ENV}.dbo.Dokument.ID'], name='DokumentRegistrering_Dokument'),
        PrimaryKeyConstraint('ID', name='PK_DokumentRegistrering'),
        {"schema": f"SbsysNet{ENV}.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    SagID = Column(Integer, nullable=False)
    DokumentID = Column(Integer, nullable=False)
    Navn = Column(Unicode(200, collation='SQL_Danish_Pref_CP1_CI_AS'), nullable=True)
    Beskrivelse = Column(Unicode(255, collation='SQL_Danish_Pref_CP1_CI_AS'), nullable=True)
    Registreret = Column(DateTime, nullable=False)
    RegistreretAfID = Column(Integer, nullable=False)

    DelforloebDokumentRegistrering = relationship('DelforloebDokumentRegistrering', back_populates='DokumentRegistrering')
    Dokument = relationship('Dokument', uselist=False, back_populates='DokumentRegistrering')


class DelforloebDokumentRegistrering(Base):
    __tablename__ = 'DelforloebDokumentRegistrering'
    __table_args__ = (
        ForeignKeyConstraint(['DelforloebID'], [f'SbsysNet{ENV}.dbo.Delforloeb.ID'], name='DelforloebDokumentRegistrering_Delforloeb'),
        ForeignKeyConstraint(['DokumentRegistreringID'], [f'SbsysNet{ENV}.dbo.DokumentRegistrering.ID'], name='DelforloebDokumentRegistrering_DokumentRegistrering'),
        PrimaryKeyConstraint('ID', name='PK_DelforloebDokumentRegistrering'),
        {"schema": f"SbsysNet{ENV}.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    DelforloebID = Column(Integer, nullable=False)
    DokumentRegistreringID = Column(Integer, nullable=False)

    DokumentRegistrering = relationship('DokumentRegistrering', back_populates='DelforloebDokumentRegistrering')


class Dokument(Base):
    __tablename__ = 'Dokument'
    __table_args__ = (
        ForeignKeyConstraint(['FraKladdeID'], [f'SbsysNet{ENV}.dbo.Kladde.ID'], name='Dokument_Kladde', ondelete='SET NULL'),
        PrimaryKeyConstraint('ID', name='PK_Dokument'),
        {"schema": f"SbsysNet{ENV}.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    Navn = Column(Unicode(200, collation='SQL_Danish_Pref_CP1_CI_AS'), nullable=False)
    Beskrivelse = Column(Unicode(255, collation='SQL_Danish_Pref_CP1_CI_AS'), nullable=True)
    FraKladdeID = Column(Integer, nullable=True)
    DokumentArtID = Column(Integer, nullable=False)
    DokumentType = Column(Integer, nullable=True)
    OprettetAfID = Column(Integer, nullable=False)
    Oprettet = Column(DateTime, nullable=False)
    PostlisteTitel = Column(Unicode(255, collation='SQL_Danish_Pref_CP1_CI_AS'), nullable=True)
    PrimaryDokumentDataInfoID = Column(Integer, nullable=True)

    DokumentRegistrering = relationship('DokumentRegistrering', uselist=False, back_populates='Dokument')
    DokumentData = relationship('DokumentData', uselist=False, back_populates='Dokument')
    DokumentDataInfo = relationship('DokumentDataInfo', uselist=False, back_populates='Dokument')


class DokumentData(Base):
    __tablename__ = 'DokumentData'
    __table_args__ = (
        ForeignKeyConstraint(['DokumentID'], [f'SbsysNet{ENV}.dbo.Dokument.ID'], name='DokumentData_Dokument'),
        ForeignKeyConstraint(['DokumentDataInfoID'], [f'SbsysNet{ENV}.dbo.DokumentDataInfo.ID'], name='DokumentData_DokumentDataInfo'),
        PrimaryKeyConstraint('ID', name='PK_DokumentData'),
        {"schema": DOKUMENTDATA_SHARD_SCHEMA}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    DokumentID = Column(Integer)
    DokumentDataInfoID = Column(Integer, nullable=True)
    Data = Column(LargeBinary, nullable=True)

    Dokument = relationship('Dokument', uselist=False, back_populates='DokumentData')


class DokumentDataInfo(Base):
    __tablename__ = 'DokumentDataInfo'
    __table_args__ = (
        ForeignKeyConstraint(['DokumentID'], [f'SbsysNet{ENV}.dbo.Dokument.ID'], name='DokumentDataInfo_Dokument'),
        PrimaryKeyConstraint('ID', name='PK_DokumentDataInfo'),
        {"schema": f"SbsysNet{ENV}.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    DokumentID = Column(Integer, nullable=False)
    DokumentDataType = Column(Integer, nullable=False)
    DokumentDataInfoType = Column(Integer, nullable=False)
    FileName = Column(Unicode(255, collation='SQL_Danish_Pref_CP1_CI_AS'), nullable=True)
    FileSize = Column(Integer, nullable=False)
    FileExtension = Column(Unicode(30, collation='SQL_Danish_Pref_CP1_CI_AS'), nullable=True)

    Dokument = relationship('Dokument', uselist=False, back_populates='DokumentDataInfo')


class Delforloeb(Base):
    __tablename__ = 'Delforloeb'
    __table_args__ = (
        PrimaryKeyConstraint('ID', name='PK_Delforloeb'),
        {"schema": f"SbsysNet{ENV}.dbo"}
    )

    ID = Column(Integer, Identity(start=1, increment=1), primary_key=True)
