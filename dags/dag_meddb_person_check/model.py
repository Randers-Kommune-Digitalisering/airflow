from sqlalchemy import Column, String, Integer, Unicode, Boolean, ForeignKey, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class PersonSkoleAD(Base):
    __tablename__ = 'person'
    __table_args__ = {'schema': 'skolead'}

    Skole = Column(String(128), nullable=False)
    BrugerNavn = Column(String(50), primary_key=True)
    Navn = Column(String(50), nullable=False)
    Mail = Column(String(50), unique=True, nullable=True)
    DQnummer = Column(String(50), nullable=True)
    updated = Column(String(50), nullable=True)


class PersonMedDB(Base):
    __tablename__ = "person"
    __table_args__ = {"schema": 'meddb'}

    id = Column(Integer, primary_key=True)
    username = Column(Unicode(100), nullable=True)
    name = Column(Unicode(255), nullable=False)
    email = Column(Unicode(255), unique=True, nullable=False)
    organization = Column(Unicode(100), nullable=True)
    found_in_system = Column(Boolean, default=False)


class CommitteeMembership(Base):
    __tablename__ = "committee_membership"
    __table_args__ = (PrimaryKeyConstraint("person_id", "role_id", "committee_id"), {"schema": 'meddb'})

    person_id = Column(Integer, ForeignKey('meddb.person.id'), nullable=False)
    role_id = Column(Integer, ForeignKey('meddb.role.id'), nullable=False)
    committee_id = Column(Integer, ForeignKey('meddb.committee.id'), nullable=False)
