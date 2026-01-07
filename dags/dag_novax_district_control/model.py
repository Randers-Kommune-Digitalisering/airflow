from sqlalchemy import Column, DateTime, Integer, Boolean, ForeignKey, String
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from dataclasses import dataclass


Base = declarative_base()


class NovaxHistory(Base):
    __tablename__ = 'novax_journal_runs'
    id = Column(Integer, primary_key=True)
    ts = Column(DateTime, nullable=False)
    completed = Column(Boolean, nullable=False)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)
    records = relationship('NovaxRecord', back_populates='history')


class NovaxRecord(Base):
    __tablename__ = 'novax_journal_records'
    id = Column(Integer, primary_key=True)
    nameid = Column(String, nullable=False)
    success = Column(Boolean, nullable=False)
    runid = Column(Integer, ForeignKey('novax_journal_runs.id'))
    history = relationship('NovaxHistory', back_populates='records')
