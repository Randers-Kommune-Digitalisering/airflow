from sqlalchemy import Column, DateTime, Integer, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class NovaxHistory(Base):
    __tablename__ = 'novax_runs'
    id = Column(Integer, primary_key=True)
    ts = Column(DateTime, nullable=False)
    duration = Column(Integer, nullable=False)
    completed = Column(Boolean, nullable=False)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)
    records = relationship('NovaxRecord', back_populates='history')


class NovaxRecord(Base):
    __tablename__ = 'novax_records'
    id = Column(Integer, primary_key=True)
    nameid = Column(Integer, nullable=False)
    success = Column(Boolean, nullable=False)
    runid = Column(Integer, ForeignKey('novax_runs.id'))
    history = relationship('NovaxHistory', back_populates='records')
