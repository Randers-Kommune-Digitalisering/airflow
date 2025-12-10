from sqlalchemy import Column, DateTime, Integer, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class History(Base):
    __tablename__ = 'runs'
    id = Column(Integer, primary_key=True)
    ts = Column(DateTime, nullable=False)
    duration = Column(Integer, nullable=False)
    completed = Column(Boolean, nullable=False)
    records = relationship('Record', back_populates='history')


class Record(Base):
    __tablename__ = 'records'
    id = Column(Integer, primary_key=True)
    nameid = Column(Integer, nullable=False)
    success = Column(Boolean, nullable=False)
    runid = Column(Integer, ForeignKey('runs.id'))
    history = relationship('History', back_populates='records')
