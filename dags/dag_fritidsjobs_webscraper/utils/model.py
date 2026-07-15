from sqlalchemy import Column, DateTime, Integer, String, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Job(Base):
    __tablename__ = 'job'
    __table_args__ = (
        UniqueConstraint('title', 'url', name='uq_job_title_url'),
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String, nullable=False)
    date = Column(DateTime, nullable=True)
    url = Column(String, nullable=False)
