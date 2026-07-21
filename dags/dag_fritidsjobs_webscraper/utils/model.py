from sqlalchemy import Column, DateTime, Integer, String, UniqueConstraint
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class Job(Base):
    __tablename__ = 'job'
    __table_args__ = (
        UniqueConstraint('title', 'url', name='uq_job_title_url'),
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    site = Column(String, nullable=False)
    title = Column(String, nullable=False)
    date = Column(DateTime, nullable=True, default=func.current_timestamp())
    url = Column(String, nullable=False)
