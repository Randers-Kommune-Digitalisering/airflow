# from utils.config import POSTGRES_USER, POSTGRES_PASS, POSTGRES_HOST, POSTGRES_DB
# from utils.database import DatabaseClient
# from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey
# from sqlalchemy.orm import relationship
# from sqlalchemy.ext.declarative import declarative_base
# from utils.logging import logging


# Base = declarative_base()
# logger = logging.getLogger(__name__)


# class History(Base):
#     __tablename__ = 'runs'
#     id = Column(Integer, primary_key=True)
#     ts = Column(DateTime, nullable=False)
#     duration = Column(Integer, nullable=False)
#     completed = Column(Boolean, nullable=False)
#     records = relationship('Record', back_populates='history')


# class Record(Base):
#     __tablename__ = 'records'
#     id = Column(Integer, primary_key=True)
#     nameid = Column(Integer, nullable=False)
#     success = Column(Boolean, nullable=False)
#     runid = Column(Integer, ForeignKey('runs.id'))
#     history = relationship('History', back_populates='records')


# def create_db_client():
#     """
#     Factory function to create a DatabaseClient instance.
#     """
#     db_client = get_db_client()
#     engine = db_client.engine
#     try:
#         Base.metadata.create_all(engine)
#     except Exception as e:
#         logger.error(f"Error creating tables or columns: {e}")
#     return db_client


# def get_db_client():
#     return DatabaseClient(
#         db_type='postgresql',
#         database=POSTGRES_DB,
#         username=POSTGRES_USER,
#         password=POSTGRES_PASS,
#         host=POSTGRES_HOST,
#         port='5432'
#     )
