from sqlalchemy import CHAR, Column, Integer, DateTime, BOOLEAN
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class BiMailUser(Base):
    __tablename__ = "bi_user_mails"
    id = Column(Integer, primary_key=True, autoincrement=True)
    creation_date = Column(DateTime, nullable=False, default='CURRENT_TIMESTAMP')
    name = Column(CHAR(100), nullable=False, default='')
    dq = Column(CHAR(25), nullable=False, default='')
    email = Column(CHAR(200), nullable=False, default='')
    user_group = Column(CHAR(50), nullable=False, default='')
    email_sent = Column(BOOLEAN, nullable=False, default=False)
    email_sent_date = Column(DateTime, nullable=True, default=None)
