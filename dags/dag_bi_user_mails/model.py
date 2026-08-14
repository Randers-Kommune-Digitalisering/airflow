from sqlalchemy import String, Column, Integer, DateTime, Boolean
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class BiMailUser(Base):
    __tablename__ = "bi_user_mails"
    id = Column(Integer, primary_key=True, autoincrement=True)
    creation_date = Column(DateTime, nullable=False, default='CURRENT_TIMESTAMP')
    name = Column(String(100), nullable=False, default='')
    dq = Column(String(25), nullable=False, default='')
    email = Column(String(200), nullable=False, default='')
    user_group = Column(String(50), nullable=False, default='')
    email_sent = Column(Boolean, nullable=False, default=False)
    email_sent_date = Column(DateTime, nullable=True, default=None)
