from sqlalchemy import Column, DateTime, Integer, String, Float, ForeignKey, Boolean, Table
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


user_department = Table(
    'user_department', Base.metadata,
    Column('user_id', Integer, ForeignKey('user.user_id')),
    Column('department_id', Integer, ForeignKey('department.department_id'))
)


class Department(Base):
    __tablename__ = 'department'
    department_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
    ean = Column(String)
    users = relationship('User', secondary=user_department, back_populates='departments')


class User(Base):
    __tablename__ = 'user'
    user_id = Column(Integer, primary_key=True, autoincrement=True)
    full_name = Column(String, nullable=False)
    primary_user = Column(String, nullable=False)
    departments = relationship('Department', secondary=user_department, back_populates='users')
    computers = relationship('Computer', back_populates='user')


class Computer(Base):
    __tablename__ = 'computer'
    unit_name = Column(String, primary_key=True)
    producent = Column(String)
    model = Column(String)
    device_type = Column(String)
    serial_number = Column(String)
    last_login_date = Column(DateTime)
    last_run = Column(DateTime)
    bitlocker_code = Column(String)
    bitlocker_status = Column(String)
    bitlocker_encryption_percentage = Column(String)
    os_version = Column(String)
    mac_address = Column(String)
    lan_mac_address = Column(String)
    device_license = Column(Boolean, default=False)
    drift = Column(Boolean, default=False)
    price = Column(Float)
    order_date = Column(DateTime)
    warranty = Column(DateTime)
    kob_ean_nr = Column(String)
    user_id = Column(Integer, ForeignKey('user.user_id'))
    user = relationship('User', back_populates='computers')
