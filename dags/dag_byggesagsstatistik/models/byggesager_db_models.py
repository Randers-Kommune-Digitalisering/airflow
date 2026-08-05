# This file declares models for "byggesager" postgres database on kubernetes
from sqlalchemy import Column, DateTime, ForeignKeyConstraint, Integer, PrimaryKeyConstraint, Unicode
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base, declared_attr

Base = declarative_base()


class ByggesagBase(Base):
    __abstract__ = True

    id = Column(Integer, primary_key=True)
    byggesagskode_id = Column(Integer, nullable=False)
    beslutningstype_id = Column(Integer, nullable=True)
    byggetilladelse_date = Column(DateTime, nullable=True)
    received_date = Column(DateTime, nullable=False)

    @declared_attr
    def byggesagskode(cls):
        return relationship("Byggesagskode", lazy="joined")

    @declared_attr
    def beslutningstype(cls):
        return relationship("Beslutningstype", lazy="joined")


class ByggesagByg(ByggesagBase):
    __tablename__ = 'byggesag_byg'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='pk_byggesag_byg'),
        ForeignKeyConstraint(['byggesagskode_id'], ['byggesagskode.id'], name='FK_byggesag_byg_byggesagskode'),
        ForeignKeyConstraint(['beslutningstype_id'], ['beslutningstype.id'], name='FK_byggesag_byg_beslutningstype')
    )


class ByggesagSag(ByggesagBase):
    __tablename__ = 'byggesag_sag'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='pk_byggesag_sag'),
        ForeignKeyConstraint(['byggesagskode_id'], ['byggesagskode.id'], name='FK_byggesag_sag_byggesagskode'),
        ForeignKeyConstraint(['beslutningstype_id'], ['beslutningstype.id'], name='FK_byggesag_sag_beslutningstype')
    )


class Byggesagskode(Base):
    __tablename__ = 'byggesagskode'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='pk_byggesagskode'),
        ForeignKeyConstraint(['byggesagsgruppe_id'], ['byggesagsgruppe.id'], name='FK_byggesagskode_byggesagsgruppe')
    )

    id = Column(Integer, primary_key=True)
    byggesagsgruppe_id = Column(Integer, nullable=True)
    name = Column(Unicode(100), nullable=False)

    byggesagsgruppe = relationship("Byggesagsgruppe", back_populates="byggesagskoder", lazy="joined")
    byggesagskode_byg = relationship("ByggesagByg", back_populates="byggesagskode", lazy="joined")
    byggesagskode_sag = relationship("ByggesagSag", back_populates="byggesagskode", lazy="joined")


class Beslutningstype(Base):
    __tablename__ = 'beslutningstype'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='pk_beslutningstype'),
    )

    id = Column(Integer, primary_key=True)
    name = Column(Unicode(100), nullable=False)

    byggesager_byg = relationship("ByggesagByg", back_populates="beslutningstype")
    byggesager_sag = relationship("ByggesagSag", back_populates="beslutningstype")


class Byggesagsgruppe(Base):
    __tablename__ = 'byggesagsgruppe'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='pk_byggesagsgruppe'),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(Unicode(100), nullable=False, unique=True)

    byggesagskoder = relationship("Byggesagskode", back_populates="byggesagsgruppe")
