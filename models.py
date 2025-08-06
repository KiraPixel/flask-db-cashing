from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, JSON, ForeignKey, Float, Boolean, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
import config

Base = declarative_base()


# Определение моделей
class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    username = Column(String(100), unique=True, nullable=False)
    password = Column(String(100), nullable=False)
    role = Column(Integer, nullable=False)
    last_activity = Column(DateTime, nullable=False)
    email = Column(String, nullable=False)

    def __repr__(self):
        return f'<User {self.username}>'


class Transport(Base):
    __tablename__ = 'transport'
    id = Column(Integer, primary_key=True)
    storage_id = Column(Integer, ForeignKey('storage.ID'), nullable=False)
    model_id = Column(Integer, ForeignKey('transport_model.id'), nullable=False)
    uNumber = Column(Text)
    vin = Column(Text)
    equipment = Column(JSON)
    storage = relationship('Storage', back_populates='transports')
    transport_model = relationship('TransportModel', back_populates='transports')

    def __repr__(self):
        return f'<Transport {self.uNumber}>'


class Storage(Base):
    __tablename__ = 'storage'
    ID = Column(Integer, primary_key=True)
    name = Column(String(100))
    type = Column(String(100))
    region = Column(String(100))
    address = Column(String(100))
    organization = Column(String(100))
    transports = relationship('Transport', back_populates='storage')

    def __repr__(self):
        return f'<Storage {self.name}>'


class TransportModel(Base):
    __tablename__ = 'transport_model'
    id = Column(Integer, primary_key=True)
    type = Column(String(100))
    name = Column(String(100))
    lift_type = Column(String(100))
    engine = Column(String(100))
    transports = relationship('Transport', back_populates='transport_model')

    def __repr__(self):
        return f'<TransportModel {self.name}>'


class CashCesar(Base):
    __tablename__ = 'cash_cesar'
    unit_id = Column(Integer, primary_key=True)
    object_name = Column(Text, nullable=False)
    pin = Column(Integer, default=0)
    vin = Column(Text, nullable=False)
    last_time = Column(Integer, default=0)
    pos_x = Column(Float, default=0.0)
    pos_y = Column(Float, default=0.0)
    created_at = Column(Integer, default=0)
    device_type = Column(Text, nullable=False)
    linked = Column(Boolean, nullable=True, default=False)

    __table_args__ = (
        Index('idx_cash_cesar_object_name', 'object_name'),
    )


class CashWialon(Base):
    __tablename__ = 'cash_wialon'
    id = Column(Integer, primary_key=True)
    uid = Column(Integer, nullable=False, default=0)
    nm = Column(Text, nullable=False)
    pos_x = Column(Float, default=0.0)
    pos_y = Column(Float, default=0.0)
    gps = Column(Integer, default=0)
    last_time = Column(Integer, default=0)
    last_pos_time = Column(Integer, default=0)
    linked = Column(Boolean, nullable=True, default=False)
    cmd = Column(Text, nullable=True, default='')
    sens = Column(Text, nullable=True, default='')
    valid_nav = Column(Integer, nullable=True, default=1)

    __table_args__ = (
        Index('idx_cash_wialon_nm', 'nm'),
    )


class CashHistoryWialon(Base):
    __tablename__ = 'cash_history_wialon'
    id = Column(Integer, primary_key=True)
    uid = Column(Integer, nullable=False, default=0)
    nm = Column(Text, nullable=False)
    pos_x = Column(Float, default=0.0)
    pos_y = Column(Float, default=0.0)
    last_time = Column(Integer, default=0)


class SystemSettings(Base):
    __tablename__ = 'system_settings'
    id = Column(Integer, primary_key=True)
    enable_voperator = Column(Integer)
    enable_xml_parser = Column(Integer)
    enable_db_cashing = Column(Integer)


# Индекс для поля uNumber в Transport
Index('idx_transport_unumber', Transport.uNumber)


def create_db():
    engine = create_engine(config.SQLALCHEMY_DATABASE_URL)
    Base.metadata.create_all(engine)
    return engine


def create_session(engine):
    Session = sessionmaker(bind=engine)
    return Session()
