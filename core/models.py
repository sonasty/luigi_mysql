from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy import Column, Integer, String, Date, Float
from sqlalchemy import create_engine
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
import config

db_host = config.db_host
db_db_name = config.db_db_name
db_user = config.db_user
db_password = config.db_password

def create_mysql_engine():
    return create_engine(
        'mysql+mysqldb://{}:{}@{}/{}'.format(
            db_user,db_password,db_host,db_db_name), 
        pool_recycle=3600)

class QuandlBase(object):

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    @classmethod
    def column_names(cls):
        return cls.__table__.columns.keys()

    @classmethod
    def create_object(cls, session, **kwargs):
        obj = cls(**kwargs)
        session.add(obj)
        session.commit()

    code = Column(String(16), primary_key=True)
    ticker = Column(String(16), primary_key=True)
    date = Column(Date, primary_key=True)

QuandlBase = declarative_base(cls=QuandlBase)

class QuandlDailyStockData(QuandlBase):
    __tablename__ = 'quandl_daily_stock_data'

    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Integer)
    ex_dividend = Column(Float)
    split_ratio = Column(Float)
    adj_open = Column(Float)
    adj_high = Column(Float)
    adj_low = Column(Float)
    adj_close = Column(Float)
    adj_volume = Column(Integer)

    def __repr__(self):
        return "<StockData(code='%s', ticker='%s', date='%s', close='%s')>" % (
            self.code, self.ticker, self.date, self.close)

    @hybrid_property
    def daily_return(self):
        return self.close - self.open

class QuandlPositiveReturn(QuandlBase):
    __tablename__ = 'quandl_positive_returns'

    open = Column(Float)
    close = Column(Float)
    daily_return = Column(Float)

    @classmethod
    def create_from_quandl_daily_stock_data(cls, session, stock_data_obj):
        daily_return = stock_data_obj.daily_return

        if daily_return <= 0:
            print('Return must be positive. Data not inserted.')
        else:
            obj = cls(code=stock_data_obj.code, ticker=stock_data_obj.ticker, 
                date=stock_data_obj.date, open=stock_data_obj.open, 
                close=stock_data_obj.close, 
                daily_return=daily_return)
            session.add(obj)
            session.commit()
            print('INSERTED: Positive return')

    def __repr__(self):
        return "<Return(code='%s', ticker='%s', date='%s', close='%s')>" % (
            self.code, self.ticker, self.date, self.daily_return)