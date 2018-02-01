from models import QuandlDailyStockData, create_mysql_engine, QuandlPositiveReturn
from sqlalchemy.orm import sessionmaker
import sys
from sqlalchemy.exc import IntegrityError

engine = create_mysql_engine()

Session = sessionmaker(bind=engine)

session = Session()

def save_positive_returns(date, code='WIKI/PRICES', ticker='A', 
                          commit_to_db=False):

    daily_stock_data = session.query(QuandlDailyStockData).filter_by(
        code=code, date=date, ticker=ticker).first()

    if daily_stock_data:
        print(daily_stock_data)
        if commit_to_db:
            try:
                positive_return = QuandlPositiveReturn.create_from_quandl_daily_stock_data(
                    session=session, stock_data_obj=daily_stock_data)
            except IntegrityError:
                print('Data already in DB')
    else:
        print('No data for date {}.'.format(date))

if __name__ == '__main__':
    save_positive_returns('1999-11-17')