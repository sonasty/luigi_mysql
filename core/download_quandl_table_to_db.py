from models import QuandlDailyStockData, create_mysql_engine

from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

from utils import slugify
import quandl
import sys
from quandl_utils import (reformat_pandas_dataframe_column_names, 
    instantiate_quandl_daily_stock_data)

import config


engine = create_mysql_engine()

Session = sessionmaker(bind=engine)

session = Session()

quandl.ApiConfig.api_key = config.quandl_api_key

# code = 'WIKI/PRICES'
# date = '1999-11-18'
# ticker = 'A'

def download_quandl_table_to_db(date, code='WIKI/PRICES', ticker='A', 
                                commit_to_db=False):
    
    quandl_table = quandl.get_table(code, date=date, 
        ticker=ticker)

    reformat_pandas_dataframe_column_names(quandl_table, slugify)

    daily_stock_data_list = instantiate_quandl_daily_stock_data(
        quandl_table, code)

    if daily_stock_data_list:
        print(daily_stock_data_list)
        if commit_to_db:
            try:
                session.add_all(daily_stock_data_list)
                session.commit()
                print('INSERTED: Stock data.')
            except IntegrityError:
                print('Data already in DB')
    else:
        print('No data for date {}.'.format(date))

if __name__ == '__main__':
    download_quandl_table_to_db(date = '2018-01-03', commit_to_db=False)
