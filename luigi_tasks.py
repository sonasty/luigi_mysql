import luigi
from luigi.contrib.mysqldb import MySqlTarget
from luigi.contrib import sqla
import datetime
import time
import config

from core.save_positive_returns import save_positive_returns
from core.download_quandl_table_to_db import download_quandl_table_to_db

db_host = config.db_host
db_db_name = config.db_db_name
db_user = config.db_user
db_password = config.db_password

class DownloadQuandlData(luigi.Task):

    __tablename__ = 'quandl_daily_stock_data'
    date = luigi.DateParameter()
    code = luigi.Parameter()
    ticker = luigi.Parameter()

    def requires(self):
        return []

    @property
    def update_id(self):
        return ':'.join([self.__tablename__,self.date.strftime('%Y-%m-%d'), self.code, self.ticker])

    def get_target(self):
        return MySqlTarget(host=db_host, database=db_db_name, user=db_user, 
            password=db_password, table=self.__tablename__, 
            update_id=self.update_id)

    def run(self):
        time.sleep(20)
        download_quandl_table_to_db(date=self.date.strftime('%Y-%m-%d'),
            code=self.code, ticker=self.ticker, commit_to_db=True)
        self.get_target().touch()

    def output(self):
        return self.get_target()

class SavePositiveReturn(luigi.Task):

    __tablename__ = 'quandl_positive_returns'
    date = luigi.DateParameter()
    code = luigi.Parameter()
    ticker = luigi.Parameter()

    def requires(self):
        return [DownloadQuandlData(self.date,self.code,self.ticker)]

    @property
    def update_id(self):
        return ':'.join([self.__tablename__,self.date.strftime('%Y-%m-%d'), self.code, self.ticker])

    def get_target(self):
        return MySqlTarget(host=db_host, database=db_db_name, user=db_user, 
            password=db_password, table=self.__tablename__, 
            update_id=self.update_id)

    def run(self):
        time.sleep(20)
        save_positive_returns(date=self.date.strftime('%Y-%m-%d'),
            code=self.code, ticker=self.ticker, commit_to_db=True)
        self.get_target().touch()

    def output(self):
        return self.get_target()

# if __name__ == '__main__':

#     #date = '1999-11-17'
#     date = datetime.date(2018,01,02)
#     code = 'WIKI/PRICES'
#     ticker = 'A'

#     task1, task2 = SavePositiveReturn(date,code,ticker), DownloadQuandlData(date,code,ticker)
#     luigi.build([task1, task2], local_scheduler=True)