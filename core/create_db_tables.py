from core.models import QuandlBase, create_mysql_engine

if __name__ == '__main__':

    engine = create_mysql_engine()
    #QuandlBase.metadata.create_all(engine)
    #QuandlBase.metadata.tables["quandl_positive_returns"].create(bind = engine)
