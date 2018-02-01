from models import QuandlDailyStockData
from collections import defaultdict

def reformat_pandas_dataframe_column_names(pandas_dataframe, func):
    pandas_dataframe.rename(columns = lambda x: func(x), inplace=True)

def instantiate_quandl_daily_stock_data(pandas_dataframe, code):
    daily_stock_data_list = []
    pd_dict = pandas_dataframe.to_dict()

    for row in pandas_dataframe.iterrows():
        attrs = defaultdict(None)
        for column_name in QuandlDailyStockData.column_names():
            if column_name == 'code':
                attrs['code'] = code
            else:
                attrs[column_name] = row[1][column_name]
        daily_stock_data_list.append(QuandlDailyStockData(**attrs))
    return daily_stock_data_list
