import tushare as ts
import re
import pickle
import pandas as pd
import time
import pandas_datareader as pdr
from pandas_datareader import data, wb

# %%

pro = ts.pro_api()
df = pro.income(ts_code='600000.SH', start_date='20180101', end_date='20180730', fields='ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,basic_eps,diluted_eps')