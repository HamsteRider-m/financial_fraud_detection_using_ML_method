'''
@Author: Hanson Mei
@Date: 2019-09-06 13:31:58
@LastEditors: Hanson Mei
@LastEditTime: 2019-09-07 14:36:24
@Description: 
'''
# %%
import numpy as np
import re
import pickle
import tushare as ts
import pandas as pd
import os
import time
from common_func import timethis, split_list
from pandas.io import sql
from sqlalchemy import create_engine
import pymysql
import multiprocessing
from multiprocessing import Manager, Pool
from os.path import abspath, join, dirname
import sys
import logging
multiprocessing.set_start_method('spawn', True)
# sys.path.insert(0, abspath(dirname(__file__)))
ts.set_token('ca8024af746387189ec46e645a5374bbe72ba29e05544a6913589b66')
pro = ts.pro_api()
stkcd_ind = pickle.load(
    open('code_dict/stkcd_ind_tushare.pk', 'rb'))
# %%


def concat_fs(stkcd):
    ins = pro.query('income', ts_code=stkcd)
    try:
        ins.sort_values(by=['end_date', 'update_flag'],
                        ascending=False, inplace=True)
    except:
        ins.sort_values(by='end_date',
                        ascending=False, inplace=True)
        pass
    ins.drop_duplicates(subset='end_date', keep='first', inplace=True)
    bls = pro.query('balancesheet', ts_code=stkcd)
    try:
        bls.sort_values(by=['end_date', 'update_flag'],
                        ascending=False, inplace=True)
    except:
        bls.sort_values(by='end_date',
                        ascending=False, inplace=True)
        pass
    bls.drop_duplicates(subset='end_date', keep='first', inplace=True)
    cfs = pro.query('cashflow', ts_code=stkcd)
    try:
        cfs.sort_values(by=['end_date', 'update_flag'],
                        ascending=False, inplace=True)
    except:
        cfs.sort_values(by='end_date',
                        ascending=False, inplace=True)
        pass
    cfs.drop_duplicates(subset='end_date', keep='first', inplace=True)
    _ib = pd.merge(ins, bls, how='inner', on=['ts_code', 'end_date'])
    all = pd.merge(_ib, cfs, how='inner', on=['ts_code', 'end_date'])
    all.fillna(0)
    return all

# def check_dup(df, stkcd, i):
#     code = {1: 'income_statement', 2: 'balancesheet', 3: 'cashflow'}
#     dup_rest = df[df['end_date'].duplicated()]
#     _b = df.drop_duplicates(subset='end_date', keep=False)
#     _c = df.drop_duplicates(subset='end_date', keep='first')
#     dup_first = _b.append(_c).drop_duplicates(subset='end_date', keep=False)
#     dup_all = dup_rest.append(dup_first)
#     if dup_all.shape[0] > 10:
#         print('Bad data sets of {},{}'.format(stkcd, code[i]))
#         return (stkcd, code[i])


def concat_fi(stkcd):
    indic = pro.query('fina_indicator', ts_code=stkcd)
    try:
        indic.sort_values(by=['end_date', 'update_flag'],
                          ascending=False, inplace=True)
    except:
        indic.sort_values(by='end_date',
                          ascending=False, inplace=True)
        pass
    indic.drop_duplicates(subset='end_date', keep='first', inplace=True)
    return indic


def batch_concat(func, work, worker_id, t, finished):
    print('worker {} has started working'.format(worker_id))
    temp = pd.DataFrame([])
    for i in work:
        finished[i] = 0
        while True:
            try:
                temp = temp.append(func(i))
                a = len(finished)
                if a > 1 and a % 10 == 0:
                    consumed = time.time()-t
                    pct = a/3670
                    left = consumed/pct - consumed
                    print('{}%finished'.format(round(pct*100, 2)))
                    print('time consumed is {}s, time left is{}s'.format(
                        consumed, left))
                break
            except:
                time.sleep(2)
                continue
    print('worker {} has finished working'.format(worker_id))
    date_col = [i for i in temp if re.match('.*date.*', i)]
    for i in date_col:
        temp[i] = pd.to_datetime(temp[i])
    return temp


def main0():
    all = pd.DataFrame([])
    t = time.time()
    p = Pool(4)
    mgr = Manager()
    multiprocessing.log_to_stderr()
    logger = multiprocessing.get_logger()
    logger.setLevel(logging.INFO)
    # ns = mgr.Namespace()
    # ns.df = all
    finished = mgr.dict()
    stkcd_list = list(stkcd_ind.keys())
    work_loads = split_list(stkcd_list, 50)
    res = []
    for worker_id, work in enumerate(work_loads):
        t1 = time.time()
        res.append(p.apply_async(
            batch_concat, args=(concat_fs, work, worker_id, t, finished)))
    p.close()
    p.join()
    for i in res:
        all = all.append(i.get())
    all.to_csv('fin_data/tushare_data_fs.csv')
    engine = create_engine(
        "mysql+pymysql://root:19960227@127.0.0.1:3306/fin_data_ts")
    con = engine.connect()
    from sqlalchemy.types import CHAR, INT, FLOAT, DATETIME
    sql.to_sql(all, con=con, name='fin_data_all', if_exists='replace',
               dtype={'ts_code': CHAR(length=9)})
    # con.execute("SELECT * FROM fin_data_all").fetchone()


def main1():
    all = pd.DataFrame([])
    t = time.time()
    p = Pool(4)
    mgr = Manager()
    multiprocessing.log_to_stderr()
    logger = multiprocessing.get_logger()
    logger.setLevel(logging.INFO)
    # ns = mgr.Namespace()
    # ns.df = all
    finished = mgr.dict()
    stkcd_list = list(stkcd_ind.keys())
    work_loads = split_list(stkcd_list, 50)
    res = []
    for worker_id, work in enumerate(work_loads):
        t1 = time.time()
        res.append(p.apply_async(
            batch_concat, args=(concat_fi, work, worker_id, t, finished)))
    p.close()
    p.join()
    for i in res:
        all = all.append(i.get())
    all.to_csv('fin_data/tushare_data_fi.csv')
    engine = create_engine(
        "mysql+pymysql://root:19960227@127.0.0.1:3306/fin_data_ts")
    con = engine.connect()
    from sqlalchemy.types import CHAR, INT, FLOAT, DATETIME
    sql.to_sql(all, con=con, name='fin_data_fi', if_exists='replace',
               dtype={'ts_code': CHAR(length=9)})
    # con.execute("SELECT * FROM fin_data_fi").fetchone()


# %%
if __name__ == '__main__':
    # main0()
    main1()
