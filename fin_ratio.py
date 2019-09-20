'''
@Author: Hanson Mei
@Date: 2019-08-24 12:19:43
@LastEditors: Hanson Mei
@LastEditTime: 2019-09-04 12:18:30
@Description:
'''
# %%
import pandas as pd
import numpy as np
import re
import time
import math
import pickle
import tushare as ts
import sqlalchemy
from sqlalchemy import create_engine
import datetime
# fin_data = pd.read_csv('processed_fin_data/NETEASE_merged_data.csv',
#                        na_values=['--', '-'], infer_datetime_format=True,
#                        converters={'stkcd': str}, parse_dates=True,
#                        dayfirst=True)
ind_stkcd = pickle.load(
    open('code_dict/ind_stkcd_tushare.pk', 'rb'))  # 行业: [stkcds...]
stkcd_ind = pickle.load(
    open('code_dict/stkcd_ind_tushare.pk', 'rb'))  # stkcd: 行业
stkcd_trans = pickle.load(
    open('code_dict/stkcd_trans.pk', 'rb'))  # stkcd: sname
labeled_data = pd.read_excel(
    'label_data/pop_data1.xlsx', header=0,
    converters={'ts_code': str, 'end_date': pd.to_datetime},)
labeled_data = labeled_data[labeled_data.end_date.map(lambda x: x.month) == 12]
labeled_data.reset_index(inplace=True)
ts.set_token('ca8024af746387189ec46e645a5374bbe72ba29e05544a6913589b66')
pro = ts.pro_api()
engine = create_engine(
    "mysql+pymysql://root:19960227@127.0.0.1:3306/fin_data_ts")
con = engine.connect()


# %%
def check_date(date):
    '''
    Check whether the date given matches the requirement of query syntax
    '''
    if isinstance(date, str):
        if re.match('\d{4}\-\d{2}\-\d{2}', date) or re.match('\d{8}', date):
            return date
        else:
            raise ValueError("wrong time format input")
    elif isinstance(date, pd.datetime):
        date = str(date)[:10]
        return date
    else:
        raise TypeError(
            "expected string or timestamp format, {} given".format(type(date)))


'''Singular ratio without referring to ratio function pro-created'''


def sql_query(table, ts_code, fields, period=None, end_date=None):
    '''
    fields: financial account/ financial indicator names\n
    table: fin_data_all/fin_data_fi\n
    ts_code: stock code in the format like '000001.SZ'\n
    period: datetime format like '2018-12-31'\n
    return: value fetched from database
    '''
    if period:
        period = check_date(period)
    elif end_date:
        end_date = check_date(end_date)
    else:
        raise ValueError("Cannot input period and end_date at the same time")

    sql = "select {} from {} where ts_code='{}' and end_date='{}'"
    if end_date:
        sql = "select {} from {} where ts_code='{}' and end_date<='{}'"
        return pd.DataFrame(con.execute(
            sql.format(fields, table, ts_code, end_date)).fetchall(),
            columns=fields.split(', '))
    return con.execute(sql.format(fields, table, ts_code, period)).fetchall()[0][0]


def to_ta(stkcd, period):
    '''
    Total operating expense/ total assets
    '''
    try:
        def oper_exp(acc):
            to = sql_query(table='fin_data_all',
                           ts_code=stkcd,
                           period=period,
                           fields=acc)
            return to
        to = oper_exp('oper_cost')
        if to is None or to == 0:
            to = oper_exp('oper_exp')
        ta = sql_query(table='fin_data_all',
                       ts_code=stkcd,
                       period=period,
                       fields='total_assets')
        return round(to/ta, 4)
    except:
        return None


def wc_ta(stkcd, period):
    try:
        wc = sql_query('fin_data_fi', stkcd,
                       period, fields='working_capital')
        if wc:
            ta = sql_query('fin_data_all', stkcd,
                           period, fields='total_assets')
            return round(wc/ta, 4)
        else:
            return None
    except:
        return None


def re_ta(stkcd, period):
    try:
        re = sql_query('fin_data_fi', stkcd,
                       period, fields='retained_earnings')
        ta = sql_query('fin_data_all', stkcd,
                       period, fields='total_assets')
        return round(re/ta, 4)
    except:
        return None


def ebit_ta(stkcd, period):
    try:
        ebit = sql_query('fin_data_fi', stkcd,
                         period, fields='ebit')
        ta = sql_query('fin_data_all', stkcd,
                       period, fields='total_assets')
        return round(ebit/ta, 4)
    except:
        return None


def emv_tl(stkcd, period):
    try:
        publish_date = sql_query(
            'fin_data_all', stkcd, period, fields='f_ann_date')
        a = 0
        while True:
            try:
                date = re.sub('-', '', str(publish_date)[:10])
                emv = pro.query('daily_basic', ts_code=stkcd,
                                trade_date=date,
                                fields='total_mv').iloc[0][0]
                break
            except:
                a += 5
                if a < 61:
                    publish_date = publish_date - datetime.timedelta(days=5)
                    continue
                else:
                    return 0
        tl = sql_query('fin_data_all', stkcd,
                       period, fields='total_liab')
        return round(emv/tl, 4)
    except Exception as e:
        print(e)
        return None


def s_ta(stkcd, period):
    try:
        s = sql_query('fin_data_all', stkcd,
                      period, fields='revenue')
        ta = sql_query('fin_data_all', stkcd,
                       period, fields='total_assets')
        return round(s/ta, 4)
    except:
        return None


def s_gr(stkcd, period):
    try:
        return sql_query('fin_data_fi', stkcd,
                         period, fields='tr_yoy')
    except:
        return None


def inv_turn(stkcd, period):
    try:
        return sql_query('fin_data_fi', stkcd,
                         period, fields='inv_turn')
    except:
        return None


def inv_ta(stkcd, period):
    try:
        inv = sql_query('fin_data_all', stkcd,
                        period, fields='inventories')
        ta = sql_query('fin_data_all', stkcd,
                       period, fields='total_assets')
        return round(inv/ta, 4)
    except:
        return None


def gp_ta(stkcd, period):
    try:
        gp = sql_query('fin_data_all', stkcd,
                       period, fields='inventories')
        ta = sql_query('fin_data_fi', stkcd,
                       period, fields='gross_margin')
        return round(gp/ta, 4)
    except:
        return None


def gm11(stkcd, period):
    try:
        publish_date = sql_query(
            'fin_data_all', stkcd, period, fields='f_ann_date')
        a = 0
        while True:
            try:
                date1 = re.sub('-', '', str(publish_date)[:10])
                emv1 = pro.query('daily_basic', ts_code=stkcd,
                                 trade_date=date1,
                                 fields='total_mv').iloc[0][0]
                previous_yr_date = publish_date - datetime.timedelta(days=365)
                date0 = re.sub('-', '', str(previous_yr_date)[:10])
                emv0 = pro.query('daily_basic', ts_code=stkcd,
                                 trade_date=date0,
                                 fields='total_mv').iloc[0][0]
                return 1 if emv1/emv0 > 1.1 else 0
            except:
                a += 17
                if a < 70:
                    publish_date = publish_date - datetime.timedelta(days=17)
                    continue
                else:
                    return None
    except Exception as e:
        print(e)
        return None


def debt_eqt(stkcd, period):
    try:
        return sql_query('fin_data_fi', stkcd,
                         period, fields='debt_eqt')
    except:
        return None


def log_debt(stkcd, period):
    try:
        debt = sql_query('fin_data_all', stkcd,
                         period, fields='total_liab')
        return math.log(debt)
    except:
        return None


def tl_ta(stkcd, period):
    try:
        tl = sql_query('fin_data_all', stkcd,
                       period, fields='total_liab')
        ta = sql_query('fin_data_all', stkcd,
                       period, fields='total_assets')
        return round(tl/ta, 4)
    except:
        return None


def log_asset(stkcd, period):
    try:
        ta = sql_query('fin_data_all', stkcd,
                       period, fields='total_assets')
        return math.log(ta)
    except:
        return None


def ar_turn(stkcd, period):
    try:
        return sql_query('fin_data_fi', stkcd,
                         period, fields='ar_turn')
    except:
        return None


def fa_eqt(stkcd, period):
    try:
        fa = sql_query('fin_data_all', stkcd,
                       period, fields='total_nca')
        eqt = sql_query('fin_data_all', stkcd,
                        period, fields='total_hldr_eqy_inc_min_int')
        return round(fa/eqt, 4)
    except:
        return None


def ca_cl(stkcd, period):
    try:
        ca = sql_query('fin_data_all', stkcd,
                       period, fields='total_cur_assets')
        cl = sql_query('fin_data_all', stkcd,
                       period, fields='total_cur_liab')
        return round(ca/cl, 4)
    except:
        return None


def cur_ratio(stkcd, period):
    try:
        return sql_query('fin_data_fi', stkcd,
                         period, fields='current_ratio')
    except:
        return None


def conse_loss(stkcd, period):
    try:
        a = 0
        date_0 = re.sub('-', '', str(period)[:10])
        date_1 = str(int(date_0)-10000)
        rev_1 = sql_query('fin_data_all', ts_code=stkcd,
                          period=date_1,
                          fields='n_income')
        if rev_1 > 0:
            return 0
        date_2 = str(int(date_1)-10000)
        rev_2 = sql_query('fin_data_all', ts_code=stkcd,
                          period=date_2,
                          fields='n_income')
        return 1 if rev_2 < 0 else 0
    except Exception as e:
        print(e)
        return None


def gw_na(stkcd, period):
    try:
        gw = sql_query('fin_data_all', stkcd,
                       period, fields='goodwill')
        eqt = sql_query('fin_data_all', stkcd,
                        period, fields='total_hldr_eqy_inc_min_int')
        return round(gw/eqt, 4)
    except:
        return None


def agg_fcff(stkcd, period):
    try:
        all = sql_query('fin_data_fi', ts_code=stkcd,
                        end_date=period, fields='end_date, fcff').sort_values(by='end_date', ascending=False)
        fcff = all[all.end_date.map(lambda x: x.month) == 12].iloc[:5]
        # con = pd.merge(fcff, rev_a, on='end_date')
        # con = con[con.notna()]
        # con['ratio'] = con.apply(
        #     lambda row: row['fcff']/row['revenue'], axis=1)
        return fcff[fcff.fcff.notna()].fcff.sum()
    except Exception as e:
        print(e)
        return None


def fin_exp_tl(stkcd, period):
    try:
        # period0 = str(period.year-1) + str(period.month) + str(period.day)
        fin_exp = sql_query('fin_data_all', stkcd,
                            period, fields='fin_exp')
        tl = sql_query('fin_data_all', stkcd,
                       period, fields='total_liab')
        return round(fin_exp/tl, 4)
    except Exception as e:
        print(e)
        return None


def np_margin(stkcd, period):
    try:
        # period0 = str(period.year-1) + str(period.month) + str(period.day)
        fin_exp = sql_query('fin_data_fi', stkcd,
                            period, fields='fin_exp')
        tl = sql_query('fin_data_all', stkcd,
                       period, fields='total_liab')
        return round(fin_exp/tl, 4)
    except Exception as e:
        print(e)
        return None

func_dic = {
    'to_ta': to_ta,
    'wc_ta': wc_ta,
    're_ta': re_ta,
    'ebit_ta': ebit_ta,
    'emv_tl': emv_tl,
    's_ta': s_ta,
    's_gr': s_gr,
    'inv_turn': inv_turn,
    'inv_ta': inv_ta,
    'gp_ta': gp_ta,
    'gm11': gm11,
    'debt_eqt': debt_eqt,
    'log_debt': log_debt,
    'tl_ta': tl_ta,
    'log_asset': log_asset,
    'ar_turn': ar_turn,
    'fa_eqt': fa_eqt,
    'ca_cl': ca_cl,
    'cur_ratio': cur_ratio,
    'conse_loss': conse_loss,
    'gw_na': gw_na,
    'agg_fcff': agg_fcff,
    'np_margin': np_margin

}

name_dic = {
    'to_ta': 'operexp_to_ta',
    'wc_ta': 'wc_to_ta',
    're_ta': 're_to_ta',
    'ebit_ta': 'ebit_to_ta',
    'emv_tl': 'MVofequity_to_tl',
    's_ta': 'sales_to_ta',
    's_gr': 'revenue_gr_yoy',
    'inv_turn': 'inv_turnover',
    'inv_ta': 'inv_to_ta',
    'altman_z': 'altman_z',
    'gp_ta': 'gp_to_ta',
}

''' integrate ratio with referring to ratios above'''


def altman_z(stkcd, period):
    try:
        ratio = []
        for _ in ['wc_ta', 're_ta', 'ebit_ta', 'emv_tl', 's_ta']:
            ratio.append(func_dic[_](period=period, stkcd=stkcd))
        return (1.2*ratio[0] + 1.4*ratio[1] + 3.3*ratio[2] + 0.6*ratio[3] + 1.0*ratio[4])
    except:
        return None


func_dic['altman_z'] = altman_z


def ind_median(stkcd, ratio, period, return_value='abs',
               descending=False, table='fin_data_fi'):
    '''
    """some ratios are only comparable
    from the industry level view.
    here we have three types of
    returned values as shown below:"""
    stkcd,year: str
    period: formatted as 20181231
    ratio: str -> for details, see https://tushare.pro/document/2?doc_id=16
    return_value: string, any of ['abs', 'quantile', 'rel_val']
    descending: bool, default False
    '''

    # if re.match('\d{6}\.\w{2}', str(stkcd)):
    #     _stkcd = stkcd
    # elif re.match('\d{6}', str(stkcd)):
    #     try:
    #         _stkcd = stkcd_trans[str(stkcd)]
    #     except:
    #         raise NameError('The input stock code is not found')
    # else:
    #     raise NameError(
    #         "unsupported input format:\nthe input string must be like either '000001' or '000001.SZ'")
    try:
        if ratio in name_dic:
            # the ratio figure of the queried company
            ratio_f = func_dic[ratio]
            score = ratio_f(stkcd, period)
        else:
            score = sql_query(table, ts_code=stkcd,
                              period=period, fields=ratio)[0][0]
    except:
        raise ValueError("ratio not found")

    if return_value == 'abs':
        return score

    else:
        if ratio in name_dic:  # pro-created ratio
            def get_ind_res(stkcd, period, ratio,
                            descend=descending):
                try:
                    _ind = stkcd_ind[stkcd]
                    _ind_code = ind_stkcd[_ind]
                    res = []
                    for code in _ind_code:
                        try:
                            res.append(func_dic[ratio](
                                period=period, stkcd=code))
                        except:  # nothing will be returned out of the func if error raised
                            continue  # the for loop will continue despite the error
                    res = [i for i in res if i is not None]
                    res.sort(reverse=descend)
                    return res
                except KeyError:
                    return None
            res = get_ind_res(stkcd=stkcd,
                              period=period,
                              ratio=ratio, descend=descending)
        else:  # ratio from tushare
            def get_ind_res(stkcd, period, ratio,
                            descend=descending):
                _ind = stkcd_ind[stkcd]
                _ind_code = ind_stkcd[_ind]
                res = []
                for code in _ind_code:
                    try:
                        res.append(sql_query(table, ts_code=code,
                                             period=period, fields=ratio)[0][0])
                    except:  # nothing will be returned out of the func if error raised
                        continue  # the for loop will continue despite the error
                res = [i for i in res if i is not None]
                res.sort(reverse=descend)
                return res
            res = get_ind_res(stkcd=stkcd, period=period,
                              ratio=ratio, descend=descending)
        if return_value == 'quantile':
            try:
                quantile = (res.index(score)+1)/len(res)
                return round(quantile, 4)
            except:
                return None
        elif return_value == 'rel_val':
            try:
                median = np.median(res)
                return round(score/median, 4)
            except:
                return None
        else:
            raise NameError('unsupported return_value type')


''' industry level ratios'''


def to_ta_m(stkcd, period, rtn_val, desc=False):
    return ind_median(stkcd=stkcd, ratio='to_ta',
                      period=period, return_value=rtn_val,
                      descending=desc)


def np_margin_m(stkcd, period, rtn_val, desc=False):
    return ind_median(stkcd=stkcd, ratio='netprofit_margin',
                      period=period, return_value=rtn_val,
                      descending=desc)


def apply_ratio(func, args=None):
    '''
    apply one above function to a dataframe column
    '''
    if args:
        labeled = labeled_data.apply(lambda x: func(x['ts_code'],
                                                    x['end_date'],
                                                    rtn_val=args), axis=1)
    else:
        labeled = labeled_data.apply(lambda x: func(x['ts_code'],
                                                    x['end_date']), axis=1)
    return labeled


# %%
if __name__ == '__main__':
    # for name, group in labeled_data.groupby('fraud'):
    #     if name == 1:
    #     # labeled_data['to_ta'] = apply_ratio(to_ta)
    #     # labeled_data['to_ta_m'] = apply_ratio(to_ta_m, args='quantile')
    #     # labeled_data['emv_tl'] = apply_ratio(emv_tl)
    #     # labeled_data['emv_tl'] = apply_ratio(gm11)
    #     # labeled_data['emv_tl'] = apply_ratio(altman_z)
    #     # labeled_data['conse_loss'] = apply_ratio(conse_loss)
    #     # labeled_data['agg_fcff_rev'] = apply_ratio(agg_fcff_rev)
    #     # labeled_data['fin_exp_tl'] = apply_ratio(fin_exp_tl)
    labeled_data['agg_fcff'] = apply_ratio(agg_fcff)
    con.close()


# %%
