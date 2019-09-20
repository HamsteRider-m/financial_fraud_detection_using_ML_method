# %%
import copy
import pandas as pd
import json
import requests
'''
url = 'http://datainterface3.eastmoney.com/EM_DataCenter_V3/api/JJSJTJ/GetJJSJTJ?tkn=eastmoney&ReportDate={}-{}&code=&type={}&zjc=0&sortField=Count&sortDirec=1&pageNum={}&pageSize=300&cfg=jjsjtj'
fund_type = {1: '基金持股', 2: 'QFII持股',
             3: '社保持股', 4: '券商持股', 5: '保险持股', 6: '信托持股'}
li = []
for fund in fund_type:
    for year in range(2009, 2020):
        for day in ['03-31', '06-30', '09-30', '12-31']:
            i = 0
            while True:
                i += 1
                try:
                    response = requests.get(url.format(year, day, fund, i))
                    lo = [(i+'|'+fund_type[fund]).split('|')
                          for i in response.json()['Data'][0]['Data']]
                    if len(lo) > 0:
                        li += lo
                        continue
                    else:
                        break
                except Exception as e:
                    print(e)
                    break

column_names = "SCode,SName,RDate,LXDM,LX,Count,CGChange,ShareHDNum,VPosition,TabRate,LTZB,ShareHDNumChange,RateChange,fund_type".split(
    ',')
df = pd.DataFrame(li, columns=column_names)
df.to_csv('processed_fin_data/重要基金持股.csv')
'''
# %%
df = pd.read_csv('processed_fin_data/重要基金持股.csv', converters={
                 'SCode': str, 'RDate': pd.to_datetime, 'Count': pd.to_numeric})
# %% 获取各类基金持股结果较好的股票日期组合
df_qs = df[(df.LXDM == '券商') & (df.Count > 15) & (df.CGChange == '增持')]
df_bx = df[(df.LXDM == '保险') & (df.Count > 2) & (df.CGChange == '增持')]
df_jj = df[(df.LXDM == '基金') & (df.Count > 200) & (df.CGChange == '增持')]
df_sb = df[(df.LXDM == '社保') & (df.Count > 2) & (df.CGChange == '增持')]
df_QFII = df[(df.LXDM == 'QFII') & (df.Count > 1) & (df.CGChange == '增持')]

# %%输出以上组合的重合部分，即多种基金持有的股票
li_solid = [i for _ in [df_qs, df_bx, df_jj, df_sb, df_QFII]
            for i in _[['SCode', 'RDate']].values.tolist()]
li_solid_copy = copy.deepcopy(li_solid)
li_solid_unique = [list(a) for a in set(tuple(i) for i in li_solid)]
for i in li_solid_unique:
    li_solid_copy.remove(i)
fraud_free_set = set(tuple(i) for i in li_solid_copy)
df_fraud_free = pd.DataFrame(fraud_free_set, columns=['SCode', 'RDate'])
df_fraud_free.to_csv('label_data/df_fraud_free.csv')
pd.merge(df_fraud_free, df,on=['SCode','RDate'])
# %%
