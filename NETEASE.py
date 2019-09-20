import re
import pickle
import pandas as pd
import time

# %%


def concat():
    df_p1 = pd.read_csv('fin_data/FS_data.csv', encoding='utf-16',
                        delimiter='\t', converters={'stkcd': str}, na_values=['--', '-'])
    df_p2 = pd.read_csv('fin_data/FS_data2.csv', encoding='utf-16',
                        delimiter='\t', converters={'stkcd': str}, na_values=['--', '-'])
    df_p3 = pd.read_csv('fin_data/FS_data3.csv', encoding='utf-16',
                        delimiter='\t', converters={'stkcd': str}, na_values=['--', '-'])
    df_all = pd.concat([df_p1, df_p2, df_p3])
    return df_all


df_all = concat()
df_all.reset_index(inplace=True, drop=True)
# %%
'''
gr1 = df_all.groupby(['stkcd', 'period'])
df_dict = dict()
i = 0
for name, group in gr1:
    i += 1
    if time == 2:
        start = time.time()
    df_dict[name] = dict(group[['account', 'balance']].values)
    if i % 50 == 0:
        pct = i*100/146794
        consumed = (time.time() - start)/60
        print("进度为{}%，已耗时{}mins，预计剩余时间{}mins"
              .format(round(pct, 2),
                      round(consumed, 2),
                      round(consumed/(pct/100)-consumed, 2)))
df = pd.DataFrame(df_dict).T
'''
# alternatively:
df = df_all.pivot(index=['stkcd, period'], columns='account', values='balance')
# %%

df.to_csv('fin_data/FS_data_all.csv')
df.pivot()
# %%

df_dict1 = dict()
no_match3 = []

try:
    for i in set(no_match2):
        if re.search(i, b) is not None:
            mat = re.findall(r'\|([^|]*{}[^|]+)\|'.format(i[3:5]), b)
            if mat is not None:
                df_dict1[i] = mat
            else:
                no_match3.append(i)
        else:
            no_match3.append(i)
except:
    pass
