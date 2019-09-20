import re
import pickle
import pandas as pd
# %%
with open('fin_data/FS_Combas[DES][csv].txt', 'r') as f:
    FS_Combas = f.readlines()
with open('fin_data/FS_Comins[DES][csv].txt', 'r') as f:
    FS_Comins = f.readlines()
with open('fin_data/FS_Comscfi[DES][csv].txt', 'r') as f:
    FS_Comscfi = f.readlines()


# %%
def get_dict(file):
    _dict = dict()
    for line in file:
        code = re.search('^(\w+)\s', line.strip())
        name = re.search('\[(.*)\]\s', line.strip())
        if code is not None and name is not None:
            _dict[code.group(1)] = name.group(1)
    return _dict


# %%
bs_dict = get_dict(FS_Combas)
is_dict = get_dict(FS_Comins)
cs_dict = get_dict(FS_Comscfi)
code2name_dict = {**bs_dict, **is_dict, **cs_dict}


def pd_read(file_path, data_dict):
    df = pd.read_csv(file_path, delimiter='\t', dayfirst=True,
                     converters={'Stkcd': str}, infer_datetime_format=True,
                     parse_dates=True)
    df.sort_values(by=['Stkcd', 'Accper'])
    df.rename(columns=data_dict, inplace=True)
    return df


# %%
df_bs = pd_read('fin_data/FS_Combas.csv', bs_dict)
df_is = pd_read('fin_data/FS_Comins.csv', is_dict)
df_cs = pd_read('fin_data/FS_Comscfi.csv', cs_dict)

# %%
set_bs = {tuple(i) for i in df_bs[['证券代码', '会计期间', '报表类型']].values}
set_is = {tuple(i) for i in df_is[['证券代码', '会计期间', '报表类型']].values}
set_cs = {tuple(i) for i in df_cs[['证券代码', '会计期间', '报表类型']].values}

# %%
df_m1 = pd.merge(df_bs, df_is, how='inner', on=['证券代码', '会计期间', '报表类型'])
set_m1 = {tuple(i) for i in df_m1[['证券代码', '会计期间', '报表类型']].values}
# %%
df_m = pd.merge(df_m1, df_cs, how='inner', on=['证券代码', '会计期间', '报表类型'])
df_m.memory_usage(index=True).sum()/(1024**2)

#%%
