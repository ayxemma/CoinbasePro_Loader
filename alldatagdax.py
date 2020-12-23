#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 25 21:55:55 2018

@author: ayx
"""

import pandas as pd
import importlib


import time
import datetime

import gdax
public_client = gdax.PublicClient()
key='e3ad1932ee76f3f9b64b75a439c0d17a'
b64secret='QDDV7SVr6ahxskOfX38Wj9IyeUpx/5y6qnOCVXe9ys2q4kMKhKegxTYk7M23oJ/E0B62q019F29vfAiAxQzJxA=='
passphrase='fo9uk36tdjl'
auth_client = gdax.AuthenticatedClient(key, b64secret, passphrase)

#public_client.get_product_historic_rates('ETH-USD', granularity=3000)

symbol='ETH-USD'
end_ts = int(round(time.time() * 1000))
print('current time: ' + datetime.datetime.fromtimestamp(end_ts/1000).strftime('%Y-%m-%d %H:%M:%S.%f'))

end_iso=datetime.datetime.fromtimestamp(end_ts/1000).isoformat()



df=pd.DataFrame()
inter_ts=100000000
#while len(candledata)>1:
i=22
while i>0:
    i=i-1
    start_ts=end_ts-inter_ts
    candledata=auth_client.get_product_historic_rates('ETH-USD', start=start_iso, end=end_iso,granularity=900)
    dft=pd.DataFrame(candledata)
    df=df.append(dft)
    end_ts=end_ts-inter_ts-1
    time.sleep(0.4)
    
df.columns=['date','open','high','low','close','amount']
print('Max time: ')
print(pd.to_datetime(min(df.date)*1000, unit='ms', origin='unix'))
print('Min time: ')
print(pd.to_datetime(max(df.date)*1000, unit='ms', origin='unix'))


df.to_pickle(r'/Users/ayx/Documents/Trading/CryptoCoin/Gdax/data/' + symbol + '.pkl')
df=pd.read_pickle(r'/Users/ayx/Documents/Trading/CryptoCoin/Gdax/data/' + symbol + '.pkl')

## grouping into 30 minutes
df.date=df.date*1000
df.date=pd.to_datetime(df.date,utc=False, unit='ms',origin='unix')
df.set_index(df.date, inplace=True,drop=True)
df=df.drop(columns=['date'])

df=df.apply(pd.to_numeric)
df_tradedata=df.loc[:,['amount']]
df_tradedata_grp=df_tradedata.resample('30Min').sum()
df_hrate=df.high.resample('30Min').max().to_frame()
df_hrate.columns=['high']
df_lrate=df.low.resample('30Min').min().to_frame()
df_lrate.columns=['low']
df_orate=df.open.resample('30Min').first().to_frame()
df_orate.columns=['open']
df_crate=df.close.resample('30Min').last().to_frame()
df_crate.columns=['close']


from functools import reduce
dfs=[df_tradedata_grp,df_orate,df_hrate,df_lrate,df_crate]
df_grp=reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True), dfs)


df_grp=df_grp.apply(pd.to_numeric)
df_grp.to_pickle(r'/Users/ayx/Documents/Trading/CryptoCoin/Gdax/data/' + symbol + '_cleaned.pkl')
