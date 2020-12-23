#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr  8 16:48:30 2018

@author: ayx
"""

import pandas as pd
import os
import importlib
import pickle
import numpy as np

import time
import datetime

import gdax
public_client = gdax.PublicClient()
key='e3ad1932ee76f3f9b64b75a439c0d17a'
b64secret='QDDV7SVr6ahxskOfX38Wj9IyeUpx/5y6qnOCVXe9ys2q4kMKhKegxTYk7M23oJ/E0B62q019F29vfAiAxQzJxA=='
passphrase='fo9uk36tdjl'
auth_client = gdax.AuthenticatedClient(key, b64secret, passphrase)

symbol=input("Input your trading pair(e.g. ETH-USD): ")
end_ts = int(round(time.time() * 1000))
print('current time: ' + datetime.datetime.fromtimestamp(end_ts/1000).strftime('%Y-%m-%d %H:%M:%S.%f'))

end_iso=datetime.datetime.fromtimestamp((end_ts)/1000).isoformat()


df=pd.DataFrame()
inter_ts=100000000
#while len(candledata)>1:
i=2000
emptycnt=0
while emptycnt<10:
    i=i-1
    start_ts=end_ts-inter_ts
    end_iso=datetime.datetime.fromtimestamp(end_ts/1000).isoformat()
    start_iso=datetime.datetime.fromtimestamp(start_ts/1000).isoformat()
    candledata=auth_client.get_product_historic_rates(symbol, start=start_iso, end=end_iso,granularity=900)
    dft=pd.DataFrame(candledata)
    time.sleep(0.7)
    end_ts=end_ts-inter_ts-1
    if dft.empty:
        emptycnt+=1
    else:
        emptycnt=0
        df=df.append(dft)
    
    

# the final end_ts is the next starting end_ts (if not downloaded all historical data)
df.columns=['timestamp','open','high','low','close','amount']
print('Max time: ')
print(pd.to_datetime(min(df.timestamp)*1000, unit='ms', origin='unix'))
print('Min time: ')
print(pd.to_datetime(max(df.timestamp)*1000, unit='ms', origin='unix'))
#df.columns=[0,1,2,3,4,5]
df.sort_values('timestamp', ascending=True, inplace=True)
with open('./data/' +symbol+ '_tradehist.pkl','wb') as outfile:
    pickle.dump(df, outfile)


