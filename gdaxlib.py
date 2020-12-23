#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 10 17:41:20 2018

@author: ayx
"""
import pandas as pd
from sklearn.ensemble import RandomForestClassifier as rfc
#from sklearn.ensemble import ExtraTreesClassifier
from sklearn.ensemble import AdaBoostClassifier as ada
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.ensemble import GradientBoostingClassifier as grd
from gradientmoniter import Monitor
#import matplotlib.pyplot as plt
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from openpyxl import Workbook
import pickle
import talib as ta
import numpy as np
from functools import reduce


def build_ts(df, ts_period):
    df.set_index(df.date, inplace=True)        
    df_hrate=df['high'].resample(ts_period).max().to_frame()
    df_hrate.columns=['high']
    df_lrate=df['low'].resample(ts_period).min().to_frame()
    df_lrate.columns=['low']
    df_orate=df['open'].resample(ts_period).first().to_frame()
    df_orate.columns=['open']
    df_crate=df['close'].resample(ts_period).last().to_frame()
    df_crate.columns=['close']
    
    df_tradedata=df['amount'].resample(ts_period).sum().to_frame()
    
    dfs=[df_orate,df_hrate,df_lrate,df_crate,df_tradedata]
    df_grp=reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True), dfs)

    
    # fill with previous for prices
    nullidx=df_grp.close.isnull()
    df_grp.close=df_grp.close.fillna(method='ffill')
    df_grp.loc[nullidx, ['close']]=df_grp.loc[nullidx, ['close']].values
    df_grp.loc[nullidx, ['open']]=df_grp.loc[nullidx, ['close']].values
    df_grp.loc[nullidx, ['high']]=df_grp.loc[nullidx, ['close']].values
    df_grp.loc[nullidx, ['low']]=df_grp.loc[nullidx, ['close']].values
    # fill with 0 for volume (already =0)
    return df_grp
