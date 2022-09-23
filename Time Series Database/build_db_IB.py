#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 29 10:58:12 2022

@author: anthony
"""
from websocket_IB import API_connection, stk, fetchHistorical
from update_db import insert_ohlcv
from query_db import queryData
from globalVariables import tickers, ticker_table, data_table

def create_hist_db(tickers):
    app = API_connection(tickers)
    tickers = queryData().query_entire_table(table_name = ticker_table)
    tickers = list(zip(*tickers))[0]
    
    data_ib = fetchHistorical(tickers)
    insert_ohlcv(data_ib, table_name = data_table)



