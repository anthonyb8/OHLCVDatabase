#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 21 22:50:58 2022

@author: anthony
"""

from build_ticker_db import create_ticker_db
from build_db_IB import create_hist_db
from globalVariables import tickers, ticker_table, data_table
from query_db import queryData
from update_db import update_ohlcv
import time as t
from datetime import datetime
from datetime import time

if __name__ == "__main__":
    
    while datetime.time(datetime.now()) > time(16,00,00):
        ## IF ticker table not create 
        try:
            queryData().query_entire_table(table_name = ticker_table)
        except:
            create_ticker_db()
        
        ## IF data table not create
        try:
            update_ohlcv(data_table)
        except:
            create_hist_db(tickers)
        
        # Recurrent timer
        t.sleep(180)
    
    