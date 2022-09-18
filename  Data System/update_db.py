#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 26 11:45:07 2022

@author: anthony
"""

from questdb.ingress import Sender
import datetime
from datetime import datetime, date
from query_db import queryData
import requests
import csv
from datetime import timedelta

def insert_ohlcv(data, table_name = 'NASDAQ_ohlcv'):
    with Sender('localhost', 9009) as sender:
        for row in data:
            #date =  datetime.strptime(row['date'], '%Y-%m-%d')
            sender.row(table_name = table_name,
                       symbols={'id':row['id']},
                       columns={'o': row['o'],'h':row['h'],'l':row['l'],'c':row['c'], 'v': row['v']}, 
                       at = row['date'] - timedelta(hours=4))#set to five when daylight savings time
        sender.flush()
            
def update_ohlcv(table_name = 'NASDAQ_ohlcv'):
    try:
        (_,_,_,_,_,_,last_date) = queryData().query_last_date(table_name = table_name)[0]
        duration = date.today() - last_date.date()
        first_time = datetime.now()
        data = fetchHistorical(tickers[:10], duration = str(duration.days) + ' D')
        total_data = [row for row in data if row['date'] >= last_date and row['date']<= first_time]
        insert_ohlcv(total_data)
        
    except Exception:
        print('table does not exist' )
