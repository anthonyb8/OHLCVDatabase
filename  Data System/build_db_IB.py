#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 29 10:58:12 2022

@author: anthony
"""
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import threading
from datetime import datetime, date
from query_db import queryData
from update_db import insert_ohlcv
import time

ticker_event = threading.Event()#cycle through tickers to get historical data

#contains all the IB wrapper functions
class TradeApp(EWrapper, EClient): 
    def __init__(self, tickers): 
        EClient.__init__(self, self) 
        self.hist_dict =  []
        self.tickers = tickers
    
#####   wrapper function for reqHistoricalData. this function gives the candle historical data
    def historicalData(self, reqId, bar):
        print('t1')
        self.hist_dict.append({'date':datetime.strptime(bar.date, '%Y%m%d %H:%M:%S'),'o':bar.open,'h':bar.high,'l':bar.low,'c':bar.close,'v':bar.volume,'id':self.tickers[reqId]})
        print('t2')
#####   wrapper function for reqHistoricalData. this function triggers when historical data extraction is completed      
    def historicalDataEnd(self, reqId, start, end):       
        print("HistoricalDataEnd. ReqId:", reqId, "from", start, "to", end)
        ticker_event.set()
        
    def error(self, reqId: TickerId, errorCode: int, errorString: str, advancedOrderRejectJson = ""):
        super().error(reqId, errorCode, errorString, advancedOrderRejectJson)
        if advancedOrderRejectJson:
            print("Error. Id:", reqId, "Code:", errorCode, "Msg:", errorString, "AdvancedOrderRejectJson:", advancedOrderRejectJson)
        else:
            print("Error. Id:", reqId, "Code:", errorCode, "Msg:", errorString)
        
#establish the deamon thread that holds the api connection 
def API_connection(tickers):   
    app = TradeApp(tickers)
    app.connect(host='127.0.0.1', port=7497, clientId=23) #port 4002 for ib gateway paper trading/7497 for TWS paper trading
    con_thread = threading.Thread(target=app.run, daemon=True)
    con_thread.start()
    while app.isConnected() == False:
        print('Waiting for Connection')	
        continue
    print('Established Connection')	
    return app


#Contract = ticker information sent to API to retrieve data
def usStk(symbol,sec_type="STK",currency="USD",exchange="ISLAND"):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.currency = currency
    contract.exchange = exchange
    return contract 

#retrieves the historical data in ticker by ticker loop, saved in the TradeApp
def fetchHistorical(tickers, timeframe = '1 min', duration = '1 D'):
    bad_tickers = []
    app = API_connection(tickers)
    try :  
        for ticker in app.tickers:
            ticker_event.clear()
            app.reqHistoricalData(reqId=app.tickers.index(ticker), 
                                  contract=usStk(ticker),
                                  endDateTime='',
                                  durationStr= duration,
                                  barSizeSetting= timeframe,
                                  whatToShow='ADJUSTED_LAST',
                                  useRTH=1,
                                  formatDate=1,
                                  keepUpToDate=0,
                                  chartOptions=[])	 # EClient function to request contract details
            ticker_event.wait()
            
    except Exception:
            bad_tickers.append(ticker)
    
        
    app.disconnect()
        
    return sorted(app.hist_dict, key=lambda d: d['date'])

tickers = queryData().query_entire_table(table_name = 'NASDAQ_tickers')
tickers = list(zip(*tickers))[0]

data_ib = fetchHistorical(tickers[:1000])
insert_ohlcv(data_ib, table_name = 'NASDAQ_ohlcv2')


def update_ohlcv(table_name = 'NASDAQ_ohlcv'):
    try:
        (_,_,_,_,_,_,last_date) = queryData().query_last_date(table_name = table_name)[0]
        duration = date.today() - last_date.date()
        first_time = datetime.now()
        data = fetchHistorical(tickers, duration = str(duration.days+1) + ' D')
        total_data = [row for row in data if row['date'] > last_date and row['date']<= first_time]
        insert_ohlcv(total_data)
        
    except Exception:
        print('table does not exist' )
