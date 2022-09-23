#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep  5 16:25:31 2022

@author: anthony
"""

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import threading
from datetime import datetime

ticker_event = threading.Event()#cycle through tickers to get historical data

#contains all the IB wrapper functions
class TradeApp(EWrapper, EClient): 
    def __init__(self, tickers): 
        EClient.__init__(self, self) 
        self.hist_dict =  []
        self.tickers = tickers
        self.shortable_tickers = []
    
#####   wrapper function for reqHistoricalData. this function gives the candle historical data
    def historicalData(self, reqId, bar):
        self.hist_dict.append({'date':datetime.strptime(bar.date, '%Y%m%d %H:%M:%S'),'o':bar.open,'h':bar.high,'l':bar.low,'c':bar.close,'v':bar.volume,'id':self.tickers[reqId]})
 
#####   wrapper function for reqHistoricalData. this function triggers when historical data extraction is completed      
    def historicalDataEnd(self, reqId, start, end):       
        print("HistoricalDataEnd. ReqId:", reqId, "from", start, "to", end)
        ticker_event.set()  

##### wrapper function for reqMktData. this function handles streaming market data    
    def tickGeneric(self, reqId, tickType, value):
        super().tickGeneric(reqId, tickType, value)
        if self.tickers[reqId] in self.shortable_tickers:
            self.cancelMktData(reqId)
        else:
            if tickType == 46 and value > 2.5:
                self.shortable_tickers.append(self.tickers[reqId])
                self.cancelMktData(reqId)
                
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
def stk(symbol,sec_type="STK",currency="USD",exchange="ISLAND"):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.currency = currency
    contract.exchange = exchange
    return contract 

#retrieves the historical data in ticker by ticker loop, saved in the TradeApp
def fetchHistorical(tickers, timeframe = '1 min', duration = '1 D'):
    app = API_connection(tickers)
    for ticker in app.tickers:
        ticker_event.clear()
        app.reqHistoricalData(reqId=app.tickers.index(ticker), 
                              contract=stk(ticker),
                              endDateTime='',
                              durationStr= duration,
                              barSizeSetting= timeframe,
                              whatToShow='ADJUSTED_LAST',
                              useRTH=1,
                              formatDate=1,
                              keepUpToDate=0,
                              chartOptions=[])	 # EClient function to request contract details
        ticker_event.wait()
        
    app.disconnect()
        
    return sorted(app.hist_dict, key=lambda d: d['date'])