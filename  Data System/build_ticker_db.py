"""
In this file pull all symbols that are tradeable

"""

# Import libraries
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from questdb.ingress import Sender
import pandas as pd
import requests
import threading
import time

#ticker_event = threading.Event()#cycle through tickers to get historical data

#contains all the IB wrapper functions
class TradeApp(EWrapper, EClient): 
    def __init__(self, tickers): 
        EClient.__init__(self, self) 
        self.shortable_tickers = [] 
        self.tickers = tickers
            
##### wrapper function for reqMktData. this function handles streaming market data    
    def tickGeneric(self, reqId, tickType, value):
        super().tickGeneric(reqId, tickType, value)
        if self.tickers[reqId] in self.shortable_tickers:
            self.cancelMktData(reqId)
        else:
            if tickType == 46 and value > 2.5:
                self.shortable_tickers.append(self.tickers[reqId])
                self.cancelMktData(reqId)

def retrieve_tickers(exchange = 'NASDAQ'):
    api_key = '517687b7075bb5d5115d85355e3dbd41'
    ticker_info = pd.DataFrame(requests.get('https://financialmodelingprep.com/api/v3/stock-screener?exchange='+ exchange +'&apikey=' + api_key).json())
    tickers = list(ticker_info['symbol'])
    return tickers

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

####  this function starts the streaming data of current ticker.
def streamSnapshotData(tickers, ticker, app):
    app.reqMktData(reqId=tickers.index(ticker), 
                   contract=usStk(ticker),
                   genericTickList="236",
                   snapshot=False,
                   regulatorySnapshot=False,
                   mktDataOptions=[])
    
#Contract = ticker information sent to API to retrieve data
def usStk(symbol,sec_type="STK",currency="USD",exchange="ISLAND"):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.currency = currency
    contract.exchange = exchange
    return contract 

# Get symbols that are shortable
def get_tradeable_symbols():
    for ticker in app.tickers:
        streamSnapshotData(app.tickers, ticker, app)
        time.sleep(3)
    return True

# Insert Ticker list
def insert_tickers(data,db_name = 'NASDAQ_tickers'):
    with Sender('localhost', 9009) as sender:
        for row in data:
            #date =  datetime.strptime(row['date'], '%Y-%m-%d')
            sender.row(table_name =db_name,
                       symbols={'ticker':row})
            sender.flush()

all_tickers = retrieve_tickers()
app = API_connection(all_tickers)
signal = get_tradeable_symbols()

if signal:
    x = app.shortable_tickers
    insert_tickers(x)   
