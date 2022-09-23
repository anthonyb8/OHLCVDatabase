"""
Create the ticker database

"""

# Import libraries
from questdb.ingress import Sender
from websocket_IB import API_connection, stk
from globalVariables import tickers, ticker_table
import time


####  this function starts the streaming data of current ticker.
def streamSnapshotData(tickers, ticker, app):
    app.reqMktData(reqId=tickers.index(ticker), 
                   contract=stk(ticker),
                   genericTickList="236",
                   snapshot=False,
                   regulatorySnapshot=False,
                   mktDataOptions=[])
    
# Get symbols that are shortable
def get_tradeable_symbols(app):
    for ticker in app.tickers:
        streamSnapshotData(app.tickers, ticker, app)
        time.sleep(3)
    return True

# Insert Ticker list
def insert_tickers(data,db_name = ticker_table):
    with Sender('localhost', 9009) as sender:
        for row in data:
            #date =  datetime.strptime(row['date'], '%Y-%m-%d')
            sender.row(table_name =db_name,
                       symbols={'ticker':row})
            sender.flush()

def create_ticker_db():
    print('tst')
    app = API_connection(tickers)
    signal = get_tradeable_symbols(app)

    if signal:
        insert_tickers(app.shortable_tickers)   
        app.disconnect()
