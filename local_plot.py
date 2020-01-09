
#from mysql_utils import cmd, cnx, QueryThread
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import kde, percentileofscore
import pandas as pd
from time import mktime, gmtime
import datetime

from threadsafesqlalchemy import Session, Base, QueryClass
from sqlalchemy import func, desc
Aggregated_prices = Base.classes.aggregated_prices
Whaletrades = Base.classes.whaletrades

## Turns datetime tuple into milliseconds integer before sending it to the API
def format_time(struct_time):
    return(int(mktime(struct_time.timetuple()))*1000)

class relativeGraph(QueryClass):

    def __init__(self,intvl='minutes',limit=100):
        super().__init__()

#        prices = self.cmd("select * from binance.aggregated_prices WHERE intvl LIKE '{0}' AND price IS NOT NULL ORDER BY endTime DESC LIMIT {1};".format(interval,limit))
        prices = self.session.query(Aggregated_prices).filter(Aggregated_prices.intvl == intvl).\
                 filter(Aggregated_prices.price != None).\
                 order_by(desc(Aggregated_prices.endTime)).limit(limit).all()

        earliest_time = gmtime(prices[-1].tstamp/1000)
        earliest_price = prices[-1].price
        print("et: " + str(earliest_time) + " with type " + str(type(earliest_time)))
        latest_time = gmtime(prices[0].tstamp/1000)        
        print("lt: " + str(latest_time) + " with type " + str(type(latest_time)))

        tstamps = [x.tstamp for x in prices]
        prices = [x.price/earliest_price for x in prices]
        df=pd.DataFrame({'x': tstamps, 'Price (rel)': prices })
        plt.plot( 'x', 'Price (rel)', data=df, marker='o', markerfacecolor='blue', markersize=0, color='skyblue', linewidth=1)

        trades = self.session.query(Whaletrades).filter(Whaletrades.transaction_time > earliest_time).\
                 filter(Whaletrades.transaction_time < latest_time).\
                order_by(desc(Whaletrades.transaction_time))
 
                 #filter(Whaletrades.transaction_time<latest_time).\
        
        print("trades r: " + str(trades))
        trades= trades.all()
        earliest_balance = trades[-1].new_balance
        transaction_times = [format_time(x.transaction_time) for x in trades]

        import math
        balances = [math.pow(x.new_balance/earliest_balance,7) for x in trades]
        df2=pd.DataFrame({'x': transaction_times, 'Whale wallet balance': balances })
        plt.plot( 'x', 'Whale wallet balance', data=df2, marker='o', markerfacecolor='red', markersize=0, color='red', linewidth=1)

        plt.legend()
        plt.show()

def pricegraph(interval='minutes',limit=100):
    c = cnx
    prices = cmd(c,"select * from binance.aggregated_prices WHERE intvl LIKE '{0}' AND price IS NOT NULL ORDER BY endTime DESC LIMIT {1};".format(interval,limit))
    tstamps = [x['tstamp'] for x in prices]
    prices = [x['price'] for x in prices]
    df=pd.DataFrame({'x': tstamps, 'y1': prices })
    plt.plot( 'x', 'y1', data=df, marker='o', markerfacecolor='blue', markersize=0, color='skyblue', linewidth=1)
    plt.legend()
    plt.show()

def tradegraph(limit=1000):
    c = cnx
    prices = cmd(c,"SELECT * FROM binance.whaletrades ORDER BY transaction_time DESC LIMIT {0};".format(limit))
    tstamps = [int(mktime(x['transaction_time'].timetuple())) for x in prices]
    balance = [int(x['new_balance']) for x in prices]
    df=pd.DataFrame({'x': tstamps, 'y1': balance })
    plt.plot( 'x', 'y1', data=df, marker='o', markerfacecolor='blue', markersize=1, color='red', linewidth=0)
    plt.legend()
    plt.show()

#pricegraph(interval='seconds',limit=6000)
relativeGraph(intvl='minutes',limit=2000)

### Create a dataset:
###df=pd.DataFrame({'x': tstamps, 'y1': prices,'y2':floating_aves })
##
##df=pd.DataFrame({'x': tstamps, 'y1': prices })
### multiple line plot
##plt.plot( 'x', 'y1', data=df, marker='o', markerfacecolor='blue', markersize=3, color='skyblue', linewidth=4)
###plt.plot( 'x', 'y2', data=df, marker='', color='olive', linewidth=2)
##
##plt.legend()
##
##plt.show()
##    
