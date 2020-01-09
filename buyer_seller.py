from binance.client import Client
from time import localtime,mktime,clock,sleep
import datetime
from datetime import timedelta, datetime
from decimal import Decimal, getcontext
## Sets decimal precision for later calculations
getcontext().prec = 28
from mysql_utils import cnx, cmd, QueryThread

api_key = os.environ["BINANCE_KEY"]
api_key = os.environ["BINANCE_SECRET"]

def norm_signals():
    c = cnx
    
    whalesize = cmd(c,"SELECT new_balance FROM binance.whaletrades ORDER BY transaction_time DESC LIMIT 1 ;",dictionary=False)[0][0]
    print("whalesize is " + str(whalesize))

    buy_quantity = cmd(c,'SELECT SUM(quantity) FROM binance.whaletrades WHERE quantity > 0;',dictionary=False)[0][0]
    print("buy quantity " + str(buy_quantity))

    sell_quantity = cmd(c,'SELECT SUM(quantity) FROM binance.whaletrades WHERE quantity < 0;',dictionary=False)[0][0]
    print("sell quantity " + str(sell_quantity))

class BuySell:
    def __init__(self,symbol):

        self.symbol = symbol

        self.client = Client(api_key, api_secret)

        self.norm_signals()
##        self.get_asset_balance(self.symbol[0:3])
##        self.get_historical_trades()


    def get_historical_trades(self, **params):
        print('historical: '+ str(self.client.get_historical_trades(symbol=self.symbol)) )
        ## {'qty': '743.00000000', 'price': '0.00000961', 'isBestMatch': True, 'isBuyerMaker': False, 'time': 1514931925943, 'id': 4875867}

    def get_asset_balance(self, asset, **params):
        print(self.client.get_asset_balance(asset))

if __name__ == '__main__':
    norm_signals()

