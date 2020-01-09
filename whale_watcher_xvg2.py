# https://verge-blockchain.info/info

import requests
from time import sleep, mktime
from datetime import datetime
from decimal import Decimal, getcontext
import decimal
## Sets decimal precision for later calculations
getcontext().prec = 28
from mysql_utils import cmd, cnx, QueryThread

from queue import Queue
from scipy.stats import percentileofscore
from bot_telegram import sendTelegram
import threading
from binance_data_recorder import retry

from threadsafesqlalchemy import Session, Base, QueryClass
from sqlalchemy import func

Aggregated_prices = Base.classes.aggregated_prices
Whaletrades = Base.classes.whaletrades


## Norms purchase quantity (-100 to 100)
def percentile(quantity):
    c = cnx
    whaletrades = cmd(c,'SELECT * FROM binance.whaletrades ORDER BY ABS(transaction_time) DESC;')
    y = [int(x['quantity']) for x in whaletrades]
    pctl = percentileofscore(y,quantity)
    if quantity < 0:
         pctl = pctl * -1

    return(pctl)


class WhaleWatcher(QueryClass):
    def __init__(self):
        super().__init__()

        self.watchWhale()

    def watchWhale(self):
        @retry
        def requestWhaleBalance():
            r = requests.get('https://verge-blockchain.info/ext/getbalance/DQkwDpRYUyNNnoEZDf5Cb3QVazh4FuPRs9',verify=False)
            return(r)
        r = requestWhaleBalance()
        
        while True:    
            sleep(0.5)

            ## Old text is the text before a new query
            old_text = r.text
            ## If server error, text contains html. Condition stops exception from
            ## failure to convert error message to decimal
            if old_text.find('<') == -1:
                old_balance = Decimal(old_text)

            r = requestWhaleBalance()
                    
            transaction_time = datetime.now()

            ## If text is number and not error message, which contains HTML
            if r.text.find('<') == -1 and old_text.find('<') == -1:
                new_balance = Decimal(r.text)

                ## If the text retrieved is new...
                if r.text != old_text:

                    new_balance = Decimal(r.text)

                    if (new_balance - old_balance) < 0:
                        action= 'sold'
                    else:
                        action = 'bought'

                    quantity = new_balance - old_balance
                    
                    ## Prevents errors from being registered as transactions
                    if quantity != 0:
                        change = abs(quantity)
                        percent = change / old_balance

                        ## REPLACE WITH API CALL!
                        ## Gets the aggregated price nearest to the time associated with the trade
                        result = self.session.query(Aggregated_prices).filter(Aggregated_prices.price != None).order_by(func.abs( func.timediff(transaction_time,Aggregated_prices.endTime) ).asc() )[0]
                        approx_price = result.price
#                       self.cmd(('''select * from binance.aggregated_prices order by ABS(TIMEDIFF(%(transaction_time)s, endTime )) LIMIT 1;'''),params=params)[0]

                        ## Percentile (may be move to separate thread)
                        pctl = percentile(quantity)
                        message = 'Whale {0} {1} XVG ( {2}% of holdings) \nTime: {3}\nApprox_price: {4} \nNormed: {5}'.format(action,int(change),
                                                                                                                             percent,transaction_time,approx_price,pctl)
#                        if pctl > 50
                        try:
                            sendTelegram(message)
                        except:
                            print("sendTelegram failed at " + str(datetime.now()))

                        ## Saves API data and nearest known price
                        params = {'new_balance':new_balance,
                         'old_balance':old_balance,
                         'quantity':quantity,
                         'transaction_time':transaction_time,'approx_price':approx_price}

                        self.session.add(Whaletrades(**params))
                        self.session.commit()

## Start recorder thread to keep the price DB current
from binance_data_recorder import record
t = threading.Thread(target=record,name='Recorder',args=('XVGBTC',))
t.daemon = True
t.start()

WhaleWatcher()
