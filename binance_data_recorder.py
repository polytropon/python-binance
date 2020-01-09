from binance.client import Client
from time import localtime,mktime,clock,sleep
import datetime
from datetime import timedelta, datetime
from decimal import Decimal, getcontext
## Sets decimal precision for later calculations
getcontext().prec = 28
import numpy
from copy import deepcopy
from mysql_utils import cnx, cmd
from queue import Queue
from threading import Thread
import multiprocessing

api_key = "1QrZrWewq1I2hvsz0iFGPTofCEEcr5fTnVSbQnSIIfVG4uaHZblTetciLAPrcGp3"
api_secret = "lHtV2OxVqeatPNqwKZmRiknHfzt93Q9FNjy4FFpD7keK4ibRKLE4M3bPwRBdTbxj"


## Sqlalchemy init block
##from sqlalchemy import create_engine
##engine = create_engine('mysql+mysqlconnector://root:d0nk3Y_KON6-@127.0.0.1:3306/binance')
##connection = engine.connect()
##from sqlalchemy.ext.automap import automap_base
##from sqlalchemy.orm import scoped_session
##from sqlalchemy.orm import sessionmaker
###http://docs.sqlalchemy.org/en/latest/orm/extensions/automap.html
##Base = automap_base()
### reflect the tables
##Base.prepare(engine, reflect=True)
##Aggregated_prices = Base.classes.aggregated_prices
##session_factory = sessionmaker(bind=engine)
##Session = scoped_session(session_factory)
## //sqlalchemy init
from threadsafesqlalchemy import Session, Base, QueryClass

Aggregated_prices = Base.classes.aggregated_prices

## Rounds down datetime object to the specified interval

def round_time(struct_time,interval):
    intervals = ["days","hours","minutes","seconds","microseconds"]
    tdelta_kwargs = {} 

    ## Gathers kwargs for timedelta such that all units of time smaller than the interval argument can be set to 0
    for intvl in intervals[intervals.index(interval)+1:]:
        tdelta_kwargs[intvl] = getattr(struct_time,intvl[:len(intvl)-1])

    ## Rounds out "now"
    struct_time = struct_time - timedelta(**tdelta_kwargs)
    return(struct_time)
  
## Turns datetime tuple into milliseconds integer before sending it to the API
def format_time(struct_time):
    return(int(mktime(struct_time.timetuple())*1000))


def retry(func):

    def wrapper(*args,**kwargs):
        trying = True
        wait = 1
        while trying:
            try:
                return(func(*args,**kwargs))
                trying = False
            except Exception as e:
                wait *= 2
                print("At {0}, exception thrown: {1}".format(datetime.now(),e))
                print("waiting for " + str(wait) + " seconds.")
                sleep(wait)

    return(wrapper)

class GetPastTrades(QueryClass):
    def __init__(self,symbol,endTime,step_back,interval,no_intervals,live=False):
        super().__init__()

        print("GetPastTrades initializing")
      
        self.symbol = symbol
        self.endTime = endTime
        self.step_back = step_back
        self.interval = interval
        self.no_intervals = no_intervals
        self.live = live

        self.client = Client(api_key, api_secret)

        print("endTime is " + str(self.endTime))
        print("step_back is " + str(self.step_back))

        ## Calculate begin of first interval period
        startTime = (endTime - step_back) + timedelta(microseconds=1)
        print("startTime is " + str(startTime))

        if live:
            self.monitor_live()
        else:
            self.update_records()

    @retry
    def call_api_and_save(self,symbol,startTime,endTime,interval,client=False):
        ## API call
        if not client:
            client = Client(api_key, api_secret)
            
        aggregated_trades = client.get_aggregate_trades(symbol=symbol,
                                                             startTime=format_time(startTime),
                                                             endTime=format_time(endTime), limit=1)

        ## If there are no trades to be aggregated in this unit of time, saves start and end time and sets price and tstamp to NULL
        if len(aggregated_trades) == 0:
            price = None
            tstamp = None
            tradeId = None
        else:
            aggregated_trades = aggregated_trades[0]
            price = aggregated_trades['p']
            tstamp = aggregated_trades['T']
            tradeId = aggregated_trades['a']


        data = {'startTime':startTime,'endTime':endTime,
                'price':price,'symbol':symbol,
                'tstamp':tstamp,'tradeId':tradeId,
                'intvl':interval} ## hours, etc.

        self.session.add(Aggregated_prices(**data))
        self.session.commit()

        ## Returns client for reuse by a loop making multiple calls to avoid unnecessarily reconnecting
        return(client)

    def cmd(self,command,**kwargs):
        result = self.session.execute(command,**kwargs)
        results = [x for x in result]
        return(results)

    ## Checks that the records have no gaps, filling such as exist.
    def update_records(self):
        ## Gap fillers
        command = "SELECT COUNT(*), MIN(startTime), MAX(startTime) FROM binance.aggregated_prices WHERE intvl LIKE '{0}' AND symbol LIKE '{1}';".format(self.interval,self.symbol)

        ### BREAKS IF NO RECORDS ARE PRESENT, FIX!!
        results = self.cmd(command)[0]
        
        no_entries = results['COUNT(*)']
        earliestEnd = results['MIN(startTime)']
        latestEnd = round_time(datetime.now(),self.interval)
        seconds_covered = (latestEnd - earliestEnd).total_seconds()
        
        conversion_dic = {'seconds':1,'minutes':60,'hours':3600,'days':'86400'}

        ## Amount of intervals covered since first interval
        intervals_covered = int(seconds_covered / conversion_dic[self.interval])
 
        ## If there are fewer entries than intervals covered...
        if no_entries < intervals_covered:

            ## no_intervals is the max amount of intervals for which records are downloaded
            command = "SELECT * FROM binance.aggregated_prices WHERE intvl LIKE '{0}' AND symbol LIKE '{1}' LIMIT {2};".format(self.interval,self.symbol,self.no_intervals)
            results = self.cmd(command)
            dts = [x['endTime'] for x in results]

            missing = intervals_covered - no_entries

            for x in range(0,intervals_covered):

                ## Rounds present to latest possible end time
                endTime = latestEnd - (x * self.step_back)
                if endTime not in dts:
                    startTime = (endTime - self.step_back) + timedelta(microseconds=1)
                    self.client = self.call_api_and_save(self.symbol,startTime,endTime,self.interval,self.client)
                    ## Got dt
                    missing -= 1

    def monitor_live(self):
            while True:
                
                command = "SELECT max(endTime) FROM binance.aggregated_prices WHERE intvl LIKE '{0}' AND symbol LIKE '{1}';".format(self.interval,self.symbol)

                results = self.cmd(command)
#                if results and (results is not None): ## Duplicate primary key throws an exception and sets results to false
                latest_end = results[0]['max(endTime)'] ## End time of most recent inverva

                since_last_end = datetime.now() - latest_end

                if since_last_end > self.step_back: ## If more than one interval has passed since the last record...
                    ## Gets the end of the interval to be requested via API
                    self.endTime = round_time(datetime.now(),self.interval)
                    ## Calculates beginning of interval from end
                    self.startTime = (self.endTime - self.step_back) + timedelta(microseconds=1)
                    ## Calls API, saves to db
                    client = self.call_api_and_save(self.symbol,self.startTime,self.endTime,self.interval,self.client)

                else:## If full interval hasn't passed, waits until it has passed until calling API
                    sleep((self.step_back - since_last_end).total_seconds())

class RecordSymbol(QueryClass):
    def __init__(self,symbol,interval):
        super().__init__()
        
        self.symbol = symbol
        self.interval = interval
        
        self.get_aggregate_trades()
        
    def cmd(self,command,**kwargs):
        result = self.session.execute(command,**kwargs)
        results = [x for x in result]
        return(results)

    ## Interval is a unit of time that serves as the kwarg for timedelta
    def get_aggregate_trades(self,no_intervals=15000):

        ## Current time in seconds
        now = round_time(datetime.now(),self.interval)

        ## Creates timedelta
        kwargs = {self.interval:1} # One day, hour etc.
        step_back = timedelta(**kwargs)

##        ## Code checks which aggregated prices are already saved and which need to be added
        command = "SELECT max(endTime),min(startTime),count(*) FROM binance.aggregated_prices WHERE intvl LIKE '{0}' AND symbol LIKE '{1}';".format(self.interval,
                                                                                                                                                    self.symbol)
        print("command is " + str(command))
        start_end = self.cmd(command)
        print("results are " + str(start_end))
        start_end = start_end[0]
        earliest_start_time = start_end['min(startTime)']
        print("earliest_start_time is " + str(earliest_start_time))
        latest_end_time = start_end['max(endTime)']
        no_trades =start_end['count(*)']

        ## PRCOESSES
        t4 = Thread(target=GetPastTrades,name='Live thread',args=(self.symbol,
                                                             earliest_start_time,step_back,
                                                             self.interval,1),kwargs={'live':True})
#        self.threads.append(t4)
        t4.start()

        t5 = Thread(target=GetPastTrades,name='Update thread',args=(self.symbol,
                                                             earliest_start_time,step_back,
                                                             self.interval,1),kwargs={'live':False})
#        self.threads.append(t5)
        t5.start()

            ## HERE CODE TO FIND AND FILL GAPS

##                    "a": Aggregate tradeId
##                    "p": "0.01633102",  # Price
##                    "q": "4.70443515",  # Quantity
##                    "f": 27781,         # First tradeId
##                    "l": 27781,         # Last tradeId
##                    "T": 1498793709153, # Timestamp
##                    "m": true,          # Was the buyer the maker?
##                    "M": true           # Was the trade the best price match?
import threading
def record(symbol):
    def singleInterval(interval):
        RecordSymbol(symbol,interval)
        ## IF NO RECORDS ARE PRESENT FOR A CERTAIN INTERVAL, update_records FUNCTION BREAKS!
        ## LIMITED LIST FOR TEST MODE ALLOWS DEBUGGING
#    intervals = ["days","hours","minutes","seconds","microseconds"]
    intervals = ['hours','minutes','seconds']
    for interval in intervals:
        t = threading.Thread(target=singleInterval,
                             name='Recorder ' + interval,
                             args=(interval,))
        t.daemon = True
        t.start()
        RecordSymbol(symbol,interval)
    
if __name__ == '__main__':
    record('XVGBTC')
