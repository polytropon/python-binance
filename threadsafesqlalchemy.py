## Asynchronously saves to MySQL database using threadsafe Sqlalchemy

## https://stackoverflow.com/questions/6297404/multi-threaded-use-of-sqlalchemy
##from time import localtime,mktime,clock,sleep
##from datetime import timedelta, datetime
##from threading import Thread
mysql_pw = os.getenv("MYSQL_PW")
from sqlalchemy import create_engine
engine = create_engine('mysql+mysqlconnector://root:{0}@127.0.0.1:3306/binance'.format(mysql_pw))
connection = engine.connect()
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
#http://docs.sqlalchemy.org/en/latest/orm/extensions/automap.html
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)
Aggregated_prices = Base.classes.aggregated_prices
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

# select * from binance.aggregated_prices WHERE intvl LIKE 'hours' ORDER BY endTime LIMIT 10;
##
class QueryClass:
    def __init__(self):
        self.session = Session()

    def cmd(self,command,**kwargs):
        result = self.session.execute(command,**kwargs)
        results = [x for x in result]
        return(results)

### USAGE:
##class relativeGraph(QueryClass):
##
##    def __init__(self,interval='seconds',limit=100):
##        super().__init__()
##
##        prices = self.session.query(Aggregated_prices).filter(Aggregated_prices.intvl == intvl).filter(Aggregated_prices.price != None).order_by(endTime)[:limit]

##        while True:
##            self.insert()
##
##    def insert(self):
##        self.session.add(Aggregated_prices(endTime=datetime.now(),symbol='WTF'))
##        self.session.commit()
##
##
##for x in range(1,30):
##    t = Thread(target=GetPastTrades,name='Update thread',args=(x,))
##    t.start()

##if __name__ == '__main__':
##    Consumer()
##cnx = mysql.connector.connect(user='root', password=mysql_pw,
##                              host='127.0.0.1',
##                              database='binance',
##                              autocommit=True
##                              )
    
