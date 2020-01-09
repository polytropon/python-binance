from mysql_utils import cmd, cnx
from scipy.stats import percentileofscore

def norm_transaction(quantity,transactions=False):
    c = cnx
    
    if quantity < 0:
        sign = '<'
    else:
        sign = '>'

    ## Transactions can be supplied as kwarg to avoid mysql query
    if not transactions:
        transactions = cmd(c,'SELECT quantity FROM binance.whaletrades WHERE quantity ' + sign + ' 0;',dictionary=True)
        transactions = [int(x['quantity']) for x in transactions]
    pctl = percentileofscore(transactions,quantity)
    return(quantity,pctl)


c = cnx
query = "select * from binance.aggregated_prices WHERE intvl LIKE 'seconds' ORDER BY endTime;"
trades = cmd(c,query,dictionary=True)


#### Either all purchases or all sales
##c = cnx
##transactions = cmd(c,'SELECT quantity FROM binance.whaletrades;',dictionary=True)
##transactions = [int(x['quantity']) for x in transactions]
##
##sales = []
##purchases = []
##for z in transactions:
##    quantity,pctl = norm_transaction(z,transactions)
##    if quantity < 0:
##        sales.append(pctl*-1)
##    else:
##        purchases.append(pctl)
##
##import seaborn as sns
##import matplotlib.pyplot as plt
##
##sns.distplot( purchases , color="skyblue", label="Purchases")
##sns.distplot( sales , color="red", label="Sales")
##plt.legend()
##plt.show()

##import pandas as pd

# Create a dataset:#df=pd.DataFrame({'x': x, 'y': y })
 
### plot
###plt.plot( 'x', 'y', data=df, linestyle='none', marker='o')
###plt.hexbin(x, y, cmap=plt.cm.BuGn_r)
##plt.hist2d(x, y, cmap=plt.cm.BuGn_r)
##plt.show()

## Norms purchase quantity (-100 to 100)
def percentile(quantity):
    c = cnx
    whaletrades = cmd(c,'SELECT * FROM binance.whaletrades ORDER BY ABS(transaction_time) DESC;')
    y = [int(x['quantity']) for x in whaletrades]
    pctl =  percentileofscore(y,quantity)
    if quantity < 0:
         pctl = pctl * -1
