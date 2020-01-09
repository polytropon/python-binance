from mysql_utils import cnx, cmd
import threading
from queue import Queue

## Also in C:\Users\Toshiba-PC\AppData\Local\Programs\Python\Python35\Lib\site-packages

## Base class for thread-safe mysql queries
class QueryThread:
    def __init__(self):

        self.queryqueue = Queue(maxsize=0)

        self.querythread = threading.Thread(target=self.process_queries,name='QueryThread')
        self.querythread.start()
                ## Locks main class thread while waiting for results
        self.lock = threading.Lock()
        ## Queue receives results from separate query thread
        self.resultqueue = Queue(maxsize=0)
        
    def cmd(self,cmd,params=False):
        print("qt putting cmd: " + str(cmd))

        self.queryqueue.put((cmd,params))
        self.lock.acquire()
        print("lock acquired")
        ## Lock is released once results are added to queue
        result = self.resultqueue.get()
        return(result)

    ## Thread is constantly running
    def process_queries(self):
        c = cnx ## Opens connection
        while True:
            if not self.queryqueue.empty():
                print("queue not empty")
                query,params = self.queryqueue.get()
                
                results = cmd(c,query,dictionary=True,params=params)
                print("querythread got result: " + st(results))
                results = False

                self.resultqueue.put(results)
                self.lock.release()

