username = 'infoc019'
api_key = '1aPcVzWxycI13BbdYmoj'
import plotly
import numpy as np
import plotly.plotly as py
import plotly.tools as tls
import plotly.graph_objs as go
from queue import Queue

#plotly.tools.set_credentials_file(username=username, api_key=api_key)
#plotly.tools.set_config_file(world_readable=True,
#                             sharing='public')

#stream_ids = tls.get_credentials_file()['stream_ids']


class StreamingGraph:
    stream_ids = [u'6nvgbrixbk',u'3v6623tpa2',u'bmqnm9gjyl',u'wjk4prhkcp',u'9rhsjdbtfc',u'y690xu4prh',u'bjg0zbq243',u'yfiek2reaa',u'uqh1k7kob1']
    stream_id = stream_ids[0]
    def __init__(self,plotqueue):
        print("initiating StreamingGraph")

        self.plotqueue = plotqueue
  
        # Make instance of stream id object 
        self.stream_1 = go.Stream(
            token=self.stream_id,  # link stream id to 'token' key
            maxpoints=80      # keep a max of 80 pts on screen
        )
        print("stream 1 created")
        trace1 = go.Scatter(
            x=[],
            y=[],
            mode='lines+markers',
            stream=self.stream_1         # (!) embed stream id, 1 per trace
        )

        data = go.Data([trace1])

        # Add title to layout object
        layout = go.Layout(title='Time Series')
        print("layout created")

        # Make a figure object
        fig = go.Figure(data=data, layout=layout)

        # Send fig to Plotly, initialize streaming plot, open new tab
        py.iplot(fig, filename='python-streaming')

        # We will provide the stream link object the same token that's associated with the trace we wish to stream to
        self.s = py.Stream(self.stream_id)

        # We then open a connection
        self.s.open()

        # (*) Import module keep track and format current time
        import datetime
        import time

        i = 0    # a counter
        k = 5    # some shape parameter

        print("sleeping")
        # Delay start of stream by 5 sec (time to switch tabs)
        time.sleep(5)
        print("slept")

        self.plot_queue = Queue(maxsize=0)

        while True:
            if not self.plot_queue.empty():
                x,y = self.plotqueue.get()
                self.add_data(x,y)

##            # Current time on x-axis, random numbers on y-axis
##            x = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
##            y = (np.cos(k*i/50.)*np.cos(i/50.)+np.random.randn(1))[0]
##
##            # Send data to your plot
##            self.s.write(dict(x=x, y=y))
##
##            #     Write numbers to stream to append current data on plot, 
##            #     write lists to overwrite existing data on plot
##
##            time.sleep(1)  # plot a point every second

        self.s.close()

    def add_data(self,x,y):
        x = x.strftime('%Y-%m-%d %H:%M:%S.%f')
        
        self.s.write(dict(x=x, y=y))

if __name__ == '__main__':
    StreamingGraph()
