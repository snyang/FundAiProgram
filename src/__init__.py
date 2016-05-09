"""Fund AI Program
"""

__author__ = "Yang Ning (Steven) (steven.n.yang@gmail.com)"
__version__ = "1.0.0"
__copyright__ = "Copyright (c) 2004-2015 Yang Ning (Steven)"
__license__ = "MIT"

__all__ = ['Runner']

import threading
from concurrent.futures import *
from datetime import *
from helpers import *

class Runner():
    def run(self) :
        systemHelper.init()

        startTime = datetime.now()
        funds = fundDataHelper.getFundCodes(spiderHelper.getText(hostInfoHelper.getAllFundDataLink(), "all_fund.txt"))
        i  = 0
        with ThreadPoolExecutor() as executor:
            for fund in funds : 
                try:
                    i += 1
                    fundCode = fund[0]
                    future = executor.submit(self.get_baseinfo, fundCode)
                    future.add_done_callback(threadCallbackHandler(fundCode).handle_callback)
                except Exception as e:
                    print("Failed {0}. {1}".format(fundCode, e))
        
        # wait all threads finish.         
        executor.shutdown(wait=True)
        endTime = datetime.now()
        print("done. total {0} funds during {1}.".format(i, endTime - startTime))
    
    def get_baseinfo(self, fundCode) :
        #soupFund = spiderHelper.getSoup(hostInfoHelper.getFundBaseInfoLink(fundCode), "jbgk_{}.html".format(fundCode))
        #print("Running thread {}...".format(fundCode))
        pass
        
    def test(self) :
        systemHelper.init()

        print(date.today().strftime('%Y-%m-%d'))

class threadCallbackHandler:
    def __init__(self, fundCode):
        self.fundCode = fundCode
        
    def handle_callback(self, future) :
        try:
            if (future.exception() != None):
                print("Failed {0}. Exception : {1}".format(self.fundCode, future.exception()))    
        except CancelledError as e:
            print("Cancelled {0}".format(self.fundCode))
        print("Done {0}".format(self.fundCode)) 
        
#By default, run.
if __name__ == '__main__':
    import sys
    runner = Runner()
    runner.run()