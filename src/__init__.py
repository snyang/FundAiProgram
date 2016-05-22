"""Fund AI Program
"""

__author__ = "Yang Ning (Steven) (steven.n.yang@gmail.com)"
__version__ = "1.0.0"
__copyright__ = "Copyright (c) 2016 Yang Ning (Steven)"
__license__ = "MIT"

__all__ = ['Runner']

import threading
from concurrent.futures import *
from datetime import *
from helpers import *


class Runner():

    def run(self):
        pass

    def test(self):
        systemHelper.init()
        print(date.today().strftime('%Y-%m-%d'))


# By default, run.
if __name__ == '__main__':
    import sys
    runner = Runner()
    runner.run()
