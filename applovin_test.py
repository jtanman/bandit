# By: James Tan

# Date: 5/17/2017

"""
Simulates and tests the applovin whitelist/blacklist report
"""
from analytics.bandit.arm import *
from analytics.bandit.bandit import *
from analytics.bandit.environment import *
from analytics.bandit.experiment import *
from analytics.bandit.bandit_report import *
from datetime import date, timedelta
import pandas as pd
import numpy as np
import time
import os

if __name__ == '__main__':

    SLIDING_WINDOW = 90

    start_date = date(2017, 2, 1)
    date1 = date(2017, 6, 28)
    date2 = date(2017, 6, 30)

    date2 - date1

    dates = list()
    for day in xrange(0, (date2 - date1).days + 1):
        dates.append(date1 + timedelta(days=1 * day))

    # os.remove('Applovin.pkl')

    for d in dates:
        print d
        report = ApplovinBanditReporter(start_date=start_date, run_date=d, sliding_window=SLIDING_WINDOW)
        report.run()

    print 'applovin test success'
