# By: James Tan

# Date: 6/6/2017

"""
Simulates and tests the bandit for crm
"""
from analytics.bandit.arm import *
from analytics.bandit.bandit import *
from analytics.bandit.environment import *
from analytics.bandit.experiment import *
from analytics.bandit.bandit_report import *
from analytics.bandit.bandit_crm import *
from analytics.bandit.bandit_queries import *
from analytics.db import redshift
from datetime import date, timedelta
import pandas as pd
import numpy as np
import time
import os

if __name__ == '__main__':

    SLIDING_WINDOW = 90

    date1 = date(2017, 6, 7)
    date2 = date(2017, 6, 5)

    date2 - date1

    dates = list()
    for day in xrange(0, (date2 - date1).days + 1):
        dates.append(date1 + timedelta(days=1*day))

    # os.remove('Applovin.pkl')

    test_2_bandit_params = {
        'k': 3,
        'bandit': BayesianBandit(),
        'arm': NormalArm(**{"v0": 1, "s_sq0": 1, "k0": 1, "m0": 1}),
        'sliding_window': 180,
        'start_date': date(2017, 8, 28),
        'allocation': [20, 30, 50],
    }

    test3_bandit_params = {
        'k': 3,
        'bandit': BayesianBandit(),
        'arm': BinomialArm(**{"alpha": 1, "beta": 1}),
        'sliding_window': 180,
        'start_date': date(2017, 8, 31),
        'allocation': [10, 30, 60],
    }

    report = BanditCRM(test_name='test_2', metric='cumarpu', day=2,
                       game='dragonsong', bandit_params=test_2_bandit_params, start_date=date(2017, 8, 28))
    report.run()

    report = BanditCRM(test_name='test3', metric='retention', day=0,
                       game='dragonsong', bandit_params=test3_bandit_params, start_date=date(2017, 8, 31))

    report.run()    

    # with redshift.managed_db_conn() as rdb:
    #     get_udid_table(rdb, 'dragonsongall', 'rewards', date1, date2, 1)
    #     ret_df = crm_retention_query(rdb, 'dragonsongall', date1, date2, 1)
    print 'crm test success'
