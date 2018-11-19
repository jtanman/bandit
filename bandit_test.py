# By: James Tan

# Date: 4/26/2017

"""
Simulates and tests a multi armed bandit experiment
"""
from analytics.bandit.arm import *
from analytics.bandit.bandit import *
from analytics.bandit.environment import *
from analytics.bandit.experiment import *
from datetime import *
import pandas as pd
import numpy as np
from analytics.tasking.command.ltv_helpers.ltv_fetch import return_query_as_df
from analytics.db import pgdb
import time
import timeit
from scipy.stats import norm, invgamma


if __name__ == '__main__':

    # with pgdb.managed_db_conn() as db:
    #     query = 'select * from abtest.abtestquerytemplate'
    #     df = return_query_as_df(db, query)

    # import ipdb; ipdb.set_trace()

    SLIDING_WINDOW = None

    date1 = date(2017, 4, 1)
    date2 = date(2017, 4, 25)

    date2 - date1

    dates = list()
    for day in xrange(0, (date2 - date1).days + 1):
        dates.append(date1 + timedelta(days=1*day))

    bandit = EpsilonGreedyBandit(epsilon=.05)
    arm = BinomialArm()

    bandit = NaiveBandit(n_days=30, both=False)

    # bandit = BayesianBandit()

    ps = [.1, .11, .12, .08, .085]
    k = len(ps)
    BATCH = 1000
    allocs = [.3, 0, .53, .005, .165]
    int_allocs = [1, 2, 3, 4, 5]
    mus = [30, 35, 33, 38, 25]
    sigmas = [5, 4, 6, 2, 3]

    test_vars1 = dict(binom_ps=ps, mus=mus, sigmas=sigmas, cycle_stop=120)

    env1 = Environment(k, BayesianBandit(), BinomialArm(), start_date=date1,
                      sliding_window=SLIDING_WINDOW, batch=1000, label='bayesian',
                      test_vars=test_vars1, print_progress=True)

    # env2 = Environment(k, EpsilonGreedyBandit(epsilon=.05), arm, start_date=date1,
    #                   sliding_window=SLIDING_WINDOW, batch=BATCH, label='epsilon greedy',
    #                   test_vars=test_vars1)

    # env3 = Environment(k, NaiveBandit(n_days=30), arm, start_date=date1,
    #                   sliding_window=SLIDING_WINDOW, batch=BATCH, label='ab test 30d',
    #                   test_vars=test_vars1)

    # env4 = Environment(k, RandomBandit(), arm, start_date=date1,
    #                   sliding_window=SLIDING_WINDOW, batch=BATCH, label='random',
    #                   test_vars=test_vars1)

    # exp = Experiment([env1, env2, env3, env4], 120)
    # exp.run()

    # import ipdb; ipdb.set_trace()

    env1.run()

    print env1.get_performance()
    print env1.get_allocation()

    import ipdb; ipdb.set_trace()


    a = BinomialArm()

    a = pd.DataFrame()
    b = pd.DataFrame()
    c = pd.DataFrame()

    for d in dates:

        # pull values for a
        a_temp = np.random.binomial(1, .2, 5)
        a_date = np.repeat(d, len(a_temp))
        a_dict = dict(value=a_temp, date=a_date, shard=0)
        a = a.append(pd.DataFrame(a_dict), ignore_index=True)
        # pull values for b
        b_temp = np.random.binomial(1, .3, 10)
        b_date = np.repeat(d, len(b_temp))
        b_dict = dict(value=b_temp, date=b_date, shard=1)
        b = b.append(pd.DataFrame(b_dict), ignore_index=True)
        # pull values for c
        c_temp = np.random.binomial(1, .15, 2)
        c_date = np.repeat(d, len(c_temp))
        c_dict = dict(value=c_temp, date=c_date, shard=2)
        c = c.append(pd.DataFrame(c_dict), ignore_index=True)


    # a = np.random.binomial(1, .2, 500)
    # a_index = np.concatenate((np.repeat(pd.datetime(2016,1,1), 200), np.repeat(pd.datetime(2016,1,2), 300)))
    # a_dict = dict(value=a, date=a_index, shard='a')
    # a_df = pd.DataFrame(a_dict)

    df = pd.DataFrame()
    df = df.append(a, ignore_index=True)
    df = df.append(b, ignore_index=True)
    df = df.append(c, ignore_index=True)

    data_a = df[(df.shard == 0) & (df.date >= date.today() - timedelta(days=SLIDING_WINDOW))].value
    data_b = df[(df.shard == 1) & (df.date >= date.today() - timedelta(days=SLIDING_WINDOW))].value
    data_c = df[(df.shard == 2) & (df.date >= date.today() - timedelta(days=SLIDING_WINDOW))].value

    binom_arm = BinomialArm()
    binom_arm.sample(data_a, 1000)
    binom_arm.sample(data_b)
    binom_arm.sample(data_c)
