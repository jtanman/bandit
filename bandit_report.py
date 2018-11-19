# By: James Tan

# Date: 5/16/2017

"""Runs a bandit daily and publishes the report"""
import numpy as np
import pandas as pd
from datetime import date, timedelta
from analytics.bandit.bandit_queries import publisher_query, publisher_retention_query
from analytics.tasking.command import Command
from analytics.bandit.arm import *
from analytics.bandit.bandit import *
from analytics.bandit.environment import *
from analytics.db import redshift
from analytics.shared import ClassProperty
from analytics.db.redshift_util import RedshiftDictWriter
import cPickle as pickle
import os.path
import time
import logging

DEFAULT_START_DATE = date(2016, 1, 1)
DEFAULT_MIN_SIZE = 50
STORAGE_PATH = '/mnt/bandit'


def daterange(start_date, end_date):
    """returns iteratable from start_date to end_date exclusive of end_date"""
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


class BanditReporter(Command):
    """Base class for daily bandit reports"""

    def __init__(self, run_date=None):
        super(BanditReporter, self).__init__(no_db=True)
        if run_date is None:
            run_date = date.today()
        self.run_date = run_date

    def get_name(self):
        """Abstract method to return string describing bandit report"""
        raise NotImplementedError

    def run(self):
        """Runs daily bandit"""
        raise NotImplementedError

    @classmethod
    def is_game_specific(cls):
        return False


class ApplovinBanditReporter(BanditReporter):
    """Runs daily bandit for determining whitelist/blacklist allocations for Applovin publishers"""
    def __init__(self, start_date=None, run_date=None, sliding_window=None, ret_day=1,
                 min_size=None):
        super(ApplovinBanditReporter, self).__init__(
            run_date=run_date
        )
        self.sliding_window = sliding_window
        if start_date is None:
            if sliding_window is None:
                self.start_date = DEFAULT_START_DATE
            else:
                self.start_date = self.run_date - timedelta(days=sliding_window)
        else:
            self.start_date = start_date
        self.ret_day = ret_day
        self.min_size = DEFAULT_MIN_SIZE if min_size is None else min_size
        self.report_description = 'Applovin'
        self.channel = 'applovin'
        self.filename = self.report_description + '.pkl'
        self.path = os.path.join(STORAGE_PATH, self.filename)
        self.allocation_table = 'bandit_allocation_applovin'
        self.performance_table = 'bandit_performance_applovin'

    @ClassProperty
    @classmethod
    def default_schedule(cls):
        """Every night at 3am"""
        return "cron 0 3 * * *"

    @ClassProperty
    @classmethod
    def description(cls):
        return 'Runs a bandit reporter for Applovin publishers'

    @staticmethod
    def get_allocation_columns():
        """Column names for allocation report"""
        other_columns = ['run_date', ]
        str_columns = ['publisher', ]
        allocation_columns = ['allocation', ]
        columns = other_columns + str_columns + allocation_columns
        return columns, str_columns

    @staticmethod
    def get_performance_columns(sliding_window=None):
        """Column names for performance report"""
        other_columns = ['run_date', ]
        str_columns = ['publisher', ]
        if sliding_window is not None:
            performance_columns = ['len', 'mean', 'std', 'sem', 'len_sw', 'mean_sw', 'std_sw', 'sem_sw', ]
        else:
            performance_columns = ['len', 'mean', 'std', 'sem', ]
        columns = other_columns + str_columns + performance_columns
        return columns, str_columns

    def get_name(self):
        """Return description of bandit reporter"""
        return self.report_description

    def run(self):
        """Runs a bandit reporter for Applovin publishers"""

        if os.path.isfile(self.path):
            logging.info('environment already exists')
            with open(self.path, 'rb') as pickle_input:
                env = pickle.load(pickle_input)

            if self.run_date > env.run_date:
                self.update(env, env.run_date, self.run_date)

        else:
            env = self.backfill()

        logging.info('generating reports')

        # parse reports into format for writing to redshift
        allocation_report = env.get_allocation()
        allocation_report = pd.DataFrame({'run_date': self.run_date,
                                          'publisher': allocation_report.index,
                                          'allocation': allocation_report.tolist()})
        allocation_report = allocation_report.to_dict('records')

        performance_report = env.get_performance(sort=True, sliding_window=False)
        performance_report.len = performance_report.len.apply(int)
        if self.sliding_window is None:
            pass
        else:
            performance_report_sw = env.get_performance(sort=True, sliding_window=True)
            performance_report_sw.len = performance_report_sw.len.apply(int)
            performance_report_sw.rename(columns={'len': 'len_sw',
                                                  'mean': 'mean_sw',
                                                  'std': 'std_sw',
                                                  'sem': 'sem_sw'
                                                 }, inplace=True)
            performance_report = pd.merge(performance_report, performance_report_sw, how='left', on='name')
        performance_report.fillna(0, inplace=True)
        performance_report['run_date'] = self.run_date
        performance_report.rename(columns={'name': 'publisher'}, inplace=True)
        performance_report = performance_report.to_dict('records')

        # write to redshift
        with redshift.managed_db_conn() as rdb:
            columns, str_columns = self.get_allocation_columns()
            with RedshiftDictWriter(columns=columns, str_columns=str_columns) as writer:

                logging.info('Writing to Applovin allocation table')
                for row in allocation_report:
                    writer.writerow(row)
                writer.copy_to_table(rdb.cur, 'public', self.allocation_table)
                rdb.conn.commit()

            columns, str_columns = self.get_performance_columns(self.sliding_window)
            with RedshiftDictWriter(columns=columns, str_columns=str_columns) as writer:

                logging.info('Writing to Applovin performance table')
                for row in performance_report:
                    writer.writerow(row)
                writer.copy_to_table(rdb.cur, 'public', self.performance_table)
                rdb.conn.commit()

    def update(self, env, start_date=None, run_date=None):
        """update environment with data from users installing on start_date to data received
        by run_date"""

        with redshift.managed_db_conn() as rdb:
            update_data = publisher_retention_query(rdb, self.channel, start_date, run_date,
                                                    self.ret_day)

        pubs = update_data.publisher.unique()
        new_pubs = [p for p in pubs if p not in env.get_arm_names()]

        for p in new_pubs:
            env.add_arm(name=p)

        update_data_input = [None] * env.k

        for i, pub in enumerate(env.get_arm_names()):
            pub_df = update_data[update_data.publisher == pub]
            pub_series = pub_df.value
            pub_series.index = pub_df.date
            update_data_input[i] = pub_series

        env.run_cycle(run_date=run_date, new_data=update_data_input, min_size=self.min_size)

        with open(self.path, 'wb') as output:
            pickle.dump(env, output, -1)

        return env

    def backfill(self):
        """Create environment with data from users from start_date to run_date"""

        with redshift.managed_db_conn() as rdb:
            publishers = publisher_query(rdb, self.channel, self.start_date, self.run_date,
                                         self.ret_day)['publisher']

        publisher_list = publishers.tolist()

        k = len(publisher_list)

        env = Environment(k, BayesianBandit(), BinomialArm(alpha=1, beta=2),
                          arm_names=publisher_list, start_date=self.start_date,
                          run_date=self.run_date, sliding_window=self.sliding_window, batch=1000,
                          label='Applovin Bayesian Bandit')

        with redshift.managed_db_conn() as rdb:
            historical_data = publisher_retention_query(rdb, self.channel, self.start_date,
                                                        self.run_date, self.ret_day)

        historical_data_input = [None] * k

        for i, pub in enumerate(publisher_list):
            pub_df = historical_data[historical_data.publisher == pub]
            pub_series = pub_df.value
            pub_series.index = pub_df.date
            historical_data_input[i] = pub_series

        env.run_cycle(run_date=self.run_date, new_data=historical_data_input,
                      min_size=self.min_size)

        with open(self.path, 'wb') as output:
            pickle.dump(env, output, -1)

        return env
