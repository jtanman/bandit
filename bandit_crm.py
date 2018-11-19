# By: James Tan

# Date: 6/1/2017

"""Manages a bandit environment using the analytics crm system"""

from analytics.bandit.environment import Environment, parse_allocation
from analytics.bandit.bandit_queries import get_udid_table, crm_retention_query,\
    crm_conversion_query, crm_cumarpu_query
from datetime import date, timedelta
from analytics.db import redshift
from analytics.db.redshift_util import RedshiftDictWriter
import os.path
import cPickle as pickle
import logging
import pandas as pd

STORAGE_PATH = '/mnt/bandit'
POST_BATCH_SIZE = 100


class BaseBanditCRM(object):
    """Base class for daily crm bandit"""

    def __init__(self, run_date=None):
        self.run_date = run_date if run_date is not None else date.today()

    def get_test_name(self):
        """Abstract method to return string describing bandit report"""
        raise NotImplementedError

    def run(self):
        """Runs daily bandit"""
        raise NotImplementedError


class BanditCRM(BaseBanditCRM):
    """Base class for daily bandit crm"""

    def __init__(self, test_name, metric, day, game, bandit_params,
                 metric_query=None, start_date=None, run_date=None):
        super(BanditCRM, self).__init__(
            run_date=run_date
        )
        self.k = bandit_params['k']
        self.test_name = test_name
        self.metric = metric
        self.metric_query = metric_query
        self.day = day
        self.game = game
        self.bandit_params = bandit_params
        self.start_date = start_date
        self.filename = 'crm2_' + self.test_name + '.pkl'
        self.path = os.path.join(STORAGE_PATH, self.filename)
        self.allocation_table = 'crm2groupchanges'

    def get_test_name(self):
        """Returns string describing bandit crm report"""
        return self.test_name

    @staticmethod
    def get_allocation_columns():
        """Column names for allocation report"""
        other_columns = ['date', ]
        str_columns = ['group_name', 'sub_group', ]
        allocation_columns = ['allocation', ]
        columns = other_columns + str_columns + allocation_columns
        return columns, str_columns

    def run(self):
        """Runs daily crm bandit"""
        if os.path.isfile(self.path):
            logging.info('updating bandit')
            with open(self.path, 'rb') as pickle_input:
                env = pickle.load(pickle_input)

            if self.start_date is None:
                self.start_date = env.start_date
            if self.run_date > env.run_date:
                self.update(env, env.run_date - timedelta(days=self.day), self.run_date)

        else:
            logging.info('creating and backfilling bandit')
            if self.start_date is None:
                self.start_date = date.today()
            env = self.backfill()

        allocation_report = env.get_allocation(sort=False)
        allocation_report = pd.DataFrame({'date': self.run_date,
                                          'group_name': self.test_name,
                                          'sub_group': allocation_report.index,
                                          'allocation': parse_allocation(allocation_report,
                                                                         POST_BATCH_SIZE, 2)})
        allocation_report = allocation_report.to_dict('records')

        with redshift.managed_db_conn() as rdb:
            columns, str_columns = self.get_allocation_columns()
            with RedshiftDictWriter(columns=columns, str_columns=str_columns) as writer:

                logging.info('Writing to crm2 allocation table')
                for row in allocation_report:
                    writer.writerow(row)
                writer.copy_to_table(rdb.cur, 'public', self.allocation_table)
                rdb.conn.commit()

        return parse_allocation(env.get_allocation(sort=False), POST_BATCH_SIZE, 2)

    def get_crm_data(self, start_date=None, run_date=None):
        """Get data for new environment or to update an existing environment"""

        if start_date is None:
            start_date = self.start_date

        if run_date is None:
            run_date = self.run_date

        with redshift.managed_db_conn() as rdb:
            get_udid_table(rdb, self.game, self.test_name, start_date, run_date, self.day)
            if self.metric == 'retention':
                crm_data = crm_retention_query(rdb, self.game, self.day)
            elif self.metric == 'cumarpu':
                crm_data = crm_cumarpu_query(rdb, self.game, self.day)
            elif self.metric == 'conversion':
                crm_data = crm_conversion_query(rdb, self.game, self.day)
            elif self.metric == 'custom':
                # code for custom query
                return
            else:
                raise RuntimeError('Incorrect input for metric. Formats include retention, cumarpu,\
                                    conversion, or custom with self provided metric')

        return crm_data

    def backfill(self):
        """Create environment for the ab test"""

        env = Environment(**self.bandit_params)
        env = self.update(env, self.start_date, self.run_date)

        return env

    def update(self, env, start_date=None, run_date=None):
        """update environment with data from users installing on start_date to data received
        by run_date"""

        logging.info('Getting CRM user data')
        update_data = self.get_crm_data(start_date, run_date)

        update_data_input = [None] * self.k

        if not update_data.empty:
            for i in xrange(self.k):
                arm_df = update_data[update_data.shard == env.get_arm_names()[i]]
                arm_series = arm_df.value
                arm_series.index = arm_df.date
                update_data_input[i] = arm_series

        logging.info('Updating bandit environment')
        env.run_cycle(run_date=self.run_date, new_data=update_data_input)

        with open(self.path, 'wb') as output:
            pickle.dump(env, output, -1)

        return env
