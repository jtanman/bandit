# By: James Tan

# Date: 4/25/2017

"""Environment to run a single multi arm bandit"""

import numpy as np
# import seaborn as sns
import scipy.stats as stats
import pandas as pd
from datetime import timedelta, date
from analytics.bandit.arm import BinomialArm

DEFAULT_BATCH_SIZE = 1000


def equal_allocation(k, batch):
    """Generates an equal allocation for all arms given no prior allocation"""
    allocation_per_arm = batch / k
    diff = batch - k * allocation_per_arm
    allocation = np.empty(0, dtype=int)
    for i in xrange(k):
        addition = np.repeat(i, allocation_per_arm)
        allocation = np.append(allocation, addition)
    for i in xrange(diff):
        allocation = np.append(allocation, i)

    return pd.Series(allocation)


def get_binom_test_data(run_date, k, binom_ps, allocation, data=None):
    """get test data for running experiments"""

    data = [None] * k
    for i in xrange(k):

        if i not in allocation.value_counts().index:
            continue
        pulls = allocation.value_counts(sort=False)[i]
        temp_data = np.random.binomial(1, binom_ps[i], pulls)
        temp_dates = np.repeat(run_date, pulls)
        data[i] = pd.Series(temp_data, index=temp_dates)

    return data


def get_normal_test_data(run_date, k, mus, sigmas, allocation):
    """get test data for running experiments"""

    data = [None] * k
    for i in xrange(k):

        if i not in allocation.value_counts().index:
            continue
        pulls = allocation.value_counts(sort=False)[i]
        temp_data = np.random.normal(mus[i], sigmas[i], pulls)
        temp_dates = np.repeat(run_date, pulls)
        data[i] = pd.Series(temp_data, index=temp_dates)

    return data


def parse_allocation(allocation, batch, precision=0):
    """Parse either percentage allocations or relative size allocations into rounded integer
    allocations of size batch
    precision determines how many optional decimal points to include"""

    if precision > 0:
        batch = batch * (10 ** precision)

    rounded_allocs = [x * batch / float(sum(allocation)) for x in allocation]
    dec_allocs = [x - int(x) for x in rounded_allocs]
    floor_allocs = [int(x) for x in rounded_allocs]

    diff = batch - sum(floor_allocs)
    sorted_dec = [i[0] for i in sorted(enumerate(dec_allocs), key=lambda x: x[1], reverse=True)]

    for i in xrange(diff):
        floor_allocs[sorted_dec[i]] += 1

    if precision > 0:
        floor_allocs = [float(x) / (10 ** precision) for x in floor_allocs]

    return floor_allocs


class Environment(object):
    """The base class for a multi armed bandit experiment. It contains data for each of the arms,
    the algorithm to select the next allocation, and the ability to pull and update data."""

    def __init__(self, k, bandit, arm, arm_names=None, start_date=None, run_date=None, data=None,
                 sliding_window=None, batch=None, allocation=None, label='Multi-Armed Bandit',
                 test_vars=None, print_progress=None):
        self.start_date = start_date if start_date is not None else date.today()
        self.run_date = run_date if run_date is not None else self.start_date
        self.k = k
        self.bandit = bandit
        self.arm = arm
        self.arm_names = arm_names if arm_names is not None else map(str, range(self.k))
        self.data = data if data is not None else [pd.Series()] * k
        self.sw_data = self.data
        self.sliding_window = sliding_window
        self.batch = batch if batch is not None else DEFAULT_BATCH_SIZE
        if allocation is None:
            allocation = equal_allocation(self.k, self.batch)
        elif len(allocation) == k:
            allocation = parse_allocation(allocation, self.batch)
            allocation = pd.Series([i for i, x in enumerate(allocation) for _ in xrange(x)])
        elif len(allocation) == batch and all(x in range(self.k) for x in allocation):
            pass
        else:
            raise RuntimeError('Incorrect input for allocation. Formats include relative sizes for\
                each arm, vector of size batch with every element equal to one arm, or None\
                defaulting to equal allocations')
        self.allocation = allocation
        self.label = label
        self.test_vars = test_vars
        self.print_progress = print_progress if print_progress is not None else False

    def get_data(self, df=False):
        """Returns current data
        df returns data in dataframe format"""
        if df:
            data = pd.DataFrame()
            for i in xrange(self.k):
                temp_dict = dict(value=self.data[i], date=self.data[i].index, shard=i,
                                 name=self.arm_names[i])
                temp_df = pd.DataFrame(temp_dict)
                data = data.append(temp_df, ignore_index=True)
            return data

        return self.data

    def data_empty(self):
        """Returns true if there is no data in the bandit so far and false if there is any data"""
        return all(d.empty for d in self.data)

    def get_run_date(self):
        """Returns environment's current run date"""
        return self.run_date

    def get_label(self):
        """Returns environment's label"""
        return self.label

    def get_arm_names(self):
        """Returns environment's arm names"""
        return self.arm_names

    def update_run_date(self, run_date=None, incremental=False):
        """Updates run date after every cycle. If incremental, updates run_date by 1, mainly used
        for testing and generating test data. Otherwise, updates to one past the last day data was
        collected."""
        if run_date is not None:
            self.run_date = run_date
            return

        if incremental:
            self.run_date = self.run_date + timedelta(days=1)
        else:
            max_dates = [max(data.index) for data in self.data]
            max_date = max(max_dates)
            self.run_date = max_date + timedelta(days=1)

    def get_allocation(self, allocation=None, count=True, sort=True, names=True):
        """returns allocation in a table format"""

        if allocation is None:
            allocation = self.allocation

        if count:
            alloc = pd.Series([0] * self.k)
            counts = allocation.value_counts()
            alloc.update(counts)
            if sort:
                alloc.sort_values(ascending=False, inplace=True)
            if names:
                alloc.index = [self.arm_names[i] for i in alloc.index]
            return alloc
        else:
            if names:
                return [self.arm_names[i] for i in allocation]

            return allocation

    def get_performance(self, sort=False, sliding_window=False, min_size=None):
        """Returns performance of arms for the data
        sort: sorts final dataframe by length of data and mean
        sliding_window: only calculates performance of data within the sliding window"""

        data = pd.DataFrame()

        for i in xrange(self.k):
            temp_dict = dict(value=self.data[i], date=self.data[i].index, shard=i,
                             name=self.arm_names[i])
            temp_df = pd.DataFrame(temp_dict)
            data = data.append(temp_df, ignore_index=True)

        if sliding_window and self.sliding_window is not None:
            data = data[
                data['date'] >= self.run_date - timedelta(days=self.sliding_window)
            ]

        grouped = data.groupby(['shard', 'name'])
        perf = grouped.agg([len, np.mean, np.std, stats.sem]).value

        if min_size is not None:
            perf = perf[perf.len >= min_size]

        if sort:
            perf = perf.sort(['len', 'mean'], ascending=[False, False])

        perf = perf.reset_index(level='name')

        return perf

    def add_arm(self, name=None, data=None):
        """Add a new arm to the bandit. Data must be a pandas series indexed by date collected."""

        self.k += 1
        self.arm_names.append(name)
        if data is None:
            self.data.append(pd.Series())
        else:
            self.data.append(data)

    def calculate_allocation(self, data=None, run_date=None, sliding_window=None, n=None,
                             min_size=None):
        """Returns a new allocation of shards based on the current data"""

        # can customize variables for testing, otherwise use default
        data = self.data if data is None else data
        run_date = self.run_date if run_date is None else run_date
        sliding_window = self.sliding_window if sliding_window is None else sliding_window
        n = self.batch if n is None else n

        if min_size is not None:
            data_lengths = [len(i) for i in data]
            indexes = [i for i, length in enumerate(data_lengths) if length >= min_size]
            k = len(indexes)
            filter_data = [data[i] for i in indexes]
            allocation = self.bandit.select_arm(k, self.arm, filter_data, self.allocation,
                                                self.start_date, run_date, sliding_window, n)
            allocation = [indexes[i] for i in allocation]
            return pd.Series(allocation)

        return self.bandit.select_arm(self.k, self.arm, data, self.allocation, self.start_date,
                                      run_date, sliding_window, n)

    def run_cycle(self, num_cycle=None, run_date=None, new_data=None, incremental=False,
                  min_size=None):
        """Runs one cycle of the multi armed bandit experiment by using given data or pulling in
        new data and determining new allocations"""

        if self.print_progress:
            print 'iteration: {}'.format(num_cycle)
            print self.get_allocation()

        if new_data is None:
            if isinstance(self.arm, BinomialArm):
                # binomial test
                new_data = get_binom_test_data(self.run_date, self.k, self.test_vars['binom_ps'],
                                               self.allocation)
            else:
                # normal test
                new_data = get_normal_test_data(self.run_date, self.k, self.test_vars['mus'],
                                                self.test_vars['sigmas'], self.allocation)

        for i in xrange(self.k):
            self.data[i] = self.data[i].append(new_data[i])

        if not self.data_empty():
            self.allocation = self.calculate_allocation(min_size=min_size)
        self.update_run_date(run_date=run_date, incremental=incremental)

        return self.allocation.value_counts(sort=False)

    def run(self):
        """Runs cycles of the bandit until it reaches a stopping point given in test_vars
        Mainly used for testing"""
        for i in xrange(self.test_vars['cycle_stop']):
            self.run_cycle(num_cycle=i, incremental=True)
        return 'Done'
