# By: James Tan

# Date: 4/25/2017

"""Algorithms for selecting arms for multi armed bandit"""

import numpy as np
import pandas as pd
from datetime import timedelta
import time


class Bandit(object):
    """An algorithm for determining based on previous rewards and history how to select an action"""
    NAME = ""

    def __str__(self):
        return 'generic bandit'

    def select_arm(self, k, arm, data, allocation, start_date, run_date, sliding_window, batch):
        """Abstract method to select an arm based on the current experiment and n-batch size"""
        raise NotImplementedError

    @staticmethod
    def filter_data(k, data, run_date, sliding_window):
        """Filters data for only data within the last sliding_window days"""
        if sliding_window is not None:
            for i in xrange(k):
                data[i] = data[i][data[i].index >= run_date - timedelta(days=sliding_window)]

        return data


class RandomBandit(Bandit):
    """This policy will choose an arm at random. This is mainly for testing"""
    NAME = "random"

    def __str__(self):
        return 'random bandit'

    def select_arm(self, k, arm, data, allocation, start_date, run_date, sliding_window, batch):
        return pd.Series(np.random.randint(low=0, high=k, size=batch))


class NaiveBandit(Bandit):
    """This policy will follow the initial allocation until a certain point in time at which it will
    then choose the best performing arm from there on out. This policy is equivalent to an ab test
    where the user eventually picks one arm to use. This policy does not implement a sliding
    window, and if two arms are tied at the end of the testing period, it chooses the first one."""
    NAME = "naive"

    def __init__(self, n_days=None, n_pulls=None, both=False):
        """The naive bandit will either require n_days of testing or n_pulls from all arms
        combined. If both is True, then it will require at least n_days AND n_pulls."""
        if n_days is None and n_pulls is None:
            raise RuntimeError('No parameters for end of ab test set')
        if both and (n_days is None or n_pulls is None):
            raise RuntimeError('Tried to use both limits but at least one is not set')
        self.n_days = n_days
        self.n_pulls = n_pulls
        self.both = both

    def __str__(self):
        return 'naive bandit'

    def select_arm(self, k, arm, data, allocation, start_date, run_date, sliding_window, batch):
        num_days = (run_date - start_date).days
        data_sizes = [len(i) for i in data]
        pulls = sum(data_sizes)
        test_done = False
        if self.both:
            if num_days > self.n_days and pulls > self.n_pulls:
                test_done = True
        else:
            if self.n_days is not None and num_days > self.n_days:
                test_done = True
            if self.n_pulls is not None and pulls > self.n_pulls:
                test_done = True

        if test_done:
            data_means = [np.mean(i) for i in data]
            best_arm = np.argmax(data_means)
            allocation = pd.Series(best_arm).repeat(batch)

        return allocation


class EpsilonGreedyBandit(Bandit):
    """
    The Epsilon-Greedy policy will choose a random action with probability
    epsilon and take the best apparent approach with probability 1-epsilon. If
    multiple actions are tied for best choice, then a random action from that
    subset is selected.
    """
    NAME = "epsilon"

    def __init__(self, epsilon=.05):
        self.epsilon = epsilon

    def __str__(self):
        return (u'\u03B5-greedy (\u03B5={})'.format(self.epsilon)).encode('utf-8')

    def select_arm(self, k, arm, data, allocation, start_date, run_date, sliding_window, batch):
        choose_epsilon = np.random.random(batch) < self.epsilon

        data = self.filter_data(k, data, run_date, sliding_window)

        data_means = [np.mean(i) for i in data]
        max_arm = max(data_means)
        is_max_arm = (data_means == max_arm)

        if sum(is_max_arm) > 1:
            best_arms = [i for i, b in enumerate(is_max_arm) if b]
            best_arms_allocation = np.random.choice(best_arms, size=batch)
            random_arms = np.random.randint(low=0, high=k, size=batch)
            allocation = np.where(choose_epsilon, random_arms, best_arms_allocation)
        else:
            best_arm = np.argmax(data_means)
            random_arms = np.random.randint(low=0, high=k, size=batch)
            allocation = np.where(choose_epsilon, random_arms, best_arm)

        return pd.Series(allocation)


class BayesianBandit(Bandit):
    """
    The Bayesian bandit will build a posterior distribution using a conjugate prior and observed
    rewards. It will then sample from these distributions and choose the arm with the highest
    sampled reward.
    """
    NAME = "bayesian"

    def __str__(self):
        return 'bayesian bandit'

    def select_arm(self, k, arm, data, allocation, start_date, run_date, sliding_window, batch):
        results = pd.DataFrame()

        filter_start = time.time()
        data = self.filter_data(k, data, run_date, sliding_window)
        filter_end = time.time()

        filter_time = filter_end - filter_start
        sample_time = 0

        for i in xrange(k):
            filter_start = time.time()
            data_arm = data[i]
            filter_end = time.time()
            filter_time = filter_time + filter_end - filter_start
            start_sample = time.time()
            samples = arm.sample(data_arm, batch)
            end_sample = time.time()
            results[i] = samples
            sample_time = sample_time + (end_sample - start_sample)

        allocation = results.idxmax(axis=1)

        return allocation


ALL_BANDIT_MODELS = {x.NAME: x for x in Bandit.__subclasses__()}
