# By: James Tan

# Date: 4/25/2017

"""Experiment to test efficiency and savings of multi armed bandits over many trials"""

from analytics.bandit.arm import *
from analytics.bandit.bandit import *
from analytics.bandit.environment import *


class Experiment(object):
    """Base class for measuring performance of multi armed bandit algorithms."""

    def __init__(self, environments, cycles):
        self.environments = environments
        self.cycles = cycles

    def __str__(self):
        return 'Experiment to test and measure effectiveness of multi arm bandit algorithms'

    def run(self):
        """Function to run the various multi arm bandit experiments and output the results"""
        n = len(self.environments)
        optimal = [0] * n

        regret_report = []

        for i in xrange(n):
            env = self.environments[i]
            if isinstance(env.arm, BinomialArm):
                temp_optimal = max(env.test_vars['binom_ps'])
            else:
                temp_optimal = max(env.test_vars['mus'])
            optimal[i] = temp_optimal

        for k in xrange(self.cycles):
            print k
            for i in xrange(n):
                env = self.environments[i]
                env.run_cycle()
                perf = env.get_performance()
                perf['reward'] = perf['len'] * perf['mean']
                pulls = sum(perf['len'])
                regret = optimal[i] * pulls - sum(perf['reward'])
                bandit_date = env.get_run_date() - timedelta(days=1)

                regret_report.append(dict(env=i, label=env.label, date=bandit_date, pulls=pulls,
                                          optimal=optimal[i] * pulls, reward=sum(perf['reward']),
                                          regret=regret))

        regret_df = pd.DataFrame(regret_report)

        regret_df.to_csv('regret_df.csv')

        return regret_df
