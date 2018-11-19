# By: James Tan

# Date: 4/25/2017

"""Models multi armed bandit arms as distributions"""

from analytics.bandit.draw_log_normal import draw_log_normal_means
from analytics.bandit.draw_mus_and_sigmas import draw_mus_and_sigmas
from numpy.random import beta as beta_dist
from numpy import count_nonzero
import time


class Arm(object):
    """A distribution of rewards modeled by one arm of the bandit"""
    NAME = ""

    def sample(self, data, n):
        """Abstract method to sample from an arm"""
        raise NotImplementedError


class LogNormalArm(Arm):
    """A log normal distribution for rewards
    m0 - Guess about where the mean is.
    k0 - Certainty about m0.  Compare with number of data samples.
    s_sq0 - Number of degrees of freedom of variance.
    v0 - Scale of the sigma_squared parameter.  Compare with number of data samples."""
    NAME = "lognormal"

    def __init__(self, data=None, m0=1., k0=1., s_sq0=1., v0=1.):
        self.m0 = float(m0)
        self.k0 = float(k0)
        self.s_sq0 = float(s_sq0)
        self.v0 = float(v0)
        if data is None:
            data = []
        self.data = data

    def sample(self, data=None, n=1):
        """Return n samples from distribution"""

        if data is None:
            data = self.data
        return draw_log_normal_means(data, self.m0, self.k0, self.s_sq0, self.v0, n)


class NormalArm(Arm):
    """A normal distribution for rewards
    m0 - Guess about where the mean is.
    k0 - Certainty about m0.  Compare with number of data samples.
    s_sq0 - Number of degrees of freedom of variance.
    v0 - Scale of the sigma_squared parameter.  Compare with number of data samples."""
    NAME = "normal"

    def __init__(self, data=None, m0=1., k0=1., s_sq0=1., v0=1.):
        self.m0 = float(m0)
        self.k0 = float(k0)
        self.s_sq0 = float(s_sq0)
        self.v0 = float(v0)
        if data is None:
            data = []
        self.data = data

    def sample(self, data=None, n=1):
        """Return n samples from distribution"""

        if data is None:
            data = self.data
        sample_normal_start = time.time()
        mu_samples, __ = draw_mus_and_sigmas(data, self.m0, self.k0, self.s_sq0, self.v0, n)
        sample_normal_end = time.time()
        print 'sample normal: {}'.format(sample_normal_end - sample_normal_start)
        return mu_samples


class BinomialArm(Arm):
    """A binomial distribution for rewards"""
    NAME = "binomial"

    def __init__(self, data=None, alpha=1, beta=1):
        self.alpha = alpha
        self.beta = beta
        if data is None:
            data = []
        self.data = data

    def sample(self, data=None, n=1):
        """Return n samples from distribution"""

        if data is None:
            data = self.data

        successes = count_nonzero(data)
        total = len(data)
        samples = beta_dist(self.alpha + successes, self.beta + total - successes, n)
        return samples


ALL_BANDIT_ARMS = {x.NAME: x for x in Arm.__subclasses__()}
