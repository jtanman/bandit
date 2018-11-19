# By: James Tan

# Date 4/25/2017

"""Draws sample means from a log normal distribution"""

from numpy import exp, log, mean
from analytics.bandit.draw_mus_and_sigmas import draw_mus_and_sigmas


def draw_log_normal_means(data, m0=0., k0=1., s_sq0=1., v0=1., n_samples=1000):
    """Function that combines data with a conjugate prior to form posterior gaussian distribution on
    the log of the data
    m0 - Guess about where the mean is.
    k0 - Certainty about m0.  Compare with number of data samples.
    s_sq0 - Number of degrees of freedom of variance.
    v0 - Scale of the sigma_squared parameter.  Compare with number of data samples."""

    # log transform the data
    log_data = log(data)
    # get samples from the posterior
    mu_samples, sig_sq_samples = draw_mus_and_sigmas(log_data, m0, k0, s_sq0, v0, n_samples)
    # transform into log-normal means
    log_normal_mean_samples = exp(mu_samples + sig_sq_samples / 2)
    return log_normal_mean_samples
