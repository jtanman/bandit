# By: James Tan

# Date 4/25/2017

"""Draws sample means from a normal distribution"""

from numpy import sum, mean, size, sqrt, array
from scipy.stats import norm, invgamma
import timeit, time


def draw_mus_and_sigmas(data, m0=0., k0=1., s_sq0=1., v0=1., n_samples=1000):
    """Function that combines data with a conjugate prior to form posterior gaussian distribution
    m0 - Guess about where the mean is.
    k0 - Certainty about m0.  Compare with number of data samples.
    s_sq0 - Number of degrees of freedom of variance.
    v0 - Scale of the sigma_squared parameter.  Compare with number of data samples."""
    # draw_start = time.time()
    # number of samples
    data = array(data)
    # setup = """
    # from numpy import sum, mean, size, sqrt, array;
    # from scipy.stats import norm, invgamma;
    # data = array(%r)
    # """ % data.tolist()

    N = size(data)
    if N == 0:
        mu_samples = norm.rvs(m0, scale=s_sq0, size=n_samples)
        sig_sq_samples = (v0 * s_sq0 / 2) * invgamma.rvs(v0 / 2, size=n_samples)
        return mu_samples, sig_sq_samples

    # find the mean of the data

    # math_start = time.time()
    the_mean = mean(data)
    # sum of squared differences between data and mean
    SSD = sum((data - the_mean)**2)

    # combining the prior with the data - page 79 of Gelman et al.
    # to make sense of this note that
    # inv-chi-sq(v,s^2) = inv-gamma(v/2,(v*s^2)/2)
    kN = float(k0 + N)
    mN = (k0 / kN) * m0 + (N / kN) * the_mean
    vN = v0 + N
    vN_times_s_sqN = v0 * s_sq0 + SSD + (N * k0 * (m0 - the_mean)**2) / kN

    # 1) draw the variances from an inverse gamma
    # (params: alpha, beta)
    alpha = vN / 2
    beta = vN_times_s_sqN / 2

    # math_end = time.time()
    # thanks to wikipedia, we know that:
    # if X ~ inv-gamma(a,1) then b*X ~ inv-gamma(a,b)
    # gamma_start = time.time()
    sig_sq_samples = beta * invgamma.rvs(alpha, size=n_samples)
    # gamma_end = time.time()

    # 2) draw means from a normal conditioned on the drawn sigmas
    # (params: mean_norm, var_norm)
    mean_norm = mN
    var_norm = sqrt(sig_sq_samples / kN)
    # norm_start = time.time()
    mu_samples = norm.rvs(mean_norm, scale=var_norm, size=n_samples)
    # norm_end = time.time()
    # draw_end = time.time()
    # print 'draw: {}, math:{}, gamma_sample: {}, norm_sample: {}'.format(
    #     draw_end - draw_start, math_end - math_start, gamma_end - gamma_start,
    #     norm_end - norm_start,
    # )
    # 3) return the mu_samples and sig_sq_samples
    return mu_samples, sig_sq_samples
