# bandit

An implementation of multi arm bandits that can handle binomial, normal, and log normal distributions. Supported bandit algorithms include naive bandit (test a certain number of samples/cycles before picking the best one), epsilon greedy, bayesian, and randomization (for testing).

This implementation also supports batch jobs, delayed feedback, generating testing data, ongoing performance and status reports, sliding windows for regime changes, and initial allocations.