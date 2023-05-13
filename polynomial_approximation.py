import numpy as np
from scipy.stats import ttest_ind
import pandas as pd

def fit_polynomial_function(time_series, power=2):
    # Create arrays for the independent and dependent variables
    x = np.arange(len(time_series))
    y = np.array(time_series)

    # Fit the data to a defined degree polynomial
    coeffs = np.polyfit(x, y, power)

    # Generate the defined degree polynomial function
    power_func = np.poly1d(coeffs)

    # Evaluate the function at each point in the time series
    fitted_series = power_func(x)

    return power_func, fitted_series

def correlation_stats_of_dists(y1, y2):
    # Compute the correlation coefficient between the two functions
    corr = np.corrcoef(y1, y2)[0, 1]

    # Compute the slope and intercept of the regression line
    slope = corr * np.std(y1) / np.std(y2)
    intercept = np.mean(y1) - slope * np.mean(y2)
    
    return {'corr': corr,
            'slope': slope,
            'intercept': intercept}

def approximate_function(function, statistics):
    slope = statistics['slope']
    intercept = statistics['intercept']
    return np.poly1d(slope * function + intercept)