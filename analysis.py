import scipy.stats as stats
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from get_collected_data import (get_coin_data_closing, get_market_data_containing,
                                get_market_data_by_list)
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import Pipeline
import numpy as np
import statsmodels.api as sm
import warnings


warnings.simplefilter("ignore", UserWarning)

sns.set(color_codes=True)


PEARSON = 'pearson'
P_THRESH = 0.005
SHAPIRO = 'shapiro'

REJECTION_STRING = 'Hypothesis about normality distribution {} be rejected!'


def perform_normality_test(data, method=PEARSON):
    """
    Runs normality test on given data and visualize relevant factors

    Argument:
        data - DataFrame containing data for analysis

    Returns:
        True - if hypothesis about normality distribution can be rejected
        False - otherwise

    """
    if method == PEARSON:
        # k2, p
        normality_stats = stats.normaltest(data)
        # if p is under theoretical threshold, reject normality hypothesis
        if normality_stats[1] < P_THRESH:
            msg = REJECTION_STRING.format('can')
        else:
            msg = REJECTION_STRING.format("can't")

    elif method == SHAPIRO:
        if data.shape[0] < 5001:
            normality_stats = stats.shapiro(data)
            if normality_stats[1] < P_THRESH:
                msg = REJECTION_STRING.format('can')
            else:
                msg = REJECTION_STRING.format("can't")
        else:
            return 'Shapiro test is not perfomed, p-value may not be accurate for N > 5000'

    return normality_stats, msg


def visualization_for_normality(data):
    df_ax = data.plot.hist(bins=100, label=data.name)
    df_ax.set(xlabel='Values', ylabel='Frequency')
    df_ax.legend()
    plt.show()

    ax = sns.distplot(data)
    ax.set(xlabel='Values', ylabel='Distribution')
    ax.set_title('Univariate distribution')
    ax.legend()
    plt.show()


def color_negative_red(val):
    """
    Takes a scalar and returns a string with
    the css property `'color: red'` for negative
    strings, black otherwise.
    """
    color = 'red' if val < 0 else 'black'
    return 'color: %s' % color


def highlight_max(s):
    """"
    highlight the maximum in a Series yellow.
    """

    is_max = s == s[s != 1].max()
    return ['background-color: yellow' if v else '' for v in is_max]


def magnify():
    return [dict(selector="th",
                 props=[("font-size", "7pt")]),
            dict(selector="td",
                 props=[('padding', "0em 0em")]),
            dict(selector="th:hover",
                 props=[("font-size", "12pt")]),
            dict(selector="tr:hover td:hover",
                 props=[('max-width', '200px'),
                        ('font-size', '12pt')])
            ]


def analyse_normality(data):
    print(data.describe())
    visualization_for_normality(data)
    print('Pearson normality test: ')
    print(perform_normality_test(data, PEARSON))
    print('Shapiro normality test: ')
    print(perform_normality_test(data, SHAPIRO))


def analyse_normality_per_coin(coin_name):
    try:
        data = get_coin_data_closing(coin_name)
        analyse_normality(data[coin_name])

    except Exception as e:
        print('Error {} occurred during data retrieve/analysis'.format(e))


def check_correlation(data, method='pearson'):
    """
    NOTES:
    A Pearson's correlation is used when there are two quantitative variables.
    The possible research hypotheses are that there is a  linear relationship between the variables

    """
    correlation = data.corr(method=method)

    writer = pd.ExcelWriter('correlation_matrix.xlsx')
    s = correlation.style. \
        applymap(color_negative_red). \
        apply(highlight_max).to_excel(excel_writer=writer)
    writer.save()

    cmap = sns.diverging_palette(3, 250, as_cmap=True)

    # plot the heatmap
    plt.figure(figsize=(10, 10))
    ax = sns.heatmap(correlation,
                     xticklabels=correlation.columns,
                     yticklabels=correlation.columns,
                     annot=True,
                     cmap=cmap)
    ax.set_title('Correlation analysis')
    plt.show()

    return correlation


def analyse_correlation_coin_list(coin_list):
    try:
        data = get_market_data_by_list(coin_list)
        data.drop('time', axis=1, inplace=True)
        check_correlation(data)

    except Exception as e:
        print('Error {} occurred during data retrieve/analysis'.format(e))


def analyse_correlation_with_base(base):
    try:
        data = get_market_data_containing(base)
        data.drop('time', axis=1, inplace=True)
        check_correlation(data)

    except Exception as e:
        print('Error {} occured during data retrieve/analysis'.format(e))


def plot_dependables(data, columns=[]):
    """
    Plots scatter figure to gain insight into columns relationship
    Attributes:
        data -containing columns to compare
        columns[0] - independent
        columns[1] - dependent variable
    """
    if len(data.columns.values) < 2:
        raise ValueError('Incorrect data!')

    if not columns:
        columns = data.columns.values

    data.plot.scatter(x=columns[0], y=columns[1])
    plt.show()


def simple_regression(x, y, order=1):
    """
    runs regression with sklearn, with possibility to transform features
    into polynomial relationship

    """

    model = Pipeline([('poly', PolynomialFeatures(degree=order)),
                      ('linear', LinearRegression(normalize=True))])

    print('Training sklearn linear regressor...')
    model.fit(x, y)
    print('Model performance')
    print(model.score(x, y))
    print(model.named_steps['linear'].coef_)


def plot_regression_line(x, y, order=1):
    """
    plots regression line, with given polynomial order
    """
    plt.figure(figsize=(10, 10))
    sns.set_context(rc={"lines.linewidth": 3})
    sns.regplot(x=x, y=y, order=order)
    plt.title('Regression: poly = {}'.format(order))

    plt.show()


def regression_summary(x, y, order=1):
    """
    runs statistical regression summary
    """
    x = sm.add_constant(x)
    regr = sm.OLS(y, x).fit()
    print(regr.summary())


def run_regression_analysis(coins, order=1):
    """
    runs different regression analysis:
    - offers OLS regression summary for further analysis
    - plots regression line/curve
    - demonstrates accuracy of linear regression model
      on observed data

    when giving coins, first one is independable,
                       second one dependable variable.
    """
    if len(coins) != 2:
        raise ValueError('Exactly two coins must be provided!')

    try:
        data = get_market_data_by_list(coins)
        data.drop('time', axis=1, inplace=True)
        x = data[data.columns[0]]
        y = data[data.columns[1]]

        regression_summary(x, y, order=order)
        plot_regression_line(x, y, order=order)

        simple_regression(data[[data.columns[0]]], data[[data.columns[1]]], order=order)

    except Exception as e:
        print('Error {} occurred during data retrieve/analysis'.format(e))


