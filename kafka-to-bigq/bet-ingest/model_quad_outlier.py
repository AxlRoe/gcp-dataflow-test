import math
import os
import re
from decimal import Decimal, ROUND_HALF_UP

import matplotlib.pyplot as plt
import numpy as np
from numpy import diff
import pandas as pd
import seaborn as sns
from scipy.spatial.distance import cdist
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from sklearn.preprocessing import PolynomialFeatures
from statsmodels.graphics.gofplots import qqplot
from tqdm import tqdm
import scipy.stats as st
from numpy import diff


def round_to_quarter(d):
    return str(np.float64((Decimal(d)*2).quantize(Decimal('1'), rounding=ROUND_HALF_UP)/2))

def correlation_map(df, cols):
    fig, ax = plt.subplots(figsize=(11, 9))
    sns.set(font_scale=1.0)
    sns.heatmap(df[cols].corr(), annot=True, cmap='coolwarm', ax=ax)
    ax.set_title("Correlation Matrix of Variables", fontsize=18)
    ax.set_xticklabels(ax.get_xmajorticklabels(), fontsize=12)
    ax.set_yticklabels(ax.get_ymajorticklabels(), fontsize=12)
    plt.show()

def find_outlier_by_perc (df, col):

    sorted_values = sorted(df[col].unique())
    values = np.array(sorted_values).reshape(1,-1)
    np_sorted_values = np.array(sorted_values)

    dist_p = np.nanpercentile(values, 95)
    p_h = np.nanpercentile(values, 99)
    p_l = np.nanpercentile(values, 1)
    highest_outliers = np.where(np_sorted_values >= p_h)
    lowest_outliers = np.where(np_sorted_values <= p_l)

    lo = lowest_outliers[-1][0]
    ho = highest_outliers[0][0]

    ho_d = cdist(np.array([np_sorted_values[ho]]).reshape(1,-1), np.array([np_sorted_values[ho-1]]).reshape(1,-1), 'euclidean')
    lo_d = cdist(np.array([np_sorted_values[lo]]).reshape(1,-1), np.array([np_sorted_values[lo+1]]).reshape(1,-1), 'euclidean')

    h_outliers = []
    l_outliers = []
    if ho_d > dist_p and len(values) - ho < 5: #minum cluster size to not be classified as outlier
        h_outliers.append(np_sorted_values[ho:])

    if lo_d > dist_p and lo < 5:  # minum cluster size to not be classified as outlier
        l_outliers.append(np_sorted_values[0:lo])

    return [item for sublist in h_outliers for item in sublist], [item for sublist in l_outliers for item in sublist]

def filter_csv(file):
    return True if re.search("\\.csv", file) != None else False


def calculate_goal_diff_perc(df):

    goal_diff_total_df = df[['minute', 'lay']].groupby('minute').count()
    goal_diff_total_df = goal_diff_total_df.rename(columns={'lay': 'total'})

    goal_diff_for_favourite_df = df[(df['goal_diff_by_prediction']) >= int(1)][['minute', 'lay']].groupby('minute').count()
    goal_diff_for_favourite_df = goal_diff_for_favourite_df.rename(columns={'lay': 'count'})

    join_ok = goal_diff_total_df.join(goal_diff_for_favourite_df)
    join_ok['count'] = join_ok.apply(lambda row: 0 if math.isnan(row['count']) else row['count'], axis=1)
    join_ok['ok_goal_diff_perc_by_min'] = join_ok.apply(lambda row: round((float(row['count']) / float(row['total'])) * 100) / 100, axis=1)
    join_ok = join_ok.reset_index()

    return pd.merge(df.reset_index(), join_ok[['minute', 'ok_goal_diff_perc_by_min']], how='inner', on=['minute'])


cols = ['lay', 'start_back', 'minute', 'goal_diff_by_prediction']
feature_cols = ['minute']
label_col = ['lay']
csv_cols = ['event_id', 'lay', 'start_back', 'goal_diff_by_prediction', 'minute']
csv_to_loads = list(filter(lambda file: filter_csv(file), os.listdir("data")))

dfs = []
for i in tqdm(range(len(csv_to_loads))):
    csv = csv_to_loads[i]
    tmp_df = pd.read_csv(os.path.join("data/", csv), sep=";", usecols=csv_cols)
    dfs.append(tmp_df)

df = pd.concat(dfs).reset_index(drop=True)
final_df = df[(df['minute'] <= 120) & (~df['start_back'].isna()) & (~df['goal_diff_by_prediction'].isna())]
final_df['lay'] = final_df.apply(lambda row: round(np.log10(row.lay) * 100) / 100, axis=1)
final_df = final_df.drop(['event_id'], axis=1)

#correlation_map(final_df, cols)
#sns.pairplot(final_df[cols], diag_kind='kde')
#plt.show()

for i in np.arange(3, 6, 0.50):

    tmp_df = final_df[(final_df['start_back'] >= i) & (final_df['start_back'] < i+0.5)]
    if tmp_df.empty:
        print('Skip because not enough data')
        continue

    tmp_df = calculate_goal_diff_perc(tmp_df)
    tmp_df = tmp_df[(tmp_df['goal_diff_by_prediction'] >= 1)]
    if tmp_df.empty:
        print('Skip because favourite team is never in advantage')
        continue

    h_outliers, l_outliers = find_outlier_by_perc(tmp_df, 'start_back')

    for outlier in h_outliers:
        tmp_df = tmp_df[(tmp_df['start_back'] != outlier)]

    for outlier in l_outliers:
        tmp_df = tmp_df[(tmp_df['start_back'] != outlier)]

    #tmp_df.to_csv('tmp_df.csv', sep=';', index=False, encoding="utf-8", line_terminator='\n')

    X = tmp_df[feature_cols].values
    y = tmp_df[label_col].values

    m_x = X.flatten().min()
    M_x = 90
    X_fit = np.arange(m_x, M_x+2, 2)[:, np.newaxis]

    pr = LinearRegression()
    quadratic = PolynomialFeatures(degree=5)
    X_quad = quadratic.fit_transform(X)
    pr.fit(X_quad, y)
    y_quad_fit = pr.predict(quadratic.fit_transform(X_fit))
    y_quad_pred = pr.predict(X_quad)

    # plot results
    #https: // stackoverflow.com / questions / 55682156 / iteratively - fitting - polynomial - curve
    delta = y - y_quad_pred
    sd_p = np.std(delta)
    ok = abs(delta) < sd_p
    print("Quote " + str(i))
    print("[POLY] Training R^2 cubic: " + str(r2_score(y, y_quad_pred)) + " std " + str(sd_p))

    upper = (y_quad_fit + sd_p).reshape(1,-1).flatten()
    lower = (y_quad_fit - sd_p).reshape(1,-1).flatten()
    x_axis = X_fit.flatten()

    # plt.scatter(X, y, label='Training points-' + str(i), c=np.where(ok, 'b', 'r').reshape(1,-1).flatten())
    # plt.fill_between(x_axis, lower, upper, color='#00000020')
    # plt.plot(X_fit, y_quad_fit, label=str(i))
    # #plt.title("Quote, min: " + str(bin_dict[bin]['min']) + " max: " + str(bin_dict[bin]['max']))
    # plt.title("Quote " + str(i))
    # plt.xlabel('Explanatory variable')
    # plt.ylabel('Predicted or known target values')
    # plt.legend(loc='upper left')
    #
    # plt.tight_layout()
    # # plt.savefig('images/10_11.png', dpi=300)
    # plt.show()

    q_M = round_to_quarter(tmp_df['start_back'].max())
    q_m = round_to_quarter(tmp_df['start_back'].min())

    json_name = str(q_m) + '-' + str(q_M)
    if q_m == q_M:
        json_name = str(q_m)

    summary = {}
    perc_axis = []
    #ok_goal_diff_perc_df = tmp_df[['minute','sure_bet_perc_by_min']].groupby('minute').min()
    #ok_goal_diff_perc_dict = ok_goal_diff_perc_df.to_dict()['sure_bet_perc_by_min']

    # create 95% confidence interval for population mean weight
    interval = st.t.interval(alpha=0.95, df=len(tmp_df['start_back']) - 1, loc=np.mean(tmp_df['start_back']), scale=st.sem(tmp_df['start_back']))
    mean_responsibility = 3 * 0.95 * ((interval[1] + interval[0]) / 2 - 1)
    revenue = 2 * (10 ** y_quad_fit - 1)

    incomes = []
    for i in range(0, m_x, 2):
        incomes.append(0)

    for i in np.arange(0, M_x - m_x + 2, 2):
        idx = int(i/2)
        incomes.append(revenue[idx][0] - mean_responsibility)

    minute_axis = list(range(0,120,2))
    incomes = np.array(incomes)
    tmp_incomes = np.flip(incomes, 0)
    break_even_index = -1
    break_even_income = -1
    for idx, elem in np.ndenumerate(tmp_incomes):
        if elem <= 1:
            break_even_income = elem
            break_even_index = incomes.size - idx[0]
            break

    break_even_minute = minute_axis[break_even_index] if break_even_index >= 0 else -1
    break_even_lay = incomes[break_even_index] if break_even_index >= 0 else -1

    plt.plot(np.arange(0,M_x+2,2), incomes)
    plt.axhline(y=1, color='g', linestyle='-')
    plt.title("Quote " + json_name + 'break minute ' + str(break_even_minute) + ' break income ' + str(round(break_even_lay*100)/100))
    plt.show()

    summary = {
        'break_even_minute' : break_even_minute,
        'break_even_lay' : break_even_lay,
        #'draw_perc' : tmp_df['draw_perc'].max(),
        #'sure_bet_perc_by_min' : ok_goal_diff_perc_dict[break_even_minute]
    }


    #TODO dire al break even che probabilità c'è di sure bet e che probabilità ha il pareggio

    # lists = sorted(ok_goal_diff_perc_dict.items())  # sorted by key, return a list of tuples
    # x, y = zip(*lists)  # unpack a list of pairs into two tuples
    #
    # plt.title("Quote " + json_name)
    # plt.plot(x, y)
    # plt.show()


    # plt.plot(perc_axis, minute_axis, label=json_name)
    # # plt.title("Quote, min: " + str(bin_dict[bin]['min']) + " max: " + str(bin_dict[bin]['max']))
    # plt.title("Quote " + json_name)
    # plt.xlabel('percentage ')
    # plt.ylabel('minute')
    # plt.legend(loc='upper left')
    #
    # plt.tight_layout()
    # # plt.savefig('images/10_11.png', dpi=300)
    # plt.show()


