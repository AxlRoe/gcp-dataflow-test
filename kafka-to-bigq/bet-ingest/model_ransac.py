import os
import re
from decimal import Decimal, ROUND_HALF_UP

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from scipy.spatial.distance import cdist

from sklearn.linear_model import LinearRegression, RANSACRegressor
from sklearn.metrics import r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import PolynomialFeatures, StandardScaler
from tqdm import tqdm

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
    distances = []
    for i in range(1, len(values)):
        distances.append(cdist([values[i]], [values[i-1]], 'euclidean'))

    dist_p = np.nanpercentile(values, 95)
    p_h = np.nanpercentile(values, 99)
    p_l = np.nanpercentile(values, 1)
    highest_outliers = np.where(np_sorted_values >= p_h)
    lowest_outliers = np.where(np_sorted_values  <= p_l)

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

def round_to_quarter(d):
    return np.float64((Decimal(d)*4).quantize(Decimal('1'), rounding=ROUND_HALF_UP)/4)

def filter_csv(file):
    return True if re.search("\\.csv", file) != None else False

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
final_df = df[(df['goal_diff_by_prediction'] == 1) & (df['minute'] <= 120) & (~df['start_back'].isna())]
final_df['lay'] = final_df.apply(lambda row: round(np.log10(row.lay) * 120) / 100, axis=1)
final_df = final_df.drop(['event_id'], axis=1)

h_outliers, l_outliers = find_outlier_by_perc(final_df, 'start_back')

for outlier in h_outliers:
    final_df = final_df[(final_df['start_back'] != outlier)]

for outlier in l_outliers:
    final_df = final_df[(final_df['start_back'] != outlier)]

#correlation_map(final_df, cols)
#sns.pairplot(final_df[cols], diag_kind='kde')
#plt.show()

for i in np.arange(3, 5, 0.50):

    tmp_df = final_df[(final_df['start_back'] >= i) & (final_df['start_back'] < i+0.5)]
    if tmp_df.empty:
        continue

    X_fit = np.arange(0, 120, 2)[:, np.newaxis]
    X = tmp_df[feature_cols].values
    y = tmp_df[label_col].values

    ransac = RANSACRegressor(LinearRegression(),
                             max_trials=100,
                             min_samples=50,
                             loss='absolute_error',
                             random_state=0)

    quadratic = PolynomialFeatures(degree=3)
    X_quad = quadratic.fit_transform(X)
    ransac.fit(X_quad, y)
    inlier_mask = ransac.inlier_mask_
    outlier_mask = np.logical_not(inlier_mask)

    y_quad_fit = ransac.predict(quadratic.fit_transform(X_fit))
    y_quad_pred = ransac.predict(X_quad)
    print("Quote " + str(i))
    print("[POLY] Training R^2 cubic: %.3f" % (r2_score(y, y_quad_pred)))

    # plot results
    #plt.scatter(X, y, label='Training points-' + str(i))
    plt.scatter(X[inlier_mask], y[inlier_mask],
                c='steelblue', edgecolor='white',
                marker='o', label='Inliers')
    plt.scatter(X[outlier_mask], y[outlier_mask],
                c='limegreen', edgecolor='white',
                marker='s', label='Outliers')


    plt.plot(X_fit, y_quad_fit, label=str(i), color='black')
    #plt.title("Quote, min: " + str(bin_dict[bin]['min']) + " max: " + str(bin_dict[bin]['max']))
    plt.title("Quote " + str(i))
    plt.xlabel('Explanatory variable')
    plt.ylabel('Predicted or known target values')
    plt.legend(loc='upper left')

    plt.tight_layout()
    # plt.savefig('images/10_11.png', dpi=300)
    #plt.show()

