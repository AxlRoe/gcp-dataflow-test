import os
import re
from decimal import Decimal, ROUND_HALF_UP

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from scipy.spatial.distance import cdist

import tensorflow as tf
from sklearn.preprocessing import PolynomialFeatures, StandardScaler
from tensorflow.keras import layers
from tqdm import tqdm

def linear_regression(X, y):

    minute_model = tf.keras.Sequential([
        layers.Dense(units=1, input_shape=[4])
    ])

    minute_model.compile(
        optimizer=tf.optimizers.Adam(learning_rate=0.1),
        loss='mean_absolute_error')

    history = minute_model.fit(
        X,
        y,
        epochs=100,
        # Suppress logging.
        verbose=0,
        # Calculate validation results on 20% of the training data.
        validation_split=0.2)

    mse = history.history['loss'][-1]
    y_pred = minute_model.predict(X)

    plt.figure(figsize=(12, 7))
    plt.title('TensorFlow Model')
    plt.scatter(X[:, 1], y, label='Data $(X, y)$')
    plt.plot(X[:, 1], y_pred, color='red', label='Predicted Line $y = f(X)$', linewidth=4.0)
    plt.xlabel('$X$', fontsize=20)
    plt.ylabel('$y$', fontsize=20)
    plt.text(0, 0.70, 'MSE = {:.3f}'.format(mse), fontsize=20)
    plt.grid(True)
    plt.legend(fontsize=20)
    plt.show()

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
csv_cols = ['event_id', 'lay', 'start_back', 'goal_diff_by_prediction', 'minute']
csv_to_loads = list(filter(lambda file: filter_csv(file), os.listdir("data")))

dfs = []
for i in tqdm(range(len(csv_to_loads))):
    csv = csv_to_loads[i]
    tmp_df = pd.read_csv(os.path.join("data/", csv), sep=";", usecols=csv_cols)
    dfs.append(tmp_df)

df = pd.concat(dfs).reset_index(drop=True)
#final_df = df[(df['goal_diff_by_prediction'] == 1) & (df['minute'] <= 114) & (~df['start_back'].isna())]
final_df = df[(df['goal_diff_by_prediction'] == 1) & (df['minute'] <= 114) & (~df['start_back'].isna())]
final_df['lay'] = final_df.apply(lambda row: round(np.log10(row.lay) * 100) / 100, axis=1)
final_df = final_df.drop(['event_id'], axis=1)

h_outliers, l_outliers = find_outlier_by_perc(final_df, 'start_back')

for outlier in h_outliers:
    final_df = final_df[(final_df['start_back'] != outlier)]

for outlier in l_outliers:
    final_df = final_df[(final_df['start_back'] != outlier)]

#correlation_map(final_df, cols)
#sns.pairplot(final_df[cols], diag_kind='kde')
#plt.show()

# for i in np.arange(3, 10+0.25, 0.25):
#     tmp_df = final_df[(final_df['start_back'] == i)]
#     if tmp_df.empty:
#         continue

for i in np.arange(3, 6+0.50, 0.50):
    tmp_df = final_df[(final_df['start_back'] >= i) & (final_df['start_back'] < i+0.50)]
    if len(tmp_df) == 0:
        continue

    X = np.array(tmp_df['minute'])
    y = np.array(tmp_df['lay'])

    X = X/max(X)
    y = y/max(y)

    #https://shangeth.com/courses/deeplearning/1.2/
    poly = PolynomialFeatures(degree=3)
    X_3 = poly.fit_transform(X.reshape(-1,1))

    linear_regression(X_3, y)

