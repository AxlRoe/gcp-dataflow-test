import os
import re
from decimal import Decimal, ROUND_HALF_UP

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from scipy.spatial.distance import cdist

import tensorflow as tf
from tensorflow.keras import layers
from tqdm import tqdm

def plot_minute(x, y):
    plt.scatter(train_features['minute'], train_labels, label='Data')
    plt.plot(x, y, color='k', label='Predictions')
    plt.xlabel('minute')
    plt.ylabel('lay')
    plt.legend()
    plt.show()
    pass

def plot_loss(history):
    plt.plot(history.history['loss'], label='loss')
    plt.plot(history.history['val_loss'], label='val_loss')
    plt.ylim([0, 10])
    plt.xlabel('Epoch')
    plt.ylabel('Error [lay]')
    plt.legend()
    plt.grid(True)
    plt.show()
    pass

def linear_regression(train_labels, train_features, minute_normalizer):

    minute_model = tf.keras.Sequential([
        minute_normalizer,
        layers.Dense(units=1)
    ])

    minute_model.compile(
        optimizer=tf.optimizers.Adam(learning_rate=0.1),
        loss='mean_absolute_error')

    history = minute_model.fit(
        train_features['minute'],
        train_labels,
        epochs=100,
        # Suppress logging.
        verbose=0,
        # Calculate validation results on 20% of the training data.
        validation_split=0.2)

    hist = pd.DataFrame(history.history)
    hist['epoch'] = history.epoch
    print(hist.tail())

    plot_loss(history)

    x = tf.linspace(0, 90, 2)
    y = minute_model.predict(x)

    plot_minute(x, y)

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
feature_cols = ['start_back', 'goal_diff_by_prediction', 'minute']
label_col = ['lay']
csv_cols = ['event_id', 'lay', 'start_back', 'goal_diff_by_prediction', 'minute']
csv_to_loads = list(filter(lambda file: filter_csv(file), os.listdir("data")))

dfs = []
for i in tqdm(range(len(csv_to_loads))):
    csv = csv_to_loads[i]
    tmp_df = pd.read_csv(os.path.join("data/", csv), sep=";", usecols=csv_cols)
    dfs.append(tmp_df)

df = pd.concat(dfs).reset_index(drop=True)
#final_df = df[(df['goal_diff_by_prediction'] == 1) & (df['minute'] <= 114) & (~df['start_back'].isna())]
final_df = df[(df['goal_diff_by_prediction'] > 0) & (df['minute'] <= 90) & (~df['start_back'].isna())]
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

train_dataset = final_df
# print(train_dataset.describe().transpose()[['mean', 'std']])

train_labels = train_dataset[label_col]
train_features = train_dataset.drop(['lay'], axis=1)

normalizer = tf.keras.layers.Normalization(axis=-1)
normalizer.adapt(np.array(train_features))

minutes = np.array(train_features['minute'])
minute_normalizer = tf.keras.layers.Normalization(input_shape=[1, ], axis=None)
minute_normalizer.adapt(minutes)

#https://www.tensorflow.org/tutorials/keras/regression
linear_regression(train_labels, train_features, minute_normalizer)


