"""A word-counting workflow."""

from __future__ import absolute_import

import argparse
import json
import logging
from decimal import Decimal, ROUND_HALF_UP

import math
import apache_beam as beam
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from apache_beam import DoFn, WithKeys, GroupByKey
from apache_beam.options.pipeline_options import PipelineOptions
from scipy.spatial.distance import cdist
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from sklearn.preprocessing import PolynomialFeatures

SCHEMA = ",".join(
    [
        "id:STRING",
        "market_name:STRING",
        "runner_name:STRING",
        "lay:FLOAT64",
        "back:FLOAT64",
        "ts: TIMESTAMP"
    ]
)

def filter_matches(row):
    if math.isnan(row['goal_diff_by_prediction']) or math.isnan(row['start_back']):
        logging.info("[" + row['event_id'] + "] Skipping row because of empty goals or start quote")
        return False

    return int(row['goal_diff_by_prediction']) == 1

def remove_outliers(df):
    h_outliers, l_outliers = find_outlier_by_perc(df, 'start_back')

    for outlier in h_outliers:
        df = df[(df['start_back'] != outlier)]

    for outlier in l_outliers:
        df = df[(df['start_back'] != outlier)]

    return df


def find_outlier_by_perc (df, col):

    sorted_values = sorted(df[col].unique())
    if len(sorted_values) == 1:
        return [], []

    values = np.array(sorted_values).reshape(1,-1)
    np_sorted_values = np.array(sorted_values)

    dist_p = np.nanpercentile(values, 95)
    p_h = np.nanpercentile(values, 99)
    p_l = np.nanpercentile(values, 1)
    highest_outliers = np.where(np_sorted_values >= p_h)
    lowest_outliers = np.where(np_sorted_values <= p_l)

    lo = lowest_outliers[-1][0]
    ho = highest_outliers[0][0]

    ho_d = cdist(np_sorted_values[ho].reshape(1,-1), np_sorted_values[ho - 1].reshape(1,-1), 'euclidean')
    lo_d = cdist(np_sorted_values[lo].reshape(1,-1), np_sorted_values[lo + 1].reshape(1,-1), 'euclidean')

    h_outliers = []
    l_outliers = []
    if ho_d > dist_p and len(values) - ho < 5: #minum cluster size to not be classified as outlier
        h_outliers.append(np_sorted_values[ho:])

    if lo_d > dist_p and lo < 5:  # minum cluster size to not be classified as outlier
        l_outliers.append(np_sorted_values[0:lo])

    return [item for sublist in h_outliers for item in sublist], [item for sublist in l_outliers for item in sublist]

class Record(DoFn):
    def process(self, element):
        back, lay, start_lay, start_back, hgoal, agoal, available, matched, total_available, total_matched, prediction, event_id, runner_name, ts, minute, sum_goals, current_result, goal_diff_by_prediction = element.split(";")

        return [{
            'event_id' : event_id,
            'lay': float(lay),
            'start_back': float(start_back) if start_back != '' else float('NaN'),
            'minute': minute,
            'goal_diff_by_prediction': float(goal_diff_by_prediction) if goal_diff_by_prediction != '' else float('NaN')
        }]

def log_scale_quote(row):
    row['lay'] = round(np.log10(row['lay']) * 100) / 100
    return row

def round_to_quarter(d):
    return str(np.float64((Decimal(d)*2).quantize(Decimal('1'), rounding=ROUND_HALF_UP)/2))

def create_df_by_event(rows):
    rows.insert(0, ['event_id', 'lay', 'start_back', 'goal_diff_by_prediction', 'minute'])
    df = pd.DataFrame(rows[1:], columns=rows[0])
    return df[['lay', 'start_back', 'goal_diff_by_prediction', 'minute']]

def compute_and_store_model(df):

    q_M = round_to_quarter(df['start_back'].max())
    q_m = round_to_quarter(df['start_back'].min())

    X_fit = np.arange(0, 120, 2)[:, np.newaxis]
    X = df[['minute']].values
    y = df[['lay']].values

    pr = LinearRegression()
    quadratic = PolynomialFeatures(degree=5)
    X_quad = quadratic.fit_transform(X)
    pr.fit(X_quad, y)
    y_quad_fit = pr.predict(quadratic.fit_transform(X_fit))
    y_quad_pred = pr.predict(X_quad)

    # plot results
    # https://stackoverflow.com/questions/55682156/iteratively-fitting-polynomial-curve
    delta = y - y_quad_pred
    sd_p = np.std(delta)
    ok = abs(delta) < sd_p

    upper = (y_quad_fit + sd_p).reshape(1, -1).flatten()
    lower = (y_quad_fit - sd_p).reshape(1, -1).flatten()
    X = X.reshape(1,-1).flatten()
    y = y.reshape(1,-1).flatten()
    y_pred = y_quad_fit.reshape(1,-1).flatten()
    outlier_selector = np.where(ok, 'b', 'r').reshape(1,-1).flatten()

    json_name = str(q_m) + '-' + str(q_M)
    summary = {'interval': json_name,
               'r2': str(round(r2_score(y, y_quad_pred)*100) / 100),
               'X': list(X),
               'y': list(y),
               'pred': list(y_pred),
               'upper': list(upper),
               'lower': list(lower),
               'outliner_selector': list(outlier_selector)
              }

    with open('result\\' + json_name + '.json', 'w') as fp:
        json.dump(summary, fp)



def merge_df(dfs):
    if not dfs:
        logging.info("No dataframe to concat ")
        return pd.DataFrame([])

    return pd.concat(dfs).reset_index(drop=True)


def select_start_back_interval(row):
    last_thr = -1
    for thr in np.arange(3, 10, 0.50):
        if row['start_back'] >= thr and row['start_back'] < thr + 0.5:
            return str(thr) + '-' + str(thr+0.5)
        last_thr = thr

    return str(last_thr)

def run(input_folder, out_csv, args=None):
    """Main entry point; defines and runs the wordcount pipeline."""
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(
        args, streaming=True, save_main_session=True
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        _ = (
                pipeline
                | "Read csvs " >> beam.io.ReadFromText(file_pattern=input_folder + '\*.csv', skip_header_lines=1)
                | "Parse record " >> beam.ParDo(Record())
                | "Filter matches with valid start back quote and "
                  "one goal diff for favourite player " >> beam.Filter(lambda row: filter_matches(row))
                | "Log scale lay quote " >> beam.Map(lambda row: log_scale_quote(row))
                | "Use start_back interval as key " >> WithKeys(lambda row: select_start_back_interval(row))
                | 'group rows by quote' >> GroupByKey()
                | "Get list of records by start_back " >> beam.Map(lambda tuple: tuple[1])
                | 'Create dataframe by quote ' >> beam.Map(lambda rows: create_df_by_event(rows))
                | 'Remove outliers ' >> beam.Map(lambda df: remove_outliers(df))
                | 'Calculate and store model ' >> beam.Map(lambda df: compute_and_store_model(df))
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_csv',
        dest='match_csv',
        required=True,
        help='csv with the match to consider'
    )
    parser.add_argument(
        '--out_csv',
        dest='out_csv',
        required=True,
        help='csv with the output of the pipeline'
    )

    known_args, pipeline_args = parser.parse_known_args()
    run(known_args.match_csv, known_args.out_csv, pipeline_args)
