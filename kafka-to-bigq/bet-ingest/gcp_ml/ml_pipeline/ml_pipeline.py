"""A word-counting workflow."""

from __future__ import absolute_import

import argparse
import logging
import math
from datetime import datetime, time
from decimal import Decimal, ROUND_HALF_UP

import apache_beam as beam
import jsonpickle
import numpy as np
import pandas as pd
import scipy.stats as st
from apache_beam import DoFn, WithKeys, GroupByKey
from apache_beam.io import fileio
from apache_beam.io.fileio import destination_prefix_naming
from apache_beam.options.pipeline_options import PipelineOptions
from scipy.spatial.distance import cdist
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures


class JsonSink(fileio.TextSink):
    def write(self, record):
        self._fh.write(jsonpickle.encode(record).encode('utf8'))
        self._fh.write('\n'.encode('utf8'))

def run(args=None):
    """Main entry point; defines and runs the wordcount pipeline."""
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(
        args, save_main_session=True
    )

    bucket = 'dump-bucket-4'
    start_of_day = datetime.combine(datetime.utcnow(), time.min)
    start_of_day = start_of_day.strftime("%Y-%m-%d")

    def matches_where_favourite_is_winning(df):
        return df[(df['goal_diff_by_prediction'] >= 1)]

    def calculate_sure_bet_perc(df):

        goal_diff_total_df = df[['minute', 'lay']].groupby('minute').count()
        goal_diff_total_df = goal_diff_total_df.rename(columns={'lay': 'total'})

        goal_diff_for_favourite_df = df[(df['goal_diff_by_prediction']) >= int(1)][['minute', 'lay']].groupby(
            'minute').count()
        goal_diff_for_favourite_df = goal_diff_for_favourite_df.rename(columns={'lay': 'count'})

        join_ok = goal_diff_total_df.join(goal_diff_for_favourite_df)
        join_ok['count'] = join_ok.apply(lambda row: 0 if math.isnan(row['count']) else row['count'], axis=1)
        join_ok['sure_bet_perc_by_min'] = join_ok.apply(
            lambda row: round((float(row['count']) / float(row['total'])) * 100) / 100, axis=1)
        join_ok = join_ok.reset_index()

        return pd.merge(df.reset_index(), join_ok[['minute', 'sure_bet_perc_by_min']], how='inner', on=['minute'])

    def remove_outliers(df):
        h_outliers, l_outliers = find_outlier_by_perc(df, 'start_back')

        for outlier in h_outliers:
            df = df[(df['start_back'] != outlier)]

        for outlier in l_outliers:
            df = df[(df['start_back'] != outlier)]

        return df

    def find_outlier_by_perc(df, col):

        sorted_values = sorted(df[col].unique())
        if len(sorted_values) == 1:
            return [], []

        values = np.array(sorted_values).reshape(1, -1)
        np_sorted_values = np.array(sorted_values)

        dist_p = np.nanpercentile(values, 95)
        p_h = np.nanpercentile(values, 99)
        p_l = np.nanpercentile(values, 1)
        highest_outliers = np.where(np_sorted_values >= p_h)
        lowest_outliers = np.where(np_sorted_values <= p_l)

        lo = lowest_outliers[-1][0]
        ho = highest_outliers[0][0]

        ho_d = cdist(np_sorted_values[ho].reshape(1, -1), np_sorted_values[ho - 1].reshape(1, -1), 'euclidean')
        lo_d = cdist(np_sorted_values[lo].reshape(1, -1), np_sorted_values[lo + 1].reshape(1, -1), 'euclidean')

        h_outliers = []
        l_outliers = []
        if ho_d > dist_p and len(values) - ho < 5:  # minimum cluster size to not be classified as outlier
            h_outliers.append(np_sorted_values[ho:])

        if lo_d > dist_p and lo < 5:  # minum cluster size to not be classified as outlier
            l_outliers.append(np_sorted_values[0:lo])

        return [item for sublist in h_outliers for item in sublist], [item for sublist in l_outliers for item in sublist]

    class Record(DoFn):
        def process(self, element):
            minute, prediction, back, lay, start_lay, start_back, draw_perc, goal_diff_by_prediction = element.split(";")

            return [{
                'lay': float(lay),
                'start_back': float(start_back) if start_back != '' else float('NaN'),
                'minute': minute,
                'goal_diff_by_prediction': float(goal_diff_by_prediction) if goal_diff_by_prediction != '' else float('NaN'),
                'draw_perc': draw_perc
            }]

    def log_scale_quote(row):
        row['lay'] = round(np.log10(row['lay']) * 100) / 100
        return row

    def round_to_quarter(d):
        return str(np.float64((Decimal(d) * 2).quantize(Decimal('1'), rounding=ROUND_HALF_UP) / 2))

    def create_df_by_event(rows):
        rows.insert(0, ['lay', 'start_back', 'goal_diff_by_prediction', 'minute', 'draw_perc'])
        return pd.DataFrame(rows[1:], columns=rows[0])

    def compute_model(df):
        tmp_df = df.copy()
        q_M = round_to_quarter(tmp_df['start_back'].max())
        q_m = round_to_quarter(tmp_df['start_back'].min())
        draw_perc = tmp_df['draw_perc'].max()

        # df.to_csv('tmp_df_pipe.csv', sep=';', index=False, encoding="utf-8", line_terminator='\n')

        json_name = str(q_m) + '-' + str(q_M)
        if q_m == q_M:
            json_name = str(q_m)

        tmp_df['minute'] = pd.to_numeric(tmp_df['minute'])
        X_fit = np.arange(0, 120, 2)[:, np.newaxis]
        X = tmp_df[['minute']].values
        y = tmp_df[['lay']].values

        m_x = X.flatten().min()
        M_x = 100

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
        X = X.reshape(1, -1).flatten()
        y = y.reshape(1, -1).flatten()
        y_pred = y_quad_fit.reshape(1, -1).flatten()
        X_fit = X_fit.reshape(1, -1).flatten()
        outlier_selector = np.where(ok, 'b', 'r').reshape(1, -1).flatten()

        sure_bet_perc_df = tmp_df[['minute', 'sure_bet_perc_by_min']].groupby('minute').min()
        sure_bet_perc_dict = sure_bet_perc_df.to_dict()['sure_bet_perc_by_min']

        # create 95% confidence interval for population mean weight
        interval = st.t.interval(alpha=0.95, df=len(tmp_df['start_back']) - 1, loc=np.mean(tmp_df['start_back']), scale=st.sem(tmp_df['start_back']))
        mean_responsibility = 3 * 0.95 * ((interval[1] + interval[0]) / 2 - 1)
        revenue = 2 * (10 ** y_quad_fit - 1)

        incomes = []
        for i in range(0, m_x, 2):
            incomes.append(0)

        for i in np.arange(0, M_x - m_x + 2, 2):
            idx = int(i / 2)
            incomes.append(revenue[idx][0] - mean_responsibility)

        # plt.plot(np.arange(0,M_x+2,2), incomes)
        # plt.axhline(y=1, color='g', linestyle='-')
        # plt.title("Quote " + json_name)
        # plt.show()

        minute_axis = list(range(0, 120, 2))
        incomes = np.array(incomes)
        tmp_incomes = np.flip(incomes, 0)
        break_even_index = -1
        for idx, elem in np.ndenumerate(tmp_incomes):
            if elem <= 1:
                break_even_index = incomes.size - idx[0]
                break

        break_even_minute = minute_axis[break_even_index] if break_even_index >= 0 else -1
        break_even_income = incomes[break_even_index] if break_even_index >= 0 else -1
        sure_bet_perc = []
        for minute in range(0, 80, 10):
            if minute in sure_bet_perc_dict.keys():
                sure_bet_perc.append(sure_bet_perc_dict[minute])
            else:
                sure_bet_perc.append(0)

        summary = {
            'key': json_name,
            'q_min': q_m,
            'q_max': q_M,
            'break_even_minute': break_even_minute,
            'break_even_income': round(float(break_even_income) * 100) / 100,
            'draw_perc': draw_perc,
            'sure_bet_perc_by_10min': sure_bet_perc
        }

        # model = {
        #      'interval': json_name,
        #      'r2': str(round(r2_score(y, y_quad_pred) * 100) / 100),
        #      'X': list(X),
        #      'y': list(y),
        #      'outlier_selector': list(outlier_selector),
        #      'pred': list(y_pred),
        #      'upper': list(upper),
        #      'lower': list(lower)
        # }

        # summary = {
        #     'name': json_name,
        #     'p_axis': [float(perc) for perc in perc_axis],
        #     'm_axis': [int(minute) for minute in minute_axis],
        #     'draw_perc': draw_perc,
        #     'ok_goal_diff_by_minute': ok_goal_diff_perc_dict,
        #     'loss': mean_responsibility_quote,
        #     'revenue': break_even_lay
        # }

        return summary

    def select_start_back_interval(row):
        last_thr = -1
        for thr in np.arange(3, 6, 0.50):
            if row['start_back'] >= thr and row['start_back'] < thr + 0.5:
                return str(thr) + '-' + str(thr + 0.5)
            last_thr = thr

        return str(last_thr)

    with beam.Pipeline(options=pipeline_options) as pipeline:

        _ = (
                pipeline
                | "Read csvs " >> beam.io.ReadFromText(file_pattern='gs://' + bucket + '/stage/*.csv', skip_header_lines=1)
                #| "Read csvs " >> ReadFromText(file_pattern='C:\\Users\\mmarini\\MyGit\\gcp-dataflow-test\\kafka-to-bigq\\bet-ingest\\gcp_ml\\*.csv', skip_header_lines=1)
                | "Parse record " >> beam.ParDo(Record())
                | "drop rows with nan goal diff " >> beam.Filter(lambda row: not math.isnan(row['start_back']) and not math.isnan(row['goal_diff_by_prediction']))
                | "Log scale lay quote " >> beam.Map(lambda row: log_scale_quote(row))
                | "Use start_back interval as key " >> WithKeys(lambda row: select_start_back_interval(row))
                | 'group rows by quote' >> GroupByKey()
                | "Get list of records by start_back " >> beam.Map(lambda tuple: tuple[1])
                | 'Create dataframe by quote ' >> beam.Map(lambda rows: create_df_by_event(rows))
                | 'Calculate goal diff for favourite by minute ' >> beam.Map(lambda df: calculate_sure_bet_perc(df))
                | "Filter matches with valid start back quote and "
                  "one goal diff for favourite player " >> beam.Map(lambda df: matches_where_favourite_is_winning(df))
                | "discard empty dataframe " >> beam.Filter(lambda df: not df.empty)
                | 'Remove outliers ' >> beam.Map(lambda df: remove_outliers(df))
                | 'Calculate risk ' >> beam.Map(lambda df: compute_model(df))
                | 'write to file ' >> fileio.WriteToFiles(
                                                path='gs://' + bucket + '/model/',
                                                destination=lambda model: model['key'],
                                                sink=lambda dest: JsonSink(),
                                                max_writers_per_bundle=1,
                                                shards=1,
                                                file_naming=destination_prefix_naming(suffix='.json'))
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args()
    run(pipeline_args)
