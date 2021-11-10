import dateutil
import numpy as np
import pandas as pd
from matplotlib import pyplot
from matplotlib.dates import DateFormatter

def build_ts_axes(df_byrunner, df_match):
    join = df_match[['ts', 'event_id']].merge(df_byrunner, on=['ts', 'event_id']).drop_duplicates()
    goals_series = join[["ts"]].drop_duplicates()
    ts_axes = goals_series[["ts"]]
    ts_axes = ts_axes.sort_values(by=['ts'])
    return ts_axes


def set_goal_region(axes, axes_num, indices, color, join):
    if len(indices) > 0:
        start = join.index[indices[0]]
        end = join.index[indices[len(indices) - 1]]
        axes[axes_num][k].axvspan(start,
                                  end,
                                  facecolor=color,
                                  edgecolor='none',
                                  alpha=.2)


def fill_indices(goal_indices, index_values):
    indices = []
    if len(goal_indices) > 0:
        i = 0
        j = 0
        for idx in index_values:
            if goal_indices[i] == idx:
                indices.append(j)
                i += 1
            j += 1

    return indices

date_form = DateFormatter("%H:%M")
mydateparser = lambda x: dateutil.parser.isoparse(x)
df = pd.read_csv('bet.csv', sep=";",
                 parse_dates=['ts'], date_parser=mydateparser,
                 usecols=['id','ts','back','lay','start_back','start_lay','hgoal','agoal','runner_name','event_name','event_id','market_name'])

df['ts'] = df['ts'].apply(lambda x: (x.replace(second=0)))
df = df.sort_values(by=['ts'])
df_match = df[['ts', 'event_id', 'agoal', 'hgoal']]
df = df.drop('agoal', 1)
df = df.drop('hgoal', 1)

#df = df[(df['event_id'] == 31019505)]
event_df = df[['event_id','event_name']].drop_duplicates()

#market = 'BOTH_TEAMS_TO_SCORE'

for index, row in event_df.iterrows():

    event = row["event_id"]
    event_name = row["event_name"]

    df_by_event = df[(df['event_id'] == event)]
    markets = list(filter(lambda x: x != 'CORRECT_SCORE', df_by_event.market_name.unique()))

    for market in markets:
        df_bymarket = df_by_event[(df_by_event['market_name'] == market)]
        for runner in df_bymarket.runner_name.unique():
            fig, axes = pyplot.subplots(figsize=(15, 5))
            df_byrunner = df_bymarket[(df_bymarket['runner_name'] == runner)]
            ts_axes = build_ts_axes(df_byrunner, df_match)
            df_byrunner = df_byrunner.sort_values(by=['ts'])

            # df['quote_diff'] = df['quote_diff'].apply(lambda x: np.log10(x) if x > 0 else x)

            quote_axes = df_byrunner[["lay"]]#.apply(lambda x: np.log10(x))
            axes.plot(ts_axes, quote_axes, label=runner)
            pre_lay_quote_axes = df_byrunner[["start_lay"]]#.apply(lambda x: np.log10(x))
            axes.plot(ts_axes, pre_lay_quote_axes, label="pre " + runner, linestyle='dashdot')

            goals_series_df = df_match[(df_match['event_id'] == event)].drop_duplicates()
            runner_and_goal = pd.merge(goals_series_df, df_byrunner, on=["ts", "event_id"])

            ts_axes = runner_and_goal[["ts"]]
            ts_axes = ts_axes.sort_values(by=['ts'])

            ax_twin = axes.twinx()
            ax_twin.set_ylabel('goal')
            ax_twin.plot(ts_axes, runner_and_goal[['hgoal']], label='hgoal', color='red', linestyle='--', alpha=.8)
            ax_twin.plot(ts_axes, runner_and_goal[['agoal']], label='agoal', color='green', linestyle='--', alpha=.8)
            ax_twin.legend(loc='upper left')

            # set title and y label
            axes.set_title(event_name + " - " + market + "_" + runner, fontsize=12)
            axes.set_ylabel("lay ")
            axes.legend(loc='upper center')

            axes.xaxis.grid(b=True, which='major', color='black', linestyle='--', alpha=1)
            axes.xaxis.set_major_formatter(date_form)

            #pyplot.tight_layout()
            #pyplot.show()
            pyplot.savefig("images/" + str(event) + "_" + runner.replace(" ","_").replace(".","").replace(",","") + ".png")
            print("charts for event " + str(event) + "_" + runner.replace(" ","_").replace(".","").replace(",","") + ".png")

