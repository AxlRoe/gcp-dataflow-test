import pandas as pd
from matplotlib import pyplot
from matplotlib.dates import DateFormatter

date_form = DateFormatter("%H:%M")
mydateparser = lambda x: pd.datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f")
df = pd.read_csv('bet_old.csv', sep=";",
                 parse_dates=['ts'], date_parser=mydateparser,
                 usecols=['id', 'ts', 'available', 'away', 'event_id', 'home', 'market_name', 'matched', 'quote_diff',
                          'runner_name', 'total_available', 'total_matched'])

df_match = pd.read_csv('match_stats.csv', sep=";",
                       parse_dates=['ts'], date_parser=mydateparser,
                       usecols=['ts', 'event_id', 'agoal', 'hgoal'])

df['ts'] = df['ts'].apply(lambda x: x.replace(second=0))
#df['quote_diff'] = df['quote_diff'].apply(lambda x: np.log10(x) if x > 0 else x)
df_match['ts'] = df_match['ts'].apply(lambda x: x.replace(second=0))

df = df[(df['event_id'] == 30366510)]
df = df[(df['matched'] > 0)]
markets = df.market_name.unique()
nrows=2
ncols=3
fig, axes = pyplot.subplots(nrows=nrows, ncols=ncols, sharex=True, figsize=(15, 5))


def fill_axes(df, axes_num, axes, df_match, event_id):

    df_bymarket = df[(df['market_name'] == market)]
    for runner in df_bymarket.runner_name.unique():
        df_byrunner = df_bymarket[(df_bymarket['runner_name'] == runner)]
        ts_axes = build_ts_axes(df_byrunner, df_match)
        df_byrunner = df_byrunner.sort_values(by=['ts'])
        quote_axes = df_byrunner[["quote_diff"]]
        axes[axes_num][k].plot(ts_axes, quote_axes, label=runner)
        #axes.plot(ts_axes, quote_axes, label=runner)

    goals_series = df_match[(df_match['event_id'] == event_id)].drop_duplicates()
    ts_axes = goals_series[["ts"]]
    ts_axes = ts_axes.sort_values(by=['ts'])

    ax_twin = axes[axes_num][k].twinx()
    #ax_twin = axes.twinx()
    ax_twin.set_ylabel('goal')
    ax_twin.plot(ts_axes, goals_series[['hgoal']], label='hgoal', color='red', linestyle='--', alpha=.8)
    ax_twin.plot(ts_axes, goals_series[['agoal']], label='agoal', color='green', linestyle='--', alpha=.8)
    ax_twin.legend(loc='upper left')

    # set title and y label
    axes[axes_num][k].set_title(market, fontsize=12)
    #axes.set_title(market, fontsize=12)
    axes[axes_num][k].set_ylabel("quote (log)")
    #axes.set_ylabel("quote (log)")
    axes[axes_num][k].legend(loc='upper center')
    #axes.legend()

    #axes.xaxis.grid(b=True, which='major', color='black', linestyle='--', alpha=1)
    axes[axes_num][k].xaxis.grid(b=True, which='major', color='black', linestyle='--', alpha=1)
    #axes.xaxis.set_major_formatter(date_form)
    axes[axes_num][k].xaxis.set_major_formatter(date_form)


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

k = 0
for market in ['MATCH_ODDS', 'OVER_UNDER_05', 'OVER_UNDER_15']:
    fill_axes(df, 0, axes, df_match, 30366510)
    k += 1

k = 0
for market in ['OVER_UNDER_25', 'OVER_UNDER_35', 'BOTH_TEAMS_TO_SCORE']:
    fill_axes(df, 1, axes, df_match, 30366510)
    k += 1

pyplot.tight_layout()
pyplot.show()

