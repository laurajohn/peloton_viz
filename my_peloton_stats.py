import requests, json
import pandas as pd
from pandas.io.json import json_normalize
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import datetime as dt
import numpy as np
import config


payload = {'username_or_email': config.username
           , 'password': config.password
           }

base_url = "https://api.pelotoncycle.com/api/"
s = requests.Session()


def authenticate():
    """
    Authenticate session.  Required to access workout data.  I'm not sure how long before timeout.
    :return: auth session + user_id (uuid, not the name you see in peloton)
    """
    auth_response = s.post('https://api.onepeloton.com/auth/login', json=payload)
    parsed_response = json.loads(auth_response.text)

    return {'auth_session': parsed_response['session_id'], 'my_user_id' : parsed_response['user_id']}


def call_peloton(my_user_id, auth_session, type, workout_id=None, passed_url=None, params={}):
    """
    Get call to peloton API.  Set up to call ride, but not using it right now.

    :param my_user_id: user uuid (not the one you see on your bike/app)
    :param auth_session: authenticated session ID
    :param type: I'm putting all the URL logic here.
    :param workout_id: peloton identifier for individual ride/class
    :return: json returned
    """

    # might need to paginate here when I have more workouts?
    # it changed, now it's only 100
    if type == 'all_workouts':
        url = base_url + 'user/' + my_user_id + '/workouts?limit=100'
    elif type == 'workout':
        url = base_url + 'workout/'+workout_id
    elif type == 'ride':
        url = base_url + passed_url

    my_workouts = s.get(url
                        , json=payload
                        , params=params
                        , cookies={'peloton_session_id': auth_session})

    my_workouts_json = json.loads(my_workouts.text)

    return my_workouts_json


def paginate_workouts(auth):
    """
    Call API & paginate

    """
    # has some ride details, but not all.  The rest require a call to the specific workout
    my_workouts = call_peloton(my_user_id=auth['my_user_id']
                               , auth_session=auth['auth_session']
                               , type='all_workouts')

    yield my_workouts['data']

    page_count = my_workouts['page_count']

    for page in range(1, page_count):
        next_page = call_peloton(my_user_id=auth['my_user_id']
                                 , auth_session=auth['auth_session']
                                 , type='all_workouts'
                                 , params={'page':page})
        yield next_page['data']


def get_all_workouts(auth):
    """
    Call to get all my workouts

    :return: list of json, one per workout
    """
    all_workouts = []

    for page in paginate_workouts(auth):
        for workout in page:
            all_workouts.append(workout)

    return all_workouts


def get_ride_details(auth, all_workouts):
    """
    This calls to get individual ride details.
    Note: Not using this right now.  Moved to calc'ing duration from workout to avoid this heavy lift
    leaving code in case I decide to cut by something that is only available in the ride data

    :return: list of json, one per workout
    """
    all_ride_details = []

    for workout in all_workouts:

        my_ride_details = call_peloton(my_user_id=auth['my_user_id']
                                       , auth_session=auth['auth_session']
                                       , type='workout'
                                       , workout_id=workout['id'])

        all_ride_details.append(my_ride_details)

    return all_ride_details


def clean_dataframe(df):
    """
    add some things to the dataframe
    :param df: df
    :return: df
    """

    # calc starttime
    df['start_date'] = pd.to_datetime(df['start_time'], unit='s')

    # set the date as index
    df.index = pd.to_datetime(df.start_date)

    df['month_year'] = pd.to_datetime(df['start_date']).dt.strftime('%Y-%m')
    df['count'] = 1

    df['duration'] = df['end_time'] - df['start_time']

    # I could use ride.duration, but that requires a call to each individual ride, this is faster
    df['duration'] = np.where(
        df['duration'].between(1780, 1820, inclusive=True), 1800,
        np.where(
            df['duration'].between(1180, 1220, inclusive=True), 1200, df['duration']
        )
    )

    df['output_per_minute'] = df['total_work']/1000/(df['duration']/60)

    df['week'] = df['start_date'] - pd.to_timedelta(df['start_date'].dt.dayofweek, unit='d')
    df['week'] = pd.to_datetime(df['week']).dt.strftime('%Y-%m-%d')

    df['recent_flag'] = df['start_date'] > dt.datetime(2021, 12, 21, 0).strftime('%Y-%m-%d')

    return df


def make_dash(df):
    """
    Plotly subplots, returns figure

    :param df: pass a dataframe
    :return: return fig
    """

    cycling_df = df[(df['fitness_discipline'] == 'cycling') & (df['has_leaderboard_metrics'])]
    spintray_df = df[(df['fitness_discipline'] == 'cycling') & (~df['has_leaderboard_metrics'])]
    strength_df = df[df['fitness_discipline'] == 'strength']

    fig = make_subplots(
        rows=5, cols=2,
        specs=[[{"colspan": 2}, None],
               [{"colspan": 2}, None],
               [{"colspan": 2}, None],
               [{}, {}],
               [{}, {}]
               ],
        subplot_titles=("Class count by discipline by month"
                        , "Cycling class count by duration"
                        , "Average Output per minute over time"
                        , "Avg Output per minute - 30 min class"
                        , "Avg Output per minute - 20 min class"
                        , "Recent: Avg Output per minute - 30 min class"
                        , "Recent: Avg Output per minute - 20 min class"
                        ))

    # classes over time by discipline
    TOD_cycling = cycling_df.groupby(['month_year']).sum()
    TOD_strength = strength_df.groupby(['month_year']).sum()
    TOD_spintray = spintray_df.groupby(['month_year']).sum()
    fig.add_trace(
        go.Bar(x=TOD_cycling.index
               , y=TOD_cycling["count"]
               , text=TOD_cycling["count"]
               , name="Cycling Class Count"
               , showlegend=True
               ),
        row=1, col=1
    )
    fig.add_trace(
        go.Bar(x=TOD_strength.index
               , y=TOD_strength["count"]
               , text=TOD_strength["count"]
               , name="Strength Class Count"
               , showlegend=True
               ),
        row=1, col=1
    )
    fig.add_trace(
        go.Bar(x=TOD_spintray.index
               , y=TOD_spintray["count"]
               , text=TOD_spintray["count"]
               , name="Spintray Class Count"
               , showlegend=True
               ),
        row=1, col=1
    )

    # 20 & 30 min class counts over time by discipline
    TOD_cyc_20 = cycling_df[(cycling_df['duration'] >= 1180) & (cycling_df['duration'] <= 1220)].groupby(['month_year']).sum()
    TOD_cyc_30 = cycling_df[(cycling_df['duration'] >= 1780) & (cycling_df['duration'] <= 1820)].groupby(['month_year']).sum()
    fig.add_trace(
        go.Bar(x=TOD_cyc_30.index
               , y=TOD_cyc_30["count"]
               , text=TOD_cyc_30["count"]
               , name="30 min Class Count"
               , showlegend=True
               ),
        row=2, col=1
    )
    fig.add_trace(
        go.Bar(x=TOD_cyc_20.index
               , y=TOD_cyc_20["count"]
               , text=TOD_cyc_20["count"]
               , name="20 min Class Count"
               , showlegend=True
               ),
        row=2, col=1
    )

    # has my avg output been increasing over time?  20 min+ or higher to exclude cool-down rides
    TOD = cycling_df[cycling_df['duration'] >= 1200].groupby(['month_year']).mean().round(2)
    fig.add_trace(
        go.Bar(x=TOD.index
               , y=TOD["output_per_minute"]
               , text=TOD["output_per_minute"]
               , name="avg output per minute"
               , showlegend=False
               ),
        row=3, col=1
    )
    fig.update_traces(textposition='inside')

    # has my avg output been increasing over time?
    TOD_30 = cycling_df[(cycling_df['duration'] == 1800)].groupby(['month_year']).mean().round(2)
    TOD_20 = cycling_df[(cycling_df['duration'] == 1200)].groupby(['month_year']).mean().round(2)
    fig.add_trace(
        go.Scatter(x=TOD_30.index
                   , y=TOD_30["output_per_minute"]
                   , text=TOD_30["output_per_minute"]
                   , name="30 minute classes"
                   , textposition='top center'
                   , showlegend=False
                   , mode="lines"
                   ),
        row=4, col=1
    )
    fig.add_trace(
        go.Scatter(x=TOD_20.index
                   , y=TOD_20["output_per_minute"]
                   , text=TOD_20["output_per_minute"]
                   , name="20 minute classes"
                   , textposition='top center'
                   , showlegend=False
                   , mode="lines"
                   ),
        row=4, col=2
    )

    # has my avg output been increasing over time?  Recently, by week
    TOD_30_wk = cycling_df[(cycling_df['recent_flag']) & (cycling_df['duration'] == 1800)].groupby(['week']).mean().round(2)
    TOD_20_wk = cycling_df[(cycling_df['recent_flag']) & (cycling_df['duration'] == 1200)].groupby(['week']).mean().round(2)
    fig.add_trace(
        go.Bar(x=TOD_30_wk.index
               , y=TOD_30_wk["output_per_minute"]
               , text=TOD_30_wk["output_per_minute"]
               , name="30 minute classes"
               , textposition='inside'
               , showlegend=False
               ),
        row=5, col=1
    )
    fig.add_trace(
        go.Bar(x=TOD_20_wk.index
               , y=TOD_20_wk["output_per_minute"]
               , text=TOD_20_wk["output_per_minute"]
               , name="20 minute classes"
               , textposition='inside'
               , showlegend=False
               ),
        row=5, col=2
    )

    fig.update_layout(title_text="Peloton Dash")
    return fig


if __name__ == "__main__" or "builtins":
    auth = authenticate()
    all_workouts = get_all_workouts(auth)
    df = clean_dataframe(pd.json_normalize(all_workouts))
    fig = make_dash(df)
    fig.show()
