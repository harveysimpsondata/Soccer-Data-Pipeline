import http.client
import json
import pandas as pd
from pandas.io.json import json_normalize
import numpy as np
import csv
import os
import requests
import config
from calendar import monthrange
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta




def extract(year: str, month: str, day: str) -> pd.DataFrame:

       url1 = f"https://api.sportmonks.com/v3/football/fixtures/date/{year}-{month:02}-{day:02}?api_token={config.api_key}&include=scores;participants"
       url = f"https://api.sportmonks.com/v3/football/fixtures/date/2022-09-03?api_token={config.api_key}&include=scores;participants"

       response = requests.get(url)
       data = response.json()

       if 'data' in data:

              df = pd.DataFrame(data['data'])\
                     .drop(columns=['id', 'sport_id', 'stage_id', 'league_id', 'season_id', 'group_id', 'aggregate_id', 'round_id', 'state_id', 'venue_id', 'leg','details','length','placeholder','has_odds','starting_at_timestamp'])

              df['Date'] = pd.to_datetime(df['starting_at']).dt.date
              df = df.rename(columns={'name': 'Game Match'})

              scores = pd.json_normalize(df.scores, sep='_').rename(columns={4: 'home_score', 5: 'away_score'})\
                         .drop(columns=[0, 1, 2, 3])

              home_scores = pd.json_normalize(scores.home_score, sep='_') \
                              .rename(columns={'score_goals': 'total_home_goals'}) \
                              .drop(columns=['score_participant', 'description', 'type_id', 'fixture_id', 'id', 'participant_id'])

              away_scores = pd.json_normalize(scores.away_score, sep='_') \
                              .rename(columns={'score_goals': 'total_away_goals'}) \
                              .drop(columns=['score_participant', 'description', 'type_id', 'fixture_id', 'id', 'participant_id'])

              participants = pd.json_normalize(df.participants, sep='_').rename(columns={0: 'home', 1: 'away'})

              participants_away = pd.json_normalize(participants.away, sep='_') \
                       .rename(columns={'id':'away_id', 'name':'away_name', 'image_path':'away_image_path', 'founded':'away_founded', 'short_code':'away_short_code', 'country_id':'away_country_id'}) \
                       .drop(columns=['sport_id', 'type', 'placeholder', 'meta_location', 'meta_winner', 'meta_position', 'gender', 'last_played_at', 'venue_id'])

              participants_home = pd.json_normalize(participants.home, sep='_') \
                       .rename(columns={'id':'home_id', 'name':'home_name', 'image_path':'home_image_path', 'founded':'home_founded', 'short_code':'home_short_code', 'country_id':'home_country_id'}) \
                       .drop(columns=['sport_id', 'type', 'placeholder', 'meta_location', 'meta_winner', 'meta_position', 'gender', 'last_played_at', 'venue_id'])

              df = pd.concat([df, participants_home, participants_away, home_scores, away_scores], axis=1)

              df = df.drop(columns=['scores', 'participants', 'starting_at'])
              df['home_win'] = np.where(df['total_home_goals'] > df['total_away_goals'], 1, 0)
              df['away_win'] = np.where(df['total_home_goals'] < df['total_away_goals'], 1, 0)

              df = df[['Date', 'Game Match', 'result_info', 'home_id', 'home_name', 'home_image_path', 'home_founded',
                      'home_short_code', 'home_country_id', 'total_home_goals', 'home_win', 'away_id', 'away_name',
                      'away_image_path', 'away_founded', 'away_short_code', 'away_country_id', 'total_away_goals', 'away_win']]

              return df

       else:
           print(f'No data found for {year}-{month:02}-{day:02}')
           return pd.DataFrame()

def extract_multiple_days(years: list[int] = [2022], months: list[int] = [9], days: list[int] = [6]) -> pd.DataFrame:
       df = pd.DataFrame()
       for year in years:
           for month in months:
               for day in days:

                   df = df.append(extract(str(year), f'{month:02}', f'{day:02}'))

       return df

def generate_date_dataframe(start_year, end_year, start_month, end_month, start_day=1, end_day=None):
    if end_day is None:
        _, end_day = monthrange(end_year, end_month)

    date_range = pd.date_range(
        start=pd.Timestamp(year=start_year, month=start_month, day=start_day),
        end=pd.Timestamp(year=end_year, month=end_month, day=end_day),
        freq='D'
    )

    df = pd.DataFrame(date_range, columns=['date'])
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day

    years = date_range.year.tolist()
    months = date_range.month.tolist()
    days = date_range.day.tolist()

    return years, months, days

# def date_range_string(df):
#     df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
#     start_date = df['date'].min()
#     end_date = df['date'].max()
#     date_range_str = f"{start_date.date()} to {end_date.date()}"
#     return date_range_str

def write_local(df: pd.DataFrame) -> Path:
    """Write DataFrame out locally as parquet file"""
    df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
    start_date = df['date'].min()
    end_date = df['date'].max()
    date_range_str = f"{start_date.date()} to {end_date.date()} Scottish Premiership"


    path = Path(f"data/{date_range_str}.parquet")
    df.to_parquet(path, compression="gzip")
    return path