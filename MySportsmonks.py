import http.client
import json
import pandas as pd
from pandas.io.json import json_normalize
import numpy as np
import csv
import os
import requests
import config





def extract(year: int, month: int, day: int) -> pd.DataFrame:

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

              return pd.DataFrame()