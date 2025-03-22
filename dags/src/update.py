# -*- coding: utf-8 -*-

import json
from src.utils.sql import SQLEngine
import os
import pandas as pd

def store_last_day_channel_info(**kwargs):
  # Get channel information from the JSON file
  with open(kwargs['templates_dict']['data_path'], 'r', encoding='utf-8') as file:
    channel_info = json.load(file)

  # Build the base dictionary from channel_info
  structured_data = {
    'channelId': channel_info['channelId'],
    'title': channel_info['title'],
    'description': channel_info['description'],
    'customUrl': channel_info['customUrl'],
    'publishedAt': channel_info['publishedAt'],
    'highThumbnail': channel_info['thumbnails']['high']['url'],
    'defaultLanguage': channel_info['defaultLanguage'],
    'uploads_id': channel_info['uploads_id'],
    'viewCount': channel_info['viewCount'],
    'subscriberCount': channel_info['subscriberCount'],
    'videoCount': channel_info['videoCount'],
    'topicCategories': channel_info['topicCategories'][0],
    'privacyStatus': channel_info['privacyStatus'],
  }
  
  # Dynamically add localization keys
  for loc_key, loc_data in channel_info.get('localizations', {}).items():
    language_code = loc_key.split('_')[0].lower()
    if 'title' in loc_data:
      structured_data[f'localized_title_{language_code}'] = loc_data['title']
    if 'description' in loc_data:
      structured_data[f'localized_description_{language_code}'] = loc_data['description']

  # Establish connection to SQL DDBB
  sqlengine = SQLEngine()
  sqlengine.get_connection(database="KantianProjectDDBB")

  # IMPORTANT: Replace the following SQL query with one that explicitly lists the columns you need.
  # For demonstration, I'm listing columns explicitly.
  query = f"""
  SELECT *
  FROM KantianProjectDDBB.channel_information
  WHERE endDate IS NULL;
  """
  previous_channel_info_df = sqlengine.get_data(sql_query=query).drop(['startDate','endDate'], axis=1)

  # Compare the previous channel info with the new structured data.
  # (Make sure the data types and keys match as expected.)
  print("prevDF:", previous_channel_info_df.sort_index(axis=1))
  print("newDF:", pd.DataFrame([structured_data]).sort_index(axis=1))
  # Sort columns with `.sort_index(axis=1)`
  try:
      pd.testing.assert_frame_equal(previous_channel_info_df, pd.DataFrame([structured_data]), check_dtype=False)  # check_dtype=False ignores type differences
      print("DataFrames are equal.")
  except AssertionError as e:
      print("DataFrames differ:")
      print(e)
  if not previous_channel_info_df.reset_index(drop=True).sort_index(axis=1).astype(str).equals(pd.DataFrame([structured_data]).reset_index(drop=True).sort_index(axis=1).astype(str)):
    print("Channel attributes changed")
    # set endDate for current attributes
    sqlengine.update_cell_value('channel_information', 'endDate',
                        new_value=kwargs['ds'], where_clause='endDate IS NULL', where_params={})
    
    # set startDate for new attributes and store new attributes in a new row in the table
    structured_data['startDate'] = pd.to_datetime(kwargs['ds']).replace(hour=0, minute=0, second=0)
    structured_data['endDate'] = pd.NaT
    structured_df = pd.DataFrame([structured_data])
    sqlengine.insert_data(dataframe=structured_df, table_name='channel_information')
    
  else:
    print("No channel attributes changed.")

def youtube_channel_info_compare_with_chatGPT(**kwargs):
  # Get previous channel information
  with open(kwargs['templates_dict']['data_path_yesterday'], 'r', encoding='utf-8') as file:
    channel_info_yesterday = json.load(file)
  # Get new channel information
  with open(kwargs['templates_dict']['data_path_today'], 'r', encoding='utf-8') as file:
    channel_info_today = json.load(file)
        