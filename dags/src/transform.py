# -*- coding: utf-8 -*-

import os
from src.utils.json_to_dataframes import json_to_dataframes
from src.utils.json_to_dataframes import read_json_file
import json
from src.utils.sql import SQLEngine
import pandas as pd

def youtube_channel_info_transform(**kwargs):
    from src.selections import selections_youtube_channel_info_transform
    
    # Load the JSON data
    data = read_json_file(kwargs['templates_dict']['input_path'])

    # Structure the unstructured data
    _, separate_dfs = json_to_dataframes(data, selections_youtube_channel_info_transform)

    # separate_dfs['items']
    # separate_dfs['items_localizations_en_US']
    # separate_dfs['items_localizations_es_ES']
    # separate_dfs['items_localizations_de_DE']
    # separate_dfs['items_topicDetails_topicCategories']

    # Slowly changing dimension
    for key, df in separate_dfs.items():
        df = df.drop('parent_id', axis=1)
        df['startDate'] = pd.to_datetime(kwargs['ds']).replace(hour=0, minute=0, second=0)
        df['endDate'] = pd.NaT
        # Update the dictionary with the modified DataFrame
        separate_dfs[key] = df

    # Stablish connection to SQL DDBB
    sqlengine = SQLEngine()
    sqlengine.get_connection(database="KantianProjectDDBB")
    sqlengine.insert_data(dataframe=separate_dfs['items'], table_name='channel_information')
    sqlengine.insert_data(dataframe=separate_dfs['items_localizations_en_US'], table_name='channel_US_localizations')
    sqlengine.insert_data(dataframe=separate_dfs['items_localizations_es_ES'], table_name='channel_ES_localizations')
    sqlengine.insert_data(dataframe=separate_dfs['items_localizations_de_DE'], table_name='channel_DE_localizations')
    sqlengine.insert_data(dataframe=separate_dfs['items_topicDetails_topicCategories'], table_name='channel_topicCategories')

    # # Assert that load directories exist
    # if not os.path.exists(os.path.dirname(kwargs['templates_dict']['output_path'])):
    #     os.makedirs(os.path.dirname(kwargs['templates_dict']['output_path']), exist_ok=True)

    # # Store as .parquet file
    # with open(kwargs['templates_dict']['output_path'], "w", encoding="utf-8") as json_file:
    #     json.dump(cleaned_data, json_file, ensure_ascii=False, indent=4)

def youtube_videos_transform(**kwargs):
    from src.selections import selections_youtube_videos_transform

    # Load the JSON data
    data = read_json_file(kwargs['templates_dict']['input_path'])

    # Structure the unstructured data
    main_df, separate_dfs = json_to_dataframes(data, selections_youtube_videos_transform)

    # Slowly changing dimension
    main_df['startDate'] = pd.to_datetime(kwargs['ds']).replace(hour=0, minute=0, second=0)
    main_df['endDate'] = pd.NaT

    separate_dfs['localized']['startDate'] = pd.to_datetime(kwargs['ds']).replace(hour=0, minute=0, second=0)
    separate_dfs['localized']['endDate'] = pd.NaT

    # Stablish connection to SQL DDBB
    sqlengine = SQLEngine()
    sqlengine.get_connection(database="KantianProjectDDBB")
    sqlengine.insert_data(dataframe=main_df, table_name='videos_information')
    sqlengine.insert_data(dataframe=separate_dfs['localized'], table_name='videos_localized_titles')

def youtube_videos_to_playlists(**kwargs):
    from src.selections import selections_youtube_add_videos_playlists

    # Load the JSON data
    data = read_json_file(kwargs['templates_dict']['input_path'])

    # Structure the unstructured data
    _, separate_dfs = json_to_dataframes(data, selections_youtube_add_videos_playlists)

    separate_dfs['videos'] = separate_dfs['videos'].drop('parent_id', axis=1)
    separate_dfs['videos'] = separate_dfs['videos'][separate_dfs['videos']['status_privacyStatus'] == 'public']

    # Slowly changing dimension
    separate_dfs['videos']['startDate'] = pd.to_datetime(kwargs['ds']).replace(hour=0, minute=0, second=0)
    separate_dfs['videos']['endDate'] = pd.NaT
    
    # Stablish connection to SQL DDBB
    sqlengine = SQLEngine()
    sqlengine.get_connection(database="KantianProjectDDBB")
    sqlengine.insert_data(dataframe=separate_dfs['videos'], table_name='playlists_videos')

def youtube_comments_transform(**kwargs):
    from src.selections import selections_youtube_comments_transform
    
    # Load the JSON data
    data = read_json_file(kwargs['templates_dict']['input_path'])

    # Structure the unstructured data
    _, separate_dfs = json_to_dataframes(data, selections_youtube_comments_transform)

    # Clowly Changing Dimension
    separate_dfs['comments'] = separate_dfs['comments'].drop('id', axis=1)
    separate_dfs['comments'] = separate_dfs['comments'].drop('parent_id', axis=1)
    separate_dfs['comments_replies'] = separate_dfs['comments_replies'].drop('parent_id', axis=1)

    separate_dfs['comments']['startDate'] = pd.to_datetime(kwargs['ds']).replace(hour=0, minute=0, second=0)
    separate_dfs['comments']['endDate'] = pd.NaT
    separate_dfs['comments_replies']['startDate'] = pd.to_datetime(kwargs['ds']).replace(hour=0, minute=0, second=0)
    separate_dfs['comments_replies']['endDate'] = pd.NaT

    # Stablish connection to SQL DDBB
    sqlengine = SQLEngine()
    sqlengine.get_connection(database="KantianProjectDDBB")
    sqlengine.insert_data(dataframe=separate_dfs['comments'], table_name='comments')
    sqlengine.insert_data(dataframe=separate_dfs['comments_replies'], table_name='comments_replies')


def youtube_playlists_transform(**kwargs):
    from src.selections import selections_youtube_playlists_transform
    
    # Load the JSON data
    data = read_json_file(kwargs['templates_dict']['input_path'])

    # Structure the unstructured data
    main_df, _ = json_to_dataframes(data, selections_youtube_playlists_transform)

    # Clowly Changing Dimension
    main_df['startDate'] = pd.to_datetime(kwargs['ds']).replace(hour=0, minute=0, second=0)
    main_df['endDate'] = pd.NaT

    # Stablish connection to SQL DDBB
    sqlengine = SQLEngine()
    sqlengine.get_connection(database="KantianProjectDDBB")
    sqlengine.insert_data(dataframe=main_df, table_name='playlists')