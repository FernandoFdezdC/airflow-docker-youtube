# -*- coding: utf-8 -*-

import json
import os
import googleapiclient.discovery
from src.secrets.API_keys import DEVELOPER_KEY
from src.utils.utils import (get_videos, get_video_comments, get_playlists,
                             expand_videos_data, get_playlist_videos)


def youtube_channel_info_extract(**kwargs):
    # Access YouTube API v3
    api_service_name = "youtube"
    api_version = "v3"
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = DEVELOPER_KEY)

    # Make a request to YouTube API
    request = youtube.search().list(
        part = 'snippet',
        q = 'KantianProject'
    )
    response = request.execute()

    # Make a request to YouTube API
    request = youtube.channels().list(
        part = 'contentDetails,contentOwnerDetails,id,localizations,snippet,statistics,status,topicDetails',
        id = response['items'][0]['snippet']['channelId']
    )
    response = request.execute()
    assert len(response["items"])==1

    # Assert that directories exist
    if not os.path.exists(os.path.dirname(kwargs['templates_dict']['load_path'])):
        os.makedirs(os.path.dirname(kwargs['templates_dict']['load_path']), exist_ok=True)

    # Store as JSON file
    with open(kwargs['templates_dict']['load_path'], "w", encoding="utf-8") as json_file:
        json.dump(response, json_file, ensure_ascii=False, indent=4)


def youtube_videos_extract(**kwargs):
    # Access YouTube API v3
    api_service_name = "youtube"
    api_version = "v3"
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = DEVELOPER_KEY)

    # Get channel information
    with open(kwargs['templates_dict']['channel_info'], 'r', encoding='utf-8') as file:
        channel_data = json.load(file)

    uploads_id = channel_data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    # Get all YouTube channel videos
    videos = get_videos(youtube=youtube, uploads_id=uploads_id)

    # Assert that directories exist
    if not os.path.exists(os.path.dirname(kwargs['templates_dict']['load_path'])):
        os.makedirs(os.path.dirname(kwargs['templates_dict']['load_path']), exist_ok=True)

    # Store as JSON file
    with open(kwargs['templates_dict']['load_path'], "w", encoding="utf-8") as json_file:
        json.dump(videos, json_file, ensure_ascii=False, indent=4)


def youtube_expand_videos_data(**kwargs):
    # Access YouTube API v3
    api_service_name = "youtube"
    api_version = "v3"
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = DEVELOPER_KEY)

    # Get videos information
    with open(kwargs['templates_dict']['input_path'], 'r', encoding='utf-8') as file:
        videos = json.load(file)

    # Get more information about these videos
    expanded_videos_data = expand_videos_data(youtube=youtube, videos=videos)

    # Assert that directories exist
    if not os.path.exists(os.path.dirname(kwargs['templates_dict']['load_path'])):
        os.makedirs(os.path.dirname(kwargs['templates_dict']['load_path']), exist_ok=True)

    # Store as JSON file
    with open(kwargs['templates_dict']['load_path'], "w", encoding="utf-8") as json_file:
        json.dump(expanded_videos_data, json_file, ensure_ascii=False, indent=4)


def youtube_comments_extract(**kwargs):
    # Access YouTube API v3
    api_service_name = "youtube"
    api_version = "v3"
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = DEVELOPER_KEY)
    
    # Open and read JSON file
    with open(kwargs['templates_dict']['videos_path'], 'r', encoding='utf-8') as file:
        videos = json.load(file)

    # Get all video comments
    comments = []
    for video in videos:
        videoId = video['snippet']['resourceId']['videoId']
        comments = comments + [{'videoId': videoId, 'comments': get_video_comments(youtube=youtube, video_id=videoId)}]

    # Assert that directories exist
    if not os.path.exists(os.path.dirname(kwargs['templates_dict']['load_path'])):
        os.makedirs(os.path.dirname(kwargs['templates_dict']['load_path']), exist_ok=True)

    # Store as JSON file
    with open(kwargs['templates_dict']['load_path'], "w", encoding="utf-8") as json_file:
        json.dump(comments, json_file, ensure_ascii=False, indent=4)


def youtube_playlists_extract(**kwargs):
    # Access YouTube API v3
    api_service_name = "youtube"
    api_version = "v3"
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = DEVELOPER_KEY)

    # Get channelId
    with open(kwargs['templates_dict']['channel_info'], 'r', encoding='utf-8') as file:
        channel_data = json.load(file)

    channelId = channel_data["items"][0]["id"]

    # Get all YouTube channel playlists
    playlists = get_playlists(youtube=youtube, channel_id=channelId)

    # Assert that directories exist
    if not os.path.exists(os.path.dirname(kwargs['templates_dict']['load_path'])):
        os.makedirs(os.path.dirname(kwargs['templates_dict']['load_path']), exist_ok=True)

    # Store as JSON file
    with open(kwargs['templates_dict']['load_path'], "w", encoding="utf-8") as json_file:
        json.dump(playlists, json_file, ensure_ascii=False, indent=4)


def youtube_playlists_videos_extract(**kwargs):
    # Access YouTube API v3
    api_service_name = "youtube"
    api_version = "v3"
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = DEVELOPER_KEY)

    # Get playlists information
    with open(kwargs['templates_dict']['input_path'], 'r', encoding='utf-8') as file:
        playlists = json.load(file)

    # Get all playlists videos
    playlists_videos = []
    for playlist in playlists:
        playlistId = playlist['id']
        playlists_videos = playlists_videos + [{'playlistId': playlistId, 'videos': get_playlist_videos(youtube=youtube, playlist_id=playlistId)}]

    # Assert that directories exist
    if not os.path.exists(os.path.dirname(kwargs['templates_dict']['load_path'])):
        os.makedirs(os.path.dirname(kwargs['templates_dict']['load_path']), exist_ok=True)

    # Store as JSON file
    with open(kwargs['templates_dict']['load_path'], "w", encoding="utf-8") as json_file:
        json.dump(playlists_videos, json_file, ensure_ascii=False, indent=4)