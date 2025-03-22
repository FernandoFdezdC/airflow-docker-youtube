def get_videos(youtube, uploads_id):
    
    # Make a request to YouTube API
    request = youtube.playlistItems().list(
        part = 'snippet',
        playlistId = uploads_id,
        maxResults = 50
    )
    # Execute the request
    response = request.execute()

    videos_number = response["pageInfo"]['totalResults']
    
    # Get the elements from the response.
    videos = response['items']

    while True:
        try:
            nextPageToken = response['nextPageToken']
        except KeyError:
            break
        nextPageToken = response['nextPageToken']
        # Create a new request object with the next page token.
        next_request = youtube.playlistItems().list(part="snippet", playlistId = uploads_id, maxResults=50, pageToken=nextPageToken)
        # Execute the next request.
        response = next_request.execute()
        videos = videos + response["items"]
    
    # print(videos_number)
    # print(len(videos))
    assert videos_number == len(videos)

    return videos

def expand_videos_data(youtube, videos):
    expanded_data=[]
    for item in videos:
        # Make a request to YouTube API
        request = youtube.videos().list(
            part = 'snippet,contentDetails,statistics',
            id = item['snippet']['resourceId']['videoId'],      # Real video id
            maxResults = 50
        )
        response = request.execute()
        assert len(response["items"])==1
        details=response["items"][0]
        assert details["snippet"]["publishedAt"]==item['snippet']['publishedAt']
        expanded_data.append(
            {
                'id': item['id'],
                'title': item['snippet']['title'],
                'description': item['snippet']['description'],
                'thumbnails': item['snippet']['thumbnails'],
                'videoId': item['snippet']['resourceId']['videoId'],
                'publishedAt': item['snippet']['publishedAt'],
                # 'tags': details["snippet"]['tags'],
                'categoryId': details["snippet"]['categoryId'],
                'liveBroadcastContent': details["snippet"]['liveBroadcastContent'],
                # 'defaultLanguage': details["snippet"]['defaultLanguage'],
                'localized': details["snippet"]['localized'],
                # 'defaultAudioLanguage': details["snippet"]['defaultAudioLanguage'],
                'duration': details["contentDetails"]['duration'],
                'dimension': details["contentDetails"]['dimension'],
                "definition": details["contentDetails"]['definition'],
                "caption": details["contentDetails"]['caption'],
                "licensedContent": details["contentDetails"]['licensedContent'],
                "contentRating": details["contentDetails"]['contentRating'],
                "projection": details["contentDetails"]['projection'],
                "viewCount": details["statistics"]['viewCount'],
                "likeCount": details["statistics"]['likeCount'],
                "favoriteCount": details["statistics"]['favoriteCount'],
                "commentCount": details["statistics"]['commentCount']
            }
    )
    
    return expanded_data

def get_playlist_videos(youtube, playlist_id):
    request = youtube.playlistItems().list(
        part = 'contentDetails,id,snippet,status',
        playlistId = playlist_id,
        maxResults = 50
    )

    # Execute the request
    response = request.execute()

    playlist_videos_number = response["pageInfo"]['totalResults']
    
    # Get the elements from the response.
    playlist_videos = response['items']

    while True:
        try:
            nextPageToken = response['nextPageToken']
        except KeyError:
            break
        nextPageToken = response['nextPageToken']
        # Create a new request object with the next page token.
        next_request = youtube.playlistItems().list(
            part = 'contentDetails,id,snippet,status',
            playlistId = playlist_id,
            maxResults = 50, pageToken=nextPageToken
        )
        # Execute the next request.
        response = next_request.execute()
        playlist_videos = playlist_videos + response["items"]
    
    assert playlist_videos_number == len(playlist_videos)

    return playlist_videos


def get_video_comments(youtube, video_id):
    request = youtube.commentThreads().list(
        part = 'id,snippet,replies',
        videoId=video_id,
        maxResults = 100
    )

    # Execute the request
    response = request.execute()

    comments_number = response["pageInfo"]['totalResults']
    
    # Get the elements from the response.
    comments=response['items']

    while True:
        try:
            nextPageToken = response['nextPageToken']
        except KeyError:
            break
        nextPageToken = response['nextPageToken']
        # Create a new request object with the next page token.
        next_request = youtube.commentThreads().list(part="id,snippet,replies", videoId=video_id, maxResults=100, pageToken=nextPageToken)
        # Execute the next request.
        response = next_request.execute()
        comments = comments + response["items"]

    return comments

def get_playlists(youtube, channel_id):
    request = youtube.playlists().list(
        part = 'id,contentDetails,localizations,player,snippet,status',
        channelId = channel_id,
        maxResults = 2
    )

    # Execute the request
    response = request.execute()

    playlists_number = response["pageInfo"]['totalResults']
    
    # Get the elements from the response.
    playlists = response['items']

    while True:
        try:
            nextPageToken = response['nextPageToken']
        except KeyError:
            break
        nextPageToken = response['nextPageToken']
        # Create a new request object with the next page token.
        next_request = youtube.playlists().list(part='id,contentDetails,localizations,player,snippet,status',
                             channelId = channel_id, maxResults=2, pageToken=nextPageToken)
        # Execute the next request.
        response = next_request.execute()
        playlists = playlists + response["items"]
    
    assert playlists_number == len(playlists)

    return playlists


import json
def make_report(**kwargs):
    # Get channel information
    with open(kwargs['templates_dict']['path'], 'r', encoding='utf-8') as file:
        channel_data = json.load(file)