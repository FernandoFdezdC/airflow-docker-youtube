selections_youtube_channel_info_transform={'kind': 'skip',
    'etag': 'skip',
    'pageInfo': 'skip',
    'items': ('separateInner',
    {'items.kind': 'skip',
    'items.etag': 'skip',
    'items.id': 'keep',
    'items.snippet': ('keep',
        {'items.snippet.title': 'keep',
        'items.snippet.description': 'keep',
        'items.snippet.customUrl': 'keep',
        'items.snippet.publishedAt': 'keep',
        'items.snippet.thumbnails': ('keep',
        {'items.snippet.thumbnails.default': 'skip',
        'items.snippet.thumbnails.medium': 'skip',
        'items.snippet.thumbnails.high': ('keep',
            {'items.snippet.thumbnails.high.url': 'keep',
            'items.snippet.thumbnails.high.width': 'skip',
            'items.snippet.thumbnails.high.height': 'skip'})}),
        'items.snippet.defaultLanguage': 'keep',
        'items.snippet.localized': ('keep',
        {'items.snippet.localized.title': 'keep',
        'items.snippet.localized.description': 'keep'})}),
    'items.contentDetails': 'skip',
    'items.statistics': ('keep',
        {'items.statistics.viewCount': 'keep',
        'items.statistics.subscriberCount': 'keep',
        'items.statistics.hiddenSubscriberCount': 'skip',
        'items.statistics.videoCount': 'keep'}),
    'items.topicDetails': ('separateInner',
        {'items.topicDetails.topicIds': 'skip',
        'items.topicDetails.topicCategories': 'separateInner'}),
    'items.status': 'skip',
    'items.contentOwnerDetails': 'skip',
    'items.localizations': ('separateInner',
        {'items.localizations.en_US': ('separateInner',
        {'items.localizations.en_US.title': 'keep',
        'items.localizations.en_US.description': 'keep'}),
        'items.localizations.es_ES': ('separateInner',
        {'items.localizations.es_ES.title': 'keep',
        'items.localizations.es_ES.description': 'keep'}),
        'items.localizations.de_DE': ('separateInner',
        {'items.localizations.de_DE.title': 'keep',
        'items.localizations.de_DE.description': 'keep'})})})}


selections_youtube_videos_transform={'dimension': 'keep',
    'videoId': 'keep',
    'localized': ('separateInner',
    {'localized.title': 'keep', 'localized.description': 'keep'}),
    'projection': 'keep',
    'publishedAt': 'keep',
    'thumbnails': ('keep',
    {'thumbnails.medium': 'skip',
    'thumbnails.high': 'skip',
    'thumbnails.standard': 'skip',
    'thumbnails.maxres': ('keep',
        {'thumbnails.maxres.url': 'keep',
        'thumbnails.maxres.width': 'skip',
        'thumbnails.maxres.height': 'skip'}),
    'thumbnails.default': 'skip'}),
    'id': 'skip',
    'duration': 'keep',
    'categoryId': 'keep',
    'commentCount': 'keep',
    'viewCount': 'keep',
    'title': 'keep',
    'liveBroadcastContent': 'keep',
    'licensedContent': 'keep',
    'favoriteCount': 'skip',
    'likeCount': 'keep',
    'caption': 'skip',
    'definition': 'keep',
    'contentRating': 'skip',
    'description': 'keep'}


selections_youtube_comments_transform={'videoId': 'keep', 'comments': ('separateInner',
    {'comments.snippet': ('keep',
        {'comments.snippet.channelId': 'skip',
        'comments.snippet.videoId': 'skip',
        'comments.snippet.topLevelComment': ('keep',
        {'comments.snippet.topLevelComment.kind': 'skip',
        'comments.snippet.topLevelComment.etag': 'skip',
        'comments.snippet.topLevelComment.id': 'skip',
        'comments.snippet.topLevelComment.snippet': ('keep',
            {'comments.snippet.topLevelComment.snippet.channelId': 'skip',
            'comments.snippet.topLevelComment.snippet.videoId': 'keep',
            'comments.snippet.topLevelComment.snippet.textDisplay': 'keep',
            'comments.snippet.topLevelComment.snippet.textOriginal': 'keep',
            'comments.snippet.topLevelComment.snippet.authorDisplayName': 'keep',
            'comments.snippet.topLevelComment.snippet.authorProfileImageUrl': 'keep',
            'comments.snippet.topLevelComment.snippet.authorChannelUrl': 'keep',
            'comments.snippet.topLevelComment.snippet.authorChannelId': ('keep',
            {'comments.snippet.topLevelComment.snippet.authorChannelId.value': 'keep'}),
            'comments.snippet.topLevelComment.snippet.canRate': 'keep',
            'comments.snippet.topLevelComment.snippet.viewerRating': 'skip',
            'comments.snippet.topLevelComment.snippet.likeCount': 'keep',
            'comments.snippet.topLevelComment.snippet.publishedAt': 'keep',
            'comments.snippet.topLevelComment.snippet.updatedAt': 'keep'})}),
        'comments.snippet.canReply': 'skip',
        'comments.snippet.totalReplyCount': 'keep',
        'comments.snippet.isPublic': 'keep'}),
    'comments.id': 'keep',
    'comments.kind': 'skip',
    'comments.etag': 'skip',
    'comments.replies': ('separateInner',
        {'comments.replies.comments': ('keep',
        {'comments.replies.comments.kind': 'skip',
        'comments.replies.comments.etag': 'skip',
        'comments.replies.comments.id': 'skip',
        'comments.replies.comments.snippet': ('keep',
            {'comments.replies.comments.snippet.channelId': 'skip',
            'comments.replies.comments.snippet.videoId': 'skip',
            'comments.replies.comments.snippet.textDisplay': 'keep',
            'comments.replies.comments.snippet.textOriginal': 'keep',
            'comments.replies.comments.snippet.parentId': 'keep',
            'comments.replies.comments.snippet.authorDisplayName': 'keep',
            'comments.replies.comments.snippet.authorProfileImageUrl': 'keep',
            'comments.replies.comments.snippet.authorChannelUrl': 'keep',
            'comments.replies.comments.snippet.authorChannelId': ('keep',
            {'comments.replies.comments.snippet.authorChannelId.value': 'keep'}),
            'comments.replies.comments.snippet.canRate': 'skip',
            'comments.replies.comments.snippet.viewerRating': 'skip',
            'comments.replies.comments.snippet.likeCount': 'keep',
            'comments.replies.comments.snippet.publishedAt': 'keep',
            'comments.replies.comments.snippet.updatedAt': 'keep'})})})})}

selections_youtube_playlists_transform={'snippet': ('keep',
    {'snippet.title': 'keep',
    'snippet.channelId': 'skip',
    'snippet.channelTitle': 'keep',
    'snippet.description': 'keep',
    'snippet.publishedAt': 'keep',
    'snippet.thumbnails': ('keep',
        {'snippet.thumbnails.standard': 'skip',
        'snippet.thumbnails.maxres': 'skip',
        'snippet.thumbnails.high': ('keep',
        {'snippet.thumbnails.high.url': 'keep',
        'snippet.thumbnails.high.width': 'skip',
        'snippet.thumbnails.high.height': 'skip'}),
        'snippet.thumbnails.default': 'skip',
        'snippet.thumbnails.medium': 'skip'}),
    'snippet.localized': ('keep',
        {'snippet.localized.title': 'keep',
        'snippet.localized.description': 'keep'})}),
    'player': ('keep', {'player.embedHtml': 'keep'}),
    'status': ('keep', {'status.privacyStatus': 'keep'}),
    'id': 'keep',
    'contentDetails': ('keep', {'contentDetails.itemCount': 'keep'}),
    'kind': 'skip',
    'etag': 'skip'}


selections_youtube_add_videos_playlists={'playlistId': 'skip',
    'videos': ('separateInner',
    {'videos.contentDetails': 'skip',
    'videos.id': 'skip',
    'videos.snippet': ('keep',
        {'videos.snippet.videoOwnerChannelId': 'skip',
        'videos.snippet.position': 'skip',
        'videos.snippet.videoOwnerChannelTitle': 'skip',
        'videos.snippet.resourceId': ('keep',
        {'videos.snippet.resourceId.kind': 'skip',
        'videos.snippet.resourceId.videoId': 'keep'}),
        'videos.snippet.playlistId': 'keep',
        'videos.snippet.title': 'keep',
        'videos.snippet.channelId': 'skip',
        'videos.snippet.publishedAt': 'skip',
        'videos.snippet.thumbnails': 'skip',
        'videos.snippet.description': 'skip',
        'videos.snippet.channelTitle': 'skip'}),
    'videos.kind': 'skip',
    'videos.etag': 'skip',
    'videos.status': ('keep', {'videos.status.privacyStatus': 'keep'})})}