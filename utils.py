
SCHEMA_FILENAME = "instagram_schemas.json"
STORAGE_BUCKET_NAME = ""
FACEBOOK_CREDENTIALS_NAME = ""
SECRET_PROJECT_ID = ""

API_VERSION = "v10.0/"
HOST = "https://graph.facebook.com/"

ACCOUNT_GENERAL_DATA_ENDPOINT = HOST + API_VERSION + "{}"
ACCOUNT_INSIGHTS_ENDPOINT = HOST + API_VERSION + "{}/insights"
MEDIA_ID_ENDPOINT = HOST + API_VERSION + "{}/media"
MEDIA_GENERAL_DATA_ENDPOINT = HOST + API_VERSION + "{}"
MEDIA_INSIGHTS_ENDPOINT = HOST + API_VERSION + "{}/insights"

ACCOUNT_FIELDS = [
    "biography",
    "id",
    "ig_id",
    "followers_count",
    "follows_count",
    "media_count",
    "name",
    "profile_picture_url",
    "username",
    "website"
]
ACCOUNT_METRICS = {
    "day": [
        "email_contacts",
        "follower_count",
        "get_directions_clicks",
        "impressions",
        "phone_call_clicks",
        "profile_views",
        "reach",
        "text_message_clicks",
        "website_clicks"
    ],
    "lifetime": [
        "audience_city",
        "audience_country",
        "audience_gender_age",
        "audience_locale"
    ]
}
MEDIA_FIELDS = [
    "caption",
    "like_count",
    "media_url",
    "media_type",
    "media_product_type",
    "comments_count",
    "timestamp",
    "permalink",
    "ig_id",
    "is_comment_enabled",
    "thumbnail_url",
    "username",
    "video_title"
]
MEDIA_IMAGE_METRICS = [
    "engagement",
    "impressions",
    "reach",
    "saved"
]
MEDIA_REELS_METRICS = [
    "comments",
    "likes",
    "plays",
    "reach",
    "saved",
    "shares",
    "total_interactions"
]
MEDIA_VIDEO_METRICS = MEDIA_IMAGE_METRICS + ["video_views"]
MEDIA_ALBUM_METRICS = [
    "carousel_album_engagement",
    "carousel_album_impressions",
    "carousel_album_reach",
    "carousel_album_saved"
]
MEDIA_STORY_METRICS = [
    "exits",
    "impressions",
    "reach",
    "replies",
    "taps_forward",
    "taps_back"
]

MEDIA_TYPES = {
    "CAROUSEL_ALBUM": MEDIA_ALBUM_METRICS,
    "IMAGE": MEDIA_IMAGE_METRICS,
    "VIDEO": MEDIA_VIDEO_METRICS,
    "STORY": MEDIA_STORY_METRICS,
    "REELS": MEDIA_REELS_METRICS
}
