import os
from google.cloud import secretmanager

def get_secret(secret_id: str, version_id: str = "latest") -> str:
    """Access the payload for the given secret version if one exists."""
    project_id = os.environ.get("PROJECT_ID", "kabum-gcp") # Default or get from env
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

SCHEMA_FILENAME = "instagram_v22/schema.json"
HOST = "https://graph.facebook.com"
API_VERSION = "/v22.0/"
IG_ID_ENDPOINT = HOST + API_VERSION + "{}?fields=instagram_business_account.fields(id, username)"
DATA_ENDPOINT = HOST + API_VERSION + "{}"
MEDIA_ENDPOINT = HOST + API_VERSION + "{}" + "/media"
INSIGHTS_ENDPOINT = HOST + API_VERSION + "{}" + "/insights"

IG_FIELDS = [
    "biography",
    "id",
    "followers_count",
    "follows_count",
    "media_count",
    "name",
    "profile_picture_url",
    "username",
    "website"
]
BREAKDOWN_METRICS = {
    "media_product_type": [
        "total_interactions",
        "likes",
        "comments",
        "saves",
        "shares"
    ],
    "follow_type": [
        "follows_and_unfollows"
    ],
    "contact_button_type": [
        "profile_links_taps"
    ],
    "combined": [
        "reach"
    ],
    "n/a": [
        "impressions",
        "accounts_engaged",
        "replies",
        "website_clicks",
        "profile_views"
    ]
}
DEMOGRAPHIC_METRICS = {
    "age_gender": [
        "engaged_audience_demographics",
        "reached_audience_demographics",
        "follower_demographics"
    ],
    "city": [
        "engaged_audience_demographics",
        "reached_audience_demographics",
        "follower_demographics"
    ],
    "country": [
        "engaged_audience_demographics",
        "reached_audience_demographics",
        "follower_demographics"
    ]         
}
MEDIA_FIELDS = [
        "caption",
        "comments_count",
        "id",
        "is_comment_enabled",
        "is_shared_to_feed",
        "like_count",
        "media_product_type",    
        "media_type",            
        "media_url",
        "owner",
        "permalink",
        "shortcode",
        "timestamp",
        "username",
        "comments.limit(10000){from,hidden,id,like_count,media,parent_id,text,timestamp,user,username,replies}"
    ]
MEDIA_BREAKDOWN_METRICS = {
    "action_type": [
        "profile_activity"
    ],
    "n/a": [
        "comments",
        "follows",
        "likes",
        "profile_visits",
        "shares",
        "total_interactions"
    ]
}
DISCOVERY_FIELDS = {
    "ig": [
        "name",
        "username",
        "biography",
        "id",
        "profile_picture_url",
        "followers_count",
        "follows_count",
        "media_count"
    ],
    "media": [
        "id",
        "caption",
        "media_type",
        "media_product_type",
        "comments_count",
        "like_count",
        "audio_name"
    ]
}
PARAMS = {
    "discovery": {
        "fields": "business_discovery.username({}){{" + 
            ",".join(DISCOVERY_FIELDS["ig"]) + 
            ",media.limit(100){{" + 
            ",".join(DISCOVERY_FIELDS["media"]) + "}}" + "}}"
    },
    "ig_fields": {
        "fields": ",".join(IG_FIELDS)
    },
    "media_product_type": {
        "metric": ",".join(BREAKDOWN_METRICS["media_product_type"]),
        "breakdown": "media_product_type",
        "period": "day",
        "metric_type": "total_value",
        "since": "",
        "until": "",
        "access_token": ""
    },
    "follow_type": {
        "metric": ",".join(BREAKDOWN_METRICS["follow_type"]),
        "breakdown": "follow_type",
        "period": "day",
        "metric_type": "total_value",
        "since": "",
        "until": "",
        "access_token": ""
    },
    "contact_button_type": {
        "metric": ",".join(BREAKDOWN_METRICS["contact_button_type"]),
        "breakdown": "contact_button_type",
        "period": "day",
        "metric_type": "total_value",
        "since": "",
        "until": "",
        "access_token": ""
    },
    "n/a": {
        "metric": ",".join(BREAKDOWN_METRICS["n/a"]),
        "period": "day",
        "metric_type": "total_value",
        "since": "",
        "until": "",
        "access_token": ""
    },    
    "combined": {
        "metric": ",".join(BREAKDOWN_METRICS["combined"]),
        "breakdown": "media_product_type, follow_type",
        "period": "day",
        "metric_type": "total_value",
        "since": "",
        "until": "",
        "access_token": ""
    },
    "demographic_city": {
        "metric": ",".join(DEMOGRAPHIC_METRICS["city"]),
        "breakdown": "city",
        "timeframe": "last_90_days",
        "period": "lifetime",
        "metric_type": "total_value",
        "access_token": ""
    },
    "demographic_country": {
        "metric": ",".join(DEMOGRAPHIC_METRICS["country"]),
        "breakdown": "country",
        "timeframe": "last_90_days",
        "period": "lifetime",
        "metric_type": "total_value",
        "access_token": ""
    },
    "demographic_age_gender": {
        "metric": ",".join(DEMOGRAPHIC_METRICS["age_gender"]),
        "breakdown": "age,gender",
        "timeframe": "last_90_days",
        "period": "lifetime",
        "metric_type": "total_value",
        "access_token": ""
    },
    "media_fields": {
        "fields": ",".join(MEDIA_FIELDS) + 
            ",insights.metric(" +
            ",".join(MEDIA_BREAKDOWN_METRICS["n/a"]) + 
            "){description,id,name,period,title,values}",
        "access_token": ""
    },
    "media_action_type": {
        "fields": "id,timestamp, insights.metric(" + 
            ",".join(MEDIA_BREAKDOWN_METRICS["action_type"]) + 
            ").breakdown(action_type){name, period, values, title, description, total_value, breakdowns}",
        "access_token": ""
    }
}