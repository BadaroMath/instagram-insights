
from httplib2 import Http
from datetime import datetime
from json import dumps
from google.cloud import bigquery
from webhooks.utils import DESTINATIONS
from google.api_core.exceptions import NotFound

def chat_webhook(
        message: str
        ):
        """Google Chat incoming webhook quickstart."""
        WEBHOOK_URL = "https://chat.googleapis.com/v1/spaces/AAAAj-Vv5fQ/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=bfghBmP6Yd6JcCRnBwkzEOgwsJ9bWMSs9u2fH1e37F0"
        url = WEBHOOK_URL
        bot_message = {
            'text': f'{message}'}
        message_headers = {'Content-Type': 'application/json; charset=UTF-8'}
        http_obj = Http()
        response = http_obj.request(
            uri=url,
            method='POST',
            headers=message_headers,
            body=dumps(bot_message),
        )


def comments(
          request_json: dict
          ) -> str:
    """ comments Field Sample
    {
        "entry":[
            {
                "id":"0",
                "time":1695061705,
                "changes":[
                    {
                    "field":"comments",
                    "value":{
                        "from":{
                            "id":"232323232",
                            "username":"test"
                        },
                        "media":{
                            "id":"123123123",
                            "media_product_type":"FEED"
                        },
                        "id":"17865799348089039",
                        "parent_id":"1231231234",
                        "text":"This is an example."
                    }
                    }
                ]
            }
        ],
        "object":"instagram"
    }
    """
    ig_id = request_json['entry'][0]['id']
    project_id = DESTINATIONS[ig_id]['project_id']
    dataset_id = DESTINATIONS[ig_id]['dataset_id']
    table_id = f'instagram_comments_{ig_id}'
    client = bigquery.Client(project=project_id)
    try:
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)
    except NotFound:
        print(f"Table {table_id} not found in dataset {dataset_id}.")
        schema = [
            bigquery.SchemaField("field", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("from_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("from_username", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("media_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("media_product_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("parent_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("text", "STRING", mode="REQUIRED"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(
            "Created table {}.{}.{}".format(
                table.project, table.dataset_id, table.table_id
            )
        )            
    table = client.get_table(table_ref)
    rows_to_insert = [
        (
            request_json['entry'][0]['changes'][0]['field'],
            datetime.utcfromtimestamp(request_json['entry'][0]['time']).strftime('%Y-%m-%d %H:%M:%S'),
            request_json['entry'][0]['changes'][0]['value']['from']['id'],
            request_json['entry'][0]['changes'][0]['value']['from']['username'],
            request_json['entry'][0]['changes'][0]['value']['media']['id'],
            request_json['entry'][0]['changes'][0]['value']['media']['media_product_type'],
            request_json['entry'][0]['changes'][0]['value']['id'],
            request_json['entry'][0]['changes'][0]['value']['parent_id'],
            request_json['entry'][0]['changes'][0]['value']['text']
        )
    ]
    errors = client.insert_rows(table, rows_to_insert)
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
    return "comments added to bigquery table successfully"


def live_comments(request_json): 
    """ live_comments Field Sample
    {
        "entry":[
            {
                "id":"0",
                "time":1695061876,
                "changes":[
                    {
                    "field":"live_comments",
                    "value":{
                        "from":{
                            "id":"232323232",
                            "username":"test"
                        },
                        "media":{
                            "id":"123123123",
                            "media_product_type":"LIVE"
                        },
                        "id":"17865799348089039",
                        "text":"This is an example."
                    }
                    }
                ]
            }
        ],
        "object":"instagram"
    }
    """
    ig_id = request_json['entry'][0]['id']
    project_id = DESTINATIONS[ig_id]['project_id']
    dataset_id = DESTINATIONS[ig_id]['dataset_id']
    table_id = f'instagram_live_comments_{ig_id}'
    client = bigquery.Client(project=project_id)
    try:
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)
    except NotFound:
        print(f"Table {table_id} not found in dataset {dataset_id}.")
        schema = [
            bigquery.SchemaField("field", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("from_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("from_username", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("media_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("media_product_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("text", "STRING", mode="REQUIRED"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(
            "Created table {}.{}.{}".format(
                table.project, table.dataset_id, table.table_id
            )
        )
    table = client.get_table(table_ref)
    rows_to_insert = [
        (
            request_json['entry'][0]['changes'][0]['field'],
            datetime.utcfromtimestamp(request_json['entry'][0]['time']).strftime('%Y-%m-%d %H:%M:%S'),
            request_json['entry'][0]['changes'][0]['value']['from']['id'],
            request_json['entry'][0]['changes'][0]['value']['from']['username'],
            request_json['entry'][0]['changes'][0]['value']['media']['id'],
            request_json['entry'][0]['changes'][0]['value']['media']['media_product_type'],
            request_json['entry'][0]['changes'][0]['value']['id'],
            request_json['entry'][0]['changes'][0]['value']['text']
        )
    ]
    errors = client.insert_rows(table, rows_to_insert)
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
    return "live_comments added to bigquery table successfully"

def mentions(request_json):
    """ mentions Field Sample
    {
        "entry":[
            {
                "id":"0",
                "time":1695061948,
                "changes":[
                    {
                    "field":"mentions",
                    "value":{
                        "media_id":"17887498072083520",
                        "comment_id":"17887498072083520"
                    }
                    }
                ]
            }
        ],
        "object":"instagram"
    }
    """
    ig_id = request_json['entry'][0]['id']
    project_id = DESTINATIONS[ig_id]['project_id']
    dataset_id = DESTINATIONS[ig_id]['dataset_id']
    table_id = f'instagram_mentions_{ig_id}'
    client = bigquery.Client(project=project_id)
    try:
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)
    except NotFound:
        print(f"Table {table_id} not found in dataset {dataset_id}.")
        schema = [
            bigquery.SchemaField("field", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("media_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("comment_id", "STRING", mode="REQUIRED"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(
            "Created table {}.{}.{}".format(
                table.project, table.dataset_id, table.table_id
            )
        )
    table = client.get_table(table_ref)
    rows_to_insert = [
        (
            request_json['entry'][0]['changes'][0]['field'],
            datetime.utcfromtimestamp(request_json['entry'][0]['time']).strftime('%Y-%m-%d %H:%M:%S'),
            request_json['entry'][0]['changes'][0]['value']['media_id'],
            request_json['entry'][0]['changes'][0]['value']['comment_id']
        )
    ]
    errors = client.insert_rows(table, rows_to_insert)
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
    return "mentions added to bigquery table successfully"

def story_insights(request_json): 
    """ story_insights Field Sample
    {
        "entry":[
            {
                "id":"0",
                "time":1695062294,
                "changes":[
                    {
                    "field":"story_insights",
                    "value":{
                        "media_id":"17887498072083520",
                        "impressions":444,
                        "reach":44,
                        "taps_forward":4,
                        "taps_back":3,
                        "exits":3,
                        "replies":0
                    }
                    }
                ]
            }
        ],
        "object":"instagram"
    }
    """
    ig_id = request_json['entry'][0]['id']
    project_id = DESTINATIONS[ig_id]['project_id']
    dataset_id = DESTINATIONS[ig_id]['dataset_id']
    table_id = f'instagram_story_insights_{ig_id}'
    client = bigquery.Client(project=project_id)
    try:
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)
    except NotFound:
        print(f"Table {table_id} not found in dataset {dataset_id}.")
        schema = [
            bigquery.SchemaField("field", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("media_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("impressions", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("reach", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("taps_forward", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("taps_back", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("exits", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("replies", "STRING", mode="REQUIRED"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(
            "Created table {}.{}.{}".format(
                table.project, table.dataset_id, table.table_id
            )
        )
    table = client.get_table(table_ref)
    rows_to_insert = [
        (
            request_json['entry'][0]['changes'][0]['field'],
            datetime.utcfromtimestamp(request_json['entry'][0]['time']).strftime('%Y-%m-%d %H:%M:%S'),
            request_json['entry'][0]['changes'][0]["value"]['media_id'],
            request_json['entry'][0]['changes'][0]["value"]['impressions'],
            request_json['entry'][0]['changes'][0]["value"]['reach'],
            request_json['entry'][0]['changes'][0]["value"]['taps_forward'],
            request_json['entry'][0]['changes'][0]["value"]['taps_back'],
            request_json['entry'][0]['changes'][0]["value"]['exits'],
            request_json['entry'][0]['changes'][0]["value"]['replies']
        )
    ]
    errors = client.insert_rows(table, rows_to_insert)
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
    return "story_insights added to bigquery table successfully"


request_json = {
   "entry":[
      {
         "id":"17841402946855997",
         "time":1695062444,
         "changes":[
            {
               "field":"comments",
               "value":{
                  "from":{
                     "id":"232323232",
                     "username":"test"
                  },
                  "media":{
                     "id":"123123123",
                     "media_product_type":"FEED"
                  },
                  "id":"17865799348089039",
                  "parent_id":"1231231234",
                  "text":"This is an example."
               }
            }
         ]
      }
   ],
   "object":"instagram"
}
live = {
   "entry":[
      {
         "id":"17841402946855997",
         "time":1695062447,
         "changes":[
            {
               "field":"live_comments",
               "value":{
                  "from":{
                     "id":"232323232",
                     "username":"test"
                  },
                  "media":{
                     "id":"123123123",
                     "media_product_type":"LIVE"
                  },
                  "id":"17865799348089039",
                  "text":"This is an example."
               }
            }
         ]
      }
   ],
   "object":"instagram"
}
mentions_json = {
   "entry":[
      {
         "id":"17841402946855997",
         "time":1695062451,
         "changes":[
            {
               "field":"mentions",
               "value":{
                  "media_id":"17887498072083520",
                  "comment_id":"17887498072083520"
               }
            }
         ]
      }
   ],
   "object":"instagram"
}
story = {
   "entry":[
      {
         "id":"17841402946855997",
         "time":1695062469,
         "changes":[
            {
               "field":"story_insights",
               "value":{
                  "media_id":"17887498072083520",
                  "impressions":444,
                  "reach":44,
                  "taps_forward":4,
                  "taps_back":3,
                  "exits":3,
                  "replies":0
               }
            }
         ]
      }
   ],
   "object":"instagram"
}


def main(request):

    request_json = request.get_json()
    chat_webhook(str(request_json))
    try:
        if request_json['entry'][0]['id'] not in DESTINATIONS:
            print("ig_id not subscribed.")
        else:
            if request_json['entry'][0]['changes'][0]['field'] == 'comments':
                comments(request_json)
            elif request_json['entry'][0]['changes'][0]['field'] == 'live_comments':
                live_comments(request_json)
            elif request_json['entry'][0]['changes'][0]['field'] == 'mentions':
                mentions(request_json)
            elif request_json['entry'][0]['changes'][0]['field'] == 'story_insights':
                story_insights(request_json)
            else:
                print("field is subscribed but not implemented.")
    except Exception as e:
        print(e)
        print("Error in main function.")
        raise e
    else:
        return "OK"
            


