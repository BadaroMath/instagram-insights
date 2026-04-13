from instagram_v18.utils import (
    PARAMS, 
    IG_ID_ENDPOINT, 
    DATA_ENDPOINT, 
    MEDIA_ENDPOINT,
    INSIGHTS_ENDPOINT,
    SCHEMA_FILENAME)
import requests
import logging as log
from datetime import datetime, timedelta
import pytz
import time
from google.cloud import bigquery, storage
import json
from collections import defaultdict
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"




def config_log():
    """
    Configure logging level, output format and sends it to a file.
    """
    today = datetime.today().strftime("%Y%m%d")
    logging_level = 20
    log.basicConfig(
        level=logging_level,
        format=f'[%(asctime)s.%(msecs)03d] %(levelname)s: %(funcName)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        force=True)

def get_token():
    """
    Get token from file to authenticate API calls.

    Arguments:
        config {dict} -- params with the path to token file

    Returns:
        {str} -- authentication token
    
    """
    with open("secrets/token.json", "r") as file:
        credentials = json.load(file)

    return credentials.get("access_token")


def make_api_call(endpoint, params, retry=False):
    """
    Make an API call and check for errors.
    Exits if an unexpected error occur.

    Arguments:
        endpoint {str} -- the URL to be called
        params {dict} -- params

    Returns:
        json_response {dict} -- the response of the call
    """

    response = requests.get(
        endpoint,
        params=params
    )

    json_response = response.json()
    if response.status_code == 200:
        return json_response

    if response.status_code == 500:
        if retry == False:
            log.warning("Server error. Retrying in 30 seconds")
            time.sleep(30)
            response = requests.get(endpoint,params=params)
            retry = True
        else:
            log.error(response.text)

def get_ig_id(access_token, account_id):
    """
    Get User IG id.
    https://developers.facebook.com/docs/instagram-api/reference/user

    Arguments:
        token {str} -- authentication token
        account_id {str} -- id of the account

    Returns:
        ig_id {str} -- IG id of the account
    """

    log.info("Retrieving IG id")

    ig_id_url = IG_ID_ENDPOINT.format(account_id)

    params = {
        "access_token": access_token
    }

    json_response = make_api_call(ig_id_url, params)
    id = json_response\
        .get("instagram_business_account")\
        .get("id")
    username = json_response\
        .get("instagram_business_account")\
        .get("username")
    return(id, username)


def get_ig_media(media_breakdown, ig_id, historical_media):
    params_teste = PARAMS[media_breakdown].copy()
    url = MEDIA_ENDPOINT.format(ig_id)
    medias = []
    today = datetime.now(tz=pytz.utc)
    d_ago = today - timedelta(days=historical_media)
    breaking = False    
    while breaking == False:
        json_response = make_api_call(url, params_teste)
        max_timestamp = datetime.strptime(json_response \
                        .get("data")[-1] \
                        .get("timestamp"),
                        "%Y-%m-%dT%H:%M:%S%z"
                        )
        medias += json_response.get("data")
        if max_timestamp < d_ago:
            breaking = True
        else:
            after = json_response \
                    .get("paging") \
                    .get("cursors") \
                    .get("after")
            params_teste["after"] = after

    return medias

def get_insights_breakdown(breakdown, ig_id, range_date):
    data_atual = datetime.now()
    insights = defaultdict(dict)
    if breakdown.startswith("demographic"):
        range_date = 1
    for i in range(range_date):
        params_ig_insights = PARAMS[breakdown].copy()
        params_ig_insights["since"] = (data_atual - timedelta(days=i+1)).strftime("%Y-%m-%d")
        params_ig_insights["until"] = (data_atual - timedelta(days=i)).strftime("%Y-%m-%d")
        url_insights = INSIGHTS_ENDPOINT.format(ig_id)
        json_response = make_api_call(url_insights, params_ig_insights)
        if json_response:
            insights[params_ig_insights["until"]].update(json_response)
    return insights


def get_ig_info(ig_id):
   
    params_ig_info = PARAMS["ig_fields"]
    url_ig_info = DATA_ENDPOINT.format(ig_id)
    json_response = make_api_call(url_ig_info, params_ig_info)
    return json_response


def get_discovery(usernames, ig_id):
    data_atual = datetime.now().strftime("%Y-%m-%d")
    dicoverys = {data_atual: []}
    for username in usernames:
        params_discovery = PARAMS["discovery"].copy()
        params_discovery["fields"] = params_discovery["fields"].format(username)
        url_discovery = DATA_ENDPOINT.format(ig_id)
        json_response = make_api_call(url_discovery, params_discovery)
        if json_response:
            if "paging" in json_response.get("business_discovery").get("media"):
                del json_response["business_discovery"]["media"]["paging"]
            dicoverys[data_atual].append(json_response.get("business_discovery"))
    return dicoverys


def transform_insights(combined, media_product_type, follow_type, contact_button_type, na):
    log.info("Transforming IG Insights")
    try:
        all_insights = {}
        for date in na:
            all_insights[date] = [{"insights": {"reach": combined[date]["data"][0].copy() if "data" in combined[date] else {}}}]
            all_insights[date][0]["insights"]["media_product"] = media_product_type[date]["data"].copy()
            all_insights[date][0]["insights"]["follow"] = follow_type[date]["data"][0].copy()
            all_insights[date][0]["insights"]["contact_buttom"] = contact_button_type[date]["data"][0].copy()
            all_insights[date][0]["insights"]["metrics"] = na[date]["data"].copy()
    except Exception as e:
        log.error(e)
        return {}
    else:
        log.info("Transformed IG Insights")
        return all_insights


def transform_ig_media(ig_id, media_results, media_at_results):
    log.info("Transforming IG Medias")
    data_atual = datetime.now().strftime("%Y-%m-%d")
    try:
        all_medias = defaultdict(dict)
        for item in media_results:
            all_medias[item["id"]].update(item)

        all_medias_break = defaultdict(dict)
        for media_break in media_at_results:
            all_medias_break[media_break["id"]].update(media_break)

        for id in all_medias_break:
            if id in all_medias:
                all_medias[id]["insights"]["data"].append({"breakdowns": all_medias_break[id]["insights"]["data"]}) if "insights" in all_medias_break[id] else None

        final_results = []
        for id in all_medias:
            final_results.append(all_medias[id])

        for media in final_results:
            try:
                del media["comments"]["paging"]
            except KeyError:
                pass
            if "comments" in media:
                for comment in media["comments"]["data"]:
                    try:
                        del comment["replies"]["paging"]
                    except KeyError:
                        pass
    except Exception as e:
        log.error(e)
        return []
    else:
        log.info("Transformed IG Medias")
        return {data_atual: final_results} 


def transform_demographic(demographic_city, demographic_country, demographic_age_gender, ig_info):
    log.info("Transforming IG Demographic")  
    try:
        all_demo = {}
        for date in demographic_age_gender:
            all_demo[date] = demographic_city[date].copy()
            for field in all_demo[date]["data"]:
                field
                field["total_value"]["breakdowns"] += [item for item in demographic_country[date]["data"].copy() if item.get("name") == field["name"]][0]["total_value"]["breakdowns"].copy()
                field["total_value"]["breakdowns"] += [item for item in demographic_age_gender[date]["data"].copy() if item.get("name") == field["name"]][0]["total_value"]["breakdowns"].copy()
    except Exception as e:
        log.error(e)
        return(None, None)
    else:
        log.info("Transformed IG Demographic")
        ig_geral = {}
        for date, demog in all_demo.items():
            ig_info["demographic_insights"] = demog.get("data").copy()
            ig_geral[date] = [ig_info]
            return ig_geral
        



def get_schema():
    """
    Get schema for BigQuery table from json file, according to type.
    Possible types: daily, lifetime, post and story.

    Arguments:
        table_type {str} -- type of table

    Returns:
        {dict} -- BigQuery table schema
    """

    with open(SCHEMA_FILENAME, "r") as schema_file:
        schemas = json.load(schema_file)

    return schemas

def upload_to_gcs(
    config: dict,
    data: list[dict],
    filename: str
    ) -> None:
    '''Uploads JSON data to a GCS bucket

    Arguments:
        bucket_name (str): Bucket name
        data (list[dict]): Json data
        filename (str): Name of the file
    '''
    log.info(f"Uploading data to gs://{config.get('bucket_name')}/{filename}.json")
    ndjson = ''

    for row in data:
        ndjson += json.dumps(row) + '\n'

    storage_client = storage.Client()
    bucket = storage_client.bucket(config.get('bucket_name'))
    blob = bucket.blob(f"{filename}.json")

    blob.upload_from_string(ndjson, content_type='application/json')

def bigquery_save_data(config, table_name, schema, partitioned):
    """
    Load json data to BigQuery table.

    Arguments:
        config {dict} -- configuration with BigQuery params
        table_name {str} -- name of the table that will receive data
        data {list} -- json data to be loaded
        schema {dict} -- BigQuery table schema
    """

    log.info(f"Loading data to {table_name}")

    bigquery_client = bigquery.Client(config.get("project_id"))
    dataset = bigquery_client.dataset(config.get("dataset_id"))
    table_ref = dataset.table(table_name)

    job_config = bigquery.LoadJobConfig()
    job_config.create_disposition = "CREATE_IF_NEEDED"
    job_config.source_format = "NEWLINE_DELIMITED_JSON"    
    if partitioned == True:
        job_config.time_partitioning = bigquery.table.TimePartitioning()
    job_config.schema = schema
    job_config.write_disposition = "WRITE_TRUNCATE"

    job = bigquery_client.load_table_from_uri(
        f"gs://{config.get('bucket_name')}/{table_name}.json",
        table_ref,
        job_config=job_config
    )

    try:
        job.result()
    except Exception:
        log.error(job.errors)


def save_data(config, data, table_type, edge, username, schema):
    """
    Save all data to BigQuery tables.

    Arguments:
        config {dict} -- configuration with BigQuery params
        account_data {dict} -- lifetime and daily data
        media_data {dict} -- posts and stories data
    """
    log.info("Saving data to BigQuery")

    if table_type == "daily":
        for date, records in data.items():
            partition = date.replace("-", "")
            table_name = f"instagram_{username}_{edge}_daily${partition}"
            upload_to_gcs(config, records, table_name)
            bigquery_save_data(config, table_name, schema, partitioned=True)
    else:
        for date, records in data.items():
            partition = date.replace("-", "")
            table_name = f"instagram_{username}_{edge}_lifetime_{partition}"
            upload_to_gcs(config, records, table_name)
            bigquery_save_data(config, table_name, schema, partitioned=False)


def ig_operator(ig_id):
    
    log.info("Fetching Instagram General data")
    ig_info = get_ig_info(ig_id)
    log.info("   Demographic breakdowns...")
    demographic_city = get_insights_breakdown("demographic_city", ig_id, 1)
    demographic_country = get_insights_breakdown("demographic_country", ig_id, 1)
    demographic_age_gender = get_insights_breakdown("demographic_age_gender", ig_id, 1)
    transformed_id = transform_demographic(demographic_city, demographic_country, demographic_age_gender, ig_info)
    return transformed_id

def ig_discovery_operator(ig_id, config):
    log.info("Fetching Instagram Discovery data")
    transformed_discovery = get_discovery(config["usernames"], ig_id)
    return transformed_discovery

def ig_insights_operator(ig_id, config):
    log.info("Fetching Instagram Insights data...")
    for breakdown in ["combined", "media_product_type", "follow_type", "contact_button_type", "n/a"]:
        log.info(f"   {breakdown} breakdowns...")
        globals()[breakdown.replace("/", "")] = get_insights_breakdown(breakdown, ig_id, config.get("range_date"))
    transformed_insights = transform_insights(combined, media_product_type, follow_type, contact_button_type, na)
    return transformed_insights

def ig_media(ig_id, config):
    log.info("Fetching Instagram Media data")
    media_results = get_ig_media("media_fields", ig_id, config.get("historical_media"))
    media_at_results = get_ig_media("media_action_type", ig_id, config.get("historical_media"))
    transformed_media = transform_ig_media(ig_id, media_results, media_at_results)
    return transformed_media



def main(request):
    try:
        config = request  # You can get the JSON request directly if it's a Cloud Function trigger
        access_token = get_token()
        config_log()
        for item in PARAMS:
            PARAMS[item]["access_token"] = access_token
        ig_id, username = get_ig_id(access_token, config.get("account_id"))

        schema = get_schema()  # Adjust as needed
        save_data(config, ig_insights_operator(ig_id, config), "daily", "insights", username, schema.get("daily").get("ig-insights"))
        save_data(config, ig_operator(ig_id), "lifetime", "account", username, schema.get("lifetime").get("ig"))
        save_data(config, ig_media(ig_id, config), "lifetime", "media", username, schema.get("lifetime").get("ig-media"))
        save_data(config, ig_discovery_operator(ig_id, config), "lifetime", "discovery", username, schema.get("lifetime").get("ig-business-discovery"))
        log.info("Data processing completed successfully")

    except Exception as e:
        log.error(f"An error occurred: {str(e)}")
        raise e





request = {
    "account_id": "141453935911808",
    "usernames": [
        "instagram",
        "instagrambrasil"
    ],
    "project_id": "raccoon-ds",
    "dataset_id": "media_monks_social",
    "bucket_name": "instagram-organic",
    "historical_media": 30,
    "range_date": 2
}

main(request)