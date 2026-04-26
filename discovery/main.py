"""
Instagram Business Discovery Pipeline module.

This module provides functions to retrieve Instagram Business data,
upload to Google Cloud Storage, load into BigQuery, schedule continuation
tasks in Cloud Tasks, and orchestrate the pipeline as a Cloud Function.
"""
import json
import time
import logging as log
from datetime import datetime, timedelta
import os
import requests
from google.cloud import bigquery, storage, tasks_v2
from google.cloud.exceptions import NotFound, GoogleCloudError
from google.protobuf import timestamp_pb2
from utils import (
    PARAMS,
    IG_ID_ENDPOINT,
    DATA_ENDPOINT,
    USER_MEDIA_ENDPOINT,
    PAGINATED_MEDIA_FIELDS_STRING,
    SCHEMA_DISCOVERED_ACCOUNTS,
    SCHEMA_DISCOVERED_MEDIA,
    BQ_DISCOVERED_ACCOUNTS_TABLE_BASENAME,
    BQ_DISCOVERED_ACCOUNTS_STAGING_TABLE_BASENAME,
    BQ_DISCOVERED_MEDIA_TABLE_NAME,
    BQ_DISCOVERED_MEDIA_STAGING_TABLE_NAME,
    get_original_schemas_from_file,
    DISCOVERY_FIELDS_IG_ACCOUNT,
    DISCOVERY_FIELDS_MEDIA_ITEMS,
    API_RATE_LIMIT_ERROR_CODE,
    get_secret,
)

API_RATE_LIMIT_ERROR_CODE = 4
GCS_PROJECT_ID_GLOBAL = None


def config_log():
    """
    Configure the root logger with INFO level and a standard format.
    """
    log.basicConfig(
        level=log.INFO,
        format=f"[%(asctime)s.%(msecs)03d] %(levelname)s: %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )


def get_token(bm: bool) -> str | None:
    """
    Retrieve the Instagram Graph API access token from Secret Manager.
    """
    try:
        secret_name = "instagram_access_token_sorocaba" if bm else "instagram_access_token"
        token = get_secret(secret_name)
        if not token:
            log.error(f"Secret '{secret_name}' está vazio no Secret Manager.")
        return token
    except Exception as e:
        log.error(f"Erro ao buscar secret no Secret Manager: {e}")
    return None


def make_api_call(
    endpoint: str, params: dict, retries: int = 2, backoff_factor: int = 30
) -> tuple[dict | None, requests.Response | None]:
    """
    Perform a GET request with retry logic and exponential backoff.
    Returns JSON response and the HTTP response object.
    """
    last_response = None
    for attempt in range(retries + 1):
        try:
            log.info(f"Calling {endpoint} with params: {params}")
            response = requests.get(endpoint, params=params, timeout=60)
            log.info(f"Status: {response.status_code}")
            last_response = response
            if response.status_code == 200:
                try:
                    return response.json(), response
                except json.JSONDecodeError as e:
                    log.error(
                        f"Erro ao decodificar JSON da API {endpoint} na tentativa {attempt + 1}: {e}. Resposta: {response.text[:200]}"
                    )
            log.warning(
                f"API call {endpoint} status {response.status_code} att {attempt + 1}/{retries + 1}. Resp: {response.text[:200]}"
            )
            if response.status_code == 403:
                try:
                    error_data = response.json()
                    if (
                        error_data.get("error", {}).get("code")
                        == API_RATE_LIMIT_ERROR_CODE
                    ):
                        log.error(
                            f"Erro de Rate Limit ({API_RATE_LIMIT_ERROR_CODE}) detectado. Não haverá retentativa para esta chamada."
                        )
                        return None, response
                except json.JSONDecodeError:
                    log.warning(
                        f"Não foi possível decodificar erro JSON da resposta 403: {response.text[:200]}"
                    )
            if response.status_code >= 500 and attempt < retries:
                time.sleep(backoff_factor)
            elif response.status_code < 500 and response.status_code != 403:
                return None, response
            elif response.status_code == 403 and attempt == retries:
                return None, response
        except requests.exceptions.RequestException as e:
            log.error(f"RequestException API call {endpoint} att {attempt + 1}: {e}")
            if attempt < retries:
                time.sleep(backoff_factor)
            else:
                last_response = None
    return None, last_response


def get_ig_id(access_token: str, account_id: str) -> tuple[str | None, str | None]:
    """
    Retrieve the Instagram Business Account ID and username for a given Facebook Page.
    """
    log.info(f"Recuperando IG ID para FB Page ID: {account_id}")
    ig_id_url = IG_ID_ENDPOINT.format(account_id)
    params = {"access_token": access_token}
    json_response, response_obj = make_api_call(ig_id_url, params)
    if response_obj and response_obj.status_code == 403:
        try:
            error_data = response_obj.json()
            if error_data.get("error", {}).get("code") == API_RATE_LIMIT_ERROR_CODE:
                log.warning("Rate limit atingido ao tentar obter IG ID.")
                return "RATE_LIMIT_HIT", "RATE_LIMIT_HIT"
        except json.JSONDecodeError:
            pass
    if json_response and "instagram_business_account" in json_response:
        ig_account = json_response["instagram_business_account"]
        ig_id, username = ig_account.get("id"), ig_account.get("username")
        if ig_id and username:
            log.info(f"IG ID: {ig_id}, Username: {username} recuperados.")
            return ig_id, username
    log.error(
        f"Não foi possível obter IG ID para Page ID {account_id}. Resposta: {json_response if json_response else (response_obj.text[:200] if response_obj else 'N/A')}"
    )
    return None, None


def get_business_discovery_data(
    access_token: str,
    ig_id_for_api_call: str,
    rival_usernames_to_process: list[str],
    max_posts_per_rival: int,
    posts_per_api_call_first_page: int,
    posts_per_api_call_pagination: int,
    current_extraction_date: str,
    start_rival_username: str | None = None,
    start_rival_media_cursor: str | None = None,
) -> tuple[list[dict], list[dict], dict | None]:
    """
    Fetch business discovery data for a list of rival usernames.
    Returns discovered account records, media records, and continuation info if interrupted.
    """

    def _continuation(
        rival: str, cursor: str | None, reason: str, rate_limit: bool = False
    ) -> dict:
        return {
            "next_rival": rival,
            "next_cursor": cursor,
            "reason": reason,
            "rate_limit_hit": rate_limit,
        }

    discovered_accounts_records: list[dict] = []
    discovered_media_records: list[dict] = []
    url_template = DATA_ENDPOINT
    processing_started = not bool(start_rival_username)

    for rival_uname in rival_usernames_to_process:
        if not processing_started:
            if rival_uname == start_rival_username:
                processing_started = True
            else:
                continue

        rival_media_items: list[dict] = []
        cursor = (
            start_rival_media_cursor if rival_uname == start_rival_username else None
        )

        if cursor is None:
            ig_fields = PARAMS["discovery"]["ig_account_fields_string"]
            media_fields = PARAMS["discovery"]["media_items_fields_string"]
            fields = (
                f"business_discovery.username({rival_uname})"
                f"{{{ig_fields},media.limit({posts_per_api_call_first_page})"
                f"{{{media_fields}}}}}"
            )
            params = {"fields": fields, "access_token": access_token}
            json_page, resp = make_api_call(
                url_template.format(ig_id_for_api_call), params
            )

            if not resp or resp.status_code != 200:
                return (
                    discovered_accounts_records,
                    discovered_media_records,
                    _continuation(
                        rival_uname,
                        None,
                        f"http_{resp.status_code if resp else 'no_resp'}",
                    ),
                )

            if not json_page or "business_discovery" not in json_page:
                return (
                    discovered_accounts_records,
                    discovered_media_records,
                    _continuation(rival_uname, None, "invalid_json"),
                )

            profile = json_page["business_discovery"]
            if not profile or not profile.get("id"):
                return (
                    discovered_accounts_records,
                    discovered_media_records,
                    _continuation(rival_uname, None, "no_profile"),
                )

            acc_rec = {
                k: v
                for k, v in profile.items()
                if k in DISCOVERY_FIELDS_IG_ACCOUNT and v is not None
            }
            acc_rec["id"] = str(profile["id"])
            acc_rec["extraction_date"] = current_extraction_date
            discovered_accounts_records.append(acc_rec)

            rival_media_items.extend(profile.get("media", {}).get("data", []))
            cursor = (
                profile.get("media", {})
                .get("paging", {})
                .get("cursors", {})
                .get("after")
            )

        while cursor and len(rival_media_items) < max_posts_per_rival:
            page_fields = (
                f"business_discovery.username({rival_uname})"
                f"{{media.after({cursor}).limit({posts_per_api_call_pagination})"
                f"{{{PAGINATED_MEDIA_FIELDS_STRING}}}}}"
            )
            params = {"fields": page_fields, "access_token": access_token}
            json_page, resp = make_api_call(
                url_template.format(ig_id_for_api_call), params
            )

            if resp and resp.status_code == 403:
                if (
                    resp.json().get("error", {}).get("code")
                    == API_RATE_LIMIT_ERROR_CODE
                ):
                    return (
                        discovered_accounts_records,
                        discovered_media_records,
                        _continuation(rival_uname, cursor, "rate_limit", True),
                    )
                return (
                    discovered_accounts_records,
                    discovered_media_records,
                    _continuation(rival_uname, cursor, "rate_limit", True),
                )

            if not resp or resp.status_code != 200:
                is_rl = resp is not None and resp.status_code == 403
                return (
                    discovered_accounts_records,
                    discovered_media_records,
                    _continuation(
                        rival_uname,
                        None,
                        f"http_{resp.status_code if resp else 'no_resp'}",
                        is_rl,
                    ),
                )

            if not json_page or "business_discovery" not in json_page:
                return (
                    discovered_accounts_records,
                    discovered_media_records,
                    _continuation(rival_uname, cursor, "invalid_json"),
                )

            new_media = json_page["business_discovery"].get("media", {}).get("data", [])
            if not new_media:
                break

            rival_media_items.extend(new_media)
            cursor = (
                json_page["business_discovery"]
                .get("media", {})
                .get("paging", {})
                .get("cursors", {})
                .get("after")
            )

        for item in rival_media_items[:max_posts_per_rival]:
            media_rec = {
                k: item[k]
                for k in DISCOVERY_FIELDS_MEDIA_ITEMS
                if k in item and item[k] is not None
            }
            if media_rec.get("media_type") != "VIDEO":
                media_rec.pop("thumbnail_url", None)
            media_rec["id"] = str(item["id"])
            media_rec["username"] = rival_uname
            media_rec["extraction_date"] = current_extraction_date
            discovered_media_records.append(media_rec)

    return discovered_accounts_records, discovered_media_records, None


def upload_records_to_gcs(
    config: dict, records: list[dict], filename_stem: str
) -> str | None:
    """
    Upload a list of JSON records to Google Cloud Storage as a NDJSON file.
    Returns the GCS URI if successful.
    """
    bucket_name = config.get("bucket_name")
    if not bucket_name:
        log.error("Nome do bucket GCS não fornecido para upload.")
        return None
    if not records:
        log.info(f"Nenhum registro para fazer upload para GCS para {filename_stem}.")
        return None
    if not GCS_PROJECT_ID_GLOBAL:
        log.error("GCS_PROJECT_ID_GLOBAL não está configurado. Upload GCS falhará.")
        return None
    ndjson_data = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
    gcs_filename = f"{filename_stem}.json"
    try:
        storage_client = storage.Client(project=GCS_PROJECT_ID_GLOBAL)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(gcs_filename)
        blob.upload_from_string(
            ndjson_data, content_type="application/json; charset=utf-8"
        )
        gcs_uri = f"gs://{bucket_name}/{gcs_filename}"
        log.info(f"Dados enviados para GCS: {gcs_uri}")
        return gcs_uri
    except GoogleCloudError as e:
        log.error(
            f"Falha Google Cloud ao fazer upload de {gcs_filename} para GCS bucket '{bucket_name}': {e}"
        )
        raise
    except Exception as e:
        log.error(
            f"Exceção genérica ao fazer upload de {gcs_filename} para GCS bucket '{bucket_name}': {e}"
        )
        raise


def load_gcs_to_bq_staging_and_merge(
    config: dict,
    gcs_uri: str,
    staging_table_name: str,
    final_table_name: str,
    schema: list[bigquery.SchemaField],
    merge_key: str,
    is_daily_table: bool = False,
    extraction_date_field: str = "extraction_date",
):
    """
    Load data from GCS into a BigQuery staging table and merge into the final table.
    """
    project_id, dataset_id = config.get("project_id"), config.get("dataset_id")
    if not all(
        [
            project_id,
            dataset_id,
            gcs_uri,
            staging_table_name,
            final_table_name,
            schema,
            merge_key,
        ]
    ):
        log.error("Parâmetros ausentes para BigQuery staging e merge. Pulando.")
        return
    bigquery_client = bigquery.Client(project=project_id)
    staging_table_ref_str = f"{project_id}.{dataset_id}.{staging_table_name}"
    final_table_ref_str = f"{project_id}.{dataset_id}.{final_table_name}"
    staging_table_id_fq = f"`{staging_table_ref_str}`"
    final_table_id_fq = f"`{final_table_ref_str}`"
    query_job = None
    try:
        job_config_staging = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        )
        load_job = bigquery_client.load_table_from_uri(
            gcs_uri, staging_table_ref_str, job_config=job_config_staging
        )
        log.info(
            f"Iniciando job de carregamento BQ Staging: {load_job.job_id} para {staging_table_id_fq}"
        )
        load_job.result()
        if load_job.errors:
            log.error(
                f"Erro no job de carregamento BQ Staging (job.errors): {load_job.errors}"
            )
            if load_job.error_result:
                log.error(f"Load job error_result: {load_job.error_result}")
            raise bigquery.errors.BigQueryError(
                f"Erro no job de carregamento BQ Staging: {load_job.errors}"
            )
        log.info(f"Dados carregados com sucesso para Staging BQ: {staging_table_id_fq}")
        try:
            bigquery_client.get_table(final_table_ref_str)
            log.info(f"Tabela final {final_table_name} já existe.")
        except NotFound:
            log.info(f"Tabela final {final_table_name} não encontrada. Criando...")
            table_obj_final = bigquery.Table(final_table_ref_str, schema=schema)
            bigquery_client.create_table(table_obj_final)
            log.info(f"Tabela final {final_table_name} criada.")
        all_columns = [f.name for f in schema]
        insert_cols_list = ", ".join([f"`{col}`" for col in all_columns])
        source_cols_list = ", ".join([f"Source.`{col}`" for col in all_columns])
        if extraction_date_field in all_columns:
            qualify_clause = f"QUALIFY ROW_NUMBER() OVER (PARTITION BY `{merge_key}` ORDER BY `{extraction_date_field}` DESC) = 1"
        else:
            qualify_clause = (
                f"QUALIFY ROW_NUMBER() OVER (PARTITION BY `{merge_key}`) = 1"
            )
        merge_sql = f"""
        MERGE {final_table_id_fq} AS Target
        USING (SELECT * FROM {staging_table_id_fq} {qualify_clause}) AS Source
        ON Target.`{merge_key}` = Source.`{merge_key}`
        WHEN MATCHED THEN UPDATE SET
            Target.`username` = Source.`username`,
            Target.`id` = Source.`id`
        WHEN NOT MATCHED BY TARGET THEN INSERT ({insert_cols_list}) VALUES ({source_cols_list})
        """
        log.info(
            f"Executando MERGE para {final_table_id_fq}. SQL (início): {merge_sql[:500]}..."
        )
        query_job = bigquery_client.query(merge_sql)
        log.info(
            f"Job de MERGE BQ iniciado: {query_job.job_id} para {final_table_id_fq}"
        )
        query_job.result()
        if query_job.errors:
            log.error(f"Erro no job de MERGE BQ (job.errors): {query_job.errors}")
            if query_job.error_result:
                log.error(f"Merge job error_result: {query_job.error_result}")
            full_sql_for_log = (
                f"\nSQL Completo:\n{merge_sql}"
                if len(merge_sql) < 4000
                else f"\nSQL (início):\n{merge_sql[:1000]}..."
            )
            raise bigquery.errors.BigQueryError(
                f"Erro no job de MERGE BQ: {query_job.errors}{full_sql_for_log}"
            )
        log.info(
            f"MERGE concluído para {final_table_id_fq}. Rows afetadas: {query_job.num_dml_affected_rows if query_job.num_dml_affected_rows is not None else 'N/A (checar console)'}"
        )
    except GoogleCloudError as e:
        log_message = f"Exceção Google Cloud durante staging/merge BQ para {final_table_name} (staging: {staging_table_name}): {e}"
        if query_job:
            log_message += f" | Job ID: {query_job.job_id}"
            if query_job.errors:
                log_message += f" | Job Errors: {query_job.errors}"
            if query_job.error_result:
                log_message += f" | Job Error Result: {query_job.error_result}"
        log.error(log_message, exc_info=True)
        raise
    except Exception as e:
        log_message = f"Exceção genérica durante staging/merge BQ para {final_table_name} (staging: {staging_table_name}): {e}"
        if query_job:
            log_message += f" | Job ID: {query_job.job_id}"
        log.error(log_message, exc_info=True)
        raise


def schedule_continuation_task(
    original_request_payload: dict,
    project_id: str,
    task_queue_location: str,
    task_queue_name: str,
    cloud_function_url: str,
    next_rival_username_to_process: str,
    media_cursor_for_next_rival: str | None,
    delay_seconds: int,
    service_account_email: str,
    max_continuation_attempts: int = 5,
):
    """
    Schedule a Cloud Tasks HTTP task to continue the business discovery pipeline.
    """
    client = tasks_v2.CloudTasksClient()
    parent_queue_path = client.queue_path(
        project_id, task_queue_location, task_queue_name
    )
    new_payload = original_request_payload.copy()
    new_payload["continuation_state"] = {
        "start_rival_username": next_rival_username_to_process,
        "media_cursor": media_cursor_for_next_rival,
    }
    current_attempts = new_payload.get("continuation_attempts", 0) + 1
    if current_attempts > max_continuation_attempts:
        log.error(
            f"Máximo de {max_continuation_attempts} tentativas de continuação atingido. Não agendando mais para {next_rival_username_to_process}."
        )
        return False
    new_payload["continuation_attempts"] = current_attempts
    task_payload_json = json.dumps(new_payload)
    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": cloud_function_url,
            "headers": {"Content-Type": "application/json"},
            "body": task_payload_json.encode("utf-8"),
            "oidc_token": {"service_account_email": service_account_email},
        }
    }
    if delay_seconds > 0:
        schedule_dt = datetime.utcnow() + timedelta(seconds=delay_seconds)
        timestamp = timestamp_pb2.Timestamp()
        timestamp.FromDatetime(schedule_dt)
        task["schedule_time"] = timestamp
    log.info(
        f"Tentando agendar tarefa de continuação para {next_rival_username_to_process} (tentativa {current_attempts}). Próxima execução em aprox. {delay_seconds // 60} minutos."
    )
    try:
        response = client.create_task(parent=parent_queue_path, task=task)
        log.info(
            f"Tarefa de continuação criada: {response.name} para rival {next_rival_username_to_process} (cursor: {media_cursor_for_next_rival})"
        )
        return True
    except GoogleCloudError as e:
        log.error(
            f"Erro Google Cloud ao criar tarefa de continuação no Cloud Tasks para {next_rival_username_to_process}: {e}",
            exc_info=True,
        )
    except Exception as e:
        log.error(
            f"Exceção genérica ao criar tarefa de continuação no Cloud Tasks para {next_rival_username_to_process}: {e}",
            exc_info=True,
        )
    return False


def ig_business_discovery_pipeline(
    api_account_details: dict,
    config: dict,
    current_extraction_date_str_ymd: str,
    current_extraction_date_suffix_ymd: str,
    start_rival_username: str | None,
    start_media_cursor: str | None,
) -> tuple[bool, bool]:
    """
    Execute the Instagram business discovery pipeline: fetch data, upload to GCS, load to BigQuery,
    and handle continuation via Cloud Tasks if rate limit is hit.
    Returns (task_scheduled, error_occurred).
    """
    global GCS_PROJECT_ID_GLOBAL
    GCS_PROJECT_ID_GLOBAL = config.get("project_id")
    if not GCS_PROJECT_ID_GLOBAL:
        log.error(
            "PROJECT_ID (para dados GCS/BQ) não configurado. Operações GCS/BQ falharão."
        )
        return False, True
    all_config_rivals = config.get("usernames", [])
    if not all_config_rivals:
        log.warning(
            "Nenhum username rival fornecido. Pipeline Business Discovery encerrado."
        )
        return False, False
    token_for_api_call = api_account_details["token"]
    ig_id_for_api_call = api_account_details["ig_id"]
    log.info(f"Iniciando Business Discovery com conta IG ID: {ig_id_for_api_call}.")
    if start_rival_username:
        log.info(
            f"Continuando de: rival '{start_rival_username}', cursor: '{start_media_cursor if start_media_cursor else 'N/A'}'."
        )
    discovered_accounts_data, discovered_media_data, continuation_info = [], [], None
    try:
        (
            discovered_accounts_data,
            discovered_media_data,
            continuation_info,
        ) = get_business_discovery_data(
            token_for_api_call,
            ig_id_for_api_call,
            all_config_rivals,
            config.get("discovery_max_posts_per_rival", 200),
            config.get("discovery_posts_first_page", 100),
            config.get("discovery_posts_per_pagination_call", 100),
            current_extraction_date_str_ymd,
            start_rival_username=start_rival_username,
            start_rival_media_cursor=start_media_cursor,
        )
        if discovered_accounts_data:
            gcs_filename_accounts_staging = f"{BQ_DISCOVERED_ACCOUNTS_STAGING_TABLE_BASENAME}_{current_extraction_date_suffix_ymd}"
            uri_acc_staging = upload_records_to_gcs(
                config, discovered_accounts_data, gcs_filename_accounts_staging
            )
            if uri_acc_staging and config.get("dataset_id") and GCS_PROJECT_ID_GLOBAL:
                final_accounts_table_name = f"{BQ_DISCOVERED_ACCOUNTS_TABLE_BASENAME}_{current_extraction_date_suffix_ymd}"
                staging_accounts_table_name = f"{BQ_DISCOVERED_ACCOUNTS_STAGING_TABLE_BASENAME}_{current_extraction_date_suffix_ymd}"
                load_gcs_to_bq_staging_and_merge(
                    config,
                    uri_acc_staging,
                    staging_table_name=staging_accounts_table_name,
                    final_table_name=final_accounts_table_name,
                    schema=SCHEMA_DISCOVERED_ACCOUNTS,
                    merge_key="id",
                    is_daily_table=True,
                    extraction_date_field="extraction_date",
                )
        else:
            log.info("Nenhum dado novo de conta descoberta para salvar nesta execução.")
        if discovered_media_data:
            timestamp_suffix = int(time.time())
            gcs_filename_media_staging = f"{BQ_DISCOVERED_MEDIA_STAGING_TABLE_NAME}_{current_extraction_date_suffix_ymd}_{timestamp_suffix}"
            uri_media_staging = upload_records_to_gcs(
                config, discovered_media_data, gcs_filename_media_staging
            )
            if uri_media_staging and config.get("dataset_id") and GCS_PROJECT_ID_GLOBAL:
                load_gcs_to_bq_staging_and_merge(
                    config,
                    uri_media_staging,
                    staging_table_name=BQ_DISCOVERED_MEDIA_STAGING_TABLE_NAME,
                    final_table_name=BQ_DISCOVERED_MEDIA_TABLE_NAME,
                    schema=SCHEMA_DISCOVERED_MEDIA,
                    merge_key="id",
                    extraction_date_field="extraction_date",
                )
        else:
            log.info("Nenhum dado novo de mídia descoberta para salvar nesta execução.")
    except Exception as e:
        log.error(
            f"Erro durante coleta ou salvamento de dados no pipeline: {e}",
            exc_info=True,
        )
        return False, True
    if continuation_info:
        next_rival = continuation_info["next_rival"]
        next_cursor = continuation_info["next_cursor"]
        log.warning(
            f"Pipeline interrompido (motivo: {continuation_info['reason']}). Agendando continuação para rival {next_rival} em 1h."
        )
        cloud_tasks_config = config.get("cloud_tasks_config", {})
        tasks_project_id = cloud_tasks_config.get("project_id") or config.get(
            "project_id"
        )
        if not tasks_project_id:
            log.error(
                "ID do projeto para Cloud Tasks não encontrado em 'cloud_tasks_config.project_id' nem em 'project_id'. Não é possível agendar."
            )
            return False, True
        queue_location = cloud_tasks_config.get("queue_location", "us-central1")
        queue_name = cloud_tasks_config.get(
            "queue_name", "instagram-business-discovery"
        )
        function_url = cloud_tasks_config.get(
            "cloud_function_url",
            "https://instagram-business-discovery-52575712744.us-central1.run.app",
        )
        sa_email = cloud_tasks_config.get("service_account_email")
        delay_secs = cloud_tasks_config.get("delay_seconds", 3600)
        max_attempts = cloud_tasks_config.get("max_continuation_attempts", 5)
        if not sa_email:
            log.error(
                "Email da conta de serviço para Cloud Tasks ('service_account_email') não fornecido na configuração. Não é possível agendar tarefa."
            )
            return False, True
        scheduled_successfully = schedule_continuation_task(
            original_request_payload=config,
            project_id=tasks_project_id,
            task_queue_location=queue_location,
            task_queue_name=queue_name,
            cloud_function_url=function_url,
            next_rival_username_to_process=next_rival,
            media_cursor_for_next_rival=next_cursor,
            delay_seconds=delay_secs,
            service_account_email=sa_email,
            max_continuation_attempts=max_attempts,
        )
        return scheduled_successfully, not scheduled_successfully
    log.info(
        "Pipeline Business Discovery concluído para os rivais configurados (ou porção atual)."
    )
    return False, False


def send_slack_notification(
    message_text: str,
    status_code_for_slack: int,
    webhook_url: str | None = None,
    additional_context: dict | None = None,
):
    """
    Send a notification message to Slack via Incoming Webhook.
    """
    final_webhook_url = webhook_url or os.environ.get("SLACK_WEBHOOK_URL")
    if not final_webhook_url:
        log.warning("URL Slack webhook não configurada. Notificação não enviada.")
        return
    emoji = (
        ":arrows_counterclockwise:"
        if status_code_for_slack == 202
        else (
            ":white_check_mark:"
            if str(status_code_for_slack).startswith("2")
            else ":x:"
        )
    )
    main_notification_message = message_text.splitlines()[0]
    full_text_message = f"{emoji} Resultado Execução Função Instagram\n"
    full_text_message += f"*Status:* {status_code_for_slack}\n"
    full_text_message += f"*Mensagem:* {message_text}\n"
    if additional_context:
        context_to_send = additional_context.copy()
        if "accounts" in context_to_send:
            if isinstance(context_to_send["accounts"], list):
                for i in range(len(context_to_send["accounts"])):
                    if isinstance(context_to_send["accounts"][i], dict):
                        context_to_send["accounts"][i].pop("access_token", None)
        context_str = json.dumps(context_to_send, indent=2, ensure_ascii=False)
        if len(context_str) > 1500:
            context_str = context_str[:1500] + "... (truncado)"
        full_text_message += (
            f"\n*Contexto Adicional (Payload da Requisição):*\n```\n{context_str}\n```"
        )
    slack_payload = {"text": full_text_message}
    try:
        resp = requests.post(
            final_webhook_url,
            data=json.dumps(slack_payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        log.info(f"Notificação Slack status HTTP: {resp.status_code}")
    except Exception as e:
        log.error(f"Erro envio Slack: {e}")


def get_stories(access_token: str, ig_id: str):
    """
    Placeholder for collecting Instagram Stories metadata.
    """
    log.info(f"Coletando stories para {ig_id} (placeholder).")
    return []


def upload_stories(config: dict, username: str, records: list[dict]):
    """
    Placeholder for uploading story records to storage or database.
    """
    log.info(f"Upload de stories para {username} (placeholder).")


def main(request):
    """
    Cloud Function entry point that orchestrates token retrieval, IG ID lookup,
    pipeline execution, and Slack notification.
    """
    start_time = time.time()
    config_log()
    active_slack_webhook_url = None
    config_payload = {}
    final_slack_message_text = "Execução iniciada, mas encontrou problema inicial."
    final_http_status_code = 500
    first_configured_ig_username_for_log = "N/A"
    now_utc = datetime.utcnow()
    current_extraction_date_yyyy_mm_dd_str = now_utc.strftime("%Y-%m-%d")
    current_extraction_date_suffix_yyyymmdd = now_utc.strftime("%Y%m%d")
    try:
        request_json = request.get_json(silent=True)
        if request_json is None:
            log.warning("Payload da requisição não é JSON válido ou está vazio.")
            config_payload = {}
        else:
            config_payload = request_json
        log.info(
            f"Payload config: {json.dumps(config_payload, indent=2, ensure_ascii=False)}"
        )
        active_slack_webhook_url = config_payload.get("slack_webhook_url")
        config_payload.setdefault("cloud_tasks_config", {})
        config_payload["cloud_tasks_config"].setdefault(
            "project_id", config_payload.get("project_id")
        )
        config_payload["cloud_tasks_config"].setdefault("queue_location", "us-central1")
        config_payload["cloud_tasks_config"].setdefault(
            "queue_name", "instagram-business-discovery"
        )
        config_payload["cloud_tasks_config"].setdefault(
            "cloud_function_url",
            os.environ.get(
                "FUNCTION_TARGET",
                "https://instagram-business-discovery-52575712744.us-central1.run.app",
            ),
        )
    except Exception as e:
        log.error(f"Erro ao obter ou processar JSON da requisição: {e}", exc_info=True)
        final_slack_message_text = (
            f"Erro crítico ao processar payload da requisição: {e}"
        )
        final_http_status_code = 400
        execution_time_seconds = round(time.time() - start_time, 1)
        message_with_time = (
            f"{final_slack_message_text}\nTempo de execução: {execution_time_seconds}s"
        )
        send_slack_notification(
            message_with_time,
            final_http_status_code,
            config_payload.get("slack_webhook_url"),
            config_payload if final_http_status_code >= 400 else None,
        )
        return (final_slack_message_text.split("\n")[0], final_http_status_code)
    try:
        accounts_config_list = config_payload.get("accounts")
        if not isinstance(accounts_config_list, list) or not accounts_config_list:
            raise ValueError(
                "Configuração 'accounts' deve ser uma lista não vazia. Forneça pelo menos uma conta."
            )
        first_account_config = accounts_config_list[0]
        fb_page_id = first_account_config.get("page_id")
        bm_flag = first_account_config.get("bm_sorocaba", False)
        friendly_name = first_account_config.get(
            "friendly_name", fb_page_id or "Conta Padrão"
        )
        if not fb_page_id:
            raise ValueError(
                f"Primeira conta em 'accounts' não possui 'page_id': {first_account_config}"
            )
        access_token = get_token(bm_flag)
        if not access_token:
            raise ValueError(
                f"Falha ao obter token para a conta '{friendly_name}' (Page ID: {fb_page_id})."
            )
        _ig_id_temp, _ig_uname_temp = get_ig_id(access_token, fb_page_id)
        if _ig_id_temp == "RATE_LIMIT_HIT":
            final_slack_message_text = f"ERRO: Rate limit atingido ao obter IG ID para {friendly_name}. O processamento não continuará."
            final_http_status_code = 429
            log.error(final_slack_message_text)
            raise Exception("Rate limit inicial em get_ig_id, interrompendo execução.")
        if not _ig_id_temp or not _ig_uname_temp:
            raise ValueError(
                f"Falha ao obter IG ID/Username para '{friendly_name}' (Page ID '{fb_page_id}')."
            )
        first_configured_ig_username_for_log = _ig_uname_temp
        active_api_account_details = {
            "token": access_token,
            "ig_id": _ig_id_temp,
            "username": _ig_uname_temp,
            "fb_page_id": fb_page_id,
            "friendly_name": friendly_name,
        }
        log.info(
            f"Usando conta IG '{friendly_name}' (Username: {_ig_uname_temp}, ID: {_ig_id_temp}) para chamadas API."
        )
        continuation_state = config_payload.get("continuation_state")
        start_rival_username_from_payload = None
        start_media_cursor_from_payload = None
        if continuation_state:
            start_rival_username_from_payload = continuation_state.get(
                "start_rival_username"
            )
            start_media_cursor_from_payload = continuation_state.get("media_cursor")
            log.info(
                f"Esta é uma execução de continuação. Iniciando de: rival '{start_rival_username_from_payload}', cursor '{start_media_cursor_from_payload}'."
            )
            final_slack_message_text = f"Pipeline BD (continuação) para '{first_configured_ig_username_for_log}' iniciado."
        else:
            final_slack_message_text = f"Pipeline BD (nova execução) para '{first_configured_ig_username_for_log}' iniciado."
        if config_payload.get("run_focused_discovery_pipeline", False):
            log.info("Iniciando pipeline Business Discovery...")
            task_scheduled, error_in_pipeline = ig_business_discovery_pipeline(
                active_api_account_details,
                config_payload,
                current_extraction_date_yyyy_mm_dd_str,
                current_extraction_date_suffix_yyyymmdd,
                start_rival_username_from_payload,
                start_media_cursor_from_payload,
            )
            if error_in_pipeline:
                final_slack_message_text = f"ERRO no pipeline BD para '{first_configured_ig_username_for_log}'. Verifique os logs para detalhes."
                final_http_status_code = 500
            elif task_scheduled:
                final_slack_message_text = f"Pipeline BD para '{first_configured_ig_username_for_log}' parcialmente concluído. Rate limit atingido. Continuação agendada via Cloud Tasks."
                final_http_status_code = 202
            else:
                final_slack_message_text = f"Pipeline BD para '{first_configured_ig_username_for_log}' concluído com sucesso."
                final_http_status_code = 200
        elif config_payload.get("stories_metadata", False):
            log.info("Iniciando coleta de metadados de Stories...")
            stories_data = get_stories(
                active_api_account_details["token"], active_api_account_details["ig_id"]
            )
            if (
                stories_data
                and config_payload.get("project_id")
                and config_payload.get("dataset_id")
            ):
                upload_stories(
                    config_payload, active_api_account_details["username"], stories_data
                )
            final_slack_message_text = f"Coleta Stories para '{active_api_account_details['username']}' concluída (Placeholder)."
            final_http_status_code = 200
        else:
            final_slack_message_text = f"Nenhum pipeline principal selecionado para '{first_configured_ig_username_for_log}'. Verifique config."
            final_http_status_code = 200
            log.warning(final_slack_message_text)
    except ValueError as ve:
        log.error(f"Erro de valor/configuração: {ve}")
        final_slack_message_text = f"ERRO DE CONFIGURAÇÃO (conta: '{first_configured_ig_username_for_log}'): {str(ve)}"
        final_http_status_code = 400
    except Exception as e:
        if "Rate limit inicial em get_ig_id" in str(e):
            pass
        else:
            log.error(
                f"Erro principal não tratado no pipeline (contexto: '{first_configured_ig_username_for_log}'): {e}",
                exc_info=True,
            )
            if final_http_status_code != 202 and final_http_status_code < 400:
                final_slack_message_text = f"ERRO GERAL no processamento (contexto: '{first_configured_ig_username_for_log}'): {str(e)}"
                final_http_status_code = 500
    finally:
        execution_time_seconds = round(time.time() - start_time, 1)
        message_with_time = (
            f"{final_slack_message_text}\nTempo de execução: {execution_time_seconds}s"
        )
        log.info(
            f"Resultado final: Status {final_http_status_code}, Mensagem: {final_slack_message_text.splitlines()[0]}"
        )
        send_slack_notification(
            message_text=message_with_time,
            status_code_for_slack=final_http_status_code,
            webhook_url=active_slack_webhook_url,
            additional_context=config_payload
            if final_http_status_code >= 400
            else None,
        )
    http_response_text = final_slack_message_text.split("\n")[0]
    return (http_response_text, final_http_status_code)
