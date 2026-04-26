import os
import json
from google.cloud import secretmanager
from google.cloud import bigquery
import logging as log

SCHEMA_FILENAME = "schema.json"

def get_secret(secret_id: str, version_id: str = "latest") -> str:
    """Access the payload for the given secret version if one exists."""
    project_id = os.environ.get("PROJECT_ID", "kabum-gcp") # Default or get from env
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

HOST = "https://graph.facebook.com"
API_VERSION = "/v22.0/"

IG_ID_ENDPOINT = (
    HOST + API_VERSION + "{}?fields=instagram_business_account.fields(id,username)"
)
DATA_ENDPOINT = (
    HOST + API_VERSION + "{}"
)  # Endpoint base para chamadas, e.g., Business Discovery
USER_MEDIA_ENDPOINT = (
    HOST + API_VERSION + "{}/media"
)  # Para buscar mídias de um usuário específico

# Código de erro da API para "Application request limit reached"
API_RATE_LIMIT_ERROR_CODE = 4

# Campos da conta IG principal (se você buscar dados da sua própria conta)
IG_FIELDS = [
    "biography",
    "id",
    "followers_count",
    "follows_count",
    "media_count",
    "name",
    "profile_picture_url",
    "username",
    "website",
]

# Campos para o perfil do rival no Business Discovery
DISCOVERY_FIELDS_IG_ACCOUNT = [
    "id",
    "name",
    "username",
    "biography",
    "profile_picture_url",
    "followers_count",
    "follows_count",
    "media_count",
]

# Campos para os itens de mídia do rival no Business Discovery
DISCOVERY_FIELDS_MEDIA_ITEMS = [
    "id",
    "permalink",
    "timestamp",
    "caption",
    "media_type",
    "media_product_type",
    "comments_count",
    "like_count",
    "media_url",  # URL direta da mídia (para imagens, se fornecida pela API)
    "thumbnail_url",  # URL da thumbnail (para vídeos, se fornecida pela API)
]

# String de campos para paginação de mídia (usada após a primeira chamada de BD)
PAGINATED_MEDIA_FIELDS_STRING = ",".join(DISCOVERY_FIELDS_MEDIA_ITEMS)

# Estrutura de PARAMS para chamadas à API
PARAMS = {
    "discovery": {
        # Template para os campos do Business Discovery.
        # 1º {}: username do rival a ser descoberto.
        # 2º {}: string concatenada dos campos do perfil do rival (DISCOVERY_FIELDS_IG_ACCOUNT).
        # {media_limit}: Note que o template original tinha media.limit(100) fixo.
        # A lógica em get_business_discovery_data agora constrói essa parte dinamicamente.
        # Para referência, se o template fosse usado com .format e um placeholder {media_limit}:
        # "fields_template_dynamic": "business_discovery.username({}){{{},media.limit({media_limit}){{{}}}}}",
        "ig_account_fields_string": ",".join(DISCOVERY_FIELDS_IG_ACCOUNT),
        "media_items_fields_string": ",".join(DISCOVERY_FIELDS_MEDIA_ITEMS),
        # 'access_token' e 'base_ig_user_id_for_discovery_call' serão adicionados em main.py
    },
    "ig_fields": {  # Se você buscar dados da sua própria conta IG
        "fields": ",".join(IG_FIELDS)
        # 'access_token' seria adicionado aqui também
    },
    # Outros PARAMS (ex: para insights) podem ser adicionados conforme necessário
}

# --- SCHEMAS BIGQUERY PARA BUSINESS DISCOVERY ---
SCHEMA_DISCOVERED_ACCOUNTS = [
    bigquery.SchemaField(
        "id", "STRING", mode="REQUIRED"
    ),  # ID da conta descoberta (da API)
    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("username", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("biography", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("profile_picture_url", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("followers_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("follows_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("media_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField(
        "extraction_date", "DATE", mode="NULLABLE"
    ),  # YYYY-MM-DD, usada para partição/merge
]

SCHEMA_DISCOVERED_MEDIA = [
    bigquery.SchemaField(
        "id", "STRING", mode="REQUIRED"
    ),  # Chave para MERGE (media_id)
    bigquery.SchemaField("username", "STRING", mode="NULLABLE"),  # username do rival
    bigquery.SchemaField("permalink", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "timestamp", "STRING", mode="NULLABLE"
    ),  # Armazenado como STRING (API retorna ISO 8601)
    bigquery.SchemaField("caption", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "media_type", "STRING", mode="NULLABLE"
    ),  # IMAGE, VIDEO, CAROUSEL_ALBUM
    bigquery.SchemaField(
        "media_product_type", "STRING", mode="NULLABLE"
    ),  # e.g. FEED, STORY, REELS
    bigquery.SchemaField("comments_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("like_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("media_url", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("thumbnail_url", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("extraction_date", "DATE", mode="NULLABLE"),  # YYYY-MM-DD
]

# --- NOMES DAS TABELAS BIGQUERY ---
# A data (YYYYMMDD) será anexada ao nome base da tabela de contas final e staging.
BQ_DISCOVERED_ACCOUNTS_TABLE_BASENAME = "instagram_discovered_accounts_info"
BQ_DISCOVERED_ACCOUNTS_STAGING_TABLE_BASENAME = (
    "instagram_discovered_accounts_info_staging"
)

# A tabela de mídia não é sharded por data no nome, usa MERGE com uma staging única.
BQ_DISCOVERED_MEDIA_TABLE_NAME = "instagram_discovered_media_info"
BQ_DISCOVERED_MEDIA_STAGING_TABLE_NAME = "instagram_discovered_media_info_staging"  # Pode ser um nome único ou com data se preferir


def get_original_schemas_from_file(filename: str = SCHEMA_FILENAME) -> dict:
    """Carrega schemas de um arquivo JSON (ex: para fluxos legados como stories)."""
    try:
        with open(filename, "r") as schema_file:
            schemas = json.load(schema_file)
        return schemas
    except FileNotFoundError:
        logger = log.getLogger()
        if logger.hasHandlers() and logger.getEffectiveLevel() <= log.ERROR:
            log.error(f"Arquivo de schema original '{filename}' não encontrado.")
        else:
            print(f"ERRO: Arquivo de schema original '{filename}' não encontrado.")
        return {}
    except json.JSONDecodeError:
        logger = log.getLogger()
        if logger.hasHandlers() and logger.getEffectiveLevel() <= log.ERROR:
            log.error(
                f"Erro ao decodificar JSON do arquivo de schema original '{filename}'."
            )
        else:
            print(
                f"ERRO: Erro ao decodificar JSON do arquivo de schema original '{filename}'."
            )
        return {}
