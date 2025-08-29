from google.cloud import bigquery

DATASET = "dashboard"
BQ_SERVICE_ACCOUNT_FILE = "/secrets/bigquery/service-account.json"

bq_client = bigquery.Client.from_service_account_json(BQ_SERVICE_ACCOUNT_FILE)


def query(sql: str):
    return bq_client.query(sql).result()


def exec(sql: str):
    bq_client.query(sql).result()


def load_json(table: str, rows: list[dict]):
    """
    Carrega dados no BigQuery via API de streaming.
    table pode ser 'brand', 'social_post', '_tmp_brand', etc.
    """
    table_id = f"{DATASET}.{table}"
    errors = bq_client.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(f"Erro ao inserir em {table_id}: {errors}")


def delete_table(table: str):
    """Dropa uma tabela (usado p/ staging)."""
    table_id = f"{DATASET}.{table}"
    bq_client.delete_table(table_id, not_found_ok=True)
