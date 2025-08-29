from google.cloud import bigquery

BQ_SERVICE_ACCOUNT_FILE = "/secrets/bigquery/service-account.json"
DATASET = "dashboard"

bq_client = bigquery.Client.from_service_account_json(BQ_SERVICE_ACCOUNT_FILE)


def exec(sql: str):
    """Executa query SQL no BigQuery"""
    bq_client.query(sql).result()


def query(sql: str):
    """Executa query e retorna lista de dicts"""
    job = bq_client.query(sql)
    return [dict(row) for row in job.result()]


def load_json(table: str, rows: list[dict]):
    """Carrega lista de dicionários para uma tabela BigQuery usando load_table_from_json (sem streaming buffer)."""
    table_id = f"{bq_client.project}.{DATASET}.{table}"
    job = bq_client.load_table_from_json(
        rows,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"  # sempre adiciona
        ),
    )
    job.result()  # espera o job terminar

    return {"inserted": len(rows)}


def delete_table(table: str):
    """Remove tabela temporária"""
    table_id = f"{bq_client.project}.{DATASET}.{table}"
    bq_client.delete_table(table_id, not_found_ok=True)
