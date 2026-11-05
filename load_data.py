import os
import sys
import pandas as pd
import psycopg2
from google.cloud import bigquery

# Konfiguracja środowiska
PG_CONN_STRING = os.environ.get("NEON_DATABASE_URL")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
DATASET_ID = os.environ.get("BIGQUERY_DATASET", "kfd_landing")

# Tryb ładowania: 'full_refresh' lub 'incremental'
LOAD_MODE = os.environ.get("LOAD_MODE", "full_refresh")

# Definicja kolejności tabel wg relacji PK-FK (Krok 17)
TABLES_CONFIG = [
    {"table": "customers", "pk": "id", "inc_col": "updated_at"},
    {"table": "products", "pk": "id", "inc_col": "updated_at"},
    {"table": "orders", "pk": "id", "inc_col": "updated_at"},
    {"table": "order_items", "pk": "id", "inc_col": "updated_at"},
]

def get_max_watermark(client, dataset_id, table_name, inc_col):
    """Pobiera maksymalny znacznik czasu z tabeli w BigQuery dla ładowania incremental"""
    query = f"SELECT MAX({inc_col}) as max_val FROM `{GCP_PROJECT_ID}.{dataset_id}.{table_name}`"
    try:
        query_job = client.query(query)
        results = list(query_job.result())
        return results[0].max_val if results else None
    except Exception:
        return None

def process_table(table_cfg, bq_client, pg_conn):
    table_name = table_cfg["table"]
    inc_col = table_cfg["inc_col"]
    print(f"--- Przetwarzanie tabeli: {table_name} [Tryb: {LOAD_MODE}] ---")

    query = f"SELECT * FROM {table_name}"
    
    if LOAD_MODE == "incremental":
        watermark = get_max_watermark(bq_client, DATASET_ID, table_name, inc_col)
        if watermark:
            print(f"Ostatni watermark w BigQuery dla {table_name}: {watermark}")
            query += f" WHERE {inc_col} > '{watermark}'"
        else:
            print(f"Brak danych w BigQuery dla {table_name}. Wykonuję pełny odczyt.")

    df = pd.read_sql(query, pg_conn)
    print(f"Pobrano {len(df)} wierszy z NeonDB.")

    if df.empty:
        print(f"Brak nowych danych do załadowania dla {table_name}.")
        return

    # Przygotowanie BigQuery load job
    table_ref = f"{GCP_PROJECT_ID}.{DATASET_ID}.{table_name}"
    job_config = bigquery.LoadJobConfig()

    if LOAD_MODE == "full_refresh":
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    else:
        # Przy ładowaniu do warstwy landing doklejamy zmienione/nowe rekordy (APPEND)
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Pomyślnie załadowano dane do BigQuery: {table_ref}")

def main():
    bq_client = bigquery.Client()
    pg_conn = psycopg2.connect(PG_CONN_STRING)

    try:
        for cfg in TABLES_CONFIG:
            process_table(cfg, bq_client, pg_conn)
    finally:
        pg_conn.close()

if __name__ == "__main__":
    main()
