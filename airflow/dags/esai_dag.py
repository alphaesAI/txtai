from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import json
from pathlib import Path

from esai_flow.src.connector.interfaces import RDBMSDatabase
from esai_flow.src.schemas.database.config import DatabaseConfig
from esai_flow.src.extractor.dbextractor import DBExtractor
from esai_flow.src.transmission import DataTransmitter
from esai_flow.src.elasticsearch.connection import ElasticsearchConfig, ElasticsearchClientFactory
from esai_flow.src.elasticsearch.loader import ElasticsearchLoader, BulkIngestion

def setup_database():
    
    config = DatabaseConfig(
        database_url="postgresql://rag_user:rag_password@localhost:5432/paper_curator",
        echo_sql=True,
    )

    db = RDBMSDatabase(config)
    db.startup()
    print("Database connection successful")
    return db

def extract_data(**context):
    db = setup_database()
    extractor = DBExtractor(db)
    df = extractor.extract_table("students", to_csv=True)
    df.to_csv("students_export_airflow.csv", index=False)
    context['ti'].xcom_push(key='df_path', value="students_export_airflow.csv")
    
def transmit_data(**context):
    import pandas as pd
    df_path = context['ti'].xcom_pull(key='df_path', task_ids="extract_data")
    df = pd.read_csv(df_path)
    json_output_path = Path("students_export_airflow.json")
    transmitter = DataTransmitter(df, save_to_file=True, output_path=json_output_path)
    json_str = transmitter()
    context['ti'].xcom_push(key='json_str', value=json_str)

def ingest_to_elasticsearch(**context):
    json_str = context['ti'].xcom_pull(key='json_str', task_ids='transmit_data')
    es_config = ElasticsearchConfig(
        host="http://localhost:9200",
        username="elastic",
        password="q+nR3Kse*QW5kpoacWn3",
        verify_certs=False,
    )

    loader = ElasticsearchLoader(es_config, index_name="students_index")
    loader.connect()
    loader.index_exists()

    bulk_ingestor = BulkIngestion(loader.client)
    response = bulk_ingestor.ingest_json(json_str, loader.index_name)
    print(response)

default_args = {
    "owner": "logi",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}
    
with DAG(
    dag_id="esai_pipeline_dag",
    default_args=default_args,
    description="DAG for Postgres -> JSON -> Elasticsearch ingestion",
    schedule_interval='*/45 * * * *',
    start_date=datetime(2025, 10, 29),
    catchup=False,
    tags=["esai_flow"],
) as dag:
    
    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        provide_context=True,
    )

    transmit = PythonOperator(
        task_id="transmit_data",
        python_callable=transmit_data,
        provide_context=True,
    )

    ingest = PythonOperator(
        task_id="ingest_elasticsearch",
        python_callable=ingest_to_elasticsearch,
        provide_context=True,
    )

    extract >> transmit >> ingest