from etl_pipeline.dags.tasks import load_config
from etl_pipeline.dags.tasks import create_postgres_connector, create_elasticsearch_connector, extract_table_data, transform_data
from etl_pipeline.dags.tasks import load_to_elasticsearch, cleanup_connections

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


# Global config
CONFIG = load_config()

#define default arguments
default_args = {
    'owner': CONFIG['airflow']['default_args']['owner'],
    'depends_on_past': False,
    'email_on_failure': CONFIG['airflow']['default_args'].get('email_on_failure', False),
    'email_on_retry': CONFIG['airflow']['default_args'].get('email_on_retry', False),
    'retries': CONFIG['airflow']['default_args'].get('retries', 2),
    'retry_delay': timedelta(minutes=CONFIG['airflow']['default_args'].get('retry_delay_minutes', 5)),
}

#create DAG
with DAG(
    dag_id=CONFIG['airflow']['dag_id'],
    default_args=default_args,
    description='ETL pipeline from postgresql to elasticsearch',
    schedule_interval=CONFIG['airflow']['schedule_interval'],
    start_date=datetime.fromisoformat(CONFIG['airflow']['start_date']),
    catchup=CONFIG['airflow'].get('catchup', False),
    max_active_runs=CONFIG['airflow'].get('max_active_runs', 1),
    tags=CONFIG['airflow'].get('tags', []),
) as dag:
    
    #create connectors
    create_postgres_conn = PythonOperator(
        task_id='create_postgres_connector',
        python_callable=create_postgres_connector,
        provide_context=True,
    )

    create_es_conn = PythonOperator(
        task_id='create_elasticsearch_connector',
        python_callable=create_elasticsearch_connector,
        provide_context=True,
    )

    #cleanup task
    cleanup = PythonOperator(
        task_id='cleanup_connections',
        python_callable=cleanup_connections,
        provide_context=True,
        trigger_rule='all_done'
    )

    #create tasks for each table
    table_tasks = []
    
    for table_config in CONFIG['extraction']['tables']:
        table_name = table_config['table_name']

        #extract task
        extract_task = PythonOperator(
            task_id=f'extract_{table_name}',
            python_callable=extract_table_data,
            op_kwargs={'table_config_dict': table_config},
            provide_context=True,
        )

        #transform task
        transform_task = PythonOperator(
            task_id=f'transform_{table_name}',
            python_callable=transform_data,
            op_kwargs={'extraction_result': "{{ task_instance.xcom_pull(task_ids='extract_" + table_name + "') }}"},
            provide_context=True,
        )

        #load task
        load_task = PythonOperator(
            task_id=f'load_{table_name}',
            python_callable=load_to_elasticsearch,
            op_kwargs={'transformation_result': "{{ task_instance.xcom_pull(task_ids='transform_" + table_name + "') }}"},
            provide_context=True,
        )

        #set dependencies for this table's pipeline
        create_postgres_conn >> extract_task >> transform_task >> load_task >> cleanup
        create_es_conn >> load_task

        table_tasks.append((extract_task, transform_task, load_task))
