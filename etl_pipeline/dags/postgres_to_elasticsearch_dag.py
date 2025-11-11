# """
# Airflow DAG for extracting data from PostgreSQL, transforming to JSON,
# and loading to Elasticsearch.
# """
# import sys
# from pathlib import Path

# # Add parent directory to path for imports
# sys.path.insert(0, str(Path(__file__).parent.parent))

# from datetime import datetime, timedelta
# from typing import Dict, Any, List
# import yaml
# import logging

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

# from connector import ConnectorFactory, ConnectionManager
# from extractor import ExtractorFactory, TableConfig, StateManager, ExtractionMode
# from transformer import TransformerFactory
# from loader import LoaderFactory


# logger = logging.getLogger(__name__)


# # Load configuration
# def load_config(config_path: str = None) -> Dict[str, Any]:
#     """Load ETL configuration from YAML file."""
#     if config_path is None:
#         config_path = Path(__file__).parent.parent / "config" / "etl_config.yaml"
    
#     with open(config_path, 'r') as f:
#         config = yaml.safe_load(f)
    
#     return config


# # Global config
# CONFIG = load_config()


# def create_postgres_connector(**context) -> str:
#     """
#     Create and register PostgreSQL connector.
    
#     Returns:
#         Connection ID
#     """
#     try:
#         logger.info("Creating PostgreSQL connector")
        
#         # Create connector
#         connector = ConnectorFactory.create_connector(
#             connector_type="postgres",
#             connection_string=CONFIG['postgres']['connection_string'],
#             pool_size=CONFIG['postgres'].get('pool_size', 5),
#             max_overflow=CONFIG['postgres'].get('max_overflow', 10),
#             pool_timeout=CONFIG['postgres'].get('pool_timeout', 30),
#             echo=CONFIG['postgres'].get('echo', False)
#         )
        
#         # Connect
#         connector.connect()
        
#         # Test connection
#         if not connector.test_connection():
#             raise ConnectionError("PostgreSQL connection test failed")
        
#         # Register with connection manager
#         conn_manager = ConnectionManager()
#         connection_id = "postgres_source"
#         conn_manager.register_connection(connection_id, connector)
        
#         logger.info(f"PostgreSQL connector created and registered: {connection_id}")
        
#         # Push connection ID to XCom
#         context['task_instance'].xcom_push(key='postgres_conn_id', value=connection_id)
        
#         return connection_id
        
#     except Exception as e:
#         logger.error(f"Error creating PostgreSQL connector: {e}")
#         raise


# def create_elasticsearch_connector(**context) -> str:
#     """
#     Create and register Elasticsearch connector.
    
#     Returns:
#         Connection ID
#     """
#     try:
#         logger.info("Creating Elasticsearch connector")
        
#         # Create connector
#         connector = ConnectorFactory.create_connector(
#             connector_type="elasticsearch",
#             connection_string=CONFIG['elasticsearch']['connection_string'],
#             use_ssl=CONFIG['elasticsearch'].get('use_ssl', False),
#             verify_certs=CONFIG['elasticsearch'].get('verify_certs', False),
#             timeout=CONFIG['elasticsearch'].get('timeout', 30),
#             max_retries=CONFIG['elasticsearch'].get('max_retries', 3)
#         )
        
#         # Connect
#         connector.connect()
        
#         # Test connection
#         if not connector.test_connection():
#             raise ConnectionError("Elasticsearch connection test failed")
        
#         # Register with connection manager
#         conn_manager = ConnectionManager()
#         connection_id = "elasticsearch_target"
#         conn_manager.register_connection(connection_id, connector)
        
#         logger.info(f"Elasticsearch connector created and registered: {connection_id}")
        
#         # Push connection ID to XCom
#         context['task_instance'].xcom_push(key='es_conn_id', value=connection_id)
        
#         return connection_id
        
#     except Exception as e:
#         logger.error(f"Error creating Elasticsearch connector: {e}")
#         raise


# def extract_table_data(table_config_dict: Dict[str, Any], **context) -> Dict[str, Any]:
#     """
#     Extract data from a PostgreSQL table.
    
#     Args:
#         table_config_dict: Table configuration dictionary
        
#     Returns:
#         Extraction result with data and metadata
#     """
#     try:
#         table_name = table_config_dict['table_name']
#         logger.info(f"Extracting data from table: {table_name}")
        
#         # Get PostgreSQL connector
#         conn_manager = ConnectionManager()
#         postgres_conn_id = context['task_instance'].xcom_pull(
#             task_ids='create_postgres_connector',
#             key='postgres_conn_id'
#         )
#         postgres_connector = conn_manager.get_connection(postgres_conn_id)
        
#         if not postgres_connector:
#             raise ValueError(f"PostgreSQL connector not found: {postgres_conn_id}")
        
#         # Create extractor
#         extractor = ExtractorFactory.create_extractor(
#             extractor_type="postgres",
#             connector=postgres_connector
#         )
        
#         # Load state manager
#         state_file = CONFIG['extraction']['state_file']
#         state_manager = StateManager(state_file)
        
#         # Build table config
#         table_config = TableConfig(**table_config_dict)
        
#         # Update last extracted value from state for incremental loads
#         if table_config.extraction_mode == ExtractionMode.INCREMENTAL_CDC:
#             last_value = state_manager.get_last_extracted_value(
#                 table_name,
#                 table_config.cdc_column
#             )
#             if last_value is not None:
#                 table_config.last_extracted_value = last_value
#                 logger.info(f"Using last CDC value: {last_value}")
        
#         elif table_config.extraction_mode == ExtractionMode.INCREMENTAL_DATE:
#             last_date = state_manager.get_last_extracted_value(
#                 table_name,
#                 table_config.date_column
#             )
#             if last_date is not None:
#                 table_config.start_date = datetime.fromisoformat(last_date)
#                 logger.info(f"Using last date: {last_date}")
        
#         # Extract data
#         df = extractor.extract_with_config(table_config)
        
#         # Update state
#         if not df.empty:
#             if table_config.extraction_mode == ExtractionMode.INCREMENTAL_CDC:
#                 max_value = df[table_config.cdc_column].max()
#                 state_manager.set_last_extracted_value(
#                     table_name,
#                     table_config.cdc_column,
#                     max_value
#                 )
#                 logger.info(f"Updated CDC state: {max_value}")
            
#             elif table_config.extraction_mode == ExtractionMode.INCREMENTAL_DATE:
#                 max_date = df[table_config.date_column].max()
#                 state_manager.set_last_extracted_value(
#                     table_name,
#                     table_config.date_column,
#                     max_date.isoformat()
#                 )
#                 logger.info(f"Updated date state: {max_date}")
        
#         # Convert DataFrame to dict for XCom
#         data_dict = df.to_dict('records')
        
#         result = {
#             'table_name': table_name,
#             'row_count': len(df),
#             'data': data_dict,
#             'columns': list(df.columns)
#         }
        
#         logger.info(f"Extracted {len(df)} rows from {table_name}")
        
#         return result
        
#     except Exception as e:
#         logger.error(f"Error extracting data: {e}")
#         raise


# def transform_data(extraction_result: Dict[str, Any], **context) -> Dict[str, Any]:
#     """
#     Transform extracted data to JSON format.
    
#     Args:
#         extraction_result: Result from extraction task
        
#     Returns:
#         Transformation result
#     """
#     try:
#         table_name = extraction_result['table_name']
#         logger.info(f"Transforming data for table: {table_name}")
        
#         # Convert data back to DataFrame
#         import pandas as pd
#         df = pd.DataFrame(extraction_result['data'])
        
#         if df.empty:
#             logger.warning(f"No data to transform for {table_name}")
#             return {
#                 'table_name': table_name,
#                 'row_count': 0,
#                 'data': []
#             }
        
#         # Create transformer
#         transformer = TransformerFactory.create_transformer(
#             transformer_type="json",
#             orient=CONFIG['transformation'].get('orient', 'records'),
#             date_format=CONFIG['transformation'].get('date_format', 'iso'),
#             handle_nan=CONFIG['transformation'].get('handle_nan', 'null'),
#             include_index=CONFIG['transformation'].get('include_index', False)
#         )
        
#         # Transform to JSON
#         json_data = transformer.transform(df)
        
#         result = {
#             'table_name': table_name,
#             'row_count': len(json_data),
#             'data': json_data
#         }
        
#         logger.info(f"Transformed {len(json_data)} rows for {table_name}")
        
#         return result
        
#     except Exception as e:
#         logger.error(f"Error transforming data: {e}")
#         raise


# def load_to_elasticsearch(transformation_result: Dict[str, Any], **context) -> bool:
#     """
#     Load transformed data to Elasticsearch.
    
#     Args:
#         transformation_result: Result from transformation task
        
#     Returns:
#         True if successful
#     """
#     try:
#         table_name = transformation_result['table_name']
#         logger.info(f"Loading data to Elasticsearch for table: {table_name}")
        
#         # Get data
#         data = transformation_result['data']
        
#         if not data:
#             logger.warning(f"No data to load for {table_name}")
#             return True
        
#         # Get Elasticsearch connector
#         conn_manager = ConnectionManager()
#         es_conn_id = context['task_instance'].xcom_pull(
#             task_ids='create_elasticsearch_connector',
#             key='es_conn_id'
#         )
#         es_connector = conn_manager.get_connection(es_conn_id)
        
#         if not es_connector:
#             raise ValueError(f"Elasticsearch connector not found: {es_conn_id}")
        
#         # Get index configuration
#         index_config = CONFIG['loading']['index_mappings'].get(table_name, {})
#         index_name = index_config.get('index_name', f"etl_{table_name}")
#         id_field = index_config.get('id_field')
        
#         # Create index if it doesn't exist
#         if not es_connector.index_exists(index_name):
#             es_connector.create_index(index_name)
#             logger.info(f"Created index: {index_name}")
        
#         # Create loader
#         loader = LoaderFactory.create_loader(
#             loader_type="elasticsearch",
#             connector=es_connector,
#             index_name=index_name,
#             id_field=id_field,
#             bulk_size=CONFIG['loading']['elasticsearch'].get('bulk_size', 1000),
#             max_retries=CONFIG['loading']['elasticsearch'].get('max_retries', 3),
#             raise_on_error=CONFIG['loading']['elasticsearch'].get('raise_on_error', False)
#         )
        
#         # Load data in bulk
#         success = loader.load_batch(data)
        
#         if success:
#             logger.info(f"Successfully loaded {len(data)} documents to {index_name}")
#         else:
#             logger.warning(f"Load completed with some errors for {index_name}")
        
#         return success
        
#     except Exception as e:
#         logger.error(f"Error loading to Elasticsearch: {e}")
#         raise


# def cleanup_connections(**context):
#     """
#     Clean up all connections.
#     """
#     try:
#         logger.info("Cleaning up connections")
#         conn_manager = ConnectionManager()
#         conn_manager.close_all()
#         logger.info("All connections closed")
#     except Exception as e:
#         logger.error(f"Error during cleanup: {e}")
#         # Don't raise - cleanup is best effort


# # Define default arguments
# default_args = {
#     'owner': CONFIG['airflow']['default_args']['owner'],
#     'depends_on_past': False,
#     'email_on_failure': CONFIG['airflow']['default_args'].get('email_on_failure', False),
#     'email_on_retry': CONFIG['airflow']['default_args'].get('email_on_retry', False),
#     'retries': CONFIG['airflow']['default_args'].get('retries', 2),
#     'retry_delay': timedelta(minutes=CONFIG['airflow']['default_args'].get('retry_delay_minutes', 5)),
# }

# # Create DAG
# with DAG(
#     dag_id=CONFIG['airflow']['dag_id'],
#     default_args=default_args,
#     description='ETL pipeline from PostgreSQL to Elasticsearch',
#     schedule_interval=CONFIG['airflow']['schedule_interval'],
#     start_date=datetime.fromisoformat(CONFIG['airflow']['start_date']),
#     catchup=CONFIG['airflow'].get('catchup', False),
#     max_active_runs=CONFIG['airflow'].get('max_active_runs', 1),
#     tags=CONFIG['airflow'].get('tags', []),
# ) as dag:
    
#     # Create connectors
#     create_postgres_conn = PythonOperator(
#         task_id='create_postgres_connector',
#         python_callable=create_postgres_connector,
#         provide_context=True
#     )
    
#     create_es_conn = PythonOperator(
#         task_id='create_elasticsearch_connector',
#         python_callable=create_elasticsearch_connector,
#         provide_context=True
#     )
    
#     # Cleanup task
#     cleanup = PythonOperator(
#         task_id='cleanup_connections',
#         python_callable=cleanup_connections,
#         provide_context=True,
#         trigger_rule='all_done'  # Run regardless of upstream success/failure
#     )
    
#     # Create tasks for each table
#     table_tasks = []
    
#     for table_config in CONFIG['extraction']['tables']:
#         table_name = table_config['table_name']
        
#         # Extract task
#         extract_task = PythonOperator(
#             task_id=f'extract_{table_name}',
#             python_callable=extract_table_data,
#             op_kwargs={'table_config_dict': table_config},
#             provide_context=True
#         )
        
#         # Transform task
#         transform_task = PythonOperator(
#             task_id=f'transform_{table_name}',
#             python_callable=transform_data,
#             op_kwargs={'extraction_result': "{{ task_instance.xcom_pull(task_ids='extract_" + table_name + "') }}"},
#             provide_context=True
#         )
        
#         # Load task
#         load_task = PythonOperator(
#             task_id=f'load_{table_name}',
#             python_callable=load_to_elasticsearch,
#             op_kwargs={'transformation_result': "{{ task_instance.xcom_pull(task_ids='transform_" + table_name + "') }}"},
#             provide_context=True
#         )
        
#         # Set dependencies for this table's pipeline
#         create_postgres_conn >> extract_task >> transform_task >> load_task >> cleanup
#         create_es_conn >> load_task
        
#         table_tasks.append((extract_task, transform_task, load_task))
    
#     # Set overall dependencies
#     # Connectors are created first, then all table pipelines run in parallel, then cleanup
