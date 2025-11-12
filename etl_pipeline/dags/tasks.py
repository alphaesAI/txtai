from typing import Dict, Any, List
import yaml
from datetime import datetime, timedelta
from pathlib import Path

import logging

from etl_pipeline.connector import ConnectorFactory, ConnectionManager
from etl_pipeline.extractor import ExtractorFactory, TableConfig, StateManager, ExtractionMode
from etl_pipeline.transformer import TransformerFactory
from etl_pipeline.loader import LoaderFactory

logger = logging.getLogger(__name__)

# Load configuration
def load_config(config_path: str = None) -> Dict[str, Any]:
    """ Load ETL configuration from YAML file. """
    if config_path is None:
        config_path = Path(__file__).parent.parent / "config" / "etl_config.yaml"

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    return config

# Global config
CONFIG = load_config()

def create_postgres_connector(**context) -> str:
    """ 
    Create and register PostgreSQL connector.
    
    Returns:
        Connection ID
    """
    try:
        logger.info("Creating postgresql connector")

        #create connector
        connector = ConnectorFactory.create_connector(
            connector_type="postgres",
            connection_string=CONFIG['postgres']['connection_string'],
            pool_size=CONFIG['postgres'].get('pool_size', 5),
            max_overflow=CONFIG['postgres'].get('max_overflow', 10),
            pool_timeout=CONFIG['postgres'].get("pool_timeout", 30),
            echo=CONFIG['postgres'].get('echo', False)
        )

        #connect
        connector.connect()

        #test connection
        if not connector.test_connection():
            raise ConnectionError("postgresql connection test failed")
        
        #register with connection manager
        conn_manager = ConnectionManager()
        connection_id = "postgres_source"
        conn_manager.register_connection(connection_id, connector)

        logger.info(f"postgresql connector created and registered: {connection_id}")

        #push connection id to xcom
        context['task_instance'].xcom_push(key='postgres_conn_id', value=connection_id)
        
        return connection_id
    
    except Exception as e:
        logger.error(f"Error creating postgresql connector: {e}")
        raise

def create_elasticsearch_connector(**context) -> str:
    """ 
    Create and register Elasticsearch connector.
     
    Returns:
        Connedtion ID
    """
    try:
        logger.info("Creating Elasticsearch connector")

        #create connector
        connector = ConnectorFactory.create_connector(
            connector_type="elasticsearch",
            connection_string=CONFIG['elasticsearch']['connection_string'],
            use_ssl=CONFIG['elasticsearch'].get('use_ssl', False),
            verify_certs=CONFIG['elasticsearch'].get('verify_certs', False),
            timeout=CONFIG['elasticsearch'].get('timeout', 30),
            max_retries=CONFIG['elasticsearch'].get('max_retries', 3)
        )

        #connect
        connector.connect()

        #test connection
        if not connector.test_connection():
            raise ConnectionError("Elasticsearch connection test failed")
        
        conn_manager = ConnectionManager()
        connection_id = "elasticsearch_target"
        conn_manager.register_connection(connection_id, connector)

        logger.info(f"Elasticsearch connector created and registered: {connection_id}")

        #push connection ID to Xcom
        context['task_instance'].xcom_push(key="es_conn_id", value=connection_id)

        return connection_id
    
    except Exception as e:
        logger.error(f"Error creating Elasticsearch connector: {e}")
        raise

def extract_table_data(table_config_dict: Dict[str, Any], **context) -> Dict[str, Any]:
    """ 
    Extract data from a postgresql table.
     
    Args:
        table_config_dict: Table configuration dictionary
    
    Returns:
        Extraction result with data and metadata
    """
    try:
        table_name = table_config_dict['table_name']
        logger.info(f"Extracting data from table: {table_name}")

        #get postgresql connector
        conn_manager = ConnectionManager()
        postgres_conn_id = context['task_instance'].xcom_pull(
            task_ids='create_postgres_connector',
            key='postgres_conn_id'
        )
        postgres_connector = conn_manager.get_connection(postgres_conn_id)

        logger.info(f"Using postgres connector: {postgres_conn_id}, found: {bool(postgres_connector)}")
        if not postgres_connector:
            connection_id = create_postgres_connector(**context)
            postgres_connector = conn_manager.get_connection(connection_id)
            #raise ValueError(f"PostgresSQL connector not found: {postgres_conn_id}")
        
        #create extractor
        extractor = ExtractorFactory.create_extractor(
            extractor_type="postgres",
            connector=postgres_connector
        )

        #load state manager
        state_file = CONFIG['extraction']['state_file']
        state_manager = StateManager(state_file)

        #build table config
        table_config = TableConfig(**table_config_dict)

        #update last extracted value from state for incremental loads
        if table_config.extraction_mode == ExtractionMode.INCREMENTAL_DATE:
            last_date = state_manager.get_last_extracted_value(
                table_name,
                table_config.date_column
            )
            if last_date is not None:
                table_config.start_date = datetime.fromisoformat(last_date)
                logger.info(f"Using last date: {last_date}")

        #extract data
        df = extractor.extract_with_config(table_config)

        #update state
        if not df.empty:
            if table_config.extraction_mode == ExtractionMode.INCREMENTAL_DATE:
                max_date = df[table_config.date_column].max()
                # Ensure max_date is converted to ISO format if it's a datetime object
                if hasattr(max_date, 'isoformat'):
                    max_date_iso = max_date.isoformat()
                else:
                    max_date_iso = str(max_date)
                state_manager.set_last_extracted_value(
                    table_name, 
                    table_config.date_column,
                    max_date_iso
                )
                logger.info(f"Updated date state: {max_date_iso}")

        #convert dataframe to dict for XCom
        data_dict = df.to_dict('records')
        
        # Ensure all datetime objects are serialized to ISO format
        for record in data_dict:
            for key, value in record.items():
                if hasattr(value, 'isoformat'):  # Check if it's a datetime-like object
                    record[key] = value.isoformat()

        result = {
            'table_name': table_name,
            'row_count': len(df),
            'data': data_dict,
            'columns': list(df.columns)
        }

        logger.info(f"Extracted {len(df)} rows from {table_name}")
        
        return result
    
    except Exception as e:
        logger.error(f"Error extracting data: {e}")
        raise

def transform_data(table_name: str, **context) -> Dict[str, Any]:
    """Transform extracted data to JSON format for a specific table."""
    try:
        # Pull extraction result from XCom
        extraction_result = context['task_instance'].xcom_pull(
            task_ids=f"extract_{table_name}"
        )

        if not extraction_result:
            logger.warning(f"No extraction result found for {table_name}")
            return {'table_name': table_name, 'row_count': 0, 'data': []}

        logger.info(f"Transforming data for table: {table_name}")

        import pandas as pd
        df = pd.DataFrame(extraction_result['data'])

        if df.empty:
            logger.warning(f"No data to transform for {table_name}")
            return {'table_name': table_name, 'row_count': 0, 'data': []}

        transformer = TransformerFactory.create_transformer(
            transformer_type="json",
            orient=CONFIG['transformation'].get('orient', 'records'),
            date_format=CONFIG['transformation'].get('date_format', 'iso'),
            handle_nan=CONFIG['transformation'].get('handle_nan', 'null'),
            include_index=CONFIG['transformation'].get('include_index', False)
        )

        json_data = transformer.transform(df)

        result = {
            'table_name': table_name,
            'row_count': len(json_data),
            'data': json_data
        }

        logger.info(f"Transformed {len(json_data)} rows for {table_name}")
        return result

    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise

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

#         #convert data back to datframe
#         import pandas as pd
#         df = pd.DataFrame(extraction_result['data'])

#         if df.empty:
#             logger.warning(f"No data to transform for {table_name}")
#             return {
#                 'table_name': table_name,
#                 'row_count': 0,
#                 'data': []
#             }
        
#         #create transformer
#         transformer = TransformerFactory.create_transformer(
#             transformer_type="json",
#             orient=CONFIG['transformation'].get('orient', 'records'),
#             date_format=CONFIG['transformation'].get('date_format', 'iso'),
#             handle_nan=CONFIG['transformation'].get('handle_nan', 'null'),
#             include_index=CONFIG['transformation'].get('include_index', False)
#         )

#         #transform to JSON
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

def load_to_elasticsearch(table_name: str, **context) -> bool:
    """ 
    Load transformed data to elasticsearch.
    
    Args:
        table_name: Name of the table to load
        
    Returns:
        True if successful
    """
    try:
        logger.info(f"Loading data to elasticsearch for table: {table_name}")

        # Pull transformation result from XCom
        transformation_result = context['task_instance'].xcom_pull(
            task_ids=f"transform_{table_name}"
        )
        
        if not transformation_result:
            logger.warning(f"No transformation result found for {table_name}")
            return True

        #get data
        data = transformation_result['data']

        if not data:
            logger.warning(f"No data to load for {table_name}")
            return True
        
        #get elasticsearch connector
        conn_manager = ConnectionManager()
        es_conn_id = context['task_instance'].xcom_pull(
            task_ids='create_elasticsearch_connector',
            key='es_conn_id'
        )
        es_connector = conn_manager.get_connection(es_conn_id)

        logger.info(f"using elasticsearch connector: {es_conn_id}, found: {bool(es_connector)}")
        if not es_connector:
            logger.warning("elasticsearch connector not found in ConnectionManager, recreating...")
            es_conn_id = create_elasticsearch_connector(**context)
            es_connector = conn_manager.get_connection(es_conn_id)
            #raise ValueError(f"Elasticsearch connector not found: {es_conn_id}")

        #get index configuration
        index_config = CONFIG['loading']['index_mappings'].get(table_name, {})
        index_name = index_config.get('index_name', f"etl_{table_name}")
        id_field = index_config.get('id_field')

        #create index if it doesn't exist
        if not es_connector.index_exists(index_name):
            es_connector.create_index(index_name)
            logger.info(f"Created index: {index_name}")

        #create loader
        loader = LoaderFactory.create_loader(
            loader_type="elasticsearch",
            connector=es_connector,
            index_name=index_name,
            id_field=id_field,
            bulk_size=CONFIG['loading']['elasticsearch'].get('bulk_size', 1000),
            max_retries=CONFIG['loading']['elasticsearch'].get('max_retries', 3),
            raise_on_error=CONFIG['loading']['elasticsearch'].get('raise_on_error', False),
        )

        #load data in bulk
        success = loader.load_batch(data)

        if success:
            logger.info(f"Successfully loaded {len(data)} documents to {index_name}")
        else:
            logger.warning(f"Load completed with some errors for {index_name}")

        return success
    
    except Exception as e:
        logger.error(f"Error loading to elasticsearch: {e}")
        raise

def cleanup_connections(**context):
    """
    Clean up all connections.
    """
    try:
        logger.info("cleaning up connections")
        conn_manager = ConnectionManager()
        conn_manager.close_all()
        logger.info("All connections closed")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        