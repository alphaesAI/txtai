import logging
from pathlib import Path
from datetime import datetime

import yaml
import pandas as pd



from etl_pipeline.connector import ConnectorFactory, ConnectionManager
from etl_pipeline.extractor import ExtractorFactory, TableConfig, StateManager, ExtractionMode
from etl_pipeline.transformer import TransformerFactory
from etl_pipeline.loader import LoaderFactory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ETL_TEST")

def load_config(config_path: str = None):
    if config_path is None:
        config_path = Path(__file__).parent / "/home/logi/txtai/etl_pipeline/config/etl_config.yaml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config

print("read successful")

def run_etl_test():
    config = load_config()
    tables = config['extraction']['tables']

    #create and register connectors
    logger.info("=== step 1: create postgresql connector ===")
    postgres_connector = ConnectorFactory.create_connector(
        connector_type="postgres",
        connection_string=config['postgres']['connection_string'],
        pool_size=config['postgres'].get('pool_size', 5),
        max_overflow=config['postgres'].get('max_overflow', 10),
        pool_timeout=config['postgres'].get('pool_timeout', 30),
        echo=config['postgres'].get('echo', False)
    )
    postgres_connector.connect()
    if not postgres_connector.test_connection():
        raise ConnectionError("Postgresql connection test failed")
    
    conn_manager = ConnectionManager()
    postgres_conn_id = "postgres_source"
    conn_manager.register_connection(postgres_conn_id, postgres_connector)

    logger.info("=== STEP 2: Create Elasticsearch connector ===")
    es_connector = ConnectorFactory.create_connector(
        connector_type="elasticsearch",
        connection_string=config['elasticsearch']['connection_string'],
        use_ssl=config['elasticsearch'].get('use_ssl', False),
        verify_certs=config['elasticsearch'].get('verify_certs', False),
        timeout=config['elasticsearch'].get('timeout', 30),
        max_retries=config['elasticsearch'].get('max_retries', 3)
    )
    es_connector.connect()
    if not es_connector.test_connection():
        raise ConnectionError("Elasticsearch connection test failed")
    es_conn_id = "elasticsearch_target"
    conn_manager.register_connection(es_conn_id, es_connector)

    for table_config_dict in tables:
        table_name = table_config_dict['table_name']
        logger.info(f"=== PROCESSING TABLE: {table_name} ===")

        # --- Extract ---
        logger.info(f"-> Extracting data from {table_name}")
        table_config = TableConfig(**table_config_dict)
        state_manager = StateManager(config['extraction']['state_file'])

        # Load last extracted value for incremental loads
        if table_config.extraction_mode == ExtractionMode.INCREMENTAL_DATE:
            last_date = state_manager.get_last_extracted_value(
                table_name, table_config.date_column
            )
            if last_date:
                table_config.start_date = datetime.fromisoformat(last_date)
                logger.info(f"Using last extracted date: {last_date}")

        extractor = ExtractorFactory.create_extractor(
            extractor_type="postgres",
            connector=postgres_connector
        )
        df = extractor.extract_with_config(table_config)

        # Update state
        if not df.empty and table_config.extraction_mode == ExtractionMode.INCREMENTAL_DATE:
            max_date = df[table_config.date_column].max()
            state_manager.set_last_extracted_value(
                table_name, table_config.date_column, max_date.isoformat()
            )

        extraction_result = {
            'table_name': table_name,
            'row_count': len(df),
            'data': df.to_dict('records'),
            'columns': list(df.columns)
        }
        logger.info(f"Extracted {len(df)} rows from {table_name}")

        # --- Transform ---
        logger.info(f"-> Transforming data for {table_name}")
        transformer = TransformerFactory.create_transformer(
            transformer_type="json",
            orient=config['transformation'].get('orient', 'records'),
            date_format=config['transformation'].get('date_format', 'iso'),
            handle_nan=config['transformation'].get('handle_nan', 'null'),
            include_index=config['transformation'].get('include_index', False)
        )
        json_data = transformer.transform(pd.DataFrame(extraction_result['data']))
        transformation_result = {
            'table_name': table_name,
            'row_count': len(json_data),
            'data': json_data
        }
        logger.info(f"Transformed {len(json_data)} rows for {table_name}")

        # --- Load ---
        logger.info(f"-> Loading data to Elasticsearch for {table_name}")
        index_config = config['loading']['index_mappings'].get(table_name, {})
        index_name = index_config.get('index_name', f"etl_{table_name}")
        id_field = index_config.get('id_field')

        if not es_connector.index_exists(index_name):
            es_connector.create_index(index_name)
            logger.info(f"Created index: {index_name}")

        loader = LoaderFactory.create_loader(
            loader_type="elasticsearch",
            connector=es_connector,
            index_name=index_name,
            id_field=id_field,
            bulk_size=config['loading']['elasticsearch'].get('bulk_size', 1000),
            max_retries=config['loading']['elasticsearch'].get('max_retries', 3),
            raise_on_error=config['loading']['elasticsearch'].get('raise_on_error', False),
        )
        success = loader.load_batch(transformation_result['data'])
        if success:
            logger.info(f"✅ Successfully loaded {len(transformation_result['data'])} rows to {index_name}")
        else:
            logger.warning(f"⚠️ Load completed with some errors for {index_name}")

    # Cleanup
    logger.info("=== CLEANUP CONNECTIONS ===")
    conn_manager.close_all()
    logger.info("All connections closed successfully.")


if __name__ == "__main__":
    run_etl_test()