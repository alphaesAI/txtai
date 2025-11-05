"""
Standalone example demonstrating the ETL pipeline usage without Airflow.
This script shows how to use the modules programmatically.
"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime
import logging

from connector import ConnectorFactory, ConnectionManager
from extractor import ExtractorFactory, TableConfig, ExtractionMode, StateManager
from transformer import TransformerFactory
from loader import LoaderFactory


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """
    Main ETL pipeline execution.
    """
    # Configuration
    postgres_conn_str = "postgresql://username:password@localhost:5432/database"
    es_conn_str = "http://localhost:9200"
    
    state_file = "/tmp/etl_state_example.json"
    
    # Initialize state manager
    state_manager = StateManager(state_file)
    
    try:
        # Step 1: Create connectors
        logger.info("=" * 50)
        logger.info("STEP 1: Creating Database Connectors")
        logger.info("=" * 50)
        
        # PostgreSQL connector
        pg_connector = ConnectorFactory.create_connector(
            connector_type="postgres",
            connection_string=postgres_conn_str,
            pool_size=5,
            echo=False
        )
        pg_connector.connect()
        logger.info("✓ PostgreSQL connected")
        
        # Elasticsearch connector
        es_connector = ConnectorFactory.create_connector(
            connector_type="elasticsearch",
            connection_string=es_conn_str,
            use_ssl=False,
            verify_certs=False
        )
        es_connector.connect()
        logger.info("✓ Elasticsearch connected")
        
        # Step 2: Extract data
        logger.info("\n" + "=" * 50)
        logger.info("STEP 2: Extracting Data from PostgreSQL")
        logger.info("=" * 50)
        
        # Create extractor
        extractor = ExtractorFactory.create_extractor(
            extractor_type="postgres",
            connector=pg_connector
        )
        
        # Example 1: Full extraction
        logger.info("\n--- Example 1: Full Extraction ---")
        full_config = TableConfig(
            table_name="products",
            schema="public",
            columns=["id", "name", "price", "category"],
            extraction_mode=ExtractionMode.FULL,
            batch_size=1000
        )
        df_full = extractor.extract_with_config(full_config)
        logger.info(f"Extracted {len(df_full)} rows (full extraction)")
        
        # Example 2: Incremental CDC extraction
        logger.info("\n--- Example 2: CDC Incremental Extraction ---")
        
        # Get last extracted value from state
        last_id = state_manager.get_last_extracted_value("users", "id")
        
        cdc_config = TableConfig(
            table_name="users",
            schema="public",
            columns=["id", "username", "email", "created_at"],
            extraction_mode=ExtractionMode.INCREMENTAL_CDC,
            cdc_column="id",
            last_extracted_value=last_id,
            batch_size=1000,
            order_by="id"
        )
        df_cdc = extractor.extract_with_config(cdc_config)
        logger.info(f"Extracted {len(df_cdc)} rows (CDC incremental)")
        
        # Update state with new max value
        if not df_cdc.empty:
            max_id = df_cdc['id'].max()
            state_manager.set_last_extracted_value("users", "id", max_id)
            logger.info(f"Updated state: users.id = {max_id}")
        
        # Example 3: Date-based incremental extraction
        logger.info("\n--- Example 3: Date-based Incremental Extraction ---")
        
        last_date = state_manager.get_last_extracted_value("orders", "order_date")
        start_date = datetime.fromisoformat(last_date) if last_date else None
        
        date_config = TableConfig(
            table_name="orders",
            schema="public",
            columns=["order_id", "user_id", "order_date", "total_amount"],
            extraction_mode=ExtractionMode.INCREMENTAL_DATE,
            date_column="order_date",
            start_date=start_date,
            batch_size=1000
        )
        df_date = extractor.extract_with_config(date_config)
        logger.info(f"Extracted {len(df_date)} rows (date-based incremental)")
        
        # Update state
        if not df_date.empty:
            max_date = df_date['order_date'].max()
            state_manager.set_last_extracted_value(
                "orders",
                "order_date",
                max_date.isoformat()
            )
            logger.info(f"Updated state: orders.order_date = {max_date}")
        
        # Step 3: Transform data
        logger.info("\n" + "=" * 50)
        logger.info("STEP 3: Transforming Data to JSON")
        logger.info("=" * 50)
        
        # Create transformer
        transformer = TransformerFactory.create_transformer(
            transformer_type="json",
            orient="records",
            date_format="iso",
            handle_nan="null"
        )
        
        # Transform all datasets
        json_full = transformer.transform(df_full) if not df_full.empty else []
        json_cdc = transformer.transform(df_cdc) if not df_cdc.empty else []
        json_date = transformer.transform(df_date) if not df_date.empty else []
        
        logger.info(f"✓ Transformed products: {len(json_full)} records")
        logger.info(f"✓ Transformed users: {len(json_cdc)} records")
        logger.info(f"✓ Transformed orders: {len(json_date)} records")
        
        # Step 4: Load data to Elasticsearch
        logger.info("\n" + "=" * 50)
        logger.info("STEP 4: Loading Data to Elasticsearch")
        logger.info("=" * 50)
        
        # Create indices if they don't exist
        for index_name in ["etl_products", "etl_users", "etl_orders"]:
            if not es_connector.index_exists(index_name):
                es_connector.create_index(index_name)
                logger.info(f"Created index: {index_name}")
        
        # Load products
        if json_full:
            logger.info("\n--- Loading Products ---")
            products_loader = LoaderFactory.create_loader(
                loader_type="elasticsearch",
                connector=es_connector,
                index_name="etl_products",
                id_field="id",
                bulk_size=1000
            )
            success = products_loader.load_batch(json_full)
            logger.info(f"✓ Loaded {len(json_full)} products (success: {success})")
        
        # Load users
        if json_cdc:
            logger.info("\n--- Loading Users ---")
            users_loader = LoaderFactory.create_loader(
                loader_type="elasticsearch",
                connector=es_connector,
                index_name="etl_users",
                id_field="id",
                bulk_size=1000
            )
            success = users_loader.load_batch(json_cdc)
            logger.info(f"✓ Loaded {len(json_cdc)} users (success: {success})")
        
        # Load orders
        if json_date:
            logger.info("\n--- Loading Orders ---")
            orders_loader = LoaderFactory.create_loader(
                loader_type="elasticsearch",
                connector=es_connector,
                index_name="etl_orders",
                id_field="order_id",
                bulk_size=1000
            )
            success = orders_loader.load_batch(json_date)
            logger.info(f"✓ Loaded {len(json_date)} orders (success: {success})")
        
        # Step 5: Verify results
        logger.info("\n" + "=" * 50)
        logger.info("STEP 5: Verifying Results")
        logger.info("=" * 50)
        
        for index_name in ["etl_products", "etl_users", "etl_orders"]:
            loader = LoaderFactory.create_loader(
                loader_type="elasticsearch",
                connector=es_connector,
                index_name=index_name
            )
            count = loader.count()
            logger.info(f"Index '{index_name}': {count} documents")
        
        logger.info("\n" + "=" * 50)
        logger.info("ETL Pipeline Completed Successfully! ✓")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {e}", exc_info=True)
        raise
        
    finally:
        # Cleanup connections
        logger.info("\nCleaning up connections...")
        try:
            pg_connector.disconnect()
            logger.info("✓ PostgreSQL disconnected")
        except:
            pass
        
        try:
            es_connector.disconnect()
            logger.info("✓ Elasticsearch disconnected")
        except:
            pass


if __name__ == "__main__":
    main()
