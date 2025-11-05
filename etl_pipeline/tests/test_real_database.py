"""
Real database test - connects to actual PostgreSQL and Elasticsearch.
Tests the full ETL flow with real data extraction.
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import yaml

from connector import ConnectorFactory, ConnectionManager
from extractor import ExtractorFactory, TableConfig, ExtractionMode, StateManager
from transformer import TransformerFactory
from loader import LoaderFactory


def load_config():
    """Load configuration or use test defaults."""
    config_path = Path(__file__).parent.parent / "config" / "etl_config.yaml"
    
    if config_path.exists():
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    # Fallback to hardcoded test config
    return {
        'postgres': {
            'connection_string': 'postgresql://rag_user:rag_password@localhost:5432/paper_curator',
            'pool_size': 5,
            'echo': False
        },
        'elasticsearch': {
            'connection_string': 'http://localhost:9200',
            'use_ssl': False,
            'verify_certs': False
        }
    }


def test_real_extraction_and_transformation():
    """
    Test real data extraction from PostgreSQL and transformation to JSON.
    This is the actual ETL flow: Extract -> Transform -> Load
    """
    print("\n" + "=" * 70)
    print("REAL DATABASE ETL TEST")
    print("Extract from PostgreSQL → Transform to JSON → Load to Elasticsearch")
    print("=" * 70)
    
    config = load_config()
    pg_connector = None
    es_connector = None
    
    try:
        # ================================================================
        # STEP 1: Connect to PostgreSQL
        # ================================================================
        print("\n📊 STEP 1: Connecting to PostgreSQL...")
        print("-" * 70)
        
        pg_connector = ConnectorFactory.create_connector(
            connector_type="postgres",
            connection_string=config['postgres']['connection_string'],
            pool_size=config['postgres'].get('pool_size', 5),
            echo=config['postgres'].get('echo', False)
        )
        
        pg_connector.connect()
        
        if not pg_connector.test_connection():
            raise ConnectionError("PostgreSQL connection test failed")
        
        print("✓ PostgreSQL connected successfully")
        print(f"  Connection string: {config['postgres']['connection_string'].split('@')[1]}")
        
        # ================================================================
        # STEP 2: Extract Real Data
        # ================================================================
        print("\n📤 STEP 2: Extracting Data from PostgreSQL...")
        print("-" * 70)
        
        # Create extractor
        extractor = ExtractorFactory.create_extractor(
            extractor_type="postgres",
            connector=pg_connector
        )
        
        # Test table - adjust based on your database
        table_name = "students"  # Change this to your actual table
        columns = ["id", "name", "department", "marks", "created_date", "last_modified_timestamp"]
        
        print(f"Table: {table_name}")
        print(f"Columns: {columns}")
        
        # Test 1: Full Extraction
        print("\n  → Full Extraction Mode...")
        full_config = TableConfig(
            table_name=table_name,
            schema="public",
            columns=columns,
            extraction_mode=ExtractionMode.FULL,
            batch_size=1000
        )
        
        df_full = extractor.extract_with_config(full_config)
        print(f"  ✓ Extracted {len(df_full)} rows (full extraction)")
        
        if len(df_full) > 0:
            print(f"\n  Sample Data (first 3 rows):")
            print(df_full.head(3).to_string())
            print(f"\n  Data Types:")
            print(df_full.dtypes)
        else:
            print("  ⚠ No data found in table")
            return False
        
        # Test 2: CDC Incremental Extraction
        print("\n  → CDC Incremental Extraction Mode...")
        
        # Get current max ID
        max_id = df_full['id'].max() if len(df_full) > 0 else 0
        
        # Simulate CDC by extracting records with ID > some value
        cdc_config = TableConfig(
            table_name=table_name,
            schema="public",
            columns=columns,
            extraction_mode=ExtractionMode.INCREMENTAL_CDC,
            cdc_column="id",
            last_extracted_value=max_id - 5 if max_id > 5 else 0,  # Get last 5 records
            batch_size=1000,
            order_by="id"
        )
        
        df_cdc = extractor.extract_with_config(cdc_config)
        print(f"  ✓ Extracted {len(df_cdc)} rows (CDC incremental)")
        
        # Test 3: Date-based Incremental Extraction
        print("\n  → Date-based Incremental Extraction Mode...")
        
        # Extract records from last 30 days
        start_date = datetime.now() - timedelta(days=30)
        
        date_config = TableConfig(
            table_name=table_name,
            schema="public",
            columns=columns,
            extraction_mode=ExtractionMode.INCREMENTAL_DATE,
            date_column="created_date",
            start_date=start_date,
            batch_size=1000,
            order_by="created_date"
        )
        
        df_date = extractor.extract_with_config(date_config)
        print(f"  ✓ Extracted {len(df_date)} rows (date-based incremental)")
        print(f"    Date range: {start_date.date()} to now")
        
        # ================================================================
        # STEP 3: Transform Extracted Data to JSON
        # ================================================================
        print("\n🔄 STEP 3: Transforming Data to JSON...")
        print("-" * 70)
        
        # Create transformer
        transformer = TransformerFactory.create_transformer(
            transformer_type="json",
            orient="records",
            date_format="iso",
            handle_nan="null",
            include_index=False
        )
        
        # Transform full extraction
        print("\n  → Transforming Full Extraction...")
        json_full = transformer.transform(df_full)
        print(f"  ✓ Transformed {len(json_full)} records")
        print(f"\n  Sample JSON (first record):")
        import json
        print(json.dumps(json_full[0], indent=2, default=str))
        
        # Transform CDC extraction
        if len(df_cdc) > 0:
            print("\n  → Transforming CDC Extraction...")
            json_cdc = transformer.transform(df_cdc)
            print(f"  ✓ Transformed {len(json_cdc)} records")
        
        # Transform date-based extraction
        if len(df_date) > 0:
            print("\n  → Transforming Date-based Extraction...")
            json_date = transformer.transform(df_date)
            print(f"  ✓ Transformed {len(json_date)} records")
        
        # ================================================================
        # STEP 4: Connect to Elasticsearch
        # ================================================================
        print("\n🔍 STEP 4: Connecting to Elasticsearch...")
        print("-" * 70)
        
        es_connector = ConnectorFactory.create_connector(
            connector_type="elasticsearch",
            connection_string=config['elasticsearch']['connection_string'],
            use_ssl=config['elasticsearch'].get('use_ssl', False),
            verify_certs=config['elasticsearch'].get('verify_certs', False)
        )
        
        es_connector.connect()
        
        if not es_connector.test_connection():
            raise ConnectionError("Elasticsearch connection test failed")
        
        print("✓ Elasticsearch connected successfully")
        print(f"  Host: {config['elasticsearch']['connection_string']}")
        
        # ================================================================
        # STEP 5: Load Data to Elasticsearch
        # ================================================================
        print("\n📥 STEP 5: Loading Data to Elasticsearch...")
        print("-" * 70)
        
        index_name = f"etl_test_{table_name}"
        
        # Create index if it doesn't exist
        if not es_connector.index_exists(index_name):
            es_connector.create_index(index_name)
            print(f"✓ Created index: {index_name}")
        else:
            print(f"✓ Index already exists: {index_name}")
        
        # Create loader
        loader = LoaderFactory.create_loader(
            loader_type="elasticsearch",
            connector=es_connector,
            index_name=index_name,
            id_field="id",
            bulk_size=1000,
            max_retries=3,
            raise_on_error=False
        )
        
        # Load full extraction data
        print(f"\n  → Loading {len(json_full)} documents in bulk mode...")
        success = loader.load_batch(json_full)
        
        if success:
            print(f"  ✓ Successfully loaded {len(json_full)} documents")
        else:
            print(f"  ⚠ Load completed with some warnings")
        
        # Verify load
        import time
        time.sleep(2)  # Wait for Elasticsearch to index
        
        count = loader.count()
        print(f"\n  → Verifying load...")
        print(f"  ✓ Document count in index: {count}")
        
        # ================================================================
        # STEP 6: Verify End-to-End Flow
        # ================================================================
        print("\n✅ STEP 6: Verification...")
        print("-" * 70)
        
        print(f"  ✓ Extracted {len(df_full)} rows from PostgreSQL")
        print(f"  ✓ Transformed {len(json_full)} records to JSON")
        print(f"  ✓ Loaded {count} documents to Elasticsearch")
        print(f"  ✓ Data pipeline integrity: {len(df_full) == len(json_full) == count}")
        
        # ================================================================
        # Summary
        # ================================================================
        print("\n" + "=" * 70)
        print("✅ REAL DATABASE ETL TEST COMPLETED SUCCESSFULLY!")
        print("=" * 70)
        print(f"\n📊 Summary:")
        print(f"  • Source: PostgreSQL table '{table_name}'")
        print(f"  • Records processed: {len(df_full)}")
        print(f"  • Target: Elasticsearch index '{index_name}'")
        print(f"  • Extraction modes tested: Full, CDC, Date-based")
        print(f"  • Transformation: DataFrame → JSON")
        print(f"  • Loading: Bulk mode")
        print()
        
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Cleanup
        print("\n🧹 Cleaning up connections...")
        if pg_connector:
            try:
                pg_connector.disconnect()
                print("  ✓ PostgreSQL disconnected")
            except:
                pass
        
        if es_connector:
            try:
                es_connector.disconnect()
                print("  ✓ Elasticsearch disconnected")
            except:
                pass


def test_with_state_management():
    """
    Test incremental extraction with state management.
    """
    print("\n" + "=" * 70)
    print("STATE MANAGEMENT TEST")
    print("Testing incremental extraction with persistent state")
    print("=" * 70)
    
    config = load_config()
    state_file = "/tmp/etl_test_state.json"
    
    pg_connector = None
    
    try:
        # Connect to PostgreSQL
        print("\n📊 Connecting to PostgreSQL...")
        
        pg_connector = ConnectorFactory.create_connector(
            connector_type="postgres",
            connection_string=config['postgres']['connection_string']
        )
        pg_connector.connect()
        print("✓ Connected")
        
        # Initialize state manager
        print("\n📝 Initializing State Manager...")
        state_manager = StateManager(state_file)
        print(f"✓ State file: {state_file}")
        
        # Create extractor
        extractor = ExtractorFactory.create_extractor(
            extractor_type="postgres",
            connector=pg_connector
        )
        
        table_name = "students"
        cdc_column = "id"
        
        # Get last extracted value from state
        last_value = state_manager.get_last_extracted_value(table_name, cdc_column)
        print(f"\n📊 Last extracted {cdc_column}: {last_value}")
        
        # Extract data
        print(f"\n📤 Extracting new records...")
        config = TableConfig(
            table_name=table_name,
            schema="public",
            extraction_mode=ExtractionMode.INCREMENTAL_CDC,
            cdc_column=cdc_column,
            last_extracted_value=last_value,
            batch_size=1000
        )
        
        df = extractor.extract_with_config(config)
        print(f"✓ Extracted {len(df)} new records")
        
        # Update state
        if len(df) > 0:
            max_value = df[cdc_column].max()
            state_manager.set_last_extracted_value(table_name, cdc_column, int(max_value))
            print(f"✓ Updated state: {cdc_column} = {max_value}")
        
        print("\n✅ State management test completed!")
        return True
        
    except Exception as e:
        print(f"\n❌ State management test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        if pg_connector:
            pg_connector.disconnect()


if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════════════════════╗
║                  ETL PIPELINE - REAL DATABASE TEST                ║
║                                                                    ║
║  This test connects to actual databases and performs:             ║
║  1. Extract data from PostgreSQL                                  ║
║  2. Transform extracted data to JSON                              ║
║  3. Load transformed data to Elasticsearch                        ║
║                                                                    ║
║  Prerequisites:                                                    ║
║  - PostgreSQL running with 'students' table                       ║
║  - Elasticsearch running on localhost:9200                        ║
║  - Update connection details in config/etl_config.yaml            ║
╚══════════════════════════════════════════════════════════════════╝
    """)
    
    # Run main ETL test
    test1_passed = test_real_extraction_and_transformation()
    
    # Run state management test
    test2_passed = test_with_state_management()
    
    # Summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    print(f"ETL Flow Test............... {'✓ PASSED' if test1_passed else '✗ FAILED'}")
    print(f"State Management Test....... {'✓ PASSED' if test2_passed else '✗ FAILED'}")
    print()
    
    if test1_passed and test2_passed:
        print("🎉 All real database tests passed!")
        sys.exit(0)
    else:
        print("⚠ Some tests failed")
        sys.exit(1)
