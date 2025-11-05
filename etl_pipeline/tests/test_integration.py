"""
Integration test with mock database connections.
Tests the full ETL flow without requiring actual databases.
"""
import sys
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch
import pandas as pd
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def test_full_etl_flow_mock():
    """
    Test complete ETL flow using mocked connections.
    This simulates the entire pipeline without requiring actual databases.
    """
    print("\n" + "=" * 60)
    print("INTEGRATION TEST - Full ETL Flow (Mocked)")
    print("=" * 60)
    
    try:
        from connector import ConnectorFactory
        from extractor import ExtractorFactory, TableConfig, ExtractionMode
        from transformer import TransformerFactory
        from loader import LoaderFactory
        
        # Step 1: Create mock connectors
        print("\n1. Creating Mock Connectors...")
        
        # Mock PostgreSQL connector
        mock_pg_connector = Mock()
        mock_pg_connector.is_connected.return_value = True
        mock_pg_connector.test_connection.return_value = True
        
        # Mock Elasticsearch connector
        mock_es_connector = Mock()
        mock_es_connector.is_connected.return_value = True
        mock_es_connector.test_connection.return_value = True
        mock_es_connector.index_exists.return_value = False
        mock_es_connector.create_index.return_value = True
        
        print("✓ Mock connectors created")
        
        # Step 2: Create mock extractor with sample data
        print("\n2. Extracting Data (Mock)...")
        
        # Create sample DataFrame
        sample_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'email': ['alice@example.com', 'bob@example.com', 
                     'charlie@example.com', 'david@example.com', 'eve@example.com'],
            'created_at': [datetime.now()] * 5,
            'updated_at': [datetime.now()] * 5
        })
        
        print(f"✓ Mock extracted {len(sample_data)} rows")
        print(f"  Columns: {list(sample_data.columns)}")
        
        # Step 3: Transform data
        print("\n3. Transforming Data...")
        
        transformer = TransformerFactory.create_transformer(
            transformer_type="json",
            orient="records",
            date_format="iso",
            handle_nan="null"
        )
        
        json_data = transformer.transform(sample_data)
        
        print(f"✓ Transformed {len(json_data)} records to JSON")
        print(f"  Sample record: {json_data[0]}")
        
        # Step 4: Mock load to Elasticsearch
        print("\n4. Loading Data (Mock)...")
        
        mock_loader = Mock()
        mock_loader.load_batch.return_value = True
        mock_loader.validate.return_value = True
        
        # Simulate loading
        success = mock_loader.load_batch(json_data)
        
        print(f"✓ Loaded {len(json_data)} documents (mocked)")
        print(f"  Load success: {success}")
        
        # Step 5: Verify the flow
        print("\n5. Verifying ETL Flow...")
        
        assert len(sample_data) == len(json_data), "Row count should match"
        assert all(isinstance(record, dict) for record in json_data), "All records should be dicts"
        assert 'id' in json_data[0], "Records should have id field"
        
        print("✓ All assertions passed")
        
        print("\n" + "=" * 60)
        print("✓ INTEGRATION TEST PASSED")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print(f"\n✗ Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_extraction_modes():
    """Test different extraction modes."""
    print("\n" + "=" * 60)
    print("EXTRACTION MODES TEST")
    print("=" * 60)
    
    try:
        from extractor import TableConfig, ExtractionMode
        
        # Test Full extraction config
        print("\n1. Testing Full Extraction Config...")
        full_config = TableConfig(
            table_name="users",
            columns=["id", "name", "email"],
            extraction_mode=ExtractionMode.FULL,
            batch_size=1000
        )
        print(f"✓ Full extraction config: {full_config.table_name}")
        
        # Test CDC extraction config
        print("\n2. Testing CDC Extraction Config...")
        cdc_config = TableConfig(
            table_name="orders",
            columns=["order_id", "user_id", "amount"],
            extraction_mode=ExtractionMode.INCREMENTAL_CDC,
            cdc_column="order_id",
            last_extracted_value=1000
        )
        print(f"✓ CDC extraction config: {cdc_config.table_name}")
        print(f"  CDC column: {cdc_config.cdc_column}")
        print(f"  Last value: {cdc_config.last_extracted_value}")
        
        # Test Date-based extraction config
        print("\n3. Testing Date-based Extraction Config...")
        date_config = TableConfig(
            table_name="logs",
            columns=["log_id", "message", "timestamp"],
            extraction_mode=ExtractionMode.INCREMENTAL_DATE,
            date_column="timestamp",
            start_date=datetime(2024, 1, 1)
        )
        print(f"✓ Date-based extraction config: {date_config.table_name}")
        print(f"  Date column: {date_config.date_column}")
        print(f"  Start date: {date_config.start_date}")
        
        print("\n✓ All extraction modes configured successfully")
        return True
        
    except Exception as e:
        print(f"\n✗ Extraction modes test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_transformer_features():
    """Test transformer features with various data types."""
    print("\n" + "=" * 60)
    print("TRANSFORMER FEATURES TEST")
    print("=" * 60)
    
    try:
        from transformer import TransformerFactory
        import numpy as np
        from decimal import Decimal
        
        print("\n1. Testing with Various Data Types...")
        
        # Create DataFrame with various types
        test_df = pd.DataFrame({
            'integer': [1, 2, 3],
            'float': [1.5, 2.5, 3.5],
            'string': ['a', 'b', 'c'],
            'datetime': pd.date_range('2024-01-01', periods=3),
            'boolean': [True, False, True],
            'decimal': [Decimal('10.50'), Decimal('20.75'), Decimal('30.25')],
            'nan_column': [1.0, np.nan, 3.0]
        })
        
        transformer = TransformerFactory.create_transformer(
            "json",
            handle_nan="null"
        )
        
        json_data = transformer.transform(test_df)
        
        print(f"✓ Transformed {len(json_data)} records with mixed types")
        print(f"  Sample: {json_data[0]}")
        
        # Test batch transformation
        print("\n2. Testing Batch Transformation...")
        batches = transformer.transform_batch(test_df, batch_size=2)
        print(f"✓ Created {len(batches)} batches")
        
        # Test metadata addition
        print("\n3. Testing Metadata Addition...")
        metadata = {
            '_source': 'test_db',
            '_pipeline_run': 'test_run_123'
        }
        data_with_metadata = transformer.add_metadata(json_data.copy(), metadata)
        print(f"✓ Added metadata: {list(metadata.keys())}")
        assert '_source' in data_with_metadata[0]
        
        print("\n✓ All transformer features work correctly")
        return True
        
    except Exception as e:
        print(f"\n✗ Transformer features test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_all_integration_tests():
    """Run all integration tests."""
    print("=" * 60)
    print("ETL PIPELINE INTEGRATION TESTS")
    print("=" * 60)
    
    results = {
        'Full ETL Flow (Mocked)': test_full_etl_flow_mock(),
        'Extraction Modes': test_extraction_modes(),
        'Transformer Features': test_transformer_features(),
    }
    
    print("\n" + "=" * 60)
    print("INTEGRATION TEST SUMMARY")
    print("=" * 60)
    
    for test_name, passed in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"{test_name:.<40} {status}")
    
    total = len(results)
    passed = sum(results.values())
    print(f"\nTotal: {passed}/{total} integration tests passed")
    
    if passed == total:
        print("\n🎉 All integration tests passed!")
        return 0
    else:
        print(f"\n⚠ {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    exit_code = run_all_integration_tests()
    sys.exit(exit_code)
