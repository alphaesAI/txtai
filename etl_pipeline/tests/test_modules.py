"""
Basic module import and structure tests.
Tests module loading without requiring database connections.
"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from datetime import datetime


def test_connector_imports():
    """Test connector module imports."""
    print("\n=== Testing Connector Module ===")
    
    try:
        from connector import (
            BaseConnector,
            ConnectionManager,
            PostgresConnector,
            ElasticsearchConnector,
            ConnectorFactory,
            ConnectorType
        )
        print("✓ All connector imports successful")
        
        # Test ConnectionManager singleton
        manager1 = ConnectionManager()
        manager2 = ConnectionManager()
        assert manager1 is manager2, "ConnectionManager should be singleton"
        print("✓ ConnectionManager singleton pattern works")
        
        # Test factory registration
        supported = ConnectorFactory.list_supported_connectors()
        print(f"✓ Supported connectors: {supported}")
        
        return True
    except Exception as e:
        print(f"✗ Connector import failed: {e}")
        return False


def test_extractor_imports():
    """Test extractor module imports."""
    print("\n=== Testing Extractor Module ===")
    
    try:
        from extractor import (
            BaseExtractor,
            TableConfig,
            ExtractionConfig,
            ExtractionMode,
            PostgresExtractor,
            StateManager,
            ExtractorFactory,
            ExtractorType
        )
        print("✓ All extractor imports successful")
        
        # Test TableConfig creation
        config = TableConfig(
            table_name="test_table",
            columns=["id", "name"],
            extraction_mode=ExtractionMode.FULL
        )
        print(f"✓ TableConfig created: {config.table_name}")
        
        # Test state manager (without file)
        import tempfile
        state_file = tempfile.mktemp(suffix=".json")
        state_mgr = StateManager(state_file)
        state_mgr.set_last_extracted_value("test_table", "id", 100)
        value = state_mgr.get_last_extracted_value("test_table", "id")
        assert value == 100, "State manager should store and retrieve values"
        print("✓ StateManager works correctly")
        
        return True
    except Exception as e:
        print(f"✗ Extractor import failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_transformer_imports():
    """Test transformer module imports."""
    print("\n=== Testing Transformer Module ===")
    
    try:
        from transformer import (
            BaseTransformer,
            JSONTransformer,
            TransformerFactory,
            TransformerType
        )
        print("✓ All transformer imports successful")
        
        # Test JSON transformer with sample data
        transformer = TransformerFactory.create_transformer("json")
        print("✓ JSONTransformer created via factory")
        
        # Test transformation
        test_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'created_at': [datetime.now(), datetime.now(), datetime.now()]
        })
        
        json_data = transformer.transform(test_df)
        print(f"✓ Transformed {len(json_data)} records to JSON")
        print(f"  Sample: {json_data[0]}")
        
        return True
    except Exception as e:
        print(f"✗ Transformer import failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_loader_imports():
    """Test loader module imports."""
    print("\n=== Testing Loader Module ===")
    
    try:
        from loader import (
            BaseLoader,
            ElasticsearchLoader,
            LoaderFactory,
            LoaderType
        )
        print("✓ All loader imports successful")
        
        # Test factory
        supported = [lt.value for lt in LoaderType]
        print(f"✓ Supported loaders: {supported}")
        
        return True
    except Exception as e:
        print(f"✗ Loader import failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_factory_patterns():
    """Test factory pattern implementations."""
    print("\n=== Testing Factory Patterns ===")
    
    try:
        from connector import ConnectorFactory
        from extractor import ExtractorFactory
        from transformer import TransformerFactory
        from loader import LoaderFactory
        
        # Test that factories have proper methods
        assert hasattr(ConnectorFactory, 'create_connector')
        assert hasattr(ExtractorFactory, 'create_extractor')
        assert hasattr(TransformerFactory, 'create_transformer')
        assert hasattr(LoaderFactory, 'create_loader')
        print("✓ All factories have create methods")
        
        # Test that they raise errors for invalid types
        try:
            ConnectorFactory.create_connector("invalid_type", "connection_string")
            print("✗ Factory should raise error for invalid type")
            return False
        except ValueError:
            print("✓ Factories properly validate connector types")
        
        return True
    except Exception as e:
        print(f"✗ Factory pattern test failed: {e}")
        return False


def test_configuration_loading():
    """Test configuration file loading."""
    print("\n=== Testing Configuration ===")
    
    try:
        import yaml
        from pathlib import Path
        
        config_path = Path(__file__).parent.parent / "config" / "etl_config.yaml"
        
        if config_path.exists():
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            print("✓ Configuration file loaded")
            print(f"  DAG ID: {config['airflow']['dag_id']}")
            print(f"  Tables configured: {len(config['extraction']['tables'])}")
            
            # Validate structure
            assert 'postgres' in config
            assert 'elasticsearch' in config
            assert 'extraction' in config
            assert 'transformation' in config
            assert 'loading' in config
            assert 'airflow' in config
            print("✓ Configuration structure is valid")
            
            return True
        else:
            print("⚠ Configuration file not found (optional)")
            return True
            
    except Exception as e:
        print(f"✗ Configuration test failed: {e}")
        return False


def run_all_tests():
    """Run all tests."""
    print("=" * 60)
    print("ETL PIPELINE MODULE TESTS")
    print("=" * 60)
    
    results = {
        'Connector Module': test_connector_imports(),
        'Extractor Module': test_extractor_imports(),
        'Transformer Module': test_transformer_imports(),
        'Loader Module': test_loader_imports(),
        'Factory Patterns': test_factory_patterns(),
        'Configuration': test_configuration_loading(),
    }
    
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    for test_name, passed in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"{test_name:.<40} {status}")
    
    total = len(results)
    passed = sum(results.values())
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed successfully!")
        return 0
    else:
        print(f"\n⚠ {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    exit_code = run_all_tests()
    sys.exit(exit_code)
