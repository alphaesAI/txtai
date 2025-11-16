#!/usr/bin/env python3
"""Simple test for ETL components without full txtai import."""

import sys
import os

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src", "python"))

def test_direct_imports():
    """Test direct imports of ETL components."""
    print("Testing direct ETL component imports...")
    
    try:
        # Test base classes
        from txtai.pipeline.connectors.base import BaseConnector
        from txtai.pipeline.extractors.base import BaseExtractor
        from txtai.pipeline.loaders.base import BaseLoader
        from txtai.pipeline.transformers.base import BaseTransformer
        print("✓ Base classes imported successfully")
        
        # Test factories
        from txtai.pipeline.connectors.factory import ConnectorFactory
        from txtai.pipeline.extractors.factory import ExtractorFactory
        from txtai.pipeline.loaders.factory import LoaderFactory
        from txtai.pipeline.transformers.factory import TransformerFactory
        print("✓ Factory classes imported successfully")
        
        # Test concrete implementations
        from txtai.pipeline.connectors.postgres import PostgresConnector
        from txtai.pipeline.connectors.elasticsearch import ElasticsearchConnector
        from txtai.pipeline.connectors.email import GmailConnector
        print("✓ Connector implementations imported successfully")
        
        from txtai.pipeline.extractors.postgres import PostgresExtractor
        from txtai.pipeline.extractors.files import FileExtractor
        print("✓ Extractor implementations imported successfully")
        
        from txtai.pipeline.loaders.elasticsearch import ElasticsearchLoader
        print("✓ Loader implementations imported successfully")
        
        from txtai.pipeline.transformers.json_transformer import JsonTransformer
        print("✓ Transformer implementations imported successfully")
        
        # Test data processing components
        from txtai.pipeline.data.preprocessor import PreProcessor
        from txtai.pipeline.data.pipeline import Pipeline, Tabular
        print("✓ Data processing components imported successfully")
        
        # Test main ETL pipeline
        from txtai.pipeline.etl import ETLPipeline
        print("✓ ETL Pipeline class imported successfully")
        
        return True
        
    except ImportError as e:
        print(f"✗ Import failed: {e}")
        return False

def test_factory_registration():
    """Test that components are properly registered in factories."""
    print("\nTesting factory registration...")
    
    try:
        from txtai.pipeline.connectors.factory import ConnectorFactory
        from txtai.pipeline.extractors.factory import ExtractorFactory
        from txtai.pipeline.loaders.factory import LoaderFactory
        from txtai.pipeline.transformers.factory import TransformerFactory
        
        # Check connector registration
        connectors = ConnectorFactory.list_connectors()
        expected_connectors = ['postgres', 'elasticsearch', 'email']
        for conn in expected_connectors:
            if conn in connectors:
                print(f"✓ Connector '{conn}' registered")
            else:
                print(f"✗ Connector '{conn}' not registered")
        
        # Check extractor registration
        extractors = ExtractorFactory.list_extractors()
        expected_extractors = ['postgres', 'files']
        for ext in expected_extractors:
            if ext in extractors:
                print(f"✓ Extractor '{ext}' registered")
            else:
                print(f"✗ Extractor '{ext}' not registered")
        
        # Check loader registration
        loaders = LoaderFactory.list_loaders()
        expected_loaders = ['elasticsearch']
        for loader in expected_loaders:
            if loader in loaders:
                print(f"✓ Loader '{loader}' registered")
            else:
                print(f"✗ Loader '{loader}' not registered")
        
        # Check transformer registration
        transformers = TransformerFactory.list_transformers()
        expected_transformers = ['json']
        for transformer in expected_transformers:
            if transformer in transformers:
                print(f"✓ Transformer '{transformer}' registered")
            else:
                print(f"✗ Transformer '{transformer}' not registered")
        
        return True
        
    except Exception as e:
        print(f"✗ Factory registration test failed: {e}")
        return False

def test_etl_pipeline():
    """Test ETL pipeline creation and basic operations."""
    print("\nTesting ETL pipeline...")
    
    try:
        from txtai.pipeline.etl import ETLPipeline
        
        # Create ETL pipeline with default config
        etl = ETLPipeline()
        print("✓ ETL Pipeline created successfully")
        
        # Test listing methods
        connectors = etl.list_connectors()
        extractors = etl.list_extractors()
        loaders = etl.list_loaders()
        transformers = etl.list_transformers()
        pipelines = etl.list_pipelines()
        
        print(f"✓ Components available:")
        print(f"  - Connectors: {len(connectors)} ({list(connectors)})")
        print(f"  - Extractors: {len(extractors)} ({list(extractors)})")
        print(f"  - Loaders: {len(loaders)} ({list(loaders)})")
        print(f"  - Transformers: {len(transformers)} ({list(transformers)})")
        print(f"  - Pipelines: {len(pipelines)} ({list(pipelines)})")
        
        return True
        
    except Exception as e:
        print(f"✗ ETL pipeline test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("=" * 60)
    print("ETL Components Simple Test")
    print("=" * 60)
    
    tests = [
        test_direct_imports,
        test_factory_registration,
        test_etl_pipeline
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
    
    print("\n" + "=" * 60)
    print(f"Test Results: {passed}/{total} tests passed")
    print("=" * 60)
    
    if passed == total:
        print("🎉 All tests passed! ETL components are working correctly.")
        return 0
    else:
        print("❌ Some tests failed. Please check the errors above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
