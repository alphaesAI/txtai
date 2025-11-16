#!/usr/bin/env python3
"""Test script for ETL pipeline integration with txtai."""

import sys
import os
import logging

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src", "python"))

def test_imports():
    """Test that all ETL components can be imported."""
    print("Testing imports...")
    
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
        
        # Test txtai integration
        from txtai.pipeline import ETLPipeline as TxtaiETLPipeline
        print("✓ txtai pipeline integration imported successfully")
        
        return True
        
    except ImportError as e:
        print(f"✗ Import failed: {e}")
        return False

def test_factories():
    """Test factory registration and creation."""
    print("\nTesting factories...")
    
    try:
        from txtai.pipeline.connectors.factory import ConnectorFactory
        from txtai.pipeline.extractors.factory import ExtractorFactory
        from txtai.pipeline.loaders.factory import LoaderFactory
        from txtai.pipeline.transformers.factory import TransformerFactory
        
        # Test connector factory
        connectors = ConnectorFactory.list_connectors()
        print(f"✓ Available connectors: {list(connectors.keys())}")
        
        # Test extractor factory
        extractors = ExtractorFactory.list_extractors()
        print(f"✓ Available extractors: {list(extractors.keys())}")
        
        # Test loader factory
        loaders = LoaderFactory.list_loaders()
        print(f"✓ Available loaders: {list(loaders.keys())}")
        
        # Test transformer factory
        transformers = TransformerFactory.list_transformers()
        print(f"✓ Available transformers: {list(transformers.keys())}")
        
        return True
        
    except Exception as e:
        print(f"✗ Factory test failed: {e}")
        return False

def test_etl_pipeline_init():
    """Test ETL pipeline initialization."""
    print("\nTesting ETL pipeline initialization...")
    
    try:
        from txtai.pipeline.etl import ETLPipeline
        
        # Test with default config
        etl = ETLPipeline()
        print("✓ ETL Pipeline initialized with default config")
        
        # Test listing available components
        connectors = etl.list_connectors()
        extractors = etl.list_extractors()
        loaders = etl.list_loaders()
        transformers = etl.list_transformers()
        pipelines = etl.list_pipelines()
        
        print(f"✓ Listed components - Connectors: {len(connectors)}, Extractors: {len(extractors)}, Loaders: {len(loaders)}, Transformers: {len(transformers)}, Pipelines: {len(pipelines)}")
        
        return True
        
    except Exception as e:
        print(f"✗ ETL pipeline initialization failed: {e}")
        return False

def test_config_loading():
    """Test configuration loading."""
    print("\nTesting configuration loading...")
    
    try:
        from txtai.pipeline.etl import ETLPipeline
        
        # Test loading from config file
        config_path = os.path.join(os.path.dirname(__file__), "pipeline_config.yaml")
        if os.path.exists(config_path):
            etl = ETLPipeline(config_path=config_path)
            print("✓ Configuration loaded from file")
            
            # Test pipeline info
            pipelines = etl.list_pipelines()
            if pipelines:
                for pipeline_name in pipelines:
                    info = etl.get_pipeline_info(pipeline_name)
                    print(f"✓ Pipeline '{pipeline_name}': {len(info['steps'])} steps")
        else:
            print("⚠ Config file not found, using default config")
        
        return True
        
    except Exception as e:
        print(f"✗ Configuration loading failed: {e}")
        return False

def main():
    """Run all tests."""
    print("=" * 60)
    print("ETL Pipeline Integration Test")
    print("=" * 60)
    
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    tests = [
        test_imports,
        test_factories,
        test_etl_pipeline_init,
        test_config_loading
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
        print("🎉 All tests passed! ETL pipeline integration successful.")
        return 0
    else:
        print("❌ Some tests failed. Please check the errors above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
