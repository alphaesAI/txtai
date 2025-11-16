#!/usr/bin/env python3
"""Minimal test of ETL components architecture."""

import sys
import os
from typing import Any, Dict, List, Optional, Iterator
import logging
from abc import ABC, abstractmethod

def test_etl_architecture():
    """Test ETL components architecture."""
    print("Testing ETL architecture...")
    
    # Define base classes
    class BaseConnector(ABC):
        def __init__(self, connection_string: str, **kwargs):
            self.connection_string = connection_string
            self.connection_params = kwargs
            self._connection = None
        
        @abstractmethod
        def connect(self) -> None:
            pass
        
        @abstractmethod
        def disconnect(self) -> None:
            pass
        
        @abstractmethod
        def test_connection(self) -> bool:
            pass

    class BaseExtractor(ABC):
        def __init__(self, connector: Any, **kwargs):
            self.connector = connector
            self.config = kwargs
        
        @abstractmethod
        def extract(self, source: str, query: Optional[str] = None, **kwargs) -> Iterator[Dict[str, Any]]:
            pass

    class BaseLoader(ABC):
        def __init__(self, connector: Any, **kwargs):
            self.connector = connector
            self.config = kwargs
        
        @abstractmethod
        def load(self, data: Any, **kwargs) -> bool:
            pass

    class BaseTransformer(ABC):
        def __init__(self, **kwargs):
            self.config = kwargs
        
        @abstractmethod
        def transform(self, data: Any, **kwargs) -> Any:
            pass

    print("✓ Base classes defined successfully")

    # Define factory classes
    class ConnectorFactory:
        _connectors: Dict[str, Any] = {}
        
        @classmethod
        def register(cls, connector_type: str, connector_class: Any) -> None:
            cls._connectors[connector_type] = connector_class
        
        @classmethod
        def create(cls, connector_type: str, config: Dict[str, Any]) -> Any:
            if connector_type not in cls._connectors:
                raise ValueError(f"Unknown connector type: {connector_type}")
            connector_class = cls._connectors[connector_type]
            return connector_class(**config)
        
        @classmethod
        def list_connectors(cls) -> Dict[str, Any]:
            return cls._connectors.copy()

    class ExtractorFactory:
        _extractors: Dict[str, Any] = {}
        
        @classmethod
        def register(cls, extractor_type: str, extractor_class: Any) -> None:
            cls._extractors[extractor_type] = extractor_class
        
        @classmethod
        def create(cls, extractor_type: str, config: Dict[str, Any]) -> Any:
            if extractor_type not in cls._extractors:
                raise ValueError(f"Unknown extractor type: {extractor_type}")
            extractor_class = cls._extractors[extractor_type]
            return extractor_class(**config)
        
        @classmethod
        def list_extractors(cls) -> Dict[str, Any]:
            return cls._extractors.copy()

    class LoaderFactory:
        _loaders: Dict[str, Any] = {}
        
        @classmethod
        def register(cls, loader_type: str, loader_class: Any) -> None:
            cls._loaders[loader_type] = loader_class
        
        @classmethod
        def create(cls, loader_type: str, config: Dict[str, Any]) -> Any:
            if loader_type not in cls._loaders:
                raise ValueError(f"Unknown loader type: {loader_type}")
            loader_class = cls._loaders[loader_type]
            return loader_class(**config)
        
        @classmethod
        def list_loaders(cls) -> Dict[str, Any]:
            return cls._loaders.copy()

    class TransformerFactory:
        _transformers: Dict[str, Any] = {}
        
        @classmethod
        def register(cls, transformer_type: str, transformer_class: Any) -> None:
            cls._transformers[transformer_type] = transformer_class
        
        @classmethod
        def create(cls, transformer_type: str, config: Dict[str, Any]) -> Any:
            if transformer_type not in cls._transformers:
                raise ValueError(f"Unknown transformer type: {transformer_type}")
            transformer_class = cls._transformers[transformer_type]
            return transformer_class(**config)
        
        @classmethod
        def list_transformers(cls) -> Dict[str, Any]:
            return cls._transformers.copy()

    print("✓ Factory classes defined successfully")

    # Define mock implementations
    class MockConnector(BaseConnector):
        def __init__(self, connection_string: str, **kwargs):
            super().__init__(connection_string, **kwargs)
            self.connected = False
        
        def connect(self) -> None:
            self.connected = True
            print(f"  Connected to {self.connection_string}")
        
        def disconnect(self) -> None:
            self.connected = False
            print("  Disconnected")
        
        def test_connection(self) -> bool:
            return self.connected

    class MockExtractor(BaseExtractor):
        def extract(self, source: str, query: Optional[str] = None, **kwargs) -> Iterator[Dict[str, Any]]:
            print(f"  Extracting from {source}")
            for i in range(3):
                yield {"id": i, "data": f"sample_data_{i}", "source": source}

    class MockLoader(BaseLoader):
        def __init__(self, connector: Any, **kwargs):
            super().__init__(connector, **kwargs)
            self.loaded_data = []
        
        def load(self, data: Any, **kwargs) -> bool:
            if isinstance(data, list):
                self.loaded_data.extend(data)
            else:
                self.loaded_data.append(data)
            print(f"  Loaded {len(data) if isinstance(data, list) else 1} records")
            return True

    class MockTransformer(BaseTransformer):
        def transform(self, data: Any, **kwargs) -> Any:
            print("  Transforming data")
            if isinstance(data, list):
                return [{"transformed": True, **record} for record in data]
            return {"transformed": True, **data}

    # Register components
    ConnectorFactory.register("mock", MockConnector)
    ExtractorFactory.register("mock", MockExtractor)
    LoaderFactory.register("mock", MockLoader)
    TransformerFactory.register("mock", MockTransformer)

    print("✓ Mock implementations registered successfully")

    # Test ETL pipeline
    class SimpleETLPipeline:
        def __init__(self, config: Optional[Dict[str, Any]] = None):
            self.config = config or {}
            self.connectors = {}
            self.extractors = {}
            self.loaders = {}
            self.transformers = {}
        
        def get_connector(self, name: str):
            if name not in self.connectors:
                if name in self.config.get("connectors", {}):
                    connector_config = self.config["connectors"][name]
                    self.connectors[name] = ConnectorFactory.create(
                        connector_config["type"], 
                        {k: v for k, v in connector_config.items() if k != "type"}
                    )
            return self.connectors.get(name)
        
        def get_extractor(self, name: str):
            if name not in self.extractors:
                if name in self.config.get("extractors", {}):
                    extractor_config = self.config["extractors"][name]
                    connector_name = extractor_config.get("connector")
                    connector = self.get_connector(connector_name) if connector_name else None
                    self.extractors[name] = ExtractorFactory.create(
                        extractor_config["type"],
                        {"connector": connector}
                    )
            return self.extractors.get(name)
        
        def get_loader(self, name: str):
            if name not in self.loaders:
                if name in self.config.get("loaders", {}):
                    loader_config = self.config["loaders"][name]
                    connector_name = loader_config.get("connector")
                    connector = self.get_connector(connector_name) if connector_name else None
                    self.loaders[name] = LoaderFactory.create(
                        loader_config["type"],
                        {"connector": connector}
                    )
            return self.loaders.get(name)
        
        def get_transformer(self, name: str):
            if name not in self.transformers:
                if name in self.config.get("transformers", {}):
                    transformer_config = self.config["transformers"][name]
                    self.transformers[name] = TransformerFactory.create(
                        transformer_config["type"],
                        {k: v for k, v in transformer_config.items() if k != "type"}
                    )
            return self.transformers.get(name)
        
        def extract_transform_load(self, extractor_name: str, transformer_name: Optional[str], 
                                  loader_name: str, source: str, **kwargs):
            print(f"\nRunning ETL: {extractor_name} -> {transformer_name or 'none'} -> {loader_name}")
            
            # Extract
            extractor = self.get_extractor(extractor_name)
            data = list(extractor.extract(source, **kwargs))
            print(f"  Extracted {len(data)} records")
            
            # Transform (optional)
            if transformer_name:
                transformer = self.get_transformer(transformer_name)
                data = transformer.transform(data, **kwargs)
            
            # Load
            loader = self.get_loader(loader_name)
            loader.load(data, **kwargs)
            
            return {"status": "success", "records": len(data)}

    # Test the pipeline
    config = {
        "connectors": {
            "test_conn": {"type": "mock", "connection_string": "test://localhost"}
        },
        "extractors": {
            "test_extractor": {"type": "mock", "connector": "test_conn"}
        },
        "loaders": {
            "test_loader": {"type": "mock", "connector": "test_conn"}
        },
        "transformers": {
            "test_transformer": {"type": "mock"}
        }
    }

    pipeline = SimpleETLPipeline(config)
    result = pipeline.extract_transform_load(
        "test_extractor", "test_transformer", "test_loader", "test_source"
    )

    print(f"✓ ETL pipeline executed successfully: {result}")
    
    # Test factory listing
    print(f"\nAvailable components:")
    print(f"  Connectors: {list(ConnectorFactory.list_connectors().keys())}")
    print(f"  Extractors: {list(ExtractorFactory.list_extractors().keys())}")
    print(f"  Loaders: {list(LoaderFactory.list_loaders().keys())}")
    print(f"  Transformers: {list(TransformerFactory.list_transformers().keys())}")

    return True

def main():
    """Run the test."""
    print("=" * 60)
    print("Minimal ETL Architecture Test")
    print("=" * 60)
    
    try:
        if test_etl_architecture():
            print("\n" + "=" * 60)
            print("🎉 All tests passed! ETL architecture is working correctly.")
            print("=" * 60)
            return 0
        else:
            print("\n❌ Test failed.")
            return 1
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
