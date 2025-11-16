"""ETL Pipeline integration with txtai pipeline factory."""

import yaml
import logging
from typing import Any, Dict, List, Optional, Iterator
from pathlib import Path

from .connectors.factory import ConnectorFactory
from .extractors.factory import ExtractorFactory
from .loaders.factory import LoaderFactory
from .transformers.factory import TransformerFactory


class ETLPipeline:
    """
    ETL Pipeline class that integrates with txtai pipeline factory.
    Supports extract-transform-load operations with configurable components.
    """

    def __init__(self, config_path: Optional[str] = None, config: Optional[Dict[str, Any]] = None):
        """
        Initialize ETL Pipeline.
        
        Args:
            config_path: Path to configuration file
            config: Configuration dictionary (overrides config_path)
        """
        self.logger = logging.getLogger(__name__)
        
        # Load configuration
        if config:
            self.config = config
        elif config_path:
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
        else:
            # Default config path
            default_path = Path(__file__).parent.parent.parent.parent / "pipeline_config.yaml"
            if default_path.exists():
                with open(default_path, 'r') as f:
                    self.config = yaml.safe_load(f)
            else:
                self.config = self._get_default_config()
        
        # Initialize components
        self.connectors = {}
        self.extractors = {}
        self.loaders = {}
        self.transformers = {}
        
        # Statistics
        self.stats = {
            "total_processed": 0,
            "total_success": 0,
            "total_failed": 0,
            "start_time": None,
            "end_time": None
        }
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration."""
        return {
            "settings": {
                "log_level": "INFO",
                "batch_size": 1000,
                "max_retries": 3,
                "timeout": 30
            },
            "connectors": {},
            "extractors": {},
            "loaders": {},
            "transformers": {},
            "pipelines": {}
        }
    
    def get_connector(self, name: str) -> Any:
        """Get or create a connector instance."""
        if name not in self.connectors:
            if name not in self.config.get("connectors", {}):
                raise ValueError(f"Connector '{name}' not found in configuration")
            
            connector_config = self.config["connectors"][name]
            connector_type = connector_config["type"]
            connector_params = {k: v for k, v in connector_config.items() if k != "type"}
            
            self.connectors[name] = ConnectorFactory.create(connector_type, connector_params)
        
        return self.connectors[name]
    
    def get_extractor(self, name: str) -> Any:
        """Get or create an extractor instance."""
        if name not in self.extractors:
            if name not in self.config.get("extractors", {}):
                raise ValueError(f"Extractor '{name}' not found in configuration")
            
            extractor_config = self.config["extractors"][name]
            extractor_type = extractor_config["type"]
            extractor_params = {k: v for k, v in extractor_config.items() if k != "type"}
            
            # Get connector if specified
            if "connector" in extractor_params:
                connector_name = extractor_params.pop("connector")
                connector = self.get_connector(connector_name)
                extractor_params["connector"] = connector
            
            self.extractors[name] = ExtractorFactory.create(extractor_type, extractor_params)
        
        return self.extractors[name]
    
    def get_loader(self, name: str) -> Any:
        """Get or create a loader instance."""
        if name not in self.loaders:
            if name not in self.config.get("loaders", {}):
                raise ValueError(f"Loader '{name}' not found in configuration")
            
            loader_config = self.config["loaders"][name]
            loader_type = loader_config["type"]
            loader_params = {k: v for k, v in loader_config.items() if k != "type"}
            
            # Get connector if specified
            if "connector" in loader_params:
                connector_name = loader_params.pop("connector")
                connector = self.get_connector(connector_name)
                loader_params["connector"] = connector
            
            self.loaders[name] = LoaderFactory.create(loader_type, loader_params)
        
        return self.loaders[name]
    
    def get_transformer(self, name: str) -> Any:
        """Get or create a transformer instance."""
        if name not in self.transformers:
            if name not in self.config.get("transformers", {}):
                raise ValueError(f"Transformer '{name}' not found in configuration")
            
            transformer_config = self.config["transformers"][name]
            transformer_type = transformer_config["type"]
            transformer_params = {k: v for k, v in transformer_config.items() if k != "type"}
            
            self.transformers[name] = TransformerFactory.create(transformer_type, transformer_params)
        
        return self.transformers[name]
    
    def run_pipeline(self, pipeline_name: str, **kwargs) -> Dict[str, Any]:
        """
        Run a configured pipeline.
        
        Args:
            pipeline_name: Name of the pipeline to run
            **kwargs: Additional pipeline parameters
            
        Returns:
            Pipeline execution results
        """
        if pipeline_name not in self.config.get("pipelines", {}):
            raise ValueError(f"Pipeline '{pipeline_name}' not found in configuration")
        
        pipeline_config = self.config["pipelines"][pipeline_name]
        steps = pipeline_config.get("steps", [])
        
        self.logger.info(f"Running pipeline: {pipeline_name}")
        self.stats["start_time"] = None  # Will be set when processing starts
        
        results = []
        current_data = None
        
        for i, step in enumerate(steps):
            step_name = step.get("name", f"step_{i}")
            step_type = step.get("type")
            step_config = step.get("config")
            
            self.logger.info(f"Executing step {i+1}/{len(steps)}: {step_name} ({step_type})")
            
            try:
                if step_type == "extractor":
                    extractor = self.get_extractor(step_config)
                    source = kwargs.get("source", step.get("source"))
                    query = kwargs.get("query", step.get("query"))
                    current_data = extractor.extract(source, query=query, **kwargs)
                
                elif step_type == "transformer":
                    transformer = self.get_transformer(step_config)
                    if current_data is not None:
                        import pandas as pd
                        if isinstance(current_data, Iterator):
                            current_data = list(current_data)
                        if isinstance(current_data, list):
                            current_data = pd.DataFrame(current_data)
                        current_data = transformer.transform(current_data, **kwargs)
                
                elif step_type == "loader":
                    loader = self.get_loader(step_config)
                    if current_data is not None:
                        if isinstance(current_data, list):
                            success = loader.batch_load(current_data, **kwargs)
                        else:
                            success = loader.load(current_data, **kwargs)
                        results.append({"step": step_name, "success": success})
                
                elif step_type == "preprocessor":
                    from .data.preprocessor import PreProcessor
                    preprocessor = PreProcessor(step_config, **kwargs)
                    current_data = preprocessor.process()
                
                else:
                    raise ValueError(f"Unknown step type: {step_type}")
                
                results.append({"step": step_name, "status": "completed"})
                
            except Exception as e:
                self.logger.error(f"Error in step {step_name}: {e}")
                results.append({"step": step_name, "status": "failed", "error": str(e)})
                break
        
        self.stats["end_time"] = None  # Will be set when processing ends
        
        return {
            "pipeline": pipeline_name,
            "steps": results,
            "stats": self.stats
        }
    
    def extract_transform_load(
        self,
        extractor_name: str,
        loader_name: str,
        source: str,
        transformer_name: Optional[str] = None,
        query: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Perform extract-transform-load operation.
        
        Args:
            extractor_name: Name of extractor to use
            transformer_name: Name of transformer to use (optional)
            loader_name: Name of loader to use
            source: Data source
            query: Optional query/filter
            **kwargs: Additional parameters
            
        Returns:
            ETL operation results
        """
        self.logger.info(f"Starting ETL: {extractor_name} -> {transformer_name or 'none'} -> {loader_name}")
        
        try:
            # Extract
            extractor = self.get_extractor(extractor_name)
            data = extractor.extract(source, query=query, **kwargs)
            
            # Transform (optional)
            if transformer_name:
                transformer = self.get_transformer(transformer_name)
                import pandas as pd
                if isinstance(data, Iterator):
                    data = list(data)
                if isinstance(data, list):
                    data = pd.DataFrame(data)
                data = transformer.transform(data, **kwargs)
            
            # Load
            loader = self.get_loader(loader_name)
            if isinstance(data, list):
                success = loader.batch_load(data, **kwargs)
            else:
                success = loader.load(data, **kwargs)
            
            return {
                "status": "success",
                "extractor": extractor_name,
                "transformer": transformer_name,
                "loader": loader_name,
                "source": source,
                "records_processed": len(data) if isinstance(data, list) else 1,
                "success": success
            }
            
        except Exception as e:
            self.logger.error(f"ETL operation failed: {e}")
            return {
                "status": "failed",
                "error": str(e),
                "extractor": extractor_name,
                "transformer": transformer_name,
                "loader": loader_name,
                "source": source
            }
    
    def list_connectors(self) -> List[str]:
        """List available connectors."""
        return list(self.config.get("connectors", {}).keys())
    
    def list_extractors(self) -> List[str]:
        """List available extractors."""
        return list(self.config.get("extractors", {}).keys())
    
    def list_loaders(self) -> List[str]:
        """List available loaders."""
        return list(self.config.get("loaders", {}).keys())
    
    def list_transformers(self) -> List[str]:
        """List available transformers."""
        return list(self.config.get("transformers", {}).keys())
    
    def list_pipelines(self) -> List[str]:
        """List available pipelines."""
        return list(self.config.get("pipelines", {}).keys())
    
    def get_pipeline_info(self, pipeline_name: str) -> Dict[str, Any]:
        """Get information about a specific pipeline."""
        if pipeline_name not in self.config.get("pipelines", {}):
            raise ValueError(f"Pipeline '{pipeline_name}' not found")
        
        pipeline_config = self.config["pipelines"][pipeline_name]
        return {
            "name": pipeline_name,
            "description": pipeline_config.get("description", ""),
            "steps": pipeline_config.get("steps", [])
        }
