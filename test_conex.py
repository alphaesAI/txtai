import logging
from connectors.factory import ConnectorFactory
from extractors.factory import ExtractorFactory
import yaml

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
CONFIG_PATH = "config.yaml"
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

def test_connectors():
    """
    Test all registered connectors
    """
    logger.info("=== Testing Connectors ===")
    for connector_name, connector_cls in ConnectorFactory.list_connectors().items():
        logger.info(f"Testing connector: {connector_name}")
        try:
            # Get connector config if exists
            connector_config = config.get(connector_name, {})
            connector = ConnectorFactory.create(connector_name, connector_config)
            
            # Try connecting
            if hasattr(connector, "connect"):
                connector.connect()
                logger.info(f"Connected successfully: {connector_name}")
                connector.disconnect()
            else:
                logger.info(f"{connector_name} does not have a connect method, skipped.")
        
        except Exception as e:
            logger.error(f"Failed connector {connector_name}: {e}")


def test_extractors():
    """
    Test all registered extractors
    """
    logger.info("=== Testing Extractors ===")
    for extractor_name, extractor_cls in ExtractorFactory.list_extractors().items():
        logger.info(f"Testing extractor: {extractor_name}")
        try:
            # Use default config (connector=None for file extractors)
            if extractor_name.lower() in ["postgres"]:
                # Pass postgres config
                extractor_config = config.get("postgres", {})
            else:
                extractor_config = {}
            
            extractor = ExtractorFactory.create(extractor_name, extractor_config)
            
            # Test schema
            if hasattr(extractor, "get_schema"):
                try:
                    schema = extractor.get_schema("test_table")
                    logger.info(f"{extractor_name} schema fetched: {schema}")
                except Exception:
                    logger.info(f"{extractor_name} schema fetch skipped (table may not exist).")
            
            # Test extract (without query)
            if hasattr(extractor, "extract"):
                try:
                    for i, row in enumerate(extractor.extract("test_table")):
                        logger.info(f"{extractor_name} extracted row: {row}")
                        if i > 2:  # limit output
                            break
                except Exception:
                    logger.info(f"{extractor_name} extract skipped (table/file may not exist).")
        
        except Exception as e:
            logger.error(f"Failed extractor {extractor_name}: {e}")


if __name__ == "__main__":
    test_connectors()
    test_extractors()
