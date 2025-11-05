# PostgreSQL to Elasticsearch ETL Pipeline

A production-ready, modular ETL pipeline for extracting data from PostgreSQL databases, transforming it to JSON format, and loading it to Elasticsearch. Built with Apache Airflow and following software design principles and patterns.

## 🏗️ Architecture

The pipeline follows a modular architecture with clear separation of concerns:

```
etl_pipeline/
├── connector/          # Database and service connections
│   ├── base.py        # Abstract base connector
│   ├── connection.py  # Singleton connection manager
│   ├── postgres.py    # PostgreSQL connector with SQLAlchemy
│   ├── elasticsearch.py # Elasticsearch connector
│   └── factory.py     # Factory pattern for connector creation
│
├── extractor/         # Data extraction logic
│   ├── base.py       # Abstract base extractor
│   ├── config.py     # Extraction configuration classes
│   ├── postgres_extractor.py # PostgreSQL extractor
│   ├── state_manager.py      # State management for incremental loads
│   └── factory.py    # Factory pattern for extractor creation
│
├── transformer/       # Data transformation logic
│   ├── base.py       # Abstract base transformer
│   ├── json_transformer.py # JSON transformer
│   └── factory.py    # Factory pattern for transformer creation
│
├── loader/           # Data loading logic
│   ├── base.py      # Abstract base loader
│   ├── elasticsearch_loader.py # Elasticsearch loader (single/bulk)
│   └── factory.py   # Factory pattern for loader creation
│
├── config/          # Configuration files
│   └── etl_config.yaml # Main ETL configuration
│
└── dags/           # Airflow DAGs
    └── postgres_to_elasticsearch_dag.py # Main ETL DAG
```

## ✨ Features

### Design Patterns Implemented

1. **Abstract Factory Pattern**: Used in connector, extractor, transformer, and loader modules for creating instances
2. **Factory Method Pattern**: Specific factories for each module type
3. **Singleton Pattern**: ConnectionManager ensures single instance for connection pooling
4. **Strategy Pattern**: Different extraction strategies (full, incremental date, CDC)
5. **Template Method Pattern**: Base classes define workflow, subclasses implement specifics

### Key Capabilities

- ✅ **Multiple Extraction Modes**:
  - Full table extraction
  - Incremental date-based extraction
  - CDC (Change Data Capture) based extraction using primary keys or update columns

- ✅ **Connection Management**:
  - SQLAlchemy ORM for PostgreSQL
  - Connection pooling with configurable pool sizes
  - Event listeners for connection lifecycle monitoring
  - Thread-safe singleton connection manager

- ✅ **Data Transformation**:
  - DataFrame to JSON conversion
  - Multiple JSON orientations (records, split, index, etc.)
  - Automatic handling of special data types (datetime, Decimal, NaN)
  - Batch transformation support

- ✅ **Flexible Loading**:
  - Single document indexing
  - Bulk loading with configurable batch sizes
  - Streaming bulk for memory efficiency
  - Automatic index creation

- ✅ **State Management**:
  - Persistent state tracking for incremental loads
  - Automatic checkpoint management
  - Resume capability after failures

- ✅ **Airflow Integration**:
  - Dynamic task generation per table
  - Parallel table processing
  - XCom for inter-task communication
  - Comprehensive error handling and retries

## 📋 Prerequisites

- Python 3.8+
- Apache Airflow 2.7+
- PostgreSQL 12+
- Elasticsearch 8.x
- Access to source PostgreSQL database
- Access to target Elasticsearch cluster

## 🚀 Installation

1. **Clone or copy the ETL pipeline directory**:
```bash
cd /path/to/your/airflow/home
cp -r etl_pipeline .
```

2. **Install dependencies**:
```bash
pip install -r etl_pipeline/requirements.txt
```

3. **Configure your environment**:
```bash
# Edit the configuration file
nano etl_pipeline/config/etl_config.yaml
```

4. **Update Airflow configuration** (if needed):
```bash
# Add the etl_pipeline directory to PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:/path/to/etl_pipeline"
```

5. **Copy DAG to Airflow DAGs folder**:
```bash
cp etl_pipeline/dags/postgres_to_elasticsearch_dag.py $AIRFLOW_HOME/dags/
```

## ⚙️ Configuration

### Main Configuration File: `config/etl_config.yaml`

```yaml
# PostgreSQL Connection
postgres:
  connection_string: "postgresql://username:password@host:5432/database"
  pool_size: 5
  max_overflow: 10

# Elasticsearch Connection
elasticsearch:
  connection_string: "http://localhost:9200"
  use_ssl: false
  verify_certs: false

# Extraction Configuration
extraction:
  state_file: "/tmp/etl_state.json"
  tables:
    - table_name: "users"
      schema: "public"
      columns: ["id", "username", "email", "created_at"]
      extraction_mode: "incremental_cdc"  # full, incremental_date, incremental_cdc
      cdc_column: "id"
      batch_size: 1000

# Transformation Configuration
transformation:
  orient: "records"
  date_format: "iso"
  handle_nan: "null"

# Loading Configuration
loading:
  elasticsearch:
    bulk_size: 1000
    max_retries: 3
  index_mappings:
    users:
      index_name: "etl_users"
      id_field: "id"
```

### Extraction Modes

#### 1. Full Extraction
Extracts all data from the table on each run.
```yaml
extraction_mode: "full"
```

#### 2. Incremental Date-Based Extraction
Extracts only records newer than the last extraction based on a date column.
```yaml
extraction_mode: "incremental_date"
date_column: "updated_at"
```

#### 3. CDC-Based Extraction
Extracts only records with CDC column values greater than the last extracted value.
```yaml
extraction_mode: "incremental_cdc"
cdc_column: "id"  # or "version", "sequence_number", etc.
```

## 🎯 Usage

### Running the DAG

1. **Enable the DAG in Airflow UI**:
   - Navigate to Airflow web interface
   - Find `postgres_to_elasticsearch_etl` DAG
   - Toggle it ON

2. **Trigger manually** (optional):
```bash
airflow dags trigger postgres_to_elasticsearch_etl
```

3. **Monitor execution**:
   - Check DAG runs in Airflow UI
   - View task logs for detailed information
   - Monitor XCom values for data flow

### Programmatic Usage

You can also use the modules programmatically outside of Airflow:

```python
from connector import ConnectorFactory
from extractor import ExtractorFactory, TableConfig, ExtractionMode
from transformer import TransformerFactory
from loader import LoaderFactory

# Create connections
pg_conn = ConnectorFactory.create_connector(
    "postgres",
    "postgresql://user:pass@localhost/db"
)
pg_conn.connect()

es_conn = ConnectorFactory.create_connector(
    "elasticsearch",
    "http://localhost:9200"
)
es_conn.connect()

# Extract data
extractor = ExtractorFactory.create_extractor("postgres", pg_conn)
table_config = TableConfig(
    table_name="users",
    extraction_mode=ExtractionMode.FULL
)
df = extractor.extract_with_config(table_config)

# Transform data
transformer = TransformerFactory.create_transformer("json")
json_data = transformer.transform(df)

# Load data
loader = LoaderFactory.create_loader(
    "elasticsearch",
    es_conn,
    index_name="users",
    id_field="id"
)
loader.load_batch(json_data)

# Cleanup
pg_conn.disconnect()
es_conn.disconnect()
```

## 📊 Monitoring and Logging

### Logs Location
- Airflow task logs: `$AIRFLOW_HOME/logs/`
- Application logs: Check Airflow task logs for module-specific logging

### Key Metrics to Monitor
- Extraction row counts per table
- Transformation success rates
- Load success/failure counts
- Connection pool statistics
- Task execution times

### State Management
The pipeline maintains state in `etl_state.json` (configurable):
```json
{
  "users.id": 12345,
  "users.id_timestamp": "2024-01-15T10:30:00",
  "orders.order_date": "2024-01-15T00:00:00",
  "orders.order_date_timestamp": "2024-01-15T10:30:00"
}
```

## 🔧 Extending the Pipeline

### Adding a New Connector

1. Create connector class inheriting from `BaseConnector`:
```python
# connector/mydb.py
from .base import BaseConnector

class MyDBConnector(BaseConnector):
    def connect(self):
        # Implementation
        pass
    
    def disconnect(self):
        # Implementation
        pass
    # ... implement other abstract methods
```

2. Register in factory:
```python
# connector/factory.py
ConnectorFactory.register_connector(
    ConnectorType.MYDB,
    MyDBConnector
)
```

### Adding a New Transformer

Similar pattern - inherit from `BaseTransformer` and register with `TransformerFactory`.

### Adding a New Loader

Similar pattern - inherit from `BaseLoader` and register with `LoaderFactory`.

## 🛡️ Error Handling

The pipeline includes comprehensive error handling:

- **Connection failures**: Automatic retries with exponential backoff
- **Extraction errors**: Logged and can be configured to skip or fail
- **Transformation errors**: Validation before transformation
- **Load errors**: Bulk operations continue on partial failures
- **State persistence**: State saved after each successful table extraction

## 🧪 Testing

```bash
# Run all tests
pytest etl_pipeline/tests/

# Run with coverage
pytest --cov=etl_pipeline etl_pipeline/tests/

# Run specific test module
pytest etl_pipeline/tests/test_connector.py
```

## 📝 Best Practices

1. **Configuration Management**:
   - Use environment-specific config files
   - Store sensitive credentials in Airflow Variables or Secrets

2. **State Management**:
   - Regularly backup state files
   - Use shared storage for distributed Airflow setups

3. **Performance Tuning**:
   - Adjust `batch_size` based on data volume
   - Configure connection pool sizes appropriately
   - Use streaming bulk for very large datasets

4. **Monitoring**:
   - Set up alerts for DAG failures
   - Monitor Elasticsearch index growth
   - Track extraction lag times

## 🤝 Contributing

When extending this pipeline:
1. Follow the existing design patterns
2. Add comprehensive docstrings
3. Include error handling and logging
4. Write unit tests for new functionality
5. Update configuration examples

## 📄 License

[Your License Here]

## 👥 Authors

Data Engineering Team

## 🙏 Acknowledgments

- Built with Apache Airflow
- Uses SQLAlchemy for database ORM
- Elasticsearch Python client
- Pandas for data manipulation
