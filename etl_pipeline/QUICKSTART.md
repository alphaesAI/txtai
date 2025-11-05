# Quick Start Guide

Get the ETL pipeline running in 5 minutes!

## 1. Install Dependencies

```bash
cd /home/logi/txtai/etl_pipeline
pip install -r requirements.txt
```

## 2. Configure Connections

Edit `config/etl_config.yaml`:

```yaml
postgres:
  connection_string: "postgresql://your_user:your_password@localhost:5432/your_db"

elasticsearch:
  connection_string: "http://localhost:9200"
```

## 3. Configure Tables

In `config/etl_config.yaml`, add your tables:

```yaml
extraction:
  tables:
    - table_name: "your_table"
      schema: "public"
      columns: ["id", "name", "created_at"]
      extraction_mode: "full"  # Start with full
      batch_size: 1000
```

## 4. Test Connections

```python
from connector import ConnectorFactory

# Test PostgreSQL
pg_conn = ConnectorFactory.create_connector(
    "postgres",
    "postgresql://user:pass@localhost/db"
)
pg_conn.connect()
print("PostgreSQL connected:", pg_conn.test_connection())

# Test Elasticsearch
es_conn = ConnectorFactory.create_connector(
    "elasticsearch",
    "http://localhost:9200"
)
es_conn.connect()
print("Elasticsearch connected:", es_conn.test_connection())
```

## 5. Set Up Airflow

```bash
# Set Airflow home
export AIRFLOW_HOME=~/airflow

# Initialize Airflow database
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Copy DAG file
cp dags/postgres_to_elasticsearch_dag.py $AIRFLOW_HOME/dags/

# Start Airflow webserver
airflow webserver --port 8080

# In another terminal, start scheduler
airflow scheduler
```

## 6. Run the DAG

1. Open Airflow UI: http://localhost:8080
2. Login with your credentials
3. Find `postgres_to_elasticsearch_etl` DAG
4. Toggle it ON
5. Click "Trigger DAG" to run manually

## 7. Verify Results

```python
from elasticsearch import Elasticsearch

es = Elasticsearch(['http://localhost:9200'])

# Check if index was created
indices = es.indices.get_alias(index="etl_*")
print("Created indices:", list(indices.keys()))

# Count documents
for index in indices:
    count = es.count(index=index)['count']
    print(f"{index}: {count} documents")
```

## Common Issues

### Issue: PostgreSQL connection failed
**Solution**: Check credentials, host, port, and database name

### Issue: Elasticsearch connection refused
**Solution**: Ensure Elasticsearch is running on specified host/port

### Issue: No data extracted
**Solution**: Check table names, schema, and database permissions

### Issue: DAG not appearing in Airflow
**Solution**: 
- Check DAG file is in `$AIRFLOW_HOME/dags/`
- Check for Python syntax errors
- Restart Airflow scheduler

## Next Steps

1. **Enable Incremental Extraction**: Change `extraction_mode` to `incremental_date` or `incremental_cdc`
2. **Add More Tables**: Add additional table configurations
3. **Schedule Regular Runs**: The DAG runs every 6 hours by default
4. **Monitor Performance**: Check Airflow logs and task durations
5. **Customize Transformations**: Modify transformer settings for your needs

## Need Help?

- Check the full README.md for detailed documentation
- Review Airflow task logs for error details
- Ensure all services (PostgreSQL, Elasticsearch, Airflow) are running
