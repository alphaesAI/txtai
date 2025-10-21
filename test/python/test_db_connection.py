import sys
import os
sys.path.append(os.path.abspath("paper_curator/"))

from src.db.interfaces.postgresql import PostgreSQLDatabase
from src.schemas.database.config import PostgreSQLSettings

config = PostgreSQLSettings(
    database_url="postgresql://rag_user:rag_password@localhost:5432/paper_curator",
    echo_sql=True,
)

db = PostgreSQLDatabase(config)

db.startup()

print("database connection successful")

db.teardown()