import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from dagster import resource

load_dotenv()

@resource
def postgres():
    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5432")
    db = os.getenv("PG_DB", "dqdb")
    user = os.getenv("PG_USER", "dq")
    pw = os.getenv("PG_PASSWORD", "dq")

    url = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)