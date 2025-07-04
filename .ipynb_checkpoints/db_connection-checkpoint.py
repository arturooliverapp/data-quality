import os
from sqlalchemy import create_engine
from urllib.parse import quote_plus

def get_pg_engine():
    # Check if running in Streamlit Cloud
    running_on_cloud = "STREAMLIT_SECRETS" in os.environ

    if running_on_cloud:
        creds = {
            "user": os.environ["PG_USER"],
            "password": os.environ["PG_PASSWORD"],
            "host": os.environ["PG_HOST"],
            "port": os.environ["PG_PORT"],
            "dbname": os.environ["PG_DB"]
        }
    else:
        from dotenv import load_dotenv
        load_dotenv()  # assumes local .env in project root

        creds = {
            "user": os.getenv("PG_USER"),
            "password": os.getenv("PG_PASSWORD"),
            "host": os.getenv("PG_HOST"),
            "port": os.getenv("PG_PORT"),
            "dbname": os.getenv("PG_DB")
        }

    password_encoded = quote_plus(creds["password"])
    connection_string = (
        f"postgresql+psycopg2://{creds['user']}:{password_encoded}"
        f"@{creds['host']}:{creds['port']}/{creds['dbname']}"
    )

    engine = create_engine(
        connection_string,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800
    )
    return engine
