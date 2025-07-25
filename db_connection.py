import os
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import streamlit as st

def get_pg_engine():
    # Load DB credentials from Streamlit secrets or fallback to .env
    creds = {
        "user": st.secrets.get("PG_USER", os.getenv("PG_USER")),
        "password": st.secrets.get("PG_PASSWORD", os.getenv("PG_PASSWORD")),
        "host": st.secrets.get("PG_HOST", os.getenv("PG_HOST")),
        "port": st.secrets.get("PG_PORT", os.getenv("PG_PORT")),
        "dbname": st.secrets.get("PG_DB", os.getenv("PG_DB")),
    }

    # Validate that all values are present
    missing = [k for k, v in creds.items() if not v]
    if missing:
        raise ValueError(f"Missing DB credentials: {', '.join(missing)}")

    # Encode password safely
    password_encoded = quote_plus(creds["password"])
    connection_string = (
        f"postgresql+psycopg2://{creds['user']}:{password_encoded}"
        f"@{creds['host']}:{creds['port']}/{creds['dbname']}"
    )

    # Create engine
    engine = create_engine(
        connection_string,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800
    )
    return engine
