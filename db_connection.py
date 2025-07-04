import os
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import streamlit as st

def get_pg_engine():
    try:
        # Try to load from Streamlit secrets (Cloud or local)
        creds = {
            "user": st.secrets["PG_USER"],
            "password": st.secrets["PG_PASSWORD"],
            "host": st.secrets["PG_HOST"],
            "port": st.secrets["PG_PORT"],
            "dbname": st.secrets["PG_DB"],
        }
    except Exception:
        # Fallback to environment (.env or local)
        creds = {
            "user": os.getenv("PG_USER"),
            "password": os.getenv("PG_PASSWORD"),
            "host": os.getenv("PG_HOST"),
            "port": os.getenv("PG_PORT"),
            "dbname": os.getenv("PG_DB"),
        }

    # Validate that all values are present
    missing = [k for k, v in creds.items() if not v]
    if missing:
        raise ValueError(f"Missing DB credentials: {', '.join(missing)}")

    password_encoded = quote_plus(creds["password"])

    connection_string = (
        f"postgresql+psycopg2://{creds['user']}:{password_encoded}"
        f"@{creds['host']}:{creds['port']}/{creds['dbname']}"
    )

    return create_engine(
        connection_string,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800
    )
