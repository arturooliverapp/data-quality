import os
import streamlit as st
from sqlalchemy import create_engine
from urllib.parse import quote_plus

def get_pg_engine():
    st.write("🔍 Loading database credentials...")

    # Check both sources explicitly
    creds = {
        "user": st.secrets.get("PG_USER") or os.getenv("PG_USER"),
        "password": st.secrets.get("PG_PASSWORD") or os.getenv("PG_PASSWORD"),
        "host": st.secrets.get("PG_HOST") or os.getenv("PG_HOST"),
        "port": st.secrets.get("PG_PORT") or os.getenv("PG_PORT"),
        "dbname": st.secrets.get("PG_DB") or os.getenv("PG_DB")
    }

    # Print each credential for debugging (hide password)
    st.write("🧪 Resolved DB credentials:")
    for k, v in creds.items():
        if k == "password":
            st.write(f"• {k}: {'***' if v else '❌ MISSING'}")
        else:
            st.write(f"• {k}: {v if v else '❌ MISSING'}")

    # Validate all values
    missing = [k for k, v in creds.items() if not v]
    if missing:
        st.error(f"❌ Missing DB credentials: {', '.join(missing)}")
        raise ValueError(f"Missing DB credentials: {', '.join(missing)}")

    # Encode password
    try:
        password_encoded = quote_plus(creds["password"])
    except Exception as e:
        st.error("❌ Failed to encode DB password.")
        raise e

    # Build connection string
    connection_string = (
        f"postgresql+psycopg2://{creds['user']}:{password_encoded}"
        f"@{creds['host']}:{creds['port']}/{creds['dbname']}"
    )

    st.write("✅ DB connection string constructed.")

    # Create engine
    try:
        engine = create_engine(
            connection_string,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800
        )
        st.write("🎉 SQLAlchemy engine created successfully.")
        return engine
    except Exception as e:
        st.error("❌ Failed to create SQLAlchemy engine.")
        raise e
