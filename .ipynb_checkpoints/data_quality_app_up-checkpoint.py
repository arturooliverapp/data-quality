import streamlit as st
import pandas as pd
import os
from sqlalchemy import text
from db_connection import get_pg_engine
from authlib.integrations.requests_client import OAuth2Session
from urllib.parse import urlencode
from dotenv import load_dotenv, dotenv_values
from pathlib import Path

env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

# Override environment variables from .env (local only)
env = dotenv_values(dotenv_path=env_path)
for k, v in env.items():
    os.environ[k] = v

# === Load secrets ===
CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI", "http://localhost:8501/")
AUTHORIZED_DOMAIN = os.getenv("AUTHORIZED_DOMAIN")
COOKIE_SECRET = os.getenv("COOKIE_SECRET")
AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"
USERINFO_URL = "https://openidconnect.googleapis.com/v1/userinfo"
SCOPE = "openid email profile"

# === Login Functions ===
def get_login_url():
    query = urlencode({
        "client_id": CLIENT_ID,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPE,
        "access_type": "offline",
        "prompt": "select_account",
        "hd": AUTHORIZED_DOMAIN
    })
    return f"{AUTH_URL}?{query}"

def fetch_user_info(auth_code):
    oauth = OAuth2Session(CLIENT_ID, CLIENT_SECRET, redirect_uri=REDIRECT_URI)
    token = oauth.fetch_token(
        TOKEN_URL,
        code=auth_code,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        include_client_id=True,
    )
    resp = oauth.get(USERINFO_URL)
    return resp.json()

# === Auth Flow ===
query_params = st.query_params.to_dict()
if "code" in query_params and "user" not in st.session_state:
    try:
        user_info = fetch_user_info(query_params["code"])
        user_email = user_info.get("email", "")
        user_domain = user_email.split("@")[-1]

        if user_domain.lower() == AUTHORIZED_DOMAIN.lower():
            st.session_state["user"] = user_info
            st.success(f"✅ Logged in as {user_email}")
        else:
            st.error("Access denied. Please use your @popularpower.io email.")
            st.stop()
    except Exception as e:
        st.error(f"Authentication failed: {e}")
        st.stop()

# === Require login ===
if "user" not in st.session_state:
    st.set_page_config(page_title="Login", layout="centered")
    st.title("🔐 Login Required")
    st.write("Please log in using your @popularpower.io Google account.")

    if st.button("🔐 Log in with Google"):
        login_url = get_login_url()
        st.markdown(f'<meta http-equiv="refresh" content="0;url={login_url}">', unsafe_allow_html=True)
        st.stop()

    st.stop()

# === Main App ===
st.set_page_config(page_title="Data Quality Checker", layout="wide")
st.title("📊 Data Quality Checker")
st.markdown("""
Welcome! This tool helps you download site-level data quality reports for any organization.  
You'll get a CSV ready for Google Sheets with all issues pre-categorized.
""")

# Step 1: Select Organization
st.markdown("### 🏢 Step 1: Select an Organization")

@st.cache_data
def get_organizations():
    engine = get_pg_engine()
    with engine.connect() as conn:
        orgs_df = pd.read_sql("SELECT id, name FROM organizations ORDER BY name;", conn)
    return orgs_df

orgs_df = get_organizations()
org_name_to_id = dict(zip(orgs_df["name"], orgs_df["id"]))
selected_org_name = st.selectbox("Choose an organization:", orgs_df["name"])
selected_org_id = org_name_to_id[selected_org_name]

# Step 2: Load Data
st.markdown("### 🔎 Step 2: Load Data for Selected Organization")
QUERY = """
SELECT
    s.id AS site_id,
    s.name AS site_name,
    s.country AS site_country,
    o.name AS organization,
    o.country AS organization_country,
    s.utility_id,
    s.syncable,
    CASE 
        WHEN EXISTS (
            SELECT 1
            FROM public.inverters i
            WHERE i.site_id = s.id
        ) THEN TRUE
        ELSE FALSE
    END AS inverters_connected,
    s.installed_capacity,
    s.performance,
    s.accumulated_energy,
    s.latitude,
    s.longitude,
    s.expected_sun_hours,
    s.expected_annual_yield,
    CASE 
        WHEN s.monthly_commitments IS NULL THEN 'NULL'
        ELSE 'NOT NULL'
    END AS monthly_commitments_flag,
    (s.monthly_commitments ->> 'jan')::numeric AS jan_commitment,
    (s.monthly_commitments ->> 'feb')::numeric AS feb_commitment,
    (s.monthly_commitments ->> 'mar')::numeric AS mar_commitment,
    (s.monthly_commitments ->> 'apr')::numeric AS apr_commitment,
    (s.monthly_commitments ->> 'may')::numeric AS may_commitment,
    (s.monthly_commitments ->> 'jun')::numeric AS jun_commitment,
    (s.monthly_commitments ->> 'jul')::numeric AS jul_commitment,
    (s.monthly_commitments ->> 'aug')::numeric AS aug_commitment,
    (s.monthly_commitments ->> 'sep')::numeric AS sep_commitment,
    (s.monthly_commitments ->> 'oct')::numeric AS oct_commitment,
    (s.monthly_commitments ->> 'nov')::numeric AS nov_commitment,
    (s.monthly_commitments ->> 'dec')::numeric AS dec_commitment
FROM 
    public.sites s
LEFT JOIN 
    public.organization_sites os ON s.id = os.site_id
LEFT JOIN 
    public.organizations o ON os.organization_id = o.id
WHERE
    o.id = :org_id
    AND s.syncable = TRUE
    AND EXISTS (
        SELECT 1
        FROM public.inverters i
        WHERE i.site_id = s.id
    );
""" 

if st.button("▶️ Run Data Query"):
    engine = get_pg_engine()
    with engine.connect() as conn:
        df_raw = pd.read_sql(text(QUERY), conn, params={"org_id": selected_org_id})
    st.session_state["df_raw"] = df_raw
    st.success(f"✅ Loaded {len(df_raw)} sites for {selected_org_name}.")
    st.dataframe(df_raw.head(10), use_container_width=True)

# Step 3: Process and Download
if "df_raw" in st.session_state:
    df = st.session_state["df_raw"].copy()
    org_name = df["organization"].iloc[0] if "organization" in df.columns else "Unknown_Org"

    st.markdown("### 🛠️ Step 3: Process and Download CSV")

    if st.button("🧪 Generate CSV with Data Quality Flags"):
        def categorize_ic(cap):
            if pd.isnull(cap): return "IC is missing"
            elif cap == 0: return "IC is 0"
            elif cap < 1: return "IC < 1 kWp"
            elif cap > 2000: return "IC > 2 MWp"
            return "IC between 1 kWp and 2 MWp"
        df["Installed Capacity Category"] = df["installed_capacity"].apply(categorize_ic)

        def categorize_perf(p):
            if pd.isnull(p): return "Performance is missing"
            elif p == 0: return "Performance is 0%"
            elif p < 50: return "Performance < 50%"
            elif 50 <= p <= 200: return "Performance between 50% and 200%"
            return "Performance > 200%"
        df["Performance Category"] = df["performance"].apply(categorize_perf)

        def categorize_geo(lat, lon):
            invalid_coords = [(0, 0), (1, 1), (0, 1), (1, 0), (0, -1), (-1, 0), (-1, -1)]
            if pd.isnull(lat) or pd.isnull(lon): return "Coordinates missing"
            if (lat, lon) in invalid_coords: return "Invalid coordinates"
            return "Coordinates OK"
        df["Geo Coordinates Category"] = df.apply(lambda row: categorize_geo(row["latitude"], row["longitude"]), axis=1)

        def categorize_commitments(row):
            flag = row.get("monthly_commitments_flag", "").strip().lower() if not pd.isnull(row.get("monthly_commitments_flag")) else "null"
            sun_hours = row.get("expected_sun_hours")
            yield_ = row.get("expected_annual_yield")
            if flag == "null" and ((sun_hours in [0, 3.5]) or pd.isnull(sun_hours)) and (pd.isnull(yield_) or yield_ == 0):
                return "Missing commitments"
            return "Commitments OK"
        df["Monthly Commitments Category"] = df.apply(categorize_commitments, axis=1)

        def categorize_utility_flag(row):
            utility = str(row.get("utility_id", "")).strip()
            country = str(row.get("site_country", "")).strip().lower()
            has_utility = utility not in ["", "0", "nan"]
            valid_format = len(utility) == 12 and utility.isdigit()
            if has_utility and not valid_format:
                return "Incorrect Format"
            if has_utility and valid_format:
                if country == "mexico": return "Correct (Mexico)"
                elif country == "": return "Correct (Missing Country)"
                return "Correct (Other Country)"
            if not has_utility:
                if country == "mexico": return "Missing (Mexico)"
                elif country == "": return "Missing (Missing Country)"
                return "Missing (Other Country)"
        df["Utility ID Format Category"] = df.apply(categorize_utility_flag, axis=1)

        def is_valid_utility(val):
            val_str = str(val).strip()
            return val_str.isdigit() and len(val_str) == 12

        def is_invalid_coords(lat, lon):
            return (lat, lon) in [(0, 0), (1, 1), (0, 1), (1, 0), (0, -1), (-1, 0), (-1, -1)]

        def classify_row(row):
            issues_missing = []
            issues_mismatch = []

            if pd.isnull(row["installed_capacity"]): issues_missing.append("Installed Capacity")
            if pd.isnull(row["performance"]): issues_missing.append("Performance")
            if pd.isnull(row["latitude"]) or pd.isnull(row["longitude"]): issues_missing.append("Coordinates")

            mc_flag = str(row.get("monthly_commitments_flag")).strip().lower() if not pd.isnull(row.get("monthly_commitments_flag")) else "null"
            if mc_flag == "null" and ((row["expected_sun_hours"] in [0, 3.5]) or pd.isnull(row["expected_sun_hours"])) and (pd.isnull(row["expected_annual_yield"]) or row["expected_annual_yield"] == 0):
                issues_missing.append("Monthly Commitments")

            if pd.isnull(row["utility_id"]) or str(row["utility_id"]).strip() in ["", "0", "nan"]:
                if str(row.get("site_country", "")).strip().lower() == "mexico":
                    issues_missing.append("Utility ID")

            if pd.notnull(row["installed_capacity"]) and (row["installed_capacity"] == 0 or row["installed_capacity"] > 2000):
                issues_mismatch.append("Installed Capacity")
            if pd.notnull(row["performance"]) and row["performance"] > 133:
                issues_mismatch.append("Performance")
            if pd.notnull(row["latitude"]) and pd.notnull(row["longitude"]) and is_invalid_coords(row["latitude"], row["longitude"]):
                issues_mismatch.append("Coordinates")
            if not is_valid_utility(row["utility_id"]) and str(row["utility_id"]).strip() not in ["", "0", "nan", None]:
                issues_mismatch.append("Utility ID")

            summary_parts = []
            if issues_missing: summary_parts.append("Missing: " + ", ".join(issues_missing))
            if issues_mismatch: summary_parts.append("Mismatch: " + ", ".join(issues_mismatch))
            summary = "; ".join(summary_parts)

            if not issues_missing and not issues_mismatch:
                quality = "Data Complete"
            elif issues_missing and issues_mismatch:
                quality = "Data Missing + Mismatch"
            elif issues_missing:
                quality = "Data Missing"
            else:
                quality = "Data Mismatch"

            return pd.Series([quality, summary])

        df[["Data Quality Flag", "Data Issues Summary"]] = df.apply(classify_row, axis=1)

        safe_name = org_name.lower().replace(" ", "_").replace("/", "_")
        filename = f"data_quality_check_{safe_name}.csv"
        csv_bytes = df.to_csv(index=False).encode("utf-8")

        st.success(f"📄 CSV ready for {org_name} ({df.shape[0]} rows).")
        st.download_button(
            label="📥 Download Cleaned CSV",
            data=csv_bytes,
            file_name=filename,
            mime="text/csv"
        )
        st.dataframe(df.head(10), use_container_width=True)
