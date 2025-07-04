# Data Quality App

This Streamlit app generates site-level data quality reports for internal solar project monitoring. It includes:

- Secure Google Workspace login (restricted to @popularpower.io accounts)
- PostgreSQL integration with Angaza production data
- Dynamic data flagging for Installed Capacity, Performance, Utility ID, Coordinates, and Monthly Commitments
- CSV export ready for Google Sheets

## 🔐 Requirements

This app requires a `.env` file in the root of this folder with the following secrets (not included in version control):

```env
PG_USER=...
PG_PASSWORD=...
PG_HOST=...
PG_PORT=...
PG_DB=...

GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
COOKIE_SECRET=... # generate your own with secrets.token_urlsafe(64)
AUTHORIZED_DOMAIN=popularpower.io
REDIRECT_URI=http://localhost:8501/

