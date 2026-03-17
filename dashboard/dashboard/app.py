import streamlit as st
import pandas as pd
import psycopg2

# DB connection
conn = psycopg2.connect(
    host="localhost",
    database="atm_db",
    user="atm_user",
    password="atm_pass",
    port=5432
)

query = "SELECT * FROM atm_reconciliation_results"
df = pd.read_sql(query, conn)

st.title("ATM Reconciliation Dashboard")

# Metrics
st.subheader("Summary Metrics")

total = len(df)
matched = len(df[df["reconciliation_status"] == "MATCHED"])
mismatch = len(df[df["reconciliation_status"] == "AMOUNT_MISMATCH"])
missing = len(df[df["reconciliation_status"].isin(["MISSING_IN_ATM", "MISSING_IN_SETTLEMENT"])])
escalated = len(df[df["escalation_status"] == "ESCALATED"])

col1, col2, col3 = st.columns(3)

col1.metric("Total Transactions", total)
col2.metric("Matched", matched)
col3.metric("Mismatched", mismatch)

col1, col2 = st.columns(2)

col1.metric("Missing", missing)
col2.metric("Escalated", escalated)

st.subheader("Reconciliation Data")
st.dataframe(df)