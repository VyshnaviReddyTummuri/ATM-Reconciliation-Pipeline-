import streamlit as st
import pandas as pd
import psycopg2

st.title("ATM Transaction Reconciliation Dashboard")

conn = psycopg2.connect(
    host="localhost",
    database="atm_db",
    user="atm_user",
    password="atm_pass"
)

query = "SELECT * FROM atm_reconciliation_results"
df = pd.read_sql(query, conn)

total_transactions = len(df)
matched = len(df[df["reconciliation_status"] == "MATCHED"])
mismatch = len(df[df["reconciliation_status"] == "AMOUNT_MISMATCH"])
missing = len(df[df["reconciliation_status"] == "MISSING_IN_SETTLEMENT"])

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Transactions", total_transactions)
col2.metric("Matched", matched)
col3.metric("Amount Mismatch", mismatch)
col4.metric("Missing Settlement", missing)

st.subheader("Reconciliation Status Distribution")
status_counts = df["reconciliation_status"].value_counts()
st.bar_chart(status_counts)