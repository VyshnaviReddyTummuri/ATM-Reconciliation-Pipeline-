import streamlit as st
import pandas as pd
import glob

# Read CSV (handles dynamic file name)
file = glob.glob("recon_output/part-*.csv")[0]
df = pd.read_csv(file)

st.title("ATM Reconciliation Dashboard")

# Metrics
total = len(df)
matched = len(df[df["reconciliation_status"] == "MATCHED"])
mismatch = len(df[df["reconciliation_status"] == "AMOUNT_MISMATCH"])
missing = len(df[df["reconciliation_status"].str.contains("MISSING")])
escalated = len(df[df["escalation_status"] == "ESCALATED"])

st.subheader("Summary Metrics")

col1, col2, col3 = st.columns(3)
col1.metric("Total Transactions", total)
col2.metric("Matched", matched)
col3.metric("Mismatched", mismatch)

col1, col2 = st.columns(2)
col1.metric("Missing", missing)
col2.metric("Escalated", escalated)

st.subheader("Detailed Data")
st.dataframe(df)