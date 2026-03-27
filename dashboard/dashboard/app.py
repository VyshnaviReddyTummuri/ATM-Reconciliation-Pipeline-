import streamlit as st
import pandas as pd

df = pd.read_csv("../recon_output_csv/part-00000-36627f27-586b-4dac-b774-fba3c3be792b-c000.csv")

st.title("ATM Reconciliation Dashboard")

st.metric("Total", len(df))
st.metric("Matched", len(df[df["reconciliation_status"]=="MATCHED"]))
st.metric("Mismatch", len(df[df["reconciliation_status"]=="MISMATCHED"]))
st.metric("Missing ATM", len(df[df["reconciliation_status"]=="MISSING_IN_ATM"]))
st.metric("Missing Settlement", len(df[df["reconciliation_status"]=="MISSING_IN_SETTLEMENT"]))

st.dataframe(df)