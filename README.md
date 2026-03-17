# ATM Transaction Reconciliation Pipeline

## Overview
This project simulates a real-time ATM transaction reconciliation system using Kafka and Spark. It processes ATM transactions, compares them with settlement records, and identifies mismatches such as missing transactions and amount differences.

---

## Architecture
ATM Producer → Kafka → Spark Streaming → PostgreSQL → Reconciliation Job → Streamlit Dashboard

---

## Tech Stack
- Python  
- Apache Kafka  
- Apache Spark (PySpark)  
- PostgreSQL  
- Docker  
- Streamlit  

---

## Use Cases
- Detect mismatches between ATM transactions and settlement records  
- Identify missing or failed ATM transactions  
- Monitor reconciliation status in real-time  
- Provide operational insights through dashboard visualization  

---

## How to Run

### 1. Start services
docker-compose up

### 2. Run ATM Producer
python3 producer/atm_producer.py

### 3. Run Spark Streaming Job
spark-submit spark/atm_stream_processor.py

### 4. Run Reconciliation Job
spark-submit spark/atm_reconciliation_job.py

### 5. Run Dashboard
streamlit run dashboard/dashboard/app.py

---

## Output
- Transactions stored in PostgreSQL  
- Reconciliation results generated  
- Dashboard visualization available  

---

## Screenshots
(Add dashboard and pipeline screenshots here)