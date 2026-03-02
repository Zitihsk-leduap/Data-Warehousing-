# Snowflake SCD2 ETL Pipeline
A complete ETL pipeline that extracts sales data from CSV files, loads it into Snowflake, and manages dimension tables using Slowly Changing Dimension Type 2 (SCD2) to retain the full history of data changes.

## Schema

<img width="531" height="534" alt="Star Schema" src="https://github.com/user-attachments/assets/70115d1b-418c-4711-a99e-0d7c7265cd1c" />

### Prerequisites

- Python
- Snowflake account

### Installation

```bash

git clone https://github.com/Zitihsk-leduap/Data-Warehousing-.git
cd snowflake-scd2-pipeline


python -m venv venv
source venv/bin/activate        # Linux/Mac
.\venv\Scripts\activate         # Windows


pip install -r requirements.txt
```

### Configuration

Update `config.json` with your Snowflake credentials:

```json
{
    "ACCOUNT": "<your-account>",
    "USER": "<your-user>",
    "PASSWORD": "<your-password>",
    "WAREHOUSE": "<your-warehouse>",
    "DATABASE": "<your-database>",
    "LOG_PATH": "log",
    "LND_SCHEMA": "LANDING",
    "STG_SCHEMA": "STAGE",
    "TMP_SCHEMA": "TEMP",
    "TGT_SCHEMA": "TARGET",
    "FILE_STAGE": "CSV_FILE"
}
```

### Create Database Objects

Run the DDL in Snowflake:

```sql
-- Execute ddl/ddl_file.sql in Snowflake worksheet
```

### Upload Source CSV

```sql
PUT file:///path/to/sales.csv @LANDING.CSV_FILE;
```

