# Snowflake SCD2 ETL Pipeline

An end-to-end ETL pipeline that extracts sales data from CSV files, loads it into **Snowflake**, and maintains dimension tables using **Slowly Changing Dimension Type 2 (SCD2)** to preserve full historical change tracking.

## Schema

<img width="531" height="534" alt="Star Schema" src="https://github.com/user-attachments/assets/70115d1b-418c-4711-a99e-0d7c7265cd1c" />




## Project Structure

```
├── config.json              
├── requirements.txt         
├── run_all.py               
├── ddl/
│   └── ddl_file.sql         
├── lib/
│   ├── Config.py            
│   ├── Logger.py            
│   └── Variable.py          
├── log/                     
└── src/
    ├── sls_extract.py       
    ├── country_load.py      
    ├── region_load.py
    ├── state_load.py
    ├── city_load.py
    ├── category_load.py
    ├── subcategory_load.py
    ├── product_load.py
    ├── segment_load.py
    ├── customer_load.py
    ├── ship_mode_load.py
    └── sales_load.py        # Fact table load
```

## Setup

### Prerequisites

- Python
- Snowflake account

### Installation

```bash

git clone https://github.com/AayusR/snowflake-scd2-pipeline.git
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

