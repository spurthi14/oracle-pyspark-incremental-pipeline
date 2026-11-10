# Oracle to PySpark Incremental Pipeline

A small data engineering project that pulls data from an on-premise Oracle database with PySpark, loads only new rows, merges them into a target table, and checks that the data matches.

## What it does
- Extracts a customer table from Oracle XE over JDBC using PySpark.
- Loads incrementally: finds the latest `create_date` already loaded (the watermark) and reads only newer rows.
- Merges new and changed rows into a target table with an Oracle `MERGE` (upsert), so re-running the job does not create duplicates.
- Validates the load by comparing row counts and checking for duplicate IDs.
- Writes Parquet output, which was loaded into a Delta bronze table in Databricks.

## Flow
```
customer_source --> incremental read (PySpark / JDBC) --> MERGE (python-oracledb) --> customer_target
                                   |
                                   +--> Parquet --> Databricks Delta (bronze)
```

## Tech
Oracle XE, PySpark, JDBC (ojdbc), python-oracledb, Parquet, Databricks, Python 3.11, Java 17

## Files
| File | Purpose |
|---|---|
| `read_oracle.py` | Full read from Oracle and write to Parquet |
| `incremental_load.py` | Reads only rows newer than the watermark |
| `merge_customers.py` | Runs the MERGE from Python (upsert into target) |
| `read_parquet.py` | Reads the Parquet output back and checks the schema |
| `validate.py` | Compares counts and checks for duplicate IDs |

## How to run
1. Install Java 17 and Python 3.11.
2. Create and activate a virtual environment, then install dependencies:
   `pip install pyspark oracledb`
3. Download the Oracle JDBC driver (`ojdbc`) and put it in a `jar` folder (not committed to the repo).
4. Set the database password as an environment variable: `ORA_PWD`
5. Run the scripts: `python incremental_load.py`, then `python merge_customers.py`, then `python validate.py`.

On Windows, running PySpark needs `winutils.exe` and `hadoop.dll` in `C:\hadoop\bin`, with `HADOOP_HOME` set.

## What I handled
- Re-runs are safe: the MERGE updates existing rows instead of duplicating them.
- Oracle `NUMBER` columns arrive in Spark as `decimal(38,10)`, so IDs are cast to integer.
- Passwords are read from an environment variable, never stored in code.
- Reconciliation: Oracle row count vs output row count, plus a duplicate ID check.

## Limitations and next steps
- The watermark uses `create_date`, so edits to old rows are not detected. A `last_updated` column would fix this.
- Add a PySpark cleaning step (trim names, lowercase emails) before the data lands.
- <your own ideas>
- Working on last_updated watermark