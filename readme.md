# SCD Data Pipeline with Hadoop Ecosystem

## Objective
This project demonstrates a robust data pipeline for implementing Slowly Changing Dimensions (SCD) Type 2 in a data warehouse using the Hadoop ecosystem on Google Cloud Platform (GCP). The pipeline extracts data from MySQL, loads it into Hadoop Distributed File System (HDFS) using Sqoop, processes it with Hive and Spark for SCD Type 2 tracking, and prepares it for analytics. It showcases my expertise in big data tools like Sqoop, HDFS, Hive, and Spark, as well as my ability to design and implement scalable data workflows.

The use case focuses on an e-commerce scenario, tracking customer and product changes (e.g., address, membership tier, price) and their impact on sales over time, enabling historical and current state analysis.

## Architecture
The pipeline follows a layered architecture:

- **Source Layer**: MySQL database (`<MYSQL_CONNECTION_STRING>`) hosting operational data (customers, products, orders, order_details).
- **Ingestion Layer**: Sqoop extracts data from MySQL into HDFS in Parquet format.
- **Storage Layer**: HDFS stores raw data, with Hive managing partitioned staging tables (`staging_*`) and processed SCD/fact tables (`dim_*`, `fact_sales`).
- **Processing Layer**: PySpark processes initial and incremental loads, implementing SCD Type 2 logic and populating the fact table.
- **Analytics Layer**: Data is ready for visualization (e.g., Looker Studio via BigQuery export, not automated here).

## Components
- **MySQL**: Source database with DDLs and Python scripts for data generation.
- **Sqoop**: Data ingestion tool for one-time and incremental loads from MySQL to HDFS.
- **HDFS**: Distributed storage for raw and processed data.
- **Hive**: Data warehouse layer with DDLs for staging and SCD/fact tables.
- **PySpark**: Processing engine for SCD Type 2 transformations and fact table generation.
- **GCP Dataproc**: Cluster environment running Hadoop, Hive, and Spark.

## Repository Structure
```
HadoopSCD/
├── one_time_load_sqoop.txt         # Sqoop commands for initial load
├── incremental_sqoop.sh            # Bash script for incremental Sqoop imports
├── hive_ddls/                      # Hive table DDLs and load commands
│   ├── initial_load.sql            # DDLs and initial partition commands
│   └── incremental_load.sql        # Incremental partition commands
├── pyspark_scripts/                # PySpark scripts
│   ├── one_time_load.py            # Initial SCD and fact table load
│   └── daily_incremental_load.py   # Daily SCD and fact table update
├── mysql/                          # MySQL DDLs and data generation
│   ├── schema.sql                  # MySQL table DDLs
│   ├── one_time_load.py            # Python script for initial MySQL data
│   └── incremental_load.py         # Python script for streaming new data
└── README.md                       # This file
```

## Installation and Setup
### Prerequisites
- **GCP Account**: With a Dataproc cluster (`<DATAPROC_CLUSTER>`, region `<REGION>`) and Cloud SQL instance (`<MYSQL_CONNECTION_STRING>`).
- **Tools**: Sqoop, Hive, Spark installed on Dataproc (default with GCP Dataproc).
- **Dependencies**: Python (`mysql-connector-python`, `faker`), `gcloud` CLI.
- **GCS Bucket**: `<GCS_BUCKET>` for script storage.

### Steps
#### Clone the Repository:
```bash
git clone https://github.com/<your-username>/scd-data-pipeline.git
cd scd-data-pipeline
```

#### Set Up MySQL:
Deploy `mysql/schema.sql` to create tables:
```bash
mysql -h <MYSQL_HOST> -u <MYSQL_USER> -p <MYSQL_DATABASE> < mysql/schema.sql
```
Run `mysql/one_time_load.py` for initial data:
```bash
python mysql/one_time_load.py
```

#### Initial Load with Sqoop:
Copy `one_time_load_sqoop.txt` to Dataproc master:
```bash
gcloud compute scp one_time_load_sqoop.txt <DATAPROC_MASTER>:~/ --zone=<ZONE>
```
SSH into master:
```bash
gcloud compute ssh <DATAPROC_MASTER> --zone=<ZONE>
```
Run commands from `one_time_load_sqoop.txt`:
```bash
bash one_time_load_sqoop.txt
```

#### Create Hive Tables:
Copy `hive_ddls/initial_load.sql` to master:
```bash
gcloud compute scp hive_ddls/initial_load.sql <DATAPROC_MASTER>:~/ --zone=<ZONE>
```
Execute:
```bash
hive -f initial_load.sql
```

#### Run PySpark Initial Load:
Upload `pyspark_scripts/one_time_load.py` to GCS:
```bash
gsutil cp pyspark_scripts/one_time_load.py gs://<GCS_BUCKET>/scripts/
```
Submit job:
```bash
gcloud dataproc jobs submit pyspark \
  --cluster=<DATAPROC_CLUSTER> \
  --region=<REGION> \
  gs://<GCS_BUCKET>/scripts/one_time_load.py
```

#### Stream Incremental Data:
Run `mysql/incremental_load.py` daily (e.g., via cron or manual execution):
```bash
python mysql/incremental_load.py
```

#### Incremental Load with Sqoop:
Copy `incremental_sqoop.sh` to master:
```bash
gcloud compute scp incremental_sqoop.sh <DATAPROC_MASTER>:~/ --zone=<ZONE>
```
Execute:
```bash
chmod +x incremental_sqoop.sh
./incremental_sqoop.sh
```

#### Run PySpark Incremental Load:
Upload `pyspark_scripts/daily_incremental_load.py` to GCS:
```bash
gsutil cp pyspark_scripts/daily_incremental_load.py gs://<GCS_BUCKET>/scripts/
```
Submit job:
```bash
gcloud dataproc jobs submit pyspark \
  --cluster=<DATAPROC_CLUSTER> \
  --region=<REGION> \
  gs://<GCS_BUCKET>/scripts/daily_incremental_load.py
```

## Usage
- **Initial Load**: Steps 2-5 populate MySQL, HDFS, Hive, and SCD/fact tables with baseline data.
- **Daily Incremental Load**: Steps 6-8 stream new data, update HDFS/Hive, and process SCD Type 2 changes.
- **Analytics**: Export Hive tables to BigQuery for Looker Studio dashboards (manual step not included here).

## Example Commands
Verify Hive data:
```sql
hive -e "SELECT * FROM dim_customers WHERE load_date = '<LOAD_DATE>' LIMIT 10;"
hive -e "SELECT * FROM fact_sales WHERE order_year = '<YEAR>' LIMIT 10;"
```

## Highlights of Hadoop Ecosystem Expertise
- **Sqoop**: Efficiently extracts data from MySQL to HDFS with incremental logic.
- **HDFS**: Stores Parquet files with partitioning for scalability.
- **Hive**: Manages staging and SCD/fact tables with time-based partitioning.
- **Spark**: Processes SCD Type 2 transformations and joins for analytics-ready data.
- **Integration**: Seamlessly combines tools in a GCP Dataproc environment.

