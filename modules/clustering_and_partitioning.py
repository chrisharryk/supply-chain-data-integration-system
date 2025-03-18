import logging
import os
from google.cloud import bigquery
from dotenv import load_dotenv

env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".env"))
load_dotenv(env_path)

PROJECT_ID = os.getenv('PROJECT_ID')
DATASET_ID = os.getenv('DATASET_ID')

client = bigquery.Client()

log_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "etl_pipeline.log"))

logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

def partition_fact_sales():
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.fact_sales_partitioned`
    PARTITION BY order_date AS
    SELECT 
        f.*, 
        d.`Order Date` AS order_date
    FROM `{PROJECT_ID}.{DATASET_ID}.fact_sales` f
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_orders` d 
    ON f.order_key = d.order_key;
    """
    try:
        client.query(query).result()
        logging.info("Partitioned table 'fact_sales_partitioned' created successfully.")
    except Exception as e:
        logging.error(f"Error partitioning fact_sales: {e}")

def cluster_fact_sales():
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.fact_sales_partitioned_clustered`
    PARTITION BY order_date
    CLUSTER BY customer_key, product_key, region_key
    AS
    SELECT 
      f.*, 
      d.`Order Date` AS order_date
    FROM `{PROJECT_ID}.{DATASET_ID}.fact_sales` f
    JOIN `{PROJECT_ID}.{DATASET_ID}.dim_orders` d 
    ON f.order_key = d.order_key;
    """
    try:
        client.query(query).result()
        logging.info("Partitioned & clustered table 'fact_sales_partitioned_clustered' created successfully.")
    except Exception as e:
        logging.error(f"Error clustering fact_sales: {e}")

def execute_partitioning_and_clustering():
    partition_fact_sales()
    cluster_fact_sales()