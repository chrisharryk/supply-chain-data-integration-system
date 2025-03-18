import os
import pandas_gbq
from dotenv import load_dotenv
from google.cloud import bigquery
import logging

env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".env"))
load_dotenv(env_path)

PROJECT_ID = os.getenv('PROJECT_ID')
DATASET_ID = os.getenv('DATASET_ID')

log_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "etl_pipeline.log"))

logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

def push_to_bigquery(tables_dict):
    try:
        client = bigquery.Client(project=PROJECT_ID)
        
        for table_name, df in tables_dict.items():
            logger.info(f"Pushing {table_name} to BigQuery...")
            pandas_gbq.to_gbq(df, f"{DATASET_ID}.{table_name}", project_id=PROJECT_ID, if_exists="replace")
            logger.info(f"Table {table_name} uploaded successfully.")

        logger.info("All tables pushed to BigQuery.")
    
    except Exception as e:
        logger.error(f"Error pushing tables to BigQuery: {e}")