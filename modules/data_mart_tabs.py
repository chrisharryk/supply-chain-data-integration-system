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

def create_data_marts():
    queries = {
        "mart_inventory_analysis": f"""CREATE OR REPLACE TABLE {PROJECT_ID}.{DATASET_ID}.mart_inventory_analysis AS
        SELECT  
            p.product_key,  
            p.`Product Name`,  
            p.`Category`,  
            p.`Sub-Category`,  
            COUNT(DISTINCT f.order_key) AS total_orders,  
            SUM(f.Sales) AS total_sales_revenue  
        FROM {PROJECT_ID}.{DATASET_ID}.fact_sales f  
        JOIN {PROJECT_ID}.{DATASET_ID}.dim_products p  
            ON f.product_key = p.product_key  
        GROUP BY  
            p.product_key, p.`Product Name`, p.`Category`, p.`Sub-Category`;""",

        "mart_order_fulfillment": f"""CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.mart_order_fulfillment`
        AS
        SELECT 
            f.order_key,
            d.`Order ID` AS order_id,
            c.`Customer ID` AS customer_id,
            c.`Customer Name` AS customer_name,
            p.`Product Name` AS product_name,
            r.`Region` AS region_name,  
            SUM(f.Sales) AS total_sales,
            COUNT(f.order_key) AS total_orders
        FROM `{PROJECT_ID}.{DATASET_ID}.fact_sales` f
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_orders` d 
            ON f.order_key = d.order_key
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_customers` c 
            ON f.customer_key = c.customer_key
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_products` p 
            ON f.product_key = p.product_key
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_regions` r 
            ON f.region_key = r.region_key  
        GROUP BY f.order_key, order_id, customer_id, customer_name, product_name, region_name;""",

        "mart_shipping_logistics": f"""CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.mart_shipping_logistics` AS
        SELECT 
            f.order_key,
            d.`Order ID` AS order_id,
            c.`Customer ID` AS customer_id,
            c.`Customer Name` AS customer_name,
            r.`Region` AS region_name,
            s.`Ship Mode` AS ship_mode,
            SUM(f.Sales) AS total_sales,
            COUNT(f.order_key) AS total_orders,
            CAST(AVG(DATE_DIFF(s.`Ship Date`, d.`Order Date`, DAY)) AS INT) AS avg_shipping_days
        FROM `{PROJECT_ID}.{DATASET_ID}.fact_sales` f
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_orders` d 
            ON f.order_key = d.order_key
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_customers` c 
            ON f.customer_key = c.customer_key
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_regions` r 
            ON f.region_key = r.region_key
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_shipping` s 
            ON f.ship_key = s.ship_key
        GROUP BY f.order_key, order_id, customer_id, customer_name, region_name, ship_mode;"""
    }

    for mart_name, query in queries.items():
        try:
            client.query(query).result()
            logger.info(f"Data mart '{mart_name}' created successfully.")
        except Exception as e:
            logger.error(f"Error creating data mart '{mart_name}': {e}")

def fetch_data_mart(mart_name):
    try:
        query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{mart_name}`"
        df = client.query(query).to_dataframe()
        logger.info(f"Fetched data mart: {mart_name}")
        return df
    except Exception as e:
        logger.error(f"Error fetching data mart '{mart_name}': {e}")
        return None