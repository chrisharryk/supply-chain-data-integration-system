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
    
def create_aggregation_procedures():
    queries = {
        "aggregate_sales_by_month": f"""
        CREATE OR REPLACE PROCEDURE {DATASET_ID}.aggregate_sales_by_month()
        BEGIN
            CREATE OR REPLACE TABLE {DATASET_ID}.agg_sales_monthly AS
            SELECT 
                EXTRACT(YEAR FROM DATE(d.`Order Date`)) AS order_year,
                EXTRACT(MONTH FROM DATE(d.`Order Date`)) AS order_month,
                SUM(f.Sales) AS total_sales,
                COUNT(DISTINCT f.order_key) AS total_orders
            FROM {DATASET_ID}.fact_sales f
            LEFT JOIN {DATASET_ID}.dim_orders d 
                ON f.order_key = d.order_key
            GROUP BY order_year, order_month
            ORDER BY order_year, order_month;
        END;
        """,
        
        "aggregate_sales_by_product": f"""
        CREATE OR REPLACE PROCEDURE {DATASET_ID}.aggregate_sales_by_product()
        BEGIN
            CREATE OR REPLACE TABLE {DATASET_ID}.agg_sales_product AS
            SELECT 
                f.product_key,
                p.`Product Name` AS product_name,  
                p.`Category` AS category,
                p.`Sub-Category` AS sub_category,
                SUM(f.`Sales`) AS total_sales,
                COUNT(DISTINCT f.order_key) AS total_orders
            FROM {DATASET_ID}.fact_sales f
            LEFT JOIN {DATASET_ID}.dim_products p 
                ON f.product_key = p.product_key
            GROUP BY f.product_key, p.`Product Name`, p.`Category`, p.`Sub-Category`
            ORDER BY total_sales DESC;
        END;
        """,
        
        "aggregate_sales_by_category": f"""
        CREATE OR REPLACE PROCEDURE {DATASET_ID}.aggregate_sales_by_category()
        BEGIN
            CREATE OR REPLACE TABLE {DATASET_ID}.agg_sales_category AS
            SELECT 
                p.`Category` AS category,
                SUM(f.`Sales`) AS total_sales,
                COUNT(DISTINCT f.order_key) AS total_orders
            FROM {DATASET_ID}.fact_sales f
            LEFT JOIN {DATASET_ID}.dim_products p 
                ON f.product_key = p.product_key
            GROUP BY p.`Category`
            ORDER BY total_sales DESC;
        END;
        """,
        
        "aggregate_sales_by_subcategory": f"""
        CREATE OR REPLACE PROCEDURE {DATASET_ID}.aggregate_sales_by_subcategory()
        BEGIN
            CREATE OR REPLACE TABLE {DATASET_ID}.agg_sales_subcategory AS
            SELECT 
                p.`Sub-Category` AS subcategory,
                SUM(f.`Sales`) AS total_sales,
                COUNT(DISTINCT f.order_key) AS total_orders
            FROM {DATASET_ID}.fact_sales f
            LEFT JOIN {DATASET_ID}.dim_products p 
                ON f.product_key = p.product_key
            GROUP BY p.`Sub-Category`
            ORDER BY total_sales DESC;
        END;
        """,
        
        "aggregate_revenue_by_region": f"""
        CREATE OR REPLACE PROCEDURE {DATASET_ID}.aggregate_revenue_by_region()
        BEGIN
            CREATE OR REPLACE TABLE {DATASET_ID}.agg_revenue_region AS
            SELECT 
                r.state AS region,
                SUM(f.`Sales`) AS total_revenue,
                COUNT(DISTINCT f.order_key) AS total_orders
            FROM {DATASET_ID}.fact_sales f
            LEFT JOIN {DATASET_ID}.dim_regions r 
                ON f.region_key = r.region_key
            GROUP BY r.state
            ORDER BY total_revenue DESC;
        END;
        """
    }

    for procedure_name, query in queries.items():
        try:
            client.query(query).result()
            logging.info(f"Created procedure: {procedure_name}")
        except Exception as e:
            logging.error(f"Error creating procedure '{procedure_name}': {e}")

def execute_aggregation_procedure(procedure_name, output_table):
    query = f"CALL {DATASET_ID}.{procedure_name}();"
    
    try:
        logger.info(f"Executing procedure: {procedure_name}...")
        client.query(query).result()
        logger.info(f"Successfully executed procedure: {procedure_name}. Fetching results...")
        
        df = client.query(f"SELECT * FROM {DATASET_ID}.{output_table}").to_dataframe()
        logger.info(f"Successfully fetched data from {output_table}.")
        return df
    
    except Exception as e:
        logger.error(f"Error executing procedure '{procedure_name}': {e}")
        return None

def execute_all_aggregations():
    aggregation_procedures = {
        "aggregate_sales_by_month": "agg_sales_monthly",
        "aggregate_sales_by_product": "agg_sales_product",
        "aggregate_sales_by_category": "agg_sales_category",
        "aggregate_sales_by_subcategory": "agg_sales_subcategory",
        "aggregate_revenue_by_region": "agg_revenue_region"
    }

    results = {}

    for procedure, output_table in aggregation_procedures.items():
        df = execute_aggregation_procedure(procedure, output_table)
        if df is not None:
            results[procedure] = df

    return results