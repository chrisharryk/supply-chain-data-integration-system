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

def create_kpi_procedures():
    queries = {
        "calculate_lead_time": f"""CREATE OR REPLACE PROCEDURE {DATASET_ID}.calculate_lead_time()
        BEGIN
            CREATE OR REPLACE TABLE {DATASET_ID}.kpi_lead_time AS
            SELECT 
                f.order_key, 
                d.`Order ID`, 
                DATE_DIFF(s.`Ship Date`, d.`Order Date`, DAY) AS lead_time_days
            FROM {DATASET_ID}.fact_sales f    
            LEFT JOIN {DATASET_ID}.dim_orders d ON f.order_key = d.order_key
            LEFT JOIN {DATASET_ID}.dim_shipping s ON f.ship_key = s.ship_key;
        END;""",
        
        "product_category_performance": f"""CREATE OR REPLACE PROCEDURE {DATASET_ID}.product_category_performance()
        BEGIN
            CREATE OR REPLACE TABLE {DATASET_ID}.kpi_product_category_performance AS
            SELECT 
                p.Category, 
                COUNT(f.product_key) AS total_sales,
                SUM(f.Sales) AS total_revenue,
                AVG(f.Sales) AS avg_revenue_per_sale
            FROM {DATASET_ID}.fact_sales f
            LEFT JOIN {DATASET_ID}.dim_products p ON f.product_key = p.product_key
            GROUP BY p.Category;
        END;""",
        
        "product_subcategory_performance": f"""CREATE OR REPLACE PROCEDURE {DATASET_ID}.product_subcategory_performance()
        BEGIN
            CREATE OR REPLACE TABLE {DATASET_ID}.kpi_product_subcategory_performance AS
            SELECT 
                p.Category, 
                p.`Sub-Category`, 
                COUNT(f.product_key) AS total_sales,
                SUM(f.Sales) AS total_revenue,
                AVG(f.Sales) AS avg_revenue_per_sale
            FROM {DATASET_ID}.fact_sales f
            LEFT JOIN {DATASET_ID}.dim_products p ON f.product_key = p.product_key
            GROUP BY p.Category, p.`Sub-Category`;
        END;""",
        
        "avg_order_value_per_category": f"""CREATE OR REPLACE PROCEDURE {DATASET_ID}.avg_order_value_per_category()
        BEGIN
            CREATE OR REPLACE TABLE {DATASET_ID}.kpi_avg_order_value_per_category AS
            SELECT 
                p.Category,
                AVG(f.Sales) AS avg_order_value
            FROM {DATASET_ID}.fact_sales f
            LEFT JOIN {DATASET_ID}.dim_products p ON f.product_key = p.product_key
            GROUP BY p.Category;
        END;""",
        
        "avg_order_frequency_by_customer": f"""CREATE OR REPLACE PROCEDURE {DATASET_ID}.avg_order_frequency_by_customer()
        BEGIN
            CREATE OR REPLACE TABLE {DATASET_ID}.kpi_avg_order_frequency_by_customer AS
            SELECT 
                c.`Customer Name`,
                COUNT(DISTINCT f.order_key) / COUNT(DISTINCT d.`Order Date`) AS avg_order_frequency
            FROM {DATASET_ID}.fact_sales f
            LEFT JOIN {DATASET_ID}.dim_orders d ON f.order_key = d.order_key
            LEFT JOIN {DATASET_ID}.dim_customers c ON f.customer_key = c.customer_key
            GROUP BY c.`Customer Name`;
        END;"""
    }
    
    for proc_name, query in queries.items():
        try:
            client.query(query).result()
            logging.info(f"Procedure '{proc_name}' created successfully.")
        except Exception as e:
            logging.error(f"Error creating procedure '{proc_name}': {e}")

def execute_kpi_procedure(procedure_name, output_table):
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

def execute_all_kpis():
    kpi_procedures = {
        "calculate_lead_time": "kpi_lead_time",
        "product_category_performance": "kpi_product_category_performance",
        "product_subcategory_performance": "kpi_product_subcategory_performance",
        "avg_order_value_per_category": "kpi_avg_order_value_per_category",
        "avg_order_frequency_by_customer": "kpi_avg_order_frequency_by_customer"
    }

    results = {}

    for procedure, output_table in kpi_procedures.items():
        df = execute_kpi_procedure(procedure, output_table)
        if df is not None:
            results[procedure] = df

    return results