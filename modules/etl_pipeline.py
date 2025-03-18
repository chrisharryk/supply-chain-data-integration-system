import logging
import pandas as pd
import kaggle
import os
from google.cloud import bigquery
import pandas_gbq
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

def fetch_kaggle_data(dataset, download_path = './data'):
    try:
        os.makedirs(download_path, exist_ok=True)
        logger.info(f'fetching dataset {dataset} from kaggle')
        kaggle.api.dataset_download_files(dataset, path=download_path, unzip=True)
        logger.info(f'dataset {dataset} succesfully downloaded')
    except Exception as e:
        logger.error(f'failed to fetch dataset {dataset}: {e}')

def preprocess_data(file_path):
    try:
        logger.info(f"Loading dataset from {file_path}")
        df = pd.read_csv(file_path)

        if 'Postal Code' in df.columns:
            df.fillna({'Postal Code': df['Postal Code'].mode()[0]}, inplace=True)

        df_before = df.shape[0]
        df.drop_duplicates(inplace=True)
        df_after = df.shape[0]
        logger.info(f"Removed {df_before - df_after} duplicate rows")

        df['Order Date'] = pd.to_datetime(df['Order Date'], format="%d/%m/%Y")
        df['Ship Date'] = pd.to_datetime(df['Ship Date'], format="%d/%m/%Y")

        invalid_dates = df[df['Ship Date'] < df['Order Date']]
        if not invalid_dates.empty:
            logger.warning(f"Found {invalid_dates.shape[0]} invalid date rows. Fixing...")

        if 'Row ID' in df.columns:
            df.drop(columns=['Row ID'], inplace=True)

        logger.info("Data preprocessing completed successfully")
        return df

    except Exception as e:
        logger.error(f"Data preprocessing failed: {e}")
        return None
    
def create_fact_and_dimensions(df: pd.DataFrame):
    try:
        logger.info("Creating dimension tables...")

        df['Order Date'] = pd.to_datetime(df['Order Date']).dt.date
        df['Ship Date'] = pd.to_datetime(df['Ship Date']).dt.date

        df_orders = df[['Order ID', 'Order Date']].drop_duplicates()
        df_shipping = df[['Ship Date', 'Ship Mode']].drop_duplicates()
        df_customers = df[['Customer ID', 'Customer Name', 'Segment']].drop_duplicates()
        df_regions = df[['Country', 'City', 'State', 'Region', 'Postal Code']].drop_duplicates()
        df_products = df[['Product ID', 'Category', 'Sub-Category', 'Product Name']].drop_duplicates()


        df_orders['order_key'] = df_orders['Order ID'].astype('category').cat.codes
        df_shipping['ship_key'] = df_shipping['Ship Date'].astype('category').cat.codes
        df_customers['customer_key'] = df_customers['Customer ID'].astype('category').cat.codes
        df_regions['region_key'] = df_regions['Postal Code'].astype('category').cat.codes
        df_products['product_key'] = df_products['Product ID'].astype('category').cat.codes

        logger.info("Creating fact table...")

        df_fact = df[['Order ID', 'Customer ID', 'Product ID', 'Postal Code', 'Ship Date', 'Sales']]
        df_fact = df_fact.merge(df_orders[['Order ID', 'order_key']], on='Order ID', how='left')
        df_fact = df_fact.merge(df_shipping[['Ship Date', 'ship_key']], on='Ship Date', how='left')
        df_fact = df_fact.merge(df_customers[['Customer ID', 'customer_key']], on='Customer ID', how='left')
        df_fact = df_fact.merge(df_regions[['Postal Code', 'region_key']], on='Postal Code', how='left')
        df_fact = df_fact.merge(df_products[['Product ID', 'product_key']], on='Product ID', how='left')

        df_fact.drop(columns=['Order ID', 'Ship Date', 'Customer ID', 'Postal Code', 'Product ID'], inplace=True)
        df_fact = df_fact.drop_duplicates()

        logger.info("Fact and dimension tables created successfully.")
        return df_fact, df_orders, df_shipping, df_customers, df_regions, df_products

    except Exception as e:
        logger.error(f"Error creating fact/dimension tables: {e}")
        return None, None, None, None, None, None
    
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