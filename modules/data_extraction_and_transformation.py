import os
import kaggle
import logging
import pandas as pd

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
