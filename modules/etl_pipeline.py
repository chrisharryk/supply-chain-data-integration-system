import logging
import pandas as pd
import kaggle
import os

log_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "etl_pipeline.log"))

logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

"""
pipe line:
1. fetching data from kaggle
2. data pre-processing
3. making facts and dimensions
5. pushing to big query
4. partitioning and clustering
5. make kpis
6. aggregation tables
7. create data marts
8. manage analytics and show them on the other pages
"""

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