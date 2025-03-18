from modules.etl_pipeline import *

def main():
    fetch_kaggle_data('rohitsahoo/sales-forecasting')
    df = preprocess_data('./data/train.csv')
    df_fact, df_orders, df_shipping, df_customers, df_regions, df_products = create_fact_and_dimensions(df)
    tables = {
        'fact_sales': df_fact,
        'dim_orders': df_orders,
        'dim_shipping': df_shipping,
        'dim_customers': df_customers,
        'dim_regions': df_regions,
        'dim_products': df_products
    }
    project_id = "learned-spider-453507-p8"
    dataset_id = "sales_analysis"
    push_to_bigquery(tables, project_id, dataset_id)

if __name__ == '__main__':
    main()