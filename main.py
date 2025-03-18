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
    # project_id = "learned-spider-453507-p8"
    # dataset_id = "sales_analysis"
    push_to_bigquery(tables)
    create_kpi_procedures()
    mp = execute_all_kpis()
    # for k, v in mp.items():
    #     print(v.head())
    # df = execute_kpi_procedure('avg_order_frequency_by_customer', 'kpi_avg_order_frequency_by_customer')
    # print(df.head())
    create_aggregation_procedures()
    mp = execute_all_aggregations()
    # for k, v in mp.items():
    #     print(v.head())
    #     print()
    execute_partitioning_and_clustering()
    create_data_marts()
    # df_inventory = fetch_data_mart("mart_inventory_analysis")
    # df_order_fulfillment = fetch_data_mart("mart_order_fulfillment")
    # df_shipping_logistics = fetch_data_mart("mart_shipping_logistics")
    # print(df_inventory.head(), '\n', df_order_fulfillment.head(), '\n', df_shipping_logistics.head())

if __name__ == '__main__':
    main()