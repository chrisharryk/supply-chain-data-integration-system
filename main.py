from matplotlib import pyplot as plt
import streamlit as st
import seaborn as sns
import plotly.express as px
from modules.aggregation_tabs import create_aggregation_procedures, execute_aggregation_procedure, execute_all_aggregations
from modules.data_extraction_and_transformation import *
from modules.data_mart_tabs import create_data_marts, fetch_data_mart
from modules.kpi_tabs import create_kpi_procedures, execute_all_kpis, execute_kpi_procedure
from modules.pushing_to_bigquery import push_to_bigquery

@st.cache_data
def get_data_mart(mart_name):
    return fetch_data_mart(mart_name)

def main():
    """
    pipeline flow :-
    1. fetching data from api
    2. preprocessing
    3. creating facts and dims
    4. pushing to bigquery
    5. creating aggregate procs
    6. executing all aggregates
    7. creating kpi procs
    8. executing all procs
    9. partitioning fact tab
    10. clustering
    11. creating data marts
    12. fetching data marts
    """

    # Initializing session state for pulling and pre-processing data
    if "step_1_done" not in st.session_state:
        st.session_state.step_1_done = False
    if "step_2_3_done" not in st.session_state:
        st.session_state.step_2_3_done = False
    if "step_4_done" not in st.session_state:
        st.session_state.step_4_done = False
    
    st.title('Supply Chain Analytics Dashboard')
    st.sidebar.title("Supply Chain Analysis")
    # main sections
    section = st.sidebar.radio("Go to", ["ETL", "EDA", "Schema", "Analysis"])

    if section == 'ETL':
        # 1. fetching data from API
        dataset_link = st.text_input('Enter the Kaggle dataset link:')
        if st.button('Fetch Data'):
            if dataset_link:
                fetch_kaggle_data(dataset_link)
                st.success(f'Data fetched from {dataset_link}')
                st.session_state.step_1_done = True
            else:
                st.error('Please enter a valid link')

        # 2 & 3. preprocessing + creating facts and dims
        if st.session_state.step_1_done:
            csv_path = "./data/train.csv"
            df = preprocess_data(csv_path)
            df_fact, df_orders, df_shipping, df_customers, df_regions, df_products = create_fact_and_dimensions(df)
            st.success("Data pre-processed successfully!")
            st.session_state.tables = {
                'fact_sales': df_fact,
                'dim_orders': df_orders,
                'dim_shipping': df_shipping,
                'dim_customers': df_customers,
                'dim_regions': df_regions,
                'dim_products': df_products
            }
            st.session_state.step_2_3_done = True

        # 4. pushing to bigquery
        if st.session_state.step_2_3_done:
            if st.button("Push data to bigquery"):
                with st.spinner("Pushing data to bigquery..."):
                    push_to_bigquery(st.session_state.tables)
                st.success("Data pushed to bigquery successfully!")
                st.session_state.step_4_done = True
    
    elif section == 'EDA':
            # Subsection for EDA
        eda_section = st.sidebar.radio("Select Analysis Section:", 
                                    ["Inventory Analysis", "Order Fulfillment", "Shipping Logistics"])

        if st.button("Fetch Data Marts & Generate Analysis Report"):
            create_data_marts()
            df_inventory = get_data_mart("mart_inventory_analysis")
            df_order_fulfillment = get_data_mart("mart_order_fulfillment")
            df_shipping_logistics = get_data_mart("mart_shipping_logistics")
            st.success("Data Marts Fetched!")

            # Inventory Analysis
            if eda_section == "Inventory Analysis":
                st.subheader("Inventory Analysis")
                st.markdown("**Overview of inventory sales and order distribution.**")
                st.dataframe(df_inventory.head(10))  # Show limited rows

                # Pie Chart - Sales Revenue by Category
                st.subheader("Sales Revenue Distribution by Category")
                fig = px.pie(df_inventory, names="Category", values="total_sales_revenue", hole=0.3)
                st.plotly_chart(fig, use_container_width=True)

                # Bar Chart - Total Orders by Sub-Category
                st.subheader("Total Orders by Sub-Category")
                df_top_subcategories = df_inventory.groupby("Sub-Category")["total_orders"].sum().nlargest(10).reset_index()
                fig = px.bar(df_top_subcategories, x="Sub-Category", y="total_orders", text_auto=True)
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)

            # Order Fulfillment
            elif eda_section == "Order Fulfillment":
                st.subheader("Order Fulfillment")
                st.markdown("**Analysis of order fulfillment performance across different regions.**")
                st.dataframe(df_order_fulfillment.head(10))

                # Bar Chart - Total Sales by Region
                st.subheader("Total Sales by Region")
                fig = px.bar(df_order_fulfillment, x="region_name", y="total_sales", text_auto=True)
                st.plotly_chart(fig, use_container_width=True)

                # Pie Chart - Top 5 Customers by Sales
                st.subheader("Top 5 Customers by Sales Contribution")
                top_customers = df_order_fulfillment.groupby("customer_name")["total_sales"].sum().nlargest(5).reset_index()
                fig = px.pie(top_customers, names="customer_name", values="total_sales", hole=0.3)
                st.plotly_chart(fig, use_container_width=True)

            # Shipping Logistics
            elif eda_section == "Shipping Logistics":
                st.subheader("Shipping Logistics")
                st.markdown("**Insights into shipping performance, average delivery times, and regional sales.**")
                st.dataframe(df_shipping_logistics.head(10))

                # Line Chart - Average Shipping Days Trend (Sampled Data)
                st.subheader("Trend of Average Shipping Days Over Orders")
                df_sampled_shipping = df_shipping_logistics.sample(n=100, random_state=42)  # Reduce points plotted
                fig = px.line(df_sampled_shipping, x="order_id", y="avg_shipping_days", markers=True)
                st.plotly_chart(fig, use_container_width=True)

                # Bar Chart - Average Shipping Days by Ship Mode
                st.subheader("Average Shipping Days by Ship Mode")
                fig = px.bar(df_shipping_logistics, x="ship_mode", y="avg_shipping_days", text_auto=True)
                st.plotly_chart(fig, use_container_width=True)

                # Pie Chart - Sales Distribution by Region
                st.subheader("Sales Distribution by Region")
                fig = px.pie(df_shipping_logistics, names="region_name", values="total_sales", hole=0.3)
                st.plotly_chart(fig, use_container_width=True)

    elif section == 'Schema':
        st.subheader('Schema Overview')
        st.image("ER Diagram.svg", caption="ER Diagram", use_container_width=True)
        st.markdown("""
            ### **Fact Table: fact_sales**
            - Contains transactional data linked to dimensions via surrogate keys.
            - Uses integer-based surrogate keys instead of original categorical IDs for performance, storage efficiency, and consistency.

            ### **Dimension Tables**
            - **dim_orders** - Stores order-related details (Order ID, Order Date).
            - **dim_shipping** - Holds shipping details (Ship Date, Ship Mode).
            - **dim_customers** - Contains customer information (Customer ID, Name, Segment).
            - **dim_regions** - Stores geographic details (Country, City, State, Region, Postal Code).
            - **dim_products** - Includes product attributes (Product ID, Category, Sub-Category, Product Name).

            ### **Why Surrogate Keys?**
            - **Performance** - Joins on integers are faster than on string-based natural keys.
            - **Storage Efficiency** - Integers take up less space, making queries more efficient.
            - **Data Consistency** - Natural keys can change over time, but surrogate keys remain stable.

            ### **Schema Insights**
            - The **fact table** holds transactional data and links to dimensions via surrogate keys.
            - The **star schema** makes querying simple and fast, reducing complexity.
            - This setup is optimized for **sales and supply chain analytics**, making aggregations and reporting more efficient.
        """)

    elif section == "Analysis":
        analysis_subsection = st.sidebar.radio("Select Analysis Type", ["KPIs", "Aggregations"])
        if st.button('Invoke Procedures & Generate Analysis Report'):
            if analysis_subsection == "KPIs":
                st.subheader("Key Performance Indicators (KPIs)")
                st.markdown("**Overview of important supply chain KPIs.**")

                create_kpi_procedures()
                kpi_results = execute_all_kpis()

                for kpi_name, df_kpi in kpi_results.items():
                    st.subheader(f"{kpi_name.replace('_', ' ').title()}")
                    st.dataframe(df_kpi)

                    # KPI Charts
                    if "total_sales" in df_kpi.columns:
                        fig = px.bar(df_kpi, x=df_kpi.columns[0], y="total_sales", text_auto=True)
                        st.plotly_chart(fig, use_container_width=True)

                    if "total_revenue" in df_kpi.columns:
                        fig = px.pie(df_kpi, names=df_kpi.columns[0], values="total_revenue", hole=0.3)
                        st.plotly_chart(fig, use_container_width=True)

                    if "avg_order_value" in df_kpi.columns:
                        fig = px.bar(df_kpi, x=df_kpi.columns[0], y="avg_order_value", text_auto=True)
                        st.plotly_chart(fig, use_container_width=True)

                    if "lead_time_days" in df_kpi.columns:
                        fig = px.histogram(df_kpi, x="lead_time_days", nbins=20)
                        st.plotly_chart(fig, use_container_width=True)

                    if "avg_order_frequency" in df_kpi.columns:
                        fig = px.bar(df_kpi, x=df_kpi.columns[0], y="avg_order_frequency", text_auto=True)
                        st.plotly_chart(fig, use_container_width=True)

            elif analysis_subsection == "Aggregations":
                st.subheader("Aggregated Metrics")
                st.markdown("**Summarized insights from supply chain data.**")

                create_aggregation_procedures()
                agg_results = execute_all_aggregations()

                for agg_name, df_agg in agg_results.items():
                    st.subheader(f"{agg_name.replace('_', ' ').title()}")
                    st.dataframe(df_agg.head(10))

                    # Aggregation Charts
                    if "total_sales" in df_agg.columns:
                        fig = px.bar(df_agg, x=df_agg.columns[0], y="total_sales", text_auto=True)
                        st.plotly_chart(fig, use_container_width=True)

                    if "total_orders" in df_agg.columns:
                        fig = px.bar(df_agg, x=df_agg.columns[0], y="total_orders", text_auto=True)
                        st.plotly_chart(fig, use_container_width=True)

                    if "total_revenue" in df_agg.columns:
                        fig = px.pie(df_agg, names=df_agg.columns[0], values="total_revenue", hole=0.3)
                        st.plotly_chart(fig, use_container_width=True)


if __name__ == '__main__':
    main()