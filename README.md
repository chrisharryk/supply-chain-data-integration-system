# Supply Chain Analytics Dashboard

An integrated ETL pipeline for analyzing **sales**, **inventory**, and **product performance** using **BigQuery** and **Streamlit**.

This dashboard provides real-time insights to monitor and optimize supply chain operations, helping businesses track performance, improve vendor relationships, and manage inventory efficiently.

---

## **Features**
- **Key Performance Indicators (KPIs)**:
    - Track **total sales**, **order count**, **average order value**, and other essential metrics.
    - Analyze **order fulfillment efficiency** and **lead times**.
  
- **Aggregations & Trends**:
    - View **monthly sales trends** and **customer segment performance**.
    - Identify **top-selling products** and **demand patterns**.

- **Data Marts & Advanced Insights**:
    - Analyze **order fulfillment**, **inventory levels**, and **shipping logistics**.
    - Use **partitioning** and **clustering** to optimize query performance.

- **Performance Optimization**:
    - **Partitioning**: Optimizes fact tables using date-based partitions to reduce query costs.  
    - **Clustering**: Speeds up filtering and sorting on frequently queried columns.  

---

## **Setup Instructions**

### 1. **Clone the Repository**
```bash
git clone https://github.com/your-username/supply-chain-dashboard.git
cd supply-chain-dashboard
```
### 2. Configure BigQuery Credentials
Ensure your Google Cloud service account key along with the project ID is set up for BigQuery access:
```bash
gcloud init
gcloud auth application-default login
```
### 3. Install the requirements
Use the requirements.txt file to install the dependencies
```bash
pip install -r requirements.txt
```
### 4. Run the Streamlit App
```bash
streamlit run main.py
```