# Azure Retail Data Engineering Pipeline

This project demonstrates an end-to-end data engineering pipeline built on Microsoft Azure to process and analyze retail sales data.  
The solution follows the **Medallion Architecture (Bronze → Silver → Gold)** using **Azure Data Lake, Databricks, Delta Lake, and Power BI**.

The pipeline transforms raw data into analytics-ready datasets and provides business insights through an interactive Power BI dashboard.

---

# Architecture

The pipeline architecture is designed using the modern **lakehouse approach**.
![Architecture Diagram](architecture Image.png)

Data Flow:

Raw Data (JSON / CSV)
        ↓
Azure Data Lake Storage Gen2
        ↓
Azure Databricks (PySpark ETL)
        ↓
Delta Lake
Bronze → Silver → Gold
        ↓
Power BI Dashboard

---

# Project Components

## 1 Data Ingestion
Raw retail datasets are stored in **Azure Data Lake Storage Gen2**.

The datasets include:

- Customers
- Products
- Stores
- Transactions

These datasets represent the **Bronze Layer** of the Medallion architecture.

---

## 2 Data Transformation (Databricks)

Using **PySpark in Azure Databricks**, the raw datasets are cleaned and transformed.

Steps performed:

- Data type conversion
- Removing duplicates
- Joining datasets
- Creating calculated columns
- Preparing analytics-ready data

The cleaned dataset forms the **Silver Layer**.

---

## 3 Aggregation Layer

The Silver data is aggregated to generate analytics datasets such as:

- Total revenue
- Sales by product
- Sales by store
- Category performance
- Sales trends over time

This becomes the **Gold Layer**, optimized for reporting.

---

## 4 Data Storage

All transformed datasets are stored in **Delta Lake format**, which provides:

- ACID transactions
- Schema enforcement
- Time travel
- Improved performance

---

## 5 Business Intelligence

Power BI is used to build a **Retail Sales Performance Dashboard**.

Key metrics include:

- Total Revenue
- Total Transactions
- Total Quantity Sold
- Average Order Value

Insights provided:

- Revenue trends over time
- Top-performing products
- Store performance
- Category contribution to revenue

---

# Dashboard Preview

![Retail Dashboard](screenshots/dashboard.png)

---

# Technologies Used

Azure Data Lake Storage Gen2  
Azure Databricks  
PySpark  
Delta Lake  
Power BI  
SQL  

---

# Key Features

End-to-end Azure data engineering pipeline  
Medallion architecture implementation  
Delta Lake storage format  
PySpark data transformations  
Interactive Power BI dashboard  

---

# Author

Alvin David
