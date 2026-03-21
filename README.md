# E-commerce-Sales-Data-Warehouse
E-commerce Sales Data Warehouse using SQL Server, SSIS, and Power BI

## Business Process:
* Online product sales transactions

## Dataset:
* Olist E-commerce Dataset


--

## bronze - raw imported data

## silver - cleaned data

## gold - dimensional model

## etl - ETL logs



        ┌────────────────────┐
        │   Source System    │
        │   (CSV Files)      │
        └────────┬───────────┘
                 │
                 ▼
        ┌────────────────────┐
        │  SSIS Package 01   │
        │  Load Bronze       │
        └────────┬───────────┘
                 │
                 ▼
        ┌────────────────────┐
        │   Bronze Layer     │
        │ (Raw Data Tables)  │
        └────────┬───────────┘
                 │
                 ▼
        ┌────────────────────┐
        │  SSIS Package 02   │
        │ Transform → Silver │
        └────────┬───────────┘
                 │
                 ▼
        ┌────────────────────┐
        │   Silver Layer     │
        │ (Cleaned Data)     │
        └────────┬───────────┘
                 │
                 ▼
        ┌────────────────────┐
        │  SSIS Package 03   │
        │ Load Dimensions    │
        └────────┬───────────┘
                 │
                 ▼
        ┌────────────────────┐
        │   Gold Layer       │
        │ (Dim Tables)       │
        └────────┬───────────┘
                 │
                 ▼
        ┌────────────────────┐
        │  SSIS Package 04   │
        │ Load Facts         │
        └────────┬───────────┘
                 │
                 ▼
        ┌────────────────────┐
        │   Fact Tables      │
        │ (Business Metrics) │
        └────────┬───────────┘
                 │
                 ▼
        ┌────────────────────┐
        │   Reporting Layer  │
        │ (Power BI / SQL)   │
        └────────────────────┘
