# Data Warehouse Project

Data warehouse built using SQL and following a layered architecture pattern.

## Overview

This repository contains a data warehouse implementation, including ETL processes, data modeling (star schema) and analytics-ready structures using postgreSQL.

## Dataset Reference

Original dataset and project guidance from [DataWithBaraa](https://github.com/DataWithBaraa/sql-data-warehouse-project)

## Repository Structure

data-warehouse-project/
├── datasets/ # Raw data 
├── docs/ # Architecture diagrams, data catalog, data flows, naming conventions
├── scripts/ # SQL ETL scripts: bronze → silver → gold
├── tests/ # Data quality checks and validation scripts
└── README.md # This documentation

## Installation & Setup

1. Clone the repository:
   git clone https://github.com/diogoo-lemos/data-warehouse-project.git

2. Place the datasets/ folder files (ERP/CRM CSVs) into your local datasets directory.

3. Set Up Your Database
  Create a new database in a PostgreSQL environment.
  Update any connection details in your ETL scripts or config files.
  Ensure the database user has permissions to:
    Create tables
    Insert data
    Run transformations

4. Run the SQL scripts sequentially:
  Create the Schemas (create_schemas.sql)
  Create bronze layer tables (create_bronze_tables.sql) and load the data by running load_bronze_procedure.sql and using the command CALL bronze.load_bronze()
  Create silver layer tables (create_silver_tables.sql) and load and transform the data by running load_silver_procedure.sql and using the command CALL silver.load_silver()
  Run create_gold_views to generate the views that contain the analytics_ready data.

## Architecture & Design

Bronze Layer: Ingest raw CSV files into staging tables.

Silver Layer: Clean, normalize, and enrich the data from the bronze layer.

Gold Layer: Organize into star schema for analytics-ready-data.

## Thank you!!
