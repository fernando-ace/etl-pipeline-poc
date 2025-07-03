# ETL Pipeline Proof of Concept — Vehicle Sales Data

## Overview  
This repository contains a proof-of-concept Python ETL pipeline that loads, cleans, and transforms vehicle sales data from Kaggle into Snowflake using Snowpark.

The pipeline:  
- Parses and standardizes complex sale date formats  
- Calculates profit margins (selling price minus MMR)  
- Casts odometer readings to integers  
- Filters and validates data quality  
- Saves cleaned data into a Snowflake table

## Dataset  
- The original dataset is the **Car Prices** dataset downloaded from [Kaggle](https://www.kaggle.com/datasets).  
- Because of size limits, this repo includes a **sample CSV** (`car_prices_sample.csv`) with the top 10,000 rows for demonstration and testing purposes.  
- You can download the full dataset from Kaggle if needed.

## Files  
- `etl.py` — The main Python ETL script designed to run inside Snowflake’s Snowpark environment.  
- `car_prices_sample.csv` — Sample vehicle sales data subset from Kaggle.  
- `.env.example` — Template for environment variables needed to connect to Snowflake.  
- `README.md` — This file.

## Setup & Usage  
1. Create a `.env` file locally based on `.env.example`, and fill in your Snowflake credentials.  
2. Run the ETL pipeline using Snowflake Snowpark.  
3. The pipeline will create or overwrite a table named `clean_vehicle_sales` with cleaned and transformed data.

## Notes  
- Make sure not to commit your real `.env` file with credentials.  
- This is a prototype intended to be adapted to enterprise data workflows.

---

Feel free to reach out if you have any questions or want to contribute!

