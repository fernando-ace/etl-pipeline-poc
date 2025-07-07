# ETL Pipeline Proof of Concept — Vehicle Sales Data

## Overview
This repository contains a proof-of-concept Python ETL pipeline that loads, cleans, and transforms vehicle sales data from Kaggle into Snowflake using Snowpark.

The pipeline:
- Parses and standardizes complex sale date formats
- Calculates profit margins (`SELLINGPRICE - MMR`)
- Casts odometer readings to integers
- Filters and validates data quality
- Saves cleaned data into a Snowflake table

## Dataset
- The original dataset is the **Car Prices** dataset downloaded from [Kaggle](https://www.kaggle.com/datasets).
- Because of size limits, this repo includes a **sample input CSV** (`car_prices_sample.csv`) with the top 10,000 rows for demonstration and testing purposes.
- You can download the full dataset from Kaggle if needed.

## Files
- `etl_pipeline.py` — Main Python ETL script designed to run inside Snowflake’s Snowpark environment.
- `car_prices_sample.csv` — Sample input vehicle sales data (first 10,000 rows).
- `output_sample.csv` — Sample output data (first 10,000 cleaned rows) written by the pipeline.
- `.env.example` — Template for environment variables needed to connect to Snowflake (optional).
- `README.md` — This file.

## Sample Output

Below is a sample of the cleaned and transformed data written by the ETL pipeline to the `clean_vehicle_sales` table in Snowflake.

See `output_sample.csv` in this repository for a full example of the output (first 10,000 rows).

| YEAR | VIN                | CONDITION | MMR   | SELLINGPRICE | SALEDATE                     | MAKE       | MODEL          | TRIM             | BODY       | COLOR  | INTERIOR | TRANSMISSION | STATE | SELLER                                 | SALE_DATE                       | PROFIT | ODOMETER |
|------|--------------------|-----------|-------|--------------|-------------------------------|------------|----------------|------------------|------------|--------|----------|--------------|-------|---------------------------------------|--------------------------------|--------|----------|
| 2010 | 2cnalfew9a6370363  | 34        | 15900 | 16750        | 2015-06-16T02:30:00-07:00    | Chevrolet  | Equinox        | LTZ              | suv        | gray   | black    | automatic    | OH    | Stratton Auto Sales Inc             | 2015-06-16 02:30:00.000 -0700  | 850    | 28374    |
| 2010 | 1ftfw1ev7afa46460  | 24        | 24900 | 27700        | 2015-06-18T02:30:00-07:00    | Ford       | F-150          | Harley-Davidson | supercrew  | black  | red      | automatic    | MI    | Multi Auto Trading Inc             | 2015-06-18 02:30:00.000 -0700  | 2800   | 66985    |
| 2010 | 1j4pn3gk2aw180907  | 39        | 11050 | 12000        | 2015-06-18T05:00:00-07:00    | Jeep       | Liberty        | Renegade         | suv        | green  | gray     | automatic    | AZ    | First Investors Servicing Corp     | 2015-06-18 05:00:00.000 -0700  | 950    | 80537    |
| 2015 | 1n4al3ap5fc148229  | 5         | 15750 | 16600        | 2015-06-16T04:00:00-07:00    | Nissan     | Altima         | 2.5 S            | sedan      | brown  | black    | automatic    | MO    | Jack Schmitt Cadillac Oldsmobile Inc | 2015-06-16 04:00:00.000 -0700  | 850    | 6617     |
| 2009 | 1yvhp81a895m29773  | 28        | 7475  | 8300         | 2015-06-16T02:30:00-07:00    | Mazda      | Mazda6         | i Sport          | sedan      | blue   | black    | automatic    | OH    | Pauley Motor Car Co Preowned Vehicles Llc | 2015-06-16 02:30:00.000 -0700  | 825    | 77319    |

These rows demonstrate that the pipeline:
- Parses complex `SALEDATE` strings into proper timestamps
- Computes `PROFIT` as `SELLINGPRICE - MMR`
- Casts `ODOMETER` to integer

## Setup & Usage
1. Create a `.env` file locally based on `.env.example`, and fill in your Snowflake credentials.
2. Run the ETL pipeline using Snowflake Snowpark.
3. The pipeline will create or overwrite a table named `clean_vehicle_sales` with cleaned and transformed data.

## Notes
- Make sure not to commit your real `.env` file with credentials.
- This is a prototype intended to be adapted to enterprise data workflows.

---

Feel free to reach out or open an issue if you have any questions!
