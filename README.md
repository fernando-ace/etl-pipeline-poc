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

| YEAR | MAKE        | MODEL          | TRIM | BODY   | TRANSMISSION | VIN                | STATE | CONDITION | COLOR  | INTERIOR | SELLER                                           | MMR  | SELLINGPRICE | SALEDATE                     | SALE_DATE   | PROFIT | ODOMETER |
|------|-------------|----------------|------|--------|--------------|---------------------|-------|-----------|--------|----------|-------------------------------------------------|------|-------------|-------------------------------|-------------|--------|----------|
| 2005 | Mitsubishi  | Galant         | ES   | sedan | automatic    | 4a3ab36f75e066354  | mn    | 29        | silver | black    | onemain rem/us auto fleet group               | 1975 | 3100        | 2015-06-03T03:30:00-07:00   | 30:00.0     | 1125   | 130422   |
| 2005 | Mercury     | Grand Marquis  | GS   | sedan | automatic    | 2mefm74w45x611338  | fl    | 24        | white  | tan      | regional acceptance corporation / greenville | 3950 | 4100        | 2015-06-02T09:00:00-07:00   | 00:00.0     | 150    | 84204    |
| 2005 | Mazda       | Tribute        | s    | suv   | automatic    | 4f2cz06115km48407  | tx    | 29        | red    | black    | fairway ford henderson                       | 3550 | 4600        | 2015-06-03T03:00:00-07:00   | 00:00.0     | 1050   | 91127    |

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
