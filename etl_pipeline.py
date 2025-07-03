import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, current_timestamp, avg
from dateutil import parser

def safe_parse_date(date_str):
    """
    Attempts to parse a date string into ISO 8601 format.
    Returns None if parsing fails.
    """
    try:
        if date_str:
            return parser.parse(date_str).isoformat()
        else:
            return None
    except Exception:
        return None

def load_raw_data(session: snowpark.Session) -> snowpark.DataFrame:
    """
    Load raw vehicle sales data from Snowflake table.
    """
    return session.table("car_prices")

def clean_and_transform(df_raw: snowpark.DataFrame, session: snowpark.Session) -> snowpark.DataFrame:
    """
    Clean and transform the raw vehicle sales data.

    - Filters out rows missing critical pricing info
    - Parses and standardizes sale date timestamps
    - Calculates profit margin
    - Casts odometer to integer
    - Validates profit, odometer, and sale date ranges
    """
    # Filter rows missing pricing info
    df_filtered = df_raw.filter(
        (col("SELLINGPRICE").is_not_null()) & (col("MMR").is_not_null())
    )

    # Convert to pandas to safely parse SALEDATE strings
    pdf = df_filtered.to_pandas()

    # Parse SALEDATE to ISO 8601, handle parse failures gracefully
    pdf['SALEDATE'] = pdf['SALEDATE'].apply(safe_parse_date)

    # Log number of rows with unparseable SALEDATE
    invalid_dates = pdf['SALEDATE'].isnull().sum()
    print(f"INFO: Skipped {invalid_dates} rows due to invalid SALEDATE values.")

    # Recreate Snowpark DataFrame from cleaned pandas DF
    df_cleaned = session.create_dataframe(pdf)

    # Apply transformations
    df_transformed = (
        df_cleaned.withColumn("SALE_DATE", col("SALEDATE").cast("timestamp_tz"))
                  .withColumn("PROFIT", col("SELLINGPRICE") - col("MMR"))
                  .withColumn("ODOMETER", col("ODOMETER").cast("int"))
    )

    # Validate data: profit >= 0, odometer >= 0, sale_date not in the future
    df_validated = df_transformed.filter(
        (col("PROFIT") >= 0) &
        (col("ODOMETER") >= 0) &
        (col("SALE_DATE") <= current_timestamp())
    )

    return df_validated

def save_clean_data(df: snowpark.DataFrame):
    """
    Save the cleaned and validated data into a Snowflake table.
    """
    df.write.mode("overwrite").save_as_table("clean_vehicle_sales")
    print(f"INFO: Saved cleaned data to 'clean_vehicle_sales' table. Row count: {df.count()}")

def main(session: snowpark.Session) -> snowpark.DataFrame:
    """
    ETL pipeline to clean and transform vehicle sales data in Snowflake.

    Steps:
    - Load raw data from 'car_prices' table
    - Filter rows missing key pricing info
    - Parse and standardize sale date timestamps with timezone
    - Calculate profit margin as selling price minus MMR
    - Cast odometer readings to integers
    - Validate data quality constraints
    - Save cleaned data to 'clean_vehicle_sales' table

    Args:
        session (snowpark.Session): Snowflake Snowpark session

    Returns:
        snowpark.DataFrame: Transformed DataFrame with cleaned and validated data
    """
    # Load raw data
    df_raw = load_raw_data(session)

    # Clean and transform
    df_cleaned = clean_and_transform(df_raw, session)

    # Save results
    save_clean_data(df_cleaned)

    # Show sample results
    df_cleaned.show(10)

    return df_cleaned
