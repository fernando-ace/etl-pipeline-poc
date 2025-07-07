import pandas as pd
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import (
    col, current_timestamp, to_timestamp_tz, lit, when, upper, length
)
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

def fill_unknown_extended(col_name):
    """
    Helper to fill null, empty, or non-standard missing placeholders with 'unknown'.
    Treats: NULL, '', '—', '-', 'N/A', 'None' as missing.
    """
    missing_values = ["—", "-", "N/A", "None"]
    return when(
        (col(col_name).isNull()) |
        (col(col_name) == "") |
        (col(col_name).isin(missing_values)),
        lit("unknown")
    ).otherwise(col(col_name))

def load_raw_data(session: snowpark.Session) -> snowpark.DataFrame:
    """
    Load raw vehicle sales data from Snowflake table.
    """
    return session.table("car_prices")

def clean_and_transform(df_raw: snowpark.DataFrame, session: snowpark.Session) -> snowpark.DataFrame:
    """
    Clean and transform the raw vehicle sales data.

    Includes:
    - Fill missing descriptive fields with "unknown" including non-standard placeholders
    - Fill missing transmission with "unknown"
    - Uppercase state codes
    - Title case seller names
    - Filter invalid VINs (length must be 17)
    - Standardize SALEDATE timestamp
    - Calculate profit margin
    - Cast odometer to integer
    - Validate profit, odometer, sale_date ranges
    """
    # Fill missing descriptive fields with extended logic
    df_filled = df_raw.withColumn("MAKE", fill_unknown_extended("MAKE")) \
                      .withColumn("MODEL", fill_unknown_extended("MODEL")) \
                      .withColumn("TRIM", fill_unknown_extended("TRIM")) \
                      .withColumn("BODY", fill_unknown_extended("BODY")) \
                      .withColumn("COLOR", fill_unknown_extended("COLOR")) \
                      .withColumn("INTERIOR", fill_unknown_extended("INTERIOR")) \
                      .withColumn(
                          "TRANSMISSION",
                          when(
                              (col("TRANSMISSION").isNull()) |
                              (col("TRANSMISSION") == "") |
                              (col("TRANSMISSION").isin(["—", "-", "N/A", "None"])),
                              lit("unknown")
                          ).otherwise(col("TRANSMISSION"))
                      )

    # Uppercase state codes
    df_state_upper = df_filled.withColumn("STATE", upper(col("STATE")))

    # Title case seller names (using UDF)
    def title_case_seller(seller_name):
        if seller_name:
            return " ".join(word.capitalize() for word in seller_name.split())
        else:
            return None

    title_case_udf = session.udf.register(
        func=title_case_seller,
        return_type=snowpark.types.StringType(),
        input_types=[snowpark.types.StringType()]
    )

    df_seller_title = df_state_upper.withColumn("SELLER", title_case_udf(col("SELLER")))

    # Filter invalid VINs (length must be 17)
    df_valid_vin = df_seller_title.filter(col("VIN").isNotNull() & (length(col("VIN")) == 17))

    # Filter rows missing critical pricing info
    df_filtered = df_valid_vin.filter(
        (col("SELLINGPRICE").is_not_null()) & (col("MMR").is_not_null())
    )

    # Convert to pandas for safe date parsing
    pdf = df_filtered.to_pandas()
    pdf['SALEDATE'] = pdf['SALEDATE'].apply(safe_parse_date)

    invalid_dates = pdf['SALEDATE'].isnull().sum()
    print(f"INFO: Skipped {invalid_dates} rows due to invalid SALEDATE values.")
    pdf = pdf[pd.notnull(pdf['SALEDATE'])]

    df_cleaned = session.create_dataframe(pdf)

    df_transformed = (
        df_cleaned.withColumn("SALE_DATE", to_timestamp_tz(col("SALEDATE")))
        .withColumn("PROFIT", col("SELLINGPRICE") - col("MMR"))
        .withColumn("ODOMETER", col("ODOMETER").cast("int"))
    )

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

def main(session: snowpark.Session):
    """
    Snowflake worksheet entry point.
    """
    df_raw = load_raw_data(session)
    df_cleaned = clean_and_transform(df_raw, session)
    save_clean_data(df_cleaned)
    df_cleaned.show(10)
    return df_cleaned
