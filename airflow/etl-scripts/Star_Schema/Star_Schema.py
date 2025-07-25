import pandas as pd
import numpy as np
from faker import Faker
import os

# --- CONFIGURATION ---------------------------------------------------------------
# These paths are configured for the Docker container environment
# Airflow will execute this script inside the container where volumes are mounted
IN_CSV = "/opt/airflow/etl_scripts/Cleaned data/Cleaned_Nov.csv"
OUT_DIR = "/opt/airflow/etl_scripts/Star_Schema"

def create_dim_date(df):
    """Creates the date dimension from the event_time column."""
    print("Creating dim_date...")
    df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
    dates = df["event_time"].dt.floor("D")
    dim_date = (
        pd.DataFrame({"date": dates})
          .drop_duplicates("date")
          .assign(
              date_sk    = lambda x: x["date"].dt.strftime("%Y%m%d").astype(int),
              year       = lambda x: x["date"].dt.year,
              month      = lambda x: x["date"].dt.month,
              day_of_week= lambda x: x["date"].dt.dayofweek,
              is_weekend = lambda x: x["date"].dt.dayofweek.isin([5, 6])
          )
          .sort_values("date")
    )
    print(f"-> dim_date created with {len(dim_date)} rows.")
    return dim_date

def create_dim_session(df):
    """Creates the session dimension."""
    print("Creating dim_session...")
    dim_session = (
        df.groupby("user_session")["event_time"]
          .agg(session_start = "min",
               session_end   = "max",
               n_events      = "count")
          .assign(duration_s=lambda x: (x["session_end"]-x["session_start"])
                                       .dt.total_seconds())
          .reset_index()
          .rename(columns={"user_session": "session_sk"})
    )
    print(f"-> dim_session created with {len(dim_session)} rows.")
    return dim_session

def create_dim_product(df):
    """Creates the product dimension."""
    print("Creating dim_product...")
    dim_product = (
        df[["product_id", "category_id", "category_code", "brand"]]
          .drop_duplicates()
          .rename(columns={"product_id": "product_sk"})
    )
    print(f"-> dim_product created with {len(dim_product)} rows.")
    return dim_product

def create_dim_user(df):
    """Creates the user dimension with synthetic names."""
    print("Creating dim_user...")
    fake = Faker()
    dim_user = (
        df[["user_id"]]
          .drop_duplicates()
          .rename(columns={"user_id": "user_sk"})
          .sort_values("user_sk")
          .reset_index(drop=True)
    )
    fake.unique.clear()
    dim_user["user_name"] = [
        fake.unique.name() for _ in range(len(dim_user))
    ]
    print(f"-> dim_user created with {len(dim_user)} rows.")
    return dim_user

def create_dim_event_type(df):
    """Creates the event_type dimension and adds a surrogate key to the main DataFrame."""
    print("Creating dim_event_type...")
    codes, uniques = pd.factorize(df["event_type"], sort=True)
    dim_event_type = (
        pd.DataFrame({"event_type": uniques})
          .reset_index()
          .rename(columns={"index": "event_type_sk"})
    )
    df["event_type_sk"] = codes
    print(f"-> dim_event_type created with {len(dim_event_type)} rows.")
    return dim_event_type, df

def create_fact_events(df, dim_date):
    """Creates the final fact table by selecting and renaming foreign key columns."""
    print("Creating fact_events...")
    date_map = dict(zip(dim_date["date"], dim_date["date_sk"]))
    df["date_sk"] = df["event_time"].dt.floor("D").map(date_map)
    
    fact_events = df[[
        "user_session",
        "user_id",
        "product_id",
        "event_type_sk",
        "date_sk",
        "price",
        "is_purchase"
    ]].rename(columns={
        "user_session": "session_sk",
        "user_id":      "user_sk",
        "product_id":   "product_sk"
    })
    print(f"-> fact_events created with {len(fact_events)} rows.")
    return fact_events

def main():
    """
    Main function to orchestrate the star schema creation and save the tables as CSVs.
    """
    print("--- Starting Star Schema Generation Pipeline ---")
    
    # Ensure output directory exists
    os.makedirs(OUT_DIR, exist_ok=True)
    
    # Load the source cleaned data
    try:
        df = pd.read_csv(IN_CSV, parse_dates=["event_time"])
        print(f"Successfully loaded {len(df):,} rows from {IN_CSV}")
    except FileNotFoundError:
        print(f"ERROR: Input file not found at {IN_CSV}. Please adjust the IN_CSV variable.")
        return

    # --- Create Dimensions ---
    dim_date = create_dim_date(df)
    dim_session = create_dim_session(df)
    dim_product = create_dim_product(df)
    dim_user = create_dim_user(df)
    # This function modifies the main df to add the event_type_sk
    dim_event_type, df = create_dim_event_type(df)
    
    # --- Create Fact Table ---
    fact_events = create_fact_events(df, dim_date)

    # --- Save all tables to CSV ---
    print("\n--- Saving all tables to CSV files ---")
    tables_to_save = {
        "dim_date": dim_date,
        "dim_session": dim_session,
        "dim_product": dim_product,
        "dim_user": dim_user,
        "dim_event_type": dim_event_type,
        "fact_events": fact_events
    }
    
    for table_name, table_df in tables_to_save.items():
        output_path = f"{OUT_DIR}/{table_name}.csv"
        table_df.to_csv(output_path, index=False)
        print(f"Successfully wrote {table_name}.csv to {output_path}")
        
    print("\n--- Pipeline finished successfully! ---")

if __name__ == "__main__":
    # This makes the script runnable from the command line
    main()
