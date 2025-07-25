# 0️⃣ Imports & settings
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for containers
import matplotlib.pyplot as plt
pd.set_option("display.max_columns", 120)

INPUT_PATH  = "/opt/airflow/etl_scripts/sample_Nov.csv"
CSV_OUT     = "/opt/airflow/etl_scripts/Cleaned data/Cleaned_Nov.csv"

# 2️⃣ Missing Values per Column
df = pd.read_csv(INPUT_PATH, parse_dates=["event_time"], low_memory=False)
print("Rows:", len(df))
df.info()

# basic stats
print("Data description:")
print(df.describe().T)
print("\nMissing values per column:")
print(df.isna().sum())
# Skip visualization for ETL pipeline - focus on data processing

# Identify all numeric columns in the cleaned DataFrame
num_cols = df.select_dtypes(include=["number"]).columns

# Analyze numeric column distributions (skip plotting for ETL)
print("Numeric columns identified:", list(num_cols))
print("Basic statistics for numeric columns:")
print(df[num_cols].describe())


# Compute counts for each event type
event_counts = df['event_type'].value_counts()

# Event type analysis (without plotting for ETL)
print("Event type counts:")
print(event_counts)

# null counts
nulls = df.isna().sum().sort_values(ascending=False)
print("Columns with missing values:")
print(nulls[nulls>0])

# cardinality check for object / category columns
cat_cols = [c for c in df.columns if df[c].dtype=="object"]
card = {c: df[c].nunique() for c in cat_cols}
print("Cardinality for categorical columns:")
print(pd.Series(card).sort_values(ascending=False))


# expected types for key columns – adjust to match your schema
schema = {
    "timestamp": "datetime64[ns]",
    "user_id"  : "Int64",
    "session_id": "string",
    "event_type": "category",
    "product_id": "string",
    "price"     : "float64",
    "quantity"  : "Int64"
}

# rename columns to snake_case
df.columns = df.columns.str.lower().str.replace(" ", "_")

# cast types
for col, dtype in schema.items():
    if col in df.columns:
        df[col] = df[col].astype(dtype)

print(df.columns.tolist())


# drop exact duplicates
df = df.drop_duplicates()

# drop if <5 % missing, else fill
threshold = 0.05 * len(df)
for col in df.columns:
    n_missing = df[col].isna().sum()
    if n_missing == 0:
        continue
    if n_missing < threshold:
        df = df[df[col].notna()]
    else:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(0)
        else:
            df[col] = df[col].fillna("unknown")
#after cleaning
print("Missing values after cleaning:")
print(df.isna().sum())

# keep last event per session as example business rule
if "session_id" in df.columns:
    df = df.sort_values("timestamp").drop_duplicates("session_id", keep="last")

# filter logical errors
if "price" in df.columns:
    df = df[df["price"] >= 0]
if "quantity" in df.columns:
    df = df[df["quantity"] >= 0]

# Compute correlation matrix for numeric fields
corr = df[num_cols].corr()

# Compute correlation matrix (skip visualization for ETL)
print("Correlation matrix computed for numeric columns")
print("Correlation matrix shape:", corr.shape)


print(df.columns.tolist())


# derived flags
if "event_type" in df.columns:
    df["is_purchase"] = (df["event_type"] == "purchase")

# time features
df["hour_of_day"] = df["event_time"].dt.hour
df["day_of_week"] = df["event_time"].dt.dayofweek
df["is_weekend"]  = df["day_of_week"].isin([5,6])

# session-level agg example (adds duration per session)
if "session_id" in df.columns:
    session_span = (
        df.groupby("session_id")["timestamp"]
          .agg(["min", "max"])  # first and last event per session
          .assign(duration_s=lambda x: (x["max"] - x["min"]).dt.total_seconds()) # Diffrence between first and last in seconds
    )
    df = df.merge(session_span["duration_s"], left_on="session_id", right_index=True, how="left")

# ─── Purchase Flag Distribution ──────────────────────────────────────────
# Bar chart showing how many events are purchases vs non-purchases
import matplotlib.pyplot as plt

# Count True/False in the 'is_purchase' feature
purchase_counts = df['is_purchase'].value_counts()

# Purchase analysis (skip plotting for ETL)
print("Purchase analysis completed")
print("Purchase distribution:", purchase_counts.to_dict())

# ─── Events by Hour of Day ───────────────────────────────────────────────
# Analyze event volume across hours (skip plotting for ETL)
hourly_events = df['hour_of_day'].value_counts().sort_index()
print("Events by hour distribution completed")

# ─── Purchase Rate by Hour of Day ────────────────────────────────────────
# Calculate hourly purchase rate (skip plotting for ETL)
hourly = df.groupby('hour_of_day')['is_purchase'].mean()
print("Hourly purchase rate analysis completed")


# ─── Weekend vs Weekday Event Counts ────────────────────────────────────
# Compare total events on weekends vs weekdays
counts = df['is_weekend'].value_counts()

# Weekend vs weekday analysis (skip plotting for ETL)
print("Weekend vs weekday event counts:")
print(counts)


assert df["price"].min() >= 0, "Negative prices detected!"
if 'quantity' in df.columns:
    assert df['quantity'].min() >= 0, "Negative quantities detected!"
if "duration_s" in df.columns:
    assert (df["duration_s"] >= 0).all(), "Negative session durations!"

print("✅ All validation checks passed.")

df.to_csv(CSV_OUT, index=False)
print(f"saved to {CSV_OUT}")
