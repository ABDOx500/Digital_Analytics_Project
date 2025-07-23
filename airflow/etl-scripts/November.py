# 0️⃣ Imports & settings
import pandas as pd
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
pd.set_option("display.max_columns", 120)

INPUT_PATH  = "D:/Intership_Berexia/Digital_Analytics_Project/airflow/etl-scripts/sample_Nov.csv"
CSV_OUT     = "D:/Intership_Berexia/Digital_Analytics_Project/airflow/etl-scripts"

# 2️⃣ Missing Values per Column
df = pd.read_csv(INPUT_PATH, parse_dates=["event_time"], low_memory=False)
print("Rows:", len(df))
df.info()

# basic stats
display(df.describe().T)
plt.figure()
df.isna().sum().plot(kind="bar")
plt.title("Missing Values per Column")
plt.xlabel("Column")
plt.ylabel("Count")
plt.tight_layout()
plt.show()

# Identify all numeric columns in the cleaned DataFrame
num_cols = df.select_dtypes(include=["number"]).columns

# Plot histograms for each numeric column to see their distributions
plt.figure()
df[num_cols].hist(bins=30)
plt.suptitle("Histogram of Numerical Features")
plt.tight_layout()
plt.show()


# Compute counts for each event type
event_counts = df['event_type'].value_counts()

# Plot as a simple bar chart
plt.figure()
event_counts.plot(kind='bar')
plt.title("Counts of Each Event Type")
plt.xlabel("Event Type")
plt.ylabel("Frequency")
plt.xticks(rotation=0)
plt.tight_layout()
plt.show()

# null counts
nulls = df.isna().sum().sort_values(ascending=False)
display(nulls[nulls>0])

# cardinality check for object / category columns
cat_cols = [c for c in df.columns if df[c].dtype=="object"]
card = {c: df[c].nunique() for c in cat_cols}
display(pd.Series(card).sort_values(ascending=False))


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
plt.figure()
df.isna().sum().plot(kind="bar")
plt.title("Missing Values per Column")
plt.xlabel("Column")
plt.ylabel("Count")
plt.tight_layout()
plt.show()

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

# Display the correlation matrix as a heatmap
plt.figure(figsize=(6, 5))
plt.imshow(corr, aspect="auto", cmap="viridis")
plt.colorbar(label="Correlation")
plt.title("Correlation Matrix")
plt.xticks(range(len(num_cols)), num_cols, rotation=45)
plt.yticks(range(len(num_cols)), num_cols)
plt.tight_layout()
plt.show()


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

plt.figure()
purchase_counts.plot(kind='bar', color=['gray','blue'])
plt.title("Distribution of Purchases vs Non-Purchases")
plt.xlabel("Is Purchase")
plt.ylabel("Event Count")
plt.xticks([0,1], ['No', 'Yes'], rotation=0)
plt.tight_layout()
plt.show()


# ─── Events by Hour of Day ───────────────────────────────────────────────
# Bar chart of event volume across each hour to see daily traffic patterns
plt.figure()
df['hour_of_day'].value_counts().sort_index().plot(kind='bar')
plt.title("Events by Hour of Day")
plt.xlabel("Hour (0–23)")
plt.ylabel("Number of Events")
plt.tight_layout()
plt.show()


# ─── Purchase Rate by Hour of Day ────────────────────────────────────────
# Line plot of the proportion of events that are purchases, by hour
hourly = df.groupby('hour_of_day')['is_purchase'].mean()

plt.figure()
hourly.plot(marker='o')
plt.title("Hourly Purchase Rate")
plt.xlabel("Hour of Day")
plt.ylabel("Purchase Rate")
plt.xticks(range(0,24))
plt.tight_layout()
plt.show()


# ─── Weekend vs Weekday Event Counts ────────────────────────────────────
# Compare total events on weekends vs weekdays
counts = df['is_weekend'].value_counts()

plt.figure()
counts.plot(kind='bar', color=['green','orange'])
plt.title("Event Count: Weekday vs Weekend")
plt.xlabel("Is Weekend")
plt.ylabel("Number of Events")
plt.xticks([0,1], ['Weekday','Weekend'], rotation=0)
plt.tight_layout()
plt.show()


assert df["price"].min() >= 0, "Negative prices detected!"
if 'quantity' in df.columns:
    assert df['quantity'].min() >= 0, "Negative quantities detected!"
if "duration_s" in df.columns:
    assert (df["duration_s"] >= 0).all(), "Negative session durations!"

print("✅ All validation checks passed.")

df.to_csv(CSV_OUT, index=False)
print(f"saved to {CSV_OUT}")
