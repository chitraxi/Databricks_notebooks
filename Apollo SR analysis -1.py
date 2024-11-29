# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import pyspark.sql.types as T
from pyspark.sql.window import Window as W
from datetime import datetime
import pandas as pd

# Create Spark session
spark = SparkSession.builder.appName("Optimizer Apollo SR insights script").getOrCreate()

# Define date ranges
start_date = '2024-02-01'
end_date = '2024-02-29'
previous_start_date = '2024-01-01'
previous_end_date = '2024-01-31'

# Load data into DataFrame
df_base_data = spark.sql(f"""
    SELECT
        a.merchant_id, 
        COALESCE(a.dba, b.billing_label, b.name) AS name,
        CASE 
            WHEN a.internal_external_flag = 'external' THEN gateway
            WHEN a.internal_external_flag = 'internal' THEN 'Razorpay'
            ELSE NULL
        END AS aggregator,
        a.method_advanced,
        a.network,
        a.network_tokenised_payment,
        IF(a.internal_error_code = '' OR a.internal_error_code IS NULL, 'Success', a.internal_error_code) AS error_code,
        a.wallet,
        a.bank,
        CASE
            WHEN a.internal_error_code IN (
                'list of error codes'
            ) THEN 'customer'
            WHEN a.internal_error_code IN (
                'list of error codes'
            ) THEN 'internal'
            WHEN a.internal_error_code IN (
                'list of error codes'
            ) THEN 'gateway'
            ELSE 'business'
        END AS error_source,
        a.internal_error_code,
        a.created_date,
        CASE 
            WHEN cast(a.created_date as date) BETWEEN date '{start_date}' AND date '{end_date}' THEN 'T-2'
            WHEN cast(a.created_date as date) BETWEEN date '{previous_start_date}' AND date '{previous_end_date}' THEN 'T-1'
            ELSE NULL
        END AS period,
        COUNT(DISTINCT CASE WHEN authorized_at IS NOT NULL THEN a.id ELSE NULL END) AS payment_success,
        COUNT(DISTINCT a.id) AS payment_attempts
    FROM 
        (
            SELECT * 
            FROM aggregate_ba.payments_optimizer_flag_sincejan2021 
            WHERE created_date BETWEEN '{previous_start_date}' AND '{end_date}' 
            AND optimizer_flag = 1 
            AND golive_date IS NOT NULL 
            AND gateway IS NOT NULL 
            AND method_advanced NOT IN ("Unknown Card", "upi", "card", "paylater", "app", "UPI Unknown")
        ) a 
        INNER JOIN realtime_hudi_api.merchants b 
        ON a.merchant_id = b.id 
    GROUP BY 
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
""")

# Convert Spark DataFrame to Pandas DataFrame

df_base_data = df_base_data.fillna('na')
df_data = df_base_data.toPandas()






# COMMAND ----------

print(df_data)

# COMMAND ----------

df_data.shape

# COMMAND ----------

drilled_SR_df = df_data
drilled_SR = drilled_SR_df.pivot_table(
    index=["method_advanced", "aggregator", "merchant_id", "name", "network", "network_tokenised_payment", "bank"],
    columns="period",
    values=["payment_attempts", "payment_success"],
    aggfunc="sum"  # Changed from sum to "sum"
).fillna(0)


# COMMAND ----------

print(drilled_SR)

# COMMAND ----------

# Rename columns
#drilled_SR.columns = [f"{col[0]}_{col[1]}" for col in drilled_SR.columns]

drilled_SR_reset = drilled_SR.reset_index()

# COMMAND ----------

print(drilled_SR_reset.columns)

# COMMAND ----------

drilled_SR_reset

# COMMAND ----------

print(drilled_SR_reset.columns)
print(drilled_SR_reset.index)

# COMMAND ----------


# Calculate success rates for T-2 and T-1
drilled_SR_reset["T-2 SR%"] = round((drilled_SR_reset["payment_success"]["T-2"] / drilled_SR_reset["payment_attempts"]["T-2"]) * 100, 2)
drilled_SR_reset["T-1 SR%"] = round((drilled_SR_reset["payment_success"]["T-1"] / drilled_SR_reset["payment_attempts"]["T-1"]) * 100, 2)



# COMMAND ----------

# Create DataFrame from data
SR = drilled_SR_df.pivot_table(
    index=["method_advanced", "aggregator"],
    columns="period",
    values=["payment_success", "payment_attempts"],
    aggfunc=sum
).fillna(0)

SR

# COMMAND ----------

print(SR.columns)

# COMMAND ----------

sr_reset = SR.reset_index()

# COMMAND ----------

sr_reset

# COMMAND ----------


print(sr_reset.columns)


# Flatten the column names by joining the levels with underscores
#SR.columns = [f"{col[0]}_{col[1]}" for col in SR.columns]

print(SR.columns)

# Calculate Success Rates for T-2 and T-1
sr_reset["T-2_SR%_agg"] = round((sr_reset["payment_success"]["T-2"] / sr_reset["payment_attempts"]["T-2"]) * 100, 4)
sr_reset["T-1_SR%_agg"] = round((sr_reset["payment_success"]["T-1"] / sr_reset["payment_attempts"]["T-1"]) * 100, 2)
sr_reset["total_agg"] = sr_reset["payment_attempts"]["T-1"] + sr_reset["payment_attempts"]["T-2"]
sr_reset["Delta_SR_agg"] = round(sr_reset["T-1_SR%_agg"] - sr_reset["T-2_SR%_agg"], 2)

# Filter for Delta SR < -1 and total > 1000
SR_filtered = sr_reset[(sr_reset["Delta_SR_agg"] < -1) & (sr_reset["total_agg"] > 1000)]

SR_filtered



# COMMAND ----------

print(SR_filtered.columns)
print(drilled_SR_reset.columns)

# COMMAND ----------

# Check index names of drilled_SR and SR_filtered DataFrames
drilled_index_names = set(drilled_SR_reset.index.names)
SR_index_names = set(SR_filtered.index.names)

print(drilled_index_names)

print(SR_index_names)


# Merge drilled_SR with SR DataFrame based on index
merged_df = drilled_SR_reset.merge(SR_filtered,
                              on=['method_advanced', 'aggregator'], how="inner")

merged_df

# COMMAND ----------



# Identify rows where payment_attempts_x for T-1 is zero
zero_attempts_mask = merged_df["payment_attempts_x"]["T-1"] == 0

# Update values in "T-1 SR%" based on the condition
merged_df.loc[zero_attempts_mask, "T-1 SR%"] = merged_df.loc[zero_attempts_mask, "T-2 SR%"]

# Identify rows where payment_attempts_x for T-2 is zero
zero_attempts_mask = merged_df["payment_attempts_x"]["T-2"] == 0

# Update values in "T-2 SR%" based on the condition
merged_df.loc[zero_attempts_mask, "T-2 SR%"] = merged_df.loc[zero_attempts_mask, "T-1 SR%"]

merged_df["%total_T-1"] = round((merged_df["payment_attempts_x"]["T-1"] / merged_df["payment_attempts_y"]["T-1"]), 4)
merged_df["%total_T-2"] = round((merged_df["payment_attempts_x"]["T-2"] / merged_df["payment_attempts_y"]["T-2"]), 4)

merged_df

# COMMAND ----------

merged_df

# COMMAND ----------

merged_df["VMM_SR"] = round((merged_df["%total_T-2"] * (merged_df["T-1 SR%"] - merged_df["T-2 SR%"])), 2)
merged_df["VMM_TX"] = round((merged_df["T-1 SR%"] * (merged_df["%total_T-1"] - merged_df["%total_T-2"])) , 2)

# COMMAND ----------

merged_df

# COMMAND ----------

merged_df.to_csv("/dbfs/FileStore/Chitraxi/merged_df_v1.csv")

spark.read.format('csv').load('dbfs:/FileStore/Chitraxi/merged_df_v1.csv').display()

# COMMAND ----------

merged_df.columns

# COMMAND ----------

# Group by 'method_advanced' and 'aggregator' and select the smallest 5 records based on 'VMM_SR'
top5_vmm_sr = merged_df.groupby(["method_advanced", "aggregator"]).apply(lambda x: x.nsmallest(5, "VMM_SR"))

# Reset index to remove the multi-index structure
#top5_vmm_sr.reset_index(inplace=True, drop=True)

# Print the resulting DataFrame
print(top5_vmm_sr)

# COMMAND ----------

top5_vmm_sr

# COMMAND ----------

top5_vmm_sr.to_csv("/dbfs/FileStore/Chitraxi/top5_vmm_sr.csv")

spark.read.format('csv').load('dbfs:/FileStore/Chitraxi/top5_vmm_sr.csv').display()

# COMMAND ----------

# Assuming merged_df is your DataFrame
column_names_level1 = top5_vmm_sr.columns.get_level_values(0)
print(column_names_level1)

# COMMAND ----------

# Assuming df is your DataFrame
column_headers = top5_vmm_sr.columns.tolist()

# Print the column headers
print(column_headers)

# COMMAND ----------

top5_vmm_sr_reset = top5_vmm_sr.reset_index()

print(top5_vmm_sr_reset)

# COMMAND ----------

print(top5_vmm_sr_reset)

# COMMAND ----------

top5_vmm_sr_reset

# COMMAND ----------




