# Databricks notebook source
import shutil
from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from IPython.display import display, HTML
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("Rule intelligence") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .config("spark.sql.parquet.dictionary.encoding.enabled", "false") \
    .getOrCreate()


spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "20000")
spark.conf.set("spark.databricks.io.cache.enabled", "true")





# COMMAND ----------

#Define Rank based on percentile value
percentile = 'percentile_50'


df_base_data = spark.sql(f"""
WITH aggregated_data AS (
  SELECT
    DISTINCT merchants_id,
    CASE
      WHEN terminals_procurer = 'razorpay' THEN 'razorpay'
      ELSE payments_gateway
    END AS agg,
    CASE
      WHEN payments_method = 'card' AND cards_type = 'debit' OR cards_type = 'Debit' THEN 'Debit Card'
      WHEN payments_method = 'card' AND cards_type = 'credit' OR cards_type = 'Credit' THEN 'Credit Card'
      WHEN payments_method = 'card' AND cards_type = 'prepaid' OR cards_type = 'Prepaid' THEN 'Prepaid Card'
      WHEN (cards_type IS NULL OR cards_type = '') AND payments_method = 'card' THEN 'Unknown Card'
      WHEN payments_method = 'emi' AND cards_type = 'debit' OR cards_type = 'Debit' THEN 'DC emi'
      WHEN payments_method = 'emi' AND cards_type = 'credit' OR cards_type = 'Credit' THEN 'CC emi'
      WHEN payments_method = 'emi' AND cards_type = 'prepaid' OR cards_type = 'Prepaid' THEN 'Prepaid Card emi'
      WHEN upi_type = 'collect' THEN 'UPI Collect'
      WHEN upi_type = 'pay' OR upi_type = 'PAY' THEN 'UPI Intent'
      WHEN upi_type IS NULL AND payments_method = 'upi' THEN 'UPI Unknown'
      ELSE payments_method
    END AS method,
    cards_network,
    CASE
      WHEN cards_trivia IS NULL OR cards_trivia = '2' THEN '0'
      ELSE cards_trivia
    END AS tokenization_flag,
    COALESCE(payments_wallet, 'na') AS payments_wallet,
    payments_international,
    CASE
      WHEN payments_method = 'card' AND payments.cards_issuer IS NOT NULL AND optimizer_top_issuer.cards_issuer IS NULL THEN 'others'
      ELSE optimizer_top_issuer.cards_issuer
    END AS top_card_issuer,
    COALESCE(
      CASE
        WHEN payments_method = 'netbanking' AND payments.payments_bank IS NOT NULL AND optimizer_top_banks.bank_group IS NULL THEN 'others'
        ELSE optimizer_top_banks.bank_group
      END,
      'na'
    ) AS top_card_banks,
    --CASE
      --WHEN payment_analytics_platform = 1 AND payment_analytics_device IN (2, 3) THEN 'mobile_browser'WHEN payment_analytics_platform = 2 THEN 'mobile_sdk' WHEN payment_analytics_platform = 1 AND payment_analytics_device = 1 THEN 'desktop_browser' WHEN payment_analytics_platform = 3 THEN 'CORDOVA' WHEN payment_analytics_platform = 4 THEN 'server' ELSE NUL END AS channel,
    payments_created_date,
    INT((payments_created_at) / (3600 * 60)) AS hourly_bucket,
    COUNT(DISTINCT payments_id) AS payment_attempts,
    COUNT(DISTINCT CASE WHEN payments_authorized_at IS NOT NULL THEN payments_id END) * 100.00 / COUNT(DISTINCT payments_id) AS success_rate
  FROM warehouse.payments
    LEFT JOIN analytics_selfserve.optimizer_top_banks ON payments.payments_bank = optimizer_top_banks.bank_group
    LEFT JOIN analytics_selfserve.optimizer_top_issuer ON payments.cards_issuer = optimizer_top_issuer.cards_issuer
  WHERE payments_created_date >= '2024-07-01'
    AND merchants_category2 = 'ecommerce'
    AND optimizer_payment_id IS NOT NULL
    AND payments_method IN ('card', 'upi', 'netbanking', 'wallet')
    AND payments_recurring = 0
    AND payments_international = 0
    AND payments_gateway IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
),

final_data AS (
  SELECT
    method,
    agg,
    cards_network,
    tokenization_flag,
    payments_wallet,
    top_card_issuer,
    top_card_banks,
    --channel,
    hourly_bucket,
    payments_created_date,
    SUM(payment_attempts) AS total_payment_attempts,
    count(agg) OVER (PARTITION BY method, cards_network, tokenization_flag, payments_wallet, top_card_issuer, top_card_banks,  hourly_bucket, payments_created_date order by hourly_bucket) AS competition_count,
    ROUND(APPROX_PERCENTILE(success_rate, 0.1), 2) AS percentile_10,
    ROUND(APPROX_PERCENTILE(success_rate, 0.25), 2) AS percentile_25,
    ROUND(APPROX_PERCENTILE(success_rate, 0.3), 2) AS percentile_30,
    ROUND(APPROX_PERCENTILE(success_rate, 0.5), 2) AS percentile_50,
    ROUND(APPROX_PERCENTILE(success_rate, 0.7), 2) AS percentile_70,
    ROUND(APPROX_PERCENTILE(success_rate, 0.75), 2) AS percentile_75,
    ROUND(APPROX_PERCENTILE(success_rate, 0.8), 2) AS percentile_80,
    ROUND(APPROX_PERCENTILE(success_rate, 0.9), 2) AS percentile_90,
    STDDEV(success_rate) AS standard_deviation
  FROM aggregated_data
  WHERE payment_attempts >= 30
    AND success_rate BETWEEN 5 AND 95
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
)

SELECT * FROM final_data where competition_count > 1;




                        
""")




# COMMAND ----------

df_pd = df_base_data.toPandas()

# COMMAND ----------

df_pd['cards_network'].fillna(value='na', inplace=True)
df_pd['tokenization_flag'].fillna(value=0, inplace=True)
df_pd['top_card_issuer'].fillna(value='na', inplace=True)
#df_pd['channel'].fillna(value='na', inplace=True)
df_pd['top_card_banks'].fillna(value='na', inplace=True)
df_pd['payments_wallet'].fillna(value='na', inplace=True)

# COMMAND ----------

# Define the columns to partition by for ranking
partition_cols = ['method', 'cards_network', 'tokenization_flag', 'payments_wallet',
                  'top_card_issuer', 'top_card_banks', 'hourly_bucket', 'payments_created_date']

# Perform ranking using pandas rank function
df_pd['rank_success_rate'] = df_pd.groupby(partition_cols)['percentile_50'].rank(method='dense', ascending=False)

# COMMAND ----------

df_pd.columns

# COMMAND ----------

df_pd

# COMMAND ----------

df_pd = df_pd.dropna(subset=['rank_success_rate'])

# COMMAND ----------

print(df_pd['rank_success_rate'].unique())

# COMMAND ----------

df_pd['time_bucket'] = df_pd['payments_created_date'] + '_' + df_pd['hourly_bucket'].astype(str)

# COMMAND ----------

pivot_table = df_pd.pivot_table(
    index=['method', 'agg'],
    columns='rank_success_rate',
    values='time_bucket',
    aggfunc='count',  # Using 'count' to count occurrences
    fill_value=0  # Fill NaN values with 0
).astype(int) 

# COMMAND ----------

pivot_table

# COMMAND ----------

bucket_with_agg_rank = df_pd.pivot_table(
    index=['method', 'cards_network', 'tokenization_flag', 'payments_wallet',
                  'top_card_issuer', 'top_card_banks', 'agg'],
    columns='rank_success_rate',
    values='time_bucket',
    aggfunc='count',  # Using 'count' to count occurrences
    fill_value=0  # Fill NaN values with 0
).astype(int).reset_index()

# COMMAND ----------

bucket_with_agg_rank.columns

# COMMAND ----------

bucket_with_total_rank = df_pd.pivot_table(
    index=['method', 'cards_network', 'tokenization_flag', 'payments_wallet',
                  'top_card_issuer', 'top_card_banks'],
    columns='rank_success_rate',
    values='time_bucket',
    aggfunc='count',  # Using 'count' to count occurrences
    fill_value=0  # Fill NaN values with 0
).astype(int).reset_index()






# COMMAND ----------

bucket_with_total_rank

# COMMAND ----------

bucket_with_total_rank

# COMMAND ----------

# Reset the index of both DataFrames
bucket_with_total_rank = bucket_with_total_rank.reset_index()
bucket_with_agg_rank = bucket_with_agg_rank.reset_index()


# COMMAND ----------

# Reset indexes if not already reset
bucket_with_total_rank.reset_index(drop=True, inplace=True)
bucket_with_agg_rank.reset_index(drop=True, inplace=True)
joined_df = pd.merge(bucket_with_agg_rank,bucket_with_total_rank, 
                     on=['method', 'cards_network', 'tokenization_flag', 'payments_wallet',
                         'top_card_issuer', 'top_card_banks'],
                     suffixes=('', '_agg'))

# Display the joined DataFrame
print("Joined DataFrame:")
print(joined_df)

# COMMAND ----------

sorted_df = joined_df.sort_values(by=['method', 'cards_network', 'tokenization_flag', 'payments_wallet',
                                      'top_card_issuer', 'top_card_banks'])

# Display the sorted DataFrame
print("Sorted DataFrame:")
print(sorted_df)

# COMMAND ----------

sorted_df

# COMMAND ----------

sorted_df['Rank_ratio_1'] = round((sorted_df['1.0'] / sorted_df['1.0_agg']) *100,2)
sorted_df['Rank_ratio_2'] = round((sorted_df['2.0'] / sorted_df['2.0_agg'])*100,2)
#sorted_df['Rank_ratio_3'] = round((sorted_df['3.0'] / sorted_df['3.0_agg'])*100,2)
#sorted_df['Rank_ratio_4'] = round((sorted_df['4.0'] / sorted_df['4.0_agg'])*100,2)
#sorted_df['Rank_ratio_4'] = round((sorted_df['5.0'] / sorted_df['5.0_agg'])*100,2)


# COMMAND ----------

final_df = sorted_df[['method', 'cards_network', 'tokenization_flag', 'payments_wallet',
                      'top_card_issuer', 'top_card_banks', 'agg',
                      '1.0', '2.0', 
                      #'3.0', '4.0', '5.0', 
                      'Rank_ratio_1', 'Rank_ratio_2'
                      #,'Rank_ratio_3', 'Rank_ratio_4'
                      ]]

# COMMAND ----------

final_df

# COMMAND ----------

final_df.to_csv("/dbfs/FileStore/Chitraxi/final_df.csv")


spark.read.format('csv').load('dbfs:/FileStore/Chitraxi/final_df.csv').display()

# COMMAND ----------

recommended_df = final_df.sort_values(by='Rank_ratio_1', ascending=False) \
                      .groupby(['method', 'cards_network', 'tokenization_flag', 'payments_wallet',
                                'top_card_issuer', 'top_card_banks']) \
                      .head(2) 
                     

print(recommended_df)

# COMMAND ----------


print(recommended_df.columns)

# COMMAND ----------

recommended_df.to_csv("/dbfs/FileStore/Chitraxi/recommended_df.csv")


spark.read.format('csv').load('dbfs:/FileStore/Chitraxi/recommended_df.csv').display()

# COMMAND ----------

# Step 1: Calculate rank based on '1.0' column grouped by specified columns
recommended_df['rank'] = recommended_df.groupby(['method', 'cards_network', 'tokenization_flag', 'payments_wallet', 'top_card_issuer', 'top_card_banks'])['1.0'].rank(method='dense', ascending=False)

# Step 2: Perform self-join based on ranks
df1 = recommended_df[recommended_df['rank'] == 1]
df2 = recommended_df[recommended_df['rank'] == 2]

cohorts_with_2_priority = pd.merge(df1, df2, on=['method', 'cards_network', 'tokenization_flag', 'payments_wallet', 'top_card_issuer', 'top_card_banks'], suffixes=('_rank1', '_rank2'))




# COMMAND ----------

cohorts_with_2_priority['cohort'] = cohorts_with_2_priority['method'] + cohorts_with_2_priority['cards_network'] + cohorts_with_2_priority['tokenization_flag'] + cohorts_with_2_priority['payments_wallet'] + cohorts_with_2_priority['top_card_issuer'] + cohorts_with_2_priority['top_card_banks'] 

# COMMAND ----------

cohorts_with_2_priority.to_csv("/dbfs/FileStore/Chitraxi/cohorts_with_2_priority.csv")


spark.read.format('csv').load('dbfs:/FileStore/Chitraxi/cohorts_with_2_priority.csv').display()

# COMMAND ----------

recommended_df

# COMMAND ----------

df_median_sr_data = spark.sql(f""" select * from analytics_selfserve.optimizer_ecommerce_cohort_median_SR

  """)
  

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

df_median_sr_data_pd = df_median_sr_data.toPandas()

# COMMAND ----------

df_median_sr_data_pd

# COMMAND ----------

df_median_sr_data_pd_selected = df_median_sr_data_pd[['method', 'cards_network', 'tokenization_flag', 'payments_wallet', 'top_card_issuer', 'top_card_banks','cohort','agg','percentile_50','count_buckets','agg_rank_2','percentile_50_rank2','count_bukcets_rank2']]

# COMMAND ----------

df_median_sr_data_pd_selected

# COMMAND ----------

cohorts_with_2_priority.columns

# COMMAND ----------

cohorts_with_2_priority_selected = cohorts_with_2_priority[['method', 'cards_network', 'tokenization_flag', 'payments_wallet', 'top_card_issuer', 'top_card_banks','cohort','agg_rank1','1.0_rank1','agg_rank2','1.0_rank2']]

# COMMAND ----------

rules_base = pd.merge(df_median_sr_data_pd_selected, cohorts_with_2_priority_selected, on=['method', 'cards_network', 'tokenization_flag', 'payments_wallet', 'top_card_issuer', 'top_card_banks'], how='outer',suffixes=('_basedonmedian', '_basedoncohort'))

# COMMAND ----------

rules_base

# COMMAND ----------

# Function to apply the conditions
def calculate_final_agg_rank(row):
    if pd.notna(row['agg_rank1']) and pd.notna(row['agg_rank2']) and abs(row['1.0_rank1'] - row['1.0_rank2']) < 3:
        if row['agg_rank1'] == 'razorpay':
            return row['agg_rank2']
        else:
            return row['agg_rank1']
    elif pd.isna(row['agg_rank1']):
        return row['agg']
    else:
        return row['agg_rank1']
    
    
def calculate_final_agg_rank2(row):
    if pd.notna(row['agg_rank1']) and pd.notna(row['agg_rank2']) and abs(row['1.0_rank1'] - row['1.0_rank2']) < 3:
        if row['agg_rank1'] == 'razorpay':
            return row['agg_rank1']
        else:
            return row['agg_rank2']
    elif pd.isna(row['agg_rank2']):
        return row['agg_rank_2']
    else:
        return row['agg_rank2']
    

# Apply the function row-wise
rules_base['final_agg_rank1'] = rules_base.apply(lambda row: calculate_final_agg_rank(row), axis=1)

# Apply for final_agg_rank2
rules_base['final_agg_rank2'] = rules_base.apply(lambda row: calculate_final_agg_rank2(row), axis=1)

# COMMAND ----------


rules_base.to_csv("/dbfs/FileStore/Chitraxi/rules_base.csv")


spark.read.format('csv').load('dbfs:/FileStore/Chitraxi/rules_base.csv').display()

# COMMAND ----------

spark.sql("REFRESH TABLE analytics_selfserve.optimizer_insurance_cohort_median_SR_for_all")

# COMMAND ----------

df_median_sr_data_for_all = spark.sql(f""" select * from analytics_selfserve.optimizer_insurance_cohort_median_SR_for_all

  """)
df_median_sr_data_for_all_pd = df_median_sr_data_for_all.toPandas()

# COMMAND ----------

df_median_sr_data_for_all_pd.columns


# COMMAND ----------

df_median_sr_data_for_all_pandas = df_median_sr_data_for_all.toPandas()

rules_base_merged = pd.merge(rules_base, df_median_sr_data_for_all_pandas, 
                             left_on=['method', 'cards_network', 'tokenization_flag', 'payments_wallet',
                                      'top_card_issuer', 'top_card_banks', 'final_agg_rank1'],
                             right_on=['method', 'cards_network', 'tokenization_flag', 'payments_wallet',
                                       'top_card_issuer', 'top_card_banks', 'agg'],
                             suffixes=('', '_agg_final_rank1'),how='left')



rules_base_merged = pd.merge(rules_base_merged, df_median_sr_data_for_all_pandas, 
                             left_on=['method', 'cards_network', 'tokenization_flag', 'payments_wallet',
                                      'top_card_issuer', 'top_card_banks', 'final_agg_rank2'],
                             right_on=['method', 'cards_network', 'tokenization_flag', 'payments_wallet',
                                       'top_card_issuer', 'top_card_banks', 'agg'],
                             suffixes=('', '_agg_final_rank2'),how='left')

# COMMAND ----------

df_median_sr_data_for_all_pandas.to_csv("/dbfs/FileStore/Chitraxi/df_median_sr_data_for_all_pandas.csv")


spark.read.format('csv').load('dbfs:/FileStore/Chitraxi/df_median_sr_data_for_all_pandas.csv').display()

# COMMAND ----------

rules_base_merged.columns

# COMMAND ----------

rules_base_merged

# COMMAND ----------



rules_base_merged['priority_1_provider'] = np.where(
    rules_base_merged['percentile_50_agg_final_rank1'] - rules_base_merged['percentile_50'] >= 2,
    rules_base_merged['agg'],
    rules_base_merged['final_agg_rank1']
)

rules_base_merged['priority_1_median_SR'] = np.where(
    rules_base_merged['percentile_50_agg_final_rank1'] - rules_base_merged['percentile_50'] >= 2,
    rules_base_merged['percentile_50'],
    rules_base_merged['percentile_50_agg_final_rank1']
)

rules_base_merged['priority_2_provider'] = np.where(
    (rules_base_merged['final_agg_rank2'] == rules_base_merged['priority_1_provider']) &
    (rules_base_merged['final_agg_rank2'] == rules_base_merged['final_agg_rank1']),
    np.where(
        rules_base_merged['final_agg_rank2'] == rules_base_merged['agg_rank_2'],
        rules_base_merged['agg'],
        rules_base_merged['final_agg_rank1']
    ),
    rules_base_merged['final_agg_rank2']
)

'''rules_base_merged['priority_2_median_SR'] = np.where(
    (rules_base_merged['final_agg_rank2'] == rules_base_merged['priority_1_provider']) &
    (rules_base_merged['final_agg_rank2'] == rules_base_merged['final_agg_rank1']),
    np.where(
        rules_base_merged['final_agg_rank2'] == rules_base_merged['agg_rank_2'],
        rules_base_merged['agg'],
        rules_base_merged['final_agg_rank1']
    ),
    rules_base_merged['final_agg_rank2']
)

rules_base_merged['priority_3'] = np.where(
    (rules_base_merged['final_agg_rank2'] == rules_base_merged['priority_1_provider']) &
    (rules_base_merged['final_agg_rank2'] == rules_base_merged['final_agg_rank2']),
    np.where(
        rules_base_merged['final_agg_rank2'] == rules_base_merged['agg_rank_2'],
        rules_base_merged['percentile_50'],
        rules_base_merged['percentile_50_agg_final_rank1']
    ),
    rules_base_merged['percentile_50_agg_final_rank2']
)

# Condition for priority_1_median_SR
rules_base_merged['priority_1_median_SR'] = np.where(
    rules_base_merged['priority_1_median_SR'].isnull(),
    np.maximum(
        np.maximum(rules_base_merged['percentile_50_agg_final_rank1'].fillna(0), 0),
        np.maximum(rules_base_merged['percentile_50'].fillna(0), 0),
        np.maximum(rules_base_merged['percentile_50_rank2'].fillna(0), 0)
    ),
    rules_base_merged['priority_1_median_SR']
)'''



rules_base_merged = pd.merge(rules_base_merged, df_median_sr_data_for_all_pandas, 
                             left_on=['method', 'cards_network', 'tokenization_flag', 'payments_wallet',
                                      'top_card_issuer', 'top_card_banks', 'priority_1_provider'],
                             right_on=['method', 'cards_network', 'tokenization_flag', 'payments_wallet',
                                       'top_card_issuer', 'top_card_banks', 'agg'],
                             suffixes=('', '_priority_1'),how='left')



rules_base_merged = pd.merge(rules_base_merged, df_median_sr_data_for_all_pandas, 
                             left_on=['method', 'cards_network', 'tokenization_flag', 'payments_wallet',
                                      'top_card_issuer', 'top_card_banks', 'priority_2_provider'],
                             right_on=['method', 'cards_network', 'tokenization_flag', 'payments_wallet',
                                       'top_card_issuer', 'top_card_banks', 'agg'],
                             suffixes=('', '_priority_2'),how='left')

# COMMAND ----------

rules_base_merged.to_csv("/dbfs/FileStore/Chitraxi/rules_base_merged.csv")


spark.read.format('csv').load('dbfs:/FileStore/Chitraxi/rules_base_merged.csv').display()

# COMMAND ----------

df_median_sr_data_for_all_method = spark.sql(f""" select * from analytics_selfserve.optimizer_insurance_cohort_median_SR_for_all_method """)
df_median_sr_data_for_all_pd_method = df_median_sr_data_for_all.toPandas()

# COMMAND ----------

rules_base_merged = pd.merge(rules_base_merged, df_median_sr_data_for_all_pd_method, 
                             left_on=['method', 'cards_network', 'tokenization_flag', 'payments_wallet',
                                      'top_card_issuer', 'top_card_banks', 'priority_1_provider'],
                             right_on=['method', 'cards_network', 'tokenization_flag', 'payments_wallet',
                                       'top_card_issuer', 'top_card_banks', 'agg'],
                             suffixes=('', '_priority_1'),how='left')



rules_base_merged = pd.merge(rules_base_merged, df_median_sr_data_for_all_pd_method, 
                             left_on=['method', 'cards_network', 'tokenization_flag', 'payments_wallet',
                                      'top_card_issuer', 'top_card_banks', 'priority_2_provider'],
                             right_on=['method', 'cards_network', 'tokenization_flag', 'payments_wallet',
                                       'top_card_issuer', 'top_card_banks', 'agg'],
                             suffixes=('', '_priority_2'),how='left')

# COMMAND ----------

rules_base_merged.columns

# COMMAND ----------

rules_base_merged

# COMMAND ----------


                                                        

# COMMAND ----------

# Check if the column 'priority_1_median_sr' exists in the DataFrame
if 'priority_1_median_sr' not in rules_base_merged.columns:
    # Add the column 'priority_1_median_sr' to the DataFrame
    rules_base_merged = rules_base_merged.withColumn('priority_1_median_sr', F.lit(None))

# Assign values to 'priority_1_median_sr' based on conditions
rules_base_merged = rules_base_merged.withColumn('priority_1_median_sr',
                                                 F.coalesce(F.col('percentile_50_priority_1'),
                                                            F.col('percentile_50_method_agg_priority_1')))

# COMMAND ----------



# Check if the column 'priority_1_median_sr' exists in the DataFrame
if 'priority_1_median_sr' not in rules_base_merged.columns:
    # Add the column 'priority_1_median_sr' to the DataFrame
    rules_base_merged = rules_base_merged.withColumn('priority_1_median_sr', np.lit(None))

# Assign values to 'priority_1_median_sr' based on conditions
rules_base_merged = rules_base_merged.withColumn('priority_1_median_sr',
                                                 np.when(np.col('percentile_50_priority_1').isNotNull(),
                                                      np.col('percentile_50_priority_1'))
                                                 .otherwise(np.col('percentile_50_method_agg_priority_1')))

# Check if the column 'priority_2_median_sr' exists in the DataFrame
if 'priority_2_median_sr' not in rules_base_merged.columns:
    # Add the column 'priority_2_median_sr' to the DataFrame
    rules_base_merged = rules_base_merged.withColumn('priority_2_median_sr', np.lit(None))

# Assign values to 'priority_2_median_sr' based on conditions
rules_base_merged = rules_base_merged.withColumn('priority_2_median_sr',
                                                 np.when(np.col('percentile_50_priority_2').isNotNull(),
                                                      np.col('percentile_50_priority_2'))
                                                 .otherwise(np.col('percentile_50_method_agg_priority_2')))

# COMMAND ----------

i = 0
for _ in iter(int, 1):
    print(i)
    i += 1

# COMMAND ----------

print(df_pd['agg'].unique())
