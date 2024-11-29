# Databricks notebook source
# Install the python-docx library
#%pip install python-docx

# COMMAND ----------

# Import necessary libraries
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from difflib import SequenceMatcher
import pandas as pd

# Create Spark session
spark = SparkSession.builder.appName("Optimizer Fuzzy match for callback url").getOrCreate()


# Load data into DataFrame
df_base_data = spark.sql(f"""
            SELECT 
                CASE 
                    WHEN TRY_CAST(SPLIT_PART(callback_url, '/', 3) AS VARCHAR(100)) IS NOT NULL THEN SPLIT_PART(callback_url, '/', 3)
                    ELSE NULL
                END AS call_back,
                merchants.website, merchants.category2,merchant_id,count(distinct payments.id) as payment_attempts, sum(base_amount) as gmv
            FROM
                realtime_hudi_api.payments AS payments
            LEFT JOIN
                realtime_hudi_api.merchants AS merchants
            ON merchants.id = payments.merchant_id
            WHERE 
                payments.created_date  between '2024-05-01'   and '2024-05-31'  and callback_url is not null
                group by 1,2,3,4    
""")

# COMMAND ----------

# Convert Spark DataFrame to Pandas DataFrame
df_data = df_base_data.fillna('').toPandas()



# COMMAND ----------

df_data

# COMMAND ----------

# Function to remove everything after the last dot
def remove_after_last_dot(url):
    last_dot_index = url.rfind('.')
    if last_dot_index != -1:
        return url[:last_dot_index]
    else:
        return url
    

# Apply the function to the 'call_back' column
df_data['call_back'] = df_data['call_back'].apply(remove_after_last_dot)

# COMMAND ----------

df_data

# COMMAND ----------



def longest_common_substring(url1, url2):
    m = len(url1)
    n = len(url2)
    
    # Initialize variables to track the longest substring
    longest_substring = ""
    longest_length = 0
    
    # Iterate through all possible substrings of url1
    for i in range(m):
        for j in range(i + 1, m + 1):
            substring = url1[i:j]
            if substring in url2 and len(substring) > longest_length:
                longest_substring = substring
                longest_length = len(substring)
    
    return longest_substring, longest_length
    

df_data['matching_substring'], df_data['substring_length'] = zip(*df_data.apply(lambda row: longest_common_substring(row['call_back'], row['website']), axis=1))

# COMMAND ----------

df_data

# COMMAND ----------

#spark.read.format('docx').load('/dbfs/FileStore/Chitraxi/output.docx').display()

# COMMAND ----------

df_data

# COMMAND ----------

df_data_filtered = df_data[
    df_data['call_back'].apply(lambda x: len(x) > 1) &  # Filter by length of call_back > 1
    df_data['matching_substring'].apply(lambda x: len(x) <= 4) & ~df_data['matching_substring'].isin(['success', 'subject'])  # Filter by length of matching_substring > 2 and exclude specific substrings
]

# COMMAND ----------

df_data_filtered.to_csv("/dbfs/FileStore/Chitraxi/df_data_filtered.csv")


spark.read.format('csv').load('dbfs:/FileStore/Chitraxi/df_data_filtered.csv').display()

# COMMAND ----------


# Grouping and Aggregation
df_grouped = df_data_filtered.groupby(['call_back', 'category2']).agg(
    distinct_merchant_id_count=pd.NamedAgg(column='merchant_id', aggfunc=pd.Series.nunique),
    total_payment_attempts=pd.NamedAgg(column='payment_attempts', aggfunc='sum'),
    total_base_amount=pd.NamedAgg(column='gmv', aggfunc='sum')
).reset_index()

# Calculate industry_base_amount_share
df_grouped['industry_base_amount_share'] = round(df_grouped['total_base_amount']* 100.00 / df_grouped.groupby('call_back')['total_base_amount'].transform('sum'),2)

# Display the resulting DataFrame
print(df_grouped)

# COMMAND ----------

# Concatenate two columns data
df_grouped['industry_share'] = df_grouped['category2'] + ' - ' + df_grouped['industry_base_amount_share'].astype(str)

# Display the resulting DataFrame
display(df_grouped)

# COMMAND ----------

def aggregate_industry_share(series):
    return list(set(series.str.split(', ').explode().unique()))

# Aggregate the data by call_back
df_aggregated = df_grouped.groupby('call_back').agg(
    sum_distinct_merchant_id_count=('distinct_merchant_id_count', 'sum'),
    total_payment_attempts=('total_payment_attempts', 'sum'),
    total_base_amount=('total_base_amount', 'sum'),
    industry_array=('industry_share', aggregate_industry_share)
).reset_index()

# Display the resulting DataFrame
print(df_aggregated)

# COMMAND ----------

df_aggregated.to_csv("/dbfs/FileStore/Chitraxi/df_aggregated.csv")


spark.read.format('csv').load('dbfs:/FileStore/Chitraxi/df_aggregated.csv').display()

# COMMAND ----------



# COMMAND ----------

distinct_matching_strings = df_data_filtered['matching_substring'].unique()

# COMMAND ----------


distinct_matching_strings

# COMMAND ----------

from pyspark.sql.types import StringType
display(spark.createDataFrame(distinct_matching_strings.astype(str), StringType()))

# COMMAND ----------

df_data.to_csv("/dbfs/FileStore/Chitraxi/df_data.csv")


spark.read.format('csv').load('dbfs:/FileStore/Chitraxi/df_data.csv').display()
