# Databricks notebook source
#pip install gspread oauth2client

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import pyspark.sql.types as T
from pyspark.sql.window import Window as W
from time import time
from datetime import datetime
#import nltk 
import matplotlib.pyplot as plt
import re 
import pandas as pd

# Create Spark session
spark = SparkSession.builder.appName("Optimizer Apollo SR insights script") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .config("spark.sql.parquet.dictionary.encoding.enabled", "false") \
    .getOrCreate()


start_date = '2024-02-01'
end_date = '2024-02-29'
previous_start_date = '2024-01-01'
previous_end_date = '2024-01-31'

# Use Spark session instead of sqlContext
df_base_data = spark.sql(f"""SELECT
        a.merchant_id, 
        COALESCE(a.dba,b.billing_label, b.name) AS name,
        CASE 
        WHEN a.internal_external_flag = 'external' THEN gateway
        WHEN a.internal_external_flag = 'internal' THEN 'Razorpay' else null end as aggregator,
        a.method_advanced,
        a.network,
        a.network_tokenised_payment,
        IF(a.internal_error_code = '' OR a.internal_error_code IS NULL, 'Success', a.internal_error_code) AS error_Code,
        a.wallet,
        a.bank,
        CASE
            WHEN a.internal_error_code IN (
                'BAD_REQUEST_CARD_DISABLED_FOR_ONLINE_PAYMENTS',
                'SERVER_ERROR_OTP_ELF_INVALID_ARGUMENT',
                'BAD_REQUEST_PAYMENT_CUSTOMER_DROPPED_OFF',
                'BAD_REQUEST_PAYMENT_CANCELLED',
                'BAD_REQUEST_CARD_CREDIT_LIMIT_REACHED',
                'GATEWAY_ERROR_CREDIT_FAILED',
                'GATEWAY_ERROR_BENEFICIARY_BANK_DEEMED_HIGH_RESPONSE_TIME_CHECK_DECLINE',
                'GATEWAY_ERROR_REMITTER_BANK_DEEMED_HIGH_RESPONSE_TIME_CHECK_DECLINE',
                'GATEWAY_ERROR_MANDATE_CREATION_EXPIRED',
                'GATEWAY_ERROR_REQAUTH_DECLINED',
                'BAD_REQUEST_PAYMENT_DECLINED_BY_CUSTOMER',
                'BAD_REQUEST_TRANSACTION_AMOUNT_LIMIT_EXCEEDED',
                'GATEWAY_ERROR_PAYMENT_FAILED',
                'BAD_REQUEST_TRANSACTION_AMOUNT_LIMIT_AT_REMITTER_EXCEEDED',
                'GATEWAY_ERROR_REMITTER_BANK_NOT_AVAILABLE',
                'GATEWAY_ERROR_PAYER_PSP_NOT_AVAILABLE',
                'BAD_REQUEST_TRANSACTION_FREQUENCY_LIMIT_AT_REMITTER_EXCEEDED',
                'GATEWAY_ERROR_MANDATE_CREATION_TIMEOUT_AT_REMITTER_END',
                'GATEWAY_ERROR_REMITTER_CUTOFF_IN_PROGRESS',
                'BAD_REQUEST_PAYMENT_UPI_MANDATE_REJECTED',
                'BAD_REQUEST_CARD_MANDATE_IS_NOT_ACTIVE',
                'GATEWAY_ERROR_SUPPORT_AUTH_NOT_FOUND',
                'BAD_REQUEST_ERROR_OTP_ELF_INVALID_OTP_LENGTH',
                'BAD_REQUEST_CARD_MANDATE_CUSTOMER_NOT_APPROVED',
                'BAD_REQUEST_USER_NOT_AUTHENTICATED',
                'BAD_REQUEST_CARD_INVALID_DATA',
                'BAD_REQUEST_NON_3DS_INTERNATIONAL_NOT_ALLOWED',
                'BAD_REQUEST_CARD_MANDATE_MANDATE_NOT_ACTIVE',
                'GATEWAY_ERROR_MANDATE_CREATION_DECLINED_BY_REMITTER_BANK',
                'GATEWAY_ERROR_PAYMENT_NOT_FOUND',
                'BAD_REQUEST_AUTHENTICATION_FAILED',
                'BAD_REQUEST_EMANDATE_REGISTRATION_ALREADY_DECLINED_BY_BANK',
                'BAD_REQUEST_EMANDATE_REGISTRATION_ALREADY_IN_PROGRESS',
                'BAD_REQUEST_PAYMENT_PENDING',
                'BAD_REQUEST_PAYMENT_CANCELLED_AT_EMANDATE_REGISTRATION',
                'GATEWAY_ERROR_PAYMENT_NOT_FOUND',
                'GATEWAY_ERROR_PAYMENT_FAILED',
                'BAD_REQUEST_ACCOUNT_BLOCKED',
                'BAD_REQUEST_ACCOUNT_CLOSED',
                'BAD_REQUEST_EMANDATE_AMOUNT_LIMIT_EXCEEDED',
                'BAD_REQUEST_EMANDATE_CANCELLED_INACTIVE',
                'BAD_REQUEST_EMANDATE_REGISTRATION_ACTION_NEEDED',
                'BAD_REQUEST_EMANDATE_REGISTRATION_FAILED',
                'BAD_REQUEST_GATEWAY_TOKEN_EMPTY',
                'BAD_REQUEST_INVALID_ACCOUNT_HOLDER_NAME',
                'BAD_REQUEST_INVALID_TRANSACTION_AMOUNT',
                'BAD_REQUEST_INVALID_USER_CREDENTIALS',
                'BAD_REQUEST_NETBANKING_USER_NOT_REGISTERED',
                'BAD_REQUEST_NO_DR_ALLOWED',
                'BAD_REQUEST_PAYMENT_ACCOUNT_INSUFFICIENT_BALANCE',
                'BAD_REQUEST_PAYMENT_ACCOUNT_MAX_LIMIT_EXCEEDED',
                'BAD_REQUEST_PAYMENT_ACCOUNT_WITHDRAWAL_FROZEN',
                'BAD_REQUEST_PAYMENT_CANCELLED_BY_CUSTOMER',
                'BAD_REQUEST_PAYMENT_CANCELLED_BY_USER',
                'BAD_REQUEST_PAYMENT_FAILED',
                'BAD_REQUEST_PAYMENT_INVALID_ACCOUNT',
                'BAD_REQUEST_PAYMENT_INVALID_AMOUNT_OR_CURRENCY',
                'BAD_REQUEST_PAYMENT_KYC_PENDING',
                'BAD_REQUEST_PAYMENT_NETBANKING_CANCELLED_BY_USER',
                'BAD_REQUEST_PAYMENT_POSSIBLE_FRAUD',
                'BAD_REQUEST_PAYMENT_POSSIBLE_FRAUD_WEBSITE_MISMATCH',
                'BAD_REQUEST_PAYMENT_TIMED_OUT',
                'BAD_REQUEST_PAYMENT_VERIFICATION_FAILED',
                'BAD_REQUEST_TRANSACTION_AMOUNT_LIMIT_EXCEEDED',
                'GATEWAY_ERROR_AMOUNT_TAMPERED',
                'GATEWAY_ERROR_CALLBACK_EMPTY_INPUT',
                'GATEWAY_ERROR_DEBIT_BEFORE_MANDATE_START',
                'GATEWAY_ERROR_DEBIT_FAILED',
                'GATEWAY_ERROR_FATAL_ERROR',
                'GATEWAY_ERROR_INVALID_PARAMETERS',
                'GATEWAY_ERROR_INVALID_RESPONSE',
                'GATEWAY_ERROR_ISSUER_DOWN',
                'GATEWAY_ERROR_MANDATE_CREATION_FAILED',
                'GATEWAY_ERROR_MERCHANT_IP_NOT_WHITELISTED',
                'GATEWAY_ERROR_PAYMENT_VERIFICATION_ERROR',
                'GATEWAY_ERROR_REQUEST_ERROR',
                'GATEWAY_ERROR_REQUEST_TIMEOUT',
                'GATEWAY_ERROR_UNKNOWN_ERROR',
                'SERVER_ERROR_AMOUNT_TAMPERED',
                'SERVER_ERROR_ASSERTION_ERROR',
                'BAD_REQUEST_EMANDATE_REGISTRATION_FAILED_JOINT_ACCOUNT',
                'BAD_REQUEST_EMANDATE_REGISTRATION_DUPLICATE_REQUEST',
                'BAD_REQUEST_PAYMENT_OTP_INCORRECT',
                'BAD_REQUEST_PAYMENT_CARD_INVALID_PIN',
                'BAD_REQUEST_PAYMENT_OTP_VALIDATION_ATTEMPT_LIMIT_EXCEEDED',
                'BAD_REQUEST_PAYMENT_CARD_EXPIRED',
                'BAD_REQUEST_PAYMENT_CARD_NUMBER_POSSIBLY_INVALID',
                'SERVER_ERROR_NBPLUS_PAYMENT_SERVICE_FAILURE',
                'GATEWAY_ERROR_COMMUNICATION_ERROR',
                'BAD_REQUEST_PAYMENT_DECLINED_BY_BANK_DUE_TO_BLOCKED_CARD',
                'BAD_REQUEST_EMANDATE_INACTIVE',
                'BAD_REQUEST_CARD_INACTIVE',
                'BAD_REQUEST_PAYMENT_CARD_INVALID_CVV',
                'BAD_REQUEST_VALIDATION_FAILURE',
                'BAD_REQUEST_ACCOUNT_DORMANT',
                'BAD_REQUEST_PAYMENT_CARD_INVALID_EXPIRY_DATE',
                'BAD_REQUEST_PAYMENT_ALREADY_PROCESSED',
                'SERVER_ERROR_RUNTIME_ERROR',
                'GATEWAY_ERROR_CHECKSUM_MATCH_FAILED') THEN 
                CASE
                    WHEN a.internal_error_code IN (
                        'BAD_REQUEST_CARD_DISABLED_FOR_ONLINE_PAYMENTS',
                        'BAD_REQUEST_PAYMENT_CUSTOMER_DROPPED_OFF',
                        'BAD_REQUEST_PAYMENT_CANCELLED',
                        'BAD_REQUEST_CARD_CREDIT_LIMIT_REACHED',
                        'BAD_REQUEST_PAYMENT_DECLINED_BY_CUSTOMER',
                        'BAD_REQUEST_TRANSACTION_AMOUNT_LIMIT_EXCEEDED',
                        'BAD_REQUEST_PAYMENT_UPI_MANDATE_REJECTED',
                        'BAD_REQUEST_CARD_MANDATE_IS_NOT_ACTIVE',
                        'BAD_REQUEST_CARD_INVALID_DATA',
                        'BAD_REQUEST_NON_3DS_INTERNATIONAL_NOT_ALLOWED',
                        'BAD_REQUEST_CARD_MANDATE_MANDATE_NOT_ACTIVE',
                        'BAD_REQUEST_AUTHENTICATION_FAILED',
                        'BAD_REQUEST_EMANDATE_REGISTRATION_ALREADY_DECLINED_BY_BANK',
                        'BAD_REQUEST_EMANDATE_REGISTRATION_ALREADY_IN_PROGRESS',
                        'BAD_REQUEST_PAYMENT_PENDING',
                        'BAD_REQUEST_PAYMENT_CANCELLED_AT_EMANDATE_REGISTRATION',
                        'BAD_REQUEST_ACCOUNT_BLOCKED',
                        'BAD_REQUEST_ACCOUNT_CLOSED',
                        'BAD_REQUEST_EMANDATE_CANCELLED_INACTIVE',
                        'BAD_REQUEST_EMANDATE_REGISTRATION_ACTION_NEEDED',
                        'BAD_REQUEST_GATEWAY_TOKEN_EMPTY',
                        'BAD_REQUEST_INVALID_ACCOUNT_HOLDER_NAME',
                        'BAD_REQUEST_INVALID_TRANSACTION_AMOUNT',
                        'BAD_REQUEST_INVALID_USER_CREDENTIALS',
                        'BAD_REQUEST_NETBANKING_USER_NOT_REGISTERED',
                        'BAD_REQUEST_NO_DR_ALLOWED',
                        'BAD_REQUEST_PAYMENT_ACCOUNT_INSUFFICIENT_BALANCE',
                        'BAD_REQUEST_PAYMENT_ACCOUNT_MAX_LIMIT_EXCEEDED',
                        'BAD_REQUEST_PAYMENT_ACCOUNT_WITHDRAWAL_FROZEN',
                        'BAD_REQUEST_PAYMENT_CANCELLED_BY_CUSTOMER',
                        'BAD_REQUEST_PAYMENT_CANCELLED_BY_USER',
                        'BAD_REQUEST_PAYMENT_FAILED',
                        'BAD_REQUEST_PAYMENT_INVALID_ACCOUNT',
                        'BAD_REQUEST_PAYMENT_INVALID_AMOUNT_OR_CURRENCY',
                        'BAD_REQUEST_PAYMENT_KYC_PENDING',
                        'BAD_REQUEST_PAYMENT_NETBANKING_CANCELLED_BY_USER',
                        'BAD_REQUEST_PAYMENT_POSSIBLE_FRAUD',
                        'BAD_REQUEST_PAYMENT_POSSIBLE_FRAUD_WEBSITE_MISMATCH',
                        'BAD_REQUEST_PAYMENT_TIMED_OUT',
                        'BAD_REQUEST_PAYMENT_VERIFICATION_FAILED',
                        'BAD_REQUEST_TRANSACTION_AMOUNT_LIMIT_EXCEEDED',
                        'BAD_REQUEST_EMANDATE_REGISTRATION_FAILED_JOINT_ACCOUNT',
                        'BAD_REQUEST_EMANDATE_REGISTRATION_DUPLICATE_REQUEST',
                        'BAD_REQUEST_PAYMENT_OTP_INCORRECT',
                        'BAD_REQUEST_PAYMENT_CARD_INVALID_PIN',
                        'BAD_REQUEST_PAYMENT_OTP_VALIDATION_ATTEMPT_LIMIT_EXCEEDED',
                        'BAD_REQUEST_PAYMENT_CARD_EXPIRED',
                        'BAD_REQUEST_PAYMENT_CARD_NUMBER_POSSIBLY_INVALID',
                        'BAD_REQUEST_PAYMENT_DECLINED_BY_BANK_DUE_TO_BLOCKED_CARD',
                        'BAD_REQUEST_EMANDATE_INACTIVE',
                        'BAD_REQUEST_CARD_INACTIVE',
                        'BAD_REQUEST_PAYMENT_CARD_INVALID_CVV',
                        'BAD_REQUEST_VALIDATION_FAILURE',
                        'BAD_REQUEST_ACCOUNT_DORMANT',
                        'BAD_REQUEST_PAYMENT_CARD_INVALID_EXPIRY_DATE',
                        'SERVER_ERROR_RUNTIME_ERROR'
                    ) THEN 'customer'
                    WHEN a.internal_error_code IN (
                        'SERVER_ERROR_OTP_ELF_INVALID_ARGUMENT',
                        'GATEWAY_ERROR_REQAUTH_DECLINED',
                        'GATEWAY_ERROR_PAYMENT_VERIFICATION_ERROR',
                        'GATEWAY_ERROR_REQUEST_ERROR',
                        'GATEWAY_ERROR_UNKNOWN_ERROR',
                        'SERVER_ERROR_AMOUNT_TAMPERED',
                        'SERVER_ERROR_ASSERTION_ERROR',
                        'GATEWAY_ERROR_CHECKSUM_MATCH_FAILED'
                    ) THEN 'internal'
                    WHEN a.internal_error_code IN (
                        'GATEWAY_ERROR_CREDIT_FAILED',
                        'GATEWAY_ERROR_BENEFICIARY_BANK_DEEMED_HIGH_RESPONSE_TIME_CHECK_DECLINE',
                        'GATEWAY_ERROR_REMITTER_BANK_DEEMED_HIGH_RESPONSE_TIME_CHECK_DECLINE',
                        'GATEWAY_ERROR_PAYMENT_FAILED',
                        'GATEWAY_ERROR_MANDATE_CREATION_TIMEOUT_AT_REMITTER_END',
                        'GATEWAY_ERROR_REMITTER_CUTOFF_IN_PROGRESS',
                        'GATEWAY_ERROR_SUPPORT_AUTH_NOT_FOUND',
                        'GATEWAY_ERROR_FATAL_ERROR',
                        'GATEWAY_ERROR_INVALID_PARAMETERS',
                        'GATEWAY_ERROR_INVALID_RESPONSE',
                        'GATEWAY_ERROR_ISSUER_DOWN',
                        'GATEWAY_ERROR_MANDATE_CREATION_FAILED',
                        'GATEWAY_ERROR_MERCHANT_IP_NOT_WHITELISTED',
                        'GATEWAY_ERROR_COMMUNICATION_ERROR',
                        'GATEWAY_ERROR_REQUEST_TIMEOUT'
                    ) THEN 'gateway'
                    ELSE 'business'
                END
            ELSE a.internal_error_code
        END AS `error_source`,
        a.internal_error_code,
        created_date,
        CASE 
            WHEN cast(a.created_date as date) BETWEEN date '{start_date}' AND date '{end_date}' THEN 'T-2'
            WHEN cast(a.created_date as date) BETWEEN date '{previous_start_date}' AND date '{previous_end_date}' THEN 'T-1'
            ELSE NULL
        END AS period,
        count(distinct CASE WHEN authorized_at IS NOT NULL THEN a.id ELSE NULL END) AS payment_success,
        count(distinct a.id) AS payment_attempts
    FROM 
        (select * from aggregate_ba.payments_optimizer_flag_sincejan2021 where created_date between '{previous_start_date}' and '{end_date}' and optimizer_flag = 1 and golive_date is not null and gateway is not null and method_advanced not in ("Unknown Card", "upi", "card", "paylater", "app", "UPI Unknown")) a 
        INNER JOIN (SELECT id, billing_label, name FROM realtime_hudi_api.merchants) b ON a.merchant_id = b.id group by 1,2,3,4,5,6,7,8,9,10,11,12  """)

# Display count and type of the DataFrame


df_base_data = df_base_data.fillna('na')

# Filtering df_current and df_base based on date range
'''df_current = df_base_data.filter((F.col("created_date") >= start_date) & (F.col("created_date") <= end_date))
df_base = df_base_data.filter((F.col("created_date") >= previous_start_date) & (F.col("created_date") <= previous_end_date))

# Selecting columns and filling NaN values with 'na' for df_transformed_current and df_transformed_base
df_transformed_current = df_current.select(
    "id", "method", "merchant_id", "dba", "network_tokenised_payment", 
    "card_type", "bank", "wallet", "method_advanced", "network", 
    "issuer", "authorized_at", "aggregator", "created_date", 
    "error_code_bucket_reason", "internal_error_code"
).fillna('na')'''

'''df_transformed_base = df_base.select(
    "id", "method", "merchant_id", "dba", "network_tokenised_payment", 
    "card_type", "bank", "wallet", "method_advanced", "network", 
    "issuer", "authorized_at", "aggregator", "created_date", 
    "error_code_bucket_reason", "internal_error_code"
).fillna('na')'''

# COMMAND ----------

df_data = df_base_data.toPandas()

# COMMAND ----------

drilled_SR_df = df_data

# Print the index and DataFrame shape
#print("Index:", drilled_SR_df.columns)
#print("Dataframe shape:", drilled_SR_df.shape)

# Unique values in index columns
'''print("Unique values in index columns:")
for col in drilled_SR_df.columns:
    print(col, ":", drilled_SR_df[col].unique())

print("Dataframe shape:", drilled_SR_df.shape)'''
#for col in ["method_advanced", "merchant_id", "name", "aggregator", "network_tokenised_payment", "network", "bank", "wallet"]:
    #print(col, ":", drilled_SR_df[col].unique())

# Calculate Success Rates
drilled_SR = drilled_SR_df.pivot_table(
    index=["method_advanced","aggregator","merchant_id","name","network","network_tokenised_payment","bank","wallet"],
    columns="period",
    values=["payment_attempts", "payment_success"],
    aggfunc=sum
)

# Fill missing values with zeros
drilled_SR = drilled_SR.fillna(0)

# Rename the index name
#rilled_SR.index.name = "IndexName"

# Calculate Success Rates for T-2 and T-1
if ("payment_success" in drilled_SR.columns.get_level_values(0)) and ("payment_attempts" in drilled_SR.columns.get_level_values(0)):
    drilled_SR[("T-2 SR%", "")] = round((drilled_SR[("payment_success", "T-2")] / drilled_SR[("payment_attempts", "T-2")]) * 100, 2)
    drilled_SR[("T-1 SR%", "")] = round((drilled_SR[("payment_success", "T-1")] / drilled_SR[("payment_attempts", "T-1")]) * 100, 2)
else:
    print("Columns 'payment_success' or 'payment_attempts' do not exist in the DataFrame.")





# Sort the columns in reverse order
#drilled_SR = drilled_SR.reindex(columns=sorted(drilled_SR.columns, reverse=True))

drilled_SR = drilled_SR.reset_index()

# Display the resulting DataFrame
print("Dataframe shape drilled df:", drilled_SR.shape)

# COMMAND ----------

pivot_table = drilled_SR_df.pivot_table(
    index=["method_advanced","aggregator","merchant_id","name","network","network_tokenised_payment","bank","wallet"],
    columns="period",
    values=["payment_attempts", "payment_success"],
    aggfunc=sum
)

drilled_SR = pivot_table.fillna(0)


# Calculate Success Rates for T-2 and T-1
drilled_SR[("T-2 SR%")] = round((drilled_SR["payment_success"]["T-2"] / drilled_SR["payment_attempts"]["T-2"]) * 100, 4)
drilled_SR[("T-1 SR%")] = round((drilled_SR["payment_success"]["T-1"] / drilled_SR["payment_attempts"]["T-1"]) * 100, 4)
drilled_SR["total"] =  drilled_SR["payment_attempts"]["T-1"] + drilled_SR["payment_attempts"]["T-2"]
drilled_SR["Delta SR"] = round(drilled_SR["T-1 SR%"] - drilled_SR["T-2 SR%"], 2)

# Filter for Delta SR < -1 and total > 1000
#drilled_SR =  drilled_SR[drilled_SR["total"] > 10]

# Sort the columns in reverse order
#drilled_SR = drilled_SR.reindex(columns=sorted(drilled_SR.columns, reverse=True))

drilled_SR = drilled_SR.reset_index()




# Display the pivot table
print(drilled_SR)

# COMMAND ----------

drilled_SR.to_csv("/dbfs/FileStore/Chitraxi/drilled_sr.csv")

# Reset display options to default values (optional)
#pd.reset_option('display.max_rows')
#pd.reset_option('display.max_columns')

spark.read.format('csv').load('dbfs:/FileStore/Chitraxi/drilled_sr.csv').display()


# COMMAND ----------


# Calculate Success Rates
SR = df_data.pivot_table(
    index=["method_advanced", "aggregator"],
    columns="period",
    values=["payment_success", "payment_attempts"],
    aggfunc=sum
)

# Fill missing values with zeros
SR = SR.fillna(0)

# Calculate Success Rates for T-2 and T-1
SR["T-2 SR%_agg"] = round((SR["payment_success"]["T-2"] / SR["payment_attempts"]["T-2"]) *100, 4)
SR["T-1 SR%_agg"] = round((SR["payment_success"]["T-1"] / SR["payment_attempts"]["T-1"]) * 100, 2)
SR["total_agg"] =  SR["payment_attempts"]["T-1"] + SR["payment_attempts"]["T-2"]
SR["Delta_SR_agg"] = round(SR["T-1 SR%_agg"] - SR["T-2 SR%_agg"], 2)

# Filter for Delta SR < -1 and total > 1000
SR_filtered = SR[(SR["Delta_SR_agg"] < -1) & (SR["total_agg"] > 1000)]

# Sort the columns in reverse order
#SR_filtered = SR_filtered.reindex(columns=sorted(SR_filtered.columns, reverse=True))

SR_filtered = SR_filtered.reset_index()

# Display the resulting DataFrame
print(SR_filtered)


# COMMAND ----------

SR_filtered.to_csv("/dbfs/FileStore/Chitraxi/SR_filtered.csv")

spark.read.format('csv').load('dbfs:/FileStore/Chitraxi/SR_filtered.csv').display()

# COMMAND ----------

overall_SR = SR.sum().astype(int)
overall_SR["T-2 SR%"] = round((overall_SR["payment_success"]["T-2"] / overall_SR["payment_attempts"]["T-2"]) * 100, 4)
overall_SR["T-1 SR%"] = round((overall_SR["payment_success"]["T-1"] / overall_SR["payment_attempts"]["T-1"]) * 100, 4)

overall_delta_SR  = overall_SR["T-1 SR%"] - overall_SR["T-2 SR%"]

print(overall_SR)
print(overall_delta_SR)

# COMMAND ----------

merged_df = drilled_SR.merge(SR_filtered, left_index=True, right_index=True,how='inner')


merged_df = merged_df.reset_index()

# Display the resulting merged DataFrame
print(merged_df)

merged_df.to_csv("/dbfs/FileStore/Chitraxi/merged_df.csv")


spark.read.format('csv').load('dbfs:/FileStore/Chitraxi/merged_df.csv').display()


# COMMAND ----------

print(merged_df.columns)

merged_df["%total_T-1"] = round((merged_df["payment_attempts_x"]["T-1"] / merged_df["payment_attempts_y"]["T-1"]) , 4)
merged_df["%total_T-2"] = round((merged_df["payment_attempts_x"]["T-2"] / merged_df["payment_attempts_y"]["T-2"]) , 4)

merged_df = merged_df.fillna(0)

# Identify rows where payment_attempts_x for T-1 is zero
zero_attempts_mask = merged_df[("payment_attempts_x", "T-1")] == 0

# Update values in "T-1 SR%" based on the condition
merged_df.loc[zero_attempts_mask, ("T-1 SR%", "")] = merged_df.loc[zero_attempts_mask, ("T-2 SR%", "")]

# Identify rows where payment_attempts_x for T-2 is zero
zero_attempts_mask = merged_df[("payment_attempts_x", "T-2")] == 0

# Update values in "T-2 SR%" based on the condition
merged_df.loc[zero_attempts_mask, ("T-2 SR%", "")] = merged_df.loc[zero_attempts_mask, ("T-1 SR%", "")]

merged_df["VMM_SR"] = round((merged_df["%total_T-2"] * (merged_df["T-1 SR%"] - merged_df["T-2 SR%"])), 2)
merged_df["VMM_TX"] = round((merged_df["T-1 SR%"] * (merged_df["%total_T-1"] - merged_df["%total_T-2"])) , 2)

grouped_sorted_df_reset = merged_df.reset_index()

# Group by "method_advanced" and "agg", then sort by "VMM_SR" 
#grouped_sorted_df = merged_df.groupby(["method_advanced", "aggregator"]).apply(lambda x: x.sort_values(by=["VMM_SR"], ascending=False))

#grouped_sorted_df = grouped_sorted_df.reset_index()

# Add cumulative sum of "VMM_SR" and "VMM_TX" grouped by "method_advanced" and "agg"
#grouped_sorted_df["VMM_SR_cumsum"] = grouped_sorted_df.groupby(["method_advanced", "aggregator"])["VMM_SR"].cumsum()



# Group by "method_advanced" and "agg", then sort by "VMM_TX" 
#grouped_sorted_df = grouped_sorted_df.groupby(["method_advanced", "aggregator"]).apply(lambda x: x.sort_values(by=["VMM_TX"], ascending=False))

# Add cumulative sum of "VMM_SR" and "VMM_TX" grouped by "method_advanced" and "agg"
#grouped_sorted_df["VMM_TX_cumsum"] = grouped_sorted_df.groupby(["method_advanced", "aggregator"])["VMM_TX"].cumsum()


#grouped_sorted_df = grouped_sorted_df.groupby(["method_advanced", "aggregator"]).apply(lambda x: x.sort_values(by=["VMM_SR","VMM_TX"], ascending=False))


# COMMAND ----------

#grouped_sorted_df.to_csv("/dbfs/FileStore/Chitraxi/merged_df.csv")

#spark.read.format('csv').load('dbfs:/FileStore/Chitraxi/merged_df.csv').display()

# COMMAND ----------

# Reset index to avoid having an index with the same name as one of the columns
#grouped_sorted_df_reset = merged_df.reset_index()

# Group by "method_advanced" and "aggregator", and get the top 5 rows based on "VMM_SR"
top5_vmm_sr = grouped_sorted_df_reset.groupby(["method_advanced_x", "aggregator_x"]).apply(lambda x: x.nsmallest(5, "VMM_SR"))

# Reset index of the resulting DataFrame
top5_vmm_sr_reset = top5_vmm_sr.reset_index(drop=True)

# Print the columns and the DataFrame
print(top5_vmm_sr_reset.columns)
print(top5_vmm_sr_reset)

# Concatenate the selected columns
#top5_vmm_sr_selected = top5_vmm_sr[selected_columns]

# Sort the columns in reverse order
#top5_vmm_sr_selected = top5_vmm_sr_selected.reindex(columns=sorted(top5_vmm_sr_selected.columns, reverse=True)).astype('double')

# Display the resulting DataFrame
#top5_vmm_sr_selected

#print(top5_vmm_sr_selected.columns)



# COMMAND ----------

top5_vmm_sr_reset.to_csv("/dbfs/FileStore/Chitraxi/top5_vmm_sr_reset.csv")

# Reset display options to default values (optional)
#pd.reset_option('display.max_rows')
#pd.reset_option('display.max_columns')

spark.read.format('csv').load('dbfs:/FileStore/Chitraxi/top5_vmm_sr_reset.csv').display()

# COMMAND ----------

top5_vmm_sr.to_csv("/dbfs/FileStore/Chitraxi/top5_vmm_sr.csv")
spark.read.format('csv').load('dbfs:/FileStore/Chitraxi/top5_vmm_sr.csv').display()
