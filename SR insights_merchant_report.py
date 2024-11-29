# Databricks notebook source
# Install the python-docx library
#%pip install python-docx

# COMMAND ----------

# Import necessary libraries
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import pyspark.sql.types as T
from pyspark.sql.window import Window as W
from datetime import datetime
import pandas as pd
#from docx import Document
#from docx.shared import Pt, Inches
#import docx.shared
from datetime import datetime
import shutil
from IPython.display import HTML

# Create Spark session
spark = SparkSession.builder.appName("Optimizer Apollo SR insights script").getOrCreate()

# Define date ranges
start_date = '2024-08-01'
end_date = '2024-08-31'
previous_start_date = '2024-07-01'
previous_end_date = '2024-07-31'

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
        case when a.network_tokenised_payment in (0,2) then 0 else network_tokenised_payment end as network_tokenised_payment,
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
            WHEN cast(a.created_date as date) BETWEEN date '{start_date}' AND date '{end_date}' THEN 'T-1'
            WHEN cast(a.created_date as date) BETWEEN date '{previous_start_date}' AND date '{previous_end_date}' THEN 'T-2'
            ELSE NULL
        END AS period,
        COUNT(DISTINCT CASE WHEN authorized_at IS NOT NULL and authorized_at != 0 THEN a.id ELSE NULL END) AS payment_success,
        COUNT(DISTINCT a.id) AS payment_attempts
    FROM 
        (
            SELECT * 
            FROM aggregate_ba.payments_optimizer_flag_sincejan2021 
            WHERE created_date BETWEEN '{previous_start_date}' AND '{end_date}' 
            and created_date >= golive_date
            AND optimizer_flag = 1 
            AND golive_date IS NOT NULL 
            AND gateway IS NOT NULL 
            AND method_advanced NOT IN ("Unknown Card", "upi", "card", "paylater", "app", "UPI Unknown","app","intl_bank_transfer")
            
            --and merchant_id = 'ENiVVm6cLOMkaa'
        ) a 
        INNER JOIN realtime_hudi_api.merchants b 
        ON a.merchant_id = b.id 
    GROUP BY 
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
""")



#


# COMMAND ----------

# Convert Spark DataFrame to Pandas DataFrame
df_data = df_base_data.fillna('na').toPandas()



# COMMAND ----------

agg_SR = df_data.pivot_table(
    index=[ "merchant_id"],
    columns="period",
    values=["payment_attempts", "payment_success"],
    aggfunc="sum"
).fillna(0).reset_index()

# COMMAND ----------



# Create drilled_SR DataFrame
drilled_SR = df_data.pivot_table(
    index=["method_advanced", "aggregator", "merchant_id", "name", "network", "network_tokenised_payment", "bank"],
    columns="period",
    values=["payment_attempts", "payment_success"],
    aggfunc="sum"
).fillna(0).reset_index()

drilled_SR

print(drilled_SR.columns)

# Calculate success rates for T-2 and T-1
drilled_SR["T-2 SR%"] = round((drilled_SR[("payment_success", "T-2")] / drilled_SR[("payment_attempts", "T-2")]) * 100, 2)
drilled_SR["T-1 SR%"] = round((drilled_SR[("payment_success", "T-1")] / drilled_SR[("payment_attempts", "T-1")]) * 100, 2)


#drilled_SR =  drilled_SR[drilled_SR[("payment_attempts", "T-2")] ]

# COMMAND ----------

merged_df = drilled_SR.merge(agg_SR,
                              on=['merchant_id'], how="inner")
print(merged_df.columns)

# COMMAND ----------

# Filter for highest two values of Rank_ratio_1 within each group
recommended_df = merged_df.sort_values(by='Rank_ratio_1', ascending=False) \
                      .groupby(['method', 'cards_network', 'tokenization_flag', 'payments_wallet',
                                'top_card_issuer', 'top_card_banks', 'agg']) \
                      .head(2)

print(filtered_df)


# COMMAND ----------



# COMMAND ----------

drilled_SR.to_csv("/dbfs/FileStore/Chitraxi/merged_df.csv")


spark.read.format('csv').load('dbfs:/FileStore/Chitraxi/merged_df.csv').display()

# COMMAND ----------


# Identify rows where payment_attempts_x for T-1 is zero
zero_attempts_mask = merged_df[("payment_attempts_x", "T-1")] == 0
merged_df.loc[zero_attempts_mask, ("T-1 SR%", "")] = merged_df.loc[zero_attempts_mask, ("T-2 SR%", "")]

# Identify rows where payment_attempts_x for T-2 is zero
zero_attempts_mask = merged_df[("payment_attempts_x", "T-2")] == 0
merged_df.loc[zero_attempts_mask, ("T-2 SR%", "")] = merged_df.loc[zero_attempts_mask, ("T-1 SR%", "")]

# Calculate %total_T-1 and %total_T-2
merged_df["%total_T-1"] = round((merged_df[("payment_attempts_x", "T-1")] / merged_df[("payment_attempts_y", "T-1")]), 4)
merged_df["%total_T-2"] = round((merged_df[("payment_attempts_x", "T-2")] / merged_df[("payment_attempts_y", "T-2")]), 4)

# Calculate VMM_SR and VMM_TX
merged_df["VMM_SR"] = round((merged_df["%total_T-2"] * (merged_df[("T-1 SR%", "")] - merged_df[("T-2 SR%", "")])), 2)
merged_df["VMM_TX"] = round((merged_df[("T-1 SR%", "")] * (merged_df["%total_T-1"] - merged_df["%total_T-2"])), 2)




# COMMAND ----------

merged_df

# COMMAND ----------

merged_df.to_csv("/dbfs/FileStore/Chitraxi/merged_df.csv")


spark.read.format('csv').load('dbfs:/FileStore/Chitraxi/merged_df.csv').display()

# COMMAND ----------

aggregated_SR_df = df_data.pivot_table(
    index=["method_advanced", "aggregator", "merchant_id", "name","network_tokenised_payment"],
    columns="period",
    values=["payment_attempts", "payment_success"],
    aggfunc="sum"
).fillna(0).reset_index()

aggregated_SR_df

print(aggregated_SR_df.columns)

# Calculate success rates for T-2 and T-1
aggregated_SR_df["T-2 SR%"] = round((aggregated_SR_df[("payment_success", "T-2")] / aggregated_SR_df[("payment_attempts", "T-2")]) * 100, 2)
aggregated_SR_df["T-1 SR%"] = round((aggregated_SR_df[("payment_success", "T-1")] / aggregated_SR_df[("payment_attempts", "T-1")]) * 100, 2)


#aggregated_SR_df =  aggregated_SR_df[aggregated_SR_df[("payment_attempts", "T-2")] > 500]

aggregated_SR_df


agg_merged_df = aggregated_SR_df.merge(agg_SR,
                              on=['merchant_id'], how="inner")
print(agg_merged_df.columns)




# COMMAND ----------

print(agg_merged_df.columns)

# COMMAND ----------

#---------VMM SR calculation--------------

# Identify rows where payment_attempts_x for T-1 is zero
zero_attempts_mask = agg_merged_df[("payment_attempts_x", "T-1")] == 0
agg_merged_df.loc[zero_attempts_mask, ("T-1 SR%", "")] = agg_merged_df.loc[zero_attempts_mask, ("T-2 SR%", "")]

# Identify rows where payment_attempts_x for T-2 is zero
zero_attempts_mask = agg_merged_df[("payment_attempts_x", "T-2")] == 0
agg_merged_df.loc[zero_attempts_mask, ("T-2 SR%", "")] = agg_merged_df.loc[zero_attempts_mask, ("T-1 SR%", "")]

# Calculate %total_T-1 and %total_T-2
agg_merged_df["%total_T-1"] = round((agg_merged_df[("payment_attempts_x", "T-1")] / agg_merged_df[("payment_attempts_y", "T-1")]), 4)
agg_merged_df["%total_T-2"] = round((agg_merged_df[("payment_attempts_x", "T-2")] / agg_merged_df[("payment_attempts_y", "T-2")]), 4)

# Calculate VMM_SR and VMM_TX
agg_merged_df["VMM_SR"] = round((agg_merged_df["%total_T-2"] * (agg_merged_df[("T-1 SR%", "")] - agg_merged_df[("T-2 SR%", "")])), 2)
agg_merged_df["VMM_TX"] = round((agg_merged_df[("T-1 SR%", "")] * (agg_merged_df["%total_T-1"] - agg_merged_df["%total_T-2"])), 2)
#aggregated_SR_df = drilled_SR.groupby(["merchant_id","aggregator","method_advanced","network_tokenised_payment"]).sum().reset_index()

# COMMAND ----------

aggregated_SR_df

# COMMAND ----------

agg_merged_df

# COMMAND ----------

# Group by 'method_advanced' and 'aggregator' and select the smallest 5 records based on 'VMM_SR'
bottom5_vmm_sr = agg_merged_df.groupby(["merchant_id"]).apply(lambda x: x.nsmallest(5, "VMM_SR")).reset_index(drop=True)
top5_vmm_sr = agg_merged_df.groupby(["merchant_id"]).apply(lambda x: x.nlargest(5, "VMM_SR")).reset_index(drop=True)

# COMMAND ----------

#top5_vmm_sr = top5_vmm_sr[top5_vmm_sr["VMM_SR"] < -0.8]

# COMMAND ----------

bottom5_vmm_tx = agg_merged_df.groupby(["merchant_id"]).apply(lambda x: x.nsmallest(5, "VMM_TX")).reset_index(drop=True)
top5_vmm_tx = agg_merged_df.groupby(["merchant_id"]).apply(lambda x: x.nlargest(5, "VMM_TX")).reset_index(drop=True)

#top5_vmm_tx = top5_vmm_tx[top5_vmm_tx["VMM_TX"] < -0.8]


# COMMAND ----------

df_failed = df_data[(df_data["payment_success"] == 0)]

# COMMAND ----------

df_failed

# COMMAND ----------

# Create drilled_SR DataFrame
error_df = df_failed.pivot_table(
    index=["method_advanced", "aggregator", "merchant_id", "name", "network", "network_tokenised_payment", "bank","internal_error_code"],
    columns="period",
    values=["payment_attempts", "payment_success"],
    aggfunc="sum"
).fillna(0).reset_index()

# COMMAND ----------

# Step 1: Confirm column existence
if (("payment_attempts", "T-1") in error_df.columns) and (("payment_attempts", "T-2") in error_df.columns):
    # Step 2: Define the filter condition
    condition = error_df[("payment_attempts", "T-2")] > 0

    # Step 3: Create new column
    error_df["delta_error_count"] = error_df[("payment_attempts", "T-1")] - error_df[("payment_attempts", "T-2")]

    # Step 4: Apply the filter
    error_df = error_df[condition]
else:
    print("Required columns not found in the DataFrame.")

# COMMAND ----------

# Group by the specified columns and sort within each group by payment_attempts
sorted_error_df = error_df.groupby(["method_advanced", "aggregator", "merchant_id", "name", "network", "network_tokenised_payment", "bank"], as_index=False).apply(lambda x: x.sort_values(by=("payment_attempts","T-1"), ascending=False))

# Reset the index
sorted_error_df = sorted_error_df.reset_index(drop=True)

# Show the sorted DataFrame
print(sorted_error_df)


# COMMAND ----------

sorted_error_df

# COMMAND ----------

# Group by specified columns and get top three rows for each group
#limited_error_df = sorted_error_df.groupby(["method_advanced", "aggregator", "merchant_id", "name", "network", "network_tokenised_payment", "bank"]).head(3)

limited_error_df = sorted_error_df.groupby(["method_advanced", "aggregator", "merchant_id", "name", "network_tokenised_payment"]).head(3)
# Show the limited DataFrame
limited_error_df

# COMMAND ----------

# Perform inner join on all common columns
top5_vmmSR_error_df = top5_vmm_sr.merge(limited_error_df, on=["method_advanced", "aggregator", "merchant_id", "name", "network_tokenised_payment"], how="inner")



# COMMAND ----------

top5_vmmSR_error_df

# COMMAND ----------

top5_vmmSR_error_df

top5_vmmSR_error_df["Delta Errors"] = round(((top5_vmmSR_error_df[("payment_attempts_x", "T-1")] - top5_vmmSR_error_df[("payment_attempts_x", "T-2")])/ top5_vmmSR_error_df[("payment_attempts_x", "T-2")] )*100 , 2)

top5_vmmSR_error_df
summary_error_df = top5_vmmSR_error_df[top5_vmmSR_error_df[("Delta Errors", "")] > 0]

summary_error_df

# COMMAND ----------

# Calculate success rate (SR) for different periods
overall_SR = df_data.pivot_table(
    index=[ "merchant_id"],
    columns="period",
    values=["payment_attempts", "payment_success"],
    aggfunc="sum"
).fillna(0)

# Calculate SR for different periods
# Assuming overall_SR is your DataFrame
overall_SR['SR_T-1'] = (overall_SR['payment_success']['T-1'] / overall_SR['payment_attempts']['T-1']) * 100
overall_SR['SR_T-2'] = (overall_SR['payment_success']['T-2'] / overall_SR['payment_attempts']['T-2']) * 100
# Fill NaN values with 0
overall_SR = overall_SR.fillna(0)

overall_SR['delta_SR'] = round(overall_SR['SR_T-1'] - overall_SR['SR_T-2'],2)


# COMMAND ----------

overall_SR

# COMMAND ----------

print(overall_SR.columns)

# COMMAND ----------

summary_error_df


# COMMAND ----------

# Define the schema for the PySpark DataFrame
schema = T.StructType([
    T.StructField("merchant_id", T.StringType(), nullable=True),
    T.StructField("delta_SR",T.DoubleType(), nullable=True),
    T.StructField("delta_change_due_to_error",T.DoubleType(), nullable=True),
    T.StructField("delta_change_due_volume",T.DoubleType(), nullable=True),
    T.StructField("insights_volume_change_pos", T.StringType(), nullable=True),
    T.StructField("insights_volume_change_neg", T.StringType(), nullable=True),
    T.StructField("insights_error_trend_pos", T.StringType(), nullable=True),
    T.StructField("insights_error_trend_neg", T.StringType(), nullable=True),
    T.StructField("created_date", T.StringType(), nullable=True)
])

# Create an empty DataFrame with the defined schema
insights_df = spark.createDataFrame([], schema)

# COMMAND ----------

agg_merged_df

# COMMAND ----------

agg_SR = agg_merged_df.groupby(["merchant_id"]).agg({("VMM_SR", ""): "sum", ("VMM_TX", ""): "sum"})

# COMMAND ----------

overall_agg_sr = overall_SR.join(agg_SR, how="inner",lsuffix="_overall", rsuffix="_agg")
overall_agg_sr

# COMMAND ----------


# Iterate over each row in the DataFrame
for index, row in overall_agg_sr.iterrows():
    delta_SR_overall = round(row["delta_SR"][""],2)
    SR_T1_overall = round(row["SR_T-1"][""],2)
    SR_T2_overall = round(row["SR_T-2"][""],2)
    VMM_SR = round(row["VMM_SR"][""],2)
    VMM_TX = round(row["VMM_TX"][""],2)
    merchant_id = index
    print(merchant_id)
    
    # Print the values
    print("\n" + "#" * 30)  # Large heading
    print(f"Overall Delata SR: {delta_SR_overall} Previous Month SR:{SR_T2_overall}   Current Month SR: {SR_T1_overall}")
    print("#" * 30 + "\n")     

    insights_error_trend_pos = "\n"
    insights_error_trend_neg = "\n"
    insights_volume_change_pos = "\n"
    insights_volume_change_neg = "\n"
       

    # Filter top5_vmm_SR_list based on method and aggregator
    filtered_bottom5_vmm_SR_list = bottom5_vmm_sr[
        (bottom5_vmm_sr[("merchant_id", "")] == merchant_id) ].iterrows()
    


    
    print("#" * 20)  # Medium heading
    print("SR drop due to change in error trends:")
    print("#" * 20 + "\n")  # Medium heading
    Counter1 = 1
    print(filtered_bottom5_vmm_SR_list)
    for inner_index, inner_row in filtered_bottom5_vmm_SR_list:
        # Extract information for the summary
        
        print()
        inner_method = inner_row[("method_advanced", "")]
        inner_aggregator = inner_row[("aggregator", "")]
        merchant_id = inner_row[("merchant_id", "")]
        name = inner_row[("name", "")]
        #bank = inner_row[("bank", "")]
        vmm_sr = round(inner_row[("VMM_SR", "")],2)
        #network = inner_row[("network", "")]
        network_tokenised_payment = inner_row[("network_tokenised_payment", "")]

        if network_tokenised_payment==1:
            tokenization_type = "tokenized payments"
        elif network_tokenised_payment is not None:
            tokenization_type = "non-tokenized payments"

        netwrok_level_SR_dip = merged_df[
        (merged_df[("method_advanced", "")] == inner_method) &
        (merged_df[("aggregator", "")] == inner_aggregator) & (merged_df[("merchant_id", "")] == merchant_id) 
        & (merged_df[("network_tokenised_payment", "")] == network_tokenised_payment) 
        ].sort_values(by=["VMM_SR"], ascending=True).head(3).reset_index() 

        netwrok_level_SR_dip = netwrok_level_SR_dip[netwrok_level_SR_dip["VMM_SR"]<= -0.5]

        netwrok_level_SR_dip

        top_networks = ''

        for index, row in netwrok_level_SR_dip.iterrows():
            top_networks += row[("network","")] + "(" + str(row[("VMM_SR","")]) +"%), "


        print(top_networks)
        if inner_method in ("Credit Card", "Debit Card", "CC emi", "DC emi"):
            print(f"    {Counter1}. On {inner_method} - {inner_aggregator}, for {tokenization_type}, sr dropped by {vmm_sr}%")
            insights_error_trend_neg += f"    \t{Counter1}. On {inner_method} - {inner_aggregator}, for {tokenization_type} SR dropped by {vmm_sr}% for networks {top_networks}\n"

        if inner_method in ("UPI Intent", "UPI Collect"):
            print(f"    {Counter1}. On {inner_method} - {inner_aggregator}, SR dropped by {vmm_sr}%")
            insights_error_trend_neg += f"    \t{Counter1}. For {inner_method} - {inner_aggregator} SR dropped by {vmm_sr}%\n"

        if inner_method in ("netbanking"):
            print(f"    {Counter1}. On {inner_method} - {inner_aggregator},   SR dropped by {vmm_sr}")
            insights_error_trend_neg += f"    \t{Counter1}. For {inner_method} - {inner_aggregator},  SR dropped by {vmm_sr}%\n"

        if inner_method in ("wallet"):
            print(f"    {Counter1}. On {inner_method} - {inner_aggregator}, SR dropped by {vmm_sr}")
            insights_error_trend_neg += f"    \t{Counter1}. For {inner_method} - {inner_aggregator} SR dropped by {vmm_sr}%\n"
        
        filtered_errors = summary_error_df[
        (summary_error_df[("method_advanced", "")] == inner_method) &
        (summary_error_df[("aggregator", "")] == inner_aggregator) & (summary_error_df[("merchant_id", "")] == merchant_id) 
        & (summary_error_df[("network_tokenised_payment", "")] == network_tokenised_payment) 
        ].iterrows()
        
        
        for index,error_row in filtered_errors:
            error = error_row[("internal_error_code", "")]
            delta_error = round(error_row["Delta Errors",""],2)
            current_attempts = error_row[("payment_attempts_x", "T-1")]
            previous_attempts = error_row[("payment_attempts_x", "T-2")]
            print(f"        {error} - incresed by {delta_error}. Previous error count - {previous_attempts} , Current error count- {current_attempts}")
            insights_error_trend_neg += f"       \t \t - {error} incresed by {delta_error}%. Previous error count - {previous_attempts} , Current error count- {current_attempts} \n"
        Counter1 +=1

    print("#" * 20)  # Medium heading
    print("SR drop due to change in Volume:")
    print("#" * 20 + "\n")  # Medium heading
    filtered_bottom5_vmm_tx_list = bottom5_vmm_tx[
        (bottom5_vmm_tx[("merchant_id", "")] == merchant_id) ].iterrows()

    print(filtered_bottom5_vmm_tx_list)
    Counter2 = 1 

    for inner_index, inner_row in filtered_bottom5_vmm_tx_list:
        # Extract information for the summary
        inner_method = inner_row[("method_advanced", "")]
        inner_aggregator = inner_row[("aggregator", "")]
        merchant_id = inner_row[("merchant_id", "")]
        name = inner_row[("name", "")]
        #bank = inner_row[("bank", "")]
        vmm_tx = round(inner_row[("VMM_TX", "")],2)
        #network = inner_row[("network", "")]
        network_tokenised_payment = inner_row[("network_tokenised_payment", "")]
        current_attempts = inner_row[("payment_attempts_x", "T-1")]
        previous_attempts = inner_row[("payment_attempts_x", "T-2")]
        try:
            delta_traffic = round(((inner_row[("payment_attempts_x", "T-1")] - inner_row[("payment_attempts_x", "T-2")]) / inner_row[("payment_attempts_x", "T-2")]) * 100, 2)
        except ZeroDivisionError:
            delta_traffic = 100

        if network_tokenised_payment==1:
            tokenization_type = "tokenized payments"
        elif network_tokenised_payment is not None:
            tokenization_type = "non-tokenized payments"

        netwrok_level_SR_dip = merged_df[
        (merged_df[("method_advanced", "")] == inner_method) &
        (merged_df[("aggregator", "")] == inner_aggregator) & (merged_df[("merchant_id", "")] == merchant_id) 
        & (merged_df[("network_tokenised_payment", "")] == network_tokenised_payment) 
        ].sort_values(by=["VMM_TX"], ascending=True).head(3).reset_index() 

        netwrok_level_SR_dip = netwrok_level_SR_dip[netwrok_level_SR_dip["VMM_TX"]<=0.5]

        top_networks = ''

        for index, row in netwrok_level_SR_dip.iterrows():
            top_networks += row[("network","")] + "(" + str(row[("VMM_TX","")]) +"%), "

        if inner_method in ("Credit Card", "Debit Card", "CC emi", "DC emi"):
            print(f"        {Counter2}. On {inner_method} - {inner_aggregator}, for  {tokenization_type},SR dropped by {vmm_tx}% due to change in volume by {delta_traffic}. Previous attempts {previous_attempts} and Current attempts - {current_attempts }. Network impacted {top_networks}")
            insights_volume_change_neg += f"        \t{Counter2}. On {inner_method} - {inner_aggregator}, for  {tokenization_type},SR dropped by {vmm_tx}% due to change in volume by {delta_traffic}%. Previous attempts {previous_attempts} and Current attempts - {current_attempts }. Network impacted {top_networks}\n"

        if inner_method in ("UPI Intent", "UPI Collect"):
            print(f"        {Counter2}. On {inner_method} - {inner_aggregator}, SR dropped by {vmm_tx}% due to change in volume by {delta_traffic}. Previous attempts {previous_attempts} and Current attempts - {current_attempts }")
            insights_volume_change_neg += f"        \t{Counter2}. On {inner_method} - {inner_aggregator}, SR dropped by {vmm_tx}% due to change in volume by {delta_traffic}%. Previous attempts {previous_attempts} and Current attempts - {current_attempts }\n"

        if inner_method in ("netbanking"):
            print(f"        {Counter2}. For {inner_method} - {inner_aggregator},SR dropped by {vmm_tx}% due to change in volume by {delta_traffic}. Previous attempts {previous_attempts} and Current attempts - {current_attempts }")
            insights_volume_change_neg += f"        \t{Counter2}. On {inner_method} - {inner_aggregator} SR dropped by {vmm_tx}% due to change in volume by {delta_traffic}%. Previous attempts {previous_attempts} and Current attempts - {current_attempts}\n"

        if inner_method in ("wallet"):
            print(f"        {Counter2}. For {inner_method} - {inner_aggregator}, SR dropped by {vmm_tx}% due to change in volume by {delta_traffic}%.Previous attempts {previous_attempts} and Current attempts - {current_attempts }")
            insights_volume_change_neg += f"        \t{Counter2}. On {inner_method} - {inner_aggregator}, SR dropped by {vmm_tx}% due to change in volume by {delta_traffic}%. Previous attempts {previous_attempts} and Current attempts - {current_attempts } \n"
        Counter2+=1

        print() 


    print("#" * 20)  # Medium heading
    print("SR incresed due to change in error_trends:")
    print("#" * 20 + "\n")  # Medium heading
    filtered_top5_vmm_SR_list = top5_vmm_sr[
        (top5_vmm_sr[("merchant_id", "")] == merchant_id) ].iterrows()

    counter3 = 1    
    for inner_index, inner_row in filtered_top5_vmm_SR_list:
        # Extract information for the summary
        
        print()
        inner_method = inner_row[("method_advanced", "")]
        inner_aggregator = inner_row[("aggregator", "")]
        merchant_id = inner_row[("merchant_id", "")]
        name = inner_row[("name", "")]
        #bank = inner_row[("bank", "")]
        vmm_sr = round(inner_row[("VMM_SR", "")],2)
        #network = inner_row[("network", "")]
        network_tokenised_payment = inner_row[("network_tokenised_payment", "")]

        if network_tokenised_payment==1:
            tokenization_type = "tokenized payments"
        elif network_tokenised_payment is not None:
            tokenization_type = "non-tokenized payments"

        netwrok_level_SR_dip = merged_df[
        (merged_df[("method_advanced", "")] == inner_method) &
        (merged_df[("aggregator", "")] == inner_aggregator) & (merged_df[("merchant_id", "")] == merchant_id) 
        & (merged_df[("network_tokenised_payment", "")] == network_tokenised_payment) 
        ].sort_values(by=["VMM_SR"], ascending=False).head(3).reset_index() 

        netwrok_level_SR_dip = netwrok_level_SR_dip[netwrok_level_SR_dip["VMM_SR"]>=0.5] 

        top_networks = ''

        for index, row in netwrok_level_SR_dip.iterrows():
            top_networks += row[("network","")] + "(" + str(row[("VMM_SR","")]) +"%), "
       

        if inner_method in ("Credit Card", "Debit Card", "CC emi", "DC emi"):
            print(f"    {counter3}. On {inner_method} - {inner_aggregator},for {tokenization_type},  SR dropped by {vmm_sr}. Networks impacted {top_networks}")
            insights_error_trend_pos += f"    \t{counter3}. On {inner_method} - {inner_aggregator},for {tokenization_type}, SR dropped by {vmm_sr}%. Networks impacted {top_networks}\n"

        if inner_method in ("UPI Intent", "UPI Collect"):
            print(f"    {counter3}. On {inner_method} - {inner_aggregator} SR dropped by {vmm_sr}")
            insights_error_trend_pos += f"    \t{counter3}. On {inner_method} - {inner_aggregator}, SR dropped by {vmm_sr}%\n"

        if inner_method in ("netbanking"):
            print(f"    {counter3}. On {inner_method} - {inner_aggregator},RC dropped by {vmm_sr}")
            insights_error_trend_pos += f"    \t{counter3}. For {inner_method} - {inner_aggregator} SR dropped by {vmm_sr}%\n"

        if inner_method in ("wallet"):
            print(f"    {counter3}. On {inner_method} - {inner_aggregator}, SR dropped by {vmm_sr}%")
            insights_error_trend_pos += f"    \t{counter3}. For {inner_method} - {inner_aggregator} SR dropped by {vmm_sr}% \n"
        
        filtered_errors = summary_error_df[
        (summary_error_df[("method_advanced", "")] == inner_method) &
        (summary_error_df[("aggregator", "")] == inner_aggregator) & (summary_error_df[("merchant_id", "")] == merchant_id) 
        & (summary_error_df[("network_tokenised_payment", "")] == network_tokenised_payment) 
        ].iterrows()
        
        for index,error_row in filtered_errors:
            error = error_row[("internal_error_code", "")]
            delta_error = round(error_row["Delta Errors",""],2)
            current_attempts = error_row[("payment_attempts_x", "T-1")]
            previous_attempts = error_row[("payment_attempts_x", "T-2")]
            print(f"        {error} - incresed by {delta_error}. Previous error count - {previous_attempts} , Current error count- {current_attempts}")
            insights_error_trend_pos += f"       \t \t - {error} incresed by {delta_error}%. Previous error count - {previous_attempts} , Current error count- {current_attempts} \n"
        counter3 +=1

    print("#" * 20)  # Medium heading
    print("SR incresed due to change in Volume:")
    print("#" * 20 + "\n")  # Medium heading
    filtered_top5_vmm_tx_list = top5_vmm_tx[
        (top5_vmm_tx[("merchant_id", "")] == merchant_id) ].iterrows()

    Counter4 = 1 

    for inner_index, inner_row in filtered_top5_vmm_tx_list:
        # Extract information for the summary
        inner_method = inner_row[("method_advanced", "")]
        inner_aggregator = inner_row[("aggregator", "")]
        merchant_id = inner_row[("merchant_id", "")]
        name = inner_row[("name", "")]
        #bank = inner_row[("bank", "")]
        vmm_tx = inner_row[("VMM_TX", "")]
        #network = inner_row[("network", "")]
        network_tokenised_payment = inner_row[("network_tokenised_payment", "")]
        current_attempts = inner_row[("payment_attempts_x", "T-1")]
        previous_attempts = inner_row[("payment_attempts_x", "T-2")]
        try:
            delta_traffic = round(((inner_row[("payment_attempts_x", "T-1")] - inner_row[("payment_attempts_x", "T-2")]) / inner_row[("payment_attempts_x", "T-2")]) * 100, 2)
        except ZeroDivisionError:
            delta_traffic = 100

        if network_tokenised_payment==1:
            tokenization_type = "tokenized payments"
        elif network_tokenised_payment is not None:
            tokenization_type = "non-tokenized payments"
        
        netwrok_level_SR_dip = merged_df[
        (merged_df[("method_advanced", "")] == inner_method) &
        (merged_df[("aggregator", "")] == inner_aggregator) & (merged_df[("merchant_id", "")] == merchant_id) 
        & (merged_df[("network_tokenised_payment", "")] == network_tokenised_payment) 
        ].sort_values(by=["VMM_TX"], ascending=False).head(3).reset_index() 

        netwrok_level_SR_dip = netwrok_level_SR_dip[netwrok_level_SR_dip["VMM_TX"]>=0.5]

        top_networks = ''

        for index, row in netwrok_level_SR_dip.iterrows():
            top_networks += row[("network","")] + "(" + str(row[("VMM_TX","")]) +"%), " 

        if inner_method in ("Credit Card", "Debit Card", "CC emi", "DC emi"):
            print(f"        {Counter4}. On {inner_method} - {inner_aggregator} for {tokenization_type}, SR dropped by {vmm_tx}% due to change in volume by {delta_traffic}%. Previous attempts {previous_attempts} and Current attempts: {current_attempts }. Networks impacted are {top_networks}")
            insights_volume_change_pos += f"        \t{Counter4}. On {inner_method} - {inner_aggregator} SR dropped by  {vmm_tx}% due to change in volume by {delta_traffic}%.Previous attempts {previous_attempts} and Current attempts: {current_attempts }. Networks impacted SR due to volume change: {top_networks}  \n"


        if inner_method in ("UPI Intent", "UPI Collect"):
            print(f"        {Counter4}. On {inner_method} - {inner_aggregator} SR dropped by {vmm_tx}% due to change in volume by {delta_traffic}%. Previous attempts {previous_attempts} and Current attempts - {current_attempts }")
            insights_volume_change_pos += f"        \t{Counter4}. On {inner_method} - {inner_aggregator} SR dropped by  {vmm_tx}% due to change in volume by {delta_traffic}%.Previous attempts: {previous_attempts} and Current attempts: {current_attempts } \n"

        if inner_method in ("netbanking"):
            print(f"        {Counter4}. On {inner_method} - {inner_aggregator} SR dropped by {vmm_tx} due to change in volume by {delta_traffic}%. Previous attempts {previous_attempts} and Current attempts - {current_attempts }")
            insights_volume_change_pos += f"       \t {Counter4}. On {inner_method} - {inner_aggregator} SR dropped by  {vmm_tx}% due to change in volume by {delta_traffic}%.Previous attempts {previous_attempts} and Current attempts: {current_attempts } \n"

        if inner_method in ("wallet"):
            print(f"        {Counter4}. On {inner_method} - {inner_aggregator} SR dropped by {vmm_tx}% due to change in volume by {delta_traffic}%.Previous attempts {previous_attempts} and Current attempts - {current_attempts }")
            insights_volume_change_pos += f"       \t {Counter4}. On {inner_method} - {inner_aggregator} SR dropped by {vmm_tx}% due to change in volume by {delta_traffic}%.Previous attempts {previous_attempts} and Current attempts: {current_attempts }\n"
        Counter4+=1

    new_row = (merchant_id, float(delta_SR_overall), float(VMM_SR),float(VMM_TX) , insights_volume_change_pos, insights_volume_change_neg, insights_error_trend_pos, insights_error_trend_neg, start_date)

    # Create a DataFrame from the new row
    new_df = spark.createDataFrame([new_row], schema=schema)

    # Union the new DataFrame with the existing DataFrame
    insights_df = insights_df.union(new_df)
    print()

# COMMAND ----------

display(insights_df)

# COMMAND ----------


# Specify the table name in the DataLake
datalake_table_name = "analytics_selfserve.optimizer_SR_insights"

#columns_order = ["source_id", "method_advanced", "gateway","pay_verify_action_pay_verify","pay_verify_json_data_pay_verify","pay_verify_error_Message_pay_verify","verify_action_verify","verify_json_data_verify","verify_error_Message_verify","created_date"]
# Write the DataFrame to the DataLake table
#pivoted_df.select(columns_order).write.format("PARQUET").mode("overwrite").saveAsTable(datalake_table_name)
insights_df.write.insertInto(datalake_table_name)

# COMMAND ----------

netwrok_level_SR_dip = drilled_SR[
        (drilled_SR[("method_advanced", "")] == inner_method) &
        (drilled_SR[("aggregator", "")] == inner_aggregator) & (drilled_SR[("merchant_id", "")] == merchant_id) 
        & (drilled_SR[("network_tokenised_payment", "")] == network_tokenised_payment) 
        ].sort_values(by=["VMM_SR"], ascending=True).head(3).reset_index() 

netwrok_level_SR_dip

netwrok_level_SR_dip = netwrok_level_SR_dip[netwrok_level_SR_dip["VMM_SR"]<= -0.5]

top_networks = ''

for index, row in netwrok_level_SR_dip.iterrows():
            top_networks += row[("network","")] + "(" + str(row[("VMM_SR","")]) +"%), "

print(top_networks)


# COMMAND ----------

netwrok_level_SR_dip

# COMMAND ----------

display(insights_df)

# COMMAND ----------

#spark.read.format('docx').load('/dbfs/FileStore/Chitraxi/output.docx').display()

# COMMAND ----------



top5_vmm_tx.to_csv("/dbfs/FileStore/Chitraxi/SR_filtered.csv")


spark.read.format('csv').load('dbfs:/FileStore/Chitraxi/top5_vmm_tx.csv').display()
