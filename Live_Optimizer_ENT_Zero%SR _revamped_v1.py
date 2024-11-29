# Databricks notebook source
# MAGIC %md
# MAGIC Base 

# COMMAND ----------

# MAGIC %pip install --upgrade jinja2
# MAGIC dbutils.library.restartPython()
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe whs_v.cards_payments_flat_fact

# COMMAND ----------

pip install gspread

# COMMAND ----------

# Standard library imports
import json
import base64
from datetime import datetime
from datetime import timedelta

# Email-related imports
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.message import EmailMessage

# Templating and Google Sheets-related imports
import jinja2
import gspread
from google.oauth2.service_account import Credentials

# Data handling and processing imports
import pandas as pd
import numpy as np

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE  aggregate_ba.final_team_tagging;

# COMMAND ----------

mm_base = spark.sql(f"""
 
         SELECT 
                DISTINCT merchant_id,
                COALESCE(c.billing_label, c.NAME) NAME ,
                a.terminal_id,
                case when date_diff(
                DAY,
                CAST(terminals.create_date AS date),
                CAST(current_date AS date)
              ) <= 20 then 'New' else 'Live' end  as terminal_flag,
                IF(internal_external_flag = 'internal', 'razorpay', gateway) gateway, 
                method_advanced , 
                network, 
                network_tokenised_payment, 
                issuer, 
                upi_app, 
                wallet, 
                bank, 
                a.created_date, 
                count(DISTINCT a.id) AS total_attempts,
                count(DISTINCT CASE WHEN authorized_at IS NOT NULL THEN a.id END) total_success

                 FROM (select * from aggregate_ba.payments_optimizer_flag_sincejan2021 where created_date >= cast(CURRENT_DATE + interval '-7' day AS varchar(10)) and optimizer_flag = 1)  a 
                 --left join analytics_selfserve.optimizer_terminal_flag_trial b
                 --on a.terminal_id = b.terminal_id
                left join (select terminal_id,procurer, min(created_date) as create_date from realtime_terminalslive.terminals group by 1,2) as terminals on terminals.terminal_id = a.terminal_id
                 inner JOIN realtime_hudi_api.merchants c 
                 ON a.merchant_id = c.id 
                 WHERE 
                  
                  merchant_id IN
                                  (
                                  SELECT DISTINCT merchant_id
                                  FROM            aggregate_ba.final_team_tagging
                                  WHERE           team_owner = 'Enterprise')
                  --AND
                  --internal_external_flag != 'internal'
                  AND
                  merchant_id NOT IN ('ELi8nocD30pFkb','O99zYaQcbOQWyx','FYWytivdWJQXmR')
                  AND terminals.procurer = 'merchant' 
                  ---and a.id not in (select id from realtime_hudi_api.payments where created_date >= cast(CURRENT_DATE + interval '-7' day AS varchar(10)) and cps_route != 100 )
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
                    """)

# COMMAND ----------

mm_base_pd = mm_base.toPandas()
mm_base_pd

# COMMAND ----------

mm_base_pd.to_csv("/dbfs/FileStore/Chitraxi/mm_base_pd.csv")


spark.read.format('csv').load('dbfs:/FileStore/Chitraxi/mm_base_pd.csv').display()

# COMMAND ----------

block_list_df = spark.sql(f"""
                       select distinct * from analytics_selfserve.optimizer_mm_live_alert_blocklist where 'pause date'>= cast(CURRENT_DATE + interval '-1' day as varchar(10)) 
                       """)

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table analytics_selfserve.optimizer_mm_live_alert_blocklist

# COMMAND ----------

block_list_pd = block_list_df.toPandas()

# COMMAND ----------

block_list_pd

# COMMAND ----------

# MAGIC %md
# MAGIC #L0 MM
# MAGIC Merchant X gateway X terminal_id

# COMMAND ----------

# MAGIC %md
# MAGIC ##L0 MM live
# MAGIC

# COMMAND ----------

mm_base_pd["created_date"] = pd.to_datetime(mm_base_pd["created_date"])

# Create the pivot table
L0_mm = mm_base_pd.pivot_table(
    index=['merchant_id', 'NAME', 'terminal_id', 'gateway','terminal_flag','created_date'],
    values=['total_attempts', 'total_success'],
    aggfunc=sum
).fillna(0).astype(int)

# Filter for terminal_flag = 'live'
L0_mm_live = L0_mm
#.loc[L0_mm.index.get_level_values('terminal_flag') == 'Live']

L0_mm_live

# Reset the index if you need a flat DataFrame
L0_mm_live = L0_mm.reset_index()
L0_mm_live = L0_mm[L0_mm['total_attempts']>=10]
L0_mm_live['SR'] = ((L0_mm['total_success'] / L0_mm['total_attempts']) * 100).round(2)
L0_mm_live_zero = L0_mm_live[L0_mm_live['total_success'] == 0]
L0_mm_live_zero

L0_mm_live_zero = L0_mm_live_zero.reset_index()

# Get yesterday's date
yesterday = datetime.now() - timedelta(days=1)
yesterday = yesterday.date()

print(yesterday)

# Filter the DataFrame for rows where the date is yesterday
L0_mm_live_zero = L0_mm_live_zero[L0_mm_live_zero['created_date'] == pd.Timestamp(yesterday)]
L0_mm_live_zero

# Get the current column names
columns = L0_mm_live_zero.columns

# Define the live column names
live_column_names = [
    'Merchant ID',
    'Merchant Name',
    'Terminal ID',
    'Gateway',
    'Terminal Flag',
    'Date',
    'Total Attempts',
    'Total Success',
    'Success Rate']

L0_mm_live_zero.columns = live_column_names
L0_mm_live_zero.drop('Total Success', axis=1, inplace=True)
L0_mm_live_zero

# COMMAND ----------

L0_mm_live_zero

# COMMAND ----------

L0_mm_live_zero.insert(0, 'level', 'L0')

# COMMAND ----------

L0_mm_live_zero['L0 Concatenated Column'] = L0_mm_live_zero['Terminal ID'] + '_' + L0_mm_live_zero['Gateway']
L0_mm_live_zero 

# COMMAND ----------

alert_list = L0_mm_live_zero

# COMMAND ----------

alert_list 

# COMMAND ----------

L0_mm_live.to_csv("/dbfs/FileStore/Chitraxi/L0_mm_live.csv")


spark.read.format('csv').load('dbfs:/FileStore/Chitraxi/L0_mm_live.csv').display()

# COMMAND ----------

''''import datetime
from decimal import Decimal
import json

# Function to convert datetime.date or Decimal object to a serializable format
def datetime_handler(x):
    if isinstance(x, datetime.date):
        return x.isoformat()
    elif isinstance(x, Decimal):
        return float(x)
    raise TypeError("Unknown type")

# Assuming L0_mm_live_zero is a DataFrame, convert it to a list of lists
data_to_upload = L0_mm_live_zero.values.tolist()

# Convert dataframe to JSON-serializable format
data_to_upload = [
    [
        json.dumps(datetime_handler(cell)).replace('"', '') if isinstance(cell, (datetime.date, Decimal)) 
        else json.dumps(cell).replace('"', '') if not isinstance(cell, (int, float, bool)) 
        else cell 
        for cell in row
    ] 
    for row in data_to_upload
]

print(data_to_upload)'''

# COMMAND ----------

mask = L0_mm_live_zero['L0 Concatenated Column'].isin(block_list_pd['l0 concatenated'])
L0_mm_live_zero_filtered = L0_mm_live_zero[~mask]
L0_mm_live_zero_filtered

# COMMAND ----------

mm_base_pd['Concatenated Column 1'] = mm_base_pd['terminal_id'] + '_' + mm_base_pd['gateway']
mm_base_pd

# COMMAND ----------

# MAGIC %md
# MAGIC ##L1 MM live
# MAGIC Merchant X gateway X terminal_id X gateway
# MAGIC

# COMMAND ----------

mm_base_pd.head(1)

# COMMAND ----------

L1_mm_live_base = mm_base_pd[
    ~mm_base_pd['Concatenated Column 1'].isin(L0_mm_live_zero['L0 Concatenated Column'])]
L1_mm_live_base

# Create the pivot table
L1_mm = L1_mm_live_base.pivot_table(
    index=['merchant_id', 'NAME', 'terminal_id', 'gateway','method_advanced','terminal_flag','created_date'],
    values=['total_attempts', 'total_success'],
    aggfunc=sum).fillna(0).astype(int)
L1_mm


# Reset the index if you need a flat DataFrame
L1_mm = L1_mm.reset_index()
L1_mm = L1_mm[L1_mm['total_attempts']>=10]
L1_mm['SR'] = ((L1_mm['total_success'] / L1_mm['total_attempts']) * 100).round(2)
L1_mm_zero = L1_mm[L1_mm['total_success'] == 0]
L1_mm_zero

#Filter for terminal_flag = 'live'
#L1_mm = L1_mm.loc[L1_mm.index.get_level_values('terminal_flag') == 'Live']
#L1_mm


# Get yesterday's date
yesterday = datetime.now() - timedelta(days=1)
yesterday = yesterday.date()

# Filter the DataFrame for rows where the date is yesterday
L1_mm_zero = L1_mm_zero[L1_mm_zero['created_date'] == pd.Timestamp(yesterday)]
L1_mm_zero

L1_mm_zero.columns

# # Get the current column names
columns = L1_mm_zero.columns

# # Define the live column names
live_column_names = [
    'Merchant ID',
    'Merchant Name',
    'Terminal ID',
    'Gateway',
    'Method', 
    'Terminal Flag',
    'Date',
    'Total Attempts',
    'Total Success',
    'Success Rate']

L1_mm_zero.columns = live_column_names
L1_mm_zero.drop('Total Success', axis=1, inplace=True)
L1_mm_zero

# COMMAND ----------

L1_mm_zero['L1 Concatenated Column'] = + L1_mm_zero['Terminal ID'] + '_' + L1_mm_zero['Gateway'] + '_' + L1_mm_zero['Method']
L1_mm_zero

# COMMAND ----------

block_list_pd

# COMMAND ----------

L1_mm_zero

# COMMAND ----------

L1_mm_zero.insert(0, 'level', 'L1')

# COMMAND ----------

L1_mm_zero.reset_index()

# COMMAND ----------

alert_list.reset_index()

# COMMAND ----------

#alert_list = alert_list.merge(L1_mm_zero, on=["Merchant ID", "Merchant Name", "Terminal ID", "Gateway"], how='outer')

# COMMAND ----------

L1_mm_zero

# COMMAND ----------

# Check if block_list_pd is not None and then check if it is empty
if block_list_pd is not None and block_list_pd.empty:
    # If block_list_pd is empty, assign L1_mm_zero directly
    L1_mm_zero 
else:
    # If block_list_pd is not None and not empty, filter L1_mm_zero based on the condition
    if block_list_pd is not None:
        L1_mm_zero = L1_mm_zero[~L1_mm_zero['L1 Concatenated Column'].isin(block_list_pd['l1 concatenated'])]

# Output the filtered DataFrame
L1_mm_zero

# COMMAND ----------

# MAGIC %md
# MAGIC Union of L1_mm_zero alerts with default list

# COMMAND ----------

# Union alert_list and L1_mm_zero with missing columns filled with 'NA'
alert_list = alert_list.reindex(columns=set(alert_list.columns).union(L1_mm_zero.columns), fill_value='All')
L1_mm_zero = L1_mm_zero.reindex(columns=set(alert_list.columns), fill_value='All')

# Concatenate the two DataFrames
alert_list = pd.concat([alert_list, L1_mm_zero], ignore_index=True)

# Display the result
alert_list

# COMMAND ----------

mm_base_pd['Concatenated Column 2'] = mm_base_pd['terminal_id'] + '_' + mm_base_pd['gateway'] + '_' + mm_base_pd['method_advanced']

mm_base_pd

# COMMAND ----------

# MAGIC %md
# MAGIC ### L2 Terminal Mx Gateway Method Paramater

# COMMAND ----------

L1_mm_zero

# COMMAND ----------

mm_base_pd

# COMMAND ----------

L2_mm_live_base_non_card_all_columns = mm_base_pd[
    ~mm_base_pd['Concatenated Column 1'].isin(L0_mm_live_zero['L0 Concatenated Column']) &
    ~mm_base_pd['Concatenated Column 2'].isin(L1_mm_zero['L1 Concatenated Column']) &
    ~mm_base_pd['method_advanced'].isin(['Credit Card', 'Debit Card', 'Prepaid Card'])]
L2_mm_live_base_non_card_all_columns

# COMMAND ----------

L2_mm_live_base_non_card_all_columns

# COMMAND ----------

#remove card related columns
L2_mm_live_base_non_card = L2_mm_live_base_non_card_all_columns.drop(columns=['network', 'network_tokenised_payment', 'issuer'])

# Apply additional filters
L2_mm_live_base_non_card = L2_mm_live_base_non_card[
    L2_mm_live_base_non_card["total_attempts"] >= 10
]

'''L2_mm_live_base_non_card["SR"] = (
    (
        L2_mm_live_base_non_card["total_success"]
        / L2_mm_live_base_non_card["total_attempts"]
    )
    * 100
).round(2)'''
L2_mm_live_base_non_card = L2_mm_live_base_non_card[L2_mm_live_base_non_card["total_success"] == 0]
L2_mm_live_base_non_card = L2_mm_live_base_non_card.sort_values(by="total_success")



# Get yesterday's date
yesterday = datetime.now() - timedelta(days = 1)
yesterday = yesterday.date()

# Reset the index for a flat DataFrame
#L2_mm_live_base_non_card = L2_mm_live_base_non_card.reset_index()

#Check if pivot DataFrame is empty
if L2_mm_live_base_non_card.empty:
    L2_mm_live_base_non_card
else:
    # Filter the DataFrame for rows where the date is yesterday
    
    L2_mm_live_base_non_card = L2_mm_live_base_non_card[
        L2_mm_live_base_non_card["created_date"] == pd.Timestamp(yesterday)
    ]
L2_mm_live_base_non_card
#display(L2_mm_live_base_non_card)

L2_mm_live_base_non_card

# COMMAND ----------

L2_mm_live_base_non_card

# COMMAND ----------

'''# Create the pivot table
L2_mm_live_base_non_card = (
    L2_mm_live_base_non_card_all_columns.pivot_table(
        index=[
            "merchant_id",
            "NAME",
            "terminal_id",
            "gateway",
            "method_advanced",
            "terminal_flag",
            "created_date",
            "upi_app",
            "wallet",
            "bank",
        ],
        values=["total_attempts", "total_success"],
        aggfunc=sum,
    )
    .fillna(0)
    .astype(int)
)

L2_mm_live_base_non_card

# Filter for terminal_flag = 'live'
#L2_mm_live_base_non_card = L2_mm_live_base_non_card.loc[
    #L2_mm_live_base_non_card.index.get_level_values("terminal_flag") == "Live"
#]

# Get yesterday's date
#yesterday = datetime.now() - timedelta(1)
#yesterday = yesterday.date()'''

# Reset the index for a flat DataFrame
'''L2_mm_live_base_non_card = L2_mm_live_base_non_card.reset_index()

# Check if pivot DataFrame is empty
if L2_mm_live_base_non_card.empty:
    L2_mm_live_base_non_card
else:
    # Filter the DataFrame for rows where the date is yesterday
    L2_mm_live_base_non_card = L2_mm_live_base_non_card[
        L2_mm_live_base_non_card["created_date"] == pd.Timestamp(yesterday)
    ]
L2_mm_live_base_non_card'''


'''if (
    "total_attempts" not in L2_mm_live_base_non_card.columns
    and "total_success" not in L2_mm_live_base_non_card.columns
):
    L2_mm_live_base_non_card["total_attempts"] = 0
    L2_mm_live_base_non_card[
        "total_success"
    ] = 0  # Setting 'init %' and 'on_hold %' to 0 as 'init' and 'on_hold' do not exist'''

# Apply additional filters
'''L2_mm_live_base_non_card = L2_mm_live_base_non_card[
    L2_mm_live_base_non_card["total_attempts"] >= 5
]'''
'''L2_mm_live_base_non_card["SR"] = (
    (
        L2_mm_live_base_non_card["total_success"]
        / L2_mm_live_base_non_card["total_attempts"]
    )
    * 100
).round(2)
L2_mm_live_base_non_card = L2_mm_live_base_non_card[L2_mm_live_base_non_card["total_success"] == 0]'''
#L2_mm_live_base_non_card = L2_mm_live_base_non_card.sort_values(by="total_success")
#display(L2_mm_live_base_non_card)

# COMMAND ----------

L2_mm_live_base_non_card

# COMMAND ----------

L2_mm_live_base_non_card

# COMMAND ----------

L2_mm_live_base_upi = L2_mm_live_base_non_card.loc[L2_mm_live_base_non_card["method_advanced"].isin(["upi", "UPI Intent", "UPI Collect"])]
L2_mm_live_base_upi

columns_to_keep_3 = {
        "merchant_id": "Merchant ID",
        "NAME": "Merchant Name",
        "terminal_id": "Terminal ID",
        "gateway": "Gateway",
        "method_advanced": "Method",
        "terminal_flag": "Terminal Flag",
        "created_date": "Date",
        "upi_app": "UPI App",
        "total_attempts": "Total Attempts",
        "total_success": "Total Success"
}

# Fixing the typo and referencing the correct DataFrame
L2_mm_live_base_upi = L2_mm_live_base_upi[columns_to_keep_3.keys()].rename(columns=columns_to_keep_3)
L2_mm_live_base_upi

L2_mm_live_base_upi['L2 Concatenated Column'] = L2_mm_live_base_upi['Terminal ID'] + '_' + L2_mm_live_base_upi['Gateway'] + '_' + L2_mm_live_base_upi['Method'] + '_' + L2_mm_live_base_upi['UPI App']
L2_mm_live_base_upi

# COMMAND ----------

# Check if block_list_pd is not None and then check if it is empty
if block_list_pd is not None and block_list_pd.empty:
    # If block_list_pd is empty, assign L1_mm_zero directly
    L2_mm_live_base_upi 
else:
    # If block_list_pd is not None and not empty, filter L1_mm_zero based on the condition
    if block_list_pd is not None:
        L2_mm_live_base_upi = L2_mm_live_base_upi[~L2_mm_live_base_upi['L2 Concatenated Column'].isin(block_list_pd['l2 concatenated upi'])]

# Output the filtered DataFrame
L2_mm_live_base_upi

# COMMAND ----------

L2_mm_live_base_netbanking = L2_mm_live_base_non_card.loc[L2_mm_live_base_non_card["method_advanced"] == "netbanking"]
L2_mm_live_base_netbanking

columns_to_keep_1 = {
    "merchant_id": "Merchant ID",
    "NAME": "Merchant Name",
    "terminal_id": "Terminal ID",
    "gateway": "Gateway",
    "method_advanced": "Method",
    "terminal_flag": "Terminal Flag",
    "created_date": "Date",
    "bank": "Bank",
    "total_attempts": "Total Attempts",
    "total_success": "Total Success",
    #"SR": "Success Rate",
}

# Fixing the typo in the next line
L2_mm_live_base_netbanking = L2_mm_live_base_netbanking[columns_to_keep_1.keys()].rename(columns=columns_to_keep_1)
columns_to_keep_1

L2_mm_live_base_netbanking['L2 Concatenated Column'] = L2_mm_live_base_netbanking['Terminal ID'] + '_' + L2_mm_live_base_netbanking['Gateway'] + '_' + L2_mm_live_base_netbanking['Method'] + '_' + L2_mm_live_base_netbanking['Bank']
L2_mm_live_base_netbanking

# COMMAND ----------

# Check if block_list_pd is not None and then check if it is empty
if block_list_pd is not None and block_list_pd.empty:
    # If block_list_pd is empty, assign L1_mm_zero directly
    L2_mm_live_base_netbanking 
else:
    # If block_list_pd is not None and not empty, filter L1_mm_zero based on the condition
    if block_list_pd is not None:
        L2_mm_live_base_netbanking = L2_mm_live_base_netbanking[~L2_mm_live_base_netbanking['L2 Concatenated Column'].isin(block_list_pd['l2 concatenated netbanking'])]

# Output the filtered DataFrame
L2_mm_live_base_netbanking

# COMMAND ----------

L2_mm_live_base_wallet = L2_mm_live_base_non_card.loc[L2_mm_live_base_non_card["method_advanced"] == "wallet"]
L2_mm_live_base_wallet

columns_to_keep_2 = {
    "merchant_id": "Merchant ID",
    "NAME": "Merchant Name",
    "terminal_id": "Terminal ID",
    "gateway": "Gateway",
    "method_advanced": "Method",
    "terminal_flag": "Terminal Flag",
    "created_date": "Date",
    "wallet": "Wallet",
    "total_attempts": "Total Attempts",
    "total_success": "Total Success",
    #"SR": "Success Rate",
}

# Fixing the typo and referencing the correct DataFrame
L2_mm_live_base_wallet = L2_mm_live_base_wallet[columns_to_keep_2.keys()].rename(columns=columns_to_keep_2)
L2_mm_live_base_wallet

L2_mm_live_base_wallet['L2 Concatenated Column'] = L2_mm_live_base_wallet['Terminal ID'] + '_' + L2_mm_live_base_wallet['Gateway'] + '_' + L2_mm_live_base_wallet['Method'] + '_' + L2_mm_live_base_wallet['Wallet']
L2_mm_live_base_wallet

# COMMAND ----------

# Check if block_list_pd is not None and then check if it is empty
if block_list_pd is not None and block_list_pd.empty:
    # If block_list_pd is empty, assign L1_mm_zero directly
    L2_mm_live_base_wallet 
else:
    # If block_list_pd is not None and not empty, filter L1_mm_zero based on the condition
    if block_list_pd is not None:
        L2_mm_live_base_wallet = L2_mm_live_base_wallet[~L2_mm_live_base_wallet['L2 Concatenated Column'].isin(block_list_pd['l2 concatenated wallet'])]

# Output the filtered DataFrame
L2_mm_live_base_wallet

# COMMAND ----------

# MAGIC %md
# MAGIC Insert Level

# COMMAND ----------

L2_mm_live_base_wallet.insert(0, 'level', 'L2')
L2_mm_live_base_netbanking.insert(0, 'level', 'L2')
L2_mm_live_base_upi.insert(0, 'level', 'L2')

# COMMAND ----------

# MAGIC %md
# MAGIC Union with alert_list
# MAGIC

# COMMAND ----------

# Union alert_list and L1_mm_zero with missing columns filled with 'NA'
alert_list = alert_list.reindex(columns=set(alert_list.columns).union(L2_mm_live_base_wallet.columns), fill_value='NA')
L2_mm_live_base_wallet = L2_mm_live_base_wallet.reindex(columns=set(alert_list.columns), fill_value='NA')
alert_list = alert_list.reindex(columns=set(alert_list.columns).union(L2_mm_live_base_netbanking.columns), fill_value='NA')
L2_mm_live_base_netbanking = L2_mm_live_base_netbanking.reindex(columns=set(alert_list.columns), fill_value='NA')
alert_list = alert_list.reindex(columns=set(alert_list.columns).union(L2_mm_live_base_upi.columns), fill_value='NA')
L2_mm_live_base_upi = L2_mm_live_base_upi.reindex(columns=set(alert_list.columns), fill_value='NA')


# Concatenate the two DataFrames
alert_list = pd.concat([alert_list, L2_mm_live_base_wallet], ignore_index=True)
alert_list = pd.concat([alert_list, L2_mm_live_base_netbanking], ignore_index=True)
alert_list = pd.concat([alert_list, L2_mm_live_base_upi], ignore_index=True)

# Display the result
display(alert_list)

# COMMAND ----------

L1_mm_zero

# COMMAND ----------

L2_mm_live_base_card_all_columns  = mm_base_pd[
    ~mm_base_pd['Concatenated Column 1'].isin(L0_mm_live_zero['L0 Concatenated Column']) &
    ~mm_base_pd['Concatenated Column 2'].isin(L1_mm_zero['L1 Concatenated Column']) &
    mm_base_pd['method_advanced'].isin(['Credit Card', 'Debit Card', 'Prepaid Card'])]
L2_mm_live_base_card_all_columns

# COMMAND ----------



L2_mm_live_base_card_all_columns

# COMMAND ----------


# Create the pivot table
#L2_mm_live_base_card = L2_mm_live_base_card_all_columns.drop(columns=['upi_app', 'wallet', 'bank'])
L2_mm_live_base_card = L2_mm_live_base_card_all_columns.pivot_table(
    index=[ 'merchant_id', 'NAME', 'terminal_id', 'terminal_flag' , 'gateway', 'method_advanced',
           'network_tokenised_payment','created_date'],
    values=['total_attempts', 'total_success'],
    aggfunc=sum).fillna(0).astype(int)

L2_mm_live_base_card

# Get yesterday's date
yesterday = datetime.now() - timedelta(days = 1)
yesterday = yesterday.date()

# Reset the index for a flat DataFrame
L2_mm_live_base_card = L2_mm_live_base_card.reset_index()

# Check if pivot DataFrame is empty
if L2_mm_live_base_card.empty:
    L2_mm_live_base_card
else:
    # Filter the DataFrame for rows where the date is yesterday
    L2_mm_live_base_card = L2_mm_live_base_card[L2_mm_live_base_card['created_date'] == pd.Timestamp(yesterday)]

#L2_mm_live_base_card.reset_index()

if 'total_attempts' not in L2_mm_live_base_card.columns and  'total_success' not in L2_mm_live_base_card.columns:
    L2_mm_live_base_card['total_attempts'] = 0
    L2_mm_live_base_card['total_success'] = 0 

L2_mm_live_base_card

L2_mm_live_base_card = L2_mm_live_base_card.sort_values(by='total_attempts', ascending=False)


L2_mm_live_base_card['SR'] = ((L2_mm_live_base_card['total_success'] / L2_mm_live_base_card['total_attempts']) * 100).round(2)

# # # Apply additional filters
#L2_mm_live_base_card = L2_mm_live_base_card[L2_mm_live_base_card['terminal_flag'] == 'Live']
L2_mm_live_base_card = L2_mm_live_base_card[L2_mm_live_base_card['total_attempts'] >= 10]
L2_mm_live_base_card = L2_mm_live_base_card[L2_mm_live_base_card['total_success'] == 0]
L2_mm_live_base_card['Concatenated Column 1'] = L2_mm_live_base_card['terminal_id'] + '_' + L2_mm_live_base_card['gateway']
L2_mm_live_base_card['Concatenated Column 2'] = L2_mm_live_base_card['terminal_id'] + '_' + L2_mm_live_base_card['gateway'] + '_' + L2_mm_live_base_card['method_advanced']

# Reset the index of L0_mm_live_zero DataFrame
L0_mm_live_zero_reset = L0_mm_live_zero.reset_index(drop=True)

# Filter out rows from L2_mm_live_base_card DataFrame
# where the values in 'Concatenated Column 1' are not present in the corresponding column of L0_mm_live_zero DataFrame
L2_mm_live_base_card = L2_mm_live_base_card[~L2_mm_live_base_card['Concatenated Column 1'].isin(L0_mm_live_zero_reset['L0 Concatenated Column'])]
L2_mm_live_base_card = L2_mm_live_base_card[~L2_mm_live_base_card['Concatenated Column 2'].isin(L1_mm_zero['L1 Concatenated Column'])]

L2_mm_live_base_card

#Select the columns you want to keep and rename them
columns_to_keep = {
        'merchant_id': 'Merchant ID',
        'NAME': 'Merchant Name',
        'terminal_id': 'Terminal ID',
        'gateway': 'Gateway',
        'method_advanced': 'Method',
        'terminal_flag': 'Terminal Flag',
        'network_tokenised_payment' : 'Tokenization',
        'created_date': 'Date',
        'total_attempts': 'Total Attempts',
        'total_success': 'Total Success',
        'SR': 'Success Rate'}

    # Select and rename the columns
L2_mm_live_base_card = L2_mm_live_base_card[columns_to_keep.keys()].rename(columns=columns_to_keep)
L2_mm_live_base_card

# COMMAND ----------

L2_mm_live_base_card

# COMMAND ----------

L2_mm_live_base_card

# COMMAND ----------

print(L2_mm_live_base_card.columns)

# COMMAND ----------

L2_mm_live_base_card['L2 Concatenated Column'] = L2_mm_live_base_card['Terminal ID'] + '_' + L2_mm_live_base_card['Gateway'] + '_' + L2_mm_live_base_card['Method'] + '_' + L2_mm_live_base_card['Tokenization']
L2_mm_live_base_card

# COMMAND ----------

# Check if block_list_pd is not None and then check if it is empty
if block_list_pd is not None and block_list_pd.empty:
    # If block_list_pd is empty, assign L1_mm_zero directly
    L2_mm_live_base_card 
else:
    # If block_list_pd is not None and not empty, filter L1_mm_zero based on the condition
    if block_list_pd is not None:
        L2_mm_live_base_card = L2_mm_live_base_card[~L2_mm_live_base_card['L2 Concatenated Column'].isin(block_list_pd['l2 concatenated cards'])]

# Output the filtered DataFrame
L2_mm_live_base_card

# COMMAND ----------

mm_base_pd['Concatenated Column 3'] = mm_base_pd['terminal_id'] + '_' + mm_base_pd['gateway'] + '_' + mm_base_pd['method_advanced'] + '_' + mm_base_pd['network_tokenised_payment']
mm_base_pd.head(1)

# COMMAND ----------

L2_mm_live_base_card.insert(0,'level','L2')

# COMMAND ----------

L2_mm_live_base_card

# COMMAND ----------

# Union alert_list and L1_mm_zero with missing columns filled with 'NA'
alert_list = alert_list.reindex(columns=set(alert_list.columns).union(L2_mm_live_base_card.columns), fill_value='NA')
L2_mm_live_base_card = L2_mm_live_base_card.reindex(columns=set(alert_list.columns), fill_value='NA')
alert_list = alert_list.reindex(columns=set(alert_list.columns).union(L2_mm_live_base_netbanking.columns), fill_value='NA')


# Concatenate the two DataFrames
alert_list = pd.concat([alert_list, L2_mm_live_base_card], ignore_index=True)


alert_list

# COMMAND ----------

alert_list

# COMMAND ----------

# MAGIC %md
# MAGIC ##L3

# COMMAND ----------

L3_mm_live_base_card = mm_base_pd[
    
    (mm_base_pd['method_advanced'].isin(['Credit Card', 'Debit Card', 'Prepaid Card']))
]

# COMMAND ----------

L3_mm_live_base_card

# COMMAND ----------

from datetime import datetime, timedelta

# Calculate yesterday's date
yesterday = datetime.now() - timedelta(days=1)
yesterday_date = yesterday.date()

# Filter data for T-1 date
L3_mm_live_base_card = mm_base_pd[
    (mm_base_pd['created_date'].dt.date == yesterday_date) &
    (mm_base_pd['method_advanced'].isin(['Credit Card', 'Debit Card', 'Prepaid Card']))
]

# Create pivot table
L3_mm_live_base_card = L3_mm_live_base_card.pivot_table(
    index=['merchant_id', 'NAME', 'terminal_id', 'gateway', 'method_advanced', 'terminal_flag', 'created_date', 'network_tokenised_payment', 'network'],
    values=['total_attempts', 'total_success'],
    aggfunc=sum
).fillna(0).astype(int)

'''# Filter for terminal_flag = 'Live'
L3_mm_live_base_card = L3_mm_live_base_card.loc[L3_mm_live_base_card.index.get_level_values('terminal_flag') == 'Live']'''

# Check and add columns if they don't exist
if 'total_attempts' not in L3_mm_live_base_card.columns:
    L3_mm_live_base_card['total_attempts'] = 0
if 'total_success' not in L3_mm_live_base_card.columns:
    L3_mm_live_base_card['total_success'] = 0
L3_mm_live_base_card

L3_mm_live_base_card['SR'] = ((L3_mm_live_base_card['total_success'] / L3_mm_live_base_card['total_attempts']) * 100).round(2)
L3_mm_live_base_card

# Apply additional filters
L3_mm_live_base_card = L3_mm_live_base_card[L3_mm_live_base_card['total_attempts'] >= 5]
L3_mm_live_base_card = L3_mm_live_base_card[L3_mm_live_base_card['total_success'] == 0]
L3_mm_live_base_card = L3_mm_live_base_card.reset_index()

L3_mm_live_base_card['Concatenated Column 1'] = L3_mm_live_base_card['terminal_id'] + '_' + L3_mm_live_base_card['gateway'] 
L3_mm_live_base_card['Concatenated Column 2'] = L3_mm_live_base_card['terminal_id'] + '_' + L3_mm_live_base_card['gateway']  + '_' + L3_mm_live_base_card['method_advanced']  
L3_mm_live_base_card['Concatenated Column 3'] = L3_mm_live_base_card['terminal_id'] + '_' + L3_mm_live_base_card['gateway']   + '_' + L3_mm_live_base_card['method_advanced']  + '_' + L3_mm_live_base_card['network_tokenised_payment']

L3_mm_live_base_card = L3_mm_live_base_card[~L3_mm_live_base_card['Concatenated Column 1'].isin(L0_mm_live_zero_reset['L0 Concatenated Column'])]
L3_mm_live_base_card = L3_mm_live_base_card[~L3_mm_live_base_card['Concatenated Column 2'].isin(L1_mm_zero['L1 Concatenated Column'])]
L3_mm_live_base_card = L3_mm_live_base_card[~L3_mm_live_base_card['Concatenated Column 3'].isin(L2_mm_live_base_card['L2 Concatenated Column'])]
L3_mm_live_base_card

# Select the columns you want to keep and rename them
columns_to_keep = {
        'merchant_id': 'Merchant ID',
        'NAME': 'Merchant Name',
        'terminal_id': 'Terminal ID',
        'gateway': 'Gateway',
        'method_advanced': 'Method',
        'terminal_flag': 'Terminal Flag',
        'network_tokenised_payment' : 'Tokenization',
        'created_date': 'Date',
        'network' : 'Network',
        'total_attempts': 'Total Attempts',
        'total_success': 'Total Success',
        'SR': 'Success Rate'}

    # Select and rename the columns
L3_mm_live_base_card = L3_mm_live_base_card[columns_to_keep.keys()].rename(columns=columns_to_keep)
L3_mm_live_base_card

# COMMAND ----------

L3_mm_live_base_card

# COMMAND ----------

L3_mm_live_base_card['L3 Concatenated Column'] = L3_mm_live_base_card['Terminal ID'] + '_' + L3_mm_live_base_card['Gateway'] + '_' + L3_mm_live_base_card['Method'] + '_' + L3_mm_live_base_card['Tokenization'] + '_' + L3_mm_live_base_card['Network']
L3_mm_live_base_card

# COMMAND ----------

# Check if block_list_pd is not None and then check if it is empty
if block_list_pd is not None and block_list_pd.empty:
    # If block_list_pd is empty, assign L1_mm_zero directly
    L3_mm_live_base_card 
else:
    # If block_list_pd is not None and not empty, filter L1_mm_zero based on the condition
    if block_list_pd is not None:
        L3_mm_live_base_card = L3_mm_live_base_card[~L3_mm_live_base_card['L3 Concatenated Column'].isin(block_list_pd['l3 concatenated'])]

# Output the filtered DataFrame
L3_mm_live_base_card

# COMMAND ----------

mm_base_pd['Concatenated Column 4'] = mm_base_pd['terminal_id'] + '_' + mm_base_pd['gateway'] + '_' + mm_base_pd['method_advanced'] + '_' + mm_base_pd['network_tokenised_payment'] + '_' + mm_base_pd['network']
mm_base_pd

# COMMAND ----------

# MAGIC %md
# MAGIC commenting L4 content

# COMMAND ----------

'''# Calculate yesterday's date
yesterday = datetime.now() - timedelta(days=1)
yesterday_date = yesterday.date()

# Calculate the date 7 days ago
seven_days_ago = yesterday - timedelta(days=7)
seven_days_ago_date = seven_days_ago.date()

# Filter data for the last 7 days
L4_mm_live_base_card = mm_base_pd[
    (mm_base_pd['created_date'].dt.date >= seven_days_ago_date) &  # Filter for the last 7 days
    (mm_base_pd['created_date'].dt.date <= yesterday_date) &       # Filter up to yesterday's date
    (mm_base_pd['method_advanced'].isin(['Credit Card', 'Debit Card', 'Prepaid Card'])) &
    (mm_base_pd['total_attempts'] >= 5)  # Add condition for attempts >= 5
]

# Create pivot table
L4_mm_live_base_card = L4_mm_live_base_card.pivot_table(
    index=['merchant_id', 'NAME', 'terminal_id', 'gateway', 'method_advanced', 'terminal_flag', 'network_tokenised_payment', 'network','issuer'],
    columns='created_date',  # Remove 'created_date' from list and specify as a single column
    values=['total_attempts', 'total_success'],
    aggfunc=sum
).fillna(0).astype(int)

current_date = datetime.now().date()
date_mapping = {}
unique_dates = sorted(L4_mm_live_base_card.columns.levels[1])
for i, date in enumerate(unique_dates):
    t_date = current_date - timedelta(days=i+1)
    date_mapping[date] = f'T-{i+1} Day'

# Rename the columns using the date_mapping
new_columns = [(val[0], date_mapping.get(val[1], val[1])) for val in L4_mm_live_base_card.columns]
L4_mm_live_base_card.columns = pd.MultiIndex.from_tuples(new_columns)

# Calculate SR (Success Rate) for each date
for t_label in date_mapping.values():
    if ('total_attempts', t_label) in L4_mm_live_base_card.columns and ('total_success', t_label) in L4_mm_live_base_card.columns:
        L4_mm_live_base_card[('SR', t_label)] = (L4_mm_live_base_card[('total_success', t_label)] / L4_mm_live_base_card[('total_attempts', t_label)] * 100).round(2)
    else:
        L4_mm_live_base_card[('SR', t_label)] = 0  # If either column is missing, set SR to 0

L4_mm_live_base_card.fillna(0)

L4_mm_live_base_card = L4_mm_live_base_card.reset_index()
L4_mm_live_base_card

L4_mm_live_base_card = L4_mm_live_base_card[L4_mm_live_base_card[('total_attempts', 'T-1 Day')] >= 5]
for day in ['T-2 Day', 'T-3 Day', 'T-4 Day', 'T-5 Day', 'T-6 Day', 'T-7 Day']:
    L4_mm_live_base_card = L4_mm_live_base_card[L4_mm_live_base_card[('total_attempts', day)] >= 5]
L4_mm_live_base_card

L4_mm_live_base_card = L4_mm_live_base_card[L4_mm_live_base_card[('SR', 'T-1 Day')] == 0]
for day in ['T-2 Day', 'T-3 Day', 'T-4 Day', 'T-5 Day', 'T-6 Day', 'T-7 Day']:
    L4_mm_live_base_card = L4_mm_live_base_card[L4_mm_live_base_card[('SR', day)] == 0]


# Accessing the 'merchant_id', 'terminal_id', and 'gateway' columns and concatenating them
L4_mm_live_base_card['Concatenated Column 1'] = (
                                        L4_mm_live_base_card[('terminal_id', '')] + '_' +
                                        L4_mm_live_base_card[('gateway', '')])

L4_mm_live_base_card['Concatenated Column 2'] = (
                                        L4_mm_live_base_card[('terminal_id', '')] + '_' +
                                        L4_mm_live_base_card[('gateway', '')] + '_' +
                                        L4_mm_live_base_card[('method_advanced', '')])

L4_mm_live_base_card['Concatenated Column 3'] =  (
                                        L4_mm_live_base_card[('terminal_id', '')] + '_' +
                                        L4_mm_live_base_card[('gateway', '')] + '_' +
                                        L4_mm_live_base_card[('method_advanced', '')]+ '_' +
                                        L4_mm_live_base_card[('network_tokenised_payment', '')])

L4_mm_live_base_card['Concatenated Column 4'] =   (
                                        L4_mm_live_base_card[('terminal_id', '')] + '_' +
                                        L4_mm_live_base_card[('gateway', '')] + '_' +
                                        L4_mm_live_base_card[('method_advanced', '')] + '_' +
                                        L4_mm_live_base_card[('network_tokenised_payment', '')] + '_' +
                                        L4_mm_live_base_card[('network', '')])

L4_mm_live_base_card = L4_mm_live_base_card[L4_mm_live_base_card['terminal_flag'] == 'Live']
L4_mm_live_base_card = L4_mm_live_base_card[~L4_mm_live_base_card['Concatenated Column 1'].isin(L0_mm_live_zero_reset['L0 Concatenated Column'])]
L4_mm_live_base_card = L4_mm_live_base_card[~L4_mm_live_base_card['Concatenated Column 2'].isin(L1_mm_zero['L1 Concatenated Column'])]
L4_mm_live_base_card = L4_mm_live_base_card[~L4_mm_live_base_card['Concatenated Column 3'].isin(L2_mm_live_base_card['L2 Concatenated Column'])]
L4_mm_live_base_card = L4_mm_live_base_card[~L4_mm_live_base_card['Concatenated Column 4'].isin(L3_mm_live_base_card['L3 Concatenated Column'])]
L4_mm_live_base_card.columns


# Specify the columns to drop
columns_to_drop = [('total_success', 'T-1 Day'), ('total_success', 'T-2 Day'), ('total_success', 'T-3 Day'),
                   ('total_success', 'T-4 Day'), ('total_success', 'T-5 Day'), ('total_success', 'T-6 Day'),
                   ('total_success', 'T-7 Day')]

# Drop the columns from the DataFrame
L4_mm_live_base_card.drop(columns=columns_to_drop, inplace=True)

# Specify the columns to drop
columns_to_drop = [
            (    'Concatenated Column 1',        ''),
            (    'Concatenated Column 2',        ''),
            (    'Concatenated Column 3',        ''),
            (    'Concatenated Column 4',        '')]

# Drop the columns from the DataFrame
L4_mm_live_base_card.drop(columns=columns_to_drop, inplace=True)
L4_mm_live_base_card

# # Get the current column names
columns = L4_mm_live_base_card.columns

# # Define the live column names
live_column_names = [
    'Merchant ID',
    'Merchant Name',
    'Terminal ID',
    'Gateway',
    'Method', 
    'Terminal Flag',
    'Network Tokenized payments',
    'Network',
    'Issuer',
    'Total Attempts T-1',
    'Total Attempts T-2',
    'Total Attempts T-3',
    'Total Attempts T-4',
    'Total Attempts T-5',
    'Total Attempts T-6',
    'Total Attempts T-7',
    'Success Rate T-1',
    'Success Rate T-2',
    'Success Rate T-3',
    'Success Rate T-4',
    'Success Rate T-5',
    'Success Rate T-6',
    'Success Rate T-7']

L4_mm_live_base_card.columns = live_column_names
L4_mm_live_base_card'''

# COMMAND ----------

'''L4_mm_live_base_card['L4 Concatenated Column'] = L4_mm_live_base_card['Terminal ID'] + '_' + L4_mm_live_base_card['Gateway'] + '_' + L4_mm_live_base_card['Method'] + '_' + L4_mm_live_base_card['Network Tokenized payments'] + '_' + L4_mm_live_base_card['Network'] + '_' + L4_mm_live_base_card['Issuer'] 
L4_mm_live_base_card'''

# COMMAND ----------

'''# Check if block_list_pd is not None and then check if it is empty
if block_list_pd is not None and block_list_pd.empty:
    # If block_list_pd is empty, assign L1_mm_zero directly
    L4_mm_live_base_card 
else:
    # If block_list_pd is not None and not empty, filter L1_mm_zero based on the condition
    if block_list_pd is not None:
        L4_mm_live_base_card = L4_mm_live_base_card[~L4_mm_live_base_card['L4 Concatenated Column'].isin(block_list_pd['l4 concatenated'])]

# Output the filtered DataFrame
L4_mm_live_base_card'''

# COMMAND ----------

# MAGIC %md
# MAGIC #Adding query logic to the table 

# COMMAND ----------

L0_mm_live_zero

# COMMAND ----------

block_list_pd

# COMMAND ----------

# MAGIC %md
# MAGIC #Adding to Block List

# COMMAND ----------

L3_mm_live_base_card

# COMMAND ----------

# Assuming L0_mm_live_zero, L1_mm_zero, L2_mm_live_base_card, L3_mm_live_base_card, L4_mm_live_base_card are DataFrames

def coalesce(*columns):
    for column in columns:
        if not column.isnull().all() and not (column == 0).all():
            return column.dt.date
    return None


date_column = coalesce(
    L0_mm_live_zero["Date"],
    L1_mm_zero["Date"],
    L2_mm_live_base_upi["Date"],
    L2_mm_live_base_wallet["Date"],
    L2_mm_live_base_netbanking["Date"],
    L2_mm_live_base_card["Date"],
    L3_mm_live_base_card["Date"]
)

block_list = pd.concat(
    [
        L0_mm_live_zero["L0 Concatenated Column"],
        L1_mm_zero["L1 Concatenated Column"],
        L2_mm_live_base_card["L2 Concatenated Column"],
        L2_mm_live_base_netbanking["L2 Concatenated Column"],
        L2_mm_live_base_upi["L2 Concatenated Column"],
        L2_mm_live_base_wallet["L2 Concatenated Column"],
        null,
        #L3_mm_live_base_card["L3 Concatenated Column"],
        null,
        #L4_mm_live_base_card["L4 Concatenated Column"],
        date_column,
    ],
    axis=1,
).fillna(0)'''


block_list = pd.concat(
    [
        alert_list["L0 Concatenated Column"],
        alert_list["L1 Concatenated Column"],
        alert_list["L2 Concatenated Column"],
        alert_list["L2 Concatenated Column"],
        alert_list["L2 Concatenated Column"],
        alert_list["L2 Concatenated Column"],
        alert_list["L3 Concatenated Column"],
        #L4_mm_live_base_card["L4 Concatenated Column"],
        date_column,
    ],
    axis=1,
).fillna(0)


block_list

'''# Convert 'Date' column to datetime
block_list["Date"] = pd.to_datetime(block_list["Date"], errors="coerce")

# Convert datetime to date, ignoring NaT values
block_list["Date"] = block_list["Date"].dt.date

# Add timedelta to 'Date' column and then convert to date
block_list["Pause Date"] = block_list["Date"] + timedelta(days=2)

block_list.columns = [
    "L0 Concatenated Column",
    "L1 Concatenated Column",
    "L2 Concatenated Column",
    "L2 Concatenated Column",
    "L2 Concatenated Column",
    "L2 Concatenated Column",
    "L3 Concatenated Column",
    "L4 Concatenated Column",
    "Date",
    "Pause Date",
]
block_list '''

# COMMAND ----------

block_list

# COMMAND ----------

# from pandas import to_datetime, Timedelta
# import pandas as pd

# # Assuming L0_mm_live_zero, L1_mm_zero, L2_mm_live_base_card, L3_mm_live_base_card, L4_mm_live_base_card are DataFrames

# def coalesce(*columns):
#     for column in columns:
#         # Convert to datetime if not already
#         column = to_datetime(column, errors='coerce')
#         if not column.isnull().all() and not (column == 0).all():
#             return column.dt.date
#     return None

# date_column = coalesce(
#     L0_mm_live_zero["Date"],
#     L1_mm_zero["Date"],
#     L2_mm_live_base_upi["Date"],
#     L2_mm_live_base_wallet["Date"],
#     L2_mm_live_base_netbanking["Date"],
#     L2_mm_live_base_card["Date"],
#     L3_mm_live_base_card["Date"]
# )

# # Ensure date_column is a Series and not None
# if date_column is None:
#     date_column = pd.Series([None] * len(L0_mm_live_zero))

# block_list = pd.concat(
#     [
#         L0_mm_live_zero["Concatenated Column"],
#         L1_mm_zero["Concatenated Column"],
#         L2_mm_live_base_card["Concatenated Column"],
#         L2_mm_live_base_netbanking["Concatenated Column"],
#         L2_mm_live_base_upi["Concatenated Column"],
#         L2_mm_live_base_wallet["Concatenated Column"],
#         L3_mm_live_base_card["Concatenated Column"],
#         L4_mm_live_base_card["Concatenated Column"],
#         date_column,
#     ],
#     axis=1,
# ).fillna(0)

# # Convert 'Date' column to datetime
# block_list["Date"] = pd.to_datetime(block_list["Date"], errors="coerce")

# # Convert datetime to date, ignoring NaT values
# block_list["Date"] = block_list["Date"].dt.date

# # Add timedelta to 'Date' column and then convert to date
# block_list["Pause Date"] = pd.to_datetime(block_list["Date"]) + Timedelta(days=2)
# block_list["Pause Date"] = block_list["Pause Date"].dt.date

# block_list.columns = [
#     "L0 Concatenated Column",
#     "L1 Concatenated Column",
#     "L2 Card Concatenated Column",
#     "L2 Netbanking Concatenated Column",
#     "L2 UPI Concatenated Column",
#     "L2 Wallet Concatenated Column",
#     "L3 Concatenated Column",
#     "L4 Concatenated Column",
#     "Date",
#     "Pause Date",
# ]

# COMMAND ----------

# from pyspark.sql import SparkSession

# # Assuming spark is your SparkSession
# spark = SparkSession.builder.getOrCreate()

# # Assuming `block_list` is your Pandas DataFrame
# # Replace `NaT` with `None` for date columns
# block_list['Date'] = block_list['Date'].where(pd.notnull(block_list['Date']), None)
# block_list['Pause Date'] = block_list['Pause Date'].where(pd.notnull(block_list['Pause Date']), None)

# # Now convert the Pandas DataFrame to a PySpark DataFrame with the defined schema
# block_list_spark_df = spark.createDataFrame(block_list, schema=schema)

# # Specify the table name in the DataLake
# datalake_table_name = "analytics_selfserve.optimizer_mm_live_alert_blocklist"

# columns_order = [
#     "L0 Concatenated Column",
#     "L1 Concatenated Column",
#     "L2 Card Concatenated Column",
#     "L2 Netbanking Concatenated Column",
#     "L2 UPI Concatenated Column",
#     "L2 Wallet Concatenated Column",
#     "L3 Concatenated Column",
#     "L4 Concatenated Column",
#     "Date",
#     "Pause Date",
# ]
# # Write the DataFrame to the DataLake table
# # pivoted_df.select(columns_order).write.format("PARQUET").mode("overwrite").saveAsTable(datalake_table_name)
# block_list_spark_df.select(columns_order).write.insertInto(datalake_table_name)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql import SparkSession

# Assuming spark is your SparkSession
spark = SparkSession.builder.getOrCreate()

# Define the schema explicitly
schema = StructType([
    StructField("L0 Concatenated Column", StringType(), True),
    StructField("L1 Concatenated Column", StringType(), True),
    StructField("L2 Concatenated Column", StringType(), True),
    StructField("L4 Concatenated Column", StringType(), True),
    StructField("Date", DateType(), True),
    StructField("Pause Date", DateType(), True),
])

# Convert the Pandas DataFrame to a PySpark DataFrame with the defined schema
block_list_spark_df = spark.createDataFrame(block_list, schema=schema)

# Specify the table name in the DataLake
datalake_table_name = "analytics_selfserve.optimizer_mm_live_alert_blocklist"

# Write the DataFrame to the DataLake table
block_list_spark_df.write.insertInto(datalake_table_name)

# COMMAND ----------

'''L0_mm_live_zero.drop('Concatenated Column', axis=1, inplace=True)
L1_mm_zero.drop('Concatenated Column', axis=1, inplace=True)
L2_mm_live_base_card.drop('Concatenated Column', axis=1, inplace=True)
L3_mm_live_base_card.drop('Concatenated Column', axis=1, inplace=True)
L4_mm_live_base_card.drop('Concatenated Column', axis=1, inplace=True)''''

# COMMAND ----------


# L0_mm_live_zero['Date'] = pd.to_datetime(L0_mm_live_zero['Date']).dt.date
# L1_mm_zero['Date'] = pd.to_datetime(L1_mm_zero['Date']).dt.date
# L2_mm_live_base_card['Date'] = pd.to_datetime(L2_mm_live_base_card['Date']).dt.date
# L2_mm_live_base_netbanking['Date'] = pd.to_datetime(L2_mm_live_base_netbanking['Date']).dt.date
# L2_mm_live_base_upi['Date'] = pd.to_datetime(L2_mm_live_base_upi['Date']).dt.date
# L2_mm_live_base_wallet['Date'] = pd.to_datetime(L2_mm_live_base_wallet['Date']).dt.date
# L3_mm_live_base_card['Date'] = pd.to_datetime(L3_mm_live_base_card['Date']).dt.date

# COMMAND ----------

L1_mm_zero

# COMMAND ----------

'''if not L0_mm_live_zero.empty:
    L0_mm_live_zero['Date'] = L0_mm_live_zero['Date'].astype(str).str[:10]

if not L1_mm_zero.empty:
    L1_mm_zero['Date'] = L1_mm_zero['Date'].astype(str).str[:10]

if not L2_mm_live_base_card.empty:
    L2_mm_live_base_card['Date'] = L2_mm_live_base_card['Date'].astype(str).str[:10]

if not L2_mm_live_base_netbanking.empty:
    L2_mm_live_base_netbanking['Date'] = L2_mm_live_base_netbanking['Date'].astype(str).str[:10]

if not L2_mm_live_base_upi.empty:
    L2_mm_live_base_upi['Date'] = L2_mm_live_base_upi['Date'].astype(str).str[:10]

if not L2_mm_live_base_wallet.empty:
    L2_mm_live_base_wallet['Date'] = L2_mm_live_base_wallet['Date'].astype(str).str[:10]

if not L3_mm_live_base_card.empty:
    L3_mm_live_base_card['Date'] = L3_mm_live_base_card['Date'].astype(str).str[:10]'''

# COMMAND ----------

L1_mm_zero

# COMMAND ----------

'''def L0_generate_query(row):
    query = "SELECT * FROM aggregate_ba.payments_optimizer_flag_sincejan2021 WHERE created_date = '" + str(row["Date"]) + \
            "' and terminal_id = '" + str(row["Terminal ID"]) + \
            "' and gateway = '" + str(row["Gateway"])
    

if not L0_mm_live_zero.empty:
    L0_mm_live_zero["Query Logic"] = L0_mm_live_zero.apply(L0_generate_query, axis=1)
else:
    print('Nothing to be done')


def L1_generate_query(row):
    # Assuming 'row' is a Series object representing a DataFrame row
    # Convert all non-string values to strings before concatenation
    query = "SELECT * FROM aggregate_ba.payments_optimizer_flag_sincejan2021 WHERE created_date = '" + str(row["Date"]) + \
            "' and terminal_id = '" + str(row["Terminal ID"]) + \
            "' and gateway = '" + str(row["Gateway"]) + \
            "' and method_advanced = '" + str(row["Method"]) + "'"
    return query

if not L1_mm_zero.empty:
    L1_mm_zero["Query Logic"] = L1_mm_zero.apply(L1_generate_query, axis=1)
else:
    print('Nothing to be done')


def L2_generate_query_cards(row):
    query = "SELECT * FROM aggregate_ba.payments_optimizer_flag_sincejan2021 WHERE created_date = '" + str(row["Date"]) + \
            "' and terminal_id = '" + str(row["Terminal ID"]) + \
            "' and gateway = '" + str(row["Gateway"]) + \
            "' and method_advanced = '" + str(row["Method"]) + \
            "' and network_tokenised_payment = '" + str(row["Tokenization"]) + "'"
    return query    
    
if not L2_mm_live_base_card.empty:
    L2_mm_live_base_card["Query Logic"] = L2_mm_live_base_card.apply(L2_generate_query_cards, axis=1)
else:
    print('Nothing to be done')

def L2_upi_generate_query_cards(row):
    # Assuming 'row' is a Series object representing a DataFrame row
    # Convert all non-string values to strings before concatenation
    query = "SELECT * FROM aggregate_ba.payments_optimizer_flag_sincejan2021 WHERE created_date = '" + str(row["Date"]) + \
            "' and terminal_id = '" + str(row["Terminal ID"]) + \
            "' and gateway = '" + str(row["Gateway"]) + \
            "' and method_advanced = '" + str(row["Method"]) + \
            "' and upi_app = '" + str(row["UPI App"]) + "'"
    return query
    
if not L2_mm_live_base_upi.empty:
    L2_mm_live_base_upi["Query Logic"] = L2_mm_live_base_upi.apply(L2_upi_generate_query_cards, axis=1)
else:
    print('Nothing to be done')

def L2_netbanking_generate_query_cards(row):
    # Assuming 'row' is a Series object representing a DataFrame row
    # Convert all non-string values to strings before concatenation
    query = "SELECT * FROM aggregate_ba.payments_optimizer_flag_sincejan2021 WHERE created_date = '" + str(row["Date"]) + \
            "' and terminal_id = '" + str(row["Terminal ID"]) + \
            "' and gateway = '" + str(row["Gateway"]) + \
            "' and method_advanced = '" + str(row["Method"]) + \
            "' and bank = '" + + str(row["Bank"]) + "'"
    return query
    
if not L2_mm_live_base_netbanking.empty:
    L2_mm_live_base_netbanking["Query Logic"] = L2_mm_live_base_netbanking.apply(L2_netbanking_generate_query_cards, axis=1)
else:
    print('Nothing to be done')

def L2_wallet_generate_query_cards(row):
    # Assuming 'row' is a Series object representing a DataFrame row
    # Convert all non-string values to strings before concatenation
    query = "SELECT * FROM aggregate_ba.payments_optimizer_flag_sincejan2021 WHERE created_date = '" + str(row["Date"]) + \
            "' and terminal_id = '" + str(row["Terminal ID"]) + \
            "' and gateway = '" + str(row["Gateway"]) + \
            "' and method_advanced = '" + str(row["Method"]) + \
            "' and wallet = '" + + str(row["Wallet"]) + "'"
    return query
    
if not L2_mm_live_base_wallet.empty:
    L2_mm_live_base_wallet["Query Logic"] = L2_mm_live_base_wallet.apply(L2_wallet_generate_query_cards, axis=1)
else:
    print('Nothing to be done')

def L3_generate_query_cards(row):
    # Assuming 'row' is a Series object representing a DataFrame row
    # Convert all non-string values to strings before concatenation
    query = "SELECT * FROM aggregate_ba.payments_optimizer_flag_sincejan2021 WHERE created_date = '" + str(row["Date"]) + \
            "' and terminal_id = '" + str(row["Terminal ID"]) + \
            "' and gateway = '" + str(row["Gateway"]) + \
            "' and method_advanced = '" + str(row["Method"]) + \
            "' and network_tokenised_payment = '" + str(row["Tokenization"]) + \
            "' and network = '" + str(row["Network"]) + "'"
    return query
    
if not L3_mm_live_base_card.empty:
    L3_mm_live_base_card["Query Logic"] = L3_mm_live_base_card.apply(L3_generate_query_cards, axis=1)
else:
    print('Nothing to be done')'''


# COMMAND ----------

L1_mm_zero

# COMMAND ----------

# MAGIC %md
# MAGIC #Sending Email

# COMMAND ----------

# MAGIC %md
# MAGIC ##L0 

# COMMAND ----------

'''import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Assuming L0_mm_live_zero is a valid DataFrame and other necessary imports are done

if not L0_mm_live_zero.empty:

    # Convert DataFrame to HTML table
    html_table = L0_mm_live_zero.to_html(index=False, border=1, classes='styled-table')

    # Email details
    sender_email = "chitraxi.raj@razorpay.com"
    receiver_email = "chitraxi.raj@razorpay.com"
    cc_email = 'chitraxi.raj@razorpay.com'  # This is already a string, not a list
    subject = "Trial : L0 Alerts for 0% SR for Optimizer Terminal Revamp V1"

    # Create email message
    message = MIMEMultipart("alternative")
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Cc"] = cc_email  # No need to join since it's already a string
    message["Subject"] = subject

    # Create HTML email content with CSS styling
    # Assuming the HTML content is correctly formatted as before

    # Attach HTML content to email
    email_body = MIMEText(html_content, "html")
    message.attach(email_body)

    # Connect to SMTP server and send email
    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls()
        server.login("chitraxi.raj@razorpay.com", "lzus crbk nefm tsdd")
        recipients = [receiver_email] + [cc_email]  # Ensure recipients is a list
        server.sendmail(sender_email, recipients, message.as_string())

    print("Email sent successfully!")

else:
    print("DataFrame is empty. Email not sent.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##L1

# COMMAND ----------

'''# Assuming you have the 'cards_dod' DataFrame ready

if not L1_mm_zero.empty:

  # Convert DataFrame to HTML table
  html_table = L1_mm_zero.to_html(index=False, border=1, classes='styled-table')

  # Email details
  sender_email = "syed.yamaan@razorpay.com"
  receiver_email = "chitraxi.raj@razorpay.com"
  cc_email = ['chitraxi.raj@razorpay.com']  # Add your CC email addresses here
  subject = "L1 Alerts for 0% SR for Optimizer Terminal"

  # Create email message
  message = MIMEMultipart("alternative")
  message["To"] = receiver_email  # No need to convert to string for a single recipient
  message["Cc"] = ', '.join(cc_email)  # Convert list to string for CC field
  message["Subject"] = subject

  # Create HTML email content with CSS styling
  html_content = f"""
  <html>
    <head>
      <style>
        .styled-table {{
          border-collapse: collapse;
          width: 100%;
        }}
        .styled-table th, .styled-table td {{
          border: 1px solid black;
          padding: 8px;
          text-align: center;
          white-space: nowrap;
          ;
        }}
        .styled-table th {{
          font-weight: bold;
        }}
      </style>
    </head>
    <body>
      <p>Hello team,<br><br>
        Please find below the live terminal along with their gateway and method having 0% SR on method as of yesterday:<br><br>
        {html_table}<br><br>
        Kindly check these terminals and make the changes so that the issue is resolved.
        <br><br>
        Kind Regards,<br>
        Syed Mohammad Yamaan<br>
        7987388624
      </p>
    </body>
  </html>
  """

  # Attach HTML content to email
  email_body = MIMEText(html_content, "html")
  message.attach(email_body)

  # Connect to SMTP server and send email
  with smtplib.SMTP('smtp.gmail.com', 587) as server:
      server.starttls()
      server.login("chitraxi.raj@razorpay.com", "vyda vhoq ixea yfcj")
      recipients = [receiver_email] + cc_email  # Combine To and CC recipients for sending
      server.sendmail(sender_email, recipients, message.as_string())

  print("Email sent successfully!")

else:
    print("DataFrame is empty. Email not sent.")

# COMMAND ----------

'''# Assuming you have the 'cards_dod' DataFrame ready

if not L2_mm_live_base_card.empty:

  # Convert DataFrame to HTML table
  html_table = L2_mm_live_base_card.to_html(index=False, border=1, classes='styled-table')

  sender_email = "syed.yamaan@razorpay.com"
  receiver_email = "chitraxi.raj@razorpay.com"
  cc_email = ['chitraxi.raj@razorpay.com']  # Add your CC email addresses here
  subject = "L2 Alerts for 0% SR for Optimizer Terminal (cards)"

  # Create email message
  message = MIMEMultipart("alternative")
  message["To"] = receiver_email  # No need to convert to string for a single recipient
  message["Cc"] = ', '.join(cc_email)  # Convert list to string for CC field
  message["Subject"] = subject


  # Create HTML email content with CSS styling
  html_content = f"""
  <html>
    <head>
      <style>
        .styled-table {{
          border-collapse: collapse;
          width: 100%;
        }}
        .styled-table th, .styled-table td {{
          border: 1px solid black;
          padding: 8px;
          text-align: center;
          white-space: nowrap;
          ;
        }}
        .styled-table th {{
          font-weight: bold;
        }}
      </style>
    </head>
    <body>
      <p>Hello team,<br><br>
        Please find below the live terminal along with their gateway and method (card) and tokenization having 0% SR on method as of yesterday:<br><br>
        {html_table}<br><br>
        Kindly check these terminals and make the changes so that the issue is resolved.
        <br><br>
        Kind Regards,<br>
        Syed Mohammad Yamaan<br>
        7987388624
      </p>
    </body>
  </html>
  """

  # Attach HTML content to email
  email_body = MIMEText(html_content, "html")
  message.attach(email_body)

  # Connect to SMTP server and send email
  with smtplib.SMTP('smtp.gmail.com', 587) as server:
      server.starttls()
      server.login("syed.yamaan@razorpay.com", "dzou qmap lqml xjhe")
      server.sendmail(sender_email, receiver_email, message.as_string())

  print("Email sent successfully!")

else:
    print("DataFrame is empty. Email not sent.")

# COMMAND ----------

'''# Assuming you have the 'cards_dod' DataFrame ready

if not L2_mm_live_base_upi.empty:

  # Convert DataFrame to HTML table
  html_table = L2_mm_live_base_upi.to_html(index=False, border=1, classes='styled-table')

  sender_email = "syed.yamaan@razorpay.com"
  receiver_email = "chitraxi.raj@razorpay.com"
  cc_email = ['chitraxi.raj@razorpay.com']  # Add your CC email addresses here
  subject = "L2 Alerts for 0% SR for Optimizer Terminal (UPI)"

    # Create email message
  message = MIMEMultipart("alternative")
  message["From"] = sender_email
  message["To"] = receiver_email  # No need to convert to string for a single recipient
  message["Cc"] = ', '.join(cc_email)
  message["Subject"] = subject

  # Create HTML email content with CSS styling
  html_content = f"""
  <html>
    <head>
      <style>
        .styled-table {{
          border-collapse: collapse;
          width: 100%;
        }}
        .styled-table th, .styled-table td {{
          border: 1px solid black;
          padding: 8px;
          text-align: center;
          white-space: nowrap;
          ;
        }}
        .styled-table th {{
          font-weight: bold;
        }}
      </style>
    </head>
    <body>
      <p>Hello team,<br><br>
        Please find below the live terminal along with their gateway and method (UPI) and UPI App having 0% SR as of yesterday:<br><br>
        {html_table}<br><br>
        Kindly check these terminals and make the changes so that the issue is resolved.
        <br><br>
        Kind Regards,<br>
        Syed Mohammad Yamaan<br>
        7987388624
      </p>
    </body>
  </html>
  """

  # Attach HTML content to email
  email_body = MIMEText(html_content, "html")
  message.attach(email_body)

  # Connect to SMTP server and send email
  with smtplib.SMTP('smtp.gmail.com', 587) as server:
      server.starttls()
      server.login("syed.yamaan@razorpay.com", "dzou qmap lqml xjhe")
      server.sendmail(sender_email, receiver_email, message.as_string())

  print("Email sent successfully!")

else:
    print("DataFrame is empty. Email not sent.")

# COMMAND ----------

'''# Assuming you have the 'cards_dod' DataFrame ready

if not L2_mm_live_base_netbanking.empty:

  # Convert DataFrame to HTML table
  html_table = L2_mm_live_base_upi.to_html(index=False, border=1, classes='styled-table')

  sender_email = "syed.yamaan@razorpay.com"
  receiver_email = "chitraxi.raj@razorpay.com"
  cc_email = ['chitraxi.raj@razorpay.com']  # Add your CC email addresses here
  subject = "L2 Alerts for 0% SR for Optimizer Terminal (Netbanking)"

    # Create email message
  message = MIMEMultipart("alternative")
  message["From"] = sender_email
  message["To"] = receiver_email  # No need to convert to string for a single recipient
  message["Cc"] = ', '.join(cc_email)
  message["Subject"] = subject

  # Create HTML email content with CSS styling
  html_content = f"""
  <html>
    <head>
      <style>
        .styled-table {{
          border-collapse: collapse;
          width: 100%;
        }}
        .styled-table th, .styled-table td {{
          border: 1px solid black;
          padding: 8px;
          text-align: center;
          white-space: nowrap;
          ;
        }}
        .styled-table th {{
          font-weight: bold;
        }}
      </style>
    </head>
    <body>
      <p>Hello team,<br><br>
        Please find below the live terminal along with their gateway and method (UPI) and UPI App having 0% SR as of yesterday:<br><br>
        {html_table}<br><br>
        Kindly check these terminals and make the changes so that the issue is resolved.
        <br><br>
        Kind Regards,<br>
        Syed Mohammad Yamaan<br>
        7987388624
      </p>
    </body>
  </html>
  """

  # Attach HTML content to email
  email_body = MIMEText(html_content, "html")
  message.attach(email_body)

  # Connect to SMTP server and send email
  with smtplib.SMTP('smtp.gmail.com', 587) as server:
      server.starttls()
      server.login("syed.yamaan@razorpay.com", "dzou qmap lqml xjhe")
      server.sendmail(sender_email, receiver_email, message.as_string())

  print("Email sent successfully!")

else:
    print("DataFrame is empty. Email not sent.")

# COMMAND ----------

'''# Assuming you have the 'cards_dod' DataFrame ready

if not L3_mm_live_base_card.empty:

  # Convert DataFrame to HTML table
  html_table = L3_mm_live_base_card.to_html(index=False, border=1, classes='styled-table')

  # Email details
  sender_email = "syed.yamaan@razorpay.com"
  receiver_email = "chitraxi.raj@razorpay.com"
  cc_email = ['chitraxi.raj@razorpay.com']  # Add your CC email addresses here
  subject = "L3 Alerts for 0% SR for Optimizer Terminal on Card"

  # Create email message
  message = MIMEMultipart("alternative")
  message["To"] = receiver_email  # No need to convert to string for a single recipient
  message["Cc"] = ', '.join(cc_email)
  message["Subject"] = subject

  # Create HTML email content with CSS styling
  html_content = f"""
  <html>
    <head>
      <style>
        .styled-table {{
          border-collapse: collapse;
          width: 100%;
        }}
        .styled-table th, .styled-table td {{
          border: 1px solid black;
          padding: 8px;
          text-align: center;
          white-space: nowrap;
          ;
        }}
        .styled-table th {{
          font-weight: bold;
        }}
      </style>
    </head>
    <body>
      <p>Hello team,<br><br>
        Please find below the live terminal along with their gateway & tokenization & network having 0% SR on method - Cards as of yesterday:<br><br>
        {html_table}<br><br>
        Kindly check these terminals and make the changes so that the issue is resolved.
        <br><br>
        Kind Regards,<br>
        Syed Mohammad Yamaan<br>
        7987388624
      </p>
    </body>
  </html>
  """

  # Attach HTML content to email
  email_body = MIMEText(html_content, "html")
  message.attach(email_body)

  # Connect to SMTP server and send email
  with smtplib.SMTP('smtp.gmail.com', 587) as server:
      server.starttls()
      server.login("syed.yamaan@razorpay.com", "dzou qmap lqml xjhe")
      server.sendmail(sender_email, receiver_email, message.as_string())

  print("Email sent successfully!")

else:
    print("DataFrame is empty. Email not sent.")

# COMMAND ----------

'''# Assuming you have the 'cards_dod' DataFrame ready

if not L4_mm_live_base_card.empty:

  # Convert DataFrame to HTML table
  html_table = L4_mm_live_base_card.to_html(index=False, border=1, classes='styled-table')

  # Email details
  sender_email = "syed.yamaan@razorpay.com"
  receiver_email = "chitraxi.raj@razorpay.com"
  cc_email = ['chitraxi.raj@razorpay.com']  # Add your CC email addresses here
  subject = "L4 Alerts for 0% SR for Optimizer Terminal on Card"

  # Create email message
  message = MIMEMultipart("alternative")
  message["To"] = receiver_email  # No need to convert to string for a single recipient
  message["Cc"] = ', '.join(cc_email)
  message["Subject"] = subject

  # Create HTML email content with CSS styling
  html_content = f"""
  <html>
    <head>
      <style>
        .styled-table {{
          border-collapse: collapse;
          width: 100%;
        }}
        .styled-table th, .styled-table td {{
          border: 1px solid black;
          padding: 8px;
          text-align: center;
          white-space: nowrap;
          ;
        }}
        .styled-table th {{
          font-weight: bold;
        }}
      </style>
    </head>
    <body>
      <p>Hello team,<br><br>
        Please find below the live terminal along with their gateway & method &  having 0% SR on method as of yesterday:<br><br>
        {html_table}<br><br>
        Kindly check these terminals and make the changes so that the issue is resolved.
        <br><br>
        Kind Regards,<br>
        Syed Mohammad Yamaan,<br>
        7987388624
      </p>
    </body>
  </html>
  """

  # Attach HTML content to email
  email_body = MIMEText(html_content, "html")
  message.attach(email_body)

  # Connect to SMTP server and send email
  with smtplib.SMTP('smtp.gmail.com', 587) as server:
      server.starttls()
      server.login("syed.yamaan@razorpay.com", "dzou qmap lqml xjhe")
      server.sendmail(sender_email, receiver_email, message.as_string())

  print("Email sent successfully!")

else:
    print("DataFrame is empty. Email not sent.")

# COMMAND ----------

# MAGIC %md
# MAGIC Upload Data to Gsheet - [Sheet Link](https://docs.google.com/spreadsheets/d/1Gb8OjsCKlHYbZOHlpGWhN8Jm1BVdaVBTv5H6LmSetAk/edit?usp=sharing)

# COMMAND ----------

alert_list

# COMMAND ----------

alert_list = alert_list.drop(columns=['L0 Concatenated Column'])
alert_list = alert_list.drop(columns=['L1 Concatenated Column'])
alert_list = alert_list.drop(columns=['L2 Concatenated Column'])
#alert_list = alert_list.drop(columns=['L3 Concatenated Column'])


# COMMAND ----------



alert_list.insert(15, 'Network', 'All')
alert_list

# COMMAND ----------

alert_list

# COMMAND ----------

# Rearranging columns in alert_list DataFrame
alert_list = alert_list[['level','Merchant ID', 'Merchant Name', 'Terminal ID', 'Terminal Flag','Gateway', 'Method', 
                         'Bank', 'Tokenization', 'Network','Wallet', 'Date', 'Total Attempts', ]]

# COMMAND ----------

alert_list_base = alert_list

# COMMAND ----------

alert_list

# COMMAND ----------

filtered_alert_list = alert_list[(alert_list['Total Attempts'] >= 10) & (alert_list['Terminal ID'].isin(['LXgDQeIW4rXvW7']))
                                 
                                 
                                 ]


'''& (~alert_list['Terminal ID'].isin(['KIBPy1j8U6wV6A','KbUS4CqEuh3NbE','MLAzCbVfmVuM4f','Hhqd44htuuO2L4','Ii0NMM0gefTchH','IfbWvRmh4cSB71','MkSjT42EqC1XUF','GeWiZPXngpWtXQ','Igq8rkoj9bMrhD','OtQX9aV99TH5uZ'])) 
                                 #& (~alert_list['Bank'].isin(['HDFC'])) '''                                
filtered_alert_list

# COMMAND ----------


alert_list  = filtered_alert_list

# COMMAND ----------

import datetime
from decimal import Decimal
import json

# Function to convert datetime.date or Decimal object to a serializable format
def datetime_handler(x):
    if isinstance(x, datetime.date):
        return x.isoformat()
    elif isinstance(x, Decimal):
        return float(x)
    raise TypeError("Unknown type")

# Assuming alert_list is a DataFrame, convert it to a list of lists
data_to_upload = alert_list.values.tolist()

# Convert dataframe to JSON-serializable format
data_to_upload = [
    [
        json.dumps(datetime_handler(cell)).replace('"', '') if isinstance(cell, (datetime.date, Decimal)) 
        else json.dumps(cell).replace('"', '') if not isinstance(cell, (int, float, bool)) 
        else cell 
        for cell in row
    ] 
    for row in data_to_upload
]

print(data_to_upload)

# COMMAND ----------

len(data_to_upload)

# COMMAND ----------

import math

def replace_invalid_json_values(data):
    valid_data = []
    for row in data:
        valid_row = []
        for value in row:
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                valid_row.append(None)  # Replace invalid float with None
            else:
                valid_row.append(value)
        valid_data.append(valid_row)
    return valid_data

# Use this function to preprocess your data before uploading
data_to_upload = replace_invalid_json_values(data_to_upload)

# COMMAND ----------

import gspread
from google.oauth2.service_account import Credentials

def upload_to_google_sheet(data, credentials_file):
    # Load credentials
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(credentials_file, scopes=scopes)
    client = gspread.authorize(creds)

    # Open the Google Sheet
    # Get the spreadsheet ID
    spreadsheet_id = '1Gb8OjsCKlHYbZOHlpGWhN8Jm1BVdaVBTv5H6LmSetAk'

    sheet_name = "Sheet1"
    spreadsheet = client.open_by_key(spreadsheet_id)

    # Get the sheet by name
    sheet = spreadsheet.worksheet(sheet_name)

    # Determine last row and resize if needed
    rowCount = sheet.row_count
    lastRow = rowCount + 1
    max_rows = 100000

    # Ensure we do not exceed the maximum allowed rows
    if len(data) + lastRow > max_rows:
        raise ValueError("Data exceeds maximum allowed rows in the sheet")

    # Resize sheet if needed
    if len(data) + rowCount > rowCount:
        sheet.resize(rows=len(data) + rowCount)
        print("Sheet resized")

    print(len(data))

    print(rowCount)

    # Define the range for the data
    data_range = f'A{lastRow}:M{lastRow + len(data) - 1}'

    # Clear the specific range in the sheet
    # sheet.clear('A5:H' + str(lastRow))
    # sheet.clear(start='A5', end='H' + str(lastRow))

    #sheet.resize(rows=len(data) + 100)

    # Upload data to the sheet
    # sheet.insert_rows(data, 6)
    sheet.batch_update([{'range': data_range, 'values': data_to_upload}], value_input_option="user_entered")

    return spreadsheet_id


# Start of Gsheet Upload

credentials_file = "/dbfs/FileStore/Manish/google_ads_json/amg_service_acc.json"


spreadsheet_id = upload_to_google_sheet(data_to_upload, credentials_file)
print("Gsheet data uploaded successfully for this day ", spreadsheet_id)
sheet_link = "https://docs.google.com/spreadsheets/d/" + spreadsheet_id
#except Exception as e:
print("Data upload failed")
sheet_link = None
    # sqlContext.sql("""INSERT INTO aggregate_ba.service_account_logs SELECT 'Cohort_dashboard_update' as automaiton_name,'monthly' as schedule_type, '"""+sheet_link+"""' as sheet_link, current_date as updated_date,'Failed' as status""")
    # sqlContext.sql("""create table aggregate_ba.service_account_logs as (SELECT 'Cohort_dashboard_update' as automaiton_name,'monthly' as schedule_type, '"""+sheet_link+"""' as sheet_link, current_date as updated_date,'Failed' as status)""")

# COMMAND ----------

data_to_upload

# COMMAND ----------


import gspread
from google.oauth2.service_account import Credentials

def upload_to_google_sheet(data, credentials_file):
    # Load credentials
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(credentials_file, scopes=scopes)
    client = gspread.authorize(creds)
    
    # Open the Google Sheet
    spreadsheet_id = '1Gb8OjsCKlHYbZOHlpGWhN8Jm1BVdaVBTv5H6LmSetAk'
    sheet_name = "Sheet1"
    spreadsheet = client.open_by_key(spreadsheet_id)
    sheet = spreadsheet.worksheet(sheet_name)
    
    # Determine last row and resize if needed
    rowCount = sheet.row_count
    lastRow = rowCount + 1
    max_rows = 1000  # or any appropriate number according to your needs

    data
    
    # Ensure we do not exceed the maximum allowed rows
    if len(data) + lastRow > max_rows:
        raise ValueError("Data exceeds maximum allowed rows in the sheet")
    
    # Resize sheet if needed
    if len(data) + rowCount > rowCount:
        sheet.resize(rows=len(data) + rowCount)
        print("sheet resized")
    
    # Define the range for the data
    data_range = f'A{lastRow}:M{lastRow + len(data) - 1}'

    print(data)
    
    # Upload data to the sheet
    sheet.batch_update([{'range':data_range,'values':data_to_upload}],value_input_option="user_entered")

    return spreadsheet_id

# Start of Gsheet Upload
credentials_file = "/dbfs/FileStore/Manish/google_ads_json/amg_service_acc.json"
#data_to_upload = [['data1', 'data2', 'data3'], ['data4', 'data5', 'data6']]  # Replace with your actual data

try:
    data_to_upload
    spreadsheet_id = upload_to_google_sheet(data_to_upload, credentials_file)
    print("Gsheet data uploaded successfully for this day ", spreadsheet_id)
    
    sheet_link = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
    
    sqlContext.sql(f"""INSERT INTO aggregate_ba.service_account_logs 
        SELECT 'Cohort_dashboard_update' as automation_name, 'monthly' as schedule_type, '{sheet_link}' as sheet_link, current_date as updated_date, 'Success' as status""")
except Exception as e:
    print(e)
    sqlContext.sql(f"""INSERT INTO aggregate_ba.service_account_logs 
        SELECT 'Cohort_dashboard_update' as automation_name, 'monthly' as schedule_type, '{sheet_link}' as sheet_link, current_date as updated_date, 'Failed' as status""")

# COMMAND ----------

while True:
    user_input = int(input("Enter a number (enter 0 to quit): "))
    if user_input == 0:
        print("Exiting the loop.")
        break
    else:
        print(f"The square of {user_input} is: {user_input ** 2}")

# COMMAND ----------


