# Databricks notebook source
pip install gspread

# COMMAND ----------

# DBTITLE 1,imports
import gspread
from google.oauth2.service_account import Credentials
import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from email.message import EmailMessage

import json
import datetime

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_selfserve.optimizer_juspay_gmv_cohorts limit 10

# COMMAND ----------

import datetime
from decimal import Decimal
import pandas as pd

# Query data from SQL context
data_to_upload = spark.sql("""select * from analytics_selfserve.optimizer_juspay_gmv_cohorts where gmv_month is not null limit 100""")
data_to_upload_df = data_to_upload.toPandas()

# Function to convert datetime.date or Decimal object to a serializable format
def datetime_handler(x):
    if isinstance(x, datetime.date):
        return x.isoformat()
    elif isinstance(x, Decimal):
        return float(x)
    raise TypeError("Unknown type")

# Convert dataframe to JSON-serializable format
data_to_upload = data_to_upload_df.values.tolist()

# Optionally, you can specify orient='records' to serialize as a list of dictionaries
data_to_upload = [[json.dumps(datetime_handler(cell)).replace('"', '') if isinstance(cell, (datetime.date, Decimal)) else json.dumps(cell).replace('"', '') if not isinstance(cell, (int, float, bool)) else cell for cell in row] for row in data_to_upload]

print(data_to_upload)

# COMMAND ----------

# DBTITLE 1,create spreadsheet

def to_create_spreadsheet(credentials_file):
    scopes = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(credentials_file,scopes=scopes)
    client = gspread.authorize(creds)
    
    #create a spreadsheet with any naming nomenclature u want
    spreadsheet = client.create(f"Call centre calling analysis")

    # Open the Google Sheet
    # Get the spreadsheet ID
    spreadsheet_id = spreadsheet.id


    #mention the email's that u wanna share this new gsheet to
    email_to_share_with = ['chitraxi.raj@razorpay.com']
    for user in email_to_share_with:
        # Share the sheet with the specified emails
        spreadsheet.share(user, perm_type='user', role='writer', notify=True)
    
    return(spreadsheet_id)

credentials_file = "/dbfs/FileStore/Manish/google_ads_json/amg_service_acc.json"

spreadsheet_id = to_create_spreadsheet(credentials_file)
print(spreadsheet_id)

# COMMAND ----------

# DBTITLE 1,Read SpreadSheet
import pandas as pd
import re
from pyspark.sql import SparkSession

# get the id from above created sheet and use that for data population

spreadsheet_id = '1eGW12ZMeuimRzFfNN6UwYdFMAiDKU4VthOHs5IImqB0'
#sheet_name = str("2024-01-21")
sheet_name = str("Sheet1")
#"23_and_24_dec_Calls"
#datetime.datetime.now().date()

def read_spreadsheet(credentials_file,spreadsheet_id,sheet_name):
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
     ]
    creds = Credentials.from_service_account_file(credentials_file,scopes=scopes)
    client = gspread.authorize(creds)

    # Open the Google Sheet
    # Get the spreadsheet ID
    spreadsheet = client.open_by_key(spreadsheet_id)
    sheet=spreadsheet.worksheet(sheet_name)
    data = sheet.get_all_values()

    if not data:
        raise ValueError("The sheet is empty or the specified sheet name does not exist.")

    data

    #print(data.columns())

    df = pd.DataFrame(data[1:], columns=data[0])
    # remove spaces and special characters from column names
    df.columns = [re.sub(r'\W+', '_', col.strip()) for col in df.columns]
    
    #spark = SparkSession.builder.getOrCreate()
    spark_df = spark.createDataFrame(df)
    spark_df.createOrReplaceTempView("temp_view_of_the_read_calling_data")
    return spark_df

credentials_file = "/dbfs/FileStore/Manish/google_ads_json/amg_service_acc.json"
df = read_spreadsheet(credentials_file,spreadsheet_id,sheet_name)

#use this temp_table to store the data that is read from spread sheet to new tables

#select * from temp_view_of_the_read_calling_data limit 1

# COMMAND ----------

data_to_upload

# COMMAND ----------

# DBTITLE 1,Upload Data to Spread Sheet
def upload_to_google_sheet(data, credentials_file):
    # Load credentials
    scopes = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(credentials_file,scopes=scopes)
    client = gspread.authorize(creds)
    
    #create a sheet with current month's name
    # spreadsheet = client.create(f"Churned MIDs in {month}")

    # Open the Google Sheet
    # Get the spreadsheet ID
    spreadsheet_id = '1Gb8OjsCKlHYbZOHlpGWhN8Jm1BVdaVBTv5H6LmSetAk'

    sheet_name = "Sheet1"
    spreadsheet = client.open_by_key(spreadsheet_id)

    # email_to_share_with = ['harshita.bhardwaj@razorpay.com']
    # for user in email_to_share_with:
    #     # Share the sheet with the specified email
    #     spreadsheet.share(user, perm_type='user', role='writer', notify=False)

    sheet=spreadsheet.worksheet(sheet_name)
    
    # print(f"Spreadsheet shared! ID: {spreadsheet_id}")

    # find the last row in the sheet
    rowCount = sheet.row_count
    lastRow = rowCount + 1

    data_range='A2:C' + str(lastRow)

    # Clear the specific range in the sheet
    # sheet.clear('A5:H' + str(lastRow))
    #sheet.clear(start='A5', end='H' + str(lastRow))
    
    sheet.resize(rows=len(data_to_upload)+1000)

    # Upload data to the sheet
    # sheet.insert_rows(data,6)
    sheet.batch_update([{'range':data_range,'values':data_to_upload}],value_input_option="user_entered")

    return spreadsheet_id

##start of Gsheet Upload

credentials_file = "/dbfs/FileStore/Manish/google_ads_json/amg_service_acc.json"

#try:
spreadsheet_id = upload_to_google_sheet(data_to_upload, credentials_file)

print("Gsheet data uploaded successfully for this day ",spreadsheet_id)

sheet_link = "https://docs.google.com/spreadsheets/d/"+spreadsheet_id

try:
    sqlContext.sql("""INSERT INTO aggregate_ba.service_account_logs SELECT 'Cohort_dashboard_update' as automaiton_name,'monthly' as schedule_type, '"""+sheet_link+"""' as sheet_link, current_date as updated_date,'Success' as status""")
except:
    sqlContext.sql("""create table aggregate_ba.service_account_logs as (SELECT 'Cohort_dashboard_update' as automaiton_name,'monthly' as schedule_type, '"""+sheet_link+"""' as sheet_link, current_date as updated_date,'Success' as status)""")

    # send_email('deekonda.sai@razorpay.com', 'deekonda.sai@razorpay.com', 'Churn MIDs List for this month', sheet_link)
'''except Exception as e:
    if(spreadsheet_id is None):
        sheet_link=None
    try:
        print(e)
        sqlContext.sql("""INSERT INTO aggregate_ba.service_account_logs SELECT 'Cohort_dashboard_update' as automaiton_name,'monthly' as schedule_type, '"""+sheet_link+"""' as sheet_link, current_date as updated_date,'Failed' as status""")
    except:
        sqlContext.sql("""create table aggregate_ba.service_account_logs as (SELECT 'Cohort_dashboard_update' as automaiton_name,'monthly' as schedule_type, '"""+sheet_link+"""' as sheet_link, current_date as updated_date,'Failed' as status)""")'''


# COMMAND ----------

import gspread
from gspread.exceptions import APIError
from google.auth.credentials import Credentials
import datetime
import pandas as pd
from pyspark.sql import SQLContext

# Assuming you are using PySpark
sqlContext = SQLContext(sc)

def upload_to_google_sheet(data_to_upload, credentials_file, sheet_id):
    try:
        # Load credentials
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_file(credentials_file, scopes=scopes)
        client = gspread.authorize(creds)

        # Open the Google Sheet
        spreadsheet = client.open_by_key(sheet_id)
        sheet_name = "Sheet1"
        sheet = spreadsheet.worksheet(sheet_name)

        # find the last row in the sheet
        rowCount = sheet.row_count
        lastRow = rowCount + 1
        data_range = f'A{lastRow}:C{lastRow + len(data_to_upload) - 1}'

        print(lastRow,rowCount)

        # Batch update the data
        sheet.batch_update([
            {'range': data_range, 'values': data_to_upload}
        ], value_input_option="USER_ENTERED")

        return sheet.id  # Return the spreadsheet ID after successful upload
    except APIError as e:
        print(f"Error uploading data to Google Sheet: {e}")
        raise  # Re-raise the exception to handle it elsewhere

# Example usage:
credentials_file = "/path/to/your/credentials.json"
sheet_id = '1eGW12ZMeuimRzFfNN6UwYdFMAiDKU4VthOHs5IImqB0'
data_to_upload = [
    ['value1', 'value2', 'value3'],
    ['value4', 'value5', 'value6']
]

try:
    print(data_to_upload, credentials_file, sheet_id)
    spreadsheet_id = upload_to_google_sheet(data_to_upload, credentials_file, sheet_id)
    print(f"Gsheet data uploaded successfully for this day, spreadsheet ID: {spreadsheet_id}")
    
    sheet_link = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"

    # Insert into SQL Context
    sql_query = f"""
        INSERT INTO aggregate_ba.service_account_logs 
        SELECT 'Cohort_dashboard_update' as automaiton_name,
               'monthly' as schedule_type,
               '{sheet_link}' as sheet_link,
               current_date as updated_date,
               'Success' as status
    """
    sqlContext.sql(sql_query)
    
except Exception as e:
    print(f"Failed to upload data to Google Sheet: {e}")

    sheet_link = None
    
    # Insert into SQL Context
    sql_query = f"""
        INSERT INTO aggregate_ba.service_account_logs 
        SELECT 'Cohort_dashboard_update' as automaiton_name,
               'monthly' as schedule_type,
               '{sheet_link}' as sheet_link,
               current_date as updated_date,
               'Failed' as status
    """
    sqlContext.sql(sql_query)


# COMMAND ----------

# DBTITLE 1,Add worksheets
def upload_to_google_sheet(l1_data_to_upload, l2_data_to_upload, credentials_file, l1_spreadsheet_id, l2_spreadsheet_id):
    # Load credentials
    scopes = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(credentials_file,scopes=scopes)
    client = gspread.authorize(creds)
    
    #create a sheet with current month's name
    # spreadsheet = client.create(f"Churned MIDs in {month}")

    # Open the Google Sheet
    
    sheet_name = "Sheet1"
    l1_spreadsheet = client.open_by_key(l1_spreadsheet_id)

    l1_spreadsheet.add_worksheet(title=str(now_local.strftime("%d-%b")),rows=len(l1_data_to_upload)+10,cols=20)

    l2_spreadsheet = client.open_by_key(l2_spreadsheet_id)

    l2_spreadsheet.add_worksheet(title=str(now_local.strftime("%d-%b")),rows=len(l2_data_to_upload)+10,cols=20)

    # email_to_share_with = ['harshita.bhardwaj@razorpay.com']
    # for user in email_to_share_with:
    #     # Share the sheet with the specified email
    #     spreadsheet.share(user, perm_type='user', role='writer', notify=False)

    l1_sheet=l1_spreadsheet.worksheet(now_local.strftime("%d-%b"))

    l2_sheet=l2_spreadsheet.worksheet(now_local.strftime("%d-%b"))
    
    # print(f"Spreadsheet shared! ID: {spreadsheet_id}")

    # find the last row in the sheet
    l1_sheet.resize(rows=len(l1_data_to_upload)+10)

    l2_sheet.resize(rows=len(l2_data_to_upload)+10)
    
    rowCount_l1 = l1_sheet.row_count
    lastRow_l1 = rowCount_l1

    l1_data_range='A1:T' + str(lastRow_l1)

    rowCount_l2 = l2_sheet.row_count
    lastRow_l2 = rowCount_l2

    l2_data_range='A1:T' + str(lastRow_l2)

    # Clear the specific range in the sheet
    # sheet.clear('A5:H' + str(lastRow))
    #sheet.clear(start='A5', end='H' + str(lastRow))

    # Upload data to the sheet
    # sheet.insert_rows(data,6)
    l1_sheet.batch_update([{'range':l1_data_range,'values':l1_data_to_upload}],value_input_option="user_entered")

    l2_sheet.batch_update([{'range':l2_data_range,'values':l2_data_to_upload}],value_input_option="user_entered")

    return [l1_spreadsheet_id,l2_spreadsheet_id]

