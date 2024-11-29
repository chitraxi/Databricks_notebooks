# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import json
import pandas as pd

# Create a Spark session
spark = SparkSession.builder \
    .appName("Gateway Response app") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

df = spark.sql("select id,gateway,action,raw_response,response_body from realtime_mozart.audits where gateway in ('billdesk_optimizer','payu','paytm','pinelabs') and created_date = '2024-03-20'")

# Create a DataFrame from the data
#df = data

# Extract the "Data" field
df = df.withColumn("json_data", F.get_json_object(F.col("response_body"), "$.Data"))

# Define a function to validate JSON
def validate_json(json_string):
    if json_string is not None:
        try:
            json.loads(json_string)
            return json_string
        except ValueError:
            return None
    else:
        return None

# Define the UDF
validate_json_udf = F.udf(validate_json, StringType())

# Apply the UDF to the "json_data" column
df = df.withColumn("valid_json_data", validate_json_udf(F.col("json_data")))

df = df.withColumn("msg", F.get_json_object(F.col("json_data"), "$.msg"))

df = df.withColumn("transaction_details", F.get_json_object(F.col("json_data"), "$.transaction_details"))

df = df.withColumn("verify_error_Message", F.when(F.col("action") == 'verify',F.coalesce(F.get_json_object(F.col("json_data"), "$.transaction_error_type"),(F.get_json_object(F.col("json_data"),"$.txMsg")),F.get_json_object(F.col("json_data"),"$.0.error_Message"),F.get_json_object(F.col("json_data"),"$.body.resultMsg"),F.get_json_object(F.col("json_data"), "$.transaction_details.0.error_Message"),F.get_json_object(F.col("json_data"), "$.ppc_ParentTxnResponseMessage"),F.get_json_object(F.col("json_data"), "$.body.resultInfo.resultMsg"),F.get_json_object(F.col("json_data"), "$.payment_message"),F.get_json_object(F.col("json_data"),"$.0.error_details.error_reason"),F.get_json_object(F.col("json_data"),"$.enc_response"),F.get_json_object(F.col("json_data"),"$.0.error_details.error_reason"),F.get_json_object(F.col("json_data"),"$.RESPMSG"),F.get_json_object(F.col("json_data"),"$.ErrorMsg"),F.get_json_object(F.col("json_data"),"$.message"),(F.get_json_object(F.col("json_data"),"$.transaction_error_desc")))))



df = df.withColumn("pay_init_error_Message", F.when(F.col("action") == 'pay_init',F.coalesce((F.get_json_object(F.col("json_data"), "$.transaction_error_desc")),(F.get_json_object(F.col("json_data"),"$.txMsg")),F.get_json_object(F.col("json_data"),"$.0.error_Message"),F.get_json_object(F.col("json_data"),"$.body.resultMsg"),F.get_json_object(F.col("json_data"), "$.transaction_details.0.error_Message"),F.get_json_object(F.col("json_data"), "$.ppc_ParentTxnResponseMessage"),F.get_json_object(F.col("json_data"), "$.body.resultInfo.resultMsg"),F.get_json_object(F.col("json_data"), "$.payment_message"),F.get_json_object(F.col("json_data"),"$.0.error_details.error_reason"),F.get_json_object(F.col("json_data"),"$.enc_response"),F.get_json_object(F.col("json_data"),"$.0.error_details.error_reason"),F.get_json_object(F.col("json_data"),"$.RESPMSG"),F.get_json_object(F.col("json_data"),"ErrorMsg"),F.get_json_object(F.col("json_data"),"message"),F.get_json_object(F.col("json_data"),"ErrorMsg"),(F.get_json_object(F.col("json_data"),"$.body.resultInfo.resultMsg")))))


#df = df.withColumn("verify_error_status", F.when(F.col("action") == 'verify',F.coalesce(F.get_json_object(F.col("json_data"), "$.RESPCODE"),(F.get_json_object(F.col("json_data"),"$.ErrorCode")),F.get_json_object(F.col("json_data"),"$.status"),F.get_json_object(F.col("json_data"),"$.transaction_error_code"),F.get_json_object(F.col("json_data"), "$.transaction_details.0.error_Message"),F.get_json_object(F.col("json_data"), "$.ppc_ParentTxnResponseMessage"),F.get_json_object(F.col("json_data"), "$.body.resultInfo.resultMsg"),F.get_json_object(F.col("json_data"), "$.payment_message"),(F.get_json_object(F.col("json_data"),"$.0.error_details.error_reason")))))

display(df.filter(((F.col("action") == 'verify') | (F.col("action") == 'pay_init')) & (F.col("raw_response")).isNotNull()) )

#display(df.filter(((F.col("action") == 'verify') | (F.col("action") == 'pay_init'))&( (F.col("pay_init_error_Message")).isNotNull()) |   (F.col("pay_init_error_Message")).isNotNull()))

# Function to get all keys from JSON schema recursively
def get_all_keys(schema):
    keys = []
    for field in schema.fields:
        if field.dataType.typeName() == "struct":
            keys += [f"{field.name}.{subkey}" for subkey in get_all_keys(field.dataType)]
        else:
            keys.append(field.name)
    return keys



# COMMAND ----------

# Step 1: Filter out null values from the json_data column
df = df.filter(F.col("json_data").isNotNull())

# Step 2: Get the schema of the JSON column
json_schema = spark.read.json(df.select("json_data").rdd.map(lambda x: x.json_data)).schema

# Function to extract all keys recursively
def get_all_keys(schema, prefix=""):
    keys = []
    for field in schema.fields:
        field_name = prefix + "." + field.name if prefix else field.name
        keys.append(field_name)
        if hasattr(field.dataType, "fields"):
            keys.extend(get_all_keys(field.dataType, prefix=field_name))
    return keys

# Step 3: Get all keys from the schema recursively
all_keys = get_all_keys(json_schema)



# COMMAND ----------

print(all_keys)

# COMMAND ----------

keys_to_extract = ['body.resultInfo.resultMsg','enc_error_code','error','error','errorMessage','redirect.error','error_Message','error_desc','paymentMethod.error.desc','paymentMethod.paymentTransaction.errorMessage','ppc_AcquirerResponseMessage','redirect.error_Message','transaction_details.0.error_Message','transaction_details.0.error_code','transaction_error_desc']

# COMMAND ----------

all_keys

# COMMAND ----------


# Convert Pandas DataFrame to Spark DataFrame
spark_df = df.select("json_data")

print(spark_df)

# Display the schema of the DataFrame to ensure it matches your expectations
spark_df.printSchema()

# Display some sample records from the DataFrame
spark_df.show()

# Convert Spark DataFrame to JSON string
json_data = spark_df.toJSON().collect()

# Load the JSON string back to a Python list of dictionaries
data_list = [json.loads(record) for record in json_data]

# Display the first few records to verify the structure
print("First few records:")
print(data_list[:5])

# COMMAND ----------

def extract_value(data, keys_to_find):
    for key in keys_to_find:
        if isinstance(data, dict):
            if key in data:
                return data[key]
            else:
                for k, v in data.items():
                    result = extract_value(v, keys_to_find)
                    if result is not None:
                        return result
        elif isinstance(data, list):
            for item in data:
                result = extract_value(item, keys_to_find)
                if result is not None:
                    return result
    return None

# COMMAND ----------

# Initialize an empty list to store the extracted values in tabular format
extracted_table = []

# Define the keys to search for in the JSON data
keys_to_find = ['resultMsg', 'error_message', 'description']  # Add other keys as needed

# Loop through each dictionary in data_list
for data_dict in data_list:
    # Initialize extracted_dict for the row
    extracted_dict = {}
    
    # Check if 'json_data' key exists in data_dict
    if 'json_data' in data_dict:
        # Attempt to parse JSON stored in 'json_data' string
        try:
            json_data = json.loads(data_dict['json_data'])
            
            # Extract value using the defined keys_to_find
            extracted_value = extract_value(json_data, keys_to_find)
            
            if extracted_value is not None:
                # Store the 'json_data' and 'extracted_value' in the row dictionary
                extracted_dict['json_data'] = data_dict['json_data']
                extracted_dict['extracted_value'] = extracted_value
        except json.JSONDecodeError as e:
            print("Error decoding JSON:", e)
    
    # Append the row dictionary to extracted_table as a tuple
    extracted_table.append((extracted_dict.get('json_data', ''), extracted_dict.get('extracted_value', '')))

# Display the extracted table
print("Print the extracted table")
for row in extracted_table:
    print(row)

# COMMAND ----------


# Initialize an empty list to store the extracted values
extracted_values = []

# Loop through each dictionary in data_list
for data_dict in data_list:
    extracted_dict = {}
    
    # Check if 'json_data' key exists in data_dict
    if 'json_data' in data_dict:
        # Attempt to parse JSON stored in 'json_data' string
        try:
            json_data = json.loads(data_dict['json_data'])
            # Access nested keys in the parsed JSON data
            if 'body' in json_data and 'resultInfo' in json_data['body']:
                result_info = json_data['body']['resultInfo']
                if 'resultMsg' in result_info:
                    extracted_dict['resultMsg'] = result_info['resultMsg']
        except json.JSONDecodeError as e:
            print("Error decoding JSON:", e)
    
    # Append the extracted_dict to extracted_values
    extracted_values.append(extracted_dict)

# Display the extracted values
print("Print the extracted values")
print(extracted_values)

# COMMAND ----------

# Convert Pandas DataFrame to Spark DataFrame
spark_df = spark.createDataFrame(pandas_df)

# Convert Spark DataFrame to JSON string
json_data = spark_df.toJSON().collect()

# Load the JSON string back to a Python list
data_list = [json.loads(record) for record in json_data]

display(data_list)

# Extract values for specified keys
extracted_values = [{key: data_dict[key] for key in keys_to_extract if key in data_dict} for data_dict in data_list]

# Convert the extracted values to JSON (if needed)
extracted_values_json = json.dumps(extracted_values, indent=2)

print("Print the extracted values")
#print(extracted_values)
# Print the extracted values
'''for data_dict in extracted_values:
    #print(data_dict)
    for key, value in data_dict.items():
        print(f"{key}: {value}")'''

# COMMAND ----------



