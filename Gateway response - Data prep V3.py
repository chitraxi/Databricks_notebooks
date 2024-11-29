# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import json
import pandas as pd
from pyspark.sql.window import Window

# Create a Spark session
spark = SparkSession.builder \
    .appName("Gateway Response app") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()


# Run the first SQL query
#result1 = spark.sql("delete from delta.analytics_selfserve.optimizer_gateway_response where created_date = current_date() + interval '-1' day")


df = spark.sql("""select p.auth_type,p.method_advanced, p.gateway, response.id,response.gateway,action,raw_response, response.source_id, response_body,response.status,p.created_date 
    from (
        select id,merchant_id,gateway,internal_error_code,method as method_advanced,created_date,auth_type 
        from realtime_hudi_api.payments 
        where created_date between '2024-04-01' and '2024-06-05'
        --and gateway ='ingenico'
        and gateway in ('cashfree') 
        and authorized_at is null and id in (select payment_id from aggregate_pa.optimizer_terminal_payments where producer_created_date between '2024-04-01' and '2024-06-05')
    ) as p
    left join (
        select id,gateway,action,raw_response,response_body,source_id ,status
        from realtime_mozart.audits 
        where created_date between '2024-04-01' and '2024-06-05'
        --and gateway in ('billdesk_optimizer','payu','paytm','pinelabs') 
        and source_type = 'payment' and action in ('pay_verify','verify','pay_init') AND (case when gateway = 'billdesk_optimizer' and action = 'pay_verify' then status != 0 else true end ) order by id) as response on response.source_id = p.id order by id
                """)
#and merchant_id = 'ENiVVm6cLOMkaa' and internal_error_code = 'GATEWAY_ERROR_PAYMENT_FAILED' and gateway = 'cashfree' \
#and gateway ='billdesk_optimizer' and network = 'Diners Club' and method_advanced = 'Credit Card' \

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


df = df.withColumn("error_message",F.coalesce((F.get_json_object(F.col('json_data'), '$.transaction_error_desc')),(F.get_json_object(F.col('json_data'), '$.txn_response_msg')),(F.get_json_object(F.col('json_data'), '$.transaction_details.0.error_Message')),(F.get_json_object(F.col("json_data"),"$.txMsg")),(F.get_json_object(F.col('json_data'), '$.Acquirer_Response_Message')),(F.get_json_object(F.col('json_data'), '$.ErrorMsg')),(F.get_json_object(F.col('json_data'), '$.ErrorText')),(F.get_json_object(F.col('json_data'), '$.RESPMSG')),(F.get_json_object(F.col('json_data'), '$.body.bankForm.redirectForm.content.msg')),(F.get_json_object(F.col('json_data'), '$.body.content.RESPCODE')),(F.get_json_object(F.col('json_data'), '$.body.content.RESPMSG')),(F.get_json_object(F.col('json_data'), '$.body.content.msg')),(F.get_json_object(F.col('json_data'), '$.body.resultInfo.resultMsg')),(F.get_json_object(F.col('json_data'), '$.body.retryInfo.retryMessage')),(F.get_json_object(F.col('json_data'), '$.error.desc')),(F.get_json_object(F.col('json_data'), '$.order_bank_response')),(F.get_json_object(F.col('json_data'), '$.errorMessage')),(F.get_json_object(F.col('json_data'), '$.paymentMethod.errorMessage')),(F.get_json_object(F.col('json_data'), '$.error_Message')),(F.get_json_object(F.col('json_data'), '$.gateway_response')),(F.get_json_object(F.col('json_data'), '$.metaData.message')),(F.get_json_object(F.col('json_data'), '$.msg')),(F.get_json_object(F.col('json_data'), '$.offer_failure_reason')),(F.get_json_object(F.col('json_data'), '$.parent_txn_response_message')),(F.get_json_object(F.col('json_data'), '$.ppc_ParentTxnResponseMessage')),(F.get_json_object(F.col('json_data'), '$.redirect.error_Message')),(F.get_json_object(F.col('json_data'), '$.response_message')),(F.get_json_object(F.col('json_data'), '$.txn_response_code')),(F.get_json_object(F.col('json_data'), '$.message')),(F.get_json_object(F.col('json_data'), '$.ppc_TxnResponseMessage')),(F.get_json_object(F.col('json_data'), '$.payment_message'))))


df = df.withColumn("pay_init_error_Message", F.when(F.col("action") == 'pay_init',F.coalesce((F.get_json_object(F.col("json_data"), "$.transaction_error_desc")),(F.get_json_object(F.col("json_data"),"$.txMsg")),F.get_json_object(F.col("json_data"),"$.0.error_Message"),F.get_json_object(F.col("json_data"),"$.body.resultMsg"),F.get_json_object(F.col("json_data"), "$.transaction_details.0.error_Message"),F.get_json_object(F.col("json_data"), "$.ppc_ParentTxnResponseMessage"),F.get_json_object(F.col("json_data"), "$.body.resultInfo.resultMsg"),F.get_json_object(F.col("json_data"), "$.payment_message"),F.get_json_object(F.col("json_data"),"$.0.error_details.error_reason"),F.get_json_object(F.col("json_data"),"$.enc_response"),F.get_json_object(F.col("json_data"),"$.0.error_details.error_reason"),F.get_json_object(F.col("json_data"),"$.RESPMSG"),F.get_json_object(F.col("json_data"),"ErrorMsg"),F.get_json_object(F.col("json_data"),"message"),F.get_json_object(F.col("json_data"),"ErrorMsg"),(F.get_json_object(F.col("json_data"),"$.body.resultInfo.resultMsg")),(F.get_json_object(F.col('json_data'), '$.message')))))


df = df.withColumn("txn_process_type", F.when(F.col("action") == 'pay_init',F.coalesce((F.get_json_object(F.col("json_data"), "$.txn_process_type")))))


df = df.withColumn("field9", F.when(F.col("action") == 'pay_verify',F.coalesce((F.get_json_object(F.col("json_data"), "$.transaction_details.0.field9")))))

df = df.withColumn("field8", F.when(F.col("action") == 'pay_verify',F.coalesce((F.get_json_object(F.col("json_data"), "$.transaction_details.0.field8")))))

#df = df.withColumn("verify_error_status", F.when(F.col("action") == 'verify',F.coalesce(F.get_json_object(F.col("json_data"), "$.RESPCODE"),(F.get_json_object(F.col("json_data"),"$.ErrorCode")),F.get_json_object(F.col("json_data"),"$.status"),F.get_json_object(F.col("json_data"),"$.transaction_error_code"),F.get_json_object(F.col("json_data"), "$.transaction_details.0.error_Message"),F.get_json_object(F.col("json_data"), "$.ppc_ParentTxnResponseMessage"),F.get_json_object(F.col("json_data"), "$.body.resultInfo.resultMsg"),F.get_json_object(F.col("json_data"), "$.payment_message"),(F.get_json_object(F.col("json_data"),"$.0.error_details.error_reason")))))

#display(df.filter(F.length(F.col("error_message")) == 0 ))

#display(df.filter(((F.col("action") == 'verify') | (F.col("action") == 'pay_init'))&( (F.col("pay_init_error_Message")).isNotNull()) |   (F.col("pay_init_error_Message")).isNotNull()))




# COMMAND ----------



# COMMAND ----------

#distinct_id_count = df.select(F.countDistinct("source_id").alias("distinct_id_count"))
#distinct_id_count.show()

# COMMAND ----------

df = df.filter((F.col("raw_response")).isNotNull()) 

# COMMAND ----------

distinct_actions = df.select("action").distinct()
distinct_actions.show()

# COMMAND ----------

# Filter DataFrame for pay_init action

pay_init_df = df.filter(df["action"] == "pay_init")

# Pivot pay_init DataFrame
pay_init_pivot = pay_init_df.groupBy("source_id","method_advanced","p.gateway","p.created_date").pivot("action").agg(
    F.last("json_data").alias("json_data_pay_init"),
    F.last("pay_init_error_Message").alias("error_Message_pay_init"),
    F.last("txn_process_type").alias("txn_process_type"),
    F.last("status").alias("status_pay_init"),
    #F.first("created_date").alias("created_date_pay_verify")
)


pay_verify_df = df.filter(df["action"] == "pay_verify")

# Pivot pay_init DataFrame
pay_verify_pivot = pay_verify_df.groupBy("source_id","method_advanced","p.gateway","p.created_date").pivot("action").agg(
    F.first("action").alias("action_pay_verify"),
    F.last("json_data").alias("json_data_pay_verify"),
    F.last("error_Message").alias("error_Message_pay_verify"),
    F.last("field8").alias("field8"),
    F.last("field9").alias("field9"),
    #F.first("created_date").alias("created_date_pay_verify")
)

#display(pay_init_pivot)

# Filter DataFrame for verify action
verify_df = df.filter(df["action"] == "verify")

# Pivot verify DataFrame
verify_pivot = verify_df.groupBy("source_id","method_advanced","p.gateway","p.created_date").pivot("action").agg(
    F.first("action").alias("action_verify"),
    F.last("json_data").alias("json_data_verify"),
    F.last("error_Message").alias("error_Message_verify"),
    #F.first("created_date").alias("created_date_verify")
)

# Join the pivoted DataFrames based on id
pivoted_df   = pay_init_pivot.join(verify_pivot, on=["source_id", "method_advanced", "gateway","created_date"], how="outer").join(pay_verify_pivot, on=["source_id", "method_advanced", "gateway","created_date"], how="outer")



# Show the resulting DataFrame
pivoted_df.show()

# COMMAND ----------

print(pivoted_df.columns)

# COMMAND ----------



# Specify the table name in the DataLake
datalake_table_name = "analytics_selfserve.optimizer_gateway_response_v2"

columns_order = ["source_id", "method_advanced", "gateway","pay_verify_action_pay_verify","pay_verify_json_data_pay_verify","pay_verify_error_Message_pay_verify","verify_action_verify","verify_json_data_verify","verify_error_Message_verify","pay_init_error_Message_pay_init","pay_verify_field8","pay_verify_field9","pay_init_status_pay_init","created_date"]
# Write the DataFrame to the DataLake table
#pivoted_df.select(columns_order).write.format("PARQUET").mode("overwrite").saveAsTable(datalake_table_name)
pivoted_df.select(columns_order).write.insertInto(datalake_table_name)





# COMMAND ----------

# MAGIC %sql
# MAGIC select p_id, count(*) from whs_trino_v.optimizer_payments_flat_fact where p_created_date between '2024-09-01' and '2024-09-15' group by 1 having 2 > 1 order by 2 desc 

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table <table_name> add column if not exists <column_name> <data_tyoe>;
