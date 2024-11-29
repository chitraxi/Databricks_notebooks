# Databricks notebook source
import shutil
from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from IPython.display import display, HTML
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("Optimizer competitior") \
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
with juspay_payments as (SELECT
    *, case when lower(callback_url) LIKE '%juspay%' then 'juspay'
                         when lower(callback_url) LIKE '%cashfree%' then 'cashfree'
                         when  lower(callback_url) LIKE '%paytm%' then 'paytm'  
                         when lower(callback_url) LIKE '%payu%' then 'payu' 
                         when  lower(callback_url) LIKE '%nimbbl%' then 'nimbbl' 
                         when lower(callback_url) LIKE '%pinelabs%' then 'pinelabs' end as callback
  FROM
    realtime_hudi_api.payments
  WHERE
    created_date BETWEEN '2023-12-01' AND '2024-06-30'
    and (lower(callback_url) LIKE '%juspay%' or lower(callback_url) LIKE '%cashfree%'
                         or  lower(callback_url) LIKE '%paytm%' or  lower(callback_url) LIKE '%payu%' or  lower(callback_url) LIKE '%nimbbl%' or  lower(callback_url) LIKE '%pinelabs%')
    AND gateway IS NOT NULL
    AND authorized_at IS NOT NULL
 ),
 
 
team_mapping_merchant_details as ( select id as merchant_id,category2,coalesce(billing_label,name) as name, team_owner from realtime_hudi_api.merchants
left join  (select merchant_id, owner_role__c, team_owner, name as poc_name from aggregate_ba.final_team_tagging) c
 on merchants.id = c.merchant_id)
 
 
select date_format(date(created_date),'%m-%y'),callback,team_owner,category2, sum(base_amount)*1.00/1000000000 as gmv,count(distinct juspay_payments.merchant_id) as mtu from juspay_payments left join team_mapping_merchant_details on team_mapping_merchant_details.merchant_id =juspay_payments.merchant_id
group by 1,2,3,4




                        
""")




# COMMAND ----------

montly_gmv = df_base_data.toPandas()

# COMMAND ----------

montly_gmv.to_csv("/dbfs/FileStore/Chitraxi/montly_gmv.csv")


spark.read.format('csv').load('dbfs:/FileStore/Chitraxi/montly_gmv.csv').display()
