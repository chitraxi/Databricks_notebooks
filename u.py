# Databricks notebook source
# MAGIC %md
# MAGIC June Payments 

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists analytics_selfserve.NIA_June_payments_v1;
# MAGIC CREATE TABLE analytics_selfserve.NIA_June_payments_v1 AS 
# MAGIC SELECT
# MAGIC   date_format(date(created_date),'%d-%m') as "month",COUNT(*) total_payments
# MAGIC FROM
# MAGIC   realtime_hudi_api.payments
# MAGIC WHERE
# MAGIC   created_date >= '2024-06-01'
# MAGIC   AND created_date <= '2024-06-30'
# MAGIC   and merchant_id = 'If9Z0dDl6Vht65'
# MAGIC   and id in (select payment_id from aggregate_pa.optimizer_terminal_payments where  created_date >= '2024-06-01'
# MAGIC   AND created_date <= '2024-06-30') group by 1;
# MAGIC   
# MAGIC select * from analytics_selfserve.NIA_June_payments_v1
